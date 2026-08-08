package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"
	"quorum/internal/job"
)

var jobsBucket = []byte("jobs")

// BoltStore is a persistent implementation of Store backed by bbolt (BoltDB).
//
// bbolt is an embedded key-value store that writes to a single file on disk.
// It provides ACID transactions and a B-tree index, making it suitable for
// production workloads that require job durability across control node restarts.
//
// Storage layout:
//   bucket "jobs" → key: strconv.Itoa(job.ID) → value: JSON-encoded job.Job
//
// All reads use bbolt read transactions (concurrent safe).
// All writes use bbolt write transactions (serialised; bbolt allows only one
// concurrent write transaction at a time, which suits our single-writer pattern).
//
// Why bbolt?
//   - Used internally by etcd for its Raft storage layer.
//   - Single binary dependency, no external process required.
//   - ACID transactions with crash-safe writes (fsync on commit).
//   - B-tree storage gives O(log n) reads and writes.
//   - Simple, well-understood API.
//
// Tradeoff: bbolt allows only one write transaction at a time. Under high
// write throughput this becomes a bottleneck. For Phase 13 (Performance) we
// would evaluate replacing with a more concurrent backend (e.g., badger, RocksDB).
type BoltStore struct {
	db *bolt.DB
}

// NewBoltStore opens (or creates) a bbolt database file at path and initialises
// the jobs bucket. Returns an error if the file cannot be opened or the
// bucket cannot be created.
func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db at %q: %w", path, err)
	}

	// Ensure the jobs bucket exists. CreateBucketIfNotExists is idempotent.
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists(jobsBucket)
		return err
	}); err != nil {
		return nil, fmt.Errorf("create jobs bucket: %w", err)
	}

	return &BoltStore{db: db}, nil
}

// Close flushes pending writes and closes the database file.
// Must be called before the process exits.
func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) Add(j job.Job) {
	if err := s.db.Update(func(tx *bolt.Tx) error {
		return put(tx, j)
	}); err != nil {
		// Store.Add has no error return by interface contract. Log and continue;
		// a failed write here means the job will be lost on restart but the
		// in-flight execution can still complete.
		// Future: propagate error through Store.Add in a later refactor.
		_ = err
	}
}

func (s *BoltStore) Get(id int) (job.Job, bool) {
	var j job.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(jobsBucket).Get(key(id))
		if v == nil {
			return errNotFound
		}
		return json.Unmarshal(v, &j)
	})
	if err != nil {
		return job.Job{}, false
	}
	return j, true
}

func (s *BoltStore) List() []job.Job {
	var jobs []job.Job
	_ = s.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		return b.ForEach(func(k, v []byte) error {
			var j job.Job
			if err := json.Unmarshal(v, &j); err != nil {
				return err
			}
			jobs = append(jobs, j)
			return nil
		})
	})
	return jobs
}

func (s *BoltStore) Update(j job.Job) {
	_ = s.db.Update(func(tx *bolt.Tx) error {
		return put(tx, j)
	})
}

func (s *BoltStore) Delete(id int) bool {
	found := false
	_ = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		if b.Get(key(id)) == nil {
			return nil
		}
		found = true
		return b.Delete(key(id))
	})
	return found
}

func (s *BoltStore) Cancel(id int) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		v := b.Get(key(id))
		if v == nil {
			return errors.New("job not found")
		}
		var j job.Job
		if err := json.Unmarshal(v, &j); err != nil {
			return fmt.Errorf("unmarshal job: %w", err)
		}
		switch j.Status {
		case job.Completed:
			return errors.New("job already completed")
		case job.Running:
			return errors.New("job already running")
		case job.Cancelled:
			return errors.New("job already cancelled")
		}
		j.Status = job.Cancelled
		return put(tx, j)
	})
}

func (s *BoltStore) RunningJobs(workerID int) []job.Job {
	var jobs []job.Job
	_ = s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(jobsBucket).ForEach(func(k, v []byte) error {
			var j job.Job
			if err := json.Unmarshal(v, &j); err != nil {
				return err
			}
			if j.WorkerID == workerID && j.Status == job.Running {
				jobs = append(jobs, j)
			}
			return nil
		})
	})
	return jobs
}

// put JSON-encodes a job and writes it to the jobs bucket within tx.
func put(tx *bolt.Tx, j job.Job) error {
	v, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job %d: %w", j.ID, err)
	}
	return tx.Bucket(jobsBucket).Put(key(j.ID), v)
}

// key converts a job ID to the byte slice used as a bbolt key.
func key(id int) []byte {
	return []byte(strconv.Itoa(id))
}

var errNotFound = errors.New("not found")
