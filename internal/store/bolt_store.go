package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	bolt "go.etcd.io/bbolt"
	"quorum/internal/cron"
	"quorum/internal/job"
)

var (
	jobsBucket            = []byte("jobs")
	cronBucket            = []byte("cron_jobs")
	dlqBucket             = []byte("dlq")
	idempotencyBucket     = []byte("idempotency_keys") // key → job ID (string)
	statusPendingBucket   = []byte("status_pending")
	statusScheduledBucket = []byte("status_scheduled")
	statusCompletedBucket = []byte("status_completed")
	statusFailedBucket    = []byte("status_failed")
	statusCancelledBucket = []byte("status_cancelled")
)

// BoltStore is a persistent implementation of Store backed by bbolt (BoltDB).
//
// Buckets layout:
//   - "jobs"              -> key: JobID -> val: JSON job
//   - "cron_jobs"         -> key: CronID -> val: JSON cron job
//   - "dlq"               -> key: JobID -> val: JSON job
//   - "idempotency_keys"  -> key: IdempotencyKey -> val: JobID (ASCII int)
//   - "status_pending"    -> key: JobID -> val: nil
//   - "status_scheduled"  -> key: JobID -> val: nil
//   - "status_completed"  -> key: JobID -> val: nil
//   - "status_failed"     -> key: JobID -> val: nil
//   - "status_cancelled"  -> key: JobID -> val: nil
type BoltStore struct {
	db *bolt.DB
}

// NewBoltStore opens (or creates) a bbolt database file at path and initialises all buckets.
func NewBoltStore(path string) (*BoltStore, error) {
	db, err := bolt.Open(path, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("open bolt db at %q: %w", path, err)
	}

	buckets := [][]byte{
		jobsBucket,
		cronBucket,
		dlqBucket,
		idempotencyBucket,
		statusPendingBucket,
		statusScheduledBucket,
		statusCompletedBucket,
		statusFailedBucket,
		statusCancelledBucket,
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		for _, b := range buckets {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return fmt.Errorf("create bucket %s: %w", string(b), err)
			}
		}
		return nil
	}); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialise bolt buckets: %w", err)
	}

	return &BoltStore{db: db}, nil
}

func (s *BoltStore) Close() error {
	return s.db.Close()
}

func (s *BoltStore) Add(j job.Job) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		if err := putJob(tx, j); err != nil {
			return err
		}
		// Persist the idempotency key → job ID mapping atomically with the job.
		if j.IdempotencyKey != "" {
			if err := tx.Bucket(idempotencyBucket).Put(
				[]byte(j.IdempotencyKey),
				[]byte(strconv.Itoa(j.ID)),
			); err != nil {
				return fmt.Errorf("write idempotency key: %w", err)
			}
		}
		return addToStatusBucket(tx, j.Status, j.ID)
	})
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

func (s *BoltStore) ListByStatus(status job.Status) ([]job.Job, error) {
	var jobs []job.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		statusB := getStatusBucket(tx, status)
		if statusB == nil {
			return nil
		}

		jobsB := tx.Bucket(jobsBucket)
		return statusB.ForEach(func(k, v []byte) error {
			jobBytes := jobsB.Get(k)
			if jobBytes == nil {
				return nil
			}
			var j job.Job
			if err := json.Unmarshal(jobBytes, &j); err != nil {
				return err
			}
			jobs = append(jobs, j)
			return nil
		})
	})
	return jobs, err
}

func (s *BoltStore) Update(j job.Job) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		v := b.Get(key(j.ID))
		if v == nil {
			return fmt.Errorf("job %d not found for update", j.ID)
		}

		var existing job.Job
		if err := json.Unmarshal(v, &existing); err != nil {
			return fmt.Errorf("unmarshal job %d: %w", j.ID, err)
		}

		if !job.IsValidTransition(existing.Status, j.Status) {
			return fmt.Errorf("invalid status transition from %s to %s for job %d", existing.Status, j.Status, j.ID)
		}

		if err := removeFromStatusBucket(tx, existing.Status, j.ID); err != nil {
			return err
		}
		if err := putJob(tx, j); err != nil {
			return err
		}
		return addToStatusBucket(tx, j.Status, j.ID)
	})
}

func (s *BoltStore) Delete(id int) (bool, error) {
	found := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(jobsBucket)
		v := b.Get(key(id))
		if v == nil {
			return nil
		}
		found = true

		var j job.Job
		if err := json.Unmarshal(v, &j); err == nil {
			_ = removeFromStatusBucket(tx, j.Status, id)
			// Remove idempotency key mapping if present.
			if j.IdempotencyKey != "" {
				_ = tx.Bucket(idempotencyBucket).Delete([]byte(j.IdempotencyKey))
			}
		}
		return b.Delete(key(id))
	})
	return found, err
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

		if j.Status == job.Completed || j.Status == job.Failed || j.Status == job.Cancelled {
			return fmt.Errorf("job already in terminal status %s", j.Status)
		}

		if err := removeFromStatusBucket(tx, j.Status, id); err != nil {
			return err
		}

		j.Status = job.Cancelled
		if err := putJob(tx, j); err != nil {
			return err
		}
		return addToStatusBucket(tx, job.Cancelled, id)
	})
}

func (s *BoltStore) AddCron(c cron.CronJob) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		v, err := json.Marshal(c)
		if err != nil {
			return err
		}
		return tx.Bucket(cronBucket).Put([]byte(c.ID), v)
	})
}

func (s *BoltStore) DeleteCron(id string) (bool, error) {
	found := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(cronBucket)
		if b.Get([]byte(id)) == nil {
			return nil
		}
		found = true
		return b.Delete([]byte(id))
	})
	return found, err
}

func (s *BoltStore) ListCrons() ([]cron.CronJob, error) {
	var crons []cron.CronJob
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(cronBucket).ForEach(func(k, v []byte) error {
			var c cron.CronJob
			if err := json.Unmarshal(v, &c); err != nil {
				return err
			}
			crons = append(crons, c)
			return nil
		})
	})
	return crons, err
}

func (s *BoltStore) AddDLQ(j job.Job) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		v, err := json.Marshal(j)
		if err != nil {
			return err
		}
		return tx.Bucket(dlqBucket).Put(key(j.ID), v)
	})
}

func (s *BoltStore) ListDLQ() ([]job.Job, error) {
	var dlqJobs []job.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(dlqBucket).ForEach(func(k, v []byte) error {
			var j job.Job
			if err := json.Unmarshal(v, &j); err != nil {
				return err
			}
			dlqJobs = append(dlqJobs, j)
			return nil
		})
	})
	return dlqJobs, err
}

// FindByIdempotencyKey looks up an existing job by its idempotency key.
// The lookup is O(1): it consults the dedicated "idempotency_keys" bucket.
// Returns (zero, false) when the key is empty or not found.
func (s *BoltStore) FindByIdempotencyKey(ikey string) (job.Job, bool) {
	if ikey == "" {
		return job.Job{}, false
	}
	var j job.Job
	err := s.db.View(func(tx *bolt.Tx) error {
		idBytes := tx.Bucket(idempotencyBucket).Get([]byte(ikey))
		if idBytes == nil {
			return errNotFound
		}
		id, err := strconv.Atoi(string(idBytes))
		if err != nil {
			return errNotFound
		}
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

func putJob(tx *bolt.Tx, j job.Job) error {
	v, err := json.Marshal(j)
	if err != nil {
		return fmt.Errorf("marshal job %d: %w", j.ID, err)
	}
	return tx.Bucket(jobsBucket).Put(key(j.ID), v)
}

func getStatusBucket(tx *bolt.Tx, status job.Status) *bolt.Bucket {
	switch status {
	case job.Pending:
		return tx.Bucket(statusPendingBucket)
	case job.Scheduled:
		return tx.Bucket(statusScheduledBucket)
	case job.Completed:
		return tx.Bucket(statusCompletedBucket)
	case job.Failed:
		return tx.Bucket(statusFailedBucket)
	case job.Cancelled:
		return tx.Bucket(statusCancelledBucket)
	default:
		return nil
	}
}

func addToStatusBucket(tx *bolt.Tx, status job.Status, id int) error {
	b := getStatusBucket(tx, status)
	if b == nil {
		return nil
	}
	return b.Put(key(id), []byte{})
}

func removeFromStatusBucket(tx *bolt.Tx, status job.Status, id int) error {
	b := getStatusBucket(tx, status)
	if b == nil {
		return nil
	}
	return b.Delete(key(id))
}

func key(id int) []byte {
	return []byte(strconv.Itoa(id))
}

var errNotFound = errors.New("not found")
