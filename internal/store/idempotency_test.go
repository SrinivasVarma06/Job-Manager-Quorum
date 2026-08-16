package store_test

import (
	"testing"

	"quorum/internal/job"
	"quorum/internal/store"
)

// ---------------------------------------------------------------------------
// MemoryStore idempotency tests
// ---------------------------------------------------------------------------

func TestMemoryStoreFindByIdempotencyKey_SameKeyReturnsSameJob(t *testing.T) {
	s := store.NewMemoryStore()

	j := job.NewJob(1, "email", 5)
	j.IdempotencyKey = "key-abc"
	if err := s.Add(j); err != nil {
		t.Fatalf("Add failed: %v", err)
	}

	got, ok := s.FindByIdempotencyKey("key-abc")
	if !ok {
		t.Fatal("expected FindByIdempotencyKey to return true for known key")
	}
	if got.ID != 1 || got.Type != "email" {
		t.Fatalf("unexpected job returned: %+v", got)
	}
}

func TestMemoryStoreFindByIdempotencyKey_DifferentKeyReturnsDifferentJob(t *testing.T) {
	s := store.NewMemoryStore()

	j1 := job.NewJob(1, "email", 5)
	j1.IdempotencyKey = "key-aaa"
	_ = s.Add(j1)

	j2 := job.NewJob(2, "sms", 3)
	j2.IdempotencyKey = "key-bbb"
	_ = s.Add(j2)

	got1, ok1 := s.FindByIdempotencyKey("key-aaa")
	if !ok1 || got1.ID != 1 {
		t.Fatalf("key-aaa: expected job 1, got ok=%v job=%+v", ok1, got1)
	}

	got2, ok2 := s.FindByIdempotencyKey("key-bbb")
	if !ok2 || got2.ID != 2 {
		t.Fatalf("key-bbb: expected job 2, got ok=%v job=%+v", ok2, got2)
	}
}

func TestMemoryStoreFindByIdempotencyKey_EmptyKeyReturnsNothing(t *testing.T) {
	s := store.NewMemoryStore()

	j := job.NewJob(1, "email", 5)
	// No idempotency key set — empty string
	_ = s.Add(j)

	_, ok := s.FindByIdempotencyKey("")
	if ok {
		t.Fatal("empty key must always return false")
	}
}

func TestMemoryStoreFindByIdempotencyKey_UnknownKeyReturnsNothing(t *testing.T) {
	s := store.NewMemoryStore()

	j := job.NewJob(1, "email", 5)
	j.IdempotencyKey = "known"
	_ = s.Add(j)

	_, ok := s.FindByIdempotencyKey("unknown")
	if ok {
		t.Fatal("unknown key must return false")
	}
}

func TestMemoryStoreJobWithoutKey_StillWorks(t *testing.T) {
	s := store.NewMemoryStore()

	j := job.NewJob(99, "report", 1)
	// IdempotencyKey intentionally empty — must not break anything
	if err := s.Add(j); err != nil {
		t.Fatalf("Add without key failed: %v", err)
	}
	got, ok := s.Get(99)
	if !ok || got.Type != "report" {
		t.Fatalf("expected to retrieve job 99, got ok=%v job=%+v", ok, got)
	}
}

// ---------------------------------------------------------------------------
// BoltStore idempotency tests
// ---------------------------------------------------------------------------

func TestBoltStoreFindByIdempotencyKey_SameKeyReturnsSameJob(t *testing.T) {
	bs, err := store.NewBoltStore(t.TempDir() + "/idem.db")
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	defer bs.Close()

	j := job.NewJob(1, "email", 5)
	j.IdempotencyKey = "bolt-key-1"
	if err := bs.Add(j); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, ok := bs.FindByIdempotencyKey("bolt-key-1")
	if !ok {
		t.Fatal("expected BoltStore FindByIdempotencyKey to return true")
	}
	if got.ID != 1 || got.Type != "email" {
		t.Fatalf("unexpected job: %+v", got)
	}
}

func TestBoltStoreFindByIdempotencyKey_EmptyKeyReturnsNothing(t *testing.T) {
	bs, err := store.NewBoltStore(t.TempDir() + "/idem_empty.db")
	if err != nil {
		t.Fatalf("NewBoltStore: %v", err)
	}
	defer bs.Close()

	_, ok := bs.FindByIdempotencyKey("")
	if ok {
		t.Fatal("empty key must always return false for BoltStore")
	}
}

func TestBoltStoreFindByIdempotencyKey_PersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := dir + "/persist.db"

	// Write
	{
		bs, _ := store.NewBoltStore(dbPath)
		j := job.NewJob(42, "payment", 10)
		j.IdempotencyKey = "pay-42"
		_ = bs.Add(j)
		_ = bs.Close()
	}

	// Read back after reopen
	{
		bs, err := store.NewBoltStore(dbPath)
		if err != nil {
			t.Fatalf("reopen: %v", err)
		}
		defer bs.Close()

		got, ok := bs.FindByIdempotencyKey("pay-42")
		if !ok {
			t.Fatal("expected idempotency key to survive DB reopen")
		}
		if got.ID != 42 || got.Type != "payment" {
			t.Fatalf("unexpected job after reopen: %+v", got)
		}
	}
}
