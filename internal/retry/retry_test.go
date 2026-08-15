package retry_test

import (
	"testing"
	"time"

	"quorum/internal/job"
	"quorum/internal/retry"
)

func TestShouldRetry(t *testing.T) {
	j := job.NewJob(1, "email", 1)

	j.RetryCount = 0
	j.MaxRetries = 3

	if !retry.ShouldRetry(j) {
		t.Fatal("expected retry to be allowed")
	}

	j.RetryCount = 3

	if retry.ShouldRetry(j) {
		t.Fatal("expected retry to be denied")
	}
}

func TestBackoffExponential(t *testing.T) {
	j := job.NewJob(1, "email", 1)

	j.RetryCount = 0

	delay := retry.Backoff(j, 30*time.Second)

	if delay != 1*time.Second {
		t.Fatalf("expected 1s, got %v", delay)
	}

	j.RetryCount = 1

	delay = retry.Backoff(j, 30*time.Second)

	if delay != 2*time.Second {
		t.Fatalf("expected 2s, got %v", delay)
	}

	j.RetryCount = 2

	delay = retry.Backoff(j, 30*time.Second)

	if delay != 4*time.Second {
		t.Fatalf("expected 4s, got %v", delay)
	}
}

func TestBackoffMaxCap(t *testing.T) {
	j := job.NewJob(1, "email", 1)

	j.RetryCount = 10

	delay := retry.Backoff(j, 30*time.Second)

	if delay != 30*time.Second {
		t.Fatalf("expected capped delay 30s, got %v", delay)
	}
}

func TestNextRetryTime(t *testing.T) {
	j := job.NewJob(1, "email", 1)

	j.RetryCount = 2

	before := time.Now()

	next := retry.NextRetryTime(j, 30*time.Second)

	expected := before.Add(4 * time.Second)

	diff := next.Sub(expected)

	if diff < 0 {
		diff = -diff
	}

	if diff > 100*time.Millisecond {
		t.Fatalf("expected retry time around %v, got %v", expected, next)
	}
}
