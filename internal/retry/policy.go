package retry

import (
	"time"

	"quorum/internal/job"
)

func ShouldRetry(j job.Job) bool {
	return j.RetryCount < j.MaxRetries
}

func Backoff(j job.Job) time.Duration {
	return time.Second * time.Duration(1<<j.RetryCount)
}

func NextRetryTime(j job.Job) time.Time {
	return time.Now().Add(Backoff(j))
}