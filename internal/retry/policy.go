package retry

import (
	"time"
	"quorum/internal/job"
)

func ShouldRetry(j job.Job) bool {
	return j.RetryCount < j.MaxRetries
}

func Backoff(j job.Job,maxBackoff time.Duration) time.Duration {
	delay := time.Second * time.Duration(1<<j.RetryCount)
	if delay > maxBackoff {
		delay = maxBackoff
	}
	return delay
}

func NextRetryTime(j job.Job,maxBackoff time.Duration) time.Time {
	return time.Now().Add(Backoff(j,maxBackoff))	
}
