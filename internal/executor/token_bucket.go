package executor

import (
	"time"
)

type TokenBucketLimiter struct {
	tokens chan struct{}
}

func NewTokenBucketLimiter(rate int, burst int) *TokenBucketLimiter {
	if rate <= 0 {
		rate = 1
	}
	if burst <= 0 {
		burst = 1
	}

	l := &TokenBucketLimiter{
		tokens: make(chan struct{}, burst),
	}

	for i := 0; i < burst; i++ {
		l.tokens <- struct{}{}
	}

	go func() {
		interval := time.Second / time.Duration(rate)
		if interval <= 0 {
			interval = time.Millisecond
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		for range ticker.C {
			select {
			case l.tokens <- struct{}{}:
			default:
			}
		}
	}()

	return l
}

func (l *TokenBucketLimiter) Acquire() {
	<-l.tokens
}