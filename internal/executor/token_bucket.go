package executor

import (
	"time"
)

type TokenBucketLimiter struct {
	tokens chan struct{}
}

func NewTokenBucketLimiter(rate int, burst int) *TokenBucketLimiter {
	l := &TokenBucketLimiter{
		tokens: make(chan struct{}, burst),
	}

	for i := 0; i < burst; i++ {
		l.tokens <- struct{}{}
	}

	go func() {
		ticker := time.NewTicker(time.Second / time.Duration(rate))
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