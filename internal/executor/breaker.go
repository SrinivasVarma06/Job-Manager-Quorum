package executor

import (
	"context"
	"errors"
	"sync"
	"time"

	"quorum/internal/job"
)

var ErrCircuitOpen = errors.New("circuit breaker is open")

type BreakerState int

const (
	Closed BreakerState = iota
	Open
	HalfOpen
)

type CircuitBreakerExecutor struct {
	next             Executor
	mu               sync.Mutex
	state            BreakerState
	failures         int
	failureThreshold int
	resetTimeout     time.Duration
	openedAt         time.Time
	halfOpenTesting  bool // guards single trial request in HalfOpen state
}

func NewCircuitBreakerExecutor(next Executor, threshold int, resetTimeout time.Duration) *CircuitBreakerExecutor {
	return &CircuitBreakerExecutor{
		next:             next,
		state:            Closed,
		failureThreshold: threshold,
		resetTimeout:     resetTimeout,
	}
}

func (c *CircuitBreakerExecutor) Execute(ctx context.Context, j job.Job) error {
	c.mu.Lock()

	switch c.state {
	case Open:
		if time.Since(c.openedAt) < c.resetTimeout {
			c.mu.Unlock()
			return ErrCircuitOpen
		}
		c.state = HalfOpen
		c.halfOpenTesting = true

	case HalfOpen:
		if c.halfOpenTesting {
			// Single trial request already in progress: reject additional callers
			c.mu.Unlock()
			return ErrCircuitOpen
		}
		c.halfOpenTesting = true
	}

	c.mu.Unlock()

	err := c.next.Execute(ctx, j)

	c.mu.Lock()
	defer c.mu.Unlock()

	c.halfOpenTesting = false

	if err == nil {
		c.failures = 0
		c.state = Closed
		return nil
	}

	c.failures++
	if c.failures >= c.failureThreshold {
		c.state = Open
		c.openedAt = time.Now()
	}
	return err
}