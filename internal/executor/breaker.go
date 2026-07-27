package executor

import (
	"errors"
	"quorum/internal/job"
	"sync"
	"time"
)
var ErrCircuitOpen = errors.New("circuit breaker is open")
type BreakerState int

const (
	Closed BreakerState = iota
	Open
	HalfOpen
)

type CircuitBreakerExecutor struct {
	next Executor
	mu sync.Mutex
	state BreakerState
	failures int
	failureThreshold int
	resetTimeout     time.Duration
	openedAt time.Time
}

func NewCircuitBreakerExecutor(next Executor,threshold int,resetTimeout time.Duration) *CircuitBreakerExecutor {
	return &CircuitBreakerExecutor{
		next:             next,
		state:            Closed,
		failureThreshold: threshold,
		resetTimeout:     resetTimeout,
	}
}

func (c *CircuitBreakerExecutor) Execute(j job.Job) error {
	c.mu.Lock()
	if c.state == Open {
		if time.Since(c.openedAt) < c.resetTimeout {
			c.mu.Unlock()
			return ErrCircuitOpen
		}
		c.state = HalfOpen
	}

	c.mu.Unlock()
	err := c.next.Execute(j)
	c.mu.Lock()
	defer c.mu.Unlock()

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