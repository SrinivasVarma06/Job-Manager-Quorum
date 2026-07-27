package config

import "time"

type Config struct {
	WorkerCount int
	MaxRetries int
	MaxBackoff time.Duration
	ResultBuffer int
	WorkerExecutionTimeout time.Duration
	DelayQueuePollInterval time.Duration
	RateLimit int
	RateBurst int
	BreakerFailureThreshold int
	BreakerResetTimeout     time.Duration
}

func Default() Config {
	return Config{
		WorkerCount:             2,
		MaxRetries:              3,
		MaxBackoff:              time.Minute,
		ResultBuffer:            100,
		WorkerExecutionTimeout:  30 * time.Second,
		DelayQueuePollInterval:  500 * time.Millisecond,
		RateLimit:               5,
		RateBurst:               10,
		BreakerFailureThreshold: 5,
		BreakerResetTimeout:     30 * time.Second,
	}
}