package config

import "time"

type Config struct {
	WorkerCount             int
	WorkerID                int
	HeartbeatTimeout        time.Duration
	MaxRetries              int
	MaxBackoff              time.Duration
	ResultBuffer            int
	WorkerExecutionTimeout  time.Duration
	DelayQueuePollInterval  time.Duration
	RateLimit               int
	RateBurst               int
	BreakerFailureThreshold int
	BreakerResetTimeout     time.Duration
	ControllerGRPCPort      int
	WorkerGRPCPort          int
	StorageType             string
	StoragePath             string
	RaftEnabled             bool
	RaftNodeID              string
	RaftAddr                string
	RaftDataDir             string
}

func Default() Config {
	return Config{
		WorkerCount:             0, // 0 = distributed-only; set >0 for local in-process workers
		WorkerID:                1,
		HeartbeatTimeout:        5 * time.Second,
		MaxRetries:              3,
		MaxBackoff:              time.Minute,
		ResultBuffer:            100,
		WorkerExecutionTimeout:  30 * time.Second,
		DelayQueuePollInterval:  500 * time.Millisecond,
		RateLimit:               5,
		RateBurst:               10,
		BreakerFailureThreshold: 5,
		BreakerResetTimeout:     30 * time.Second,
		ControllerGRPCPort:      50051,
		WorkerGRPCPort:          50052,
		StorageType:             "bolt",
		StoragePath:             "quorum.db",
		RaftEnabled:             true,
		RaftNodeID:              "node1",
		RaftAddr:                "127.0.0.1:18088",
		RaftDataDir:             "data/raft",
	}
}
