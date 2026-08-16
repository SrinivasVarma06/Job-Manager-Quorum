package benchmark

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"quorum/internal/job"
	"quorum/internal/queue"
	"quorum/internal/store"
)

func BenchmarkPriorityQueue_PushPop(b *testing.B) {
	ms := store.NewMemoryStore()
	for i := 0; i < 1000; i++ {
		j := job.NewJob(i, "email", i%100)
		_ = ms.Add(j)
	}

	jq := queue.NewJobQueue(ms, queue.PriorityComparator)
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		jq.Enqueue(i % 1000)
		_, _ = jq.Dequeue()
	}
}

func BenchmarkMemoryStore_AddGet(b *testing.B) {
	ms := store.NewMemoryStore()
	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		j := job.NewJob(i, "email", i%10)
		j.IdempotencyKey = fmt.Sprintf("key-%d", i)
		_ = ms.Add(j)
		_, _ = ms.Get(i)
	}
}

func BenchmarkBoltStore_AddGet(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "bolt_bench_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	dbPath := filepath.Join(tempDir, "bench.db")
	bs, err := store.NewBoltStore(dbPath)
	if err != nil {
		b.Fatal(err)
	}
	defer bs.Close()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		j := job.NewJob(i, "email", i%10)
		j.IdempotencyKey = fmt.Sprintf("key-%d", i)
		_ = bs.Add(j)
		_, _ = bs.Get(i)
	}
}

func BenchmarkIdempotencyKey_Find(b *testing.B) {
	ms := store.NewMemoryStore()
	for i := 0; i < 10000; i++ {
		j := job.NewJob(i, "email", 1)
		j.IdempotencyKey = fmt.Sprintf("idempotent-key-%d", i)
		_ = ms.Add(j)
	}

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("idempotent-key-%d", i%10000)
		_, _ = ms.FindByIdempotencyKey(key)
	}
}

func BenchmarkRealisticWorkload_Execute(b *testing.B) {
	exec := NewRealisticWorkloadExecutor(2048, 15)
	ctx := context.Background()
	j := job.NewJob(1, "compute", 1)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = exec.Execute(ctx, j)
	}
}
