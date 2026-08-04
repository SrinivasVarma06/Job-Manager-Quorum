# Chapter 2 — Goroutines & the Go Scheduler

> "Concurrency is not parallelism."
>
> — Rob Pike

---

# Learning Objectives

After reading this chapter, you should be able to answer:

- What is a process?
- What is a thread?
- What is a goroutine?
- Why are goroutines so lightweight?
- What happens when we write `go worker.Start()`?
- What is the Go Scheduler?
- What are G, M and P?
- Why did Quorum choose goroutines instead of OS threads?
- What are the limitations of goroutines?

---

# 1. Why Do We Need Concurrency?

Imagine Quorum receives 1,000 jobs.

Without concurrency:

```

Job 1
↓
Job 2
↓
Job 3
↓
...
↓
Job 1000

```

Suppose each job takes one second.

Total execution time:

```

1000 seconds

```

Clearly unacceptable.

Instead we create multiple workers.

```

Worker 1

Worker 2

Worker 3

Worker 4

```

Now jobs execute simultaneously.

This is concurrency.

---

# 2. Process vs Thread

Before understanding goroutines, we must understand how operating systems execute programs.

## Process

A process is an independent running program.

Example:

```

Chrome

VS Code

Discord

Quorum

```

Each process has:

- its own memory
- its own address space
- its own resources

Processes are isolated.

One process cannot directly read another process's memory.

---

## Thread

A thread is an execution path inside a process.

Example:

```

Quorum Process

│

├── Thread 1

├── Thread 2

└── Thread 3

```

Threads share memory.

They are much cheaper than processes.

---

# 3. Problems with OS Threads

Imagine creating one thread for every job.

```

10,000 Jobs

↓

10,000 Threads

```

Problems:

- Huge memory usage
- Expensive context switches
- Slow creation
- OS scheduler overhead

Operating systems were never designed for millions of threads.

---

# 4. Enter Goroutines

A goroutine is **not** an operating system thread.

A goroutine is a lightweight task managed by the Go runtime.

Example:

```go
go worker.Start(ctx)
```

The keyword

```go
go
```

does not ask Windows or Linux to create a new thread.

Instead:

```

Go Runtime

↓

Creates Goroutine

↓

Schedules it later

```

---

# 5. How Lightweight Are Goroutines?

Approximate memory usage:

| Type | Initial Stack |
|--------|--------------:|
| OS Thread | ~1 MB |
| Goroutine | ~2 KB |

Example:

```

1000 Threads

≈ 1 GB

```

versus

```

1000 Goroutines

≈ 2 MB

```

This is why Go programs comfortably run hundreds of thousands of goroutines.

---

# 6. What Happens When We Write `go`?

Example:

```go
go worker.Start(ctx)
```

Internally:

```

main()

↓

go worker.Start()

↓

Runtime creates Goroutine

↓

Places it in Run Queue

↓

Go Scheduler chooses CPU

↓

worker.Start() executes

```

Notice:

`go` does **not** mean

"execute immediately."

It means

"schedule this function."

---

# 7. The Go Scheduler

This is one of the greatest engineering achievements of Go.

Instead of relying entirely on the operating system scheduler,

Go has its own scheduler.

```

Application

↓

Go Runtime

↓

Go Scheduler

↓

Operating System

↓

CPU

```

The runtime decides which goroutine runs next.

---

# 8. G, M and P

Internally the scheduler works using three objects.

## G — Goroutine

Represents

```

Function

+

Stack

+

Execution State

```

Every

```go
go ...
```

creates one G.

---

## M — Machine

Represents

```

Operating System Thread

```

This is a real thread created by Windows or Linux.

---

## P — Processor

The most confusing part.

P is **not** a CPU.

P represents the ability to execute Go code.

Think of it as a scheduling token.

Without a P,

an M cannot execute goroutines.

---

The relationship:

```

Goroutine (G)

↓

Processor (P)

↓

OS Thread (M)

↓

CPU

```

---

# 9. Work Stealing

Suppose:

```

CPU 1

10 Goroutines

CPU 2

0 Goroutines

```

Without balancing:

CPU 2 sits idle.

Go instead performs

**work stealing.**

```

CPU 2

↓

Steals Goroutines

↓

Both CPUs stay busy

```

This dramatically improves throughput.

---

# 10. Stack Growth

Unlike OS threads,

goroutines do not allocate huge stacks.

Instead:

```

2 KB

↓

Need More Memory?

↓

Grow Automatically

↓

Need Less?

↓

Shrink

```

This allows millions of goroutines.

---

# 11. Advantages

✓ Extremely lightweight

✓ Fast creation

✓ Cheap scheduling

✓ Excellent scalability

✓ Managed by Go runtime

✓ Perfect for servers

---

# 12. Disadvantages

✗ Runtime overhead

✗ Debugging concurrent programs is harder

✗ Race conditions still possible

✗ Blocking system calls still consume OS threads

---

# 13. Alternatives

## POSIX Threads

Advantages

- Maximum control

Disadvantages

- Expensive

- Manual synchronization

---

## Java Threads

Heavier than goroutines.

Better today with Virtual Threads.

---

## Rust Async

Extremely efficient.

More difficult to learn.

Requires async ecosystem.

---

## Python Threads

Limited by the GIL.

Poor CPU concurrency.

---

# 14. Why Quorum Uses Goroutines

Quorum contains many independent long-running components.

```

Scheduler

Worker Pool

HTTP Server

gRPC Server

Cron Scheduler

Heartbeat Monitor

Snapshot Manager

Retry Queue

```

Each component becomes a goroutine.

Example:

```go
go worker.Start(ctx)

go scheduler.Start(ctx)

go grpc.Start()

go cron.Start()
```

This allows the entire system to remain responsive.

---

# 15. Code Walkthrough

Example from Quorum:

```go
go func() {
    defer e.wg.Done()
    e.Scheduler.Start(e.ctx)
}()
```

What happens?

1. Anonymous function is created.
2. Runtime creates a goroutine.
3. Scheduler decides when it runs.
4. Scheduler continues forever.
5. When it exits, `wg.Done()` signals completion.

---

# 16. Engineering Decision (ADR-002)

## Context

Quorum requires many independent background services.

## Alternatives

- OS Threads
- Thread Pools
- Goroutines

## Decision

Use goroutines.

## Rationale

- Extremely lightweight
- Built into Go
- Excellent scalability
- Simple programming model

## Consequences

Positive

- High concurrency
- Cleaner code
- Lower memory usage

Negative

- Requires understanding synchronization
- Race conditions remain possible

---

# 17. Interview Questions

### Beginner

What is a goroutine?

---

### Intermediate

How are goroutines different from operating system threads?

---

### Senior

Explain Go's G-M-P scheduler and why it scales better than creating one OS thread per task.

---

# 18. Key Takeaways

- Goroutines are lightweight execution units managed by the Go runtime.
- They are much cheaper than OS threads.
- The Go scheduler multiplexes many goroutines onto a smaller number of operating system threads.
- Quorum relies heavily on goroutines because every subsystem runs concurrently.