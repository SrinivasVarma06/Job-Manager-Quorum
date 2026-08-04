# Chapter 3 — Channels & Communicating Sequential Processes (CSP)

> "Do not communicate by sharing memory; instead, share memory by communicating."
>
> — Andrew Gerrand (Go Team)

---

# Learning Objectives

After reading this chapter, you should be able to answer:

- What is a channel?
- Why were channels invented?
- What is CSP?
- Why did Go choose channels instead of locks?
- What is a buffered channel?
- What is an unbuffered channel?
- What happens internally during a channel send?
- How does `select` work?
- When should channels NOT be used?
- Why does Quorum use both channels and mutexes?

---

# 1. The Problem

Imagine two workers.

```
Worker A

Worker B
```

Worker A produces jobs.

Worker B executes them.

How should they communicate?

One idea is

```
Shared Queue

Worker A writes

Worker B reads
```

Seems simple.

Until both access it simultaneously.

```
Worker A writes

↓

Queue

↑

Worker B reads
```

Now we have a race condition.

---

# 2. Traditional Solution

Before Go,

programmers used

```
Mutex

Semaphore

Condition Variable
```

Example

```
lock()

enqueue()

unlock()
```

This works.

But...

Imagine 100 workers.

```
Worker 1

Worker 2

...

Worker 100

↓

One Queue

↓

One Mutex
```

Now everyone waits for the lock.

---

# 3. Problems with Locks

Locks are powerful.

But they introduce several problems.

## Deadlocks

```
Worker A

holds Lock 1

waits Lock 2

↓

Worker B

holds Lock 2

waits Lock 1
```

Nobody moves.

---

## Race Conditions

Forgot to lock?

Data corruption.

---

## Lock Contention

Too many threads

↓

Waiting

↓

Poor performance

---

# 4. CSP — Communicating Sequential Processes

Before Go existed,

computer scientist

**Tony Hoare**

published a paper in 1978.

The idea:

Instead of

```
Many workers

↓

Shared Memory
```

Use

```
Many workers

↓

Message Passing
```

Workers should exchange messages,

not memory.

Go adopted this philosophy.

---

# 5. What is a Channel?

A channel is a typed communication pipe.

Example

```go
jobs := make(chan Job)
```

Think of it as

```
Worker A

↓

Channel

↓

Worker B
```

Worker A

does not care

who receives it.

Worker B

does not care

who sent it.

Only the message matters.

---

# 6. Creating Channels

Example

```go
jobs := make(chan Job)
```

Creates an **unbuffered** channel.

---

Buffered

```go
jobs := make(chan Job, 100)
```

Capacity = 100.

---

# 7. Sending

```go
jobs <- job
```

Meaning

```
Put this job

into

the channel
```

---

# 8. Receiving

```go
job := <-jobs
```

Meaning

```
Wait

until

someone sends me a job.
```

---

# 9. Unbuffered Channels

Suppose

```
jobs := make(chan Job)
```

Capacity

```
0
```

Now

```
Worker A

↓

jobs <- job
```

Worker A

cannot continue

until

another goroutine executes

```
<-jobs
```

Both synchronize.

Think of it as a handshake.

```
Sender

🤝

Receiver
```

---

# 10. Buffered Channels

Now

```go
jobs := make(chan Job, 3)
```

Capacity

```
3 Jobs
```

Now

```
Worker A

↓

Send

↓

Buffer

↓

Continue
```

until

the buffer fills.

---

Example

```
Buffer

[ ]

↓

Send

[A]

↓

Send

[A][B]

↓

Send

[A][B][C]

↓

Full

↓

Next sender blocks.
```

---

# 11. Why Quorum Uses Buffered Channels

Example

```go
Available := make(chan WorkerClient, 100)
```

Suppose

20 workers become idle

simultaneously.

Without buffering

every worker

must wait

for the scheduler.

With buffering

all workers simply

place themselves

inside

Available.

Scheduler

reads later.

This greatly improves throughput.

---

# 12. Channel Direction

Go even supports

```
Send Only

Receive Only
```

Example

```go
func producer(out chan<- Job)
```

Cannot receive.

Only send.

Receiver

```go
func consumer(in <-chan Job)
```

Cannot send.

Only receive.

This improves correctness.

---

# 13. Closing Channels

Producer

finished.

```
close(jobs)
```

Receivers

can detect

```
job, ok := <-jobs
```

If

```
ok == false
```

channel closed.

---

# 14. Select

Imagine

two channels.

```
Jobs

Results
```

Without

select

you wait

on only one.

With

```go
select {

case job := <-jobs:

case result := <-results:

}
```

Go chooses

whichever becomes ready first.

This is called

**multiplexing.**

---

# 15. How Quorum Uses select

Example

```go
select {

case <-ctx.Done():

case job := <-worker.JobChannel:

}
```

Meaning

```
Either

shutdown

OR

execute job

whichever happens first.
```

No polling.

No busy waiting.

---

# 16. Internal Implementation

Internally,

a channel contains

```
Buffer

Send Queue

Receive Queue

Mutex
```

Surprised?

Channels themselves

use mutexes internally.

Go simply hides

that complexity.

---

# 17. Advantages

✓ Easier reasoning

✓ Message passing

✓ Fewer race conditions

✓ Cleaner APIs

✓ Excellent for pipelines

✓ Excellent for worker pools

---

# 18. Disadvantages

✗ Not good for every problem

✗ Can deadlock

✗ Blocking operations

✗ Harder to debug than normal function calls

---

# 19. Why Quorum Still Uses Mutexes

A very common misunderstanding:

"If channels exist,

never use mutexes."

Wrong.

Look at

```
WorkerManager

JobStore
```

These store shared state.

Example

```
map[int]WorkerClient

map[int]Job
```

These are **shared data structures**.

Nobody is sending messages.

Many goroutines simply need to read or update the same map.

Using a channel for every map lookup would complicate the code and reduce performance.

A mutex is the right tool here.

---

# 20. Engineering Decision (ADR-003)

## Context

Quorum coordinates workers, schedulers, retries, HTTP handlers, and background services.

## Alternatives

- Mutex-only synchronization
- Condition variables
- Message queues
- Channels

## Decision

Use channels for communication.

Use mutexes for protecting shared state.

## Why?

Communication and synchronization are different problems.

Channels model communication.

Mutexes protect data.

Combining both gives the simplest architecture.

---

# 21. Where Channels Appear in Quorum

Worker availability

```go
Available <- worker
```

Worker execution

```go
job := <-JobChannel
```

Scheduler dispatch

```go
worker := <-Available
```

Shutdown

```go
<-ctx.Done()
```

Result collection

```go
result := <-Results
```

Almost every subsystem communicates using channels.

---

# 22. Interview Questions

### Beginner

What is a channel?

---

### Intermediate

Buffered vs unbuffered channels?

When would you choose each?

---

### Senior

Why does Go recommend

"Do not communicate by sharing memory"

instead of

"Protect all shared memory with mutexes"?

Can you think of situations where a mutex is actually the better choice?

---

# 23. Key Takeaways

- Channels are Go's implementation of CSP-style message passing.
- They synchronize goroutines through communication rather than shared memory.
- Buffered channels decouple producers and consumers.
- `select` enables efficient waiting on multiple communication events.
- Quorum uses channels for coordination and mutexes for protecting shared state.
- Understanding when to use each is more important than preferring one over the other.