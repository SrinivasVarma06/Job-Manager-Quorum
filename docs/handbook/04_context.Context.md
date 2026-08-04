# Chapter 4 — context.Context

> "Contexts carry deadlines, cancellation signals, and request-scoped values across API boundaries."

---

# Learning Objectives

After reading this chapter, you should be able to answer:

- What is context?
- Why was it introduced?
- What problem does it solve?
- Why does almost every Go library accept a Context?
- What happens when we call cancel()?
- What is ctx.Done()?
- How does cancellation propagate?
- Why does Quorum pass Context everywhere?

---

# 1. The Problem

Imagine Quorum receives a job.

```
HTTP Request

↓

Scheduler

↓

Worker

↓

Database

↓

gRPC

↓

Storage
```

Now imagine the client disconnects.

Should everything continue running?

Without cancellation:

```
Client Gone

↓

Scheduler keeps working

↓

Worker keeps executing

↓

Database still writing

↓

CPU wasted
```

This wastes:

- CPU
- Memory
- Network
- Storage

There is no way to tell every component

> "Stop."

---

# 2. Before Context

Older Go programs often looked like this:

```go
func ProcessJob(job Job)
```

No timeout.

No cancellation.

No deadline.

If something blocked forever...

The function blocked forever.

---

# 3. Enter context.Context

Go introduced Context to solve one problem.

> **Coordinate long-running operations.**

Instead of

```go
ProcessJob(job)
```

we write

```go
ProcessJob(ctx, job)
```

Now every function receives information about:

- Cancellation
- Deadlines
- Timeouts
- Request lifetime

---

# 4. What is Context?

Context is an interface.

```go
type Context interface {

    Deadline()

    Done()

    Err()

    Value()

}
```

Notice.

Context contains almost no logic.

Instead,

it acts like a shared control object.

---

# 5. Creating Contexts

The simplest context.

```go
ctx := context.Background()
```

Think of it as

```
Root Context
```

Everything grows from it.

---

# 6. Derived Contexts

Most contexts come from another context.

Example

```go
ctx, cancel := context.WithCancel(parent)
```

Now

```
Parent

↓

Child Context

↓

Cancel Function
```

---

# 7. The Cancel Function

This surprises many beginners.

```go
ctx, cancel := context.WithCancel(...)
```

Why return two values?

Because

```
ctx

↓

Read cancellation

cancel

↓

Trigger cancellation
```

One listens.

One sends.

---

# 8. Cancellation Tree

Imagine

```
Root

↓

Scheduler

↓

Worker

↓

Storage

↓

Database
```

If Scheduler cancels

```
Scheduler

↓

Worker cancelled

↓

Storage cancelled

↓

Database cancelled
```

Everything stops automatically.

This is called

**cancellation propagation.**

---

# 9. ctx.Done()

The most common pattern.

```go
select {

case <-ctx.Done():

    return

}
```

What is Done()?

It returns a channel.

Initially

```
Open
```

After cancellation

```
Closed
```

Every goroutine waiting on it immediately wakes up.

---

# 10. Why a Channel?

Remember CSP.

Go already has a perfect synchronization primitive.

Instead of inventing

```
CancelSignal
```

Go simply reused

```
Channel
```

So

```
ctx.Done()

↓

<-ctx.Done()

↓

Wake Up

↓

Exit
```

Simple.

Elegant.

---

# 11. Deadlines

Sometimes

we don't want manual cancellation.

We want

```
Timeout

↓

Automatically cancel
```

Example

```go
ctx, cancel := context.WithTimeout(
    parent,
    5*time.Second,
)
```

After

5 seconds

↓

cancel automatically.

---

# 12. Deadlines

Similar

```go
context.WithDeadline(...)
```

Instead of

```
5 seconds
```

we specify

```
Stop exactly at

4:30 PM
```

---

# 13. Values

Context can also carry

request-scoped values.

Example

```
Request ID

User ID

Trace ID
```

These travel through the entire request.

Not business data.

Metadata.

---

# 14. Context in Quorum

Look at our workers.

```go
func Start(ctx context.Context)
```

Why?

Because workers run forever.

Without Context

they never stop.

With Context

```
Engine.Stop()

↓

cancel()

↓

ctx.Done()

↓

Worker exits
```

Gracefully.

---

Scheduler

```go
Scheduler.Start(ctx)
```

Cron

```go
Cron.Start(ctx)
```

gRPC

```go
Server.Start(ctx)
```

All of them obey

the same shutdown signal.

---

# 15. Example From Quorum

```go
select {

case <-ctx.Done():

    return

case job := <-JobChannel:

    Execute(job)

}
```

Meaning

Either

```
Shutdown
```

or

```
Execute Job
```

Whichever happens first.

---

# 16. Internal Working

Internally

Context is a tree.

```
Background

↓

Engine

↓

Scheduler

↓

Worker

↓

Database
```

Calling

```
cancel()
```

closes one channel.

Every child context notices.

No polling.

No loops.

No timers.

---

# 17. Advantages

✓ Graceful shutdown

✓ Timeout support

✓ Deadline propagation

✓ Cancellation propagation

✓ Standard across Go ecosystem

✓ Prevents resource leaks

---

# 18. Disadvantages

✗ Easy to misuse

✗ Should not carry business data

✗ Deep context chains become harder to follow

---

# 19. Alternatives

## Global Variables

Terrible.

No isolation.

---

## Boolean Flags

Every function must check them.

Error-prone.

---

## Signals

Too coarse.

Cannot cancel one request.

---

## Custom Cancellation Objects

Possible.

But Go already provides Context.

---

# 20. Why Quorum Uses Context

Quorum contains many long-running components.

```
Workers

Scheduler

Cron

gRPC

HTTP

Heartbeat

Snapshot
```

All should stop together.

Instead of

```
Stop Worker

Stop Scheduler

Stop HTTP

Stop gRPC
```

we simply call

```go
cancel()
```

Everything receives the signal.

This makes shutdown deterministic and clean.

---

# 21. Engineering Decision (ADR-004)

## Context

Quorum contains multiple concurrent services requiring coordinated shutdown.

## Alternatives

- Boolean flags
- Global variables
- OS signals
- context.Context

## Decision

Use context.Context.

## Why?

- Standard Go practice
- Automatic cancellation propagation
- Works seamlessly with HTTP and gRPC
- Simple API

## Consequences

Positive

- Clean shutdown
- Easy timeout support
- Consistent APIs

Negative

- Requires every function to accept Context
- Beginners often misuse Value()

---

# 22. Interview Questions

### Beginner

What is Context?

---

### Intermediate

Difference between

WithCancel

WithTimeout

WithDeadline?

---

### Senior

Why does ctx.Done() return a channel instead of a boolean?

How does cancellation propagate through a Context tree?

---

# 23. Connection to Quorum

Every major subsystem you've built already depends on Context.

Examples:

```go
ctx, cancel := context.WithCancel(context.Background())
```

Creates the root context for the engine.

```go
go worker.Start(ctx)
```

Each worker listens for shutdown.

```go
go scheduler.Start(ctx)
```

The scheduler exits cleanly when the engine stops.

```go
go cron.Start(ctx)
```

The cron scheduler uses the same cancellation signal.

```go
<-ctx.Done()
```

Appears throughout the project as the standard shutdown mechanism.

Without Context, Quorum would need separate stop signals for every subsystem.

---

# Key Takeaways

- Context is Go's standard mechanism for cancellation, deadlines, and request-scoped metadata.
- It forms a tree where cancellation propagates from parent to child.
- `Done()` is a channel, allowing efficient synchronization with `select`.
- Quorum uses one root Context to control the lifetime of all long-running components.
- Context is not for passing business data; it is for controlling the lifetime of work.