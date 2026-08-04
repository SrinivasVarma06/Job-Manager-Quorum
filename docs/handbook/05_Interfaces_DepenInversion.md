# Chapter 5 — Interfaces & Dependency Inversion

> "Programs should depend upon abstractions, not concretions."
>
> — Robert C. Martin (Uncle Bob)

---

# Learning Objectives

After reading this chapter, you should be able to answer:

- What is an interface?
- Why do interfaces exist?
- How are Go interfaces different from Java or C++ interfaces?
- What is implicit implementation?
- What is Dependency Inversion?
- Why did Quorum introduce `WorkerClient`?
- Why can the scheduler now work with both local and remote workers?
- When should interfaces NOT be used?

---

# 1. The Problem

Let's go back to the very first version of Quorum.

The scheduler directly knew about workers.

```
Scheduler

↓

Worker
```

The scheduler literally depended on

```go
type Worker struct {
    ...
}
```

This seems fine.

Until requirements change.

---

Suppose tomorrow we add

```
Remote Worker

↓

gRPC
```

Now the scheduler still expects

```
Worker
```

How can a gRPC client become a Worker?

It cannot.

---

# 2. Tight Coupling

Imagine

```go
type Scheduler struct {

    Workers []*Worker

}
```

The scheduler knows

everything

about Worker.

Changing Worker

↓

changes Scheduler.

Adding RemoteWorker

↓

changes Scheduler.

Adding GPUWorker

↓

changes Scheduler.

The scheduler becomes impossible to extend.

This is called

**tight coupling.**

---

# 3. What is an Interface?

Instead of saying

```
I need a Worker.
```

We say

```
I need

something

that can

Submit Jobs.
```

Notice the difference.

We care about

**behavior**

not

**implementation.**

---

# 4. The WorkerClient Interface

Quorum defines

```go
type WorkerClient interface {

    ID() int

    Start(ctx context.Context)

    Submit(ctx context.Context, j job.Job) error

}
```

Read this carefully.

There is no mention of

```
Worker

gRPC

Network

Local

Remote
```

Only

behavior.

---

# 5. Implicit Interfaces

One of Go's greatest features.

In Java

```java
class Worker implements WorkerClient
```

You must explicitly declare it.

Go says

"No."

Instead

if a type has

```go
ID()

Start()

Submit()
```

it automatically satisfies

WorkerClient.

No keyword.

No declaration.

No inheritance.

---

# 6. Why Is This Powerful?

Our local worker

```go
type Worker struct
```

implements

```
WorkerClient
```

Our gRPC client

```go
type Client struct
```

also implements

```
WorkerClient
```

Yet

they have

completely different implementations.

---

Local

```
Submit()

↓

Channel

↓

Worker executes
```

Remote

```
Submit()

↓

gRPC

↓

Network

↓

Remote worker executes
```

The scheduler

doesn't know.

It doesn't care.

---

# 7. Dependency Inversion Principle

One of the SOLID principles.

Without interfaces

```
Scheduler

↓

Concrete Worker
```

With interfaces

```
Scheduler

↓

WorkerClient

↑

Worker

↑

RemoteClient

↑

FutureWorker
```

Notice

Scheduler now depends on

an abstraction,

not an implementation.

---

# 8. Why This Matters

Suppose tomorrow

we build

```
AWS Lambda Worker

Docker Worker

Kubernetes Worker

GPU Worker
```

Do we modify Scheduler?

No.

As long as they implement

```go
Submit()

Start()

ID()
```

Scheduler works.

Zero modifications.

This is called

**Open/Closed Principle.**

Open for extension.

Closed for modification.

---

# 9. Interfaces in Quorum

Before

```
Scheduler

↓

Worker
```

After

```
Scheduler

↓

WorkerClient

↓

Local Worker

↓

Remote Worker

↓

Future Worker
```

That one change

made distributed execution possible.

---

# 10. Dynamic Dispatch

Suppose

```go
var w WorkerClient
```

At runtime

w might actually contain

```
Local Worker
```

or

```
Remote Client
```

When Scheduler writes

```go
w.Submit(...)
```

Go decides

at runtime

which implementation

to call.

This is called

**dynamic dispatch.**

---

# 11. Advantages

✓ Loose coupling

✓ Easier testing

✓ Easier extension

✓ Cleaner architecture

✓ Enables Dependency Injection

✓ Better abstractions

---

# 12. Disadvantages

Interfaces are not free.

Problems

✗ Too many interfaces

✗ Premature abstraction

✗ Harder navigation

✗ Slight runtime overhead

---

# 13. When NOT to Use Interfaces

One of the biggest beginner mistakes.

Wrong

```go
type UserService interface

type UserRepository interface

type UserLogger interface

type UserDatabase interface
```

Everywhere.

Interfaces should exist

only

when

multiple implementations

are expected.

---

Quorum is a perfect example.

We genuinely have

```
Worker

RemoteWorker

FutureWorker
```

Multiple implementations.

Therefore

WorkerClient

is justified.

---

# 14. Testing

Interfaces make testing easy.

Instead of

real workers

we create

```
Mock Worker
```

Our scheduler tests

don't know

the difference.

Exactly why

scheduler_test.go

was simple.

---

# 15. Engineering Decision (ADR-005)

## Context

Quorum needs to support local and remote workers without changing the scheduler.

## Alternatives

- Concrete Worker type
- Inheritance
- Interfaces

## Decision

WorkerClient interface.

## Rationale

- Decouples scheduler from execution
- Enables gRPC workers
- Simplifies testing
- Supports future worker implementations

## Consequences

Positive

- Flexible architecture
- Easy extension
- Cleaner testing

Negative

- Slight increase in abstraction
- New developers must understand interfaces

---

# 16. Code Walkthrough

Earlier

```go
Scheduler

↓

Worker
```

Now

```go
Scheduler

↓

WorkerClient

↓

Submit()
```

Scheduler

never checks

```
Is this local?

Is this remote?
```

It simply says

```
Submit.
```

The implementation decides

how.

---

# 17. Real Industry Examples

Interfaces are everywhere.

Kubernetes

```
Storage Driver Interface

Container Runtime Interface (CRI)

Container Network Interface (CNI)
```

Docker

```
Logging Drivers

Storage Drivers

Network Drivers
```

Terraform

```
Provider Interface
```

Database libraries

```
sql.Driver
```

Go itself

```
io.Reader

io.Writer

http.Handler
```

One abstraction.

Many implementations.

---

# 18. Interview Questions

### Beginner

What is an interface?

---

### Intermediate

Why are Go interfaces implicitly implemented?

What advantages does this provide?

---

### Senior

Why did introducing WorkerClient enable Quorum to become distributed without changing the scheduler?

Would inheritance have solved the same problem?

Explain the tradeoffs.

---

# 19. Connection to Quorum

WorkerClient is one of the most important architectural decisions in the project.

Without it,

the scheduler would have required significant changes to support remote workers.

Because both

```
worker.Worker
```

and

```
rpc/client.Client
```

implement the same interface,

the scheduler dispatches work without knowing whether execution happens locally or over the network.

This is a textbook example of Dependency Inversion.

---

# Key Takeaways

- Interfaces describe behavior, not implementation.
- Go uses implicit interface implementation.
- WorkerClient decouples the scheduler from workers.
- This abstraction enabled Quorum to evolve from a local scheduler into a distributed scheduler.
- Interfaces should be introduced to support multiple implementations, not by default.