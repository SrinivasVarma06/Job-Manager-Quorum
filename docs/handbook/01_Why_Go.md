# Chapter 1 — Why Go?

> "Programming languages are tools. The best engineers choose the tool that best fits the problem."

---

# Learning Objectives

After reading this chapter, you should be able to answer:

- Why was Go created?
- What problems was Google trying to solve?
- Why is Go one of the most popular languages for cloud-native software?
- Why did Quorum choose Go instead of C++, Rust, Java or Python?
- What tradeoffs come with choosing Go?

---

# 1. Introduction

Quorum is not a web application.

It is a distributed job orchestration platform.

Its responsibilities include:

- Executing thousands of concurrent jobs
- Scheduling background work
- Managing worker pools
- Networking
- Storage
- Fault tolerance
- Distributed communication

These are exactly the kinds of systems Go was designed to build.

Understanding **why** we chose Go is the first architectural decision of the project.

---

# 2. The Story Behind Go

Around 2007, Google faced a serious engineering problem.

Its infrastructure had grown to millions of lines of code spread across thousands of engineers.

Most backend services were written using:

- C++
- Java
- Python

Each language solved some problems but introduced others.

### C++

Advantages

- Extremely fast
- Full hardware control

Problems

- Long compile times
- Complex syntax
- Memory management bugs
- Difficult concurrency

---

### Java

Advantages

- Automatic garbage collection
- Mature ecosystem

Problems

- Heavy runtime
- Verbose code
- Expensive threads
- High memory usage

---

### Python

Advantages

- Rapid development
- Easy syntax

Problems

- Slow execution
- Global Interpreter Lock (GIL)
- Limited CPU parallelism

---

Google wanted something different.

Their goals were simple.

- Compile as quickly as Python scripts feel.
- Run close to C++ speeds.
- Handle massive concurrency.
- Be simple enough that thousands of engineers could maintain it.

The result became **Go**.

The language was created by:

- Robert Griesemer
- Rob Pike
- Ken Thompson (creator of Unix)

---

# 3. Go's Philosophy

Go intentionally avoids many language features.

It has:

- No inheritance
- No exceptions
- No operator overloading
- Minimal syntax

This surprises many new developers.

The goal is not to make the language powerful.

The goal is to make **large software systems easier to understand**.

Go follows one core philosophy:

> Clear code is better than clever code.

---

# 4. Why Quorum Uses Go

Quorum contains many independent components.

```
                +--------------------+
                |   HTTP Server      |
                +--------------------+

                +--------------------+
                |   gRPC Server      |
                +--------------------+

                +--------------------+
                | Scheduler          |
                +--------------------+

                +--------------------+
                | Worker Pool        |
                +--------------------+

                +--------------------+
                | Cron Scheduler     |
                +--------------------+

                +--------------------+
                | Snapshot Manager   |
                +--------------------+
```

All of these execute concurrently.

Go makes this natural using goroutines and channels.

---

# 5. Comparison with Alternatives

| Feature | Go | C++ | Rust | Java | Python |
|----------|----|------|-------|--------|---------|
| Learning Curve | Easy | Hard | Very Hard | Medium | Easy |
| Compile Speed | Excellent | Poor | Moderate | Moderate | N/A |
| Networking | Excellent | Good | Excellent | Excellent | Good |
| Concurrency | Excellent | Good | Excellent | Good | Limited |
| Memory Safety | Good | Poor | Excellent | Good | Good |
| Runtime Performance | Very Good | Excellent | Excellent | Good | Poor |

---

# 6. Why Not C++?

Many students assume that faster always means better.

That is rarely true in software engineering.

Suppose Quorum eventually supports:

- 10,000 workers
- Millions of jobs
- Hundreds of API requests per second

C++ could absolutely build this.

However:

- development would be slower
- concurrency code would be more complex
- networking would require more boilerplate
- debugging would be harder

Go sacrifices a small amount of raw performance for dramatically improved engineering productivity.

For infrastructure software, this is often the correct tradeoff.

---

# 7. Industry Adoption

Go powers many of the world's largest infrastructure projects.

Examples include:

- Kubernetes
- Docker
- Terraform
- Prometheus
- Grafana
- CockroachDB
- etcd
- Cloudflare
- Temporal
- HashiCorp Vault

Quorum belongs to the same family of software.

---

# 8. Advantages

✓ Excellent concurrency model

✓ Fast compilation

✓ Small deployment binaries

✓ Excellent networking libraries

✓ Strong standard library

✓ Large cloud-native ecosystem

---

# 9. Disadvantages

✗ Garbage Collector introduces some runtime overhead

✗ Less low-level control than C++

✗ Smaller ecosystem than Java

✗ Simplicity sometimes limits advanced language abstractions

---

# 10. Engineering Decision (ADR-001)

## Context

Quorum is a distributed systems project requiring concurrency, networking, reliability, and maintainability.

## Alternatives Considered

- C++
- Rust
- Java
- Python

## Decision

Use Go.

## Rationale

- Lightweight concurrency with goroutines
- Excellent networking support
- Strong gRPC ecosystem
- Fast development cycle
- Easy deployment as a single binary

## Consequences

Positive:

- Faster development
- Simpler concurrency
- Easier maintenance

Negative:

- Slightly lower peak performance than optimized C++ or Rust
- Dependence on garbage collection

---

# 11. Interview Questions

### Beginner

Why did you choose Go?

---

### Intermediate

When would Rust be a better choice?

---

### Senior

If Quorum needed to process tens of millions of jobs per second, would you still choose Go? Why or why not?

---

# 12. Connection to Quorum

Everything implemented so far depends on Go's strengths.

- Worker pool
- Scheduler
- WAL
- HTTP API
- gRPC server
- Cron scheduler
- Retry engine
- Circuit breaker

Without Go's concurrency primitives, Quorum would be significantly more complex.

---

# Key Takeaways

- Go was designed for large-scale backend infrastructure.
- Simplicity is a deliberate design decision.
- Go optimizes engineering productivity rather than maximum raw performance.
- Quorum uses Go because distributed schedulers are fundamentally concurrent systems.