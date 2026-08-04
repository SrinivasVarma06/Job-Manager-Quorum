# Chapter 6 — gRPC & Protocol Buffers (Part 1)
## The Evolution of Distributed Communication

> "Calling a function on another machine should feel almost as easy as calling a function in your own program."

---

# Learning Objectives

After reading this chapter, you should be able to answer:

- What is RPC?
- Why was RPC invented?
- Why wasn't REST enough?
- What is gRPC?
- Why did Google create it?
- Why does Quorum use it?
- How does a gRPC request travel across the network?
- What are Protocol Buffers?

---

# 1. The Problem

Suppose Quorum grows beyond one machine.

```
Machine A

Scheduler

↓

???

↓

Worker

Machine B
```

The scheduler wants to execute a job on another computer.

How?

---

Option 1

Copy memory?

Impossible.

Different processes.

Different machines.

---

Option 2

Call the worker function directly?

Impossible.

The function exists on another computer.

---

Option 3

Send a network request.

Exactly.

Now the question becomes

**How should computers communicate?**

---

# 2. Early Distributed Systems

In the beginning,

developers used

```
Raw TCP Sockets
```

Example

```
Scheduler

↓

Socket

↓

Worker
```

The programmer manually wrote

- serialization
- parsing
- networking
- retries
- framing

Everything.

---

Problems

- Error-prone
- Hard to debug
- Every company invented its own protocol

---

# 3. Remote Procedure Call (RPC)

Engineers wanted something better.

Instead of

```
Send bytes

↓

Receive bytes
```

they wanted

```
Worker.SubmitJob(...)
```

even if the worker lived on another machine.

This idea became

**Remote Procedure Call (RPC).**

---

Imagine

Instead of writing

```
Socket

↓

Encode

↓

Send

↓

Decode

↓

Call function
```

you simply write

```go
worker.SubmitJob(job)
```

The networking happens automatically.

This is the entire idea behind RPC.

---

# 4. RPC Timeline

Distributed communication evolved over decades.

```
1980s

↓

Sun RPC

↓

CORBA

↓

XML-RPC

↓

SOAP

↓

Apache Thrift

↓

gRPC
```

Every generation solved problems of the previous one.

---

# 5. REST Changed Everything

Around 2000,

REST became extremely popular.

Example

```
POST /jobs

GET /jobs/15

DELETE /jobs/15
```

REST uses

```
HTTP

+

JSON
```

Advantages

✓ Human readable

✓ Browser friendly

✓ Easy to debug

✓ Huge ecosystem

---

But Google discovered problems.

Their services were making

millions

of requests

per second.

JSON became expensive.

HTTP/1.1 became limiting.

Manual API documentation caused bugs.

---

# 6. Why Google Created gRPC

Google already had an internal RPC framework.

It worked well.

In 2015,

they open-sourced a modern version.

It became

```
gRPC

Google Remote Procedure Call
```

Notice

gRPC

does **not** mean

```
general RPC
```

It literally started as

**Google RPC**.

Today it is maintained by the Cloud Native Computing Foundation (CNCF).

---

# 7. Design Goals

Google wanted

✓ Very fast

✓ Strongly typed

✓ Language independent

✓ Streaming support

✓ HTTP/2

✓ Code generation

✓ Backward compatible

---

# 8. How gRPC Works

Suppose Scheduler wants to execute a job.

```
Scheduler

↓

SubmitJob()

↓

Protocol Buffers

↓

HTTP/2

↓

TCP

↓

Network

↓

Worker

↓

SubmitJob()

```

Notice something interesting.

The programmer never writes

```
Socket

Connect

Send

Receive

Decode
```

gRPC does everything.

---

# 9. Why Not REST?

Suppose we send

```
{
  "id":15,
  "type":"email",
  "priority":5
}
```

JSON.

Human readable.

Easy.

But.

Every request

must

- convert object → JSON
- send text
- parse JSON
- rebuild object

Thousands of times per second.

---

gRPC instead sends

binary data.

Smaller.

Faster.

Less CPU.

---

# 10. Why Quorum Uses gRPC

Quorum is not a public API.

It is

```
Scheduler

↓

Workers

↓

Storage

↓

Future Nodes
```

All communication happens

between services.

Nobody is opening a browser.

Nobody cares whether packets are human readable.

We care about

- speed
- reliability
- contracts
- correctness

gRPC is designed exactly for this scenario.

---

# 11. Industry Usage

gRPC powers many of today's largest distributed systems.

Examples

Google Cloud

Kubernetes

etcd

CockroachDB

Temporal

Envoy

Istio

HashiCorp Consul

Cloud Spanner

Many internal services at Netflix, Uber, and Dropbox

---

# 12. Advantages

✓ Fast

✓ Binary protocol

✓ Automatic client generation

✓ Strong contracts

✓ Streaming support

✓ Language independent

✓ Built on HTTP/2

---

# 13. Disadvantages

✗ Harder to debug manually

✗ Requires code generation

✗ Less browser-friendly

✗ More tooling than REST

---

# 14. Engineering Decision (ADR-006)

## Context

Quorum is evolving from a single-process scheduler into a distributed system.

Multiple processes and machines must communicate efficiently.

## Alternatives

- Raw TCP sockets
- REST + JSON
- WebSockets
- Apache Thrift
- gRPC

## Decision

Use gRPC.

## Rationale

- Efficient binary communication.
- Strong contracts via Protocol Buffers.
- Automatic code generation.
- Excellent Go ecosystem.
- Industry-standard for service-to-service communication.

## Consequences

Positive

- High performance.
- Type-safe APIs.
- Easier evolution of distributed components.

Negative

- Requires `protoc` and generated code.
- Slightly steeper learning curve than REST.

---

# 15. Connection to Quorum

This chapter explains why we introduced files such as:

```
worker.proto

↓

protoc

↓

worker.pb.go

↓

worker_grpc.pb.go
```

Those generated files allow Quorum's scheduler to invoke remote workers as though they were local functions, while the networking details remain hidden behind the gRPC runtime.

---

# Key Takeaways

- RPC allows a program to call functions on another machine as if they were local.
- gRPC is Google's modern RPC framework built on HTTP/2 and Protocol Buffers.
- Quorum uses gRPC because it is a distributed scheduler, where efficient service-to-service communication is more important than browser compatibility.
- The `.proto` file defines the contract; generated code enforces that contract across client and server.