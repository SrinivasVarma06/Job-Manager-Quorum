# Chapter 6 — gRPC & Protocol Buffers (Part 2)
## Protocol Buffers: The Language of gRPC

> "A protocol is an agreement. Protocol Buffers are the agreement that all programs understand."

---

# Learning Objectives

After reading this chapter, you should be able to answer:

- What is Protocol Buffers?
- Why was it created?
- Why do `.proto` files exist?
- What is `protoc`?
- What are `protoc-gen-go` and `protoc-gen-go-grpc`?
- Why are request and response objects generated automatically?
- Why didn't Quorum simply send `job.Job` directly?
- What exactly gets transmitted across the network?

---

# 1. The Problem

Imagine two computers.

```
Scheduler

↓

Worker
```

The scheduler has

```go
type Job struct {

    ID int

    Type string

    Priority int

}
```

The worker also has

```go
type Job struct {

    ID int

    Type string

    Priority int

}
```

Looks fine.

But...

How do we send

a Go struct

over a network?

Networks don't understand Go.

They only understand

```
Bytes
```

---

# 2. Serialization

Suppose

```
Job

↓

Network
```

The Job must become

```
010101010010010...
```

This process is called

**Serialization.**

The receiver performs

the opposite operation.

```
Bytes

↓

Job
```

called

**Deserialization.**

---

# 3. Why Not Send Go Structs?

Imagine

Scheduler written in Go.

Worker written in Java.

```
Go Struct

↓

???

↓

Java Class
```

Impossible.

Java doesn't know

what a Go struct is.

---

Distributed systems rarely use

language-specific objects.

Instead,

they define

a common language.

---

# 4. Protocol Buffers

Protocol Buffers are

that common language.

Instead of saying

```
Here's my Go struct.
```

we say

```
Here is

the definition

of the data.
```

Example

```proto
message SubmitJobRequest {

    int32 id = 1;

    string type = 2;

    int32 priority = 3;

}
```

Notice.

No Go.

No Java.

No Python.

Only

the schema.

---

# 5. Why is it called "Protocol Buffers"?

Let's split the name.

## Protocol

A protocol means

an agreement.

Example

```
Sender

↓

Receiver

Both agree

Field 1 = ID

Field 2 = Type

Field 3 = Priority
```

Without agreement,

communication fails.

---

## Buffer

Historically,

Google internally used

efficient binary buffers

to store messages.

Hence

Protocol Buffers.

Nowadays,

people simply call them

```
protobuf
```

---

# 6. Why Do We Write `.proto` Files?

The `.proto` file is

the single source of truth.

Everything else

is generated.

Think of it like

an architectural blueprint.

```
Blueprint

↓

House
```

or

```
worker.proto

↓

Generated Code
```

---

# 7. What is `protoc`?

`protoc`

stands for

**Protocol Buffer Compiler.**

Notice

compiler.

Just like

```
C++

↓

g++

↓

Machine Code
```

we have

```
worker.proto

↓

protoc

↓

Go Code
```

`protoc`

does **not**

know Go.

It only understands

protobuf syntax.

---

# 8. Then Why Install `protoc-gen-go`?

Good question.

Think of `protoc`

as

the manager.

```
protoc

↓

Which language?

```

You answer

```
Go
```

Now

`protoc`

calls

```
protoc-gen-go
```

which knows

how to generate

Go structs.

---

# 9. Then Why `protoc-gen-go-grpc`?

Because protobuf

and gRPC

are different things.

Remember

Protocol Buffers

↓

Data

gRPC

↓

Remote Functions

---

`protoc-gen-go`

creates

```go
type SubmitJobRequest struct

type SubmitJobResponse struct
```

Only

the messages.

---

`protoc-gen-go-grpc`

creates

```go
WorkerServiceClient

WorkerServiceServer

RegisterWorker()

SubmitJob()

Heartbeat()
```

Only

the RPC layer.

Two plugins.

Two jobs.

---

# 10. What Actually Happened in Quorum?

You wrote

```
worker.proto
```

Then

```
protoc

↓

protoc-gen-go

↓

worker.pb.go
```

contains

```
Messages

Enums

Serialization

Deserialization
```

Then

```
protoc

↓

protoc-gen-go-grpc

↓

worker_grpc.pb.go
```

contains

```
Client

Server

Interfaces

Registration

RPC plumbing
```

---

# 11. Why Two Files?

Because

they solve

different problems.

```
worker.pb.go

↓

Data
```

```
worker_grpc.pb.go

↓

Networking
```

Keeping them separate

reduces coupling.

---

# 12. Why Didn't We Send `job.Job`?

One of the biggest questions.

Suppose tomorrow

we change

```go
type Job struct {

    RetryCount int

}
```

Should every remote service

break?

No.

Instead

the network contract

remains

```
SubmitJobRequest
```

The internal representation

can evolve independently.

This is

very important.

---

# 13. Contracts

Your `.proto`

is a contract.

Imagine

```
Scheduler

↓

Contract

↓

Worker
```

Neither side

needs to know

how the other

is implemented.

Only

the contract matters.

---

# 14. Backward Compatibility

Suppose

Version 1

```
id

type
```

Later

Version 2

adds

```
priority
```

Old workers

can still understand

messages.

This is one of protobuf's

greatest strengths.

We'll study

versioning

later.

---

# 15. Advantages

✓ Binary

✓ Compact

✓ Fast

✓ Strongly typed

✓ Language independent

✓ Backward compatible

✓ Automatically generated

---

# 16. Disadvantages

✗ Requires tooling

✗ Binary not human readable

✗ Generated code can be large

✗ Learning curve

---

# 17. Connection to Quorum

Let's map our project.

```
worker.proto

↓

protoc

↓

worker.pb.go

↓

SubmitJobRequest

HeartbeatRequest

RegisterWorkerRequest
```

and

```
worker_grpc.pb.go

↓

WorkerServiceClient

↓

WorkerServiceServer

↓

RegisterWorker()

SubmitJob()

Heartbeat()
```

Everything else

uses

those generated types.

---

# 18. Engineering Decision (ADR-007)

## Context

Quorum requires structured communication between independent processes.

## Alternatives

- JSON
- XML
- Gob
- MessagePack
- Protocol Buffers

## Decision

Protocol Buffers.

## Why?

- Smaller messages
- Faster serialization
- Strong contracts
- Cross-language support
- Excellent integration with gRPC

## Consequences

Positive

- Efficient communication
- Safer APIs
- Easier versioning

Negative

- Requires generated code
- Less human-readable than JSON

---

# 19. Interview Questions

### Beginner

What is Protocol Buffers?

---

### Intermediate

Why do we need `protoc`?

Why isn't the `.proto` file enough?

---

### Senior

Why should network contracts (`SubmitJobRequest`) be kept separate from internal domain models (`job.Job`)?

How does this improve maintainability?

---

# 20. Key Takeaways

- Protocol Buffers define the structure of data exchanged between services.
- `.proto` files are language-independent contracts.
- `protoc` is the compiler that reads `.proto` files.
- `protoc-gen-go` generates Go data types.
- `protoc-gen-go-grpc` generates Go RPC interfaces and client/server plumbing.
- Quorum deliberately keeps its network contract separate from its internal `job.Job` model to allow both to evolve independently.