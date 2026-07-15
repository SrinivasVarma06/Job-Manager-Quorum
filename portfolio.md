# Engineering Portfolio Redesign — Final-Year Placement Strategy

**Reviewer stance:** Senior Staff Engineer / hiring manager lens (Google, Meta, Stripe, Cloudflare, Datadog-tier bar). Blunt critique first, then a rebuilt portfolio.

---

## PART 1 — Honest Critique of the Source Material

Your source document is ~83 entries, mostly scraped from LinkedIn "project idea" posts. I read all of them. Here's the unfiltered take, grouped by pattern, not by number — there's too much noise to critique individually.

### Category A: Instant rejects (resume-red-flag territory)
- **Blockchain certificate verification (#3)** — Dead trend for 2026 hiring. Interviewers now read "blockchain" + "certificate verification" as "did a Web3 bootcamp in 2021." Kill it entirely.
- **Anything "clone" (Notion clone, Tinder-for-X, LaunchDarkly clone, Vercel clone, ecommerce/resume builder, habit tracker, bookmark manager, weather/todo/expense tracker)** — These are fine as weekend practice, worthless as portfolio centerpieces. An interviewer has seen 200 of each.
- **"Build your own X" as a bare list (Docker, curl, grep, shell, JSON parser, etc.)** — Individually these are great *learning* exercises but terrible as standalone flagship projects because they don't show *system design* — they show "I followed a tutorial for a weekend." The one exception: if you compose several of them into a coherent distributed system (see Flagship #3 below), the composition itself becomes the impressive artifact.
- **Chatbot / voice assistant / face recognition / Naruto hand-gesture detector / grocery store app** — Toy demos. Zero.
- **"Kills decision fatigue" / "personal life dashboard" / "quit smoking app" / niche student-startup ideas (#60–64)** — These read as consumer app ideas, not engineering demonstrations. A senior interviewer will ask "what's hard about this?" and there's no good answer.
- **Data analytics tutorials copy-pasted from LinkedIn (Superstore/Netflix/Yelp SQL projects, #14)** — These are literally named after the tutorial dataset. An interviewer who has hired 50 grads has seen the Superstore dataset in 40 resumes.

### Category B: Reasonable ideas, badly under-scoped
- **Real-Time Collaborative Code Editor (#1)** — The idea is good; the *implementation described* is not ("broadcast raw text over Socket.IO") — that's not collaborative editing, that's a chatroom with syntax highlighting. It breaks the moment two people type in the same region simultaneously (last-write-wins, no conflict resolution). This needs a real CRDT or OT core to be worth anything — see Flagship #1.
- **Personal Cloud Storage with AI tagging (#2)** — The "AI organization" part is a thin wrapper around a vision API call. The interesting *systems* problem here (chunked/resumable uploads, content-addressable storage, dedup, async pipelines) is buried under a boring "Dropbox clone" framing. Worth keeping only if you strip the AI-tagging gimmick and lean hard into the storage-engine mechanics — see Flagship #4.
- **Social Media Analytics Tool (#4)** — A cron job + Postgres + Chart.js. This is a data pipeline exercise, not a portfolio centerpiece. Kill, or fold into a proper streaming/ETL project if you want that skill area (not recommended as a flagship; too thin).
- **AI Content Generator with fine-tuning (#5)** — Fine-tuning via a hosted API's `/fine-tunes` endpoint is not "ML engineering," it's filling out a form. If you want LLM depth, the RAG/eval/guardrail direction (items #12, #49–51, #83) is far more defensible in an interview.
- **LedgerLens (#83)** — This is genuinely the strongest single idea in the whole document. It already has real architecture (multi-agent, verification loop, eval harness, cost-tiering). I'm keeping it almost as-is as Flagship #2, with the addition of a stronger observability/production layer since the source treats deployment as an afterthought.

### Category C: Good raw material, wrong shape
- The huge cluster of "build your own Redis / Git / DNS resolver / load balancer / rate limiter / message broker / API gateway" (#11, #19, #77) is individually thin but **collectively is the seed of an excellent systems-programming flagship** if you build them as *interoperating components of one platform* instead of disconnected weekend katas.
- The Oracle-webinar projects (#6–10) are vendor-marketing content (Oracle 26ai JSON Duality Views, OCI Generative AI) — technically fine concepts, but tying your flagship project to a single vendor's marketing stack looks like you followed a sponsored tutorial, not that you made an architectural choice. Extract the *concepts* (unified memory layer for vector+graph+relational, multi-agent supervisor pattern) and rebuild vendor-neutral.
- The "System 1 / System 2 router," "Synthetic Data Factory," "DSPy Auto-Optimizer" (#56–59) are excellent *component* ideas for a production LLM platform, not full projects on their own. Folded into Flagship #2 and #5.
- Repos/roadmap links (#15, #18) aren't projects at all — reading material, not resume lines. Ignore for portfolio purposes.

### Verdict
Out of ~83 entries, I'm keeping the *concepts* behind roughly 12 of them, discarding the rest, and merging what's left into **6 flagship projects** that each hit a distinct skill cluster the prompt asked for (distributed systems, AI infra, real-time/concurrency, storage engines, security/networking, systems programming/hardware). Six deep projects beat twenty shallow ones in every technical interview I've run.

---

## PART 2 — The Flagship Portfolio (Ranked)

| Rank | Project | Primary Skill Cluster | Est. Time |
|---|---|---|---|
| 1 | **Quorum** — Distributed Job Orchestration Platform | Distributed systems, backend, fault tolerance | 10–12 wks |
| 2 | **LedgerLens** — Multi-Agent Financial Filing Intelligence | AI infra, RAG, LLMOps | 8–10 wks |
| 3 | **Ink** — CRDT-Based Real-Time Collaborative Editor | Concurrency, real-time systems, networking | 6–8 wks |
| 4 | **Silo** — Content-Addressable Object Storage Engine | Storage internals, systems programming | 8–10 wks |
| 5 | **Wardn** — API Gateway & LLM Guardrail Gateway | Security, networking, rate limiting | 6 wks |
| 6 | **Forge** — RISC-V Toy CPU + Compiler Toolchain (stretch/hardware track) | Hardware/software co-design, compilers | 8–10 wks (optional 7th slot) |

Below, each project in full detail. Projects 1–5 are the recommended **core five**. Project 6 is an optional 6th slot *only if* you want to signal hardware/PL depth (common for infra/perf-focused roles at companies like NVIDIA, Cloudflare, or low-level teams at Google).

---

## FLAGSHIP 1 — Quorum: Distributed Job Orchestration Platform

### Elevator Pitch
A self-built Temporal/Airflow-lite: a distributed system that schedules, executes, retries, and observes millions of background jobs across a fleet of worker nodes, with leader election, exactly-once-ish delivery semantics, and a live dashboard — built from scratch, not from a framework.

### Problem Statement
Every real backend eventually needs reliable async execution: send-emails, resize-images, run-ETL, retry-payments. Cron jobs and "just use a queue" don't survive at scale — workers crash mid-job, queues need backpressure, retries need idempotency, and someone needs visibility into what's stuck. Companies pay for Temporal, Airflow, or Sidekiq Enterprise specifically because building this correctly is hard. Interviewers know this is hard, which is exactly why building it yourself is high-signal.

### Resume Description
- Designed and built a distributed job orchestration engine handling 10K+ jobs/sec with at-least-once delivery, idempotent retries, and leader-election-based coordination across worker nodes
- Implemented a custom rate limiter, circuit breaker, and priority-queue scheduler from first principles (no framework), backed by Raft-based leader election for the scheduler tier
- Built an observability stack (structured logs, Prometheus metrics, distributed tracing) exposing p50/p95/p99 job latency and a live dashboard of queue depth and worker health
- Load-tested to 5K concurrent workers using a chaos harness that randomly kills workers/network partitions mid-job and verifies no job is lost or double-executed unsafely
- Deployed on Kubernetes with Terraform-provisioned infra, horizontal autoscaling on queue depth, and blue/green deploys via GitHub Actions

### Technical Design
**Architecture (3-tier):**
1. **Control plane** — a small Raft-based cluster (3–5 nodes) holding job metadata, leader election, and the scheduling decision log. This is where you build your own "Redis"-style in-memory store with WAL persistence (pulling directly from the "build your own Redis" idea in your source list) rather than bolting on real Redis, because owning the storage layer is the actual signal.
2. **Queue/broker tier** — a custom message broker (partitioned, similar to a mini Kafka/NATS) that workers pull from. Partitioning by job-type/tenant enables horizontal scaling.
3. **Worker fleet** — stateless workers that pull jobs, execute, heartbeat, and report completion/failure. Workers implement idempotency keys so retries after a crash never double-apply side effects.

**Data flow:** Client submits job → API validates + writes to control-plane log (via Raft consensus) → job appears in broker partition → worker claims job (lease with TTL) → executes → on success, ack + write completion event; on crash (missed heartbeat), lease expires, job is requeued → circuit breaker trips if a job type fails repeatedly, preventing pathological retry storms.

**Key design decisions & trade-offs:**
- **At-least-once, not exactly-once** — explicitly chosen and documented (with an ADR) because true exactly-once requires either idempotent operations or distributed transactions; pretending to offer exactly-once and failing is a worse interview answer than correctly explaining the trade-off.
- **Custom broker vs. real Kafka** — build your own for the learning signal, but include a written comparison of what you'd lose/gain using Kafka in production. This "I could have used X, here's why I built it and here's the trade-off" narrative is exactly what senior interviewers probe for.
- **Monolith control plane, not microservices** — the control plane is small and consistency-critical; splitting it into services would only add network hops without benefit. State this explicitly to show you don't cargo-cult microservices.

### Architecture Diagram (text)
```
                ┌─────────────┐
 Client/API ───▶│  API Layer   │
                └──────┬──────┘
                       ▼
             ┌──────────────────┐
             │  Control Plane    │  (Raft cluster, 3-5 nodes)
             │  - job metadata   │
             │  - leader elect   │
             │  - WAL storage    │
             └─────────┬─────────┘
                       ▼
             ┌──────────────────┐
             │  Broker (custom)  │  partitioned queue
             └─────────┬─────────┘
             ┌─────────┼─────────┐
             ▼         ▼         ▼
        ┌────────┐┌────────┐┌────────┐
        │Worker 1││Worker 2││Worker N│  (heartbeat, lease, idempotent exec)
        └────────┘└────────┘└────────┘
                       │
                       ▼
          ┌───────────────────────┐
          │ Observability stack    │  Prometheus + Grafana + tracing
          └───────────────────────┘
```

### Feature Roadmap
- **Beginner:** single-node job queue with retries, basic priority levels
- **Intermediate:** multi-worker fleet, heartbeat/lease-based failure detection, rate limiting per job-type
- **Advanced:** Raft-based leader election for the control plane, custom partitioned broker, circuit breakers, idempotency-key enforcement
- **Stretch:** multi-tenant isolation with per-tenant quotas, dead-letter queues with replay UI, cron-style recurring jobs with drift correction
- **Research-grade:** exactly-once semantics via a two-phase commit protocol between broker and worker state; formal TLA+ spec of the leader-election/lease protocol to catch edge-case bugs before implementation

### Production Engineering
- **AuthN/Z:** mTLS between workers and control plane; API-layer OAuth2/JWT for job submission
- **RBAC:** admin / submitter / read-only-viewer roles on the dashboard
- **Rate limiting:** token-bucket per tenant on job submission
- **Caching:** in-memory hot-path cache of job metadata on control-plane leader
- **Retries/circuit breakers:** exponential backoff with jitter; circuit breaker per job-type
- **Load balancing:** worker pool behind a consistent-hashing partition assignment (avoids thundering herd on rebalance)
- **Monitoring/logging/tracing:** structured JSON logs, Prometheus metrics, OpenTelemetry traces spanning submit → schedule → execute
- **Feature flags:** toggle new scheduler strategies without redeploy
- **Secrets/config:** Vault or SOPS-encrypted config, never plaintext in repo
- **Backups/DR:** WAL snapshotting + replay; documented RPO/RTO
- **Horizontal scaling:** workers autoscale on queue depth (K8s HPA custom metric)
- **Security:** input validation on job payloads, sandboxed job execution (no arbitrary code exec without isolation)

### Deployment
Docker Compose for local dev → Kubernetes (kind/minikube locally, EKS/GKE for cloud demo) → Terraform for cluster + networking → GitHub Actions CI/CD (test → build → push → deploy) → NGINX/Envoy as reverse proxy/ingress → HTTPS via cert-manager/Let's Encrypt → Grafana Cloud or self-hosted Grafana for monitoring/alerting.

### Testing
- Unit: scheduler logic, lease expiry, circuit breaker state machine
- Integration: worker↔broker↔control-plane round trips
- Contract: API schema tests for job submission
- E2E: submit → execute → verify side effect, across a multi-node docker-compose cluster
- Load/stress: k6 or Locust driving 10K jobs/sec
- Chaos: randomly kill worker containers / partition network (via toxiproxy) mid-job and assert no unsafe double-execution

### Performance Goals
- p99 job pickup latency < 200ms at 5K jobs/sec
- Control plane survives loss of 2 of 5 nodes with zero data loss
- Worker fleet scales linearly to 500 workers with no broker bottleneck (documented via benchmark graphs)

### Learning Outcomes
Consensus protocols in practice, queueing theory, backpressure, idempotency design, chaos engineering methodology, and how to write an ADR that survives a design review.

### Possible Interview Questions
- Beginner: "How do you retry a failed job safely?"
- Mid: "How does your system handle a worker crashing mid-job?"
- Senior: "Walk me through what happens during a network partition between two control-plane nodes — what does your system guarantee, and what does it explicitly not guarantee?"
- Staff-level: "Why Raft over Paxos here? What would change if you needed multi-region?"

### GitHub Structure
`README.md` (with architecture diagram + demo GIF), `/docs/adr/*.md`, `/docs/architecture.md`, `/docs/api.md` (OpenAPI spec), `/docs/deployment.md`, `/benchmarks/results.md` with graphs, `/demo` (video link + screenshots), `CONTRIBUTING.md`, `.github/ISSUE_TEMPLATE/`, `.github/workflows/ci.yml`.

### Timeline
MVP (2 wks) → Intermediate (3 wks) → Production-ready (4 wks) → Polish/docs/benchmarks (2 wks). Total ~10–12 weeks part-time.

### Difficulty Ratings
Engineering: 9/10 · System Design: 9/10 · Backend: 9/10 · Deployment: 7/10 · Resume Value: 10/10 · Placement Impact: 10/10 · Uniqueness: 8/10 · Time: High

---

## FLAGSHIP 2 — LedgerLens: Multi-Agent Financial Filing Intelligence Platform

*(Kept largely as designed in your source material — it's the strongest idea there. Summarized here; expand the full write-up from source item #83, which already contains ingestion, retrieval, multi-agent orchestration, eval harness, and LLMOps sections in interview-ready form.)*

### Elevator Pitch
A document-intelligence system that ingests SEC filings, answers analyst-grade financial questions with page-level citations, and refuses to state a number it can't trace back to source — because it verifies every claim against a second agent and against real XBRL ground-truth data before it answers.

### Why it's kept
Unlike 90% of "RAG chatbot" projects, this one has a built-in anti-hallucination verification loop, a real eval harness with a golden dataset, CI-gated regression testing, and a cost-tiered model-routing architecture. That combination — RAG + multi-agent + eval + LLMOps — is precisely the four-part checklist every 2026 AI engineering JD asks for, per your own source analysis.

### What to add beyond the source spec (production hardening)
- **AuthN/Z + RBAC:** analyst / auditor / read-only roles; row-level access if extended to multi-tenant use
- **Rate limiting** on the query API per API key
- **Circuit breaker** around the LLM provider calls (fallback to smaller local model if GPT-4o/Claude API degrades)
- **Secrets management** for API keys via a vault, not `.env` in the repo
- **Kubernetes deployment** with GPU node pool for the local Qwen model, Terraform-provisioned
- **Disaster recovery**: nightly snapshot of the vector store + XBRL cache

### Architecture Diagram (text)
```
EDGAR full-text + XBRL APIs
        │
        ▼
 Ingestion (HTML parse / OCR fallback)
        │
        ▼
 Storage: page images + structured tables + XBRL facts
        │
        ▼
 Hybrid Retrieval (ColPali dense + BM25, pgvector)
        │
        ▼
 LangGraph Multi-Agent:
   Router → Extraction → Reconciliation (vs XBRL) → Analyst → Guardrail
        │
        ▼
 API (FastAPI) ──▶ Frontend (cited answer + source highlight)
        │
        ▼
 Observability: Langfuse traces, cost/latency dashboards, CI eval gate
```

*(Feature roadmap, testing, performance goals, timeline, difficulty ratings: reuse the structure from Flagship 1 as a template — Beginner: single-doc Q&A; Intermediate: multi-doc + XBRL reconciliation; Advanced: full multi-agent + guardrail; Stretch: multi-tenant SaaS mode; Research-grade: fine-tune a small extraction model on your golden set instead of relying purely on prompting.)*

Difficulty: Engineering 8/10 · System Design 8/10 · AI/ML depth 10/10 · Resume Value 10/10 · Placement Impact 9/10 (esp. for AI engineering roles) · Uniqueness 8/10 · Time: Medium-High.

---

## FLAGSHIP 3 — Ink: CRDT-Based Real-Time Collaborative Editor

### Elevator Pitch
A rebuild of "Google Docs internals" — a text editor where concurrent edits from many users converge correctly without a central lock, using a real CRDT (not last-write-wins broadcast), with offline editing and merge-on-reconnect.

### Problem Statement
Every collaborative tool (Docs, Figma, Linear) solves the same hard problem: multiple users mutate shared state concurrently and must converge to the same result without a central bottleneck. Naive WebSocket broadcast (as in the original source idea) silently corrupts documents under concurrent edits — this is precisely the gap between "looks like Google Docs" and "is engineered like Google Docs."

### Resume Description
- Implemented a CRDT (RGA/Yjs-style) text data structure from scratch in TypeScript, proven to converge under arbitrary concurrent edit interleavings
- Built a WebSocket sync layer with offline-first editing, local persistence, and automatic merge on reconnect with zero data loss
- Designed an awareness protocol (cursors/presence) separate from the document CRDT to avoid presence updates polluting document history
- Load-tested with 200 simulated concurrent editors typing against a shared document, verifying convergence and sub-100ms propagation
- Deployed with horizontally-scaled WebSocket servers behind sticky-session load balancing and Redis pub/sub for cross-node broadcast

### Technical Design
Core: implement (not import Yjs/Automerge, though you may benchmark against them) a sequence CRDT — each character/operation gets a unique, causally-ordered ID; inserts/deletes are commutative and idempotent, so applying them in any order converges. Document state syncs via a WebSocket gateway; multiple gateway nodes share state via Redis pub/sub so the system scales horizontally, not single-node like the original idea.

**Trade-off to document explicitly:** CRDT vs. Operational Transformation (OT). CRDTs are simpler to reason about for decentralized/offline scenarios; OT (what Google Docs actually uses) needs a central server to sequence ops but has smaller metadata overhead. Building the CRDT version and writing up why you didn't choose OT is itself a strong interview talking point.

### Architecture Diagram (text)
```
Client A (local CRDT + IndexedDB) ─┐
Client B (local CRDT + IndexedDB) ─┼─▶ WebSocket Gateway (N nodes)
Client C (offline, syncs later)   ─┘         │
                                    Redis Pub/Sub (cross-node broadcast)
                                              │
                                    Persistence: periodic CRDT snapshot → Postgres
```

### Feature Roadmap
Beginner: single-doc, single-room sync with basic OT-lite. Intermediate: full CRDT, presence/cursors. Advanced: offline editing + reconnect merge, horizontal scaling via Redis. Stretch: rich-text (not just plain text) CRDT supporting bold/italic/headings. Research-grade: compare your CRDT's metadata growth over a long-lived doc against a garbage-collection strategy (tombstone pruning).

### Production Engineering
AuthN via JWT; per-document ACL/RBAC (owner/editor/viewer); rate limiting on op submission per client; circuit breaker on Redis pub/sub failure (degrade to single-node mode); sticky-session load balancing; structured tracing per op; feature flags for rich-text mode; secrets via env-vault; periodic snapshot backups; horizontal scaling proven via load test.

### Deployment
Docker Compose → Kubernetes with a Redis StatefulSet → Terraform → GitHub Actions → NGINX ingress with WebSocket support → HTTPS/monitoring.

### Testing
Unit tests on CRDT convergence (property-based testing: generate random concurrent op sequences, assert all replicas converge). Integration: multi-client WebSocket sync tests. E2E: browser automation with multiple simulated clients typing simultaneously. Load: 200+ concurrent WS connections. Chaos: kill a gateway node mid-sync, verify no lost ops.

### Performance Goals
<100ms edit propagation at 200 concurrent editors; convergence guaranteed under any op ordering (proven via property tests, not just manual testing).

### Learning Outcomes
CRDT theory, causal ordering/vector clocks, WebSocket scaling patterns, property-based testing.

### Interview Questions
Beginner: "What breaks if two users edit the same word at once with plain broadcast?" Senior: "How does your CRDT handle deletes without leaving unbounded tombstones forever?" Staff: "CRDT vs OT — what would make you choose differently for a spreadsheet instead of text?"

Difficulty: Engineering 8/10 · System Design 8/10 · Backend 7/10 · Resume Value 8/10 · Uniqueness 9/10 (most students do the naive broadcast version) · Time: Medium.

---

## FLAGSHIP 4 — Silo: Content-Addressable Object Storage Engine

### Elevator Pitch
A miniature S3/Git-hybrid: a storage engine where files are content-addressed (hashed), deduplicated, chunked for resumable multipart uploads, and served through a custom LSM-tree-backed metadata index you build yourself — not a thin wrapper over the filesystem.

### Problem Statement
"Store a file" sounds trivial until you need: resumable uploads for flaky connections, deduplication across millions of near-identical files, fast metadata lookups at scale, and safe concurrent writes. The original "personal cloud + AI tagging" idea skipped all of this in favor of a vision-API gimmick; this version keeps the storage-engineering core and drops the AI framing, which was doing no real engineering work.

### Resume Description
- Built a content-addressable storage engine with chunked, resumable multipart uploads and automatic block-level deduplication (content hashing via BLAK3/SHA-256)
- Implemented a custom LSM-tree-based metadata index (memtable + SSTables + compaction) for O(log n) file/folder lookups at millions-of-objects scale, instead of relying on a general-purpose DB
- Designed a garbage-collection protocol for orphaned chunks after delete/dedup, with reference counting
- Built secure, expiring pre-signed URL generation for sharing, and a permission model (owner/collaborator/public-read)
- Benchmarked write/read throughput against local disk and S3-compatible MinIO baseline, documenting trade-offs

### Technical Design
Files are chunked (content-defined chunking, e.g., Rabin fingerprinting) so identical chunks across different files are stored once. Metadata (path → chunk-hash list, ownership, permissions) lives in a custom LSM-tree you implement (memtable in-memory + WAL for durability + periodic flush to sorted SSTables + background compaction) — this is the real "database internals" signal the prompt asked for, versus just using Postgres. Uploads are resumable: client and server track which chunks have landed, so a dropped connection resumes from the last acknowledged chunk instead of restarting.

**Trade-off to document:** built your own LSM-tree vs. embedding RocksDB. Building your own proves you understand write-amplification, compaction strategies, and durability — but RocksDB is what you'd actually use in production. State this explicitly.

### Architecture Diagram (text)
```
Client ──(chunked upload, resumable)──▶ API Layer
                                            │
                          ┌─────────────────┼─────────────────┐
                          ▼                                   ▼
                 Chunk Store (content-hash addressed)   Metadata Engine
                 dedup via hash lookup                  (custom LSM-tree:
                                                          memtable→WAL→SSTables
                                                          →compaction)
                          │                                   │
                          └───────────────┬───────────────────┘
                                          ▼
                              Garbage Collector (ref-counted chunk cleanup)
```

### Feature Roadmap
Beginner: single-node upload/download with simple hashing, no dedup. Intermediate: chunked resumable uploads + dedup. Advanced: custom LSM-tree metadata engine with compaction. Stretch: pre-signed URLs, sharing/permissions, replication across 2+ storage nodes. Research-grade: erasure coding (Reed-Solomon) instead of full replication for storage-efficient durability.

### Production Engineering
AuthN/Z with per-object ACLs, RBAC (owner/collaborator/viewer), rate limiting on upload API, retries on chunk upload failure, load balancing across storage nodes, full observability (chunk-store hit/miss rate, compaction latency, GC cycle duration as custom metrics), secrets/config management, backup via periodic SSTable snapshot to cold storage, horizontal scaling via consistent hashing across storage nodes, security review of pre-signed URL expiry and signature verification.

### Deployment
Docker Compose (multi-node simulation) → Kubernetes StatefulSet for storage nodes → Terraform → GitHub Actions → NGINX reverse proxy for upload endpoint (streaming support) → HTTPS/monitoring/alerting on disk usage and compaction backlog.

### Testing
Unit: LSM-tree correctness (insert/read/compact under randomized operation sequences), chunk dedup logic. Integration: multipart resumable upload across simulated network drops. Contract: storage API schema tests. E2E: full upload→dedup→download→delete→GC cycle. Load: throughput benchmark at increasing object counts (1K → 1M objects), showing lookup latency stays near-flat due to LSM-tree. Chaos: kill a storage node mid-write, verify no corruption.

### Performance Goals
Sub-10ms metadata lookup at 1M+ objects; dedup ratio benchmark on a realistic dataset (e.g., 30% space savings on redundant files); resumable upload recovers correctly after connection drop in <1s.

### Learning Outcomes
LSM-tree internals, content-addressable storage, chunking algorithms, garbage collection design, durability/WAL patterns.

### Interview Questions
Beginner: "Why chunk files instead of storing them whole?" Mid: "Walk me through what happens on a compaction — why do we need it?" Senior: "How do you guarantee no chunk is garbage-collected while another file still references it, under concurrent writes?"

Difficulty: Engineering 9/10 · System Design 8/10 · Backend 8/10 · Resume Value 9/10 · Uniqueness 9/10 · Time: Medium-High.

---

## FLAGSHIP 5 — Wardn: API Gateway & LLM Guardrail Gateway

### Elevator Pitch
A single reverse-proxy-style gateway that does double duty: standard API-gateway functions (auth, rate limiting, routing, caching) for conventional services, *and* an LLM-specific guardrail layer (PII redaction, prompt-injection detection, output schema enforcement, cost caps) sitting in front of any LLM provider — because every company shipping LLM features in 2026 needs exactly this, and almost nobody's portfolio has it.

### Problem Statement
Two real, separate problems that share the same architectural shape: (1) every microservice fleet needs a gateway handling auth/rate-limiting/routing so individual services don't reimplement it, and (2) every product with an LLM feature needs a middleware layer enforcing safety/cost/format rules before a response reaches a user — and almost no portfolio project addresses #2 despite it being one of the fastest-growing hiring needs in AI infra.

### Resume Description
- Built a custom API gateway (Go or Rust) handling request routing, JWT auth, RBAC, and token-bucket rate limiting for a multi-service backend, replacing what would otherwise be Kong/Envoy
- Implemented a pluggable middleware pipeline for LLM traffic: prompt-injection heuristics, PII redaction (regex + NER model), JSON-schema output validation with automatic re-ask on violation, and per-tenant token/cost budget enforcement
- Designed a circuit breaker and fallback-model routing so a downstream LLM provider outage automatically fails over to a secondary model within one request cycle
- Built an audit log of every request/response pair with redaction markers, satisfying a basic compliance-logging requirement
- Load-tested the gateway to 5K req/sec with sub-5ms added latency overhead versus direct calls

### Technical Design
Two logical planes sharing one process: **Standard gateway plane** (routing table, JWT verification, RBAC policy engine, token-bucket rate limiter, response cache for idempotent GETs) and **LLM guardrail plane** (a middleware chain: input scanning → provider call → output validation → audit log). Both are built in a high-performance language (Go/Rust) because gateway latency overhead directly matters — this is your chance to show you understand *why* language choice is a real system-design decision, not just a preference.

**Trade-off to document:** building a custom gateway vs. adopting Envoy/Kong + a sidecar guardrail service. State clearly that in a real company you'd likely use Envoy for the generic plane and only hand-build the LLM guardrail plane — and explain why you built both here (learning signal + to show the two planes can share infrastructure like the rate limiter).

### Architecture Diagram (text)
```
                        ┌─────────────────────┐
 Client Request ───────▶│   Wardn Gateway       │
                        │  ┌────────────────┐  │
                        │  │ AuthN/JWT verify│  │
                        │  ├────────────────┤  │
                        │  │ RBAC policy     │  │
                        │  ├────────────────┤  │
                        │  │ Rate limiter    │  │
                        │  └───────┬────────┘  │
                        │          ▼           │
              ┌─────────┴──── routing ─────────┴─────────┐
              ▼                                          ▼
     Standard service route                     LLM guardrail pipeline
     (cache → backend svc)                        input scan → provider
                                                   call → output validate
                                                   → audit log
                                                          │
                                              Circuit breaker + fallback model
```

### Feature Roadmap
Beginner: basic routing + JWT auth. Intermediate: RBAC + token-bucket rate limiting + response caching. Advanced: full LLM guardrail pipeline (PII redaction, injection detection, schema validation). Stretch: per-tenant cost-budget enforcement with real-time spend dashboard. Research-grade: train a lightweight classifier for prompt-injection detection instead of pure regex/heuristics, and benchmark false-positive/negative rates.

### Production Engineering
AuthN/Z (JWT + mTLS for service-to-service), RBAC (tenant admin/service/read-only), rate limiting (token bucket, per-tenant and per-endpoint), caching (response cache with TTL + invalidation), retries/circuit breakers (provider fallback), load balancing (round-robin/least-conn across backend replicas), full observability (per-route latency, guardrail rejection rate, cost-per-tenant metrics), feature flags (toggle guardrail rules per tenant without redeploy), secrets/config (vault-backed provider API keys), backups (audit-log durability to object storage), horizontal scaling (stateless gateway, scale via K8s HPA), security (strict input validation, no SSRF via backend routing rules).

### Deployment
Docker Compose → Kubernetes (Deployment + HPA) → Terraform → GitHub Actions (build/test/deploy) → deployed as the actual ingress (NGINX/Envoy in front only for TLS termination, Wardn handles the rest) → HTTPS via cert-manager → Grafana dashboards + alerting on guardrail rejection spikes (signal of an attack or a broken client).

### Testing
Unit: rate limiter token-bucket math, JWT validation edge cases, PII redaction regex coverage. Integration: full request pipeline through auth→rate-limit→route. Contract: OpenAPI schema tests for both planes. E2E: simulate a prompt-injection attempt and verify it's blocked; simulate a provider outage and verify failover. Load: 5K req/sec sustained, measure added latency overhead. Chaos: kill the primary LLM provider mid-traffic, verify seamless failover.

### Performance Goals
<5ms p99 added latency for standard routing; <50ms added latency for the full LLM guardrail pipeline; 5K req/sec sustained on a single gateway instance; provider failover completes within one request cycle (no dropped requests during a provider outage).

### Learning Outcomes
Gateway architecture patterns, token-bucket vs. leaky-bucket rate limiting, PII/security scanning at the middleware layer, circuit breaker design, cost-governance patterns for LLM traffic — a skill area barely anyone else's portfolio has.

### Interview Questions
Beginner: "What's the difference between rate limiting and throttling?" Mid: "How would you prevent a compromised client from bypassing your rate limiter using multiple API keys?" Senior: "Your guardrail pipeline adds latency — how do you decide which checks run synchronously vs. async/best-effort?" Staff: "How would this design change for a multi-region deployment where tenants are pinned to specific regions?"

Difficulty: Engineering 8/10 · System Design 8/10 · Backend 8/10 · Security 8/10 · Resume Value 9/10 (especially strong for infra/platform/security-adjacent roles) · Uniqueness 9/10 · Time: Medium.

---

## OPTIONAL 6TH SLOT — Forge: RISC-V Toy CPU + Compiler Toolchain

*(Include only if targeting hardware-adjacent, performance, or compiler roles — e.g., NVIDIA, ARM, or low-level infra teams. Skip if your target roles are pure backend/AI — the five above already cover that breadth comprehensively.)*

### Elevator Pitch
A small RISC-V CPU core implemented in a HDL (or Clash/Haskell for a functional hardware-description approach), paired with a minimal compiler that lowers a C-like language down to your custom ISA subset, simulated and (optionally) synthesized to an FPGA.

### Why include it
Demonstrates hardware/software co-design and compiler construction — the two skill areas most under-represented in typical CS-grad portfolios, and a strong differentiator if even one interviewer on your loop cares about systems-below-the-OS.

### Compressed Scope
Beginner: implement a subset of RV32I in simulation (single-cycle). Intermediate: pipeline it (5-stage, handle hazards). Advanced: write a compiler front-end (lexer/parser/AST) for a small C-like language targeting your ISA subset, with a simple register allocator. Stretch: run it on an actual FPGA (e.g., iCE40 via open-source toolchain). Research-grade: add a basic branch predictor and measure IPC improvement.

Resume line: *"Designed and implemented a 5-stage pipelined RISC-V core (RV32I subset) with hazard detection/forwarding, paired with a custom compiler backend targeting the ISA, verified via a 200+ test instruction-level simulator and synthesized to FPGA."*

Difficulty: Engineering 9/10 · Uniqueness 10/10 (almost nobody else in a CS placement pool will have this) · Time: High.

---

## PART 3 — Why This Set of 5 (or 6) Wins

1. **Quorum** proves you can design and reason about distributed systems under failure — the single most tested skill in senior backend interviews (consensus, idempotency, chaos testing).
2. **LedgerLens** proves current AI-engineering depth beyond "I called the OpenAI API" — RAG + multi-agent + eval + LLMOps is exactly the 2026 job-description checklist.
3. **Ink** proves you understand concurrency and convergence at a level most candidates only discuss in the abstract (most people *talk about* CRDTs; you'll have *implemented* one).
4. **Silo** proves database/storage-engine internals — LSM-trees, durability, and garbage collection are core "how does a database actually work" interview material, and building one from scratch is rare.
5. **Wardn** proves security + networking + production-operations maturity, and is the project that most directly screams "I understand what happens after code ships," including a timely angle (LLM guardrails) most candidates haven't touched.
6. **(Forge, optional)** proves hardware/software co-design depth if you want to differentiate further for infra/perf-heavy roles.

Together these five span **every** category the brief asked for — distributed systems, AI infra, concurrency/networking, storage internals, and security/production engineering — without a single CRUD app, weather tracker, or "clone" in sight. That breadth-with-depth combination, each with a real ADR trail, a chaos test, and a benchmark graph, is what separates a placement-ready portfolio from a resume-filler one.

## Suggested Build Order
1. Wardn (shortest, builds confidence + reusable rate-limiter/auth code you'll reuse in Quorum)
2. Quorum (biggest, most impressive, start early)
3. Ink (medium, good mid-year project)
4. Silo (pairs well with Quorum's storage needs — you can even have Quorum's control plane use Silo's LSM-tree)
5. LedgerLens (do this closer to interview season since AI-tooling landscape moves fast — keep it current)
6. Forge (only if time remains before placements)

---

# PART 4 — Four More Flagships: Closing the Gaps (Linux/OS, Networking, Search, Databases)

The original six are strong but leave real holes: nothing touches **Linux internals** (namespaces, cgroups, syscalls — the exact depth Nokia/telecom/embedded-Linux shops probe for), nothing does **raw networking below the application layer**, and the brief explicitly asked for **search engines** and **distributed transactions**, neither of which appears above. These four close those gaps without duplicating anything already built.

| Rank | Project | Primary Skill Cluster | Est. Time |
|---|---|---|---|
| 7 | **Cellar** — Container Runtime from Linux Primitives | Linux internals, OS, systems programming | 8–10 wks |
| 8 | **Ferry** — L4/L7 Software Load Balancer & Packet Toolkit | Raw networking, sockets, protocols, Linux/bash | 6–8 wks |
| 9 | **Vertex** — Distributed Relational Query Engine with ACID Transactions | Database internals, distributed transactions | 10–12 wks |
| 10 | **Scout** — Distributed Search Engine | Information retrieval, indexing, ranking | 8 wks |

---

## FLAGSHIP 7 — Cellar: Container Runtime Built from Linux Primitives

### Elevator Pitch
A `runc`-style container runtime you build yourself, directly on top of Linux namespaces, cgroups v2, overlayfs, and `pivot_root` — no Docker daemon, no Kubernetes library. Given a root filesystem and a spec, Cellar spins up an isolated, resource-limited process that *is* a container, because you built the isolation primitives, not because you called an API that did.

### Problem Statement
Everyone's resume says "Docker, Kubernetes." Almost nobody can answer "what actually stops a container from seeing the host's other processes?" past "namespaces, I think." Telecom/infra companies running NFV (network function virtualization) on bare-metal Linux, and any company doing platform engineering, need engineers who understand the isolation primitives underneath the orchestration layer — not just YAML fluency. This project is the difference between "I've used Docker" and "I understand what Docker is."

### Resume Description
- Built a container runtime from scratch in Go/Rust implementing process isolation via Linux namespaces (PID, mount, network, UTS, IPC, user) and resource limits via cgroups v2, without using Docker/runc as a dependency
- Implemented root filesystem setup using `pivot_root` + overlayfs for copy-on-write layered images, plus a minimal image format compatible with OCI image spec
- Built a network namespace bridge with veth pairs and iptables NAT rules to give containers outbound connectivity and inter-container communication, replicating Docker's default bridge network by hand
- Wrote a bash-based test harness that verifies isolation guarantees (a process inside the container cannot see/signal host processes, cannot exceed its memory cgroup limit, is killed correctly on OOM)
- Benchmarked container startup latency and overhead versus `runc`, documenting where the from-scratch implementation is slower and why

### Technical Design
**Core mechanism:** on `cellar run`, the runtime forks a child process with `clone()` using namespace flags (`CLONE_NEWPID | CLONE_NEWNS | CLONE_NEWNET | CLONE_NEWUTS | CLONE_NEWIPC | CLONE_NEWUSER`), sets up a new root via `pivot_root` pointing at an overlayfs mount (lower = read-only image layers, upper = writable container layer), joins a pre-created cgroup with memory/CPU limits written via the cgroup v2 filesystem interface (`/sys/fs/cgroup/.../memory.max`), and finally `exec`s the container's entrypoint.

**Networking:** each container gets a network namespace with one end of a veth pair; the other end is attached to a bridge on the host; iptables MASQUERADE rules give outbound NAT. This is literally how Docker's bridge network works — building it by hand means you can explain every hop of a packet leaving a container in an interview.

**Image handling:** implement a minimal OCI-compatible image puller (pull layers as tarballs, verify digest, extract as overlayfs lower layers) so Cellar can run real Docker Hub images, not just toy custom images — this is the detail that convinces an interviewer this isn't a toy.

**Key trade-off to document:** you're implementing a subset of what `runc` + `containerd` do. Write an ADR explicitly listing what you skipped (seccomp profiles, full OCI runtime spec compliance, rootless mode edge cases) and why, given time constraints — showing you know the boundary of your own implementation is itself a strong signal.

### Architecture Diagram (text)
```
 cellar run <image>
        │
        ▼
 Image Puller ──▶ pulls OCI layers, verifies digest, extracts to overlayfs lowerdir
        │
        ▼
 clone() with namespace flags (PID/mount/net/UTS/IPC/user)
        │
        ▼
 pivot_root into overlayfs merged dir (lower=image, upper=container writable layer)
        │
        ▼
 cgroup v2 join (memory.max, cpu.max written before exec)
        │
        ▼
 Network namespace setup: veth pair ──▶ host bridge ──▶ iptables NAT
        │
        ▼
 exec(entrypoint)  →  container process now fully isolated + resource-limited
```

### Feature Roadmap
- **Beginner:** PID + mount namespace isolation with `chroot` (not yet `pivot_root`), manual cgroup limit via shell/bash script
- **Intermediate:** full namespace set, `pivot_root` + overlayfs, cgroup v2 integration from your runtime code (not shell scripts)
- **Advanced:** network namespace + veth/bridge/iptables NAT, OCI image pulling from a real registry
- **Stretch:** a minimal orchestration layer on top (multi-container "pod" with shared network namespace, like a mini-Kubernetes pod concept), seccomp syscall filtering
- **Research-grade:** rootless containers (user namespace mapping without root), or a gVisor-style syscall interception sandbox as a security-hardened alternative runtime mode

### Production Engineering
- **Security:** this project *is* security engineering — document your isolation guarantees and their limits explicitly (namespaces are not a hard security boundary the way a VM is; say so)
- **RBAC:** if you build the stretch-goal orchestration layer, add a simple admin/operator role model for who can start/stop containers
- **Rate limiting:** cap concurrent container creation per user to prevent fork-bombing the host
- **Retries/circuit breakers:** retry image-layer pulls on registry timeout
- **Monitoring:** expose per-container cgroup stats (memory/cpu usage) via a `/metrics` endpoint, scraped by Prometheus
- **Logging:** structured logs of container lifecycle events (create/start/stop/OOM-killed)
- **Backups/DR:** not core to this project, but document how container filesystem layers could be snapshotted for backup
- **Horizontal scaling:** N/A at single-host scope — explicitly note this is the boundary vs. Quorum/Kubernetes-scale orchestration, and reference how Cellar's runtime primitives are what a Quorum-style scheduler would sit on top of

### Deployment
Runs directly on a Linux host (bare metal or a Linux VM — **this must not be run in Docker-in-Docker without care**, since you need real namespace/cgroup access) → provisioned via a bash setup script (cgroup v2 mount check, bridge creation, iptables rules) → CI runs the test harness inside a privileged CI runner or a nested-virtualization VM → documented Vagrant/VM setup for graders/interviewers who want to run it locally without touching their own host's networking.

### Testing
- Unit: namespace flag construction, cgroup limit-writing logic, overlayfs mount option building
- Integration: full container-create-to-exec pipeline against a real pulled image
- **Isolation verification tests (the most important tests in this project):** bash scripts that assert a process inside the container cannot `kill -9` a host PID, cannot see host process list via `/proc`, is actually OOM-killed when it exceeds its memory cgroup limit, and cannot reach host-only network interfaces
- Load: startup latency across 100 sequential container creates
- Chaos: kill the runtime process mid-container-creation, verify no orphaned cgroups/namespaces leak (write a cleanup/reconciliation pass)

### Performance Goals
Container cold-start under 200ms (excluding image pull); document exact overhead vs. `runc` cold-start with a benchmark table; zero namespace/cgroup leaks after 500 create/destroy cycles (verified via a leak-detection script).

### Learning Outcomes
Linux namespaces and cgroups at the syscall level, overlayfs/union filesystems, Linux networking primitives (veth, bridges, iptables/nftables), OCI image spec, and — critically — deep, defensible bash/Linux command-line fluency from building and debugging this entirely at the shell/syscall level.

### Interview Questions
- Beginner: "What's the difference between a container and a VM?"
- Mid: "Walk me through what `pivot_root` does and why you can't just use `chroot` alone for a real container."
- Senior: "How does a cgroup actually enforce a memory limit — what happens at the kernel level when a process exceeds it?"
- Staff: "Namespaces aren't a hard security boundary — what would you add to make this runtime safe for running untrusted code?"

### GitHub Structure
`README.md` with an asciinema/terminal-recording demo (containers are hard to screenshot meaningfully — a terminal recording is the right artifact here), `/docs/adr/*.md`, `/docs/isolation-guarantees.md` (explicit list of what is and isn't guaranteed), `/scripts/setup-host.sh`, `/tests/isolation/*.sh`, `.github/workflows/ci.yml` (privileged runner), `/benchmarks/startup-latency.md`.

### Timeline
MVP with chroot+basic cgroups (2 wks) → full namespace/overlayfs (3 wks) → networking (3 wks) → image pulling + tests + docs (2–3 wks). Total ~10 weeks.

### Difficulty Ratings
Engineering: 9/10 · System Design: 7/10 · Linux/OS depth: 10/10 · Deployment: 6/10 · Resume Value: 9/10 (exceptionally high for infra/platform/telecom roles) · Placement Impact: 9/10 · Uniqueness: 10/10 (almost nobody builds this from raw primitives) · Time: High

---

## FLAGSHIP 8 — Ferry: L4/L7 Software Load Balancer & Packet Toolkit

### Elevator Pitch
A software load balancer built directly on raw sockets and `epoll`, implementing your own TCP connection handling, health checking, and both Layer-4 (TCP passthrough) and Layer-7 (HTTP-aware) load balancing modes — plus a small companion toolkit (a `tcpdump`-lite packet sniffer and a `traceroute` clone) that proves you understand the network stack from the wire up, not just from a library's abstractions.

### Problem Statement
Wardn (Flagship 5) operates at the *application* layer using standard HTTP libraries. This project deliberately goes one level lower: raw socket programming, manual event-loop design, and protocol-level understanding of TCP — exactly the depth telecom/networking companies test for (packet processing, connection state machines, network function implementation), and a level almost no student portfolio reaches because frameworks hide it entirely.

### Resume Description
- Built an L4/L7 software load balancer in C or Rust using raw sockets and an `epoll`-based event loop (no async framework), supporting round-robin, least-connections, and consistent-hashing backend selection
- Implemented Layer-7 HTTP-aware routing (path/header-based) by hand-parsing HTTP/1.1 request lines and headers off the raw socket buffer, without a parsing library
- Wrote active + passive backend health checking (periodic TCP/HTTP probes plus real-traffic error-rate tracking) with automatic backend ejection and re-admission
- Built companion CLI tools in C: a packet sniffer using raw sockets/`AF_PACKET` that decodes Ethernet/IP/TCP headers by hand (a mini `tcpdump`), and a `traceroute` clone using ICMP TTL manipulation
- Load-tested to 20K concurrent connections on commodity hardware, documenting `epoll` edge-triggered vs. level-triggered performance differences with benchmarks

### Technical Design
**Event loop:** single-threaded (or thread-per-core) `epoll`-based reactor — accept connections non-blocking, register read/write interest, dispatch on readiness. This is the same core pattern behind NGINX/HAProxy, and building it yourself means you can discuss edge-triggered vs. level-triggered `epoll` semantics from experience, not documentation.

**L4 mode:** pure TCP proxy — accept client connection, open a connection to a chosen backend, splice bytes between the two sockets with minimal parsing (fastest, protocol-agnostic).

**L7 mode:** parse the HTTP request line and headers directly from the socket's byte buffer (handle partial reads — a request can arrive across multiple `recv()` calls, and handling this correctly is the actual hard part professionals get wrong), then route based on path/header rules to a backend pool.

**Health checking:** a background thread/loop does active probes (periodic `SYN`+optional HTTP GET) and passively tracks real traffic error rates per backend; a backend crossing an error-rate threshold is ejected from rotation and periodically re-probed for recovery — this is a circuit breaker implemented at the infrastructure layer instead of the application layer, a distinction worth stating explicitly in interviews.

**Companion tools (bash/Linux fluency signal):** a raw-socket packet sniffer (`AF_PACKET`, `SOCK_RAW`) that manually parses Ethernet/IP/TCP header byte offsets to print a `tcpdump`-style summary, and a `traceroute` implementation using incrementing IP TTL + ICMP Time-Exceeded replies — both are classic "prove you understand the OSI model past the diagram" exercises.

### Architecture Diagram (text)
```
                     ┌───────────────────────┐
 Clients ───────────▶│   Ferry (epoll loop)   │
                     │  ┌─────────────────┐  │
                     │  │ Accept + dispatch│  │
                     │  ├─────────────────┤  │
                     │  │ L4: raw splice   │  │
                     │  │ L7: HTTP parse   │  │
                     │  │     + routing    │  │
                     │  └────────┬────────┘  │
                     └───────────┼────────────┘
                                 ▼
                    ┌─────────────────────────┐
                    │  Backend Pool            │
                    │  (round-robin / least-   │
                    │   conn / consistent-hash)│
                    └──────────┬──────────────┘
                               ▼
                   Health Checker (active probe +
                   passive error-rate tracking)
                               │
              ┌────────────────┴────────────────┐
              ▼                                  ▼
     Companion: packet sniffer          Companion: traceroute clone
     (AF_PACKET, manual header parse)   (ICMP TTL manipulation)
```

### Feature Roadmap
Beginner: blocking-socket TCP proxy, round-robin only. Intermediate: `epoll`-based non-blocking event loop, least-connections + consistent hashing. Advanced: L7 HTTP-aware routing with manual header parsing across partial reads, active+passive health checking. Stretch: TLS termination (integrate OpenSSL manually, understand the handshake), HTTP/2 support. Research-grade: kernel-bypass networking experiment with `io_uring` or DPDK, benchmarked against the `epoll` version.

### Production Engineering
AuthN/Z for the admin/config API (not the data plane, which is intentionally unauthenticated like a real LB); rate limiting per client IP; retries/circuit breaker via the health-checker ejection logic; **load balancing is the project itself**; observability (per-backend request count, latency histogram, health status exposed via a `/stats` endpoint scraped by Prometheus); feature flags (toggle L4 vs L7 mode, toggle balancing algorithm at runtime via config reload without restart — a real production requirement); secrets/config via a reloadable config file with SIGHUP-triggered reload (classic Unix daemon pattern); security review of the HTTP parser against malformed/adversarial input (header injection, request smuggling via ambiguous `Content-Length`/`Transfer-Encoding`).

### Deployment
Runs as a systemd service on Linux (this is the point — no Docker abstraction hiding the networking) → bash provisioning script → Terraform for a small VM fleet demo (LB + 3 backend VMs) → GitHub Actions running the test suite in a Linux CI runner → NGINX is explicitly *not* used here since Ferry replaces it → monitoring via Prometheus node-exporter-style metrics endpoint + Grafana.

### Testing
Unit: HTTP parser against malformed/partial input (fuzz it), consistent-hash distribution uniformity. Integration: full request round-trip through both L4 and L7 modes. Contract: `/stats` and config-reload API tests. E2E: kill a backend, verify health-checker ejects it within N seconds and re-admits on recovery. Load: `wrk`/`hey` driving 20K concurrent connections, document `epoll` edge- vs. level-triggered latency/throughput differences. Chaos: simulate a network partition to a backend mid-request (via `tc`/`netem`), verify graceful client-side error instead of a hang.

### Performance Goals
Sub-1ms added latency in L4 mode; sub-5ms in L7 mode; sustain 20K concurrent connections with <500MB memory on commodity hardware; health-check ejection within 3 consecutive failed probes, documented against a chosen probe interval.

### Learning Outcomes
Raw socket/`epoll` event-loop programming, TCP protocol details (handshake, partial reads, backpressure), HTTP/1.1 wire-format parsing by hand, health-checking/circuit-breaking as an infrastructure pattern, and hands-on packet-level networking (`AF_PACKET`, ICMP) — precisely the Linux/networking depth telecom and network-equipment companies screen for.

### Interview Questions
- Beginner: "What's the difference between L4 and L7 load balancing?"
- Mid: "How do you handle an HTTP request that arrives split across three `recv()` calls?"
- Senior: "Edge-triggered vs. level-triggered `epoll` — what bug do you get if you get this wrong?"
- Staff: "How would you redesign this for kernel-bypass networking, and what would you actually gain?"

### GitHub Structure
`README.md` with a terminal-recording demo of the packet sniffer/traceroute in action, `/docs/http-parsing-edge-cases.md`, `/docs/adr/*.md`, `/benchmarks/epoll-et-vs-lt.md`, `.github/workflows/ci.yml`, `/tools/sniffer`, `/tools/traceroute`.

### Timeline
MVP blocking L4 proxy (1.5 wks) → epoll + L7 parsing (3 wks) → health checking + companion tools (2 wks) → testing/benchmarks/docs (2 wks). Total ~8 weeks.

### Difficulty Ratings
Engineering: 9/10 · System Design: 7/10 · Networking/Linux depth: 10/10 · Deployment: 6/10 · Resume Value: 9/10 (very strong specifically for Nokia/telecom/networking-equipment companies) · Uniqueness: 9/10 · Time: Medium-High

---

## FLAGSHIP 9 — Vertex: Distributed Relational Query Engine with ACID Transactions

### Elevator Pitch
A small SQL database from scratch: your own storage engine, your own query parser/planner/optimizer, and — the genuinely hard part — your own transaction manager implementing real ACID guarantees (MVCC-based isolation, write-ahead logging for durability, two-phase commit across shards for distributed transactions).

### Problem Statement
Silo (Flagship 4) is a key-value storage engine; it has no query language, no transactions, no joins. This project is the other half of "database internals" — the part interviewers actually mean when they ask "how does a database guarantee consistency," which is a completely different (and harder) problem than storage. Almost no student project attempts real transaction semantics because it's genuinely difficult, which is exactly why it's high-signal.

### Resume Description
- Built a SQL query engine from scratch supporting a subset of SQL (SELECT/JOIN/WHERE/GROUP BY/INSERT/UPDATE/DELETE) with a hand-written lexer, recursive-descent parser, and a cost-based query planner choosing between index scan and full scan
- Implemented MVCC (multi-version concurrency control) for snapshot-isolation transactions, allowing concurrent readers and writers without blocking, verified against classic anomalies (dirty read, non-repeatable read, phantom read) with a targeted test suite
- Built a write-ahead log (WAL) with crash-recovery replay, guaranteeing durability verified by a kill--9-mid-write chaos test that always recovers to a consistent state
- Extended the engine to a distributed setting: sharded tables across N nodes with a two-phase commit coordinator for cross-shard transactions, documenting the trade-off against eventual-consistency alternatives
- Benchmarked against SQLite/Postgres on equivalent workloads (TPC-C-lite subset), documenting where the from-scratch engine is competitive and where it isn't, with honest numbers

### Technical Design
**Storage layer:** reuse/adapt the LSM-tree or a B+-tree (a good excuse to implement the *other* classic storage structure, complementing Silo's LSM-tree — document the trade-off: B+-trees are read-optimized and good for range queries, LSM-trees are write-optimized).

**Query layer:** lexer → recursive-descent parser → AST → logical plan → cost-based physical plan (choose index scan vs. full table scan based on estimated selectivity) → execution via a Volcano-style iterator model (`next()` pull-based operators — the same execution model real databases use).

**Transaction layer (the core of this project):** each transaction gets a monotonic timestamp; MVCC keeps multiple versions of each row tagged with the writing transaction's timestamp, so readers see a consistent snapshot without locking writers. Write conflicts are detected at commit time (optimistic concurrency control) or via row-level locks (pick one, document the trade-off). Durability comes from a WAL: every write is appended to the log and fsynced before being acknowledged, and on crash-restart the engine replays the log to reconstruct state.

**Distributed extension:** tables are sharded by a hash or range key across N nodes; a transaction touching multiple shards goes through a two-phase commit coordinator (prepare phase asks all shards to lock and stage the write; commit phase tells them to finalize) — explicitly documenting 2PC's blocking-coordinator weakness and how a real system (e.g., Spanner) solves it differently (TrueTime, Paxos groups) is a strong "I know the state of the art beyond what I built" signal.

### Architecture Diagram (text)
```
 SQL text
    │
    ▼
 Lexer → Parser → AST
    │
    ▼
 Logical Planner (cost estimation) → Physical Plan (Volcano iterator tree)
    │
    ▼
 Execution Engine ──▶ Transaction Manager (MVCC, timestamp ordering)
    │                          │
    ▼                          ▼
 Storage Engine (B+-tree)   Write-Ahead Log (durability, crash replay)
    │
    ▼
 [Distributed mode] Shard Router ──▶ 2PC Coordinator ──▶ Shard Nodes (1..N)
```

### Feature Roadmap
Beginner: single-table SELECT/INSERT with a simple B+-tree, no transactions (autocommit only). Intermediate: multi-table JOIN, WHERE/GROUP BY, WAL + crash recovery. Advanced: full MVCC snapshot isolation, cost-based planner choosing index vs. scan. Stretch: sharding + two-phase commit for distributed transactions. Research-grade: implement a second isolation level (serializable via SSI — serializable snapshot isolation) and benchmark the throughput cost of stronger isolation.

### Production Engineering
AuthN/Z (per-table grants, a minimal `GRANT`/`REVOKE` implementation — genuine RBAC at the database layer); rate limiting on query execution (protect against runaway queries); caching (buffer pool for hot pages, with an LRU/clock eviction policy — a real database component, not a bolt-on cache); retries (client-side retry on transaction-conflict abort, with exponential backoff); load balancing (read-replica routing for read-only queries in the distributed mode); observability (query latency histograms, lock-wait time, WAL fsync latency as first-class metrics); secrets/config (connection auth via a config file, not hardcoded); backups/DR (WAL-based point-in-time recovery — restore to any timestamp by replaying the log); horizontal scaling (the sharding/2PC extension); security (SQL injection is architecturally impossible here since you control the entire parser — a genuinely interesting point to make: "how do you prevent SQL injection when you wrote the SQL parser yourself?").

### Deployment
Single-binary engine, runs via Docker Compose for the distributed multi-shard demo → Kubernetes StatefulSet per shard for the distributed mode → Terraform → GitHub Actions (build/test/benchmark-on-PR) → reverse proxy not typically needed for a DB wire protocol, but document a connection-pooler (PgBouncer-style) as a stretch goal → monitoring via Grafana on the exposed metrics.

### Testing
Unit: parser against a SQL grammar test corpus, B+-tree correctness under random insert/delete/split/merge. Integration: multi-statement transaction round-trips. Contract: wire-protocol tests if you implement a client-server protocol (vs. embedded mode). **Isolation-anomaly test suite (the centerpiece):** deliberately construct dirty-read, non-repeatable-read, phantom-read, and lost-update scenarios and assert your MVCC implementation prevents each one it claims to prevent. Load/stress: a TPC-C-lite benchmark (new-order/payment transaction mix) measuring transactions/sec. Chaos: `kill -9` the engine mid-write-burst, restart, verify WAL replay recovers to the last committed state with zero corruption, repeated 50+ times.

### Performance Goals
1K+ simple transactions/sec single-node; documented, honest comparison against SQLite on the same hardware/workload (expect to be slower — the value is the benchmark methodology and the explanation, not beating SQLite); crash recovery completes and is correct 100% of 50+ repeated kill tests; 2PC cross-shard transaction latency documented and compared to single-shard latency.

### Learning Outcomes
ACID semantics from first principles, MVCC and snapshot isolation, write-ahead logging and crash recovery, cost-based query optimization, two-phase commit and its failure modes — this is the deepest possible demonstration of "database internals" short of contributing to Postgres itself.

### Interview Questions
- Beginner: "What does the 'I' in ACID actually guarantee?"
- Mid: "How does MVCC let readers and writers proceed concurrently without locking each other out?"
- Senior: "Walk me through exactly what your WAL replay does after a crash — how do you know where to resume from?"
- Staff: "Two-phase commit blocks if the coordinator dies mid-protocol — how does Spanner avoid this, and what would it take to get there from your design?"

### GitHub Structure
`README.md` with a demo of the isolation-anomaly test suite passing, `/docs/adr/*.md` (esp. an ADR on OCC vs. locking, and on 2PC vs. alternatives), `/docs/isolation-levels.md`, `/benchmarks/tpcc-lite.md`, `.github/workflows/ci.yml`, `/tests/anomalies/*` (the anomaly tests, clearly separated as the project's centerpiece).

### Timeline
MVP single-table engine (2 wks) → multi-table + WAL (3 wks) → MVCC transactions (3 wks) → distributed sharding + 2PC (3 wks) → benchmarks/docs (1–2 wks). Total ~10–12 weeks.

### Difficulty Ratings
Engineering: 10/10 · System Design: 9/10 · Database internals: 10/10 · Deployment: 6/10 · Resume Value: 10/10 · Placement Impact: 9/10 · Uniqueness: 9/10 · Time: High

---

## FLAGSHIP 10 — Scout: Distributed Search Engine

### Elevator Pitch
A search engine from scratch — your own inverted index, your own ranking function (BM25, then a learned re-ranker), and horizontal sharding across nodes with a scatter-gather query coordinator — indexing a real corpus (e.g., Wikipedia dump or a large open dataset) at a scale that actually requires the distributed design, not a toy 1000-document demo.

### Problem Statement
"Search engines" is explicitly listed in your brief's skill list and covered by none of the other nine projects. Full-text search underlies e-commerce, log analysis, documentation search, and countless internal tools — and almost every "search" student project is a thin wrapper calling Elasticsearch, which demonstrates API fluency, not search engineering. Building the index and ranking function yourself is the differentiator.

### Resume Description
- Built a distributed full-text search engine from scratch, including a custom inverted-index structure (posting lists with skip pointers for fast intersection), tokenization/stemming pipeline, and BM25 ranking, indexing a 5M+ document corpus
- Designed horizontal sharding of the index across N nodes with a scatter-gather query coordinator, merging per-shard top-K results into a globally ranked result set
- Implemented index compression (variable-byte or delta encoding of posting lists) reducing on-disk index size by 60%+, benchmarked against the uncompressed baseline
- Added a learned re-ranking stage (a small learning-to-rank model) on top of BM25 candidates, measuring NDCG improvement on a held-out relevance-labeled query set
- Load-tested the query path to sub-100ms p99 latency at 5M+ documents across a 4-shard cluster, with query-time caching for hot terms

### Technical Design
**Indexing pipeline:** documents → tokenizer (handle Unicode normalization, casing) → stemmer (Porter stemmer or similar) → stopword removal → inverted index build (term → sorted list of document IDs + positions, for phrase queries) → posting-list compression (delta-encode doc IDs, variable-byte encode) → index segments flushed to disk, periodically merged (borrowing the LSM-style "memtable + flush + compaction" pattern from Silo, but applied to postings instead of KV pairs — a nice cross-project callback to mention in interviews).

**Ranking:** BM25 as the base relevance score (term frequency, inverse document frequency, document-length normalization); optionally layer a learned re-ranker (gradient-boosted trees or a small neural model) on the top-N BM25 candidates using labeled query-document relevance judgments, since re-ranking a small candidate set is far cheaper than learning-to-rank over the whole corpus — a real production pattern worth stating explicitly.

**Distributed query path:** the corpus is sharded (by doc-ID hash) across N index nodes; a coordinator broadcasts the query to all shards, each returns its local top-K with scores, and the coordinator merges (a k-way merge, not just concatenate-and-sort) into a global top-K — this scatter-gather pattern is the same shape used by Elasticsearch/Solr internally, and building it yourself means you can discuss the trade-offs (network overhead, tail latency from the slowest shard) from experience.

### Architecture Diagram (text)
```
 Documents ──▶ Tokenizer/Stemmer ──▶ Inverted Index Builder
                                            │
                                            ▼
                                 Posting Lists (compressed, per-shard)
                                            │
              ┌─────────────────────────────┼─────────────────────────────┐
              ▼                             ▼                             ▼
        Shard Node 1                  Shard Node 2                  Shard Node N
        (local BM25 top-K)            (local BM25 top-K)            (local BM25 top-K)
              └─────────────────────────────┼─────────────────────────────┘
                                            ▼
                             Query Coordinator (scatter-gather, k-way merge)
                                            │
                                            ▼
                          Optional Learned Re-Ranker (top-N candidates)
                                            │
                                            ▼
                                     Ranked Results
```

### Feature Roadmap
Beginner: single-node inverted index, boolean AND/OR queries. Intermediate: BM25 ranking, phrase queries (using term positions), posting-list compression. Advanced: sharding across N nodes with scatter-gather coordination. Stretch: learned re-ranking stage, typo-tolerant fuzzy search (edit-distance-based query expansion), faceted search/filtering. Research-grade: approximate nearest-neighbor vector search layered alongside BM25 for a hybrid lexical+semantic search mode (a genuine callback to LedgerLens's retrieval layer, worth mentioning as cross-project synergy).

### Production Engineering
AuthN/Z on the indexing API (only authorized services can add/delete documents) and optionally per-document ACLs on query results (filter results a user isn't authorized to see — a real, often-botched requirement in enterprise search); rate limiting on query volume per client; caching (hot-query result cache, hot-term posting-list cache); retries/circuit breaker (coordinator tolerates a shard timing out and returns partial results rather than failing the whole query — document this explicitly as a deliberate availability-over-completeness trade-off); load balancing across replica shards for read scaling; observability (query latency broken down by scatter-gather phase, indexing throughput, index size over time); feature flags (toggle the learned re-ranker on/off per query for A/B testing — a real search-relevance engineering pattern); secrets/config management; backups (index segment snapshotting); horizontal scaling (add shards, rebalance — document your rebalancing strategy and its cost); security (query injection/DoS via pathological queries, e.g., extremely common terms causing huge posting-list intersections — rate-limit or reject).

### Deployment
Docker Compose (multi-shard local cluster) → Kubernetes (each shard as a StatefulSet with persistent volume for its index segments) → Terraform → GitHub Actions (index-build benchmark runs on PR to catch regressions) → NGINX/Envoy in front of the coordinator → HTTPS/monitoring/alerting on query-latency SLO breaches.

### Testing
Unit: tokenizer/stemmer correctness on edge cases (Unicode, punctuation), posting-list compression round-trip (compress→decompress→identical), BM25 scoring against hand-computed expected values. Integration: full index-then-query round trip. Contract: query API schema tests. E2E: index a known small corpus with known relevant documents for a query, assert they rank in the expected order. Load: query throughput/latency at 5M+ documents across shards using a real query-log replay (not synthetic random queries — use something like the MS MARCO query set for a realistic, citable benchmark). Chaos: kill one shard mid-query-storm, verify the coordinator degrades gracefully (partial results + a documented flag indicating incompleteness) instead of failing entirely.

### Performance Goals
p99 query latency <100ms at 5M+ documents across 4 shards; index-build throughput documented (docs/sec); posting-list compression achieving 60%+ size reduction vs. uncompressed baseline; NDCG@10 improvement documented and quantified if the learned re-ranker stretch goal is built.

### Learning Outcomes
Inverted-index construction and compression, BM25 and learning-to-rank fundamentals, scatter-gather distributed query patterns, and the general information-retrieval theory (precision/recall/NDCG) that underlies every real-world search product — genuinely rare depth in a student portfolio, and a strong complement to LedgerLens's retrieval layer since you'll be able to speak to *both* classical IR and modern dense/LLM-based retrieval in the same interview.

### Interview Questions
- Beginner: "What's an inverted index and why not just grep every document at query time?"
- Mid: "How does BM25 differ from raw term frequency, and why does document-length normalization matter?"
- Senior: "Your coordinator does scatter-gather across 4 shards — what happens to your p99 latency, and why is p99 dominated by the *slowest* shard, not the average?"
- Staff: "How would you redesign this to support real-time index updates (documents changing every second) instead of batch rebuilds?"

### GitHub Structure
`README.md` with a live query demo (search UI showing ranked results + latency breakdown), `/docs/adr/*.md` (esp. BM25-vs-learned-ranking, sharding strategy), `/docs/ranking.md` explaining the scoring math, `/benchmarks/query-latency.md` and `/benchmarks/compression.md`, `.github/workflows/ci.yml`, `/eval/ndcg-results.md` if the learning-to-rank stretch goal is built.

### Timeline
MVP single-node boolean search (1.5 wks) → BM25 + compression (2 wks) → sharding + scatter-gather (2.5 wks) → learned re-ranking stretch + benchmarks/docs (2 wks). Total ~8 weeks.

### Difficulty Ratings
Engineering: 8/10 · System Design: 8/10 · Information retrieval depth: 9/10 · Deployment: 6/10 · Resume Value: 8/10 · Uniqueness: 8/10 · Time: Medium

---

## PART 5 — Updated Full Portfolio (10 Projects) and Build Order

With all ten, your portfolio now covers **every** skill category from the original brief with zero overlap between projects:

| # | Project | Skill Cluster | Fills |
|---|---|---|---|
| 1 | Quorum | Distributed systems, fault tolerance | Consensus, scheduling, chaos testing |
| 2 | LedgerLens | AI infra, RAG, LLMOps | Modern AI engineering |
| 3 | Ink | Concurrency, real-time systems | CRDTs, convergence, WebSocket scaling |
| 4 | Silo | Storage internals (KV/LSM) | Storage engine design |
| 5 | Wardn | Security, API gateway, LLM guardrails | App-layer networking, safety-in-production |
| 6 | Forge *(optional)* | Hardware/software co-design | Compilers, RISC-V, embedded |
| 7 | **Cellar** *(new)* | **Linux internals, OS** | Namespaces, cgroups, container runtimes |
| 8 | **Ferry** *(new)* | **Raw networking, sockets** | TCP/epoll, L4/L7 LB, packet-level tools |
| 9 | **Vertex** *(new)* | **Database internals (relational)** | ACID, MVCC, 2PC, query planning |
| 10 | **Scout** *(new)* | **Search/information retrieval** | Inverted indices, ranking, scatter-gather |

**Realistically, you cannot build all ten to full depth in one final year alongside coursework and interview prep.** My honest recommendation: pick a **core 5–6**, not all 10. Given your emphasis on Linux/bash and companies like Nokia specifically, I'd bias the selection toward the systems-heavy side:

**Recommended final set (if you must cut down): Quorum, Cellar, Ferry, Vertex or Silo (pick one storage project, not both), LedgerLens.** That's five projects spanning distributed systems, Linux/OS internals, raw networking, database internals, and modern AI — which is about as complete a systems-engineering story as a final-year portfolio can tell, and it directly targets Nokia-style Linux/networking depth without dropping AI-engineering relevance.

If you have the time budget for 6–7, add Ink (concurrency) and/or Wardn (security/API layer) back in. Scout and Forge are the two most reasonable to drop first if time runs short — both are excellent but the least likely to come up in a Nokia-style interview loop compared to Cellar/Ferry.

### Updated Suggested Build Order (systems-first, given your Nokia/Linux emphasis)
1. **Ferry** (raw sockets — foundational Linux/C skills you'll reuse everywhere else)
2. **Cellar** (namespaces/cgroups — builds directly on Ferry's Linux fluency)
3. **Quorum** (distributed orchestration — can even use Cellar's isolation primitives to sandbox job execution, a nice cross-project link)
4. **Vertex or Silo** (pick one storage/database project based on whether you want relational-transaction depth or LSM/storage-engine depth)
5. **LedgerLens** (closest to interview season, keep the AI-tooling stack current)
6. *(If time remains)* Ink, Wardn, Scout, Forge — in that order of marginal resume value for your stated target companies.
