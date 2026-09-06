
# Distributed Job Queue

**Version:** `v1.0.0`  
**Stack:** Go · Redis · Docker · Prometheus · Grafana · slog · Lua

A production-inspired distributed job queue built from scratch in Go.

This is not a clone of SQS, RabbitMQ, or Sidekiq. It is an engineering sandbox for learning *why* those systems look the way they do—by implementing their core ideas one failure mode at a time.

---

## Table of Contents

- [Why this exists](#why-this-exists)
- [What it does today](#what-it-does-today)
- [Architecture](#architecture)
- [Request flow](#request-flow)
- [Quick start](#quick-start)
- [Configuration](#configuration)
- [HTTP API](#http-api)
- [Observability](#observability)
- [Testing](#testing)
- [Design guarantees](#design-guarantees)
- [Trade-offs](#trade-offs)
- [Struggles & debugging stories](#struggles--debugging-stories)
- [Insights from building v1](#insights-from-building-v1)
- [Roadmap](#roadmap)
- [Project layout](#project-layout)

---

## Why this exists

Systems like **AWS SQS**, **RabbitMQ**, **Kafka consumers**, **BullMQ**, and **Sidekiq** hide a lot of complexity:

- What happens if a worker dies mid-job?
- Why do queues need visibility timeouts?
- Why is “exactly-once” so hard?
- Why do retries need delay, not tight loops?
- Why is DLQ replay dangerous if it is not atomic?

This project rebuilds those ideas on **Redis + Go** so the trade-offs are visible in code, tests, and metrics—not only in vendor docs.

---

## What it does today

### Core queue

- Redis list as the main ready queue
- Concurrent worker pool (configurable)
- Job state store (`job:{id}`) separate from the queue
- Graceful shutdown via context + signal handling

### Reliability

- **Claim / ACK / Nack** with visibility timeout
- **Reaper** for crash recovery (expired in-flight jobs redelivered)
- **Heartbeats + ExtendVisibility** so long jobs are not stolen
- **Delayed retries** via sorted set + background mover (exponential backoff)
- **Dead Letter Queue** + replay
- **Lua-backed critical paths** (schedule, replay mutation, concurrency, visibility extend)

### Safety & control

- Optional **idempotency keys** (processed set)
- **Per-job-type concurrency limits**
- Fail-fast **config** (env + optional JSON file)
- **Liveness / readiness** endpoints (`/health`, `/ready`)

### Observability

- Structured logging (`slog`) — text in dev, JSON in prod
- **request_id** and **job_id** correlation
- Prometheus counters, histograms (duration, queue latency), gauges (depth / in-flight / delayed)
- Docker Compose stack: app + Redis + Prometheus + Grafana

### Quality

- Unit/integration tests with miniredis
- CI-friendly layout (test, lint, build)

---

## Architecture

```text
                    ┌──────────────┐
   POST /jobs       │  HTTP API    │     GET /jobs/{id}
   /dead-jobs       │  + /metrics  │     /health /ready
                    └──────┬───────┘
                           │
                           ▼
                    ┌──────────────┐
                    │ RedisQueue   │
                    │  Lua scripts │
                    └──────┬───────┘
                           │
        ┌──────────────────┼──────────────────┐
        ▼                  ▼                  ▼
   List: jobs      ZSET: processing     ZSET: delayed
   List: dead_job  String: job:{id}     SET: processed
                           │
                    ┌──────┴───────┐
                    │ Worker pool  │── heartbeat → ExtendVisibility
                    │ handlers     │
                    └──────────────┘
                           │
              Reaper · Delayed mover · Metrics sampler
```

**Idea:** the **queue** moves work; the **job store** holds truth about status, retries, timestamps, and worker ownership.

---

## Request flow

1. **Produce** — `POST /jobs` creates a job, `SaveJob`, `Push` onto `jobs`.
2. **Claim** — worker `BRPOP`s, adds id to `jobs:processing` with a deadline.
3. **Process** — status=`processing`, optional idempotency/concurrency checks, handler runs, heartbeats extend visibility.
4. **Outcome**
   - success → complete + ACK  
   - retryable fail → `Schedule` into `jobs:delayed` + leave processing  
   - max retries → DLQ + ACK  
5. **Background**
   - **Reaper** redelivers expired processing entries (crash recovery)
   - **Delayed mover** promotes due retries back to `jobs`
6. **Observe** — logs (`request_id` / `job_id`), `/metrics`, status API.

---

## Quick start

### Local (Go + Redis)

```bash
# Redis
redis-server

# App
export REDIS_ADDR=localhost:6379
export WORKER_COUNT=3
export MAX_RETRIES=3
export LOG_FORMAT=text
export LOG_LEVEL=debug

go mod download
go run ./cmd/server
```

### Docker Compose (app + Redis + Prometheus + Grafana)

```bash
docker compose up --build -d

curl http://localhost:8080/health
curl http://localhost:8080/ready
# Prometheus :9090  Grafana :3000 (admin/admin)
```

### Enqueue a job

```bash
curl -s -X POST http://localhost:8080/jobs \
  -H "Content-Type: application/json" \
  -H "X-Request-ID: demo-1" \
  -d '{
    "type": "print",
    "payload": {"name": "Grok"},
    "idempotency_key": "welcome-user-42"
  }'
```

---

## Configuration

Fail-fast load: **defaults → optional `config.json` → env (wins) → Validate()**.

| Variable | Meaning | Default |
|----------|---------|---------|
| `REDIS_ADDR` | Redis host:port | `localhost:6379` |
| `HTTP_ADDR` | Listen address | `:8080` |
| `WORKER_COUNT` | Concurrent workers | `1` |
| `MAX_RETRIES` | Retries before DLQ | `3` |
| `VISIBILITY_TIMEOUT` | In-flight lease | `30s` |
| `LOG_FORMAT` | `text` or `json` | `text` |
| `LOG_LEVEL` | slog level | `info` |
| `CONFIG_FILE` | Optional JSON path | `config.json` if present |

Bad config or unreachable Redis at startup aborts the process on purpose.

---

## HTTP API

| Method | Path | Purpose |
|--------|------|---------|
| `POST` | `/jobs` | Enqueue job |
| `GET` | `/jobs/{id}` | Job status / lifecycle fields |
| `GET` | `/dead-jobs` | List DLQ |
| `POST` | `/dead-jobs/{id}/replay` | Atomic-ish replay to main queue |
| `GET` | `/health` | Liveness |
| `GET` | `/ready` | Readiness (Redis ping) |
| `GET` | `/metrics` | Prometheus scrape |

Job JSON includes lifecycle fields such as `status`, `retry_count`, `started_at`, `finished_at`, `worker_id`, `next_retry`, and optional `idempotency_key`.

---

## Observability

**Logs**

- Dev: text handler  
- Prod: JSON handler  
- Correlation: `request_id` (HTTP middleware / `X-Request-ID`), `job_id`, `worker_id`

**Metrics (sample)**

- Counters: queued, processed, completed, retried, failed, DLQ, reaped, scheduled, heartbeats  
- Histograms: `job_duration_seconds`, `job_queue_latency_seconds`  
- Gauges: queue depth, in-flight, delayed, DLQ depth  

---

## Testing

```bash
go test ./... -count=1 -race
go test ./... -coverprofile=coverage.out
go tool cover -func=coverage.out | tail
```

Coverage focus areas:

- Claim / ACK / Nack / Reap (crash recovery)
- Delayed schedule + move
- ExtendVisibility
- DLQ replay (including double-replay)
- Config validation
- Logger context helpers
- HTTP handlers via `httptest`

---

## Design guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| At-least-once processing | Visibility timeout + reaper |
| Long-running jobs allowed | Heartbeat + `ExtendVisibility` |
| Retries do not block workers | Delayed sorted set + mover |
| Poison messages isolated | Max retries → DLQ |
| Replay mutation atomic | Lua `LREM` + `LPUSH` |
| Duplicate work mitigated | Idempotency keys + idempotent handlers |
| Safe startup | Config validation + optional Redis ping |

**Not guaranteed:** exactly-once execution. Crashes between side effects and ACK/MarkProcessed can still duplicate work. Handlers should be idempotent.

---

## Trade-offs

| Choice | Cost |
|--------|------|
| Redis list + ZSETs | Simple operationally; not a full broker |
| At-least-once | Requires idempotency for correctness |
| BRPOP + then processing ZADD | Tiny race; compensated by push-back on failure |
| Delayed mover polling (~2s) | Not millisecond-precise scheduling |
| DLQ as list | Replay scan O(N); fine while DLQ is small |
| Job state TTL (e.g. 24h) | History is not permanent unless exported |
| Single FIFO ready queue | No priorities / multi-tenant fairness yet |

These are intentional. Each one maps to a real broker feature worth learning next.

---

## Struggles & debugging stories

### 1. `GET /jobs/{id}` → “job not found”

**Symptom:** enqueue looked fine; status API missed the job.  
**Wrong first guess:** Redis persistence broken.  
**Actual issue:** queue payload vs job store lifecycle—IDs and `SaveJob` timing were inconsistent along the path.  
**Lesson:** separating **queue transport** from **job record** is powerful, but every path (claim, retry, DLQ, replay) must update the same source of truth.

### 2. Jobs failed permanently instead of retrying

**Symptom:** one failure → dead, no backoff.  
**Cause:** retry counter / branch logic and state transitions were wrong (including double-increment bugs along the way).  
**Lesson:** production retry systems are state machines. If status, `retry_count`, and “next place” (main vs delayed vs DLQ) disagree, the system lies to you.

### 3. Concurrent workers made logs unusable

**Symptom:** interleaved prints; impossible to follow one job.  
**Fix path:** structured logs, then **worker_id**, then **job_id** / **request_id** correlation.  
**Lesson:** concurrency does not only break correctness—it breaks observability first. If you cannot narrate one job’s life story, you cannot debug delivery guarantees.

### 4. Visibility timeout vs long jobs

**Symptom:** “healthy” slow handlers looked like crashes; reaper redelivered mid-flight.  
**Fix:** heartbeats + `ExtendVisibility`.  
**Lesson:** visibility timeout is not “max job duration.” It is “max time without proof of life.” That distinction is the heart of SQS-style leases.

### 5. Delayed retries used to block workers

**Old approach:** `sleep` / `select` inside the worker on failure.  
**Problem:** failed jobs parked goroutines; throughput collapsed under errors.  
**Fix:** `Schedule` into `jobs:delayed`, ACK/remove from processing, let a mover promote due work.  
**Lesson:** a worker’s job is to process, not to wait. Waiting belongs in the data plane (sorted sets), not in the thread pool.

### 6. DLQ replay races and `redis.Nil` tests

**Symptom:** replay flaky under concurrency; tests returned `redis.Nil` unexpectedly.  
**Causes mixed:** non-atomic LRANGE → LPUSH → LREM sequences; key naming mismatches; fragile ID matching.  
**Fix:** Lua script for **atomic remove-from-DLQ + push-to-main**, plus tests for success, missing id, and double replay.  
**Lesson:** if two writes define one business action (“replay”), they need one atomic boundary—or you do not control the failure modes.

### 7. Coverage as a truth serum

Large parts of `main`, handlers, and logger helpers sat at 0% until tests forced the issue.  
**Lesson:** features you cannot restart in a test are features you do not fully own. miniredis + httptest closed that gap for most of the queue and API surface.

---

## Insights from building v1

1. **Brokers are policy engines.** Redis structures are the easy part. The product is the policy: timeouts, retries, ACK rules, and what “failed” means.

2. **At-least-once is a product decision.** You buy crash safety and pay with duplicates. Exactly-once is usually “at-least-once + idempotent effects.”

3. **Background components are part of the API.** Reaper, delayed mover, and metric samplers are not ops afterthoughts—they define user-visible behavior.

4. **Lua is for invariants, not for everything.** Use it where torn writes hurt (schedule, replay, semaphores, extend-if-exists). Keep `BRPOP` in Go; scripts cannot block.

5. **Config and logging are reliability features.** Fail-fast config prevents running wrong topology. Correlated logs make delivery guarantees debuggable.

6. **Every shortcut becomes a curriculum.** List DLQ, FIFO-only, single node Redis—each limitation is a roadmap item with a clear “why.”

---

## Roadmap

**Done toward v1**

- Visibility timeout, ACK/Nack, reaper  
- Heartbeats / extend visibility  
- Delayed retries  
- DLQ + Lua replay hardening  
- Idempotency keys, concurrency limits  
- Metrics histograms/gauges, Compose observability  
- Structured correlated logging, fail-fast config  

**Next**

- Stronger multi-queue / priorities  
- Richer admin API (delayed listing, deeper stats)  
- Optional Postgres job history  
- gRPC surface  
- Deeper chaos tests (kill workers under load)

---

## Project layout

```text
cmd/server/          # process entry, background loops
internal/config/     # fail-fast config (env + file)
internal/queue/      # Redis queue, Lua, tests
internal/worker/     # pool, registry, handlers, limits
internal/handlers/   # HTTP handlers
internal/producer/   # enqueue API
internal/logger/     # slog init + context correlation
internal/metrics/    # Prometheus
internal/jobs/       # job / dead job types
internal/retry/      # backoff
deploy/              # prometheus / grafana provisioning
Dockerfile           # distroless multi-stage
docker-compose.yml
```

---

## Final thoughts

This repository is a lab for distributed systems practice.

The goal is not to out-feature SQS. The goal is to feel, in your own code, why visibility timeouts exist, why heartbeats matter, why retries must leave the worker, why DLQ replay must be atomic, and why observability is part of correctness.

Every version should teach one new way the system can fail—and one deliberate mechanism to survive it.
