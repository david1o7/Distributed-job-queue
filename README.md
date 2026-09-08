## README.md

# Distributed Job Queue (DJ)

**DJ** is a production-inspired distributed task orchestration system built with **Go**, **Redis**, **Lua**, **Docker**, **Prometheus**, and **Grafana**.  
This project exists to move beyond basic application development and deep-dive into the architectural mechanics of message brokers, distributed state, and event streaming.

---

## Demo
[System Design](docs/system,%20gen.jpeg)

## The core problem

How do independent worker processes pull work from a shared hub **without**:

- two workers claiming the same job,
- losing work when a process dies mid-execution,
- or turning retries into a thundering herd?

DJ treats that as a **systems** problem, not a CRUD problem.

### Redis execution model (current production path)

Work moves through explicit Redis structures:

| Structure | Role |
|-----------|------|
| **Priority lists** `jobs:high` / `jobs:default` / `jobs:low` | Ready work (`LPUSH` / `BRPOP`) |
| **Sorted set** `jobs:processing` | In-flight **leases** (score = visibility deadline) |
| **Sorted set** `jobs:delayed` | Retry-after timestamps |
| **List** `dead_job` | Dead-letter isolation |
| **String** `job:{id}` | Authoritative job document |
| **Lua scripts** | Multi-key mutations without app-level locks |

**Claim** takes a lease: pop from the highest non-empty priority list, then record the job id in `jobs:processing` with a deadline.  
**ACK** drops the lease. **Nack** returns the job to its priority list.

### Heartbeat pattern

A fixed visibility timeout is not a max runtime. It is a **proof-of-life window**.

While a handler runs, a background goroutine periodically calls **`ExtendVisibility`**, pushing the processing score forward.  

- Worker healthy → lease renews → reaper stays away.  
- Worker crashes → heartbeats stop → score expires → **reaper** requeues the job (**at-least-once** recovery).

Atomic **Lua** scripts protect paths that must not tear: schedule (delayed + leave processing), extend-if-present, concurrency acquire, DLQ replay (`LREM` + `LPUSH`).

---

## Multi-broker strategy

DJ is evolving behind a clean **`Broker` interface** so the worker engine does not hard-code Redis forever.

| Backend | Role in DJ | Strength | Cost |
|---------|------------|----------|------|
| **Redis** (current) | Lease-style DIY broker | Fast, explicit control of visibility, delay, priority | You own reaper/heartbeat semantics |
| **RabbitMQ** (in progress) | Push/pull queues, ACK/Nack, DLX | Native ack, routing, consumer prefetch/backpressure | Ops + connection topology; delayed needs TTL/DLX or plugin |
| **Kafka** (in progress) | Partitioned log | Throughput, replay, consumer groups | Different model: offsets ≠ Redis leases; delay/DLQ via topics |

**Redis strategy:** in-memory atomic distribution, priority lists, lease ZSET, Lua invariants.  
**RabbitMQ strategy:** exchange/queue routing, manual ack, prefetch as concurrency, DLX for poison messages.  
**Kafka strategy:** produce to topics, consumer groups, commit on success, retry/DLQ topics for failure; job state lives outside the log when needed.

Hot-swapping is the goal: same worker loop, different `Broker` implementation selected by config.

---

## Advanced production patterns

- **Exponential backoff & DLQ** — failed jobs leave the worker via **delayed ZSET**; persistent failures isolate into **dead_job** with replay.
- **Priority queues** — strict **high > default > low** via multi-key `BRPOP` order.
- **Idempotency keys** — optional processed set to skip duplicate side effects.
- **Per-type concurrency limits** — Redis semaphore (Lua) so expensive job types cannot stampede downstreams.
- **Graceful shutdown** — root `context` cancel stops claim loops; HTTP server shuts down cleanly.
- **Fail-fast config** — env + optional JSON; illegal `WORKER_COUNT` / timeouts abort before serving.
- **Observability** — `slog` (text in dev, JSON in prod), **request_id** / **job_id** correlation, Prometheus histograms/gauges, Grafana-ready Compose stack.
- **Admin surface** — `/stats`, `/delayed-jobs`, paged `/dead-jobs`, replay, `/health`, `/ready`.

---

## Project anatomy

```text
cmd/server/           # process entry, reaper / mover / metrics sampler
internal/
  broker/             # Broker interface (Redis | RabbitMQ | Kafka)
  drivers/            # concrete broker implementations (in progress)
  queue/              # Redis-backed queue + Lua (current default)
  worker/             # pool, registry, handlers, limits, heartbeats
  jobs/               # Job / DeadJob / Priority model
  producer/           # HTTP enqueue
  handlers/           # status, DLQ, admin, health
  config/             # fail-fast configuration
  logger/             # slog + context correlation
  metrics/            # Prometheus
  retry/              # backoff
deploy/               # prometheus + grafana provisioning
Dockerfile            # distroless multi-stage
docker-compose.yml
```

---

## Hard lesson: priorities vs legacy tests

**Struggle.** Priority lists replaced the single `jobs` key. Production `Push`/`Claim`/`Nack`/reaper/mover all moved to `jobs:high|default|low`.  

**Breakage.** The test suite still asserted `LLEN jobs`. Suites went red even when Claim worked. Empty priority normalized the wrong way; producer type maps ignored explicit `priority`; `metrics.Init()` double-registered in `cmd/server` tests; orphan reaper paths nil-dereferenced missing `job:{id}`.

**Resolution.**

- `readyLen` / `Stats.MainDepth` instead of legacy `LLEN jobs`
- `NormalizePriority` → **default** (not low)
- Explicit API priority wins over type map
- `sync.Once` around Prometheus register
- Orphan processing members: **ZREM only**, never marshal a nil job
- Fixtures set `Priority` when destination list matters

**Trade-off kept:** strict priority can starve low under constant high load—acceptable for the learning model; fairness is a later broker concern.

---

## Quick start

```bash
export REDIS_ADDR=localhost:6379
export WORKER_COUNT=3
export LOG_FORMAT=text

go run ./cmd/server
```

```bash
curl -s -X POST localhost:8080/jobs \
  -H 'Content-Type: application/json' \
  -d '{"type":"print","priority":"high","payload":{"name":"DJ"}}'
```

```bash
docker compose up --build -d   # app + Redis + Prometheus + Grafana
```

```bash
go test ./... -count=1 -race
```

---

## Design guarantees

| Guarantee | Mechanism |
|-----------|-----------|
| At-least-once | Visibility lease + reaper |
| Long jobs | Heartbeat + ExtendVisibility |
| Non-blocking retries | Delayed ZSET + mover |
| Poison isolation | Max retries → DLQ |
| Atomic replay | Lua LREM + LPUSH |
| Priority scheduling | Multi-list BRPOP order |

**Not guaranteed:** exactly-once. Use idempotent handlers + idempotency keys.

---

## Roadmap

- [x] Leases, heartbeats, delayed retries, DLQ, priorities  
- [x] Metrics, Compose observability, admin endpoints  
- [ ] `Broker` interface extraction  
- [ ] RabbitMQ driver (ACK/Nack/DLX)  
- [ ] Kafka driver (topics + retry/DLQ topics)  
- [ ] Optional Postgres job history  

---

DJ is a lab for distributed systems practice: visibility, failure, concurrency, and broker trade-offs—implemented in code you can read, break, and fix.
