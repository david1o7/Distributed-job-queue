# Distributed Job Queue

> **Version:** `v0.2.0`

A production-inspired distributed job queue built in Go.

This project is my journey into distributed systems and backend engineering. Rather than jumping straight into Kafka or RabbitMQ, I'm building the core concepts from scratch to understand **why** production systems are designed the way they are.

The goal isn't to clone an existing message broker.

The goal is to understand the engineering decisions behind them.

---

## Tech Stack

- Go
- Redis
- Docker
- Context API
- Goroutines
- Worker Pools

Future versions will introduce:

- RabbitMQ
- Kafka
- Prometheus
- Grafana
- PostgreSQL
- Docker Compose
- Kubernetes

---

# Current Architecture

```text
                    Producer
                       │
                       ▼
               Redis Job Queue
                       │
      ┌────────────────┼────────────────┐
      ▼                ▼                ▼
   Worker-1         Worker-2         Worker-3
```

---

# Current Features

- Produce asynchronous jobs
- Redis-backed queue
- Worker pool
- Configurable worker count
- Concurrent job processing
- Graceful shutdown
- Context cancellation
- Structured worker logging
- Environment configuration

---

# Why I Built This

I wanted to understand what actually happens behind systems like:

- RabbitMQ
- Kafka
- AWS SQS
- BullMQ
- Sidekiq

Instead of treating them like magic, I'm rebuilding many of their ideas from scratch.

Every version of this project solves one new engineering problem.

---

# Current Version

## v0.2.0

This version introduces:

- Multiple concurrent workers
- Worker IDs
- Configurable worker pool
- Better logging
- Graceful shutdown
- Context propagation

---

# Trade-offs (Current)

Every design has trade-offs.

These are the ones I've intentionally accepted so far.

### Single Redis Queue

Every job enters one queue.

**Pros**

- Very simple
- Easy to debug
- Easy to reason about

**Cons**

- Long-running jobs can block smaller jobs.
- No prioritization.

---

### Redis Lists

Using `LPUSH` + `BRPOP`.

**Pros**

- Fast
- Lightweight
- Perfect for learning

**Cons**

- No acknowledgements.
- Jobs can be lost if a worker crashes after dequeuing.

---

### FIFO Processing

Jobs are processed in arrival order.

**Pros**

- Predictable
- Easy implementation

**Cons**

- Urgent jobs cannot skip the queue.

---

### Worker Pool

Multiple workers consume jobs concurrently.

**Pros**

- Higher throughput
- Better CPU utilization
- Scales easily

**Cons**

- Concurrent logs
- More difficult debugging
- Potential race conditions if shared state is introduced later

---

### Fire-and-Forget

The producer immediately returns after enqueueing.

**Pros**

- Fast API response

**Cons**

- Clients don't know when work finishes.

---

# Edge Cases Handled

- Multiple workers consuming simultaneously
- Empty queue
- Graceful server shutdown
- Context cancellation
- Configurable worker count
- Invalid worker count configuration
- Logging worker identity
- Worker waiting efficiently using blocking operations

---

# Edge Cases Not Yet Handled

These are planned for future releases.

- Retry mechanism
- Dead Letter Queue (DLQ)
- Worker crash recovery
- Job acknowledgements
- Delayed jobs
- Scheduled jobs
- Job priorities
- Queue persistence
- Idempotency
- Duplicate job detection
- Retry backoff
- Queue monitoring
- Failed job analytics
- Worker health monitoring
- Queue length metrics

---

# Roadmap

## v0.3.0

- Retry mechanism
- Maximum retry count
- Exponential backoff

---

## v0.4.0

- Dead Letter Queue
- Failure tracking
- Retry analytics

---

## v0.5.0

- Job priorities
- Multiple queues
- Priority scheduler

---

## v0.6.0

- Delayed jobs
- Scheduled execution

---

## v0.7.0

- Prometheus metrics
- Grafana dashboard
- Queue monitoring

---

## v0.8.0

- PostgreSQL persistence
- Job status API
- Job history

---

## v1.0.0

- RabbitMQ backend
- Message acknowledgements
- Reliable delivery

---

## v2.0.0

- Kafka integration
- Event streaming
- Horizontal scaling
- Docker Compose
- Kubernetes deployment

---

# Lessons Learned

So far this project has taught me about:

- Goroutines
- Worker pools
- Context propagation
- Concurrent processing
- Redis as a queue
- Graceful shutdown
- Distributed systems fundamentals
- Production trade-offs

---

# This Is Only The Beginning

This repository is intentionally versioned because I plan to continuously evolve it.

Rather than abandoning projects after they're "working", I want this to become a long-term engineering project where every release introduces another production concept.

The goal is to eventually build something that demonstrates:

- distributed systems
- observability
- reliability
- fault tolerance
- scalability
- production engineering

Version **0.2.0** is only the foundation.

There's a long roadmap ahead, and each version will bring this project one step closer to a production-grade distributed job system.

---

# Running

```bash
git clone https://github.com/YOUR_USERNAME/distributed-job-queue.git

cd distributed-job-queue

go mod download

go run ./cmd/server
```

---

# Future Vision

By the end of this project, I want to understand—not just use—the ideas behind modern distributed systems.

Whether it's RabbitMQ, Kafka, or cloud-native queue services, the goal is to build the intuition first and then apply those concepts to production technologies.