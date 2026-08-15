# Changelog

# Changelog

## v0.4.0
- Added Delayed Queue using Redis Sorted Set (`jobs:delayed`)
- Failed jobs are now scheduled with exponential backoff instead of immediate requeue
- Added background Delayed Job Mover
- Added `Schedule` and `MoveReadyDelayedJobs` methods
- New metrics: `jobs_scheduled_total`, `jobs_delayed_moved_total`
- Improved separation between processing, delayed, and ready states

## v0.3.0
- Implemented Visibility Timeout + Acknowledgements (Claim / Ack / Nack)
- Added background Reaper for expired in-flight jobs
- Jobs are no longer lost if a worker crashes after claiming
- Moved from at-most-once toward at-least-once delivery
- Exposed Prometheus metrics endpoint (`/metrics`)
- Added `jobs_reaped_total` metric
- Fixed worker ID assignment bug
- Fixed graceful shutdown (double channel receive)
- Fixed multiple `errcheck` issues in HTTP handlers
- Added unit tests for core queue behaviour
- Added GitHub Actions CI pipeline (test, lint, build)

## v0.2.0
- Added worker pool
- Added configurable worker count
- Added graceful shutdown
- Added structured logging
- Added retry count tracking
- Added configurable max retries
- Requeue failed jobs
- Added structured slog logging
- Simulated job processing failures

## v0.1.0
- Initial Redis queue
- Producer
- Single worker