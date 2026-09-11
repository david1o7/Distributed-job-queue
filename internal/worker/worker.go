package worker

import (
	"context"
	"time"

	"distributed-job-system/internal/broker"
	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/logger"
	"distributed-job-system/internal/metrics"
	"distributed-job-system/internal/retry"
)

type Worker struct {
	ID         int
	Queue      broker.Queue
	Store      broker.JobStore
	MaxRetries int
}

func NewWorker(id int, q broker.Queue, store broker.JobStore, maxRetries int) *Worker {
	return &Worker{
		ID:         id,
		Queue:      q,
		Store:      store,
		MaxRetries: maxRetries,
	}
}

func (w *Worker) Start(ctx context.Context, registry *Registry, timeOut time.Duration) {
	logger.Log.Info(
		"Worker started", 
		"worker", w.ID,
	)

	for {
		select {
		case <-ctx.Done():
			logger.Log.Info(
				"Worker shutting Down!", 
				"worker", w.ID,
			)
			return
		default:
			job, err := w.Queue.Claim(ctx, timeOut)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Log.Error(
					"Failed to claim job", 
					"worker", w.ID, 
					"error", err,
				)
				time.Sleep(1 * time.Second)
				continue
			}

			if existing, gerr := w.Store.Get(ctx, job.ID); gerr != nil || existing == nil {
				_ = w.Store.Save(ctx, *job)
			}

			received := *job
			received.WorkerID = w.ID
			received.StartedAt = time.Now().UTC()
			received.FinishedAt = time.Time{}
			received.MaxRetries = w.MaxRetries
			received.Status = jobs.StatusProcessing

			if err = w.Store.Save(ctx, received); err != nil {
				logger.Log.Error(
					"Failed to Save job", 
					"Job ID", received.ID, 
					"error", err,
				)
				_ = w.Queue.Nack(ctx, received)
				continue
			}

			metrics.JobsProcessing.Inc()

			if lim, ok := w.Queue.(broker.ConcurrencyLimiter); ok {
				limit := GetConcurrencyLimit(received.Type)
				acquired, aerr := lim.AcquireConcurrency(ctx, received.Type, limit)
				if aerr != nil {
					logger.Log.Error(
						"Failed to acquire concurrency slot",
						"job", received.ID, 
						"type", received.Type, 
						"error", aerr,
					)
					_ = w.Queue.Nack(ctx, received)
					continue
				}
				if !acquired {
					metrics.JobsConcurrencyLimited.Inc()
					logger.Log.Info(
						"Concurrency limit reached, requeueing",
						"worker", w.ID, 
						"job", received.ID, 
						"type", received.Type, 
						"limit", limit,
					)
					_ = w.Queue.Nack(ctx, received)
					time.Sleep(100 * time.Millisecond)
					continue
				}
				defer func(jobType string) {
					if err := lim.ReleaseConcurrency(context.Background(), jobType); err != nil {
						logger.Log.Error("Failed to release concurrency slot",
							"job", received.ID, "type", jobType, "error", err)
					}
				}(received.Type)
			}

			
			if received.IdempotencyKey != "" {
				if idemp, ok := w.Queue.(broker.IdempotencyStore); ok {
					fullKey := received.Type + ":" + received.IdempotencyKey
					processed, perr := idemp.IsProcessed(ctx, fullKey)
					if perr != nil {
						logger.Log.Error(
							"Failed to check idempotency key",
							"job", received.ID, 
							"key", fullKey, 
							"error", perr,
						)
						_ = w.Queue.Nack(ctx, received)
						continue
					}
					if processed {
						logger.Log.Info(
							"Skipping already processed job (idempotent)",
							"worker", w.ID, 
							"job", received.ID, 
							"idempotency_key", received.IdempotencyKey,
						)
						received.Status = jobs.StatusCompleted
						received.FinishedAt = time.Now().UTC()
						_ = w.Store.Save(ctx, received)
						_ = w.Queue.ACK(ctx, received.ID)
						metrics.JobsCompleted.Inc()
						continue
					}
				}
			}

			hbCtx, hbCancel := context.WithCancel(ctx)
			if _, ok := w.Queue.(broker.VisibilityExtender); ok {
				go w.heartbeat(hbCtx, received.ID, timeOut)
			}

			start := time.Now()
			execerr := registry.Execute(ctx, received)
			hbCancel()

			duration := time.Since(start).Seconds()
			if duration < 0 {
				duration = 0
			}

			if execerr != nil {
				logger.Log.Error(
					"Job execution failed",
					"worker", w.ID, 
					"job", received.ID, 
					"error", execerr,
				)

				received.RetryCount++

				if received.RetryCount < w.MaxRetries {
					delay := retry.CalculateBackOff(received.RetryCount)
					received.NextRetry = time.Now().UTC().Add(delay)
					received.Status = jobs.StatusRetrying
					_ = w.Store.Save(ctx, received)

					metrics.JobsRetried.Inc()
					metrics.JobsScheduled.Inc()
					metrics.JobDuration.WithLabelValues(received.Type, "retried").Observe(duration)

					logger.Log.Warn(
						"Job failed, scheduling retry",
						"worker", w.ID,
						"job", received.ID,
						"retry_count", received.RetryCount,
						"retry_after", delay)

					if err := w.Queue.Schedule(ctx, received, delay); err != nil {
						logger.Log.Error(
							"Failed to schedule delayed retry",
							"job", received.ID, 
							"error", err,
						)
						_ = w.Queue.Nack(ctx, received)
					}
					continue
				}

				received.Status = jobs.StatusFailed
				received.FinishedAt = time.Now().UTC()
				metrics.JobsFailed.Inc()
				metrics.JobDuration.WithLabelValues(received.Type, "failed").Observe(duration)

				if err := w.Store.Save(ctx, received); err != nil {
					logger.Log.Error(
						"Failed to Save job", 
						"Job ID", received.ID, 
						"error", err,
					)
				}

				deadJob := jobs.DeadJob{
					Job:           received,
					FailureReason: execerr.Error(),
					FailedAt:      time.Now().UTC(),
				}
				if dl, ok := w.Queue.(broker.DeadLetter); ok {
					if err := dl.MoveToDeadLetter(ctx, deadJob); err != nil {
						logger.Log.Error(
							"failed moving job to DLQ", 
							"job", received.ID, 
							"error", err,
						)
					} else {
						metrics.JobsDeadLetter.Inc()
						logger.Log.Error(
							"Job moved to dead letter queue",
							"worker", w.ID, 
							"job", received.ID,
						)
					}
				}
				_ = w.Queue.ACK(ctx, received.ID)
				continue
			}

			
			received.Status = jobs.StatusCompleted
			received.FinishedAt = time.Now().UTC()
			metrics.JobsCompleted.Inc()
			metrics.JobDuration.WithLabelValues(received.Type, "completed").Observe(duration)
			_ = w.Store.Save(ctx, received)

			if received.IdempotencyKey != "" {
				if idemp, ok := w.Queue.(broker.IdempotencyStore); ok {
					fullKey := received.Type + ":" + received.IdempotencyKey
					if err := idemp.MarkProcessed(ctx, fullKey); err != nil {
						logger.Log.Error(
							"Failed to mark job as processed",
							"job", received.ID, 
							"key", fullKey, 
							"error", err)
					}
				}
			}

			if err = w.Queue.ACK(ctx, received.ID); err != nil {
				logger.Log.Error(
					"Failed to Ack job", 
					"Job ID", received.ID, 
					"error", err,
				)
			}
			logger.Log.Info(
				"job processed successful",
				"worker", w.ID, 
				"job", received.ID, 
				"Status", received.Status,
			)
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context, jobID string, timeOut time.Duration) {
	ext, ok := w.Queue.(broker.VisibilityExtender)
	if !ok {
		return
	}

	interval := timeOut / 3
	if interval < 2*time.Second {
		interval = 2 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := ext.ExtendVisibility(ctx, jobID, timeOut); err != nil {
				logger.Log.Error(
					"Heartbeat failed",
					"worker", w.ID, 
					"job", jobID, 
					"error", err,
				)
				continue
			}
			metrics.JobsHeartbeat.Inc()
			logger.Log.Debug(
			"Heartbeat sent", 
			"worker", w.ID, 
			"job", jobID,
		)
		}
	}
}