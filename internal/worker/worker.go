package worker

import (
	"context"
	// "errors"
	// "math/rand"

	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/logger"
	"distributed-job-system/internal/metrics"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/retry"
	"time"
)

type Worker struct {
	ID         int
	Queue      *queue.RedisQueue
	MaxRetries int
}

func NewWorker(id int, q *queue.RedisQueue, maxRetries int) *Worker {

	return &Worker{
		ID:         id,
		Queue:      q,
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
					logger.Log.Error(
						"Background Context Error",
						"Err", ctx.Err(),
					)
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

			Recievedjob := *job
			Recievedjob.CreatedAt = time.Now()
			Recievedjob.WorkerID = w.ID
			Recievedjob.StartedAt = time.Now().UTC()
			Recievedjob.FinishedAt = time.Time{}
			Recievedjob.MaxRetries = w.MaxRetries

			if err = w.Queue.SaveJob(ctx, Recievedjob); err != nil {
				logger.Log.Error(
					"Failed to Save job",
					"Job ID", Recievedjob.ID,
					"error", err,
				)
				job = &Recievedjob
				_ = w.Queue.Nack(ctx, *job)
				continue
			}

			metrics.JobsProcessing.Inc()

			limit := GetConcurrencyLimit(Recievedjob.Type)
			acquired, err := w.Queue.AcquireConcurrency(ctx, Recievedjob.Type, limit)
			if err != nil {
				logger.Log.Error("Failed to acquire concurrency slot",
					"job", Recievedjob.ID,
					"type", Recievedjob.Type,
					"error", err,
				)
				_ = w.Queue.Nack(ctx, Recievedjob)
				continue
			}

			if !acquired {
				metrics.JobsConcurrencyLimited.Inc()

				logger.Log.Info("Concurrency limit reached, requeueing",
					"worker", w.ID,
					"job", Recievedjob.ID,
					"type", Recievedjob.Type,
					"limit", limit,
				)
				
				_ = w.Queue.Nack(ctx, Recievedjob)
				
				time.Sleep(100 * time.Millisecond)
				continue
			}

			
			defer func() {
				if err := w.Queue.ReleaseConcurrency(ctx, Recievedjob.Type); err != nil {
					logger.Log.Error("Failed to release concurrency slot",
						"job", Recievedjob.ID,
						"type", Recievedjob.Type,
						"error", err,
					)
				}
			}()

			if Recievedjob.IdempotencyKey != "" {
				
				fullKey := Recievedjob.Type + ":" + Recievedjob.IdempotencyKey

				processed, err := w.Queue.IsProcessed(ctx, fullKey)
				if err != nil {
					logger.Log.Error("Failed to check idempotency key",
						"job", Recievedjob.ID,
						"key", fullKey,
						"error", err,
					)
					
					_ = w.Queue.Nack(ctx, Recievedjob)
					continue
				}

				if processed {
					logger.Log.Info(
						"Skipping already processed job (idempotent)",
						"worker", w.ID,
						"job", Recievedjob.ID,
						"idempotency_key", Recievedjob.IdempotencyKey,
					)

					
					Recievedjob.Status = jobs.StatusCompleted
					_ = w.Queue.SaveJob(ctx, Recievedjob)
					_ = w.Queue.ACK(ctx, Recievedjob.ID)
					metrics.JobsCompleted.Inc()
					continue
				}
			}

			hbCtx, hbCancel := context.WithCancel(ctx)
			go w.heartbeat(hbCtx, Recievedjob.ID, timeOut)

			start := time.Now()
			execerr := registry.Execute(ctx, Recievedjob)

			hbCancel()

			duration := time.Since(start).Seconds()
			if duration < 0 {
				duration = 0
			}

			if execerr != nil {

				logger.Log.Error(
					"Job execution failed",
					"worker", w.ID,
					"job", Recievedjob.ID,
					"error", err,
				)

				Recievedjob.RetryCount++

				if Recievedjob.RetryCount <= w.MaxRetries {
					metrics.JobDuration.WithLabelValues(Recievedjob.Type, "retried").Observe(duration)

					metrics.JobsRetried.Inc()
					metrics.JobsScheduled.Inc()

					delay := retry.CalculateBackOff(Recievedjob.RetryCount)

					Recievedjob.NextRetry = time.Now().UTC().Add(delay)
					
					Recievedjob.FinishedAt = time.Now().UTC()
					Recievedjob.Status = jobs.StatusRetrying

					metrics.JobsRetried.Inc()

					logger.Log.Warn(
						"Job failed, scheduling retry",
						"worker", w.ID,
						"job", Recievedjob.ID,
						"Total retries", Recievedjob.RetryCount,
						"retry_after", delay,
						"Status", Recievedjob.Status,
					)

					if err := w.Queue.Schedule(ctx, Recievedjob, delay); err != nil {
						logger.Log.Error(
							"Failed to schedule delayed retry",
							"job", Recievedjob.ID,
							"error", err,
						)

						_ = w.Queue.Nack(ctx, Recievedjob)
						continue
					}

					continue
				}

				Recievedjob.Status = jobs.StatusFailed
				Recievedjob.FinishedAt = time.Now().UTC()

				metrics.JobsFailed.Inc()
				metrics.JobDuration.WithLabelValues(Recievedjob.Type, "failed").Observe(duration)

				err = w.Queue.SaveJob(ctx, Recievedjob)

				if err != nil {
					logger.Log.Error(
						"Failed to Save job",
						"Job ID", Recievedjob.ID,
						"error", err,
					)
					return
				}

				deadJob := jobs.DeadJob{

					Job: Recievedjob,

					FailureReason: execerr.Error(),

					FailedAt: time.Now(),
				}

				logger.Log.Error(
					"job moved to dead letter queue",

					"worker", w.ID,

					"job_id", Recievedjob.ID,

					"retry_count", Recievedjob.RetryCount,

					"reason", execerr.Error(),
				)

				if err := w.Queue.MoveToDeadLetter(ctx, deadJob); err != nil {

					logger.Log.Error(
						"failed moving job to DLQ",
						"job", Recievedjob.ID,
						"error", err,
					)
				} else {
					metrics.JobsDeadLetter.Inc()

					logger.Log.Error(
						"Job moved to dead letter queue",
						"worker", w.ID,
						"job", job.ID,
					)
				}
				_ = w.Queue.ACK(ctx, Recievedjob.ID)
				continue
			}

			Recievedjob.Status = jobs.StatusCompleted
			Recievedjob.FinishedAt = time.Now().UTC()

			metrics.JobsCompleted.Inc()
			metrics.JobDuration.WithLabelValues(Recievedjob.Type, "completed").Observe(duration)
			
			_ = w.Queue.SaveJob(ctx, Recievedjob)

			if Recievedjob.IdempotencyKey != "" {
				fullKey := Recievedjob.Type + ":" + Recievedjob.IdempotencyKey
				if err := w.Queue.MarkProcessed(ctx, fullKey); err != nil {
					logger.Log.Error("Failed to mark job as processed",
						"job", Recievedjob.ID,
						"key", fullKey,
						"error", err,
					)

				}
			}

			if err = w.Queue.ACK(ctx, Recievedjob.ID); err != nil {
				logger.Log.Error(
					"Failed to Ack job",
					"Job ID", Recievedjob.ID,
					"error", err,
				)
			}

			logger.Log.Info(
				"job processed succesful",
				"worker", w.ID,
				"job", Recievedjob.ID,
				"Status", Recievedjob.Status,
			)
		}
	}
}

func (w *Worker) heartbeat(ctx context.Context, jobID string, timeOut time.Duration) {

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
			if err := w.Queue.ExtendVisibility(ctx, jobID, timeOut); err != nil {
				logger.Log.Error("Heartbeat failed",
					"worker", w.ID,
					"job", jobID,
					"error", err,
				)
				continue
			}
			metrics.JobsHeartbeat.Inc()
			logger.Log.Debug("Heartbeat sent",
				"worker", w.ID,
				"job", jobID,
			)
		}
	}
}