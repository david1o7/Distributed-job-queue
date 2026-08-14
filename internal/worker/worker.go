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

const visibilityTimeout = 30 * time.Second

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

func (w *Worker) Start(ctx context.Context, registry *Registry) {

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
			job, err := w.Queue.Claim(ctx, visibilityTimeout)

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

			Recievedjob := jobs.Job{
				ID:         job.ID,
				Type:       job.Type,
				Payload:    job.Payload,
				Status:     jobs.StatusProcessing,
				RetryCount: job.RetryCount,
				MaxRetries: w.MaxRetries,
				CreatedAt:  time.Now(),
			}

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

			err1 := registry.Execute(ctx, Recievedjob)

			if err1 != nil {

				logger.Log.Error(
					"Job execution failed",
					"worker", w.ID,
					"job", Recievedjob.ID,
					"error", err,
				)

				Recievedjob.RetryCount++

				delay := retry.CalculateBackOff(Recievedjob.RetryCount)

				if Recievedjob.RetryCount <= w.MaxRetries {

					metrics.JobsRetried.Inc()

					logger.Log.Warn(
						"Job failed, scheduling retry",
						"worker", w.ID,
						"job", Recievedjob.ID,
						"Total retries", Recievedjob.RetryCount,
						"retry_after", delay,
						"Status", Recievedjob.Status,
					)

					Recievedjob.NextRetry = time.Now().Add(delay)
					Recievedjob.Status = jobs.StatusRetrying

					if err = w.Queue.SaveJob(ctx, Recievedjob); err != nil {
						logger.Log.Error(
							"Failed to Save job's retry state",
							"Job ID", Recievedjob.ID,
							"error", err,
						)
						return
					}

					if err := w.Queue.Nack(ctx, Recievedjob); err != nil {
						logger.Log.Error(
							"Failed to Nack job",
							"worker", w.ID,
							"job_id", Recievedjob.ID,
							"Error", err,
						)
					}

					continue
				}

				Recievedjob.Status = jobs.StatusFailed

				metrics.JobsFailed.Inc()

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

					Job: jobs.Job{

						ID: Recievedjob.ID,

						Type: Recievedjob.Type,

						Payload: Recievedjob.Payload,

						Status: Recievedjob.Status,

						RetryCount: Recievedjob.RetryCount,

						MaxRetries: w.MaxRetries,

						CreatedAt: Recievedjob.CreatedAt,
					},

					FailureReason: err1.Error(),

					FailedAt: time.Now(),
				}

				logger.Log.Error(
					"job moved to dead letter queue",

					"worker", w.ID,

					"job_id", Recievedjob.ID,

					"retry_count", Recievedjob.RetryCount,

					"reason", err1.Error(),
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

			metrics.JobsCompleted.Inc()

			if err = w.Queue.SaveJob(ctx, Recievedjob); err != nil {
				logger.Log.Error(
					"Failed to Save job",
					"Job ID", Recievedjob.ID,
					"error", err,
				)
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
