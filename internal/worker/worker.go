package worker

import (
	"context"
	"errors"
	"math/rand"

	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/logger"
	"distributed-job-system/internal/metrics"
	"distributed-job-system/internal/queue"
	"distributed-job-system/internal/retry"
	"log"
	"time"
)

type Worker struct{
	ID int
	Queue *queue.RedisQueue
	MaxRetries int
}
func NewWorker(id int,q *queue.RedisQueue,maxRetries int,) *Worker {

    return &Worker{
        ID: id,
        Queue: q,
        MaxRetries: maxRetries,
    }
}

func (w *Worker) Start(ctx context.Context) {

	logger.Log.Info(
		"Worker started",
		"worker", w.ID,
	)

	for {

		select {

		case <-ctx.Done():
			log.Println("Worker shutting Down!")
			return

		default:
		job, err := w.Queue.Pop(ctx)

		if err != nil {
			logger.Log.Error(
				"Failed to pop job",
				"worker",w.ID,
				"error",err,
			)
			continue
		}

		Recievedjob := jobs.Job{
			ID: job.ID,
			Type: job.Type,
			Payload: job.Payload,
			RetryCount: job.RetryCount,
			MaxRetries: w.MaxRetries,
			CreatedAt: time.Now(),
		}

		err = Process(Recievedjob)

		if err != nil {

			job.RetryCount++

			delay := retry.CalculateBackOff(job.RetryCount)

			if job.RetryCount <= w.MaxRetries {

				metrics.JobsRetried.Inc()
				logger.Log.Warn(
					"Job failed, scheduling retry",
					"worker", w.ID,
					"job", job.ID,
					"Total retries", job.RetryCount,
					"retry_after",delay,
				)

				time.Sleep(delay)

				Updatedjob := jobs.Job{
					ID: job.ID,
					Type: job.Type,
					Payload: job.Payload,
					RetryCount: job.RetryCount,
					MaxRetries: w.MaxRetries,
					CreatedAt: time.Now(),
				}

				err := w.Queue.Push(ctx,Updatedjob)

				if err != nil {
					logger.Log.Error(
						"Failed to Enqueue job",
						"worker", w.ID,
						"job_id", job.ID,
						"Error", err,
					)
					continue
				}

					continue
				}

				logger.Log.Error(
					"job Permanently failed",
					"Worker", w.ID,
					"job", job.ID,
				)
				metrics.JobsFailed.Inc()
				continue
			}

			logger.Log.Info(
				"job processed succesful",
				"worker", w.ID,
				"job", job.ID,
			)
		}
	}
}

func Process(job jobs.Job) error{

	time.Sleep(2 * time.Second)

	if rand.Intn(10) < 3{
		return errors.New("simulated processing failure")
	}

	return nil
}