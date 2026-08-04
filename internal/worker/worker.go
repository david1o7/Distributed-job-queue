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
	"log"
	"time"


)

type Worker struct{
	ID int
	Queue *queue.RedisQueue
	MaxRetries int
}
func NewWorker(id int,q *queue.RedisQueue,maxRetries int) *Worker {

    return &Worker{
        ID: id,
        Queue: q,
        MaxRetries: maxRetries,
    }
}

func (w *Worker) Start(ctx context.Context,  registry *Registry) {

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
			Status: jobs.StatusProcessing,
			RetryCount: job.RetryCount,
			MaxRetries: w.MaxRetries,
			CreatedAt: time.Now(),
		}

		err = w.Queue.SaveJob(ctx, Recievedjob)

		if err != nil {
			logger.Log.Error(
				"Failed to Save job",
				"Job ID",Recievedjob.ID,
				"error",err,
			)
			return
		}

		err = registry.Execute(ctx, Recievedjob)
		if err != nil {

			logger.Log.Error(
				"Job execution failed",
				"worker", w.ID,
				"job", Recievedjob.ID,
				"error", err,
			)

			Recievedjob.RetryCount++
		}

		if err != nil {

			Recievedjob.RetryCount++

			delay := retry.CalculateBackOff(Recievedjob.RetryCount)

			if Recievedjob.RetryCount <= w.MaxRetries {

				metrics.JobsRetried.Inc()

				logger.Log.Warn(
					"Job failed, scheduling retry",
					"worker", w.ID,
					"job", Recievedjob.ID,
					"Total retries", Recievedjob.RetryCount,
					"retry_after",delay,
					"Status", Recievedjob.Status,
				)

				time.Sleep(delay)

				Recievedjob.NextRetry = time.Time{}.Add(delay)
				Recievedjob.Status = jobs.StatusRetrying

				err = w.Queue.SaveJob(ctx, Recievedjob)

				if err != nil {
					logger.Log.Error(
						"Failed to Save job",
						"Job ID",Recievedjob.ID,
						"error",err,
					)
					return
				}

				err := w.Queue.Push(ctx,Recievedjob)

				if err != nil {
					logger.Log.Error(
						"Failed to Enqueue job",
						"worker", w.ID,
						"job_id", Recievedjob.ID,
						"Error", err,
					)
					continue
				}

					continue
				}

				err = w.Queue.SaveJob(ctx, Recievedjob)

				if err != nil {
					logger.Log.Error(
						"Failed to Save job",
						"Job ID",Recievedjob.ID,
						"error",err,
					)
					return
				}

				Recievedjob.Status = jobs.StatusFailed

				metrics.JobsFailed.Inc()

				err = w.Queue.SaveJob(ctx, Recievedjob)

				if err != nil {
					logger.Log.Error(
						"Failed to Save job",
						"Job ID",Recievedjob.ID,
						"error",err,
					)
					return
				}


				logger.Log.Error(
					"job Permanently failed",
					"Worker", w.ID,
					"job", Recievedjob.ID,
					"Status", Recievedjob.Status,
				)
				
				continue
			}
			Recievedjob.Status = jobs.StatusCompleted

			metrics.JobsCompleted.Inc()

			err = w.Queue.SaveJob(ctx, Recievedjob)

				if err != nil {
					logger.Log.Error(
						"Failed to Save job",
						"Job ID",Recievedjob.ID,
						"error",err,
					)
					return
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

// func Process(job jobs.Job) error{

// 	time.Sleep(2 * time.Second)

// 	if rand.Intn(10) < 3{
// 		return errors.New("simulated processing failure")
// 	}

// 	return nil
// }