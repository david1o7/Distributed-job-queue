package worker

import (
	"context"
	
	"distributed-job-system/internal/queue"
	"log"
	"time"
)

type Worker struct{
	ID int
	Queue *queue.RedisQueue
}

func NewWorker(id int, q *queue.RedisQueue) *Worker{
	return &Worker{
		ID: id,
		Queue: q,
	}
}

func (w *Worker) Start(ctx context.Context) {
	log.Printf("[Worker-%d] Started", w.ID)

	for {

		select {

		case <-ctx.Done():
			log.Println("Worker shutting Down!")
			return

		default:
			job, err := w.Queue.Pop(ctx)

			if err != nil{
				log.Println("Pop error:", err)
				time.Sleep(time.Second)
				continue
			}

			log.Printf(
					"[Worker-%d] Processing Job %s (%s)",
					w.ID,
					job.ID,
					job.Type,
				)

			time.Sleep(2 *time.Second)

			log.Printf(
				"[Worker-%d] Finished Job %s",
				w.ID,
				job.ID,
			)

		}
	}
}