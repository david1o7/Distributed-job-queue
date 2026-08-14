package producer

import (
	"context"
	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/logger"
	"distributed-job-system/internal/metrics"
	"distributed-job-system/internal/queue"
	"encoding/json"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type CreateJobRequest struct {
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`
}

func Handler(q *queue.RedisQueue) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateJobRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}

		job := jobs.Job{
			ID:         uuid.NewString(),
			Type:       req.Type,
			Payload:    req.Payload,
			Status:     jobs.StatusQueued,
			RetryCount: 0,
			MaxRetries: 3,
			CreatedAt:  time.Now(),
		}

		logger.Log.Info(
			"incoming job id",
			"job_id", job.ID,
		)

		ctx := context.Background()

		if err := q.Push(ctx, job); err != nil {
			http.Error(w, "failed to enqueue job", http.StatusInternalServerError)
			return
		}

		metrics.JobsQueued.Inc()

		if err := q.SaveJob(ctx, job); err != nil {
			http.Error(w, "failed to save job", http.StatusInternalServerError)

			return
		}

		w.WriteHeader(http.StatusAccepted)

		if err := json.NewEncoder(w).Encode(map[string]string{
			"job_id": job.ID,
			"status": "queued",
		}); err != nil {
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
			return
		}

	}
}
