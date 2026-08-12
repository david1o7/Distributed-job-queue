package handlers

import (
	"distributed-job-system/internal/queue"
	"encoding/json"
	"errors"
	"net/http"

	"github.com/redis/go-redis/v9"
)

func DeadJobHandler(q *queue.RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobs, err := q.ListDeadJobs(r.Context())

		if err != nil {
			http.Error(w, "failed to fetch dead jobs", http.StatusInternalServerError)
			return
		}

		w.Header().Set(
			"Content-Type",
			"application/json",
		)

		json.NewEncoder(w).Encode(jobs)
	}
}

func ReplayDeadJobHandler(q *queue.RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")

		if id == "" {
			http.Error(w, "missing job id", http.StatusBadRequest)
			return
		}

		job, err := q.ReplayDeadJob(r.Context(), id)

		if err != nil {
			if errors.Is(err, redis.Nil) {
				http.Error(w, "dead job not found", http.StatusNotFound)

				return
			}

			http.Error(w, "failed to replay job", http.StatusInternalServerError)
			return

		}

		w.Header().Set(
			"Content-Type",
			"application/json",
		)

		w.WriteHeader(http.StatusAccepted)

		json.NewEncoder(w).Encode(map[string]interface{}{
			"job_id": job.ID,
			"status": job.Status,
		})
	}
}
