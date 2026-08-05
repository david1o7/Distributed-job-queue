package handlers

import (
	"distributed-job-system/internal/queue"
	"encoding/json"
	"net/http"
)

func DeadJobHandler(q *queue.RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request){
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