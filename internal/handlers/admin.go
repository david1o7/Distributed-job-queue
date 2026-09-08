package handlers

import (
	"encoding/json"
	"net/http"
	"strconv"

	"distributed-job-system/internal/queue"
)

func DelayedJobsHandler(q *queue.RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		items, err := q.ListDelayedJobs(r.Context())
		if err != nil {
			http.Error(w, "failed to list delayed jobs", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(items); err != nil {
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
		}
	}
}

func DeadJobsPagedHandler(q *queue.RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.ParseInt(r.URL.Query().Get("start"), 10, 64)
		count, _ := strconv.ParseInt(r.URL.Query().Get("count"), 10, 64)

		page, err := q.ListDeadJobsPage(r.Context(), start, count)
		if err != nil {
			http.Error(w, "failed to list dead jobs", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(page); err != nil {
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
		}
	}
}

func StatsHandler(q *queue.RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := q.Stats(r.Context())
		if err != nil {
			http.Error(w, "failed to load stats", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(stats); err != nil {
			http.Error(w, "failed to encode response", http.StatusInternalServerError)
		}
	}
}
