package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"distributed-job-system/internal/queue"
)

type healthResponse struct {
	Status string `json:"status"`
}

type readyResponse struct {
	Status string `json:"status"`
	Redis  string `json:"redis"`
}


func HealthHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(healthResponse{
			Status: "ok",
		})
	}
}


func ReadyHandler(q *queue.RedisQueue) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()

		if err := q.Ping(ctx); err != nil {
			w.WriteHeader(http.StatusServiceUnavailable)
			_ = json.NewEncoder(w).Encode(readyResponse{
				Status: "unavailable",
				Redis:  "down",
			})
			return
		}

		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(readyResponse{
			Status: "ready",
			Redis:  "up",
		})
	}
}