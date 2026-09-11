package producer

import (
	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/jobservice"

	"encoding/json"
	"net/http"
)

type CreateJobRequest struct {
	Type           string          `json:"type"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key,omitempty"`
	Priority       jobs.Priority   `json:"priority,omitempty"`
}

var TypePriority = map[string]jobs.Priority{
	"print": jobs.PriorityDefault,
}

func PriorityForType(job CreateJobRequest) jobs.Priority {
	if job.Priority != "" {
		return jobs.NormalizePriority(job.Priority)
	}
	if p, ok := TypePriority[job.Type]; ok {
		return p
	}
	return jobs.PriorityDefault
}

func Handler(svc *jobservice.Service) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var req CreateJobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "invalid json", http.StatusBadRequest)
			return
		}
		job, err := svc.Enqueue(r.Context(), jobservice.EnqueueRequest{
			Type:           req.Type,
			Payload:        req.Payload,
			IdempotencyKey: req.IdempotencyKey,
			Priority:       PriorityForType(req),
			MaxRetries:     3,
		})
		if err != nil {
			http.Error(w, "failed to enqueue", http.StatusServiceUnavailable)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusAccepted)
		_ = json.NewEncoder(w).Encode(map[string]string{
			"job_id": job.ID,
			"status": "queued",
		})
	}
}