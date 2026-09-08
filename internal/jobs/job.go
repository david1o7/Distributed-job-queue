package jobs

import (
	"encoding/json"
	"time"
)

type Priority string

const (
	PriorityHigh    Priority = "high"
	PriorityDefault Priority = "default"
	PriorityLow     Priority = "low"
)

type Status string

const (
	StatusQueued     Status = "queued"
	StatusProcessing Status = "processing"
	StatusRetrying   Status = "retrying"
	StatusCompleted  Status = "completed"
	StatusFailed     Status = "failed"
)

type Job struct {
	ID      string          `json:"id"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload"`

	Status     Status `json:"status"`
	RetryCount int    `json:"retry_count"`
	MaxRetries int    `json:"max_retries"`

	CreatedAt      time.Time `json:"created_at"`
	NextRetry      time.Time `json:"next_retry"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`

	WorkerID   int       `json:"worker_id"`
	StartedAt  time.Time `json:"started_at,omitempty"`
	FinishedAt time.Time `json:"finished_at,omitempty"`

	Priority Priority `json:"priority,omitempty"`
}

func NormalizePriority(p Priority) Priority {
	switch p {
	case PriorityHigh, PriorityDefault, PriorityLow:
		return p
	default:
		return PriorityDefault
	}
}

func QueueKeyFor(p Priority) string {
	switch NormalizePriority(p) {
	case PriorityHigh:
		return "jobs:high"
	case PriorityLow:
		return "jobs:low"
	default:
		return "jobs:default"
	}
}

func AllReadyQueues() []string {
	return []string{"jobs:high", "jobs:default", "jobs:low"}
}
