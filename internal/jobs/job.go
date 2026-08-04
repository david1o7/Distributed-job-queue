package jobs

import (
	"encoding/json"
	"time"
)

type Job struct{
	ID  string `json:"id"`
	Type string `json:"type"`
	Payload json.RawMessage `json:"payload"`

	Status Status `json:"status"`

	RetryCount int `json:"retry_count"`
	MaxRetries int `json:"max_retries"`

	CreatedAt time.Time `json:"created_at"`
	NextRetry time.Time `json:"next_retry"`
}

type Status string

const (
	StatusQueued Status = "queued"
	StatusProcessing Status = "processing"
	StatusRetrying Status = "retrying"
	StatusCompleted Status = "completed"
	StatusFailed Status = "failed"
)