package jobs

import "time"

type DeadJob struct {
	Job

	FailureReason string    `json:"failure_reason"`
	FailedAt      time.Time `json:"failed_at"`
}
