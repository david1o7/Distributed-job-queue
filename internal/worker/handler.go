package worker

import (
	"context"
	"distributed-job-system/internal/jobs"
)

type JobHandler interface {
	Handle(ctx context.Context, job jobs.Job) error
}
