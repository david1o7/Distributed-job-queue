package broker

import (
	"context"
	"distributed-job-system/internal/jobs"
	"time"
)

type Queue interface{
	Push(ctx context.Context, job jobs.Job) error
	Claim(ctx context.Context, visibilityTimeout time.Duration) (*jobs.Job, error)
	ACK(ctx context.Context, jobID string) error
	Nack(ctx context.Context, job jobs.Job) error
	Schedule(ctx context.Context, job jobs.Job, delay time.Duration) error
	Close(ctx context.Context) error
}

type JobStore interface{
	Save(ctx context.Context, job jobs.Job) error
	Get(ctx context.Context, id string) (*jobs.Job, error)
}

type LeaseReaper interface{
	ReapExpired(ctx context.Context) (int, error)
}

type DelayedMover interface{
	MoveReadyDelayedJobs(ctx context.Context) (int, error)
}
type VisibilityExtender interface {
	ExtendVisibility(ctx context.Context, jobID string, extension time.Duration) error
}
type DeadLetter interface {
	MoveToDeadLetter(ctx context.Context, job jobs.DeadJob) error
}
type IdempotencyStore interface {
	IsProcessed(ctx context.Context, key string) (bool, error)
	MarkProcessed(ctx context.Context, key string) error
}
type ConcurrencyLimiter interface {
	AcquireConcurrency(ctx context.Context, jobType string, limit int) (bool, error)
	ReleaseConcurrency(ctx context.Context, jobType string) error
}