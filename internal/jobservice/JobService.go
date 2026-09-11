package jobservice

import (
	"context"
	"distributed-job-system/internal/broker"
	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/logger"
	"distributed-job-system/internal/metrics"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Service struct{
	Store broker.JobStore
	Queue broker.Queue
}

func New(store broker.JobStore, q broker.Queue) *Service {
	return &Service{
		Store: store,
		Queue: q,
	}
}

type EnqueueRequest struct{
	Type string
	Payload []byte
	IdempotencyKey string
	Priority jobs.Priority
	MaxRetries int
}

func (s *Service) Enqueue(ctx context.Context, req EnqueueRequest) (*jobs.Job, error) {
	if req.MaxRetries <= 0 {
		req.MaxRetries = 3
	}
	job := jobs.Job{
		ID:             uuid.NewString(),
		Type:           req.Type,
		Payload:        req.Payload,
		Status:         jobs.StatusQueued,
		RetryCount:     0,
		MaxRetries:     req.MaxRetries,
		CreatedAt:      time.Now().UTC(),
		IdempotencyKey: req.IdempotencyKey,
		Priority:       jobs.NormalizePriority(req.Priority),
	}

	logger.WithContext(ctx).Info(
		"enqueue job",
		"job_id", job.ID, 
		"job_type", job.Type, 
		"priority", job.Priority,
	)

if err := s.Store.Save(ctx, job); err != nil {
	logger.Log.Error("jobstore save failed", "job_id", job.ID, "error", err)
	return nil, fmt.Errorf("jobstore save: %w", err)
}
if err := s.Queue.Push(ctx, job); err != nil {
	logger.Log.Error("broker push failed", "job_id", job.ID, "error", err)
	return &job, fmt.Errorf("store ok but push failed (job remains queued for retry): %w", err)
}
	metrics.JobsQueued.Inc()
	return &job, nil
}

func (s *Service) Get(ctx context.Context, id string) (*jobs.Job, error) {
	return s.Store.Get(ctx, id)
}