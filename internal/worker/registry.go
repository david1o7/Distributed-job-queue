package worker

import (
	"context"
	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/logger"
	"distributed-job-system/internal/metrics"
	"fmt"
)

type Registry struct{
	handlers map[string]JobHandler
}

func NewRegistry() *Registry{
	return &Registry{
		handlers: make(map[string]JobHandler),
	}
}

func (r *Registry) Register(jobType string, handler JobHandler){

	if handler == nil {
	panic("nil job handler")
	}

	if _, exists := r.handlers[jobType]; exists {
	panic("handler already registered: " + jobType)
	}

	r.handlers[jobType] = handler
}

func (r *Registry) Execute(ctx context.Context, job jobs.Job) error {
	    logger.Log.Info(
        "Looking up handler",
        "job_type", job.Type,
    )

	handlers, ok := r.handlers[job.Type]

	if !ok{
		metrics.UnknownJobs.Inc()
		return fmt.Errorf("no handler registered for %s", job.Type)
	}
	return handlers.Handle(ctx, job)
}