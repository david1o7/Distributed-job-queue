package worker

import (
	"context"
	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/logger"
	"encoding/json"
	"fmt"
)

type PrintHandler struct{}

func (p *PrintHandler) Handle(ctx context.Context, job jobs.Job) error {
	logger.Log.Info(
		"Processing Print job",
		"id", job.ID,
		"type", job.Type,
		"payload", string(job.Payload),
	)

	type payload struct {
		Name string `json:"name"`
	}

	var payload1 payload
	err := json.Unmarshal(job.Payload, &payload1)

	if err != nil {
		return err
	}

	fmt.Printf(
		"Welcome %s \n", payload1.Name,
	)

	return nil
}
