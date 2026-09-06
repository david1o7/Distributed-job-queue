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
	log := logger.WithContext(ctx)
    log.Info(
			"handling print job", 
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
