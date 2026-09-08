package worker

import (
	"context"
	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/queue"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

type failingHandler struct{}

func (f *failingHandler) Handle(ctx context.Context, job jobs.Job) error {
	return context.DeadlineExceeded
}

func TestWorkerSchedulesDelayedRetryOnFailure(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := queue.NewRedisQueue(mr.Addr())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	registry := NewRegistry()
	registry.Register("fail", &failingHandler{})

	job := jobs.Job{
		ID:         "fail-job",
		Type:       "fail",
		Payload:    json.RawMessage(`{}`),
		Status:     jobs.StatusQueued,
		RetryCount: 0,
		MaxRetries: 3,
		CreatedAt:  time.Now().UTC(),
	}
	require.NoError(t, q.Push(ctx, job))
	require.NoError(t, q.SaveJob(ctx, job))

	claimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)

	claimed.RetryCount++
	claimed.Status = jobs.StatusRetrying
	require.NoError(t, q.Schedule(ctx, *claimed, 2*time.Second))

	proc, _ := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NotContains(t, proc, "fail-job")

	delayed, _ := q.Client.ZRange(ctx, "jobs:delayed", 0, -1).Result()
	require.Contains(t, delayed, "fail-job")
}

func TestNewWorkerAndStartProcessesJob(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := queue.NewRedisQueue(mr.Addr())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := jobs.Job{
		ID:        "w1",
		Type:      "print",
		Payload:   json.RawMessage(`{"name":"t"}`),
		Status:    jobs.StatusQueued,
		CreatedAt: time.Now().UTC(),
	}
	require.NoError(t, q.Push(ctx, job))
	require.NoError(t, q.SaveJob(ctx, job))

	reg := NewRegistry()
	// Use real PrintHandler or okHandler
	reg.Register("print", &PrintHandler{})

	w := NewWorker(1, q, 3)
	require.Equal(t, 1, w.ID)

	go w.Start(ctx, reg, 30)

	// wait until job completed or timeout
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		got, err := q.GetJob(ctx, "w1")
		if err == nil && got.Status == jobs.StatusCompleted {
			cancel()
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	cancel()
	t.Fatal("job was not completed in time")
}

func TestHeartbeatExtendsVisibility(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := queue.NewRedisQueue(mr.Addr())
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobID := "hb-job"
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(5 * time.Second).Unix()),
		Member: jobID,
	}).Err())

	w := NewWorker(1, q, 3)
	go w.heartbeat(ctx, jobID, 30)

	time.Sleep(100 * time.Millisecond)
	require.NoError(t, q.ExtendVisibility(ctx, jobID, 30*time.Second))

	score, err := q.Client.ZScore(ctx, "jobs:processing", jobID).Result()
	require.NoError(t, err)
	require.Greater(t, score, float64(time.Now().Unix()))
	cancel()
}
