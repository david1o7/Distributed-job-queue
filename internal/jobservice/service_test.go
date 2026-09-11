package jobservice

import (
	"context"
	"errors"
	"testing"
	"time"

	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/queue"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestEnqueue_SaveThenPush(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	q := queue.NewRedisQueue(mr.Addr())
	svc := New(q, q)

	job, err := svc.Enqueue(context.Background(), EnqueueRequest{
		Type:       "print",
		Payload:    []byte(`{"name":"a"}`),
		Priority:   jobs.PriorityHigh,
		MaxRetries: 3,
	})
	require.NoError(t, err)
	require.NotEmpty(t, job.ID)
	require.Equal(t, jobs.StatusQueued, job.Status)
	require.Equal(t, jobs.PriorityHigh, job.Priority)

	// In store
	stored, err := svc.Get(context.Background(), job.ID)
	require.NoError(t, err)
	require.Equal(t, job.ID, stored.ID)

	// On ready queue
	n, err := q.Client.LLen(context.Background(), "jobs:high").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

type pushFailQueue struct {
	*queue.RedisQueue
}

func (p *pushFailQueue) Push(ctx context.Context, job jobs.Job) error {
	return errors.New("broker down")
}

func TestEnqueue_PushFailure_LeavesJobInStore(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	store := queue.NewRedisQueue(mr.Addr())
	svc := New(store, &pushFailQueue{RedisQueue: store})

	job, err := svc.Enqueue(context.Background(), EnqueueRequest{
		Type:    "print",
		Payload: []byte(`{}`),
	})
	require.Error(t, err)
	require.NotNil(t, job) 
	require.Contains(t, err.Error(), "push failed")

	stored, gerr := store.Get(context.Background(), job.ID)
	require.NoError(t, gerr)
	require.Equal(t, jobs.StatusQueued, stored.Status)
}

func TestEnqueue_DefaultPriorityAndRetries(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)

	q := queue.NewRedisQueue(mr.Addr())
	svc := New(q, q)

	job, err := svc.Enqueue(context.Background(), EnqueueRequest{
		Type:    "print",
		Payload: []byte(`{}`),
		
	})
	require.NoError(t, err)
	require.Equal(t, 3, job.MaxRetries)
	require.Equal(t, jobs.PriorityDefault, job.Priority)
	require.WithinDuration(t, time.Now().UTC(), job.CreatedAt, 2*time.Second)
}