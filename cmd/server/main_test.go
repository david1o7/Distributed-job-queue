package main

import (
	"context"
	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/metrics"
	"distributed-job-system/internal/queue"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func testQueue(t *testing.T) (*queue.RedisQueue, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	return queue.NewRedisQueue(mr.Addr()), mr
}

func readyLen(ctx context.Context, q *queue.RedisQueue) int64 {
	var total int64
	for _, key := range []string{"jobs:high", "jobs:default", "jobs:low", "jobs"} {
		n, err := q.Client.LLen(ctx, key).Result()
		if err == nil {
			total += n
		}
	}
	return total
}

func TestMain(m *testing.M) {
	metrics.Init()
	m.Run()
}

func TestStartReaperRedeliversExpired(t *testing.T) {
	q, _ := testQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := jobs.Job{
		ID:       "reap-1",
		Type:     "print",
		Status:   jobs.StatusProcessing,
		Priority: jobs.PriorityDefault,
	}
	require.NoError(t, q.SaveJob(ctx, job))

	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).Unix()),
		Member: job.ID,
	}).Err())

	go startReaper(ctx, q, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		return readyLen(ctx, q) >= 1
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
}

func TestStartDelayedMoverMovesReadyJob(t *testing.T) {
	q, _ := testQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := jobs.Job{
		ID:       "del-1",
		Type:     "print",
		Status:   jobs.StatusRetrying,
		Priority: jobs.PriorityDefault,
	}
	require.NoError(t, q.SaveJob(ctx, job))
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score:  float64(time.Now().Add(-time.Second).Unix()),
		Member: job.ID,
	}).Err())

	go startDelayedMover(ctx, q, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		return readyLen(ctx, q) >= 1
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
}

func TestStartMetricsSamplerUpdatesGauges(t *testing.T) {

	q, _ := testQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := jobs.Job{
		ID:       "m1",
		Type:     "print",
		Status:   jobs.StatusQueued,
		Priority: jobs.PriorityDefault,
	}
	require.NoError(t, q.Push(ctx, job))

	go startMetricsSampler(ctx, q, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		stats, err := q.Stats(ctx)
		return err == nil && stats.MainDepth >= 1
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
}
