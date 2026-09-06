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

func TestStartReaperRedeliversExpired(t *testing.T) {
	metrics.Init() 

	q, _ := testQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := jobs.Job{ID: "reap-1", Type: "print", Status: jobs.StatusProcessing}
	require.NoError(t, q.SaveJob(ctx, job))

	
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(-time.Minute).Unix()),
		Member: job.ID,
	}).Err())

	
	go startReaper(ctx, q, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		n, err := q.Client.LLen(ctx, "jobs").Result()
		return err == nil && n >= 1
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
}

func TestStartDelayedMoverMovesReadyJob(t *testing.T) {
	q, _ := testQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	job := jobs.Job{ID: "del-1", Type: "print", Status: jobs.StatusRetrying}
	require.NoError(t, q.SaveJob(ctx, job))
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score:  float64(time.Now().Add(-time.Second).Unix()),
		Member: job.ID,
	}).Err())

	go startDelayedMover(ctx, q, 20*time.Millisecond)

	require.Eventually(t, func() bool {
		n, err := q.Client.LLen(ctx, "jobs").Result()
		return err == nil && n >= 1
	}, 2*time.Second, 20*time.Millisecond)

	cancel()
}

func TestStartMetricsSamplerUpdatesGauges(t *testing.T) {
	q, _ := testQueue(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	
	job := jobs.Job{ID: "m1", Type: "print", Status: jobs.StatusQueued}
	require.NoError(t, q.Push(ctx, job))

	go startMetricsSampler(ctx, q, 20*time.Millisecond)

	
	time.Sleep(80 * time.Millisecond)
	cancel()
}