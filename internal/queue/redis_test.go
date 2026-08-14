package queue

import (
	"context"
	"distributed-job-system/internal/jobs"
	"encoding/json"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestClaimAndAck(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:        "job-1",
		Type:      "print",
		Payload:   json.RawMessage(`{"name":"test"}`),
		Status:    jobs.StatusQueued,
		CreatedAt: time.Now(),
	}
	require.NoError(t, q.Push(ctx, job))
	require.NoError(t, q.SaveJob(ctx, job))

	// Act
	claimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "job-1", claimed.ID)

	// Assert: job is now in the processing set
	ids, err := q.client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, ids, "job-1")

	// Act: Ack
	require.NoError(t, q.ACK(ctx, "job-1"))

	// Assert: removed from processing
	ids, err = q.client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, "job-1")
}

func TestReapExpiredRedeliversJob(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:     "job-timeout",
		Type:   "print",
		Status: jobs.StatusProcessing,
	}
	require.NoError(t, q.SaveJob(ctx, job))

	expiredScore := float64(time.Now().Add(-10 * time.Second).Unix())
	require.NoError(t, q.client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  expiredScore,
		Member: "job-timeout",
	}).Err())

	reaped, err := q.ReapExpired(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)

	length, err := q.client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), length)

	ids, _ := q.client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NotContains(t, ids, "job-timeout")
}

func TestNackReturnsJobToQueue(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:         "job-nack",
		Type:       "print",
		RetryCount: 1,
		Status:     jobs.StatusRetrying,
	}
	require.NoError(t, q.SaveJob(ctx, job))

	require.NoError(t, q.client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(30 * time.Second).Unix()),
		Member: "job-nack",
	}).Err())

	require.NoError(t, q.Nack(ctx, job))

	ids, _ := q.client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NotContains(t, ids, "job-nack")

	length, _ := q.client.LLen(ctx, "jobs").Result()
	require.Equal(t, int64(1), length)
}

func TestMaxRetriesMovesToDLQAndAcks(t *testing.T) {

	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:         "job-dlq",
		Type:       "print",
		RetryCount: 3,
		MaxRetries: 3,
		Status:     jobs.StatusProcessing,
	}
	require.NoError(t, q.SaveJob(ctx, job))
	require.NoError(t, q.client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(30 * time.Second).Unix()),
		Member: "job-dlq",
	}).Err())

	dead := jobs.DeadJob{
		Job:           job,
		FailureReason: "simulated permanent failure",
		FailedAt:      time.Now(),
	}
	require.NoError(t, q.MoveToDeadLetter(ctx, dead))
	require.NoError(t, q.ACK(ctx, "job-dlq"))

	deadJobs, err := q.ListDeadJobs(ctx)
	require.NoError(t, err)
	require.Len(t, deadJobs, 1)
	require.Equal(t, "job-dlq", deadJobs[0].ID)

	ids, _ := q.client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NotContains(t, ids, "job-dlq", "must be Acked after moving to DLQ")
}

func TestClaimRemovesJobFromMainQueue(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatal(err)
	}
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:        "job-claim-once",
		Type:      "print",
		Payload:   json.RawMessage(`{"name":"test"}`),
		Status:    jobs.StatusQueued,
		CreatedAt: time.Now(),
	}

	require.NoError(t, q.Push(ctx, job))
	require.NoError(t, q.SaveJob(ctx, job))

	length, err := q.client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), length)

	claimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "job-claim-once", claimed.ID)

	ids, err := q.client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, ids, "job-claim-once")

	length, err = q.client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), length, "after Claim the job must leave the main queue")
}
