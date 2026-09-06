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

func testQueue(t *testing.T) (*RedisQueue, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	return NewRedisQueue(mr.Addr()), mr
}

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
	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, ids, "job-1")

	// Act: Ack
	require.NoError(t, q.ACK(ctx, "job-1"))

	// Assert: removed from processing
	ids, err = q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
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
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  expiredScore,
		Member: "job-timeout",
	}).Err())

	reaped, err := q.ReapExpired(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)

	length, err := q.Client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), length)

	ids, _ := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
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

	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(30 * time.Second).Unix()),
		Member: "job-nack",
	}).Err())

	require.NoError(t, q.Nack(ctx, job))

	ids, _ := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NotContains(t, ids, "job-nack")

	length, _ := q.Client.LLen(ctx, "jobs").Result()
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
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
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

	ids, _ := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
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

	length, err := q.Client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), length)

	claimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "job-claim-once", claimed.ID)

	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, ids, "job-claim-once")

	length, err = q.Client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(0), length, "after Claim the job must leave the main queue")
}

func TestScheduleMovesJobToDelayedAndRemovesFromProcessing(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:         "job-delayed-1",
		Type:       "print",
		Status:     jobs.StatusRetrying,
		RetryCount: 1,
	}
	require.NoError(t, q.SaveJob(ctx, job))

	// Simulate that it was in processing
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(30 * time.Second).Unix()),
		Member: job.ID,
	}).Err())

	// Act
	require.NoError(t, q.Schedule(ctx, job, 10*time.Second))

	// Assert: removed from processing
	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, job.ID)

	// Assert: present in delayed set
	delayed, err := q.Client.ZRange(ctx, "jobs:delayed", 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, delayed, job.ID)
}

func TestMoveReadyDelayedJobsOnlyMovesReadyOnes(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	readyJob := jobs.Job{ID: "ready-job", Type: "print", Status: jobs.StatusRetrying}
	futureJob := jobs.Job{ID: "future-job", Type: "print", Status: jobs.StatusRetrying}

	require.NoError(t, q.SaveJob(ctx, readyJob))
	require.NoError(t, q.SaveJob(ctx, futureJob))

	// readyJob score = past
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score:  float64(time.Now().Add(-5 * time.Second).Unix()),
		Member: readyJob.ID,
	}).Err())

	// futureJob score = future
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score:  float64(time.Now().Add(10 * time.Minute).Unix()),
		Member: futureJob.ID,
	}).Err())

	// Act
	moved, err := q.MoveReadyDelayedJobs(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, moved)

	// Assert: ready job is now on main queue
	length, err := q.Client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), length)

	// Assert: future job still delayed
	delayed, err := q.Client.ZRange(ctx, "jobs:delayed", 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, delayed, futureJob.ID)
	require.NotContains(t, delayed, readyJob.ID)
}

func TestCrashRecovery_ReaperRedeliversInFlightJob(t *testing.T) {
	// Simulates: worker claimed job, then process died before ACK/Schedule.
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:     "crashed-job",
		Type:   "print",
		Status: jobs.StatusProcessing,
	}
	require.NoError(t, q.SaveJob(ctx, job))

	// Job is in processing with an already-expired visibility timeout
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(-1 * time.Minute).Unix()),
		Member: job.ID,
	}).Err())

	// Act – reaper runs (as it would after a crash)
	reaped, err := q.ReapExpired(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)

	// Assert: job is back on the main queue and no longer in processing
	length, err := q.Client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), length)

	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, job.ID)

	// Assert: it can be claimed again
	claimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "crashed-job", claimed.ID)
}

func TestCrashRecovery_MultipleExpiredJobs(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	for _, id := range []string{"c1", "c2", "c3"} {
		job := jobs.Job{ID: id, Type: "print", Status: jobs.StatusProcessing}
		require.NoError(t, q.SaveJob(ctx, job))
		require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
			Score:  float64(time.Now().Add(-30 * time.Second).Unix()),
			Member: id,
		}).Err())
	}

	reaped, err := q.ReapExpired(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, reaped)

	length, err := q.Client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(3), length)
}

func TestDelayedRetryFullCycle(t *testing.T) {
	
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	original := jobs.Job{
		ID:         "retry-cycle",
		Type:       "print",
		Payload:    json.RawMessage(`{"name":"cycle"}`),
		Status:     jobs.StatusQueued,
		RetryCount: 0,
		CreatedAt:  time.Now().UTC(),
	}
	require.NoError(t, q.Push(ctx, original))
	require.NoError(t, q.SaveJob(ctx, original))

	
	claimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "retry-cycle", claimed.ID)

	
	claimed.RetryCount++
	claimed.Status = jobs.StatusRetrying
	require.NoError(t, q.Schedule(ctx, *claimed, 1*time.Second))

	
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score:  float64(time.Now().Add(-1 * time.Second).Unix()),
		Member: claimed.ID,
	}).Err())

	
	moved, err := q.MoveReadyDelayedJobs(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, moved)

	
	reclaimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "retry-cycle", reclaimed.ID)
}

func TestMoveReadyDelayedJobsCleansOrphanedEntries(t *testing.T) {
	
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score:  float64(time.Now().Add(-5 * time.Second).Unix()),
		Member: "orphan-job",
	}).Err())

	moved, err := q.MoveReadyDelayedJobs(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, moved) 

	
	delayed, err := q.Client.ZRange(ctx, "jobs:delayed", 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, delayed, "orphan-job")
}

func TestReaperCleansOrphanedProcessingEntries(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(-1 * time.Minute).Unix()),
		Member: "orphan-processing",
	}).Err())

	reaped, err := q.ReapExpired(ctx)
	require.NoError(t, err)
	
	_ = reaped

	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, "orphan-processing")
}

func TestExtendVisibility(t *testing.T) {
	q, mr := testQueue(t) // use helper from earlier, or inline miniredis
	defer mr.Close()
	ctx := context.Background()

	job := jobs.Job{ID: "hb-1", Type: "print", Status: jobs.StatusProcessing}
	require.NoError(t, q.SaveJob(ctx, job))

	old := float64(time.Now().Add(10 * time.Second).Unix())
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score: old, Member: job.ID,
	}).Err())

	require.NoError(t, q.ExtendVisibility(ctx, job.ID, 30*time.Second))

	score, err := q.Client.ZScore(ctx, "jobs:processing", job.ID).Result()
	require.NoError(t, err)
	require.Greater(t, score, old)
}

func TestExtendVisibilityMissingJob(t *testing.T) {
	q, mr := testQueue(t)
	defer mr.Close()
	
	require.NoError(t, q.ExtendVisibility(context.Background(), "missing", 30*time.Second))
}

func TestIsProcessedAndMarkProcessed(t *testing.T) {
	q, mr := testQueue(t)
	defer mr.Close()
	ctx := context.Background()

	ok, err := q.IsProcessed(ctx, "")
	require.NoError(t, err)
	require.False(t, ok)

	require.NoError(t, q.MarkProcessed(ctx, "print:key-1"))
	ok, err = q.IsProcessed(ctx, "print:key-1")
	require.NoError(t, err)
	require.True(t, ok)
}

func TestPing(t *testing.T) {
	q, mr := testQueue(t)
	defer mr.Close()
	require.NoError(t, q.Ping(context.Background()))
}

func TestAcquireAndReleaseConcurrency(t *testing.T) {
	q, mr := testQueue(t)
	defer mr.Close()
	ctx := context.Background()

	// unlimited
	ok, err := q.AcquireConcurrency(ctx, "unlimited", 0)
	require.NoError(t, err)
	require.True(t, ok)

	// limit 1
	ok, err = q.AcquireConcurrency(ctx, "print", 1)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = q.AcquireConcurrency(ctx, "print", 1)
	require.NoError(t, err)
	require.False(t, ok) // limit reached

	require.NoError(t, q.ReleaseConcurrency(ctx, "print"))

	ok, err = q.AcquireConcurrency(ctx, "print", 1)
	require.NoError(t, err)
	require.True(t, ok)
}

func TestReplayDeadJobLua_Success(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	dead := jobs.DeadJob{
		Job: jobs.Job{
			ID:         "dead-1",
			Type:       "print",
			Payload:    json.RawMessage(`{"name":"x"}`),
			Status:     jobs.StatusFailed,
			RetryCount: 3,
			MaxRetries: 3,
			CreatedAt:  time.Now().UTC(),
		},
		FailureReason: "boom",
		FailedAt:      time.Now().UTC(),
	}
	require.NoError(t, q.MoveToDeadLetter(ctx, dead))

	// sanity
	list, err := q.ListDeadJobs(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)

	job, err := q.ReplayDeadJob(ctx, "dead-1")
	require.NoError(t, err)
	require.Equal(t, "dead-1", job.ID)
	require.Equal(t, jobs.StatusQueued, job.Status)
	require.Equal(t, 0, job.RetryCount)

	// removed from DLQ
	list, err = q.ListDeadJobs(ctx)
	require.NoError(t, err)
	require.Len(t, list, 0)

	// on main queue
	n, err := q.Client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)

	// persisted state
	stored, err := q.GetJob(ctx, "dead-1")
	require.NoError(t, err)
	require.Equal(t, jobs.StatusQueued, stored.Status)
}

func TestReplayDeadJobLua_NotFound(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	_, err = q.ReplayDeadJob(context.Background(), "missing")
	require.ErrorIs(t, err, redis.Nil)
}

func TestReplayDeadJobLua_EmptyID(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	_, err = q.ReplayDeadJob(context.Background(), "")
	require.ErrorIs(t, err, redis.Nil)
}

func TestReplayDeadJobLua_ConcurrentSecondCallFails(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	dead := jobs.DeadJob{
		Job: jobs.Job{
			ID:     "dead-once",
			Type:   "print",
			Status: jobs.StatusFailed,
		},
		FailureReason: "x",
		FailedAt:      time.Now().UTC(),
	}
	require.NoError(t, q.MoveToDeadLetter(ctx, dead))

	job, err := q.ReplayDeadJob(ctx, "dead-once")
	require.NoError(t, err)
	require.Equal(t, "dead-once", job.ID)

	// second replay must not find it
	_, err = q.ReplayDeadJob(ctx, "dead-once")
	require.ErrorIs(t, err, redis.Nil)

	// still only one copy on main queue
	n, err := q.Client.LLen(ctx, "jobs").Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), n)
}

func TestReplayDeadJobLua_IgnoresInvalidJSONInDLQ(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	// junk entry
	require.NoError(t, q.Client.LPush(ctx, "dead_job", "not-json").Err())

	dead := jobs.DeadJob{
		Job: jobs.Job{
			ID:     "good-dead",
			Type:   "print",
			Status: jobs.StatusFailed,
		},
		FailureReason: "x",
		FailedAt:      time.Now().UTC(),
	}
	require.NoError(t, q.MoveToDeadLetter(ctx, dead))

	job, err := q.ReplayDeadJob(ctx, "good-dead")
	require.NoError(t, err)
	require.Equal(t, "good-dead", job.ID)
}