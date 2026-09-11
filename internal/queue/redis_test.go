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

func readyLen(ctx context.Context, q *RedisQueue) int64 {
	var total int64
	for _, key := range jobs.AllReadyQueues() {
		n, err := q.Client.LLen(ctx, key).Result()
		if err == nil {
			total += n
		}
	}
	return total
}

func lenKey(ctx context.Context, q *RedisQueue, key string) int64 {
	n, _ := q.Client.LLen(ctx, key).Result()
	return n
}

func testQueue(t *testing.T) (*RedisQueue, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	return NewRedisQueue(mr.Addr()), mr
}

func TestClaimAndAck(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
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

	claimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "job-1", claimed.ID)

	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, ids, "job-1")

	require.NoError(t, q.ACK(ctx, "job-1"))

	ids, err = q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, "job-1")
}

func TestReapExpiredRedeliversJob(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:       "job-timeout",
		Type:     "print",
		Status:   jobs.StatusProcessing,
		Priority: jobs.PriorityDefault,
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

	require.Equal(t, int64(1), readyLen(ctx, q))

	ids, _ := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NotContains(t, ids, "job-timeout")
}

func TestNackReturnsJobToQueue(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:         "job-nack",
		Type:       "print",
		RetryCount: 1,
		Status:     jobs.StatusRetrying,
		Priority:   jobs.PriorityDefault,
	}
	require.NoError(t, q.SaveJob(ctx, job))

	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(30 * time.Second).Unix()),
		Member: "job-nack",
	}).Err())

	require.NoError(t, q.Nack(ctx, job))

	ids, _ := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NotContains(t, ids, "job-nack")

	require.Equal(t, int64(1), readyLen(ctx, q))
	require.Equal(t, int64(1), lenKey(ctx, q, "jobs:default"))
}

func TestMaxRetriesMovesToDLQAndAcks(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
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
	require.NoError(t, err)
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

	require.Equal(t, int64(1), readyLen(ctx, q))

	claimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "job-claim-once", claimed.ID)

	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, ids, "job-claim-once")

	require.Equal(t, int64(0), readyLen(ctx, q), "after Claim the job must leave ready queues")
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

	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(30 * time.Second).Unix()),
		Member: job.ID,
	}).Err())

	require.NoError(t, q.Schedule(ctx, job, 10*time.Second))

	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, job.ID)

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

	readyJob := jobs.Job{ID: "ready-job", Type: "print", Status: jobs.StatusRetrying, Priority: jobs.PriorityDefault}
	futureJob := jobs.Job{ID: "future-job", Type: "print", Status: jobs.StatusRetrying, Priority: jobs.PriorityDefault}

	require.NoError(t, q.SaveJob(ctx, readyJob))
	require.NoError(t, q.SaveJob(ctx, futureJob))

	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score:  float64(time.Now().Add(-5 * time.Second).Unix()),
		Member: readyJob.ID,
	}).Err())

	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score:  float64(time.Now().Add(10 * time.Minute).Unix()),
		Member: futureJob.ID,
	}).Err())

	moved, err := q.MoveReadyDelayedJobs(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, moved)

	require.Equal(t, int64(1), readyLen(ctx, q))

	delayed, err := q.Client.ZRange(ctx, "jobs:delayed", 0, -1).Result()
	require.NoError(t, err)
	require.Contains(t, delayed, futureJob.ID)
	require.NotContains(t, delayed, readyJob.ID)
}

func TestCrashRecovery_ReaperRedeliversInFlightJob(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:       "crashed-job",
		Type:     "print",
		Status:   jobs.StatusProcessing,
		Priority: jobs.PriorityDefault,
	}
	require.NoError(t, q.SaveJob(ctx, job))

	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(-1 * time.Minute).Unix()),
		Member: job.ID,
	}).Err())

	reaped, err := q.ReapExpired(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, reaped)

	require.Equal(t, int64(1), readyLen(ctx, q))

	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, job.ID)

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
		job := jobs.Job{ID: id, Type: "print", Status: jobs.StatusProcessing, Priority: jobs.PriorityDefault}
		require.NoError(t, q.SaveJob(ctx, job))
		require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
			Score:  float64(time.Now().Add(-30 * time.Second).Unix()),
			Member: id,
		}).Err())
	}

	reaped, err := q.ReapExpired(ctx)
	require.NoError(t, err)
	require.Equal(t, 3, reaped)

	require.Equal(t, int64(3), readyLen(ctx, q))
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
		Priority:   jobs.PriorityDefault,
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

	require.Equal(t, 0, reaped)

	ids, err := q.Client.ZRange(ctx, "jobs:processing", 0, -1).Result()
	require.NoError(t, err)
	require.NotContains(t, ids, "orphan-processing")
}

func TestExtendVisibility(t *testing.T) {
	q, mr := testQueue(t)
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

	ok, err := q.AcquireConcurrency(ctx, "unlimited", 0)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = q.AcquireConcurrency(ctx, "print", 1)
	require.NoError(t, err)
	require.True(t, ok)

	ok, err = q.AcquireConcurrency(ctx, "print", 1)
	require.NoError(t, err)
	require.False(t, ok)

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
			Priority:   jobs.PriorityDefault,
		},
		FailureReason: "boom",
		FailedAt:      time.Now().UTC(),
	}
	require.NoError(t, q.MoveToDeadLetter(ctx, dead))

	list, err := q.ListDeadJobs(ctx)
	require.NoError(t, err)
	require.Len(t, list, 1)

	job, err := q.ReplayDeadJob(ctx, "dead-1")
	require.NoError(t, err)
	require.Equal(t, "dead-1", job.ID)
	require.Equal(t, jobs.StatusQueued, job.Status)
	require.Equal(t, 0, job.RetryCount)

	list, err = q.ListDeadJobs(ctx)
	require.NoError(t, err)
	require.Len(t, list, 0)

	require.Equal(t, int64(1), readyLen(ctx, q))
	require.Equal(t, int64(1), lenKey(ctx, q, "jobs:default"))

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
			ID:       "dead-once",
			Type:     "print",
			Status:   jobs.StatusFailed,
			Priority: jobs.PriorityDefault,
		},
		FailureReason: "x",
		FailedAt:      time.Now().UTC(),
	}
	require.NoError(t, q.MoveToDeadLetter(ctx, dead))

	job, err := q.ReplayDeadJob(ctx, "dead-once")
	require.NoError(t, err)
	require.Equal(t, "dead-once", job.ID)

	_, err = q.ReplayDeadJob(ctx, "dead-once")
	require.ErrorIs(t, err, redis.Nil)

	require.Equal(t, int64(1), readyLen(ctx, q))
}

func TestReplayDeadJobLua_IgnoresInvalidJSONInDLQ(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	require.NoError(t, q.Client.LPush(ctx, "dead_job", "not-json").Err())

	dead := jobs.DeadJob{
		Job: jobs.Job{
			ID:       "good-dead",
			Type:     "print",
			Status:   jobs.StatusFailed,
			Priority: jobs.PriorityDefault,
		},
		FailureReason: "x",
		FailedAt:      time.Now().UTC(),
	}
	require.NoError(t, q.MoveToDeadLetter(ctx, dead))

	job, err := q.ReplayDeadJob(ctx, "good-dead")
	require.NoError(t, err)
	require.Equal(t, "good-dead", job.ID)
}

// ---- Priority alignment tests ----

func TestPushRoutesByPriority(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	require.NoError(t, q.Push(ctx, jobs.Job{ID: "h", Type: "print", Priority: jobs.PriorityHigh}))
	require.NoError(t, q.Push(ctx, jobs.Job{ID: "d", Type: "print", Priority: jobs.PriorityDefault}))
	require.NoError(t, q.Push(ctx, jobs.Job{ID: "l", Type: "print", Priority: jobs.PriorityLow}))

	require.Equal(t, int64(1), lenKey(ctx, q, "jobs:high"))
	require.Equal(t, int64(1), lenKey(ctx, q, "jobs:default"))
	require.Equal(t, int64(1), lenKey(ctx, q, "jobs:low"))
	require.Equal(t, int64(0), lenKey(ctx, q, "jobs"), "legacy list must stay unused")
	require.Equal(t, int64(3), readyLen(ctx, q))
}

func TestClaimPrefersHighOverLow(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	require.NoError(t, q.Push(ctx, jobs.Job{ID: "low-1", Type: "print", Priority: jobs.PriorityLow}))
	require.NoError(t, q.Push(ctx, jobs.Job{ID: "high-1", Type: "print", Priority: jobs.PriorityHigh}))

	claimed, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "high-1", claimed.ID)

	claimed2, err := q.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "low-1", claimed2.ID)
}

func TestNackPreservesHighPriority(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{
		ID:       "p1",
		Type:     "print",
		Priority: jobs.PriorityHigh,
		Status:   jobs.StatusProcessing,
	}
	require.NoError(t, q.SaveJob(ctx, job))
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score:  float64(time.Now().Add(30 * time.Second).Unix()),
		Member: job.ID,
	}).Err())

	require.NoError(t, q.Nack(ctx, job))

	require.Equal(t, int64(1), lenKey(ctx, q, "jobs:high"))
	require.Equal(t, int64(0), lenKey(ctx, q, "jobs:default"))
	require.Equal(t, int64(0), lenKey(ctx, q, "jobs:low"))
}

func TestEmptyPriorityNormalizesToDefault(t *testing.T) {
	require.Equal(t, jobs.PriorityDefault, jobs.NormalizePriority(""))
	require.Equal(t, "jobs:default", jobs.QueueKeyFor(""))
}

func TestStats_PriorityLists(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	require.NoError(t, q.Push(ctx, jobs.Job{ID: "h1", Type: "print", Priority: jobs.PriorityHigh}))
	require.NoError(t, q.Push(ctx, jobs.Job{ID: "d1", Type: "print", Priority: jobs.PriorityDefault}))
	require.NoError(t, q.Push(ctx, jobs.Job{ID: "l1", Type: "print", Priority: jobs.PriorityLow}))
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:processing", redis.Z{
		Score: float64(time.Now().Add(30 * time.Second).Unix()), Member: "p1",
	}).Err())
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score: float64(time.Now().Add(time.Minute).Unix()), Member: "del1",
	}).Err())
	require.NoError(t, q.MoveToDeadLetter(ctx, jobs.DeadJob{
		Job: jobs.Job{ID: "dead1", Type: "print", Status: jobs.StatusFailed},
		FailureReason: "x", FailedAt: time.Now().UTC(),
	}))

	st, err := q.Stats(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(3), st.MainDepth)
	require.Equal(t, int64(1), st.ReadyHigh)
	require.Equal(t, int64(1), st.ReadyDefault)
	require.Equal(t, int64(1), st.ReadyLow)
	require.Equal(t, int64(1), st.Processing)
	require.Equal(t, int64(1), st.Delayed)
	require.Equal(t, int64(1), st.DeadLetter)
}

func TestListDelayedJobs_ReadyFutureOrphan(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	ready := jobs.Job{ID: "d-ready", Type: "print", Status: jobs.StatusRetrying}
	future := jobs.Job{ID: "d-future", Type: "print", Status: jobs.StatusRetrying}
	require.NoError(t, q.SaveJob(ctx, ready))
	require.NoError(t, q.SaveJob(ctx, future))
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score: float64(time.Now().Add(-time.Minute).Unix()), Member: ready.ID,
	}).Err())
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score: float64(time.Now().Add(10 * time.Minute).Unix()), Member: future.ID,
	}).Err())
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score: float64(time.Now().Unix()), Member: "orphan-delayed",
	}).Err())

	items, err := q.ListDelayedJobs(ctx)
	require.NoError(t, err)
	require.Len(t, items, 3)

	byID := map[string]DelayedJobView{}
	for _, it := range items {
		byID[it.Job.ID] = it
	}
	require.False(t, byID["d-ready"].Orphan)
	require.Equal(t, int64(0), byID["d-ready"].ReadyIn)
	require.Greater(t, byID["d-future"].ReadyIn, int64(0))
	require.True(t, byID["orphan-delayed"].Orphan)
}

func TestListDeadJobsPage_Pagination(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	require.NoError(t, q.Client.LPush(ctx, "dead_job", "not-json").Err())
	for i := 0; i < 3; i++ {
		require.NoError(t, q.MoveToDeadLetter(ctx, jobs.DeadJob{
			Job: jobs.Job{
				ID:     "dead-" + string(rune('0'+i)),
				Type:   "print",
				Status: jobs.StatusFailed,
			},
			FailureReason: "x",
			FailedAt:      time.Now().UTC(),
		}))
	}

	page, err := q.ListDeadJobsPage(ctx, 0, 2)
	require.NoError(t, err)
	require.Equal(t, int64(4), page.Total) // 3 + junk
	require.LessOrEqual(t, len(page.Items), 2)

	page2, err := q.ListDeadJobsPage(ctx, -1, 0) // clamp
	require.NoError(t, err)
	require.Equal(t, int64(0), page2.Start)
	require.GreaterOrEqual(t, page2.Count, int64(1))
}

func TestSaveGetClose_JobStoreAliases(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()
	q := NewRedisQueue(mr.Addr())
	ctx := context.Background()

	job := jobs.Job{ID: "s1", Type: "print", Status: jobs.StatusQueued}
	require.NoError(t, q.Save(ctx, job))
	got, err := q.Get(ctx, "s1")
	require.NoError(t, err)
	require.Equal(t, "s1", got.ID)
	require.NoError(t, q.Close(ctx))
}