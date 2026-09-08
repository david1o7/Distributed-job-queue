package handlers

import (
	"context"
	"distributed-job-system/internal/jobs"
	"distributed-job-system/internal/queue"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func setupAdminQueue(t *testing.T) (*queue.RedisQueue, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	t.Cleanup(mr.Close)
	return queue.NewRedisQueue(mr.Addr()), mr
}

func TestDelayedJobsHandler_OK(t *testing.T) {
	q, _ := setupAdminQueue(t)
	ctx := context.Background()

	job := jobs.Job{ID: "dj1", Type: "print", Status: jobs.StatusRetrying, Priority: jobs.PriorityDefault}
	require.NoError(t, q.SaveJob(ctx, job))
	require.NoError(t, q.Client.ZAdd(ctx, "jobs:delayed", redis.Z{
		Score: float64(time.Now().Add(time.Minute).Unix()), Member: job.ID,
	}).Err())

	req := httptest.NewRequest(http.MethodGet, "/delayed-jobs", nil)
	rr := httptest.NewRecorder()
	DelayedJobsHandler(q).ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var items []queue.DelayedJobView
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&items))
	require.Len(t, items, 1)
	require.Equal(t, "dj1", items[0].Job.ID)
}

func TestDeadJobsPagedHandler_OK(t *testing.T) {
	q, _ := setupAdminQueue(t)
	ctx := context.Background()

	require.NoError(t, q.MoveToDeadLetter(ctx, jobs.DeadJob{
		Job:           jobs.Job{ID: "x1", Type: "print", Status: jobs.StatusFailed},
		FailureReason: "boom",
		FailedAt:      time.Now().UTC(),
	}))

	req := httptest.NewRequest(http.MethodGet, "/dead-jobs?start=0&count=10", nil)
	rr := httptest.NewRecorder()
	DeadJobsPagedHandler(q).ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var page queue.DeadJobPage
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&page))
	require.Equal(t, int64(1), page.Total)
	require.Len(t, page.Items, 1)
	require.Equal(t, "x1", page.Items[0].ID)
}

func TestStatsHandler_OK(t *testing.T) {
	q, _ := setupAdminQueue(t)
	ctx := context.Background()

	require.NoError(t, q.Push(ctx, jobs.Job{ID: "s1", Type: "print", Priority: jobs.PriorityDefault}))

	req := httptest.NewRequest(http.MethodGet, "/stats", nil)
	rr := httptest.NewRecorder()
	StatsHandler(q).ServeHTTP(rr, req)

	require.Equal(t, http.StatusOK, rr.Code)
	var st queue.QueueStats
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&st))
	require.GreaterOrEqual(t, st.MainDepth, int64(1))
}
