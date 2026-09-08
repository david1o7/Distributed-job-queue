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
	"github.com/stretchr/testify/require"
)

func setupQueue(t *testing.T) (*queue.RedisQueue, *miniredis.Miniredis) {
	t.Helper()
	mr, err := miniredis.Run()
	require.NoError(t, err)
	return queue.NewRedisQueue(mr.Addr()), mr
}

func TestHealthHandler(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	rr := httptest.NewRecorder()
	HealthHandler().ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestReadyHandlerUp(t *testing.T) {
	q, mr := setupQueue(t)
	defer mr.Close()

	req := httptest.NewRequest(http.MethodGet, "/ready", nil)
	rr := httptest.NewRecorder()
	ReadyHandler(q).ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestGetJobHandler(t *testing.T) {
	q, mr := setupQueue(t)
	defer mr.Close()
	ctx := context.Background()

	job := jobs.Job{ID: "j1", Type: "print", Status: jobs.StatusQueued, CreatedAt: time.Now().UTC()}
	require.NoError(t, q.SaveJob(ctx, job))

	req := httptest.NewRequest(http.MethodGet, "/jobs/j1", nil)
	req.SetPathValue("id", "j1") // Go 1.22+
	rr := httptest.NewRecorder()
	GetJobHandler(q).ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	var got jobs.Job
	require.NoError(t, json.NewDecoder(rr.Body).Decode(&got))
	require.Equal(t, "j1", got.ID)
}

func TestDeadJobHandlerAndReplay(t *testing.T) {
	q, mr := setupQueue(t)
	defer mr.Close()
	ctx := context.Background()

	dead := jobs.DeadJob{
		Job:           jobs.Job{ID: "d1", Type: "print", Status: jobs.StatusFailed},
		FailureReason: "x",
		FailedAt:      time.Now().UTC(),
	}
	require.NoError(t, q.MoveToDeadLetter(ctx, dead))

	req := httptest.NewRequest(http.MethodGet, "/dead-jobs", nil)
	rr := httptest.NewRecorder()
	DeadJobHandler(q).ServeHTTP(rr, req)
	require.Equal(t, http.StatusOK, rr.Code)

	req2 := httptest.NewRequest(http.MethodPost, "/dead-jobs/d1/replay", nil)
	req2.SetPathValue("id", "d1")
	rr2 := httptest.NewRecorder()
	ReplayDeadJobHandler(q).ServeHTTP(rr2, req2)
	require.Equal(t, http.StatusAccepted, rr2.Code)
}
