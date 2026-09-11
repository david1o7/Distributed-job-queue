package producer

import (
	"bytes"
	"distributed-job-system/internal/jobservice"
	"distributed-job-system/internal/queue"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/stretchr/testify/require"
)

func TestProducerHandler(t *testing.T) {
	mr, err := miniredis.Run()
	require.NoError(t, err)
	defer mr.Close()

	q := queue.NewRedisQueue(mr.Addr())
	jobS := jobservice.New(q,q)
	body := []byte(`{"type":"print","payload":{"name":"x"}}`)

	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rr := httptest.NewRecorder()

	Handler(jobS).ServeHTTP(rr, req)
	require.True(t, rr.Code == http.StatusOK || rr.Code == http.StatusAccepted || rr.Code == http.StatusCreated)
}
