package rabbit

import (
	"context"
	"os"
	"testing"
	"time"

	"distributed-job-system/internal/jobs"

	"github.com/stretchr/testify/require"
)

func TestRabbitPushClaimAck(t *testing.T) {
	url := os.Getenv("RABBIT_URL")
	if url == "" {
		t.Skip("RABBIT_URL not set")
	}

	b, err := New(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close(context.Background()) })

	ctx := context.Background()
	job := jobs.Job{
		ID:       "r-test-1",
		Type:     "print",
		Priority: jobs.PriorityHigh,
		Status:   jobs.StatusQueued,
		Payload:  []byte(`{}`),
	}
	require.NoError(t, b.Push(ctx, job))

	got, err := b.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.Equal(t, "r-test-1", got.ID)
	require.NoError(t, b.ACK(ctx, got.ID))
}

func TestRabbitSchedulePublishesToWait(t *testing.T) {
	url := os.Getenv("RABBIT_URL")
	if url == "" {
		t.Skip("RABBIT_URL not set")
	}
	b, err := New(url)
	require.NoError(t, err)
	t.Cleanup(func() { _ = b.Close(context.Background()) })

	ctx := context.Background()
	job := jobs.Job{ID: "r-delay-1", Type: "print", Priority: jobs.PriorityDefault}
	require.NoError(t, b.Push(ctx, job))
	got, err := b.Claim(ctx, 30*time.Second)
	require.NoError(t, err)
	require.NoError(t, b.Schedule(ctx, *got, 2*time.Second))
}