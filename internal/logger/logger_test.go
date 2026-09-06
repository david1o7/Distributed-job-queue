package logger

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWithRequestIDAndFromCtx(t *testing.T) {
	ctx := context.Background()
	require.Equal(t, "", RequestIDFromCtx(ctx))

	ctx = WithRequestID(ctx, "req-123")
	require.Equal(t, "req-123", RequestIDFromCtx(ctx))

	
	ctx2 := WithRequestID(context.Background(), "")
	require.Equal(t, "", RequestIDFromCtx(ctx2))
}

func TestWithJobIDAndFromCtx(t *testing.T) {
	ctx := context.Background()
	require.Equal(t, "", JobIDFromCtx(ctx))

	ctx = WithJobID(ctx, "job-abc")
	require.Equal(t, "job-abc", JobIDFromCtx(ctx))
}

func TestWithWorkerID(t *testing.T) {
	ctx := WithWorkerID(context.Background(), 7)

	
	v := ctx.Value(WorkerIDKey)
	require.Equal(t, 7, v)
}

func TestWithContextIncludesIDs(t *testing.T) {
	
	Init("text", "debug")

	ctx := context.Background()
	ctx = WithRequestID(ctx, "r1")
	ctx = WithJobID(ctx, "j1")
	ctx = WithWorkerID(ctx, 3)

	log := WithContext(ctx)
	require.NotNil(t, log)

	
	log.Info("test message")
}

func TestWithContextNilAndEmpty(t *testing.T) {
	Init("json", "info")

	// nil ctx
	log := WithContext(context.TODO())
	require.NotNil(t, log)

	// empty ctx → base logger
	log2 := WithContext(context.Background())
	require.NotNil(t, log2)
}

func TestInitTextAndJSON(t *testing.T) {
	Init("text", "debug")
	require.NotNil(t, Log)
	Log.Debug("debug ok")

	Init("json", "warn")
	require.NotNil(t, Log)
	Log.Warn("warn ok")

	
	Init("nope", "info")
	require.NotNil(t, Log)
}