package logger

import (
	"context"
	"log/slog"
	"os"
	"strings"
)

type ctxKey string

const (
	RequestIDKey ctxKey = "request_id"
	JobIDKey     ctxKey = "job_id"
	WorkerIDKey  ctxKey = "worker_id"
)

var Log *slog.Logger = slog.Default()


func Init(format, level string) {
	var lvl slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "json":
		handler = slog.NewJSONHandler(os.Stdout, opts)
	default:
		handler = slog.NewTextHandler(os.Stdout, opts)
	}

	Log = slog.New(handler)
	slog.SetDefault(Log)
}


func WithContext(ctx context.Context) *slog.Logger {
	if ctx == nil {
		return Log
	}

	args := make([]any, 0, 6)

	if v, ok := ctx.Value(RequestIDKey).(string); ok && v != "" {
		args = append(args, "request_id", v)
	}
	if v, ok := ctx.Value(JobIDKey).(string); ok && v != "" {
		args = append(args, "job_id", v)
	}
	if v, ok := ctx.Value(WorkerIDKey).(int); ok {
		args = append(args, "worker_id", v)
	}
	
	if v, ok := ctx.Value(WorkerIDKey).(string); ok && v != "" {
		args = append(args, "worker_id", v)
	}

	if len(args) == 0 {
		return Log
	}
	return Log.With(args...)
}



func WithRequestID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, RequestIDKey, id)
}

func WithJobID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, JobIDKey, id)
}

func WithWorkerID(ctx context.Context, id int) context.Context {
	return context.WithValue(ctx, WorkerIDKey, id)
}

func RequestIDFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(RequestIDKey).(string); ok {
		return v
	}
	return ""
}

func JobIDFromCtx(ctx context.Context) string {
	if v, ok := ctx.Value(JobIDKey).(string); ok {
		return v
	}
	return ""
}