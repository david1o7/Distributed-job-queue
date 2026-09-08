package middleware

import (
	"net/http"

	"distributed-job-system/internal/logger"

	"github.com/google/uuid"
)

const RequestIDHeader = "X-Request-ID"

func RequestID(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get(RequestIDHeader)
		if id == "" {
			id = uuid.NewString()
		}

		ctx := logger.WithRequestID(r.Context(), id)
		w.Header().Set(RequestIDHeader, id)

		logger.WithContext(ctx).Info(
			"request started",
			"method", r.Method,
			"path", r.URL.Path,
			"remote", r.RemoteAddr,
		)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}
