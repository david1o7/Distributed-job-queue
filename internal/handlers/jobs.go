package handlers

import (
    "encoding/json"
    "net/http"
    "strings"

    "distributed-job-system/internal/queue"
)

func GetJobHandler(q *queue.RedisQueue) http.HandlerFunc {

    return func(w http.ResponseWriter, r *http.Request) {

        id := strings.TrimPrefix(
            r.URL.Path,
            "/jobs/",
        )

        if id == "" {

            http.Error(
                w,
                "missing id",
                http.StatusBadRequest,
            )

            return
        }

        job, err := q.GetJob(
            r.Context(),
            id,
        )

        if err != nil {

            http.Error(
                w,
                "job not found",
                http.StatusNotFound,
            )

            return
        }

        w.Header().Set(
            "Content-Type",
            "application/json",
        )

        json.NewEncoder(w).Encode(job)
    }
}