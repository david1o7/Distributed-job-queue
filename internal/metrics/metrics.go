package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

var JobsRetried = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "jobs_retried_total",
		Help: "Total number of retried jobs",
	},
)

var JobsFailed = prometheus.NewCounter(
	prometheus.CounterOpts{
		Name: "jobs_failed_total",
		Help: "Total permanently failed jobs",
	},
)

var UnknownJobs = prometheus.NewCounter(

	prometheus.CounterOpts{

		Name: "unknown_jobs_total",

		Help: "Total unknown job types",
	},
)

var JobsCompleted = prometheus.NewCounter(

	prometheus.CounterOpts{

		Name: "jobs_completed_total",

		Help: "Total completed job types",
	},
)

var JobsProcessing = prometheus.NewCounter(

	prometheus.CounterOpts{

		Name: "jobs_processed_total",

		Help: "Total processed jobs types",
	},
)

var JobsQueued = prometheus.NewCounter(

	prometheus.CounterOpts{

		Name: "jobs_queued_total",

		Help: "Total queued jobs types",
	},
)

var JobsDeadLetter = prometheus.NewCounter(

	prometheus.CounterOpts{

		Name: "jobs_dead_letter_total",

		Help: "Total jobs moved to dead letter queue",
	},
)

var JobsReaped = prometheus.NewCounter(

	prometheus.CounterOpts{

		Name: "jobs_reaped_total",

		Help: "Total jobs reaped after visibility timeout expired",
	},
)

var JobsScheduled = prometheus.NewCounter(
	prometheus.CounterOpts{

		Name: "jobs_scheduled_total",

		Help: "Total jobs scheduled for delayed retry",
	})

var JobsDelayedMoved = prometheus.NewCounter(
	prometheus.CounterOpts{

		Name: "jobs_delayed_moved_total",

		Help: "Total delayed jobs moved back to the main queue",
	})

var JobsHeartbeat = prometheus.NewCounter(
	prometheus.CounterOpts{

	Name: "jobs_heartbeat_total",

	Help: "Total visibility extensions (heartbeats) performed",
})

var JobsConcurrencyLimited = prometheus.NewCounter(
	prometheus.CounterOpts{

	Name: "jobs_concurrency_limited_total",

	Help: "Jobs that were requeued because the per-type concurrency limit was reached",
})


var JobDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name: "job_duration_seconds",
	Help: "Time spent executing a job handler",
	Buckets: []float64{
		0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120,
	},
}, []string{"job_type", "status"}) 

var QueueLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Name: "job_queue_latency_seconds",
	Help: "Time a job spent waiting in the queue before processing started",
	Buckets: []float64{
		0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300,
	},
}, []string{"job_type"})


var QueueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "jobs_queue_depth",
	Help: "Current number of jobs in the main queue",
})


var JobsInFlight = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "jobs_in_flight",
	Help: "Current number of jobs being processed (processing set size)",
})


var JobsDelayed = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "jobs_delayed",
	Help: "Current number of jobs waiting for delayed retry",
})


var DeadLetterDepth = prometheus.NewGauge(prometheus.GaugeOpts{
	Name: "jobs_dead_letter_depth",
	Help: "Current number of jobs in the dead letter queue",
})

func Init() {

	prometheus.MustRegister(
		JobsRetried,
		JobsFailed,
		UnknownJobs,
		JobsCompleted,
		JobsQueued,
		JobsProcessing,
		JobsDeadLetter,
		JobsReaped,
		JobsScheduled,
		JobsDelayedMoved,
		JobsHeartbeat,
		JobsConcurrencyLimited,
		JobDuration,
		QueueLatency,
		QueueDepth,
		JobsInFlight,
		JobsDelayed,
		DeadLetterDepth,
	)

}
