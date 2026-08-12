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

func Init() {

	prometheus.MustRegister(
		JobsRetried,
		JobsFailed,
		UnknownJobs,
		JobsCompleted,
		JobsQueued,
		JobsProcessing,
		JobsDeadLetter,
	)

}
