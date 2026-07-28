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

func Init(){

	prometheus.MustRegister(
		JobsRetried,
		JobsFailed,
	)

}