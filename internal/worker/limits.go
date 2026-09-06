package worker

import "os"


var DefaultConcurrencyLimits = map[string]int{
	"print":        5,
}


var DefaultLimitForUnknown = 0

func GetConcurrencyLimit(jobType string) int {
	if limit, ok := DefaultConcurrencyLimits[jobType]; ok {
		return limit
	}
	return DefaultLimitForUnknown
}


func init() {
	
	_ = os.Getenv
}