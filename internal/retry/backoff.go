package retry

import "time"

func CalculateBackOff(retryCount int) time.Duration {

	baseDelay := time.Second

	delay := baseDelay * time.Duration(1<<retryCount)

	return delay
}
