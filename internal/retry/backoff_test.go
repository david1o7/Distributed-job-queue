package retry

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCalculateBackOff(t *testing.T) {
	require.Equal(t, 2*time.Second, CalculateBackOff(1)) 
	require.Equal(t, 4*time.Second, CalculateBackOff(2))
	require.Equal(t, 8*time.Second, CalculateBackOff(3))
	require.Equal(t, 1*time.Second, CalculateBackOff(0))
}