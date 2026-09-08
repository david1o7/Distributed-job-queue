package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetConcurrencyLimit(t *testing.T) {

	if limit, ok := DefaultConcurrencyLimits["print"]; ok {
		require.Equal(t, limit, GetConcurrencyLimit("print"))
	}

	require.Equal(t, DefaultLimitForUnknown, GetConcurrencyLimit("no-such-type"))
}
