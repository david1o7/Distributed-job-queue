package metrics

import "testing"

func TestInitDoesNotPanic(t *testing.T) {
	// Safe to call once; if tests import metrics elsewhere, guard with sync.Once in Init.
	Init()
}
