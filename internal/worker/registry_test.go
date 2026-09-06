package worker

import (
	"context"
	"distributed-job-system/internal/jobs"
	"testing"

	"github.com/stretchr/testify/require"
)

type okHandler struct{}

func (okHandler) Handle(ctx context.Context, job jobs.Job) error { return nil }

type errHandler struct{}

func (errHandler) Handle(ctx context.Context, job jobs.Job) error {
	return context.Canceled
}

func TestRegistryExecute(t *testing.T) {
	r := NewRegistry()
	r.Register("ok", okHandler{})

	err := r.Execute(context.Background(), jobs.Job{Type: "ok"})
	require.NoError(t, err)

	err = r.Execute(context.Background(), jobs.Job{Type: "missing"})
	require.Error(t, err)
}

func TestHandlerDoesFail(t *testing.T){
	r := NewRegistry()
	r.Register("fail", errHandler{})

	err := r.Execute(context.Background(), jobs.Job{Type: "print"})
	require.Error(t, err)
}

func TestRegistryRegisterPanicsOnDuplicate(t *testing.T) {
	r := NewRegistry()
	r.Register("x", okHandler{})
	require.Panics(t, func() { r.Register("x", okHandler{}) })

}