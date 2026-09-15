package appserver

import (
	"context"
	"errors"
	"testing"
)

func TestPreCanceledEmptyQueueDoesNotWrite(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	defer c.Close(context.Background())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.call(ctx, RPCRequest{ID: "pre-canceled", Method: "never"})
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Evidence.Phase != WriteProvenBeforeWrite {
		t.Fatalf("pre-canceled evidence = %T %+v", err, err)
	}
	select {
	case payload := <-f.writes:
		t.Fatalf("pre-canceled request wrote %s", payload)
	default:
	}
}
