package appserver

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestByteBudgetsDisconnectAndClearRetainedQueues(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{ReadByteBudget: 1000, EventByteBudget: 100, EventCapacity: 8})
	defer c.Close(context.Background())
	pushJSON(f, `{"method":"notice","params":{"blob":"`+strings.Repeat("x", 70)+`"}}`)
	pushJSON(f, `{"method":"notice","params":{"blob":"`+strings.Repeat("y", 70)+`"}}`)
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("byte budget overflow did not terminate generation")
	}
	c.budgetMu.Lock()
	readBytes, eventBytes := c.readBytes, c.eventBytes
	c.budgetMu.Unlock()
	if readBytes != 0 || eventBytes != 0 {
		t.Fatalf("retained byte budgets after terminal cleanup: read=%d event=%d", readBytes, eventBytes)
	}
}

func TestRawFrameBudgetDisconnectsBeforeAdmission(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{ReadByteBudget: 100, EventByteBudget: 1000})
	defer c.Close(context.Background())
	pushJSON(f, `{"method":"notice","params":{"blob":"`+strings.Repeat("z", 120)+`"}}`)
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("raw frame budget overflow did not terminate generation")
	}
	c.budgetMu.Lock()
	readBytes := c.readBytes
	c.budgetMu.Unlock()
	if readBytes != 0 {
		t.Fatalf("raw frame bytes retained after cleanup: %d", readBytes)
	}
}

func TestCanceledWrittenCallClosesAndClearsPendingState(t *testing.T) {
	f := newFakeTransport()
	c := New(f, Options{})
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := c.call(ctx, RPCRequest{ID: "deadline", Method: "accepted-no-response"})
	var callErr *CallError
	if !errors.As(err, &callErr) || callErr.Evidence.Phase == WriteProvenBeforeWrite {
		t.Fatalf("deadline evidence = %T %+v", err, err)
	}
	select {
	case <-c.Done():
	case <-time.After(time.Second):
		t.Fatal("canceled written call left generation alive")
	}
	c.mu.Lock()
	reserved, retired := len(c.reserved), len(c.retired)
	c.mu.Unlock()
	if reserved != 0 || retired != 1 {
		t.Fatalf("pending/reservation cleanup: reserved=%d retired=%d", reserved, retired)
	}
}
