package mektup

import (
	"fmt"
	"sync"
	"time"
)

// EventSequencer enforces the per-operation JSONL event contract: sequence
// numbers start at one, increase without gaps, and stop after one terminal
// record. It does not write stdout, so journal-before-output remains the
// caller's responsibility.
type EventSequencer struct {
	mu          sync.Mutex
	operationID string
	next        uint64
	terminal    bool
}

func NewEventSequencer(operationID string) *EventSequencer {
	if operationID == "" {
		operationID = NewOperationID()
	}
	return &EventSequencer{operationID: operationID}
}

func (s *EventSequencer) OperationID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.operationID
}

func (s *EventSequencer) Sequence() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.next
}

// Next creates and records the next event. The returned event is ready for
// EncodeJSONLine after the caller commits its corresponding journal transition.
func (s *EventSequencer) Next(name string, terminal, ok bool, data map[string]any) (Event, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return Event{}, fmt.Errorf("mektup: event sequencer is already terminal")
	}
	if name == "" {
		return Event{}, fmt.Errorf("mektup: event name is required")
	}
	s.next++
	e := Event{Schema: EventSchema, Event: name, EventID: NewEventID(), Sequence: s.next,
		OperationID: s.operationID, Timestamp: time.Now().UTC().Format("2006-01-02T15:04:05.000000Z"),
		Terminal: terminal, OK: ok, Warnings: []Warning{}, Data: data}
	if err := e.Validate(); err != nil {
		s.next--
		return Event{}, err
	}
	if terminal {
		s.terminal = true
	}
	return e, nil
}

// NextEvent is a descriptive alias for Next.
func (s *EventSequencer) NextEvent(name string, terminal, ok bool, data map[string]any) (Event, error) {
	return s.Next(name, terminal, ok, data)
}

// Append accepts an event produced elsewhere while enforcing the same
// operation-local sequencing and terminal rules.
func (s *EventSequencer) Append(e Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.terminal {
		return fmt.Errorf("mektup: event sequencer is already terminal")
	}
	if e.OperationID != s.operationID {
		return fmt.Errorf("mektup: event operation ID mismatch")
	}
	if e.Sequence != s.next+1 {
		return fmt.Errorf("mektup: event sequence %d, want %d", e.Sequence, s.next+1)
	}
	if err := e.Validate(); err != nil {
		return err
	}
	s.next = e.Sequence
	if e.Terminal {
		s.terminal = true
	}
	return nil
}
