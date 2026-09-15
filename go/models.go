package mektup

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const ReceiptSchema = "mektup/receipt/v1"
const EventSchema = "mektup/event/v1"

// ReceiptIdentity is portable routing and observed identity metadata. It has
// no filesystem, executable, credential, or authority-bearing fields.
type ReceiptIdentity struct {
	EndpointID    string         `json:"endpointId,omitempty"`
	Alias         string         `json:"alias,omitempty"`
	Transport     string         `json:"transport,omitempty"`
	ServerVersion string         `json:"serverVersion,omitempty"`
	Compatibility string         `json:"compatibility,omitempty"`
	ThreadID      string         `json:"threadId,omitempty"`
	Requested     string         `json:"requested,omitempty"`
	Resolved      string         `json:"resolved,omitempty"`
	Evidence      map[string]any `json:"evidence,omitempty"`
}

type Identity = ReceiptIdentity

type ReceiptMessage struct {
	MessageID       string `json:"messageId"`
	InReplyTo       string `json:"inReplyTo,omitempty"`
	Kind            string `json:"kind"`
	ReplyRequested  bool   `json:"replyRequested"`
	PayloadBytes    uint64 `json:"payloadBytes"`
	PayloadSHA256   string `json:"payloadSha256"`
	ClientMessageID string `json:"clientMessageId,omitempty"`
	TurnID          string `json:"turnId,omitempty"`
	AcceptedAt      string `json:"acceptedAt,omitempty"`
	ReplyAt         string `json:"replyAt,omitempty"`
}

// ContentRef locates body content for an explicit authorized lookup. It never
// contains the body itself.
type ContentRef struct {
	EndpointID      string `json:"endpointId"`
	ThreadID        string `json:"threadId"`
	TurnID          string `json:"turnId,omitempty"`
	ItemID          string `json:"itemId,omitempty"`
	ClientMessageID string `json:"clientMessageId,omitempty"`
	PayloadBytes    uint64 `json:"payloadBytes"`
	PayloadSHA256   string `json:"payloadSha256"`
}

type EvidenceRecord struct {
	State     EvidenceState  `json:"state"`
	At        string         `json:"at,omitempty"`
	Kind      string         `json:"kind,omitempty"`
	Reference string         `json:"reference,omitempty"`
	Details   map[string]any `json:"details,omitempty"`
}

type ManualResolution struct {
	Assertion    string `json:"assertion"`
	Actor        string `json:"actor"`
	Reason       string `json:"reason"`
	Timestamp    string `json:"timestamp"`
	EvidenceRef  string `json:"evidenceRef"`
	Presentation string `json:"presentation"`
}

// Receipt is the portable metadata-only receipt. Deliberately there is no
// Body field anywhere in the receipt model.
type Receipt struct {
	Schema      string            `json:"schema"`
	ReceiptID   string            `json:"receiptId"`
	OperationID string            `json:"operationId"`
	Operation   string            `json:"operation"`
	State       EvidenceState     `json:"state"`
	Source      ReceiptIdentity   `json:"source"`
	Target      ReceiptIdentity   `json:"target"`
	Message     ReceiptMessage    `json:"message"`
	ContentRef  *ContentRef       `json:"contentRef,omitempty"`
	Evidence    []EvidenceRecord  `json:"evidence"`
	Warnings    []Warning         `json:"warnings"`
	Resolution  *ManualResolution `json:"resolution,omitempty"`
	CreatedAt   string            `json:"createdAt"`
	UpdatedAt   string            `json:"updatedAt"`
}

func (r Receipt) MarshalJSON() ([]byte, error) {
	type plain Receipt
	if r.Evidence == nil {
		r.Evidence = []EvidenceRecord{}
	}
	if r.Warnings == nil {
		r.Warnings = []Warning{}
	}
	return json.Marshal(plain(r))
}

func (r Receipt) Validate() error {
	if r.Schema != ReceiptSchema {
		return fmt.Errorf("mektup: unsupported receipt schema %q", r.Schema)
	}
	if err := ValidateID(r.ReceiptID, ReceiptIDPrefix); err != nil {
		return fmt.Errorf("receipt id: %w", err)
	}
	if err := ValidateID(r.OperationID, OperationIDPrefix); err != nil {
		return fmt.Errorf("operation id: %w", err)
	}
	if strings.TrimSpace(r.Operation) == "" {
		return fmt.Errorf("mektup: receipt operation is required")
	}
	if !r.State.Valid() {
		return fmt.Errorf("mektup: invalid receipt state %q", r.State)
	}
	if err := validateTimestamp(r.CreatedAt, "createdAt"); err != nil {
		return err
	}
	if err := validateTimestamp(r.UpdatedAt, "updatedAt"); err != nil {
		return err
	}
	if r.Source.EndpointID == "" || r.Target.EndpointID == "" {
		return fmt.Errorf("mektup: receipt source and target endpoint IDs are required")
	}
	if err := ValidateID(r.Source.EndpointID, EndpointIDPrefix); err != nil {
		return err
	}
	if err := ValidateID(r.Target.EndpointID, EndpointIDPrefix); err != nil {
		return err
	}
	if err := ValidateID(r.Message.MessageID, MessageIDPrefix); err != nil {
		return fmt.Errorf("message id: %w", err)
	}
	if r.Message.Kind != string(KindMessage) && r.Message.Kind != string(KindReply) {
		return fmt.Errorf("mektup: invalid message kind %q", r.Message.Kind)
	}
	if !validDigest(r.Message.PayloadSHA256) {
		return fmt.Errorf("mektup: invalid message payload digest")
	}
	for i, ev := range r.Evidence {
		if !ev.State.Valid() {
			return fmt.Errorf("mektup: evidence %d has invalid state", i)
		}
		if ev.At != "" {
			if err := validateTimestamp(ev.At, "evidence.at"); err != nil {
				return err
			}
		}
	}
	for _, w := range r.Warnings {
		if err := w.Validate(); err != nil {
			return err
		}
	}
	if r.ContentRef != nil {
		if r.ContentRef.EndpointID != "" && !strings.HasPrefix(r.ContentRef.EndpointID, EndpointIDPrefix) {
			return fmt.Errorf("mektup: invalid content endpoint")
		}
		if !validDigest(r.ContentRef.PayloadSHA256) {
			return fmt.Errorf("mektup: invalid content digest")
		}
	}
	return nil
}

// ParseReceipt decodes a portable receipt, ignores additive unknown fields,
// and validates the stable contract before returning it.
func ParseReceipt(data []byte) (Receipt, error) {
	var r Receipt
	if err := json.Unmarshal(data, &r); err != nil {
		return Receipt{}, err
	}
	if err := r.Validate(); err != nil {
		return Receipt{}, err
	}
	return r, nil
}

func MarshalReceipt(r Receipt) ([]byte, error) { return json.Marshal(r) }

func validateTimestamp(value, field string) error {
	if value == "" {
		return fmt.Errorf("mektup: %s is required", field)
	}
	if _, err := time.Parse(time.RFC3339Nano, value); err != nil {
		return fmt.Errorf("mektup: invalid %s: %w", field, err)
	}
	return nil
}

func validDigest(value string) bool {
	if len(value) != len("sha256:")+64 || !strings.HasPrefix(value, "sha256:") || strings.ToLower(value) != value {
		return false
	}
	for _, c := range value[len("sha256:"):] {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}

type Event struct {
	Schema      string         `json:"schema"`
	Event       string         `json:"event"`
	EventID     string         `json:"eventId"`
	Sequence    uint64         `json:"sequence"`
	OperationID string         `json:"operationId"`
	Timestamp   string         `json:"timestamp"`
	Terminal    bool           `json:"terminal"`
	OK          bool           `json:"ok"`
	Warnings    []Warning      `json:"warnings"`
	Data        map[string]any `json:"data"`
	Error       *Error         `json:"error,omitempty"`
}

func (e Event) MarshalJSON() ([]byte, error) {
	type plain Event
	if e.Warnings == nil {
		e.Warnings = []Warning{}
	}
	if e.Data == nil {
		e.Data = map[string]any{}
	}
	return json.Marshal(plain(e))
}

func (e Event) Validate() error {
	if e.Schema != EventSchema {
		return fmt.Errorf("mektup: unsupported event schema %q", e.Schema)
	}
	if e.Event == "" {
		return fmt.Errorf("mektup: event name is required")
	}
	if err := ValidateID(e.EventID, EventIDPrefix); err != nil {
		return fmt.Errorf("event id: %w", err)
	}
	if e.Sequence == 0 {
		return fmt.Errorf("mektup: event sequence starts at one")
	}
	if err := ValidateID(e.OperationID, OperationIDPrefix); err != nil {
		return fmt.Errorf("operation id: %w", err)
	}
	if err := validateTimestamp(e.Timestamp, "timestamp"); err != nil {
		return err
	}
	for _, w := range e.Warnings {
		if err := w.Validate(); err != nil {
			return err
		}
	}
	if e.Error != nil {
		if err := e.Error.Validate(); err != nil {
			return err
		}
	}
	return nil
}

func ParseEvent(data []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(data, &e); err != nil {
		return Event{}, err
	}
	if err := e.Validate(); err != nil {
		return Event{}, err
	}
	return e, nil
}

func MarshalEvent(e Event) ([]byte, error) { return json.Marshal(e) }

// EncodeJSONLine emits one compact lifecycle record and exactly one LF.
func EncodeJSONLine(value any) ([]byte, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}
