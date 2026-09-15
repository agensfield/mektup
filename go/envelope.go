package mektup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"
	"unicode/utf8"
)

const EnvelopeSchema = "Mektup/1"

type MessageKind string

const (
	KindMessage MessageKind = "message"
	KindReply   MessageKind = "reply"
)

type ReplyStatus string

const (
	ReplySuccess ReplyStatus = "success"
	ReplyError   ReplyStatus = "error"
)

// Envelope is the target-visible Mektup message. Body is intentionally not
// serialized as a header or included in portable receipts.
type Envelope struct {
	MessageID              string      `json:"message-id"`
	Kind                   MessageKind `json:"kind"`
	FromEndpointID         string      `json:"from-endpoint-id"`
	From                   string      `json:"from"`
	FromKind               string      `json:"from-kind"`
	FromHerdr              string      `json:"from-herdr"`
	ToEndpointID           string      `json:"to-endpoint-id"`
	To                     string      `json:"to"`
	RequestedTarget        string      `json:"requested-target"`
	InReplyTo              string      `json:"in-reply-to"`
	ReplyRequested         bool        `json:"reply-requested"`
	ReplyEndpointID        string      `json:"reply-endpoint-id"`
	ReplyTo                string      `json:"reply-to"`
	ReplyCustodyEndpointID string      `json:"reply-custody-endpoint-id"`
	ReplyCustodyStoreID    string      `json:"reply-custody-store-id"`
	ReplyStatus            ReplyStatus `json:"reply-status"`
	ReplyErrorCode         string      `json:"reply-error-code"`
	SentAt                 string      `json:"sent-at"`
	PayloadBytes           uint64      `json:"payload-bytes"`
	PayloadSHA256          string      `json:"payload-sha256"`
	Provenance             string      `json:"provenance"`
	Body                   string      `json:"-"`
}

// Message is retained as an ergonomic synonym for Envelope.
type Message = Envelope

var envelopeFields = []string{
	"message-id", "kind", "from-endpoint-id", "from", "from-kind", "from-herdr",
	"to-endpoint-id", "to", "requested-target", "in-reply-to", "reply-requested",
	"reply-endpoint-id", "reply-to", "reply-custody-endpoint-id", "reply-custody-store-id",
	"sent-at", "payload-bytes", "payload-sha256", "provenance",
}

func sha256Digest(body string) string {
	sum := sha256.Sum256([]byte(body))
	return "sha256:" + hex.EncodeToString(sum[:])
}

// Validate checks envelope relationships and body integrity.
func (e Envelope) Validate() error {
	if e.Kind != KindMessage && e.Kind != KindReply {
		return fmt.Errorf("mektup: invalid envelope kind %q", e.Kind)
	}
	if err := ValidateID(e.MessageID, MessageIDPrefix); err != nil {
		return err
	}
	if err := ValidateID(e.FromEndpointID, EndpointIDPrefix); err != nil {
		return fmt.Errorf("from endpoint: %w", err)
	}
	if err := ValidateID(e.ToEndpointID, EndpointIDPrefix); err != nil {
		return fmt.Errorf("to endpoint: %w", err)
	}
	if e.To == "" {
		return fmt.Errorf("mektup: destination thread is required")
	}
	if e.FromKind != "agent" && e.FromKind != "human" {
		return fmt.Errorf("mektup: invalid from-kind %q", e.FromKind)
	}
	if e.FromKind == "agent" && e.From == "" {
		return fmt.Errorf("mektup: agent sender thread is required")
	}
	if e.Kind == KindReply {
		if e.InReplyTo == "" {
			return fmt.Errorf("mektup: reply must reference an original message")
		}
		if err := ValidateID(e.InReplyTo, MessageIDPrefix); err != nil {
			return fmt.Errorf("in-reply-to: %w", err)
		}
		if e.ReplyStatus != ReplySuccess && e.ReplyStatus != ReplyError {
			return fmt.Errorf("mektup: invalid reply status %q", e.ReplyStatus)
		}
	} else if e.InReplyTo != "" || e.ReplyStatus != "" || e.ReplyErrorCode != "" {
		return fmt.Errorf("mektup: reply fields are not valid on a message")
	}
	if e.ReplyRequested {
		if err := ValidateID(e.ReplyEndpointID, EndpointIDPrefix); err != nil {
			return fmt.Errorf("reply endpoint: %w", err)
		}
		if e.ReplyTo == "" || e.ReplyCustodyEndpointID == "" {
			return fmt.Errorf("mektup: reply route is incomplete")
		}
		if err := ValidateID(e.ReplyCustodyEndpointID, EndpointIDPrefix); err != nil {
			return fmt.Errorf("reply custody endpoint: %w", err)
		}
		if err := ValidateID(e.ReplyCustodyStoreID, StoreIDPrefix); err != nil {
			return fmt.Errorf("reply custody store: %w", err)
		}
	} else if e.ReplyEndpointID != "" || e.ReplyTo != "" || e.ReplyCustodyEndpointID != "" || e.ReplyCustodyStoreID != "" {
		return fmt.Errorf("mektup: reply route present without reply-requested")
	}
	if e.SentAt == "" {
		return fmt.Errorf("mektup: sent-at is required")
	}
	if t, err := time.Parse(time.RFC3339Nano, e.SentAt); err != nil || !strings.HasSuffix(e.SentAt, "Z") || t.Location() != time.UTC {
		return fmt.Errorf("mektup: sent-at must be UTC RFC3339: %q", e.SentAt)
	}
	if !utf8.ValidString(e.Body) {
		return fmt.Errorf("mektup: body is not valid UTF-8")
	}
	if e.PayloadBytes != uint64(len([]byte(e.Body))) {
		return fmt.Errorf("mektup: payload-bytes mismatch: got %d want %d", e.PayloadBytes, len([]byte(e.Body)))
	}
	if len(e.PayloadSHA256) != len("sha256:")+64 || !strings.HasPrefix(e.PayloadSHA256, "sha256:") || strings.ToLower(e.PayloadSHA256) != e.PayloadSHA256 {
		return fmt.Errorf("mektup: invalid payload-sha256")
	}
	if e.PayloadSHA256 != sha256Digest(e.Body) {
		return fmt.Errorf("mektup: payload-sha256 mismatch")
	}
	if e.Provenance != "observed" {
		return fmt.Errorf("mektup: unsupported provenance %q", e.Provenance)
	}
	return nil
}

// ValidateAddressToThread verifies that an envelope is addressed to the
// currently executing Codex thread. It intentionally does not infer identity.
func (e Envelope) ValidateAddressToThread(currentThread string) error {
	if currentThread == "" || e.To != currentThread {
		return fmt.Errorf("%s: envelope targets %q, current thread is %q", ErrMessageNotAddressedThread, e.To, currentThread)
	}
	return nil
}

// ValidateReplyFor checks the immutable routing relationship between a reply
// and its original request. It does not authenticate the author.
func (e Envelope) ValidateReplyFor(original Envelope) error {
	if e.Kind != KindReply || original.Kind != KindMessage {
		return fmt.Errorf("mektup: invalid message/reply relationship")
	}
	if e.InReplyTo != original.MessageID {
		return fmt.Errorf("mektup: reply references %q, want %q", e.InReplyTo, original.MessageID)
	}
	if e.To != original.ReplyTo || e.ToEndpointID != original.ReplyEndpointID {
		return fmt.Errorf("mektup: reply destination does not match original route")
	}
	return nil
}

func (e Envelope) normalizedForRender() (Envelope, error) {
	if !utf8.ValidString(e.Body) {
		return Envelope{}, fmt.Errorf("mektup: body is not valid UTF-8")
	}
	wantBytes := uint64(len([]byte(e.Body)))
	wantDigest := sha256Digest(e.Body)
	if e.PayloadBytes != 0 && e.PayloadBytes != wantBytes {
		return Envelope{}, fmt.Errorf("mektup: payload-bytes mismatch: got %d want %d", e.PayloadBytes, wantBytes)
	}
	if e.PayloadSHA256 != "" && e.PayloadSHA256 != wantDigest {
		return Envelope{}, fmt.Errorf("mektup: payload-sha256 mismatch")
	}
	e.PayloadBytes = wantBytes
	e.PayloadSHA256 = wantDigest
	if e.SentAt == "" {
		e.SentAt = time.Now().UTC().Format("2006-01-02T15:04:05.000000Z07:00")
	}
	if e.Provenance == "" {
		e.Provenance = "observed"
	}
	return e, e.Validate()
}

func scalarJSON(value any) (string, error) {
	b, err := json.Marshal(value)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

// RenderEnvelope returns the canonical target-visible envelope bytes. Header
// fields are emitted in contract order and body bytes are copied verbatim.
func RenderEnvelope(e Envelope) ([]byte, error) {
	e, err := e.normalizedForRender()
	if err != nil {
		return nil, err
	}
	values := map[string]any{
		"message-id": e.MessageID, "kind": string(e.Kind), "from-endpoint-id": e.FromEndpointID,
		"from": nullableString(e.From), "from-kind": e.FromKind, "from-herdr": nullableString(e.FromHerdr),
		"to-endpoint-id": e.ToEndpointID, "to": e.To, "requested-target": e.RequestedTarget,
		"in-reply-to": nullableString(e.InReplyTo), "reply-requested": e.ReplyRequested,
		"reply-endpoint-id": nullableString(e.ReplyEndpointID), "reply-to": nullableString(e.ReplyTo),
		"reply-custody-endpoint-id": nullableString(e.ReplyCustodyEndpointID), "reply-custody-store-id": nullableString(e.ReplyCustodyStoreID),
		"sent-at": e.SentAt, "payload-bytes": e.PayloadBytes, "payload-sha256": e.PayloadSHA256, "provenance": e.Provenance,
	}
	var out strings.Builder
	out.WriteString("[Mektup/1]\n")
	fields := envelopeFields
	if e.Kind == KindReply {
		// Reply status is part of the canonical reply extension and appears
		// before the timestamp. The sender-defined error code is optional.
		fields = append(append([]string(nil), envelopeFields[:15]...), "reply-status")
		if e.ReplyErrorCode != "" {
			fields = append(fields, "reply-error-code")
		}
		fields = append(fields, envelopeFields[15:]...)
		values["reply-status"] = string(e.ReplyStatus)
		values["reply-error-code"] = nullableString(e.ReplyErrorCode)
	}
	for _, field := range fields {
		v, err := scalarJSON(values[field])
		if err != nil {
			return nil, fmt.Errorf("mektup: encode %s: %w", field, err)
		}
		out.WriteString(field)
		out.WriteString(": ")
		out.WriteString(v)
		out.WriteByte('\n')
	}
	out.WriteString("---\n")
	out.WriteString(e.Body)
	return []byte(out.String()), nil
}

func (e Envelope) Render() ([]byte, error) { return RenderEnvelope(e) }

func (e Envelope) MarshalText() ([]byte, error) { return RenderEnvelope(e) }

func MarshalEnvelope(e Envelope) ([]byte, error) { return RenderEnvelope(e) }

func ParseEnvelopeString(input string) (Envelope, error) { return ParseEnvelope([]byte(input)) }

func (e *Envelope) UnmarshalText(input []byte) error {
	parsed, err := ParseEnvelope(input)
	if err != nil {
		return err
	}
	*e = parsed
	return nil
}

// MustRenderEnvelope is the panic-on-invalid convenience used by fixed golden
// fixtures and package consumers that have already validated their envelope.
func MustRenderEnvelope(e Envelope) []byte {
	b, err := RenderEnvelope(e)
	if err != nil {
		panic(err)
	}
	return b
}

type scalar struct {
	value any
}

func parseScalar(raw string) (scalar, error) {
	if strings.ContainsAny(raw, "\r\n") {
		return scalar{}, fmt.Errorf("mektup: header scalar spans lines")
	}
	var v any
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&v); err != nil {
		return scalar{}, fmt.Errorf("mektup: malformed JSON scalar: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return scalar{}, fmt.Errorf("mektup: multiple JSON values in header")
		}
		return scalar{}, fmt.Errorf("mektup: malformed JSON scalar: %w", err)
	}
	switch v.(type) {
	case nil, string, bool, json.Number:
		return scalar{v}, nil
	default:
		return scalar{}, fmt.Errorf("mektup: header value must be a JSON scalar string, boolean, or null")
	}
}

func getString(fields map[string]scalar, name string, nullable bool) (string, error) {
	f, ok := fields[name]
	if !ok {
		return "", fmt.Errorf("mektup: missing header %q", name)
	}
	if f.value == nil {
		if nullable {
			return "", nil
		}
		return "", fmt.Errorf("mektup: header %q cannot be null", name)
	}
	v, ok := f.value.(string)
	if !ok {
		return "", fmt.Errorf("mektup: header %q must be a JSON string", name)
	}
	return v, nil
}

func getBool(fields map[string]scalar, name string) (bool, error) {
	f, ok := fields[name]
	if !ok {
		return false, fmt.Errorf("mektup: missing header %q", name)
	}
	v, ok := f.value.(bool)
	if !ok {
		return false, fmt.Errorf("mektup: header %q must be a JSON boolean", name)
	}
	return v, nil
}

func getUint(fields map[string]scalar, name string) (uint64, error) {
	f, ok := fields[name]
	if !ok {
		return 0, fmt.Errorf("mektup: missing header %q", name)
	}
	var n uint64
	b, _ := json.Marshal(f.value)
	if err := json.Unmarshal(b, &n); err != nil {
		return 0, fmt.Errorf("mektup: header %q must be an unsigned integer", name)
	}
	return n, nil
}

// ParseEnvelope parses and validates a complete target-visible envelope. It
// stops at the first exact delimiter after the header; delimiter-looking body
// lines remain ordinary body bytes.
func ParseEnvelope(input []byte) (Envelope, error) {
	if !utf8.Valid(input) {
		return Envelope{}, fmt.Errorf("mektup: envelope is not valid UTF-8")
	}
	const marker = "[Mektup/1]\n"
	if !bytes.HasPrefix(input, []byte(marker)) {
		return Envelope{}, fmt.Errorf("mektup: unsupported or malformed envelope version")
	}
	rest := input[len(marker):]
	fields := make(map[string]scalar, len(envelopeFields))
	known := make(map[string]struct{}, len(envelopeFields))
	for _, f := range envelopeFields {
		known[f] = struct{}{}
	}
	known["reply-status"] = struct{}{}
	known["reply-error-code"] = struct{}{}
	var body []byte
	for {
		i := bytes.IndexByte(rest, '\n')
		if i < 0 {
			return Envelope{}, fmt.Errorf("mektup: missing header delimiter")
		}
		line := string(rest[:i])
		rest = rest[i+1:]
		if line == "---" {
			body = rest
			break
		}
		if line == "" || strings.ContainsRune(line, '\r') {
			return Envelope{}, fmt.Errorf("mektup: malformed header line")
		}
		key, raw, ok := strings.Cut(line, ": ")
		if !ok || key == "" {
			return Envelope{}, fmt.Errorf("mektup: malformed header line %q", line)
		}
		if _, ok := known[key]; !ok {
			return Envelope{}, fmt.Errorf("mektup: unknown header %q", key)
		}
		if _, ok := fields[key]; ok {
			return Envelope{}, fmt.Errorf("mektup: duplicate header %q", key)
		}
		value, err := parseScalar(raw)
		if err != nil {
			return Envelope{}, fmt.Errorf("%s: %w", key, err)
		}
		fields[key] = value
	}
	get := func(name string) (string, error) { return getString(fields, name, false) }
	getNullable := func(name string) (string, error) { return getString(fields, name, true) }
	messageID, err := get("message-id")
	if err != nil {
		return Envelope{}, err
	}
	kind, err := get("kind")
	if err != nil {
		return Envelope{}, err
	}
	fromEndpoint, err := get("from-endpoint-id")
	if err != nil {
		return Envelope{}, err
	}
	from, err := getNullable("from")
	if err != nil {
		return Envelope{}, err
	}
	fromKind, err := get("from-kind")
	if err != nil {
		return Envelope{}, err
	}
	fromHerdr, err := getNullable("from-herdr")
	if err != nil {
		return Envelope{}, err
	}
	toEndpoint, err := get("to-endpoint-id")
	if err != nil {
		return Envelope{}, err
	}
	to, err := get("to")
	if err != nil {
		return Envelope{}, err
	}
	requested, err := get("requested-target")
	if err != nil {
		return Envelope{}, err
	}
	inReply, err := getNullable("in-reply-to")
	if err != nil {
		return Envelope{}, err
	}
	replyRequested, err := getBool(fields, "reply-requested")
	if err != nil {
		return Envelope{}, err
	}
	replyEndpoint, err := getNullable("reply-endpoint-id")
	if err != nil {
		return Envelope{}, err
	}
	replyTo, err := getNullable("reply-to")
	if err != nil {
		return Envelope{}, err
	}
	custodyEndpoint, err := getNullable("reply-custody-endpoint-id")
	if err != nil {
		return Envelope{}, err
	}
	custodyStore, err := getNullable("reply-custody-store-id")
	if err != nil {
		return Envelope{}, err
	}
	replyStatus, err := getNullableOptional(fields, "reply-status")
	if err != nil {
		return Envelope{}, err
	}
	replyError, err := getNullableOptional(fields, "reply-error-code")
	if err != nil {
		return Envelope{}, err
	}
	sentAt, err := get("sent-at")
	if err != nil {
		return Envelope{}, err
	}
	payloadBytes, err := getUint(fields, "payload-bytes")
	if err != nil {
		return Envelope{}, err
	}
	digest, err := get("payload-sha256")
	if err != nil {
		return Envelope{}, err
	}
	provenance, err := get("provenance")
	if err != nil {
		return Envelope{}, err
	}
	if MessageKind(kind) == KindMessage {
		if _, ok := fields["reply-status"]; ok {
			return Envelope{}, fmt.Errorf("mektup: reply-status is not valid on a message")
		}
		if _, ok := fields["reply-error-code"]; ok {
			return Envelope{}, fmt.Errorf("mektup: reply-error-code is not valid on a message")
		}
	}
	if MessageKind(kind) == KindReply {
		if _, ok := fields["reply-status"]; !ok {
			return Envelope{}, fmt.Errorf("mektup: reply-status is required on a reply")
		}
	}
	e := Envelope{MessageID: messageID, Kind: MessageKind(kind), FromEndpointID: fromEndpoint, From: from,
		FromKind: fromKind, FromHerdr: fromHerdr, ToEndpointID: toEndpoint, To: to, RequestedTarget: requested,
		InReplyTo: inReply, ReplyRequested: replyRequested, ReplyEndpointID: replyEndpoint, ReplyTo: replyTo,
		ReplyCustodyEndpointID: custodyEndpoint, ReplyCustodyStoreID: custodyStore, ReplyStatus: ReplyStatus(replyStatus),
		ReplyErrorCode: replyError, SentAt: sentAt, PayloadBytes: payloadBytes, PayloadSHA256: digest,
		Provenance: provenance, Body: string(body)}
	if err := e.Validate(); err != nil {
		return Envelope{}, err
	}
	return e, nil
}

func getNullableOptional(fields map[string]scalar, name string) (string, error) {
	if _, ok := fields[name]; !ok {
		return "", nil
	}
	return getString(fields, name, true)
}

func ParseMessage(input []byte) (Message, error) { return ParseEnvelope(input) }
