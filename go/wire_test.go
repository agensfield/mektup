package mektup

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func fixtureEnvelope() Envelope {
	return Envelope{
		MessageID: NewMessageID(), Kind: KindMessage, FromEndpointID: NewEndpointID(),
		From: "codex://local/thread/source", FromKind: "agent", FromHerdr: "herdr://local/agent/a",
		ToEndpointID: NewEndpointID(), To: "codex://local/thread/destination", RequestedTarget: "agent\nforged: true",
		ReplyRequested: true, ReplyEndpointID: NewEndpointID(), ReplyTo: "codex://local/thread/source",
		ReplyCustodyEndpointID: NewEndpointID(), ReplyCustodyStoreID: NewStoreID(),
		SentAt: "2026-09-15T03:00:00.000000Z", Provenance: "observed",
		Body: "ünicode🙂\n---\nbytes",
	}
}

func TestEnvelopeRoundTripPreservesUTF8BytesAndDelimiter(t *testing.T) {
	e := fixtureEnvelope()
	want, err := RenderEnvelope(e)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(want, []byte("requested-target: \"agent\\nforged: true\"\n")) {
		t.Fatalf("header injection was not JSON escaped: %q", want)
	}
	got, err := ParseEnvelope(want)
	if err != nil {
		t.Fatal(err)
	}
	if got.Body != e.Body || got.PayloadBytes != uint64(len([]byte(e.Body))) {
		t.Fatalf("body changed: %#v", got)
	}
	if got.RequestedTarget != e.RequestedTarget {
		t.Fatalf("escaped header changed: %q", got.RequestedTarget)
	}
	if !bytes.HasSuffix(want, []byte(e.Body)) {
		t.Fatal("render added bytes after body")
	}
}

func TestEnvelopeDeterministic(t *testing.T) {
	e := fixtureEnvelope()
	a, err := RenderEnvelope(e)
	if err != nil {
		t.Fatal(err)
	}
	b, err := RenderEnvelope(e)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(a, b) {
		t.Fatalf("same envelope rendered differently:\n%s\n%s", a, b)
	}
	if !strings.Contains(string(a), "[Mektup/1]\nmessage-id: ") {
		t.Fatal("missing canonical opening")
	}
}

func TestEnvelopeRejectsMalformedAndMismatchedInputs(t *testing.T) {
	e := fixtureEnvelope()
	b, err := RenderEnvelope(e)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := ParseEnvelope(b)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := append([]byte(nil), b...)
	needle := []byte("kind: \"message\"\n")
	duplicate = bytes.Replace(duplicate, needle, append(needle, needle...), 1)
	if _, err := ParseEnvelope(duplicate); err == nil {
		t.Fatal("duplicate header accepted")
	}
	wrongDigest := bytes.Replace(b, []byte(parsed.PayloadSHA256), []byte("sha256:"+strings.Repeat("0", 64)), 1)
	if _, err := ParseEnvelope(wrongDigest); err == nil {
		t.Fatal("digest mismatch accepted")
	}
	if err := e.ValidateAddressToThread("codex://local/thread/other"); err == nil {
		t.Fatal("fork/target mismatch accepted")
	}
	if err := e.ValidateAddressToThread(e.To); err != nil {
		t.Fatal(err)
	}
}

func TestReplyRelationshipAndUnknownReceiptFields(t *testing.T) {
	original := fixtureEnvelope()
	reply := fixtureEnvelope()
	reply.Kind = KindReply
	reply.MessageID = NewMessageID()
	reply.InReplyTo = original.MessageID
	reply.To = original.ReplyTo
	reply.ToEndpointID = original.ReplyEndpointID
	reply.ReplyRequested = false
	reply.ReplyEndpointID, reply.ReplyTo = "", ""
	reply.ReplyCustodyEndpointID, reply.ReplyCustodyStoreID = "", ""
	reply.ReplyStatus = ReplySuccess
	reply.PayloadBytes = uint64(len([]byte(reply.Body)))
	reply.PayloadSHA256 = sha256Digest(reply.Body)
	if err := reply.ValidateReplyFor(original); err != nil {
		t.Fatal(err)
	}
	if err := reply.Validate(); err != nil {
		t.Fatal(err)
	}
	original.PayloadBytes = uint64(len([]byte(original.Body)))
	original.PayloadSHA256 = sha256Digest(original.Body)
	receipt := Receipt{Schema: ReceiptSchema, ReceiptID: NewReceiptID(), OperationID: NewOperationID(), Operation: "send", State: StateAccepted,
		Source: Identity{EndpointID: NewEndpointID()}, Target: Identity{EndpointID: NewEndpointID()},
		Message:   ReceiptMessage{MessageID: original.MessageID, Kind: string(KindMessage), PayloadBytes: original.PayloadBytes, PayloadSHA256: original.PayloadSHA256},
		CreatedAt: original.SentAt, UpdatedAt: original.SentAt}
	b, err := json.Marshal(receipt)
	if err != nil {
		t.Fatal(err)
	}
	var decoded Receipt
	if err := json.Unmarshal(append(bytes.TrimSpace(b), []byte(`,"unknown":true}`)...), &decoded); err == nil {
		t.Fatal("constructed invalid JSON unexpectedly decoded")
	}
	var fixture map[string]any
	if err := json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	fixture["unknown"] = true
	b, _ = json.Marshal(fixture)
	if err := json.Unmarshal(b, &decoded); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Validate(); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(b, []byte("Body")) || bytes.Contains(b, []byte(original.Body)) {
		t.Fatal("receipt serialized body")
	}
}

func TestEventSequencer(t *testing.T) {
	s := NewEventSequencer(NewOperationID())
	a, err := s.Next("send.accepted", false, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if a.Sequence != 1 || a.Terminal {
		t.Fatalf("unexpected first event: %#v", a)
	}
	b, err := s.Next("reply.accepted", true, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if b.Sequence != 2 || !b.Terminal {
		t.Fatalf("unexpected terminal event: %#v", b)
	}
	if _, err := s.Next("late", false, true, nil); err == nil {
		t.Fatal("event after terminal accepted")
	}
	if err := s.Append(Event{Schema: EventSchema, Event: "bad", EventID: NewEventID(), Sequence: 4, OperationID: s.OperationID(), Timestamp: a.Timestamp, OK: true}); err == nil {
		t.Fatal("sequence gap accepted")
	}
}
