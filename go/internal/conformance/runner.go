// Package conformance consumes the language-neutral Mektup contracts.
//
// This is deliberately a semantic consumer, not another JSON Schema
// validator. The repository's AJV job owns schema coverage; this package
// exercises the public Go wire APIs and the contract's state/exit mappings.
package conformance

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/cli"
)

const (
	manifestPath    = "contracts/fixtures/v1/manifest.json"
	scenariosPath   = "conformance/v1/scenarios.json"
	transitionsPath = "conformance/v1/state-transitions.json"
	fixturesDir     = "contracts/fixtures/v1"
)

// Root locates the repository independent of the caller's working directory.
// The source-file fallback matters for `go test` launched from a package
// directory, while the cwd walk supports installed/source-built commands.
func Root() (string, error) {
	if _, file, _, ok := runtime.Caller(0); ok {
		if root, err := walkForRoot(filepath.Dir(file)); err == nil {
			return root, nil
		}
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return walkForRoot(cwd)
}

func walkForRoot(start string) (string, error) {
	current, err := filepath.Abs(start)
	if err != nil {
		return "", err
	}
	for {
		if isRoot(current) {
			return current, nil
		}
		parent := filepath.Dir(current)
		if parent == current {
			break
		}
		current = parent
	}
	return "", fmt.Errorf("mektup: repository root not found from %q", start)
}

func isRoot(dir string) bool {
	for _, name := range []string{"contracts", "conformance", "go"} {
		if info, err := os.Stat(filepath.Join(dir, name)); err != nil || !info.IsDir() {
			return false
		}
	}
	return true
}

type manifest struct {
	Schema          string            `json:"schema"`
	ContractVersion string            `json:"contractVersion"`
	Fixtures        []manifestFixture `json:"fixtures"`
}

type manifestFixture struct {
	Kind   string `json:"kind"`
	Schema string `json:"schema"`
	Path   string `json:"path"`
	Expect string `json:"expected"`
}

type scenariosDocument struct {
	Schema      string             `json:"schema"`
	Version     string             `json:"version"`
	SpecVersion string             `json:"specVersion"`
	Source      string             `json:"source"`
	Profiles    map[string]profile `json:"profiles"`
	Scenarios   []scenario         `json:"scenarios"`
}

type profile struct {
	Transitions []string `json:"transitions"`
	Terminal    terminal `json:"terminal"`
}

type terminal struct {
	Event string `json:"event"`
	Exit  int    `json:"exit"`
	State string `json:"state"`
}

type scenario struct {
	ID         string   `json:"id"`
	Matrix     string   `json:"matrix"`
	Profile    string   `json:"profile"`
	Observable []string `json:"observable"`
}

type transitionsDocument struct {
	Schema      string       `json:"schema"`
	Contract    string       `json:"contract"`
	SpecVersion string       `json:"specVersion"`
	States      []string     `json:"states"`
	Transitions []transition `json:"transitions"`
	Forbidden   []forbidden  `json:"forbiddenTransitions"`
	Invariants  []string     `json:"invariants"`
}

type transition struct {
	From  string `json:"from"`
	Event string `json:"event"`
	To    string `json:"to"`
}

type forbidden struct {
	From string `json:"from"`
	To   string `json:"to"`
}

// FixtureResult is one bounded release-evidence record.
type FixtureResult struct {
	Path     string
	Kind     string
	Expected string
	Observed string
}

// ScenarioResult records declarative checks only. It is intentionally not a
// live acceptance claim.
type ScenarioResult struct {
	ID       string
	Matrix   string
	Profile  string
	Exit     int
	Terminal string
	Coverage string
}

// Summary is deterministic when emitted by WriteEvidence.
type Summary struct {
	ContractVersion string
	Fixtures        []FixtureResult
	Scenarios       []ScenarioResult
	UncoveredLive   map[string]int
}

// Run consumes all root fixtures, scenarios and state transitions.
func Run(root string) (Summary, error) {
	var out Summary
	out.UncoveredLive = map[string]int{}
	if root == "" {
		var err error
		root, err = Root()
		if err != nil {
			return out, err
		}
	}
	m, err := readJSON[manifest](filepath.Join(root, manifestPath))
	if err != nil {
		return out, fmt.Errorf("manifest: %w", err)
	}
	if m.Schema != "mektup/contracts/v1/manifest" || m.ContractVersion == "" {
		return out, fmt.Errorf("manifest: unsupported contract metadata")
	}
	out.ContractVersion = m.ContractVersion
	fixtureMap := make(map[string]manifestFixture, len(m.Fixtures))
	for _, f := range m.Fixtures {
		if f.Path == "" || filepath.IsAbs(f.Path) || filepath.Clean(f.Path) != f.Path || strings.HasPrefix(f.Path, "../") {
			return out, fmt.Errorf("manifest: unsafe fixture path %q", f.Path)
		}
		if err := validateManifestFixture(f); err != nil {
			return out, err
		}
		if _, exists := fixtureMap[f.Path]; exists {
			return out, fmt.Errorf("manifest: duplicate fixture %q", f.Path)
		}
		fixtureMap[f.Path] = f
	}
	entries, err := os.ReadDir(filepath.Join(root, fixturesDir))
	if err != nil {
		return out, err
	}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") || entry.Name() == "manifest.json" {
			continue
		}
		if _, ok := fixtureMap[entry.Name()]; !ok {
			return out, fmt.Errorf("manifest: fixture is not listed: %s", entry.Name())
		}
	}
	paths := make([]string, 0, len(m.Fixtures))
	for p := range fixtureMap {
		paths = append(paths, p)
	}
	sort.Strings(paths)
	for _, path := range paths {
		f := fixtureMap[path]
		data, readErr := os.ReadFile(filepath.Join(root, fixturesDir, path))
		if readErr != nil {
			// A missing (or unreadable) fixture is a broken manifest/repository,
			// never a valid way to satisfy an expected-invalid entry.
			return out, fmt.Errorf("fixture %s: read: %w", path, readErr)
		}
		semanticErr := validateFixture(f, data, root)
		result := FixtureResult{Path: path, Kind: f.Kind, Expected: expectedLabel(f)}
		if f.Expect == "invalid" {
			if semanticErr == nil {
				return out, fmt.Errorf("fixture %s: expected invalid, accepted", path)
			}
			result.Observed = "invalid"
		} else {
			if semanticErr != nil {
				return out, fmt.Errorf("fixture %s: %w", path, semanticErr)
			}
			result.Observed = "valid"
		}
		out.Fixtures = append(out.Fixtures, result)
	}
	if err := validateEventSequence(root); err != nil {
		return out, err
	}

	s, err := readJSON[scenariosDocument](filepath.Join(root, scenariosPath))
	if err != nil {
		return out, fmt.Errorf("scenarios: %w", err)
	}
	t, err := readJSON[transitionsDocument](filepath.Join(root, transitionsPath))
	if err != nil {
		return out, fmt.Errorf("transitions: %w", err)
	}
	if err := validateScenarioDocuments(s, t); err != nil {
		return out, err
	}
	for _, item := range s.Scenarios {
		p := s.Profiles[item.Profile]
		out.Scenarios = append(out.Scenarios, ScenarioResult{ID: item.ID, Matrix: item.Matrix, Profile: item.Profile, Exit: p.Terminal.Exit, Terminal: p.Terminal.Event, Coverage: "declarative"})
		out.UncoveredLive[item.Matrix]++
	}
	return out, nil
}

func expectedLabel(f manifestFixture) string {
	if f.Expect == "invalid" {
		return "invalid"
	}
	return "valid"
}

func validateManifestFixture(f manifestFixture) error {
	if f.Expect != "" && f.Expect != "valid" && f.Expect != "invalid" {
		return fmt.Errorf("manifest: fixture %q has unknown expected value %q", f.Path, f.Expect)
	}
	want, ok := map[string]string{
		"envelope-metadata": "Mektup/1",
		"envelope-rendered": "Mektup/1",
		"receipt":           "mektup/receipt/v1",
		"event":             "mektup/event/v1",
		"warning":           "mektup/warning/v1",
		"error":             "mektup/error/v1",
		"control":           "mektup/control/v1",
		"control-negative":  "mektup/control/v1",
	}[f.Kind]
	if !ok {
		return fmt.Errorf("manifest: fixture %q has unknown kind %q", f.Path, f.Kind)
	}
	if f.Schema == "" || f.Schema != want {
		return fmt.Errorf("manifest: fixture %q schema %q, want %q", f.Path, f.Schema, want)
	}
	return nil
}

func readJSON[T any](path string) (T, error) {
	var value T
	data, err := os.ReadFile(path)
	if err != nil {
		return value, err
	}
	if err := json.Unmarshal(data, &value); err != nil {
		return value, err
	}
	return value, nil
}

func validateFixture(f manifestFixture, data []byte, root string) error {
	switch f.Kind {
	case "envelope-metadata":
		return validateEnvelopeMetadata(data, root)
	case "envelope-rendered":
		return validateRenderedEnvelope(data)
	case "receipt":
		return validateReceiptFixture(data)
	case "event":
		return validateEventFixture(data)
	case "warning":
		var warning mektup.Warning
		if err := json.Unmarshal(data, &warning); err != nil {
			return err
		}
		return warning.Validate()
	case "error":
		return validateErrorFixture(data)
	case "control":
		return validateControlFixture(data)
	case "control-negative":
		return validateControlFixture(data)
	default:
		return fmt.Errorf("unsupported manifest kind %q", f.Kind)
	}
}

func validateRenderedEnvelope(data []byte) error {
	e, err := mektup.ParseEnvelope(data)
	if err != nil {
		return err
	}
	rendered, err := mektup.RenderEnvelope(e)
	if err != nil {
		return err
	}
	if !bytes.Equal(rendered, data) {
		return errors.New("canonical envelope rendering differs from fixture")
	}
	if e.PayloadBytes != uint64(len([]byte(e.Body))) {
		return errors.New("envelope payload byte count does not match body")
	}
	if digest(e.Body) != e.PayloadSHA256 {
		return errors.New("envelope payload digest does not match body")
	}
	return nil
}

func validateEnvelopeMetadata(data []byte, root string) error {
	// Envelope implements encoding.TextUnmarshaler for the target-visible
	// rendering, so JSON metadata must be decoded through a method-free alias.
	var metadata struct {
		MessageID              string             `json:"message-id"`
		Kind                   mektup.MessageKind `json:"kind"`
		FromEndpointID         string             `json:"from-endpoint-id"`
		From                   string             `json:"from"`
		FromKind               string             `json:"from-kind"`
		FromHerdr              string             `json:"from-herdr"`
		ToEndpointID           string             `json:"to-endpoint-id"`
		To                     string             `json:"to"`
		RequestedTarget        string             `json:"requested-target"`
		InReplyTo              string             `json:"in-reply-to"`
		ReplyRequested         bool               `json:"reply-requested"`
		ReplyEndpointID        string             `json:"reply-endpoint-id"`
		ReplyTo                string             `json:"reply-to"`
		ReplyCustodyEndpointID string             `json:"reply-custody-endpoint-id"`
		ReplyCustodyStoreID    string             `json:"reply-custody-store-id"`
		ReplyStatus            mektup.ReplyStatus `json:"reply-status"`
		ReplyErrorCode         string             `json:"reply-error-code"`
		SentAt                 string             `json:"sent-at"`
		PayloadBytes           uint64             `json:"payload-bytes"`
		PayloadSHA256          string             `json:"payload-sha256"`
		Provenance             string             `json:"provenance"`
	}
	if err := json.Unmarshal(data, &metadata); err != nil {
		return err
	}
	e := mektup.Envelope{MessageID: metadata.MessageID, Kind: metadata.Kind, FromEndpointID: metadata.FromEndpointID, From: metadata.From, FromKind: metadata.FromKind, FromHerdr: metadata.FromHerdr, ToEndpointID: metadata.ToEndpointID, To: metadata.To, RequestedTarget: metadata.RequestedTarget, InReplyTo: metadata.InReplyTo, ReplyRequested: metadata.ReplyRequested, ReplyEndpointID: metadata.ReplyEndpointID, ReplyTo: metadata.ReplyTo, ReplyCustodyEndpointID: metadata.ReplyCustodyEndpointID, ReplyCustodyStoreID: metadata.ReplyCustodyStoreID, ReplyStatus: metadata.ReplyStatus, ReplyErrorCode: metadata.ReplyErrorCode, SentAt: metadata.SentAt, PayloadBytes: metadata.PayloadBytes, PayloadSHA256: metadata.PayloadSHA256, Provenance: metadata.Provenance}
	if err := validateEnvelopeFields(e); err != nil {
		return err
	}
	// A target-visible body is available for the request fixture. Exercise the
	// complete public parser/renderer when its paired fixture is present.
	if e.Kind == mektup.KindMessage {
		raw, err := os.ReadFile(filepath.Join(root, fixturesDir, "envelope-agent-request.txt"))
		if err != nil {
			return err
		}
		parsed, err := mektup.ParseEnvelope(raw)
		if err != nil {
			return err
		}
		if parsed.MessageID != e.MessageID || parsed.PayloadBytes != e.PayloadBytes || parsed.PayloadSHA256 != e.PayloadSHA256 {
			return errors.New("metadata and rendered envelope disagree")
		}
		canonical, err := mektup.RenderEnvelope(parsed)
		if err != nil {
			return err
		}
		if !bytes.Equal(canonical, raw) {
			return errors.New("request envelope is not canonically rendered")
		}
	}
	return nil
}

func validateEnvelopeFields(e mektup.Envelope) error {
	if e.Kind != mektup.KindMessage && e.Kind != mektup.KindReply {
		return fmt.Errorf("invalid envelope kind %q", e.Kind)
	}
	for _, item := range []struct{ name, value, prefix string }{
		{"message-id", e.MessageID, mektup.MessageIDPrefix},
		{"from-endpoint-id", e.FromEndpointID, mektup.EndpointIDPrefix},
		{"to-endpoint-id", e.ToEndpointID, mektup.EndpointIDPrefix},
	} {
		if err := mektup.ValidateID(item.value, item.prefix); err != nil {
			return fmt.Errorf("%s: %w", item.name, err)
		}
	}
	if e.To == "" || e.RequestedTarget == "" || e.FromKind == "" || e.SentAt == "" || e.Provenance != "observed" {
		return errors.New("envelope metadata has missing stable fields")
	}
	if _, err := mektup.ParseThreadURI(e.To); err != nil {
		return fmt.Errorf("to: %w", err)
	}
	if e.FromKind == "agent" {
		if _, err := mektup.ParseThreadURI(e.From); err != nil {
			return fmt.Errorf("from: %w", err)
		}
	}
	if strings.HasPrefix(e.RequestedTarget, "codex://") {
		if _, err := mektup.ParseThreadURI(e.RequestedTarget); err != nil {
			return fmt.Errorf("requested-target: %w", err)
		}
	} else if strings.HasPrefix(e.RequestedTarget, "herdr://") {
		if _, err := mektup.ParseHerdrURI(e.RequestedTarget); err != nil {
			return fmt.Errorf("requested-target: %w", err)
		}
	}
	if _, err := time.Parse(time.RFC3339Nano, e.SentAt); err != nil {
		return fmt.Errorf("sent-at: %w", err)
	}
	if !validDigest(e.PayloadSHA256) || e.PayloadBytes == 0 {
		return errors.New("envelope payload metadata is invalid")
	}
	if e.Kind == mektup.KindReply {
		if e.InReplyTo == "" || mektup.ValidateID(e.InReplyTo, mektup.MessageIDPrefix) != nil || (e.ReplyStatus != mektup.ReplySuccess && e.ReplyStatus != mektup.ReplyError) {
			return errors.New("reply envelope relationship is incomplete")
		}
		if e.ReplyStatus == mektup.ReplySuccess && e.ReplyErrorCode != "" {
			return errors.New("successful reply carries an error code")
		}
	} else if e.InReplyTo != "" || e.ReplyStatus != "" || e.ReplyErrorCode != "" {
		return errors.New("message envelope carries reply-only fields")
	}
	return nil
}

type wireReceipt struct {
	Schema      string               `json:"schema"`
	ReceiptID   string               `json:"receiptId"`
	OperationID string               `json:"operationId"`
	Operation   string               `json:"operation"`
	State       mektup.EvidenceState `json:"state"`
	Source      wireIdentity         `json:"source"`
	Target      wireIdentity         `json:"target"`
	Message     wireMessage          `json:"message"`
	Evidence    []wireEvidence       `json:"evidence"`
	Warnings    []mektup.Warning     `json:"warnings"`
	CreatedAt   string               `json:"createdAt"`
	UpdatedAt   string               `json:"updatedAt"`
}

type wireIdentity struct {
	Endpoint struct {
		ID        string `json:"id"`
		Alias     string `json:"alias"`
		Transport string `json:"transport"`
		Server    struct {
			Version       string `json:"version"`
			Compatibility string `json:"compatibility"`
		} `json:"server"`
	} `json:"endpoint"`
	Thread *struct {
		ID string `json:"id"`
	} `json:"thread"`
	Requested *string `json:"requestedSelector"`
	Resolved  *string `json:"resolvedSelector"`
}

type wireMessage struct {
	MessageID       string  `json:"messageId"`
	InReplyTo       *string `json:"inReplyTo"`
	Kind            string  `json:"kind"`
	ReplyRequested  bool    `json:"replyRequested"`
	BodyBytes       uint64  `json:"bodyBytes"`
	BodySHA256      string  `json:"bodySha256"`
	ClientMessageID string  `json:"clientUserMessageId"`
	TurnID          *string `json:"turnId"`
	AcceptedAt      string  `json:"acceptedAt"`
}

type wireEvidence struct {
	State      mektup.EvidenceState `json:"state"`
	ObservedAt string               `json:"observedAt"`
	Source     string               `json:"source"`
}

func validateReceiptFixture(data []byte) error {
	var wire wireReceipt
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	identity := func(in wireIdentity) mektup.ReceiptIdentity {
		out := mektup.ReceiptIdentity{EndpointID: in.Endpoint.ID, Alias: in.Endpoint.Alias, Transport: in.Endpoint.Transport, ServerVersion: in.Endpoint.Server.Version, Compatibility: in.Endpoint.Server.Compatibility}
		if in.Thread != nil {
			out.ThreadID = in.Thread.ID
		}
		if in.Requested != nil {
			out.Requested = *in.Requested
		}
		if in.Resolved != nil {
			out.Resolved = *in.Resolved
		}
		return out
	}
	inReply := ""
	if wire.Message.InReplyTo != nil {
		inReply = *wire.Message.InReplyTo
	}
	turn := ""
	if wire.Message.TurnID != nil {
		turn = *wire.Message.TurnID
	}
	receipt := mektup.Receipt{Schema: wire.Schema, ReceiptID: wire.ReceiptID, OperationID: wire.OperationID, Operation: wire.Operation, State: wire.State, Source: identity(wire.Source), Target: identity(wire.Target), Message: mektup.ReceiptMessage{MessageID: wire.Message.MessageID, InReplyTo: inReply, Kind: wire.Message.Kind, ReplyRequested: wire.Message.ReplyRequested, PayloadBytes: wire.Message.BodyBytes, PayloadSHA256: wire.Message.BodySHA256, ClientMessageID: wire.Message.ClientMessageID, TurnID: turn, AcceptedAt: wire.Message.AcceptedAt}, Warnings: wire.Warnings, CreatedAt: wire.CreatedAt, UpdatedAt: wire.UpdatedAt}
	for _, ev := range wire.Evidence {
		receipt.Evidence = append(receipt.Evidence, mektup.EvidenceRecord{State: ev.State, At: ev.ObservedAt, Kind: ev.Source})
	}
	if err := receipt.Validate(); err != nil {
		return err
	}
	if wire.State == mektup.StateReplyAccepted && len(receipt.Evidence) < 2 {
		return errors.New("reply-accepted receipt lacks accepted evidence")
	}
	return nil
}

func validateEventFixture(data []byte) error {
	e, err := mektup.ParseEvent(data)
	if err != nil {
		return err
	}
	if e.OperationID == "" || e.Sequence == 0 {
		return errors.New("event identity is incomplete")
	}
	return nil
}

func validateErrorFixture(data []byte) error {
	var wrapper struct {
		Error mektup.Error `json:"error"`
	}
	if err := json.Unmarshal(data, &wrapper); err != nil {
		return err
	}
	if err := wrapper.Error.Validate(); err != nil {
		return err
	}
	if wrapper.Error.Code == mektup.ErrOutcomeUnknown && cli.ExitCodeForError(string(wrapper.Error.Code)) != cli.ExitUnknown {
		return errors.New("outcome_unknown exit mapping changed")
	}
	return nil
}

func validateControlFixture(data []byte) error {
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		return err
	}
	stringValue := func(name string) (string, error) {
		v, ok := value[name].(string)
		if !ok || v == "" {
			return "", fmt.Errorf("control %s is required", name)
		}
		return v, nil
	}
	if schema, err := stringValue("schema"); err != nil || schema != "mektup/control/v1" {
		return errors.New("invalid control schema")
	}
	kind, err := stringValue("kind")
	if err != nil || (kind != "request" && kind != "result") {
		return errors.New("invalid control kind")
	}
	op, err := stringValue("operation")
	if err != nil {
		return err
	}
	if op != "claim" && op != "heartbeat" && op != "commit" && op != "abandon" && op != "status" && op != "reconcile" {
		return fmt.Errorf("invalid control operation %q", op)
	}
	for _, item := range []struct{ name, prefix string }{{"operationId", mektup.OperationIDPrefix}, {"replyMessageId", mektup.MessageIDPrefix}, {"originalMessageId", mektup.MessageIDPrefix}} {
		v, err := stringValue(item.name)
		if err != nil {
			return err
		}
		if err := mektup.ValidateID(v, item.prefix); err != nil {
			return err
		}
	}
	custody, ok := value["custody"].(map[string]any)
	if !ok {
		return errors.New("control custody is required")
	}
	endpoint, ok := custody["endpointId"].(string)
	if !ok || mektup.ValidateID(endpoint, mektup.EndpointIDPrefix) != nil {
		return errors.New("control custody endpoint is invalid")
	}
	store, ok := custody["storeId"].(string)
	if !ok || mektup.ValidateID(store, mektup.StoreIDPrefix) != nil {
		return errors.New("control custody store is invalid")
	}
	destination, ok := value["replyDestination"].(map[string]any)
	if !ok {
		return errors.New("control reply destination is required")
	}
	destinationEndpoint, ok := destination["endpointId"].(string)
	if !ok || mektup.ValidateID(destinationEndpoint, mektup.EndpointIDPrefix) != nil {
		return errors.New("control destination endpoint is invalid")
	}
	if thread, ok := destination["threadId"].(string); !ok || thread == "" {
		return errors.New("control destination thread is required")
	}
	if kind == "request" && op == "claim" {
		if _, has := value["fencingToken"]; has {
			return errors.New("claim request cannot select fencingToken")
		}
		if _, has := value["lease"]; has {
			return errors.New("claim request cannot select lease")
		}
		if _, has := value["result"]; has {
			return errors.New("claim request cannot carry result")
		}
	}
	if kind == "request" && (op == "heartbeat" || op == "commit" || op == "abandon") {
		if _, ok := value["fencingToken"].(string); !ok {
			return errors.New("fenced control request requires fencingToken")
		}
		lease, ok := value["lease"].(map[string]any)
		if !ok {
			return errors.New("fenced control request requires lease")
		}
		if _, ok := lease["expiresAt"].(string); !ok {
			return errors.New("lease expiry is required")
		}
	}
	if kind == "result" {
		result, ok := value["result"].(map[string]any)
		if !ok {
			return errors.New("control result requires result")
		}
		if _, ok := value["fencingToken"]; ok {
			return errors.New("control result cannot carry top-level fencingToken")
		}
		if _, ok := value["lease"]; ok {
			return errors.New("control result cannot carry top-level lease")
		}
		if op == "claim" {
			disposition, ok := result["disposition"].(string)
			if !ok || (disposition != "claimed" && disposition != "existing") {
				return errors.New("claim result requires claimed or existing disposition")
			}
			state, ok := result["state"].(string)
			if !ok || !mektup.EvidenceState(state).Valid() {
				return errors.New("claim result requires valid state")
			}
			switch disposition {
			case "claimed":
				token, ok := result["fencingToken"].(string)
				if !ok || token == "" {
					return errors.New("claimed result requires fencingToken")
				}
				lease, ok := result["lease"].(map[string]any)
				if !ok {
					return errors.New("claimed result requires lease")
				}
				if expires, ok := lease["expiresAt"].(string); !ok || !validControlTimestamp(expires) {
					return errors.New("claimed result requires lease expiry")
				}
				for _, field := range []string{"acquiredAt", "heartbeatAt"} {
					if raw, present := lease[field]; present {
						value, ok := raw.(string)
						if !ok || !validControlTimestamp(value) {
							return fmt.Errorf("claimed result lease %s must be a UTC timestamp", field)
						}
					}
				}
			case "existing":
				if _, ok := result["fencingToken"]; ok {
					return errors.New("existing result forbids fencingToken")
				}
				if _, ok := result["lease"]; ok {
					return errors.New("existing result forbids lease")
				}
				if status, present := result["status"]; present {
					if text, ok := status.(string); !ok || text == "" {
						return errors.New("existing result status must be a nonempty string")
					}
				}
				if winner, present := result["winner"]; present {
					if _, ok := winner.(map[string]any); !ok {
						return errors.New("existing result winner must be an object")
					}
				}
			}
		}
	}
	if kind == "request" && (op == "claim" || op == "heartbeat" || op == "commit" || op == "abandon") {
		if n, ok := value["bodyBytes"].(float64); !ok || n < 0 {
			return errors.New("bodyBytes is required")
		}
		body, ok := value["bodySha256"].(string)
		if !ok || !validDigest(body) {
			return errors.New("bodySha256 is required")
		}
		if status, ok := value["replyStatus"].(string); !ok || (status != "success" && status != "error") {
			return errors.New("replyStatus is required")
		}
		if owner, ok := value["attemptOwner"].(string); !ok || owner == "" {
			return errors.New("attemptOwner is required")
		}
	}
	return nil
}

func validateScenarioDocuments(s scenariosDocument, t transitionsDocument) error {
	if s.Schema != "mektup/conformance/v1/scenarios" || s.Version == "" || s.SpecVersion != "1.0.3" || len(s.Profiles) == 0 || len(s.Scenarios) == 0 {
		return errors.New("scenarios document metadata is incomplete")
	}
	if t.SpecVersion != "1.0.3" {
		return errors.New("transitions document spec revision is not 1.0.3")
	}
	states := map[string]bool{"none": true}
	for _, state := range t.States {
		if states[state] {
			return fmt.Errorf("transitions: duplicate state %q", state)
		}
		states[state] = true
	}
	byEvent := map[string]transition{}
	for _, item := range t.Transitions {
		if !states[item.From] || !states[item.To] {
			return fmt.Errorf("transitions: unknown state in %s", item.Event)
		}
		key := item.From + "\x00" + item.Event
		if _, exists := byEvent[key]; exists {
			return fmt.Errorf("transitions: duplicate state/event %q", item.Event)
		}
		byEvent[key] = item
	}
	if len(t.Forbidden) == 0 || len(t.Invariants) == 0 {
		return errors.New("transitions: forbidden transitions and invariants are required")
	}
	for _, want := range []forbidden{
		{From: "outcome_unknown", To: "rejected"},
		{From: "outcome_unknown", To: "not_sent"},
		{From: "reply_outcome_unknown", To: "reply_dispatch_claimed"},
		{From: "reply_accepted", To: "reply_accepted"},
	} {
		found := false
		for _, item := range t.Forbidden {
			if item.From == want.From && item.To == want.To {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("transitions: missing forbidden transition %s -> %s", want.From, want.To)
		}
	}
	seen := map[string]bool{}
	for _, item := range s.Scenarios {
		if item.ID == "" || item.Matrix == "" || item.Profile == "" || len(item.Observable) == 0 {
			return fmt.Errorf("scenario %q is incomplete", item.ID)
		}
		if seen[item.ID] {
			return fmt.Errorf("duplicate scenario %q", item.ID)
		}
		seen[item.ID] = true
		p, ok := s.Profiles[item.Profile]
		if !ok {
			return fmt.Errorf("scenario %s references unknown profile %q", item.ID, item.Profile)
		}
		if err := validateProfile(p, byEvent); err != nil {
			return fmt.Errorf("profile %s: %w", item.Profile, err)
		}
	}
	return nil
}

func validateProfile(p profile, byEvent map[string]transition) error {
	state := "none"
	for _, event := range p.Transitions {
		tr, ok := byEvent[state+"\x00"+event]
		if !ok {
			// Waiting is an observable operation phase, not a durable evidence
			// transition. It is intentionally absent from state-transitions.json.
			if event == "wait.started" && state == string(mektup.StateAccepted) {
				continue
			}
			return fmt.Errorf("unknown transition event %q", event)
		}
		if tr.From != state {
			return fmt.Errorf("event %s starts at %s, current state is %s", event, tr.From, state)
		}
		state = tr.To
	}
	if p.Terminal.Event == "" || p.Terminal.State == "" || p.Terminal.Exit < 0 || p.Terminal.Exit > 5 {
		return errors.New("invalid terminal mapping")
	}
	if p.Terminal.State != state && !(len(p.Transitions) == 0 && (p.Terminal.State == string(mektup.StateNotSent) || p.Terminal.State == string(mektup.StateAccepted))) {
		return fmt.Errorf("terminal state %s does not follow %s", p.Terminal.State, state)
	}
	exit := expectedExit(p)
	if exit != p.Terminal.Exit {
		return fmt.Errorf("terminal event %s has exit %d, want %d", p.Terminal.Event, p.Terminal.Exit, exit)
	}
	return nil
}

func expectedExit(p profile) int {
	switch p.Terminal.Event {
	case "send.accepted", "reply.accepted", "thread.read.completed":
		return mektup.ExitSuccess
	case "operation.rejected":
		return int(cli.ExitCodeForError(string(mektup.ErrDeliveryRejected)))
	case "operation.failed":
		if len(p.Transitions) > 0 {
			return int(cli.ExitCodeForError(string(mektup.ErrInternal)))
		}
		return int(cli.ExitCodeForError(string(mektup.ErrInvalidArguments)))
	case "operation.unknown":
		return int(cli.ExitCodeForError(string(mektup.ErrOutcomeUnknown)))
	case "wait.incomplete":
		return int(cli.ExitCodeForError(string(mektup.ErrWaitIncomplete)))
	default:
		return -1
	}
}

func validateEventSequence(root string) error {
	paths := []string{"event-send-accepted.json", "event-reply-accepted.json"}
	events := make([]mektup.Event, 0, len(paths))
	for _, path := range paths {
		data, err := os.ReadFile(filepath.Join(root, fixturesDir, path))
		if err != nil {
			return fmt.Errorf("event sequence: %w", err)
		}
		event, err := mektup.ParseEvent(data)
		if err != nil {
			return fmt.Errorf("event sequence %s: %w", path, err)
		}
		events = append(events, event)
	}
	sort.Slice(events, func(i, j int) bool { return events[i].Sequence < events[j].Sequence })
	sequencer := mektup.NewEventSequencer(events[0].OperationID)
	for _, event := range events {
		if err := sequencer.Append(event); err != nil {
			return fmt.Errorf("event sequence: %w", err)
		}
	}
	return nil
}

func digest(value string) string {
	sum := sha256.Sum256([]byte(value))
	return "sha256:" + hex.EncodeToString(sum[:])
}

func validControlTimestamp(value string) bool {
	if len(value) < len("2006-01-02T15:04:05.0Z") || !strings.HasSuffix(value, "Z") || len(value) <= 19 || value[19] != '.' {
		return false
	}
	timestamp, err := time.Parse(time.RFC3339Nano, value)
	return err == nil && timestamp.Location() == time.UTC
}

func validDigest(value string) bool {
	return len(value) == len("sha256:")+64 && strings.HasPrefix(value, "sha256:") && strings.ToLower(value) == value && value[7:] == strings.Map(func(r rune) rune {
		if (r >= '0' && r <= '9') || (r >= 'a' && r <= 'f') {
			return r
		}
		return -1
	}, value[7:])
}

// WriteEvidence emits bounded, deterministic release evidence. Scenario lines
// say "declarative" so they cannot be mistaken for live/service acceptance.
func (s Summary) WriteEvidence(w io.Writer) error {
	if w == nil {
		return errors.New("nil evidence writer")
	}
	f := append([]FixtureResult(nil), s.Fixtures...)
	sort.Slice(f, func(i, j int) bool { return f[i].Path < f[j].Path })
	c := append([]ScenarioResult(nil), s.Scenarios...)
	sort.Slice(c, func(i, j int) bool { return c[i].ID < c[j].ID })
	if _, err := fmt.Fprintf(w, "mektup-conformance contract=%s fixtures=%d scenarios=%d coverage=declarative\n", s.ContractVersion, len(f), len(c)); err != nil {
		return err
	}
	for _, item := range f {
		if _, err := fmt.Fprintf(w, "fixture path=%s kind=%s expected=%s observed=%s\n", item.Path, item.Kind, item.Expected, item.Observed); err != nil {
			return err
		}
	}
	for _, item := range c {
		if _, err := fmt.Fprintf(w, "scenario id=%s matrix=%s profile=%s terminal=%s exit=%d coverage=%s\n", item.ID, item.Matrix, item.Profile, item.Terminal, item.Exit, item.Coverage); err != nil {
			return err
		}
	}
	matrices := make([]string, 0, len(s.UncoveredLive))
	for matrix := range s.UncoveredLive {
		matrices = append(matrices, matrix)
	}
	sort.Strings(matrices)
	for _, matrix := range matrices {
		if _, err := fmt.Fprintf(w, "uncovered-live matrix=%s scenarios=%d reason=requires-live-or-service-harness\n", matrix, s.UncoveredLive[matrix]); err != nil {
			return err
		}
	}
	return nil
}
