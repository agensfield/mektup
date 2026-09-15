package messageexecutor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/receipts"
	"github.com/agensfield/mektup/go/internal/service"
)

const (
	testEndpoint = "ep_01999999-9999-7999-8999-999999999999"
	testTarget   = "ep_02999999-9999-7999-8999-999999999999"
)

func testReceipt(state mektup.EvidenceState) mektup.Receipt {
	now := time.Now().UTC().Format(time.RFC3339Nano)
	return mektup.Receipt{Schema: mektup.ReceiptSchema, ReceiptID: "rcpt_03999999-9999-7999-8999-999999999999", OperationID: "op_04999999-9999-7999-8999-999999999999", Operation: "message", State: state,
		Source: mektup.ReceiptIdentity{EndpointID: testEndpoint, ThreadID: "source"}, Target: mektup.ReceiptIdentity{EndpointID: testTarget, ThreadID: "target", Resolved: "codex://target/thread/target"},
		Message:  mektup.ReceiptMessage{MessageID: "msg_05999999-9999-7999-8999-999999999999", Kind: "message", ReplyRequested: true, PayloadBytes: 5, PayloadSHA256: "sha256:2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"},
		Evidence: []mektup.EvidenceRecord{{State: state, At: now}}, CreatedAt: now, UpdatedAt: now}
}

type fakeService struct {
	sendCalls int
	last      service.SendRequest
	send      service.SendResult
	sendErr   error
	wait      service.WaitResult
	waitErr   error
}

func (s *fakeService) Send(_ context.Context, req service.SendRequest) (service.SendResult, error) {
	s.sendCalls++
	s.last = req
	return s.send, s.sendErr
}
func (*fakeService) Reply(context.Context, service.OriginalResolver, service.ReplyRequest) (service.ReplyResult, error) {
	return service.ReplyResult{}, errors.New("unused")
}
func (s *fakeService) Wait(context.Context, service.WaitRequest) (service.WaitResult, error) {
	return s.wait, s.waitErr
}

type fakeInput struct{ file, stdin []byte }

func (f fakeInput) ReadFile(context.Context, string, int64) ([]byte, error) { return f.file, nil }
func (f fakeInput) ReadStdin(context.Context, int64) ([]byte, error)        { return f.stdin, nil }

type fakeReceipts struct {
	resolveCalls int
	receipt      mektup.Receipt
}

func (f *fakeReceipts) List(context.Context, receipts.ListOptions) ([]mektup.Receipt, error) {
	return []mektup.Receipt{f.receipt}, nil
}
func (f *fakeReceipts) Show(context.Context, string, receipts.ShowOptions) (mektup.Receipt, error) {
	return f.receipt, nil
}
func (*fakeReceipts) Content(context.Context, string, receipts.HistoryPort, receipts.ContentOptions) (receipts.ContentResult, error) {
	return receipts.ContentResult{}, nil
}
func (f *fakeReceipts) Reconcile(context.Context, string, receipts.HistoryPort) (mektup.Receipt, error) {
	return f.receipt, nil
}
func (f *fakeReceipts) Resolve(_ context.Context, req receipts.ResolveRequest) (mektup.Receipt, error) {
	f.resolveCalls++
	if req.Gate == nil {
		return mektup.Receipt{}, receipts.ErrHumanGateRequired
	}
	return f.receipt, nil
}
func (f *fakeReceipts) Inspect(context.Context, string, receipts.TargetInspector, receipts.InspectOptions) (receipts.InspectResult, error) {
	return receipts.InspectResult{}, nil
}

type fakeOriginal struct{}

func (fakeOriginal) ResolveOriginal(context.Context, string) (service.OriginalMessage, error) {
	return service.OriginalMessage{}, errors.New("unused")
}

type fakeGate struct{}

func (fakeGate) Authorize(context.Context, receipts.ResolveIntent) (string, error) {
	return "gate-token", nil
}

func TestSendJSONLStreamsAcceptedThenReplyWithOneOperationID(t *testing.T) {
	accepted := testReceipt(mektup.StateAccepted)
	reply := accepted
	reply.State = mektup.StateReplyAccepted
	svc := &fakeService{send: service.SendResult{Receipt: accepted, Wait: &service.WaitResult{Receipt: reply, State: mektup.StateReplyAccepted, ReplyID: "msg_06999999-9999-7999-8999-999999999999"}}}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
	var out, errOut bytes.Buffer
	app := &cli.App{In: strings.NewReader(""), Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: exec}
	if code := app.Run([]string{"send", "target", "hello", "--wait"}); code != int(cli.ExitSuccess) {
		t.Fatalf("exit=%d stderr=%q", code, errOut.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("JSONL lines=%d output=%q", len(lines), out.String())
	}
	for _, line := range lines {
		var event map[string]any
		if err := json.Unmarshal([]byte(line), &event); err != nil {
			t.Fatal(err)
		}
		if event["operationId"] != accepted.OperationID {
			t.Fatalf("operation ID mismatch: %#v", event)
		}
		if strings.Contains(strings.ToLower(line), "body") {
			t.Fatalf("body leaked into lifecycle output: %s", line)
		}
	}
	if svc.sendCalls != 1 || svc.last.Body != "hello" || !svc.last.Wait {
		t.Fatalf("send mapping = calls=%d request=%#v", svc.sendCalls, svc.last)
	}
}

func TestBodyCharacterLimitRejectsBeforeService(t *testing.T) {
	svc := &fakeService{}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
	inv := cli.Invocation{Command: "send", Position: []string{"target", strings.Repeat("ş", MaxInputChars+1)}, Options: map[string][]string{}}
	_, err := exec.Execute(context.Background(), inv)
	var ce *cli.Error
	if !errors.As(err, &ce) || ce.Code != "input_too_large" || svc.sendCalls != 0 {
		t.Fatalf("limit result=%v calls=%d", err, svc.sendCalls)
	}
}

func TestReceiptResolveAlwaysUsesInjectedAssertionGate(t *testing.T) {
	store := &fakeReceipts{receipt: testReceipt(mektup.StateOutcomeUnknown)}
	exec := New(Ports{Receipts: store, HumanGate: func(context.Context, cli.Invocation) (receipts.HumanGate, error) { return fakeGate{}, nil }, Actor: "operator"})
	inv := cli.Invocation{Command: "receipt", Position: []string{"resolve", store.receipt.ReceiptID}, Options: map[string][]string{"resolve-as": {"accepted"}, "reason": {"external"}, "evidence": {"case-1"}}, Resolved: cli.ResolvedGlobals{Output: cli.PresentationJSON}}
	result, err := exec.Execute(context.Background(), inv)
	if err != nil || len(result.Events) != 1 || store.resolveCalls != 1 {
		t.Fatalf("resolve = %#v err=%v calls=%d", result, err, store.resolveCalls)
	}
}

func TestRawWaitIsRejectedByDomainMapping(t *testing.T) {
	svc := &fakeService{sendErr: &service.Error{Code: mektup.ErrInvalidRawWait, Message: "raw wait", Cause: errors.New("invalid")}}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
	inv := cli.Invocation{Command: "send", Position: []string{"target", "hello"}, Options: map[string][]string{"raw": {"true"}, "wait": {"true"}}}
	_, err := exec.Execute(context.Background(), inv)
	var ce *cli.Error
	if !errors.As(err, &ce) || ce.Code != string(mektup.ErrInvalidRawWait) {
		t.Fatalf("raw wait error=%v", err)
	}
}

var _ receipts.HumanGate = fakeGate{}
