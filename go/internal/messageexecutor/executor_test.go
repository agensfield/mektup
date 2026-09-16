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
	"github.com/agensfield/mektup/go/internal/endpoint"
	"github.com/agensfield/mektup/go/internal/journal"
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
	reply     service.ReplyResult
	replyErr  error
}

type stagedService struct {
	accepted chan struct{}
	release  chan struct{}
	receipt  mektup.Receipt
}

func (s *stagedService) Send(context.Context, service.SendRequest) (service.SendResult, error) {
	return service.SendResult{Receipt: s.receipt}, nil
}
func (s *stagedService) Reply(context.Context, service.OriginalResolver, service.ReplyRequest) (service.ReplyResult, error) {
	return service.ReplyResult{Receipt: s.receipt}, nil
}
func (s *stagedService) Wait(context.Context, service.WaitRequest) (service.WaitResult, error) {
	return service.WaitResult{Receipt: s.receipt}, nil
}
func (s *stagedService) SendWithAcceptance(_ context.Context, _ service.SendRequest, onAccepted service.AcceptanceCallback) (service.SendResult, error) {
	if err := onAccepted(service.SendResult{Receipt: s.receipt}); err != nil {
		return service.SendResult{Receipt: s.receipt}, err
	}
	close(s.accepted)
	<-s.release
	terminal := s.receipt
	terminal.State = mektup.StateReplyAccepted
	return service.SendResult{Receipt: s.receipt, Wait: &service.WaitResult{Receipt: terminal, State: terminal.State}}, nil
}
func (s *stagedService) ReplyWithAcceptance(context.Context, service.OriginalResolver, service.ReplyRequest, service.ReplyAcceptanceCallback) (service.ReplyResult, error) {
	return service.ReplyResult{}, errors.New("unused")
}

func (s *fakeService) Send(_ context.Context, req service.SendRequest) (service.SendResult, error) {
	s.sendCalls++
	s.last = req
	return s.send, s.sendErr
}
func (s *fakeService) Reply(context.Context, service.OriginalResolver, service.ReplyRequest) (service.ReplyResult, error) {
	return s.reply, s.replyErr
}
func (s *fakeService) SendWithAcceptance(ctx context.Context, req service.SendRequest, onAccepted service.AcceptanceCallback) (service.SendResult, error) {
	result, err := s.Send(ctx, req)
	if onAccepted != nil && result.Receipt.ReceiptID != "" {
		if callbackErr := onAccepted(service.SendResult{Receipt: result.Receipt}); callbackErr != nil {
			return result, callbackErr
		}
	}
	return result, err
}
func (s *fakeService) ReplyWithAcceptance(ctx context.Context, resolver service.OriginalResolver, req service.ReplyRequest, onAccepted service.ReplyAcceptanceCallback) (service.ReplyResult, error) {
	result, err := s.Reply(ctx, resolver, req)
	if onAccepted != nil && result.Receipt.ReceiptID != "" {
		if callbackErr := onAccepted(service.ReplyResult{Receipt: result.Receipt}); callbackErr != nil {
			return result, callbackErr
		}
	}
	return result, err
}
func (s *fakeService) Wait(context.Context, service.WaitRequest) (service.WaitResult, error) {
	return s.wait, s.waitErr
}

type fakeInput struct{ file, stdin []byte }

func (f fakeInput) ReadFile(context.Context, string, int64) ([]byte, error) { return f.file, nil }
func (f fakeInput) ReadStdin(context.Context, int64) ([]byte, error)        { return f.stdin, nil }

type recordingInput struct {
	data  []byte
	limit int64
}

func (r *recordingInput) ReadFile(_ context.Context, _ string, limit int64) ([]byte, error) {
	r.limit = limit
	return r.data, nil
}
func (r *recordingInput) ReadStdin(_ context.Context, limit int64) ([]byte, error) {
	r.limit = limit
	return r.data, nil
}

type fakeReceipts struct {
	resolveCalls int
	lastResolve  receipts.ResolveRequest
	receipt      mektup.Receipt
	inspect      receipts.InspectResult
	inspectErr   error
	lastInspect  receipts.InspectOptions
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
	f.lastResolve = req
	if req.Gate == nil {
		return mektup.Receipt{}, receipts.ErrHumanGateRequired
	}
	return f.receipt, nil
}
func (f *fakeReceipts) Inspect(_ context.Context, _ string, _ receipts.TargetInspector, options receipts.InspectOptions) (receipts.InspectResult, error) {
	f.lastInspect = options
	return f.inspect, f.inspectErr
}

type fakeOriginal struct{}

func (fakeOriginal) ResolveOriginal(context.Context, string) (service.OriginalMessage, error) {
	return service.OriginalMessage{}, errors.New("unused")
}

type acceptedOriginal struct{}

func (acceptedOriginal) ResolveOriginal(context.Context, string) (service.OriginalMessage, error) {
	return service.OriginalMessage{}, nil
}

type fakeImportResolver struct{ originalCalls, waitCalls int }

func (r *fakeImportResolver) ResolveOriginal(context.Context, cli.Invocation, receipts.Imported) (service.OriginalResolver, error) {
	r.originalCalls++
	return fakeOriginal{}, nil
}
func (r *fakeImportResolver) ResolveWaitReference(context.Context, cli.Invocation, receipts.Imported) (string, error) {
	r.waitCalls++
	return "rcpt-imported", nil
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

func TestJoinedReplyClaimDoesNotAdvertiseAcceptance(t *testing.T) {
	receipt := testReceipt(mektup.StateReplyDispatchClaimed)
	svc := &fakeService{reply: service.ReplyResult{Receipt: receipt}, replyErr: &service.Error{Code: mektup.ErrWaitIncomplete, Message: "already in flight"}}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }, Original: func(context.Context, cli.Invocation) (service.OriginalResolver, error) { return fakeOriginal{}, nil }})
	var out, errOut bytes.Buffer
	app := &cli.App{Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: exec}
	if code := app.Run([]string{"reply", receipt.Message.MessageID, "hello"}); code != int(cli.ExitIncomplete) {
		t.Fatalf("exit=%d stderr=%q", code, errOut.String())
	}
	if strings.Contains(out.String(), `"event":"reply.accepted"`) || strings.Contains(out.String(), `"ok":true`) {
		t.Fatalf("joined claim advertised acceptance: %s", out.String())
	}
}

func TestStagedServiceEmitsAcceptanceBeforeWaitReturns(t *testing.T) {
	receipt := testReceipt(mektup.StateAccepted)
	svc := &stagedService{accepted: make(chan struct{}), release: make(chan struct{}), receipt: receipt}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
	var out, errOut bytes.Buffer
	app := &cli.App{Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: exec}
	done := make(chan int, 1)
	go func() { done <- app.Run([]string{"send", "target", "hello", "--wait"}) }()
	<-svc.accepted
	if !strings.Contains(out.String(), `"event":"send.accepted"`) || strings.Contains(out.String(), `"event":"reply.accepted"`) {
		t.Fatalf("acceptance was not observable before wait: %s", out.String())
	}
	close(svc.release)
	if code := <-done; code != int(cli.ExitSuccess) {
		t.Fatalf("staged send exit=%d stderr=%q", code, errOut.String())
	}
	if lines := strings.Split(strings.TrimSpace(out.String()), "\n"); len(lines) != 2 {
		t.Fatalf("staged send lines=%d output=%q", len(lines), out.String())
	}
}

func TestAcceptedReceiptIsPreservedWhenPostAcceptanceErrorReturns(t *testing.T) {
	accepted := testReceipt(mektup.StateAccepted)
	svc := &fakeService{send: service.SendResult{Receipt: accepted}, sendErr: &service.Error{Code: mektup.ErrInternal, Message: "receipt projection failed"}}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
	result, err := exec.Execute(context.Background(), cli.Invocation{Command: "send", Position: []string{"target", "hello"}, Options: map[string][]string{}})
	gotReceipt, _ := result.Receipt.(mektup.Receipt)
	if err != nil || gotReceipt.ReceiptID != accepted.ReceiptID || result.Exit != cli.ExitInternal || len(result.Events) != 1 {
		t.Fatalf("accepted error evidence lost: result=%#v err=%v", result, err)
	}
}

func TestAttachedReceiptErrorUsesCanonicalWireShape(t *testing.T) {
	accepted := testReceipt(mektup.StateAccepted)
	svc := &fakeService{send: service.SendResult{Receipt: accepted}, sendErr: &service.Error{Code: mektup.ErrInternal, Message: "cleanup"}}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
	var out, errOut bytes.Buffer
	app := &cli.App{Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: exec}
	if code := app.Run([]string{"send", "target", "body"}); code != int(cli.ExitInternal) {
		t.Fatalf("exit=%d stderr=%q", code, errOut.String())
	}
	var event map[string]any
	if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &event); err != nil {
		t.Fatal(err)
	}
	errObject := event["data"].(map[string]any)["error"].(map[string]any)
	if _, ok := errObject["retryable"].(bool); !ok {
		t.Fatalf("retryable missing: %#v", errObject)
	}
	if _, ok := errObject["details"].(map[string]any); !ok {
		t.Fatalf("details is not an object: %#v", errObject)
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

func TestFileInputGetsByteHeadroomAndRejectsOverflow(t *testing.T) {
	in := &recordingInput{data: []byte(strings.Repeat("a", 4*MaxInputChars+1))}
	svc := &fakeService{}
	exec := New(Ports{Input: in, Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
	inv := cli.Invocation{Command: "send", Position: []string{"target"}, Options: map[string][]string{"file": {"body.txt"}}}
	_, err := exec.Execute(context.Background(), inv)
	var ce *cli.Error
	if in.limit != int64(4*MaxInputChars+1) || !errors.As(err, &ce) || ce.Code != "input_too_large" || svc.sendCalls != 0 {
		t.Fatalf("file bound=%d err=%v calls=%d", in.limit, err, svc.sendCalls)
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
	if store.lastResolve.Presentation != "agent" {
		t.Fatalf("machine presentation=%q, want agent", store.lastResolve.Presentation)
	}
}

func TestPortableReceiptOutputIsDirectlyImportable(t *testing.T) {
	store := &fakeReceipts{receipt: testReceipt(mektup.StateAccepted)}
	exec := New(Ports{Receipts: store})
	var out, errOut bytes.Buffer
	app := &cli.App{Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: exec}
	if code := app.Run([]string{"receipt", "show", store.receipt.ReceiptID, "--portable"}); code != int(cli.ExitSuccess) {
		t.Fatalf("exit=%d stderr=%q", code, errOut.String())
	}
	if _, err := receipts.Import(out.Bytes()); err != nil {
		t.Fatalf("portable stdout is not importable: %v output=%q", err, out.String())
	}
}

func TestPortableReceiptHumanPresentationRemainsPortable(t *testing.T) {
	store := &fakeReceipts{receipt: testReceipt(mektup.StateAccepted)}
	exec := New(Ports{Receipts: store})
	var out, errOut bytes.Buffer
	app := &cli.App{Out: &out, Err: &errOut, Executor: exec}
	if code := app.Run([]string{"receipt", "show", store.receipt.ReceiptID, "--portable", "--human"}); code != int(cli.ExitSuccess) {
		t.Fatalf("exit=%d stderr=%q", code, errOut.String())
	}
	var imported mektup.Receipt
	if err := json.Unmarshal(out.Bytes(), &imported); err != nil || imported.Schema != mektup.ReceiptSchema {
		t.Fatalf("portable human output=%q err=%v", out.String(), err)
	}
}

func TestHumanReceiptListAndContentHaveOutput(t *testing.T) {
	store := &fakeReceipts{receipt: testReceipt(mektup.StateAccepted)}
	exec := New(Ports{Receipts: store, History: func(context.Context, cli.Invocation, mektup.Receipt) (receipts.HistoryPort, error) {
		return emptyHistory{}, nil
	}})
	var listOut, listErr bytes.Buffer
	listApp := &cli.App{Out: &listOut, Err: &listErr, Executor: exec}
	if code := listApp.Run([]string{"receipt", "list", "--human"}); code != int(cli.ExitSuccess) || !strings.Contains(listOut.String(), store.receipt.ReceiptID) {
		t.Fatalf("human list exit=%d stderr=%q output=%q", code, listErr.String(), listOut.String())
	}
	var contentOut, contentErr bytes.Buffer
	contentApp := &cli.App{Out: &contentOut, Err: &contentErr, Executor: exec}
	if code := contentApp.Run([]string{"receipt", "show", store.receipt.ReceiptID, "--content", "--human"}); code != int(cli.ExitSuccess) || !strings.Contains(contentOut.String(), "content bytes=") {
		t.Fatalf("human content exit=%d stderr=%q output=%q", code, contentErr.String(), contentOut.String())
	}
}

func TestInspectZeroReceiptsIsIdentityOnlyAndDefaultIsBounded(t *testing.T) {
	store := &fakeReceipts{inspect: receipts.InspectResult{Target: receipts.TargetIdentity{
		EndpointID: "ep_01999999-9999-7999-8999-999999999999", EndpointAlias: "local", Transport: "unix",
		ServerVersion: "0.154.0", Compatibility: "tested", ThreadID: "thread-1", Requested: "mektup-sage",
		Resolved: "codex://local/thread/thread-1", Loaded: true, Status: "idle",
	}}}
	exec := New(Ports{Receipts: store, Inspector: func(context.Context, cli.Invocation) (receipts.TargetInspector, error) { return nil, nil }})
	var out, errOut bytes.Buffer
	app := &cli.App{Out: &out, Err: &errOut, Executor: exec}
	if code := app.Run([]string{"inspect", "mektup-sage", "--receipts", "0", "--human"}); code != int(cli.ExitSuccess) {
		t.Fatalf("identity-only inspect exit=%d stderr=%q", code, errOut.String())
	}
	if store.lastInspect.ReceiptLimit != 0 || !strings.Contains(out.String(), "target mektup-sage") || !strings.Contains(out.String(), "receipts: 0") {
		t.Fatalf("identity-only options=%+v output=%q", store.lastInspect, out.String())
	}
	out.Reset()
	if code := app.Run([]string{"inspect", "mektup-sage", "--human"}); code != int(cli.ExitSuccess) || store.lastInspect.ReceiptLimit != 10 {
		t.Fatalf("default inspect exit=%d options=%+v stderr=%q", code, store.lastInspect, errOut.String())
	}
}

func TestHumanInspectShowsRoutingReceiptsAndBlockers(t *testing.T) {
	now := time.Date(2026, 9, 16, 10, 0, 0, 0, time.UTC)
	receipt := testReceipt(mektup.StateAccepted)
	receipt.Target.Alias = "local"
	result := receipts.InspectResult{
		Target:   receipts.TargetIdentity{EndpointID: receipt.Target.EndpointID, EndpointAlias: "local", Transport: "unix", ServerVersion: "0.154.0", Compatibility: "tested", ThreadID: receipt.Target.ThreadID, Requested: "mektup-sage", Resolved: receipt.Target.Resolved, Loaded: true, Status: "idle", HerdrEvidence: map[string]any{"name": "mektup-sage", "paneId": "w3:p28", "status": "working"}},
		Receipts: []mektup.Receipt{receipt},
		Blockers: []journal.Blocker{{Method: "item/tool/requestUserInput", CorrelationID: "7", TurnID: "turn-1", LastSeen: now}},
	}
	got := humanInspect(result)
	for _, want := range []string{"target mektup-sage", "local (", "0.154.0", "herdr: name=mektup-sage pane=w3:p28", "RECEIPT", receipt.ReceiptID, "BLOCKERS", "item/tool/requestUserInput", "pending"} {
		if !strings.Contains(strings.ToUpper(got), strings.ToUpper(want)) {
			t.Fatalf("inspect output missing %q: %q", want, got)
		}
	}
}

func TestInspectResolverErrorsAreTypedAndNonMutating(t *testing.T) {
	tests := []struct {
		err  error
		code string
		exit cli.ExitCode
	}{
		{endpoint.ErrResolverNotFound, "endpoint_unavailable", cli.ExitRejected},
		{endpoint.ErrResolverAmbiguous, "target_ambiguous", cli.ExitRejected},
		{endpoint.ErrInvalidTarget, "invalid_target", cli.ExitUsage},
	}
	for _, test := range tests {
		mapped := mapError(test.err)
		var cliError *cli.Error
		if !errors.As(mapped, &cliError) || cliError.Code != test.code || cliError.Effect != "not_sent" || cliError.Exit != test.exit {
			t.Fatalf("%v mapped to %#v", test.err, mapped)
		}
	}
}

type emptyHistory struct{}

func (emptyHistory) FullHistory(context.Context, string, string) ([]receipts.HistoryItem, error) {
	return nil, nil
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

func TestUnsupportedServerVersionIsRejected(t *testing.T) {
	svc := &fakeService{sendErr: &service.Error{Code: mektup.ErrUnsupportedServerVersion, Message: "unsupported"}}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
	var out, errOut bytes.Buffer
	app := &cli.App{Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: exec}
	if code := app.Run([]string{"send", "target", "hello"}); code != int(cli.ExitRejected) {
		t.Fatalf("unsupported server exit=%d output=%s", code, out.String())
	}
}

func TestEarlyStreamingDomainErrorsStillRenderTerminalJSON(t *testing.T) {
	for _, code := range []mektup.ErrorCode{mektup.ErrOutcomeUnknown, mektup.ErrDeliveryRejected, mektup.ErrUnsupportedServerVersion} {
		t.Run(string(code), func(t *testing.T) {
			svc := &fakeService{sendErr: &service.Error{Code: code, Message: "delivery evidence", Details: map[string]any{"phase": "may_have_written"}}}
			exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
			var out, errOut bytes.Buffer
			app := &cli.App{Out: &out, Err: &errOut, Executor: exec}
			exit := app.Run([]string{"send", "target", "body", "--json"})
			var event map[string]any
			if err := json.Unmarshal(out.Bytes(), &event); err != nil {
				t.Fatalf("domain error lost terminal evidence: exit=%d stdout=%q stderr=%q", exit, out.String(), errOut.String())
			}
			if event["terminal"] != true || event["ok"] != false {
				t.Fatalf("bad terminal event: %#v", event)
			}
		})
	}
}

func TestReceiptFileIsUntrustedAndRequiresInjectedResolver(t *testing.T) {
	receipt := testReceipt(mektup.StateAccepted)
	encoded, _ := json.Marshal(receipt)
	input := fakeInput{file: encoded}
	svc := &fakeService{reply: service.ReplyResult{Receipt: receipt}}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }, Input: input})
	inv := cli.Invocation{Command: "reply", Position: []string{"ignored", "answer"}, Options: map[string][]string{"receipt-file": {"receipt.json"}}}
	_, err := exec.Execute(context.Background(), inv)
	var ce *cli.Error
	if !errors.As(err, &ce) || ce.Code != "route_unavailable" || svc.sendCalls != 0 {
		t.Fatalf("portable receipt authority was invented: err=%v", err)
	}
	resolver := &fakeImportResolver{}
	exec = New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }, Input: input, ImportResolver: resolver})
	_, _ = exec.Execute(context.Background(), inv)
	if resolver.originalCalls != 1 {
		t.Fatalf("import resolver calls=%d", resolver.originalCalls)
	}
}

func TestReplyErrorUsesReplyAcceptedTerminalEvent(t *testing.T) {
	receipt := testReceipt(mektup.StateReplyAccepted)
	receipt.Message.Kind = "reply"
	svc := &fakeService{reply: service.ReplyResult{Receipt: receipt, Wait: &service.WaitResult{Receipt: receipt, State: mektup.StateReplyAccepted, ReplyStatus: "error"}}, replyErr: &service.Error{Code: mektup.ErrDeliveryRejected, Message: "error reply"}}
	exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }, Original: func(context.Context, cli.Invocation) (service.OriginalResolver, error) {
		return acceptedOriginal{}, nil
	}})
	var out, errOut bytes.Buffer
	app := &cli.App{Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: exec}
	if code := app.Run([]string{"reply", receipt.Message.MessageID, "answer", "--wait", "--status", "error"}); code != int(cli.ExitRejected) {
		t.Fatalf("exit=%d stderr=%q", code, errOut.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("lines=%d output=%q", len(lines), out.String())
	}
	var terminal map[string]any
	if err := json.Unmarshal([]byte(lines[1]), &terminal); err != nil {
		t.Fatal(err)
	}
	if terminal["event"] != "reply.accepted" || terminal["ok"] != false {
		t.Fatalf("terminal=%#v", terminal)
	}
}

func TestSinceParseRejectsBeforeReceiptStore(t *testing.T) {
	store := &fakeReceipts{receipt: testReceipt(mektup.StateAccepted)}
	exec := New(Ports{Receipts: store})
	inv := cli.Invocation{Command: "receipt", Position: []string{"list"}, Options: map[string][]string{"since": {"not-a-time"}}}
	_, err := exec.Execute(context.Background(), inv)
	var ce *cli.Error
	if !errors.As(err, &ce) || ce.Code != "invalid_arguments" || store.resolveCalls != 0 {
		t.Fatalf("since parse = %v", err)
	}
}

func TestWaitJSONLTerminalClasses(t *testing.T) {
	cases := []struct {
		name       string
		state      mektup.EvidenceState
		status     string
		incomplete bool
		callErr    error
		event      string
		exit       cli.ExitCode
		ok         bool
	}{
		{"success", mektup.StateReplyAccepted, "success", false, nil, "reply.accepted", cli.ExitSuccess, true},
		{"error", mektup.StateReplyAccepted, "error", false, &service.Error{Code: mektup.ErrDeliveryRejected, Message: "error reply", Cause: errors.New("reply")}, "reply.accepted", cli.ExitRejected, false},
		{"timeout", mektup.StateAccepted, "", true, &service.Error{Code: mektup.ErrWaitIncomplete, Message: "timeout", Cause: errors.New("timeout")}, "wait.incomplete", cli.ExitIncomplete, false},
		{"unknown", mektup.StateReplyOutcomeUnknown, "", true, &service.Error{Code: mektup.ErrReplyOutcomeUnknown, Message: "unknown", Cause: errors.New("unknown")}, "reply.unknown", cli.ExitUnknown, false},
		{"cancel", mektup.StateAccepted, "", true, context.Canceled, "wait.incomplete", cli.ExitIncomplete, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			receipt := testReceipt(tc.state)
			svc := &fakeService{wait: service.WaitResult{Receipt: receipt, State: tc.state, ReplyStatus: tc.status, Incomplete: tc.incomplete}, waitErr: tc.callErr}
			exec := New(Ports{Service: func(context.Context, cli.Invocation) (MessagingService, error) { return svc, nil }})
			var out, errOut bytes.Buffer
			app := &cli.App{Out: &out, Err: &errOut, Env: []string{"MEKTUP_AGENT=1"}, Executor: exec}
			if got := app.Run([]string{"wait", receipt.ReceiptID}); got != int(tc.exit) {
				t.Fatalf("exit=%d want=%d stderr=%q", got, tc.exit, errOut.String())
			}
			var event map[string]any
			if err := json.Unmarshal([]byte(strings.TrimSpace(out.String())), &event); err != nil {
				t.Fatal(err)
			}
			if event["event"] != tc.event || event["ok"] != tc.ok {
				t.Fatalf("event=%#v", event)
			}
		})
	}
}

func TestServiceErrorExitMapping(t *testing.T) {
	cases := []struct {
		code   mektup.ErrorCode
		exit   cli.ExitCode
		effect string
	}{
		{mektup.ErrInputTooLarge, cli.ExitUsage, "not_sent"}, {mektup.ErrDeliveryRejected, cli.ExitRejected, "rejected"}, {mektup.ErrReplyOutcomeUnknown, cli.ExitUnknown, "unknown"}, {mektup.ErrWaitIncomplete, cli.ExitIncomplete, "unknown"},
	}
	for _, tc := range cases {
		err := mapError(&service.Error{Code: tc.code, Message: "x"})
		var ce *cli.Error
		if !errors.As(err, &ce) || ce.Exit != tc.exit || ce.Effect != tc.effect {
			t.Fatalf("%s -> %#v", tc.code, err)
		}
	}
}

var _ receipts.HumanGate = fakeGate{}
