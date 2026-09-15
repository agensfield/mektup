// Package messageexecutor adapts validated messaging/receipt invocations to
// injected domain ports. It never opens a daemon, reads ambient environment,
// resolves endpoints, or performs transport setup.
package messageexecutor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/receipts"
	"github.com/agensfield/mektup/go/internal/service"
)

const MaxInputChars = service.MaxInputChars

type MessagingService interface {
	Send(context.Context, service.SendRequest) (service.SendResult, error)
	Reply(context.Context, service.OriginalResolver, service.ReplyRequest) (service.ReplyResult, error)
	Wait(context.Context, service.WaitRequest) (service.WaitResult, error)
}

type ServiceFactory func(context.Context, cli.Invocation) (MessagingService, error)
type OriginalResolverFactory func(context.Context, cli.Invocation) (service.OriginalResolver, error)
type InputPort interface {
	ReadFile(context.Context, string, int64) ([]byte, error)
	ReadStdin(context.Context, int64) ([]byte, error)
}
type ArtifactFactory func(context.Context, cli.Invocation) (receipts.SpillWriter, error)
type HistoryFactory func(context.Context, cli.Invocation, mektup.Receipt) (receipts.HistoryPort, error)
type InspectorFactory func(context.Context, cli.Invocation) (receipts.TargetInspector, error)
type HumanGateFactory func(context.Context, cli.Invocation) (receipts.HumanGate, error)

type ReceiptStore interface {
	List(context.Context, receipts.ListOptions) ([]mektup.Receipt, error)
	Show(context.Context, string, receipts.ShowOptions) (mektup.Receipt, error)
	Content(context.Context, string, receipts.HistoryPort, receipts.ContentOptions) (receipts.ContentResult, error)
	Reconcile(context.Context, string, receipts.HistoryPort) (mektup.Receipt, error)
	Resolve(context.Context, receipts.ResolveRequest) (mektup.Receipt, error)
	Inspect(context.Context, string, receipts.TargetInspector, receipts.InspectOptions) (receipts.InspectResult, error)
}

type Ports struct {
	Service   ServiceFactory
	Original  OriginalResolverFactory
	Receipts  ReceiptStore
	Input     InputPort
	Artifacts ArtifactFactory
	History   HistoryFactory
	Inspector InspectorFactory
	HumanGate HumanGateFactory
	Actor     string
}

type Executor struct{ ports Ports }

func New(ports Ports) *Executor { return &Executor{ports: ports} }

var _ cli.Executor = (*Executor)(nil)
var _ ReceiptStore = (*receipts.Store)(nil)

func (e *Executor) Execute(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	switch inv.Command {
	case "send":
		return e.send(ctx, inv)
	case "reply":
		return e.reply(ctx, inv)
	case "wait":
		return e.wait(ctx, inv)
	case "inspect":
		return e.inspect(ctx, inv)
	case "receipt":
		return e.receipt(ctx, inv)
	default:
		return cli.ExecutionResult{}, &cli.Error{Code: "internal_error", Message: "message executor does not own command " + inv.Command, Effect: "not_sent", Exit: cli.ExitInternal}
	}
}

func (e *Executor) service(ctx context.Context, inv cli.Invocation) (MessagingService, error) {
	if e == nil || e.ports.Service == nil {
		return nil, cliErr("internal_error", "messaging service factory is required", "not_sent", cli.ExitInternal)
	}
	s, err := e.ports.Service(ctx, inv)
	if err != nil {
		return nil, mapError(err)
	}
	if s == nil {
		return nil, cliErr("internal_error", "messaging service factory returned nil", "not_sent", cli.ExitInternal)
	}
	return s, nil
}

func (e *Executor) send(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) < 1 {
		return cli.ExecutionResult{}, cliErr("invalid_arguments", "send target is required", "not_sent", cli.ExitUsage)
	}
	body, err := e.body(ctx, inv, 1)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	request := service.SendRequest{Target: inv.Position[0], Body: body, Raw: has(inv, "raw"), RequestReply: has(inv, "request-reply"), Wait: has(inv, "wait"), Source: inv.Option("reply-to")}
	request.DeliveryTimeout, err = duration(inv, "delivery-timeout")
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	request.DisableDeliveryTimeout = inv.Option("delivery-timeout") == "0"
	request.WaitTimeout, err = duration(inv, "wait-timeout")
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	svc, err := e.service(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	result, callErr := svc.Send(ctx, request)
	return e.messagingResult("send", result.Receipt, result.Wait, callErr)
}

func (e *Executor) reply(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) < 1 {
		return cli.ExecutionResult{}, cliErr("invalid_arguments", "reply reference is required", "not_sent", cli.ExitUsage)
	}
	body, err := e.body(ctx, inv, 1)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	status := mektup.ReplySuccess
	if inv.Option("status") == "error" {
		status = mektup.ReplyError
	}
	request := service.ReplyRequest{Reference: inv.Position[0], Body: body, Status: status, ErrorCode: inv.Option("error-code"), Source: inv.Option("reply-to"), Wait: has(inv, "wait")}
	request.DeliveryTimeout, err = duration(inv, "delivery-timeout")
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	request.DisableDeliveryTimeout = inv.Option("delivery-timeout") == "0"
	request.WaitTimeout, err = duration(inv, "wait-timeout")
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	svc, err := e.service(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	if e.ports.Original == nil {
		return cli.ExecutionResult{}, cliErr("message_not_found", "original message resolver factory is required", "not_sent", cli.ExitInternal)
	}
	original, err := e.ports.Original(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, mapError(err)
	}
	if original == nil {
		return cli.ExecutionResult{}, cliErr("message_not_found", "original message resolver factory returned nil", "not_sent", cli.ExitInternal)
	}
	result, callErr := svc.Reply(ctx, original, request)
	return e.messagingResult("reply", result.Receipt, result.Wait, callErr)
}

func (e *Executor) wait(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) != 1 {
		return cli.ExecutionResult{}, cliErr("invalid_arguments", "wait reference is required", "not_sent", cli.ExitUsage)
	}
	timeout, err := duration(inv, "timeout")
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	svc, err := e.service(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	result, callErr := svc.Wait(ctx, service.WaitRequest{Reference: inv.Position[0], Timeout: timeout})
	if result.Receipt.ReceiptID == "" {
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr)
		}
		return cli.ExecutionResult{}, cliErr("internal_error", "wait returned no receipt", "unknown", cli.ExitInternal)
	}
	eventName := "wait.completed"
	if result.Incomplete {
		eventName = "wait.incomplete"
	}
	event := lifecycle(eventName, result.Receipt, true, result.State == mektup.StateReplyAccepted || result.State == mektup.StateReplyObserved)
	if callErr != nil {
		return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: event}}, Receipt: result.Receipt, Exit: errorExit(callErr)}, nil
	}
	return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: event}}, Receipt: result.Receipt}, nil
}

func (e *Executor) inspect(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) != 1 {
		return cli.ExecutionResult{}, cliErr("invalid_arguments", "inspect target is required", "not_sent", cli.ExitUsage)
	}
	if e.ports.Receipts == nil || e.ports.Inspector == nil {
		return cli.ExecutionResult{}, cliErr("internal_error", "receipt store and inspector factories are required", "not_sent", cli.ExitInternal)
	}
	inspector, err := e.ports.Inspector(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, mapError(err)
	}
	result, err := e.ports.Receipts.Inspect(ctx, inv.Position[0], inspector, receipts.InspectOptions{ReceiptLimit: optionInt(inv, "receipts"), Blockers: has(inv, "blockers")})
	if err != nil {
		return cli.ExecutionResult{}, mapError(err)
	}
	return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: map[string]any{"schema": cli.EventSchema, "event": "inspect.completed", "terminal": true, "ok": true, "data": map[string]any{"target": result.Target, "receipts": result.Receipts, "blockers": result.Blockers}}}}}, nil
}

func (e *Executor) receipt(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) < 1 || e.ports.Receipts == nil {
		return cli.ExecutionResult{}, cliErr("internal_error", "receipt store is required", "not_sent", cli.ExitInternal)
	}
	sub := inv.Position[0]
	switch sub {
	case "list":
		items, err := e.ports.Receipts.List(ctx, receipts.ListOptions{State: mektup.EvidenceState(inv.Option("state")), Since: parseSince(inv.Option("since")), Limit: optionInt(inv, "limit")})
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: map[string]any{"schema": cli.EventSchema, "event": "receipt.list", "terminal": true, "ok": true, "data": map[string]any{"receipts": items}}}}}, nil
	case "show":
		if len(inv.Position) != 2 {
			return cli.ExecutionResult{}, cliErr("invalid_arguments", "receipt show reference is required", "not_sent", cli.ExitUsage)
		}
		receipt, err := e.ports.Receipts.Show(ctx, inv.Position[1], receipts.ShowOptions{Portable: has(inv, "portable"), Content: has(inv, "content")})
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		if !has(inv, "content") {
			return receiptResult("receipt.show", receipt)
		}
		if e.ports.History == nil {
			return cli.ExecutionResult{}, cliErr("content_unavailable", "exact history factory is required for receipt content", "not_sent", cli.ExitRejected)
		}
		history, err := e.ports.History(ctx, inv, receipt)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		var spill receipts.SpillWriter
		if e.ports.Artifacts != nil {
			spill, err = e.ports.Artifacts(ctx, inv)
			if err != nil {
				return cli.ExecutionResult{}, mapError(err)
			}
		}
		content, err := e.ports.Receipts.Content(ctx, inv.Position[1], history, receipts.ContentOptions{MaxInlineBytes: MaxInputChars, Spill: spill})
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		data := map[string]any{"receipt": receipt, "bytes": content.Bytes, "digest": content.Digest}
		if content.Spill != nil {
			data["artifact"] = content.Spill
		} else {
			data["body"] = string(content.Body)
		}
		return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: map[string]any{"schema": cli.EventSchema, "event": "receipt.content", "terminal": true, "ok": true, "data": data}}}}, nil
	case "reconcile":
		if len(inv.Position) != 2 || e.ports.History == nil {
			return cli.ExecutionResult{}, cliErr("content_unavailable", "receipt reconcile requires exact history", "not_sent", cli.ExitRejected)
		}
		receipt, err := e.ports.Receipts.Show(ctx, inv.Position[1], receipts.ShowOptions{})
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		history, err := e.ports.History(ctx, inv, receipt)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		updated, err := e.ports.Receipts.Reconcile(ctx, inv.Position[1], history)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		return receiptResult("receipt.reconciled", updated)
	case "resolve":
		if len(inv.Position) != 2 || e.ports.HumanGate == nil {
			return cli.ExecutionResult{}, cliErr("effect_acknowledgment_required", "receipt resolve requires an injected assertion gate", "not_sent", cli.ExitUsage)
		}
		gate, err := e.ports.HumanGate(ctx, inv)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		assertion := strings.ReplaceAll(inv.Option("resolve-as"), "-", "_")
		actor := e.ports.Actor
		if actor == "" {
			actor = "cli"
		}
		presentation := string(inv.Resolved.Output)
		if presentation == "" {
			presentation = "agent"
		}
		updated, err := e.ports.Receipts.Resolve(ctx, receipts.ResolveRequest{Reference: inv.Position[1], Assertion: assertion, Actor: actor, Reason: inv.Option("reason"), EvidenceRef: inv.Option("evidence"), Presentation: presentation, Intent: "receipt.resolve", Gate: gate})
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		return receiptResult("receipt.resolved", updated)
	default:
		return cli.ExecutionResult{}, cliErr("invalid_arguments", "unsupported receipt subcommand", "not_sent", cli.ExitUsage)
	}
}

func (e *Executor) body(ctx context.Context, inv cli.Invocation, messageIndex int) (string, error) {
	stdin, file := has(inv, "stdin"), inv.Option("file") != ""
	inline := len(inv.Position) > messageIndex
	if boolCount(stdin, file, inline) != 1 {
		return "", cliErr("invalid_arguments", "exactly one message source is required", "not_sent", cli.ExitUsage)
	}
	var data []byte
	var err error
	switch {
	case stdin:
		if e.ports.Input == nil {
			return "", cliErr("internal_error", "input port is required", "not_sent", cli.ExitInternal)
		}
		data, err = e.ports.Input.ReadStdin(ctx, int64(4*MaxInputChars))
	case file:
		if e.ports.Input == nil {
			return "", cliErr("internal_error", "input port is required", "not_sent", cli.ExitInternal)
		}
		data, err = e.ports.Input.ReadFile(ctx, inv.Option("file"), int64(4*MaxInputChars))
	default:
		data = []byte(inv.Position[messageIndex])
	}
	if err != nil {
		return "", cliErr("invalid_arguments", "message source could not be read", "not_sent", cli.ExitUsage)
	}
	if len(data) == 0 || !utf8.Valid(data) {
		return "", cliErr("invalid_arguments", "message body must be non-empty UTF-8", "not_sent", cli.ExitUsage)
	}
	if utf8.RuneCount(data) > MaxInputChars {
		return "", cliErr("input_too_large", "message body exceeds the 2^20 character limit", "not_sent", cli.ExitUsage)
	}
	return string(data), nil
}

func (e *Executor) messagingResult(kind string, accepted mektup.Receipt, wait *service.WaitResult, callErr error) (cli.ExecutionResult, error) {
	if accepted.ReceiptID == "" {
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr)
		}
		return cli.ExecutionResult{}, cliErr("internal_error", "messaging service returned no receipt", "unknown", cli.ExitInternal)
	}
	events := []cli.OutputEvent{{Machine: lifecycle(kind+".accepted", accepted, wait == nil, true)}}
	final := accepted
	exit := cli.ExitSuccess
	if wait != nil {
		final = wait.Receipt
		name := "reply.received"
		if wait.Incomplete {
			name = "wait.incomplete"
		}
		if wait.State == mektup.StateReplyOutcomeUnknown {
			name = "reply.unknown"
		}
		events = append(events, cli.OutputEvent{Machine: lifecycle(name, final, true, !wait.Incomplete && wait.State != mektup.StateReplyOutcomeUnknown)})
		exit = waitExit(wait, callErr)
	}
	if callErr != nil && wait == nil {
		return cli.ExecutionResult{}, mapError(callErr)
	}
	return cli.ExecutionResult{Events: events, Receipt: final, Exit: exit}, nil
}

func lifecycle(name string, receipt mektup.Receipt, terminal, ok bool) map[string]any {
	return map[string]any{"schema": cli.EventSchema, "event": name, "operationId": receipt.OperationID, "terminal": terminal, "ok": ok, "data": map[string]any{"receipt": receipt}}
}
func receiptResult(name string, receipt mektup.Receipt) (cli.ExecutionResult, error) {
	return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: lifecycle(name, receipt, true, true)}}, Receipt: receipt}, nil
}
func waitExit(wait *service.WaitResult, err error) cli.ExitCode {
	if wait.Incomplete {
		return cli.ExitIncomplete
	}
	if wait.State == mektup.StateReplyOutcomeUnknown {
		return cli.ExitUnknown
	}
	if wait.ReplyStatus == string(mektup.ReplyError) {
		return cli.ExitRejected
	}
	if err != nil {
		return errorExit(err)
	}
	return cli.ExitSuccess
}
func errorExit(err error) cli.ExitCode {
	var ce *cli.Error
	if errors.As(err, &ce) {
		return ce.Exit
	}
	var se *service.Error
	if errors.As(err, &se) {
		switch se.Code {
		case mektup.ErrWaitIncomplete:
			return cli.ExitIncomplete
		case mektup.ErrReplyOutcomeUnknown, mektup.ErrOutcomeUnknown:
			return cli.ExitUnknown
		case mektup.ErrDeliveryRejected:
			return cli.ExitRejected
		}
	}
	return cli.ExitInternal
}
func mapError(err error) error {
	if err == nil {
		return nil
	}
	var ce *cli.Error
	if errors.As(err, &ce) {
		return ce
	}
	var se *service.Error
	if errors.As(err, &se) {
		effect := "unknown"
		if se.Code == mektup.ErrInputTooLarge || se.Code == mektup.ErrInvalidArguments {
			effect = "not_sent"
		}
		return &cli.Error{Code: string(se.Code), Message: se.Message, Effect: effect, Exit: errorExit(err), Details: se.Details}
	}
	return &cli.Error{Code: "internal_error", Message: err.Error(), Effect: "unknown", Exit: cli.ExitInternal}
}
func cliErr(code, message, effect string, exit cli.ExitCode) error {
	return &cli.Error{Code: code, Message: message, Effect: effect, Exit: exit}
}
func has(inv cli.Invocation, name string) bool { return len(inv.Options[name]) > 0 }
func boolCount(values ...bool) int {
	n := 0
	for _, value := range values {
		if value {
			n++
		}
	}
	return n
}
func duration(inv cli.Invocation, name string) (time.Duration, error) {
	value := inv.Option(name)
	if value == "" {
		return 0, nil
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed < 0 {
		return 0, cliErr("invalid_arguments", "invalid --"+name, "not_sent", cli.ExitUsage)
	}
	return parsed, nil
}
func optionInt(inv cli.Invocation, name string) int {
	var n int
	_, _ = fmt.Sscan(inv.Option(name), &n)
	return n
}
func parseSince(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, _ := time.Parse(time.RFC3339Nano, value)
	return parsed
}
