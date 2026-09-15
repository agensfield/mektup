// Package messageexecutor adapts validated messaging/receipt invocations to
// injected domain ports. It never opens a daemon, reads ambient environment,
// resolves endpoints, or performs transport setup.
package messageexecutor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"

	mektup "github.com/agensfield/mektup/go"
	"github.com/agensfield/mektup/go/internal/cli"
	"github.com/agensfield/mektup/go/internal/journal"
	"github.com/agensfield/mektup/go/internal/receipts"
	"github.com/agensfield/mektup/go/internal/service"
)

const MaxInputChars = service.MaxInputChars

type MessagingService interface {
	Send(context.Context, service.SendRequest) (service.SendResult, error)
	Reply(context.Context, service.OriginalResolver, service.ReplyRequest) (service.ReplyResult, error)
	Wait(context.Context, service.WaitRequest) (service.WaitResult, error)
}

type AcceptanceMessagingService interface {
	SendWithAcceptance(context.Context, service.SendRequest, service.AcceptanceCallback) (service.SendResult, error)
	ReplyWithAcceptance(context.Context, service.OriginalResolver, service.ReplyRequest, service.ReplyAcceptanceCallback) (service.ReplyResult, error)
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
type ReceiptImportResolver interface {
	ResolveOriginal(context.Context, cli.Invocation, receipts.Imported) (service.OriginalResolver, error)
	ResolveWaitReference(context.Context, cli.Invocation, receipts.Imported) (string, error)
}

type ReceiptStore interface {
	List(context.Context, receipts.ListOptions) ([]mektup.Receipt, error)
	Show(context.Context, string, receipts.ShowOptions) (mektup.Receipt, error)
	Content(context.Context, string, receipts.HistoryPort, receipts.ContentOptions) (receipts.ContentResult, error)
	Reconcile(context.Context, string, receipts.HistoryPort) (mektup.Receipt, error)
	Resolve(context.Context, receipts.ResolveRequest) (mektup.Receipt, error)
	Inspect(context.Context, string, receipts.TargetInspector, receipts.InspectOptions) (receipts.InspectResult, error)
}

type Ports struct {
	Service        ServiceFactory
	Original       OriginalResolverFactory
	Receipts       ReceiptStore
	Input          InputPort
	Artifacts      ArtifactFactory
	History        HistoryFactory
	Inspector      InspectorFactory
	HumanGate      HumanGateFactory
	ImportResolver ReceiptImportResolver
	Actor          string
}

type Executor struct{ ports Ports }

func New(ports Ports) *Executor { return &Executor{ports: ports} }

var _ cli.Executor = (*Executor)(nil)
var _ cli.StreamingExecutor = (*Executor)(nil)
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

// ExecuteStream is the staged CLI boundary for send/reply --wait. Concrete
// services that implement AcceptanceMessagingService call the emit function
// after durable acceptance and before entering their correlated wait. The
// compatibility path remains buffered for injected legacy services that have
// no acceptance callback, while the production service uses the staged path.
func (e *Executor) ExecuteStream(ctx context.Context, inv cli.Invocation, emit func(cli.ExecutionResult) error) error {
	if emit == nil {
		return cliErr("internal_error", "streaming output callback is required", "unknown", cli.ExitInternal)
	}
	switch inv.Command {
	case "send":
		svc, request, err := e.sendInvocation(ctx, inv)
		if err != nil {
			return err
		}
		stream, ok := svc.(AcceptanceMessagingService)
		if request.Wait && !ok {
			return cliErr("internal_error", "send --wait requires the durable acceptance stream", "unknown", cli.ExitInternal)
		}
		if !request.Wait {
			result, callErr := svc.Send(ctx, request)
			output, resultErr := e.messagingResult("send", result.Receipt, result.Wait, callErr)
			if resultErr != nil {
				return resultErr
			}
			output.Streaming = true
			return emit(output)
		}
		return e.streamSend(ctx, stream, request, emit)
	case "reply":
		svc, original, request, err := e.replyInvocation(ctx, inv)
		if err != nil {
			return err
		}
		stream, ok := svc.(AcceptanceMessagingService)
		if request.Wait && !ok {
			return cliErr("internal_error", "reply --wait requires the durable acceptance stream", "unknown", cli.ExitInternal)
		}
		if !request.Wait {
			result, callErr := svc.Reply(ctx, original, request)
			output, resultErr := e.messagingResult("reply", result.Receipt, result.Wait, callErr)
			if resultErr != nil {
				return resultErr
			}
			output.Streaming = true
			return emit(output)
		}
		return e.streamReply(ctx, stream, original, request, emit)
	default:
		result, err := e.Execute(ctx, inv)
		if err != nil {
			return err
		}
		result.Streaming = true
		return emit(result)
	}
}

func (e *Executor) streamSend(ctx context.Context, svc AcceptanceMessagingService, request service.SendRequest, emit func(cli.ExecutionResult) error) error {
	var callbackErr error
	var acceptedReceipt mektup.Receipt
	result, callErr := svc.SendWithAcceptance(ctx, request, func(accepted service.SendResult) error {
		acceptedReceipt = accepted.Receipt
		output := acceptedResult("send", accepted.Receipt)
		callbackErr = emit(output)
		return callbackErr
	})
	if callbackErr != nil {
		return callbackErr
	}
	output, resultErr := e.terminalResult("send", result.Receipt, result.Wait, callErr)
	if resultErr != nil {
		if acceptedReceipt.ReceiptID != "" {
			return emit(terminalErrorResult("send", acceptedReceipt, resultErr))
		}
		return resultErr
	}
	return emit(output)
}

func (e *Executor) streamReply(ctx context.Context, svc AcceptanceMessagingService, original service.OriginalResolver, request service.ReplyRequest, emit func(cli.ExecutionResult) error) error {
	var callbackErr error
	var acceptedReceipt mektup.Receipt
	result, callErr := svc.ReplyWithAcceptance(ctx, original, request, func(accepted service.ReplyResult) error {
		acceptedReceipt = accepted.Receipt
		output := acceptedResult("reply", accepted.Receipt)
		callbackErr = emit(output)
		return callbackErr
	})
	if callbackErr != nil {
		return callbackErr
	}
	output, resultErr := e.terminalResult("reply", result.Receipt, result.Wait, callErr)
	if resultErr != nil {
		if acceptedReceipt.ReceiptID != "" {
			return emit(terminalErrorResult("reply", acceptedReceipt, resultErr))
		}
		return resultErr
	}
	return emit(output)
}

func acceptedResult(kind string, receipt mektup.Receipt) cli.ExecutionResult {
	return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: lifecycle(kind+".accepted", receipt, false, true)}}, Receipt: receipt, Streaming: true}
}

func terminalErrorResult(kind string, receipt mektup.Receipt, err error) cli.ExecutionResult {
	event := lifecycle(kind+".accepted", receipt, true, false)
	attachError(event, err)
	return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: event}}, Receipt: receipt, Exit: errorExit(err), Streaming: true}
}

func (e *Executor) terminalResult(kind string, receipt mektup.Receipt, wait *service.WaitResult, callErr error) (cli.ExecutionResult, error) {
	result, err := e.messagingResult(kind, receipt, wait, callErr)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	if wait != nil && len(result.Events) > 1 {
		result.Events = result.Events[len(result.Events)-1:]
	}
	result.Streaming = true
	return result, nil
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
	svc, request, err := e.sendInvocation(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	result, callErr := svc.Send(ctx, request)
	return e.messagingResult("send", result.Receipt, result.Wait, callErr)
}

func (e *Executor) sendInvocation(ctx context.Context, inv cli.Invocation) (MessagingService, service.SendRequest, error) {
	if len(inv.Position) < 1 {
		return nil, service.SendRequest{}, cliErr("invalid_arguments", "send target is required", "not_sent", cli.ExitUsage)
	}
	body, err := e.body(ctx, inv, 1)
	if err != nil {
		return nil, service.SendRequest{}, err
	}
	request := service.SendRequest{Target: inv.Position[0], Body: body, Raw: has(inv, "raw"), RequestReply: has(inv, "request-reply"), Wait: has(inv, "wait"), Source: inv.Option("reply-to")}
	request.DeliveryTimeout, err = duration(inv, "delivery-timeout")
	if err != nil {
		return nil, service.SendRequest{}, err
	}
	request.DisableDeliveryTimeout = inv.Option("delivery-timeout") == "0"
	request.WaitTimeout, err = duration(inv, "wait-timeout")
	if err != nil {
		return nil, service.SendRequest{}, err
	}
	svc, err := e.service(ctx, inv)
	if err != nil {
		return nil, service.SendRequest{}, err
	}
	return svc, request, nil
}

func (e *Executor) reply(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	svc, original, request, err := e.replyInvocation(ctx, inv)
	if err != nil {
		return cli.ExecutionResult{}, err
	}
	result, callErr := svc.Reply(ctx, original, request)
	return e.messagingResult("reply", result.Receipt, result.Wait, callErr)
}

func (e *Executor) replyInvocation(ctx context.Context, inv cli.Invocation) (MessagingService, service.OriginalResolver, service.ReplyRequest, error) {
	if len(inv.Position) < 1 {
		return nil, nil, service.ReplyRequest{}, cliErr("invalid_arguments", "reply reference is required", "not_sent", cli.ExitUsage)
	}
	body, err := e.body(ctx, inv, 1)
	if err != nil {
		return nil, nil, service.ReplyRequest{}, err
	}
	status := mektup.ReplySuccess
	if inv.Option("status") == "error" {
		status = mektup.ReplyError
	}
	request := service.ReplyRequest{Reference: inv.Position[0], Body: body, Status: status, ErrorCode: inv.Option("error-code"), Source: inv.Option("reply-to"), Wait: has(inv, "wait")}
	request.DeliveryTimeout, err = duration(inv, "delivery-timeout")
	if err != nil {
		return nil, nil, service.ReplyRequest{}, err
	}
	request.DisableDeliveryTimeout = inv.Option("delivery-timeout") == "0"
	request.WaitTimeout, err = duration(inv, "wait-timeout")
	if err != nil {
		return nil, nil, service.ReplyRequest{}, err
	}
	svc, err := e.service(ctx, inv)
	if err != nil {
		return nil, nil, service.ReplyRequest{}, err
	}
	if e.ports.Original == nil {
		if inv.Option("receipt-file") == "" || e.ports.ImportResolver == nil {
			if inv.Option("receipt-file") != "" {
				return nil, nil, service.ReplyRequest{}, cliErr("route_unavailable", "receipt-file requires an injected untrusted receipt resolver", "not_sent", cli.ExitRejected)
			}
			return nil, nil, service.ReplyRequest{}, cliErr("message_not_found", "original message resolver factory is required", "not_sent", cli.ExitInternal)
		}
	}
	var original service.OriginalResolver
	if inv.Option("receipt-file") != "" {
		if e.ports.ImportResolver == nil {
			return nil, nil, service.ReplyRequest{}, cliErr("route_unavailable", "receipt-file requires an injected untrusted receipt resolver", "not_sent", cli.ExitRejected)
		}
		imported, importErr := e.importReceipt(ctx, inv)
		if importErr != nil {
			return nil, nil, service.ReplyRequest{}, importErr
		}
		original, err = e.ports.ImportResolver.ResolveOriginal(ctx, inv, imported)
	} else {
		original, err = e.ports.Original(ctx, inv)
	}
	if err != nil {
		return nil, nil, service.ReplyRequest{}, mapError(err)
	}
	if original == nil {
		return nil, nil, service.ReplyRequest{}, cliErr("message_not_found", "original message resolver factory returned nil", "not_sent", cli.ExitInternal)
	}
	return svc, original, request, nil
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
	reference := inv.Position[0]
	if inv.Option("receipt-file") != "" {
		if e.ports.ImportResolver == nil {
			return cli.ExecutionResult{}, cliErr("route_unavailable", "receipt-file requires an injected untrusted receipt resolver", "not_sent", cli.ExitRejected)
		}
		imported, importErr := e.importReceipt(ctx, inv)
		if importErr != nil {
			return cli.ExecutionResult{}, importErr
		}
		reference, err = e.ports.ImportResolver.ResolveWaitReference(ctx, inv, imported)
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
	}
	result, callErr := svc.Wait(ctx, service.WaitRequest{Reference: reference, Timeout: timeout})
	if result.Receipt.ReceiptID == "" {
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr)
		}
		return cli.ExecutionResult{}, cliErr("internal_error", "wait returned no receipt", "unknown", cli.ExitInternal)
	}
	eventName := "reply.accepted"
	if result.State == mektup.StateReplyOutcomeUnknown {
		eventName = "reply.unknown"
	} else if result.Incomplete {
		eventName = "wait.incomplete"
	}
	ok := result.State == mektup.StateReplyAccepted || result.State == mektup.StateReplyObserved
	if result.ReplyStatus == string(mektup.ReplyError) {
		ok = false
	}
	event := lifecycle(eventName, result.Receipt, true, ok)
	if callErr != nil {
		attachError(event, callErr)
		return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: event}}, Receipt: result.Receipt, Exit: waitExit(&result, callErr)}, nil
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
	return cli.ExecutionResult{Events: []cli.OutputEvent{{
		Machine: map[string]any{
			"schema":   cli.EventSchema,
			"event":    "inspect.completed",
			"terminal": true,
			"ok":       true,
			"data":     map[string]any{"target": result.Target, "receipts": result.Receipts, "blockers": result.Blockers},
		},
		Human: humanInspect(result),
	}}}, nil
}

func (e *Executor) receipt(ctx context.Context, inv cli.Invocation) (cli.ExecutionResult, error) {
	if len(inv.Position) < 1 || e.ports.Receipts == nil {
		return cli.ExecutionResult{}, cliErr("internal_error", "receipt store is required", "not_sent", cli.ExitInternal)
	}
	sub := inv.Position[0]
	switch sub {
	case "list":
		since, sinceErr := parseSince(inv.Option("since"))
		if sinceErr != nil {
			return cli.ExecutionResult{}, sinceErr
		}
		items, err := e.ports.Receipts.List(ctx, receipts.ListOptions{State: mektup.EvidenceState(inv.Option("state")), Since: since, Limit: optionInt(inv, "limit")})
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		return cli.ExecutionResult{Events: []cli.OutputEvent{{
			Machine: map[string]any{
				"schema":   cli.EventSchema,
				"event":    "receipt.list",
				"terminal": true,
				"ok":       true,
				"data":     map[string]any{"receipts": items},
			},
			Human: humanReceiptList(items),
		}}}, nil
	case "show":
		if len(inv.Position) != 2 {
			return cli.ExecutionResult{}, cliErr("invalid_arguments", "receipt show reference is required", "not_sent", cli.ExitUsage)
		}
		receipt, err := e.ports.Receipts.Show(ctx, inv.Position[1], receipts.ShowOptions{Portable: has(inv, "portable"), Content: has(inv, "content")})
		if err != nil {
			return cli.ExecutionResult{}, mapError(err)
		}
		if !has(inv, "content") {
			if has(inv, "portable") {
				encoded, marshalErr := json.Marshal(receipt)
				if marshalErr != nil {
					return cli.ExecutionResult{}, mapError(marshalErr)
				}
				return cli.ExecutionResult{RawJSON: encoded, Human: humanReceipt(receipt)}, nil
			}
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
		return cli.ExecutionResult{Events: []cli.OutputEvent{{
			Machine: map[string]any{"schema": cli.EventSchema, "event": "receipt.content", "terminal": true, "ok": true, "data": data},
			Human:   humanContent(content),
		}}}, nil
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
		presentation := "agent"
		if inv.Resolved.Output == cli.PresentationHuman {
			presentation = "human"
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
		data, err = e.ports.Input.ReadStdin(ctx, int64(4*MaxInputChars+1))
	case file:
		if e.ports.Input == nil {
			return "", cliErr("internal_error", "input port is required", "not_sent", cli.ExitInternal)
		}
		data, err = e.ports.Input.ReadFile(ctx, inv.Option("file"), int64(4*MaxInputChars+1))
	default:
		data = []byte(inv.Position[messageIndex])
	}
	if err != nil {
		return "", cliErr("invalid_arguments", "message source could not be read", "not_sent", cli.ExitUsage)
	}
	if len(data) > 4*MaxInputChars {
		return "", cliErr("input_too_large", "message body exceeds the UTF-8 byte bound for the character limit", "not_sent", cli.ExitUsage)
	}
	if len(data) == 0 || !utf8.Valid(data) {
		return "", cliErr("invalid_arguments", "message body must be non-empty UTF-8", "not_sent", cli.ExitUsage)
	}
	if utf8.RuneCount(data) > MaxInputChars {
		return "", cliErr("input_too_large", "message body exceeds the 2^20 character limit", "not_sent", cli.ExitUsage)
	}
	return string(data), nil
}

func (e *Executor) importReceipt(ctx context.Context, inv cli.Invocation) (receipts.Imported, error) {
	if inv.Option("receipt-file") == "" || e.ports.Input == nil {
		return receipts.Imported{}, cliErr("invalid_arguments", "--receipt-file requires an injected input port", "not_sent", cli.ExitUsage)
	}
	data, err := e.ports.Input.ReadFile(ctx, inv.Option("receipt-file"), int64(4*MaxInputChars+1))
	if err != nil {
		return receipts.Imported{}, cliErr("invalid_arguments", "receipt file could not be read", "not_sent", cli.ExitUsage)
	}
	if len(data) > 4*MaxInputChars {
		return receipts.Imported{}, cliErr("input_too_large", "receipt file exceeds the bounded input size", "not_sent", cli.ExitUsage)
	}
	imported, err := receipts.Import(data)
	if err != nil {
		return receipts.Imported{}, cliErr("invalid_arguments", "receipt file is not a valid untrusted portable receipt", "not_sent", cli.ExitUsage)
	}
	return imported, nil
}

func (e *Executor) messagingResult(kind string, accepted mektup.Receipt, wait *service.WaitResult, callErr error) (cli.ExecutionResult, error) {
	if accepted.ReceiptID == "" {
		if callErr != nil {
			return cli.ExecutionResult{}, mapError(callErr)
		}
		return cli.ExecutionResult{}, cliErr("internal_error", "messaging service returned no receipt", "unknown", cli.ExitInternal)
	}
	acceptedEvidence := accepted.State == mektup.StateAccepted || accepted.State == mektup.StateReplyAccepted || accepted.State == mektup.StateReplyObserved
	if wait == nil && !acceptedEvidence {
		name := kind + ".incomplete"
		ok := false
		if accepted.State == mektup.StateReplyOutcomeUnknown {
			name = kind + ".unknown"
		}
		event := lifecycle(name, accepted, true, ok)
		if callErr != nil {
			attachError(event, callErr)
		}
		return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: event}}, Receipt: accepted, Exit: errorExit(callErr)}, nil
	}
	events := []cli.OutputEvent{{Machine: lifecycle(kind+".accepted", accepted, wait == nil, acceptedEvidence)}}
	final := accepted
	exit := cli.ExitSuccess
	if wait != nil {
		final = wait.Receipt
		name := "reply.accepted"
		if wait.State == mektup.StateReplyOutcomeUnknown {
			name = "reply.unknown"
		} else if wait.Incomplete {
			name = "wait.incomplete"
		}
		ok := !wait.Incomplete && wait.State != mektup.StateReplyOutcomeUnknown && wait.ReplyStatus != string(mektup.ReplyError)
		terminal := lifecycle(name, final, true, ok)
		if callErr != nil {
			attachError(terminal, callErr)
		}
		events = append(events, cli.OutputEvent{Machine: terminal})
		exit = waitExit(wait, callErr)
	}
	if callErr != nil && wait == nil {
		attachError(events[0].Machine.(map[string]any), callErr)
		return cli.ExecutionResult{Events: events, Receipt: final, Exit: errorExit(callErr)}, nil
	}
	return cli.ExecutionResult{Events: events, Receipt: final, Exit: exit}, nil
}

func attachError(event map[string]any, err error) {
	data, _ := event["data"].(map[string]any)
	if data == nil {
		data = map[string]any{}
		event["data"] = data
	}
	mapped := mapError(err)
	if ce, ok := mapped.(*cli.Error); ok {
		details := ce.Details
		if details == nil {
			details = map[string]any{}
		}
		data["error"] = map[string]any{"code": ce.Code, "message": ce.Message, "retryable": ce.Retryable, "effectState": ce.Effect, "details": details}
	}
}

func lifecycle(name string, receipt mektup.Receipt, terminal, ok bool) map[string]any {
	return map[string]any{"schema": cli.EventSchema, "event": name, "operationId": receipt.OperationID, "terminal": terminal, "ok": ok, "data": map[string]any{"receipt": receipt}}
}
func receiptResult(name string, receipt mektup.Receipt) (cli.ExecutionResult, error) {
	return cli.ExecutionResult{Events: []cli.OutputEvent{{Machine: lifecycle(name, receipt, true, true)}}, Receipt: receipt}, nil
}

func humanReceipt(receipt mektup.Receipt) string {
	return fmt.Sprintf("receipt %s state=%s operation=%s", receipt.ReceiptID, receipt.State, receipt.OperationID)
}
func humanReceiptList(items []mektup.Receipt) string {
	lines := make([]string, 0, len(items))
	for _, item := range items {
		lines = append(lines, humanReceipt(item))
	}
	if len(lines) == 0 {
		return "no receipts"
	}
	return strings.Join(lines, "\n")
}
func humanInspect(result receipts.InspectResult) string {
	return fmt.Sprintf("target %s thread=%s endpoint=%s receipts=%d blockers=%d", result.Target.Requested, result.Target.ThreadID, result.Target.EndpointID, len(result.Receipts), len(result.Blockers))
}
func humanContent(content receipts.ContentResult) string {
	if content.Spill != nil {
		return fmt.Sprintf("content spilled %s bytes=%d digest=%s", content.Spill.Path, content.Spill.Bytes, content.Spill.Digest)
	}
	if len(content.Body) == 0 {
		return fmt.Sprintf("content bytes=%d digest=%s", content.Bytes, content.Digest)
	}
	return string(content.Body)
}
func waitExit(wait *service.WaitResult, err error) cli.ExitCode {
	if wait.State == mektup.StateReplyOutcomeUnknown {
		return cli.ExitUnknown
	}
	if wait.Incomplete {
		return cli.ExitIncomplete
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
		details := se.Details
		if details == nil {
			details = map[string]any{}
		}
		return &cli.Error{Code: string(se.Code), Message: se.Message, Retryable: se.Retryable, Effect: serviceEffect(se.Code), Exit: serviceExit(se.Code), Details: details}
	}
	if errors.Is(err, receipts.ErrNotFound) || errors.Is(err, journal.ErrNotFound) {
		return cliErr("message_not_found", "receipt was not found", "rejected", cli.ExitRejected)
	}
	if errors.Is(err, receipts.ErrAmbiguous) || errors.Is(err, journal.ErrReceiptConflict) {
		return cliErr("target_ambiguous", "receipt reference is ambiguous", "rejected", cli.ExitRejected)
	}
	if errors.Is(err, receipts.ErrUntrusted) {
		return cliErr("invalid_arguments", "portable receipt is untrusted input", "not_sent", cli.ExitUsage)
	}
	if errors.Is(err, receipts.ErrIdentityMismatch) || errors.Is(err, receipts.ErrDigestMismatch) || errors.Is(err, journal.ErrIdentityConflict) {
		return cliErr("message_identity_conflict", "receipt evidence does not match the pinned message identity", "rejected", cli.ExitRejected)
	}
	if errors.Is(err, receipts.ErrContentUnavailable) {
		return cliErr("content_unavailable", "receipt content is unavailable", "rejected", cli.ExitRejected)
	}
	if errors.Is(err, receipts.ErrOutputTooLarge) {
		return cliErr("output_too_large", "receipt content exceeds the inline output bound", "rejected", cli.ExitRejected)
	}
	if errors.Is(err, receipts.ErrReconcileIncomplete) {
		return cliErr("wait_incomplete", "receipt reconciliation is incomplete", "unknown", cli.ExitIncomplete)
	}
	if errors.Is(err, receipts.ErrRouteUnavailable) {
		return cliErr("route_unavailable", "receipt route is unavailable", "rejected", cli.ExitRejected)
	}
	if errors.Is(err, receipts.ErrHumanGateRequired) {
		return cliErr("effect_acknowledgment_required", "an explicit assertion gate is required", "not_sent", cli.ExitUsage)
	}
	if errors.Is(err, receipts.ErrPortableContent) || errors.Is(err, receipts.ErrInvalidArguments) || errors.Is(err, receipts.ErrLimit) {
		return cliErr("invalid_arguments", "invalid receipt request", "not_sent", cli.ExitUsage)
	}
	if errors.Is(err, journal.ErrStorageBusy) {
		return cliErr("storage_busy", "receipt storage is busy", "unknown", cli.ExitUnknown)
	}
	if errors.Is(err, journal.ErrAlreadyWon) || errors.Is(err, journal.ErrClaimExpired) || errors.Is(err, journal.ErrClaimNotOwned) {
		return cliErr("reply_outcome_unknown", "reply custody outcome is unknown", "unknown", cli.ExitUnknown)
	}
	if errors.Is(err, journal.ErrStorageCorrupt) || errors.Is(err, journal.ErrCorrupt) {
		return cliErr("storage_corrupt", "receipt storage is corrupt", "unknown", cli.ExitInternal)
	}
	return &cli.Error{Code: "internal_error", Message: err.Error(), Effect: "unknown", Exit: cli.ExitInternal}
}

func serviceEffect(code mektup.ErrorCode) string {
	switch code {
	case mektup.ErrInvalidArguments, mektup.ErrInvalidTarget, mektup.ErrResolverUnavailable, mektup.ErrTargetAmbiguous, mektup.ErrRouteUnavailable, mektup.ErrEndpointUnavailable, mektup.ErrUnsupportedServerVersion, mektup.ErrInputTooLarge, mektup.ErrReplyRouteRequired, mektup.ErrInvalidRawWait, mektup.ErrInvalidRawReplyRequest, mektup.ErrEffectAcknowledgmentRequired:
		return "not_sent"
	case mektup.ErrDeliveryRejected, mektup.ErrReplyRouteUnavailable, mektup.ErrReplyNotRequested, mektup.ErrMessageNotFound, mektup.ErrMessageIdentityConflict, mektup.ErrMessageNotAddressedThread, mektup.ErrContentUnavailable:
		return "rejected"
	case mektup.ErrOutcomeUnknown, mektup.ErrReplyOutcomeUnknown, mektup.ErrStorageBusy:
		return "unknown"
	case mektup.ErrDeliveryTemporarilyUnavailable:
		return "rejected"
	case mektup.ErrWaitIncomplete, mektup.ErrWaitInterrupted:
		return "unknown"
	default:
		return "unknown"
	}
}

func serviceExit(code mektup.ErrorCode) cli.ExitCode {
	switch code {
	case mektup.ErrInvalidArguments, mektup.ErrInvalidTarget, mektup.ErrInputTooLarge, mektup.ErrReplyRouteRequired, mektup.ErrInvalidRawWait, mektup.ErrInvalidRawReplyRequest, mektup.ErrEffectAcknowledgmentRequired:
		return cli.ExitUsage
	case mektup.ErrResolverUnavailable, mektup.ErrTargetAmbiguous, mektup.ErrRouteUnavailable, mektup.ErrEndpointUnavailable, mektup.ErrUnsupportedServerVersion, mektup.ErrDeliveryRejected, mektup.ErrDeliveryTemporarilyUnavailable, mektup.ErrReplyRouteUnavailable, mektup.ErrReplyNotRequested, mektup.ErrMessageNotFound, mektup.ErrMessageIdentityConflict, mektup.ErrMessageNotAddressedThread, mektup.ErrContentUnavailable:
		return cli.ExitRejected
	case mektup.ErrOutcomeUnknown, mektup.ErrReplyOutcomeUnknown, mektup.ErrStorageBusy:
		return cli.ExitUnknown
	case mektup.ErrWaitIncomplete:
		return cli.ExitIncomplete
	case mektup.ErrWaitInterrupted:
		return cli.ExitIncomplete
	default:
		return cli.ExitInternal
	}
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
func parseSince(value string) (time.Time, error) {
	if value == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, cliErr("invalid_arguments", "invalid --since timestamp", "not_sent", cli.ExitUsage)
	}
	return parsed, nil
}
