package sshproxy

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"

	mektup "github.com/agensfield/mektup/go"
)

func validPreflightRequest() PreflightDocument {
	ep := mektup.NewEndpointID()
	return PreflightDocument{Schema: PreflightSchema, Kind: "request", RequestID: mektup.NewPrefixedID("probe_"), Mode: "route", TargetEndpointID: mektup.NewEndpointID(), ReplyDestination: &DestinationRef{EndpointID: ep, ThreadID: "thread-1", URI: "codex://" + ep + "/thread/thread-1"}, Custody: &CustodyRef{EndpointID: ep, StoreID: mektup.NewStoreID()}}
}
func TestPreflightMetadataRejectsPathsBodiesAliasesAndBounds(t *testing.T) {
	req := validPreflightRequest()
	data, _ := json.Marshal(req)
	if _, err := DecodePreflight(data); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"body", "path", "executable", "shell", "codexHome"} {
		var raw map[string]any
		_ = json.Unmarshal(data, &raw)
		raw[field] = "untrusted"
		encoded, _ := json.Marshal(raw)
		if _, err := DecodePreflight(encoded); err == nil {
			t.Fatalf("accepted %s", field)
		}
	}
	req.ReplyDestination.URI = "codex://alias/thread/thread-1"
	data, _ = json.Marshal(req)
	if _, err := DecodePreflight(data); err == nil {
		t.Fatal("accepted alias instead of exact stable URI")
	}
	if _, err := DecodePreflight([]byte(strings.Repeat(" ", int(PreflightLimit)+1))); !errors.Is(err, ErrControlTooLarge) {
		t.Fatalf("size=%v", err)
	}
}
func TestInvokePreflightUsesFixedCommandAndCorrelates(t *testing.T) {
	for _, swapped := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid", true: "swapped"}[swapped], func(t *testing.T) {
			p := newFakeProcess()
			factory := &fakeFactory{process: p}
			req := validPreflightRequest()
			go func() {
				line, _ := bufio.NewReader(p.stdinR).ReadBytes('\n')
				received, err := DecodePreflight(line)
				if err != nil {
					return
				}
				received.Kind = "result"
				if swapped {
					received.RequestID = mektup.NewPrefixedID("probe_")
				}
				data, _ := json.Marshal(received)
				_, _ = p.stdoutW.Write(data)
				_ = p.stdoutW.Close()
				_ = p.stderrW.Close()
				p.releaseWait()
			}()
			err := InvokePreflight(context.Background(), Config{Host: "peer"}, req, factory)
			if (err != nil) != swapped {
				t.Fatalf("err=%v", err)
			}
			if !reflect.DeepEqual(factory.argv, []string{"ssh", "--", "peer", "mektup", "control", "preflight"}) {
				t.Fatalf("argv=%v", factory.argv)
			}
		})
	}
}
func TestInvokePreflightCancellationAndOutputBound(t *testing.T) {
	for _, large := range []bool{false, true} {
		t.Run(map[bool]string{false: "canceled", true: "large"}[large], func(t *testing.T) {
			p := newFakeProcess()
			go func() {
				_, _ = bufio.NewReader(p.stdinR).ReadBytes('\n')
				if large {
					_, _ = io.WriteString(p.stdoutW, strings.Repeat("x", int(PreflightLimit)+1))
					_ = p.stdoutW.Close()
					_ = p.stderrW.Close()
					p.releaseWait()
				}
			}()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
			defer cancel()
			err := InvokePreflight(ctx, Config{Host: "peer", CleanupTimeout: 20 * time.Millisecond}, validPreflightRequest(), &fakeFactory{process: p})
			if large && !errors.Is(err, ErrControlTooLarge) {
				t.Fatalf("bound=%v", err)
			}
			if !large && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("cancel=%v", err)
			}
		})
	}
}

func TestInvokePreflightReportsOldPeerWithoutDispatchFallback(t *testing.T) {
	p := newFakeProcess()
	p.waitErr = errors.New("exit status 2")
	go func() {
		_, _ = bufio.NewReader(p.stdinR).ReadBytes('\n')
		_, _ = io.WriteString(p.stderrW, "unknown command: control")
		_ = p.stderrW.Close()
		_ = p.stdoutW.Close()
		p.releaseWait()
	}()
	err := InvokePreflight(context.Background(), Config{Host: "old-peer"}, validPreflightRequest(), &fakeFactory{process: p})
	if !errors.Is(err, ErrPreflightUnsupported) {
		t.Fatalf("unsupported=%v", err)
	}
}
