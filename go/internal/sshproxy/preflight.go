package sshproxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	mektup "github.com/agensfield/mektup/go"
)

const PreflightSchema = "mektup/preflight/v1"
const PreflightLimit int64 = 16 << 10

var ErrPreflightUnsupported = errors.New("peer does not support reply-route preflight; upgrade both Mektup peers")

// PreflightDocument contains only portable identity metadata. RequestID is a
// correlation nonce, not a durable operation or dispatch authority.
type PreflightDocument struct {
	Schema           string          `json:"schema"`
	Kind             string          `json:"kind"`
	RequestID        string          `json:"requestId"`
	Mode             string          `json:"mode"`
	TargetEndpointID string          `json:"targetEndpointId"`
	ReplyDestination *DestinationRef `json:"replyDestination,omitempty"`
	Custody          *CustodyRef     `json:"custody,omitempty"`
	ErrorCode        string          `json:"errorCode,omitempty"`
	ErrorMessage     string          `json:"errorMessage,omitempty"`
}

func DecodePreflight(data []byte) (PreflightDocument, error) {
	var d PreflightDocument
	if int64(len(data)) > PreflightLimit {
		return d, ErrControlTooLarge
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&d); err != nil {
		return d, fmt.Errorf("preflight: invalid metadata: %w", err)
	}
	if err := decoder.Decode(new(any)); err != io.EOF {
		return d, errors.New("preflight: expected one metadata document")
	}
	if d.Schema != PreflightSchema || (d.Kind != "request" && d.Kind != "result") || mektup.ValidateID(d.RequestID, "probe_") != nil || mektup.ValidateID(d.TargetEndpointID, mektup.EndpointIDPrefix) != nil {
		return d, errors.New("preflight: invalid version or identity")
	}
	if d.Kind == "request" && (d.ErrorCode != "" || d.ErrorMessage != "") {
		return d, errors.New("preflight: request cannot contain result fields")
	}
	if (d.ErrorCode == "") != (d.ErrorMessage == "") {
		return d, errors.New("preflight: incomplete error result")
	}
	switch d.Mode {
	case "route":
		if d.ReplyDestination == nil || d.Custody == nil {
			return d, errors.New("preflight: route requires return and custody identities")
		}
	case "identity":
		if d.ReplyDestination != nil || d.Custody != nil {
			return d, errors.New("preflight: identity accepts only target identity")
		}
	case "custody":
		if d.ReplyDestination != nil || d.Custody == nil || d.Custody.EndpointID != d.TargetEndpointID {
			return d, errors.New("preflight: custody requires matching target identity")
		}
	default:
		return d, errors.New("preflight: unknown mode")
	}
	if c := d.Custody; c != nil {
		if mektup.ValidateID(c.EndpointID, mektup.EndpointIDPrefix) != nil || mektup.ValidateID(c.StoreID, mektup.StoreIDPrefix) != nil {
			return d, errors.New("preflight: invalid custody identity")
		}
	}
	if dest := d.ReplyDestination; dest != nil {
		address, err := mektup.ParseThreadURI(dest.URI)
		if err != nil || address.Endpoint != dest.EndpointID || address.ThreadID != dest.ThreadID || mektup.ValidateID(dest.EndpointID, mektup.EndpointIDPrefix) != nil {
			return d, errors.New("preflight: return URI must name the exact stable endpoint and thread")
		}
	}
	return d, nil
}

func (c Config) PreflightArgv() ([]string, error) {
	c = c.normalized()
	if err := c.Validate(); err != nil {
		return nil, err
	}
	return []string{c.SSHBinary, "--", c.Host, "mektup", "control", "preflight"}, nil
}

// InvokePreflight never dispatches a message body. All I/O and child cleanup
// are bounded; callers provide the total deadline, including reverse probes.
func InvokePreflight(ctx context.Context, cfg Config, req PreflightDocument, factory ProcessFactory) (returnErr error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	if _, err = DecodePreflight(data); err != nil {
		return err
	}
	if req.Kind != "request" {
		return errors.New("preflight: expected request")
	}
	argv, err := cfg.PreflightArgv()
	if err != nil {
		return err
	}
	child, err := startChild(argv, cfg, factory)
	if err != nil {
		return err
	}
	defer func() {
		if err := child.close(); returnErr == nil {
			returnErr = err
		}
	}()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = child.close()
		case <-done:
		}
	}()
	writeDone := make(chan error, 1)
	go func() {
		data = append(data, '\n')
		n, err := child.stdin.Write(data)
		if err == nil && n != len(data) {
			err = io.ErrShortWrite
		}
		closeErr := child.stdin.Close()
		if err == nil {
			err = closeErr
		}
		writeDone <- err
	}()
	select {
	case err = <-writeDone:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	readDone := make(chan controlReadResult, 1)
	go func() {
		defer child.markStdoutDone()
		data, err := io.ReadAll(io.LimitReader(child.stdout, PreflightLimit+1))
		readDone <- controlReadResult{body: data, err: err}
	}()
	var result controlReadResult
	select {
	case result = <-readDone:
	case <-ctx.Done():
		return ctx.Err()
	}
	if result.err != nil {
		return result.err
	}
	if int64(len(result.body)) > PreflightLimit {
		return ErrControlTooLarge
	}
	timer := time.NewTimer(cfg.normalized().CleanupTimeout)
	defer timer.Stop()
	select {
	case <-child.waitDone:
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrCleanupTimeout
	}
	if err, stderr, _ := child.status(); err != nil {
		lower := strings.ToLower(stderr)
		if strings.Contains(lower, "unknown command") || strings.Contains(lower, "unknown subcommand") || strings.Contains(lower, "unrecognized command") {
			return ErrPreflightUnsupported
		}
		return fmt.Errorf("preflight: remote command failed: %w", err)
	}
	response, err := DecodePreflight(result.body)
	if err != nil {
		return fmt.Errorf("preflight: invalid peer response (requires %s): %w", PreflightSchema, err)
	}
	expected := req
	expected.Kind = "result"
	expected.ErrorCode, expected.ErrorMessage = response.ErrorCode, response.ErrorMessage
	want, _ := json.Marshal(expected)
	got, _ := json.Marshal(response)
	if !bytes.Equal(want, got) {
		return errors.New("preflight: response identity mismatch")
	}
	if response.ErrorCode != "" {
		return fmt.Errorf("preflight: %s: %s", response.ErrorCode, response.ErrorMessage)
	}
	return nil
}
