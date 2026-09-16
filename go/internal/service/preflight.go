package service

import (
	"fmt"
	"unicode/utf8"

	"github.com/agensfield/mektup/go"
)

func preflight(rendered []byte, body string) error {
	bytes := len(rendered)
	chars := utf8.RuneCount(rendered)
	if chars > MaxInputChars {
		return semantic(mektup.ErrInputTooLarge, "message envelope exceeds the app-server text-input limit", map[string]any{
			"inputChars": chars, "maxChars": MaxInputChars,
			"inputBytes": bytes, "bodyBytes": len([]byte(body)), "bodyChars": utf8.RuneCountInString(body),
			"envelopeOverheadBytes": bytes - len([]byte(body)),
		}, nil)
	}
	return nil
}

func validateObservedEnvelope(item ObservedItem, original OperationStatus, accepted bool) (mektup.Envelope, error) {
	if item.NativeType != "" && item.NativeType != "userMessage" {
		return mektup.Envelope{}, fmt.Errorf("reply was observed in a non-user native item")
	}
	e, err := mektup.ParseEnvelopeString(item.Text)
	if err != nil {
		return mektup.Envelope{}, err
	}
	if e.Kind != mektup.KindReply || e.InReplyTo != original.MessageID {
		return mektup.Envelope{}, fmt.Errorf("reply correlation mismatch")
	}
	if original.ReplyStatus != "" && string(e.ReplyStatus) != original.ReplyStatus {
		return mektup.Envelope{}, fmt.Errorf("reply status mismatch")
	}
	if (original.ReplyStatus == string(mektup.ReplySuccess) && e.ReplyErrorCode != "") || (original.ReplyStatus == string(mektup.ReplyError) && e.ReplyErrorCode != original.ReplyErrorCode) {
		return mektup.Envelope{}, fmt.Errorf("reply error code mismatch")
	}
	expectedRoute := original.ReplyRoute
	if expectedRoute == "" {
		expectedRoute = original.SourceRoute
	}
	if expectedRoute == "" || e.To != expectedRoute {
		return mektup.Envelope{}, fmt.Errorf("reply destination mismatch")
	}
	if original.TargetRoute != "" && e.From != original.TargetRoute {
		return mektup.Envelope{}, fmt.Errorf("reply sender route mismatch")
	}
	if original.Operation.TargetEndpointID != "" && e.FromEndpointID != original.Operation.TargetEndpointID {
		return mektup.Envelope{}, fmt.Errorf("reply sender endpoint mismatch")
	}
	expectedEndpoint := original.ReplyEndpointID
	if expectedEndpoint == "" {
		expectedEndpoint = original.Operation.SourceEndpointID
	}
	if expectedEndpoint != "" && e.ToEndpointID != expectedEndpoint {
		return mektup.Envelope{}, fmt.Errorf("reply destination endpoint mismatch")
	}
	if original.ReplyDigest != "" && e.PayloadSHA256 != original.ReplyDigest {
		return mektup.Envelope{}, fmt.Errorf("reply body digest mismatch")
	}
	if original.ReplyDigest != "" && int64(e.PayloadBytes) != original.ReplyBodySize {
		return mektup.Envelope{}, fmt.Errorf("reply body size mismatch")
	}
	if item.ThreadID == "" || item.ThreadID != threadID(expectedRoute) {
		return mektup.Envelope{}, fmt.Errorf("reply was observed in a different thread")
	}
	if item.ClientMessageID != "" && item.ClientMessageID != e.MessageID {
		return mektup.Envelope{}, fmt.Errorf("native client message ID mismatch")
	}
	if accepted && !original.ReplyRequested {
		return mektup.Envelope{}, fmt.Errorf("reply was not requested")
	}
	return e, nil
}
