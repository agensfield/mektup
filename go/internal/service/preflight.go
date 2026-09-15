package service

import (
	"fmt"
	"unicode/utf8"

	"github.com/agensfield/mektup/go"
)

func preflight(rendered []byte, body string) error {
	bytes := len(rendered)
	chars := utf8.RuneCount(rendered)
	if bytes > MaxInputBytes || chars > MaxInputChars {
		return semantic(mektup.ErrInputTooLarge, "message envelope exceeds the app-server text-input limit", map[string]any{
			"bytes": bytes, "chars": chars, "maxBytes": MaxInputBytes, "maxChars": MaxInputChars,
			"bodyBytes": len([]byte(body)), "bodyChars": utf8.RuneCountInString(body),
		}, nil)
	}
	return nil
}

func validateObservedEnvelope(item ObservedItem, original OperationStatus, accepted bool) (mektup.Envelope, error) {
	e, err := mektup.ParseEnvelopeString(item.Text)
	if err != nil {
		return mektup.Envelope{}, err
	}
	if e.Kind != mektup.KindReply || e.InReplyTo != original.MessageID {
		return mektup.Envelope{}, fmt.Errorf("reply correlation mismatch")
	}
	if original.SourceRoute == "" || e.To != original.SourceRoute {
		return mektup.Envelope{}, fmt.Errorf("reply route mismatch")
	}
	if e.PayloadSHA256 != original.Digest || int64(e.PayloadBytes) != original.BodySize {
		return mektup.Envelope{}, fmt.Errorf("reply body evidence mismatch")
	}
	if item.ThreadID != "" && item.ThreadID != threadID(original.SourceRoute) {
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
