package mektup

import (
	"fmt"
	"net/url"
	"strings"
)

// ThreadAddress is the pinned identity in a codex:// thread URI. Endpoint is
// the URI's installation-local alias; it is not authority for a portable ID.
type ThreadAddress struct {
	Endpoint string
	ThreadID string
}

func ParseThreadURI(value string) (ThreadAddress, error) {
	u, err := parseAddressURL(value, "codex")
	if err != nil {
		return ThreadAddress{}, err
	}
	parts := splitAddressPath(u.EscapedPath())
	if len(parts) != 2 || parts[0] != "thread" {
		return ThreadAddress{}, fmt.Errorf("mektup: codex URI must be codex://<endpoint>/thread/<thread-id>")
	}
	id, err := url.PathUnescape(parts[1])
	if err != nil || id == "" || strings.ContainsAny(id, "/\\") {
		return ThreadAddress{}, fmt.Errorf("mektup: invalid codex thread ID")
	}
	return ThreadAddress{Endpoint: u.Host, ThreadID: id}, nil
}

type HerdrAddress struct {
	Endpoint string
	Kind     string
	Selector string
}

func ParseHerdrURI(value string) (HerdrAddress, error) {
	u, err := parseAddressURL(value, "herdr")
	if err != nil {
		return HerdrAddress{}, err
	}
	parts := splitAddressPath(u.EscapedPath())
	if len(parts) != 2 || (parts[0] != "agent" && parts[0] != "pane") {
		return HerdrAddress{}, fmt.Errorf("mektup: herdr URI must be herdr://<endpoint>/(agent|pane)/<selector>")
	}
	selector, err := url.PathUnescape(parts[1])
	if err != nil || selector == "" || strings.ContainsAny(selector, "/\\") && parts[0] == "agent" {
		return HerdrAddress{}, fmt.Errorf("mektup: invalid herdr selector")
	}
	return HerdrAddress{Endpoint: u.Host, Kind: parts[0], Selector: selector}, nil
}

func parseAddressURL(value, scheme string) (*url.URL, error) {
	u, err := url.Parse(value)
	if err != nil || u.Scheme != scheme || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, fmt.Errorf("mektup: invalid %s address URI %q", scheme, value)
	}
	return u, nil
}

func splitAddressPath(path string) []string {
	trimmed := strings.TrimPrefix(path, "/")
	if trimmed == "" || strings.HasSuffix(path, "/") {
		return nil
	}
	return strings.Split(trimmed, "/")
}

func (e Envelope) validateAddresses() error {
	if _, err := ParseThreadURI(e.To); err != nil {
		return fmt.Errorf("to: %w", err)
	}
	if e.From != "" {
		if _, err := ParseThreadURI(e.From); err != nil {
			return fmt.Errorf("from: %w", err)
		}
	}
	if e.FromHerdr != "" {
		if _, err := ParseHerdrURI(e.FromHerdr); err != nil {
			return fmt.Errorf("from-herdr: %w", err)
		}
	}
	if e.ReplyTo != "" {
		if _, err := ParseThreadURI(e.ReplyTo); err != nil {
			return fmt.Errorf("reply-to: %w", err)
		}
	}
	return nil
}
