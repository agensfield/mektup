// Package compat classifies the app-server version observed during initialize.
package compat

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/mod/semver"
)

const MinimumExclusive = "v0.142.0"

var testedVersions = [...]string{"v0.154.0", "v0.155.1", "v0.156.0", "v0.156.1", "v0.157.0"}

// Class describes whether Mektup has conformance evidence for a server.
type Class string

const (
	Tested      Class = "tested"
	Untested    Class = "untested"
	Unsupported Class = "unsupported"
	Unknown     Class = "unknown"
)

// Stable warning codes from the wire contract.
const (
	WarningUntested = "untested_server_version"
	WarningUnknown  = "server_version_unknown"
)

var ErrMissingUserAgent = errors.New("initialize response has no nonempty userAgent string")

// x/mod/semver deliberately accepts shorthand such as v1 and v1.2. App-server
// compatibility evidence requires the complete SemVer core emitted by a real
// release before it can be compared with a tested version or support floor.
var completeSemver = regexp.MustCompile(`^[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?(?:\+[0-9A-Za-z-]+(?:\.[0-9A-Za-z-]+)*)?$`)

// Result preserves the authoritative handshake value and its classification.
type Result struct {
	UserAgent string
	Product   string
	Version   string
	Class     Class
	Warning   string
}

// Classify parses the first product/version token from initialize.userAgent.
// A present but unparseable version is allowed with an explicit unknown warning.
func Classify(userAgent string) (Result, error) {
	userAgent = strings.TrimSpace(userAgent)
	if userAgent == "" {
		return Result{}, ErrMissingUserAgent
	}

	result := Result{UserAgent: userAgent}
	token := strings.Fields(userAgent)[0]
	product, rawVersion, ok := strings.Cut(token, "/")
	if !ok || product == "" || rawVersion == "" {
		result.Class = Unknown
		result.Warning = WarningUnknown
		return result, nil
	}
	result.Product = product
	originalVersion := rawVersion
	if !completeSemver.MatchString(originalVersion) {
		result.Version = originalVersion
		result.Class = Unknown
		result.Warning = WarningUnknown
		return result, nil
	}
	if !strings.HasPrefix(rawVersion, "v") {
		rawVersion = "v" + rawVersion
	}
	if !semver.IsValid(rawVersion) {
		result.Version = strings.TrimPrefix(rawVersion, "v")
		result.Class = Unknown
		result.Warning = WarningUnknown
		return result, nil
	}

	result.Version = strings.TrimPrefix(rawVersion, "v")
	switch {
	case semver.Compare(rawVersion, MinimumExclusive) <= 0:
		result.Class = Unsupported
	case isTested(rawVersion):
		result.Class = Tested
	default:
		result.Class = Untested
		result.Warning = WarningUntested
	}
	return result, nil
}

func isTested(version string) bool {
	for _, tested := range testedVersions {
		if semver.Compare(version, tested) == 0 {
			return true
		}
	}
	return false
}

// RequireSupported converts only the locked compatibility floor into an error.
// Untested and unknown versions remain best-effort and carry warning codes.
func RequireSupported(result Result) error {
	if result.Class != Unsupported {
		return nil
	}
	return fmt.Errorf("app-server %s is unsupported; Mektup requires a version newer than %s", result.Version, strings.TrimPrefix(MinimumExclusive, "v"))
}
