package compat

import (
	"errors"
	"testing"
)

func TestClassify(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		userAgent string
		version   string
		class     Class
		warning   string
	}{
		{"tested", "codex_cli_rs/0.154.0 (Mac OS; arm64)", "0.154.0", Tested, ""},
		{"tested build metadata", "codex_app_server/0.154.0+managed", "0.154.0+managed", Tested, ""},
		{"latest tested", "codex_cli_rs/0.155.1 (Mac OS; arm64)", "0.155.1", Tested, ""},
		{"latest tested build metadata", "codex_app_server/0.155.1+managed", "0.155.1+managed", Tested, ""},
		{"newer", "codex_cli_rs/0.155.0", "0.155.0", Untested, WarningUntested},
		{"between floor and tested", "codex_cli_rs/0.142.1", "0.142.1", Untested, WarningUntested},
		{"floor", "codex_cli_rs/0.142.0", "0.142.0", Unsupported, ""},
		{"floor prerelease", "codex_cli_rs/0.142.0-rc.1", "0.142.0-rc.1", Unsupported, ""},
		{"older", "codex_cli_rs/0.141.9", "0.141.9", Unsupported, ""},
		{"unparseable", "codex_cli_rs/dev-build", "dev-build", Unknown, WarningUnknown},
		{"abbreviated tested", "codex_cli_rs/0.154", "0.154", Unknown, WarningUnknown},
		{"abbreviated floor", "codex_cli_rs/0.142", "0.142", Unknown, WarningUnknown},
		{"abbreviated major", "codex_cli_rs/0", "0", Unknown, WarningUnknown},
		{"invalid leading zero", "codex_cli_rs/0.154.00", "0.154.00", Unknown, WarningUnknown},
		{"unrecognized shape", "codex-app-server 0.154.0", "", Unknown, WarningUnknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got, err := Classify(test.userAgent)
			if err != nil {
				t.Fatal(err)
			}
			if got.Version != test.version || got.Class != test.class || got.Warning != test.warning {
				t.Fatalf("Classify(%q) = %#v", test.userAgent, got)
			}
		})
	}
}

func TestClassifyRequiresUserAgent(t *testing.T) {
	t.Parallel()
	_, err := Classify("  ")
	if !errors.Is(err, ErrMissingUserAgent) {
		t.Fatalf("expected ErrMissingUserAgent, got %v", err)
	}
}

func TestRequireSupported(t *testing.T) {
	t.Parallel()
	unsupported, err := Classify("codex_cli_rs/0.142.0")
	if err != nil {
		t.Fatal(err)
	}
	if err := RequireSupported(unsupported); err == nil {
		t.Fatal("expected compatibility floor error")
	}

	unknown, err := Classify("codex_cli_rs/dev")
	if err != nil {
		t.Fatal(err)
	}
	if err := RequireSupported(unknown); err != nil {
		t.Fatalf("unknown version should proceed best-effort: %v", err)
	}
}
