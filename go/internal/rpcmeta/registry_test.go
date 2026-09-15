package rpcmeta

import (
	"bytes"
	"encoding/json"
	"errors"
	"sort"
	"testing"
)

func TestRegistryCoversPinnedClientInventory(t *testing.T) {
	all := Registry()
	if got, want := len(all), 162; got != want {
		t.Fatalf("registry method count = %d, want %d", got, want)
	}
	stable, experimental := 0, 0
	for _, metadata := range all {
		if metadata.Stability == StabilityStable {
			stable++
		} else if metadata.Stability == StabilityExperimental {
			experimental++
		} else {
			t.Errorf("%s has unknown stability", metadata.Method)
		}
		if metadata.ResponseNote == "" || metadata.TerminalNote == "" || len(metadata.ServerCaveats) == 0 {
			t.Errorf("%s is missing response/terminal/caveat metadata", metadata.Method)
		}
		if len(metadata.Effects) == 0 || metadata.RetrySafety == "" {
			t.Errorf("%s is missing effect/retry metadata", metadata.Method)
		}
	}
	if stable != 102 || experimental != 60 {
		t.Fatalf("stability counts = stable %d experimental-only %d, want 102/60", stable, experimental)
	}
	for _, method := range stableMethods {
		metadata, ok := Lookup(method)
		if !ok || metadata.Stability != StabilityStable {
			t.Errorf("stable inventory method %q is absent or not stable", method)
		}
	}
	for _, method := range experimentalOnlyMethods {
		metadata, ok := Lookup(method)
		if !ok || metadata.Stability != StabilityExperimental || !metadata.Experimental {
			t.Errorf("experimental inventory method %q is absent or not experimental", method)
		}
	}
}

func TestRegistryJSONIsDeterministic(t *testing.T) {
	first, err := RegistryJSON()
	if err != nil {
		t.Fatal(err)
	}
	second, err := RegistryJSON()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first, second) {
		t.Fatal("registry JSON changed between calls")
	}
	var decoded []Metadata
	if err := json.Unmarshal(first, &decoded); err != nil {
		t.Fatal(err)
	}
	methods := make([]string, len(decoded))
	for i, metadata := range decoded {
		methods[i] = metadata.Method
	}
	if !sort.StringsAreSorted(methods) {
		t.Fatal("registry JSON methods are not lexical")
	}
}

func TestRepresentativeEffectClasses(t *testing.T) {
	tests := []struct {
		method string
		want   []EffectClass
	}{
		{"thread/read", []EffectClass{EffectRead}},
		{"thread/timeline/list", []EffectClass{EffectRead}},
		{"model/list", []EffectClass{EffectNetworkRead}},
		{"account/read", []EffectClass{EffectNetworkRead}},
		{"turn/start", []EffectClass{EffectThreadWrite}},
		{"thread/shellCommand", []EffectClass{EffectHostWrite}},
		{"account/login/start", []EffectClass{EffectAuth}},
		{"fs/remove", []EffectClass{EffectHostWrite, EffectDestructive}},
		{"mock/experimentalMethod", []EffectClass{EffectUnknown}},
		{"mcpServer/tool/call", []EffectClass{EffectNetworkRead, EffectUnknown}},
	}
	for _, test := range tests {
		decision := Evaluate(test.method, nil)
		if len(decision.Effects) != len(test.want) {
			t.Fatalf("%s effects = %v, want %v", test.method, decision.Effects, test.want)
		}
		for i := range test.want {
			if decision.Effects[i] != test.want[i] {
				t.Errorf("%s effects = %v, want %v", test.method, decision.Effects, test.want)
				break
			}
		}
	}
}

func TestKnownMutationsNeverDefaultToPlainRead(t *testing.T) {
	for _, method := range []string{
		"experimentalFeature/enablement/set", "plugin/install", "plugin/share/checkout", "externalAgentConfig/detect",
		"thread/backgroundTerminals/terminate", "remoteControl/enable", "mcpServer/oauth/login",
	} {
		decision := Evaluate(method, nil)
		if len(decision.Effects) == 1 && decision.Effects[0] == EffectRead {
			t.Errorf("known mutation %q was classified as plain read", method)
		}
	}
}

func TestLocalSearchesArePlainReads(t *testing.T) {
	for _, method := range []string{"thread/search", "thread/searchOccurrences", "fuzzyFileSearch"} {
		decision := Evaluate(method, nil)
		if len(decision.Effects) != 1 || decision.Effects[0] != EffectRead {
			t.Errorf("%s effects = %v, want [read]", method, decision.Effects)
		}
		if err := Gate(GateRequest{Method: method}); err != nil {
			t.Errorf("%s read gate failed: %v", method, err)
		}
	}
}

func TestNetworkReadsDispatchWithoutGrantButRetainReceiptEffect(t *testing.T) {
	for _, method := range []string{"model/list", "account/read", "remoteControl/client/list"} {
		decision := Evaluate(method, nil)
		if !containsEffect(decision.Effects, EffectNetworkRead) {
			t.Errorf("%s decision lost network-read effect: %v", method, decision.Effects)
		}
		if err := Gate(GateRequest{Method: method}); err != nil {
			t.Errorf("%s network read unexpectedly gated: %v", method, err)
		}
	}
}

func TestAuthReadsAreNotAuthMutations(t *testing.T) {
	for _, method := range []string{
		"account/read", "account/rateLimits/read", "account/usage/read", "account/workspaceMessages/read",
		"getAuthStatus", "remoteControl/status/read", "remoteControl/client/list", "userVerification/status",
	} {
		decision := Evaluate(method, nil)
		if containsEffect(decision.Effects, EffectAuth) {
			t.Errorf("observational auth method %s incorrectly requires auth effect: %v", method, decision.Effects)
		}
		if err := Gate(GateRequest{Method: method}); err != nil {
			t.Errorf("observational auth method %s unexpectedly gated: %v", method, err)
		}
	}
	for _, method := range []string{"account/login/start", "account/logout", "account/bedrock/setup", "userVerification/enroll", "userVerification/delete", "userVerification/verify"} {
		if !containsEffect(Evaluate(method, nil).Effects, EffectAuth) {
			t.Errorf("auth mutation %s lost auth effect", method)
		}
		if err := Gate(GateRequest{Method: method}); err == nil {
			t.Errorf("auth mutation %s passed without auth grant", method)
		}
	}
}

func TestMCPToolCallIsUnknownAndGated(t *testing.T) {
	decision := Evaluate("mcpServer/tool/call", json.RawMessage(`{"server":"arbitrary","tool":"write"}`))
	if !containsEffect(decision.Effects, EffectUnknown) || !containsEffect(decision.Effects, EffectNetworkRead) {
		t.Fatalf("MCP tool call effects = %v, want unknown plus network-read", decision.Effects)
	}
	if err := Gate(GateRequest{Method: "mcpServer/tool/call", Grants: []EffectClass{EffectNetworkRead}}); err == nil {
		t.Fatal("MCP tool call passed with network-read acknowledgment only")
	}
	if err := Gate(GateRequest{Method: "mcpServer/tool/call", Grants: []EffectClass{EffectUnknown}}); err != nil {
		t.Fatal(err)
	}
}

func TestTurnInterruptRetrySafetyOverride(t *testing.T) {
	if got := Evaluate("turn/interrupt", nil).Metadata.RetrySafety; got != RetryAfterReconciliation {
		t.Fatalf("turn/interrupt retry safety = %q, want %q", got, RetryAfterReconciliation)
	}
}

func TestExperimentalMethodAndFieldTriggers(t *testing.T) {
	method := Evaluate("project/list", nil)
	if !method.Experimental || method.Metadata.Stability != StabilityExperimental {
		t.Fatalf("experimental method decision = %+v", method)
	}

	plain := Evaluate("thread/fork", json.RawMessage(`{"threadId":"thr"}`))
	if plain.Experimental || len(plain.ExperimentalFields) != 0 {
		t.Fatalf("plain fork unexpectedly experimental: %+v", plain)
	}
	for _, params := range []string{
		`{"threadId":"thr","beforeTurnId":"turn"}`,
		`{"threadId":"thr","before_turn_id":"turn"}`,
		`{"threadId":"thr","before-turn":"turn"}`,
	} {
		decision := Evaluate("thread/fork", json.RawMessage(params))
		if !decision.Experimental || len(decision.ExperimentalFields) != 1 || decision.ExperimentalFields[0] != "thread/fork.beforeTurnId" {
			t.Fatalf("before-turn params %s decision = %+v", params, decision)
		}
	}

	for _, params := range []string{
		`{"type":"amazonBedrock","apiKey":"x","region":"us-east-1"}`,
		`{"type":"amazonBedrockAccessKeys","accessKeyId":"x","secretAccessKey":"y","region":"us-east-1"}`,
	} {
		decision := Evaluate("account/login/start", json.RawMessage(params))
		if !decision.Experimental {
			t.Fatalf("experimental account variant %s not detected: %+v", params, decision)
		}
	}
}

func TestParameterRiskCanOnlyIncrease(t *testing.T) {
	plain := Evaluate("account/read", json.RawMessage(`{"includeUsage":false}`))
	refresh := Evaluate("account/read", json.RawMessage(`{"forceRefresh":true}`))
	if refresh.Experimental {
		t.Fatal("stable refresh parameter was incorrectly treated as experimental")
	}
	if containsEffect(plain.Effects, EffectAuth) || containsEffect(refresh.Effects, EffectAuth) {
		t.Fatalf("observational account read unexpectedly carries auth mutation risk: plain=%v refresh=%v", plain.Effects, refresh.Effects)
	}
	if !containsEffect(refresh.Effects, EffectNetworkRead) {
		t.Fatalf("forceRefresh did not retain network-read risk: %v", refresh.Effects)
	}
	if len(refresh.Effects) < len(plain.Effects) {
		t.Fatalf("parameter risk lowered: plain=%v refresh=%v", plain.Effects, refresh.Effects)
	}
}

func TestGateRequiresEveryMutationClassBeforeDispatch(t *testing.T) {
	called := false
	dispatch := func() { called = true }
	if err := Gate(GateRequest{Method: "thread/read"}); err != nil {
		t.Fatal(err)
	}
	dispatch()
	if !called {
		t.Fatal("read callback was not reached")
	}

	called = false
	if err := Gate(GateRequest{Method: "fs/remove", Grants: []EffectClass{EffectHostWrite}}); err == nil {
		t.Fatal("destructive effect passed with only host-write grant")
	} else {
		gateErr := new(GateError)
		if !errors.As(err, &gateErr) || len(gateErr.Missing) != 1 || gateErr.Missing[0] != EffectDestructive {
			t.Fatalf("gate error = %T %v", err, err)
		}
	}
	if called {
		t.Fatal("dispatch callback would have run before missing grant")
	}
	if err := Gate(GateRequest{Method: "fs/remove", Grants: []EffectClass{EffectHostWrite, EffectDestructive}}); err != nil {
		t.Fatal(err)
	}

	if err := Gate(GateRequest{Method: "model/list"}); err != nil {
		t.Fatalf("network-read should dispatch without grant: %v", err)
	}
	if err := Gate(GateRequest{Method: "model/list", Grants: []EffectClass{EffectNetworkRead}}); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownMethodRequiresUnknownGrant(t *testing.T) {
	decision := Evaluate("future/method", nil)
	if decision.Metadata.Stability != StabilityUnknown || len(decision.Effects) != 1 || decision.Effects[0] != EffectUnknown {
		t.Fatalf("unknown decision = %+v", decision)
	}
	if err := Gate(GateRequest{Method: "future/method"}); err == nil {
		t.Fatal("unknown method passed without unknown grant")
	} else if gateErr, ok := err.(*GateError); !ok || !gateErr.IsUnknownMethod() {
		t.Fatalf("unknown gate error = %T %v", err, err)
	}
	if err := Gate(GateRequest{Method: "future/method", Grants: []EffectClass{EffectUnknown}}); err != nil {
		t.Fatal(err)
	}
}
