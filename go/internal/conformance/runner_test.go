package conformance

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestRunFromRepositoryAndEmitDeterministicEvidence(t *testing.T) {
	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	summary, err := Run(root)
	if err != nil {
		t.Fatal(err)
	}
	if len(summary.Fixtures) != 17 || len(summary.Scenarios) != 102 {
		t.Fatalf("unexpected coverage: fixtures=%d scenarios=%d", len(summary.Fixtures), len(summary.Scenarios))
	}
	var first, second bytes.Buffer
	if err := summary.WriteEvidence(&first); err != nil {
		t.Fatal(err)
	}
	if err := summary.WriteEvidence(&second); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(first.Bytes(), second.Bytes()) {
		t.Fatal("evidence output is not deterministic")
	}
	if bytes.Contains(first.Bytes(), []byte("coverage=live")) {
		t.Fatal("declarative fixtures were presented as live coverage")
	}
}

func TestRootIgnoresWorkingDirectory(t *testing.T) {
	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	old, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	temp := t.TempDir()
	if err := os.Chdir(temp); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chdir(old) })
	got, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	if got != root {
		t.Fatalf("root changed with cwd: got %q want %q", got, root)
	}
}

func TestTamperedRenderedFixtureFailsSemanticValidation(t *testing.T) {
	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, fixturesDir, "envelope-agent-request.txt"))
	if err != nil {
		t.Fatal(err)
	}
	tampered := bytes.Replace(data, []byte("hello from mektup"), []byte("hello from tamper"), 1)
	if bytes.Equal(data, tampered) {
		t.Fatal("test did not tamper fixture")
	}
	if err := validateRenderedEnvelope(tampered); err == nil {
		t.Fatal("tampered body unexpectedly passed digest validation")
	}
}

func TestTamperedScenarioExpectationFailsSemanticValidation(t *testing.T) {
	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	scenarios, err := readJSON[scenariosDocument](filepath.Join(root, scenariosPath))
	if err != nil {
		t.Fatal(err)
	}
	transitions, err := readJSON[transitionsDocument](filepath.Join(root, transitionsPath))
	if err != nil {
		t.Fatal(err)
	}
	profile := scenarios.Profiles["rejected"]
	profile.Terminal.Exit = 0
	scenarios.Profiles["rejected"] = profile
	if err := validateScenarioDocuments(scenarios, transitions); err == nil {
		t.Fatal("tampered terminal exit unexpectedly passed")
	}
}

func TestMissingExpectedInvalidFixtureIsManifestFailure(t *testing.T) {
	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	m, err := readJSON[manifest](filepath.Join(root, manifestPath))
	if err != nil {
		t.Fatal(err)
	}
	const missing = "missing-negative.invalid.json"
	for i := range m.Fixtures {
		if m.Fixtures[i].Expect == "invalid" {
			m.Fixtures[i].Path = missing
			break
		}
	}
	temp := t.TempDir()
	fixtureRoot := filepath.Join(temp, fixturesDir)
	if err := os.MkdirAll(fixtureRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(filepath.Join(root, "conformance"), filepath.Join(temp, "conformance")); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range m.Fixtures {
		if fixture.Path == missing {
			continue
		}
		source := filepath.Join(root, fixturesDir, fixture.Path)
		destination := filepath.Join(temp, fixturesDir, fixture.Path)
		if err := os.Symlink(source, destination); err != nil {
			t.Fatal(err)
		}
	}
	manifestData, err := json.Marshal(m)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(temp, manifestPath), manifestData, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Run(temp); err == nil {
		t.Fatal("missing expected-invalid fixture unexpectedly passed")
	}
}

func TestManifestRejectsUnknownExpectedAndSchema(t *testing.T) {
	if err := validateManifestFixture(manifestFixture{Kind: "event", Schema: "mektup/event/v1", Path: "x.json", Expect: "typo"}); err == nil {
		t.Fatal("unknown expected value unexpectedly accepted")
	}
	if err := validateManifestFixture(manifestFixture{Kind: "event", Schema: "mektup/warning/v1", Path: "x.json"}); err == nil {
		t.Fatal("schema drift unexpectedly accepted")
	}
}

func TestManifestAndScenarioDecodeUnknownAdditiveFields(t *testing.T) {
	root, err := Root()
	if err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(filepath.Join(root, scenariosPath))
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err := json.Unmarshal(data, &value); err != nil {
		t.Fatal(err)
	}
	value["future"] = map[string]any{"version": 2}
	augmented, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var decoded scenariosDocument
	if err := json.Unmarshal(augmented, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.Schema != "mektup/conformance/v1/scenarios" || len(decoded.Scenarios) != 102 {
		t.Fatalf("additive scenario field changed stable content: %#v", decoded.Schema)
	}
}
