package doctor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestDefaultDoctorIsReadOnly(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	config := filepath.Join(root, "config")
	report, err := Run(context.Background(), Options{Paths: Paths{StateDir: state, ConfigDir: config, ConfigFile: filepath.Join(config, "config.json")}}, false)
	if err != nil {
		t.Fatal(err)
	}
	if !report.ReadOnly || report.Fix || len(report.Plan) != 2 || len(report.Repairs) != 0 {
		t.Fatalf("unexpected read-only report %#v", report)
	}
	if _, err := os.Stat(state); !os.IsNotExist(err) {
		t.Fatalf("doctor created state directory: %v", err)
	}
	if _, err := os.Stat(config); !os.IsNotExist(err) {
		t.Fatalf("doctor created config directory: %v", err)
	}
}

func TestDoctorFixReceiptsEachOwnerPrivateRepair(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	config := filepath.Join(root, "config")
	if err := os.MkdirAll(state, 0755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(config, 0755); err != nil {
		t.Fatal(err)
	}
	configFile := filepath.Join(config, "config.json")
	if err := os.WriteFile(configFile, []byte(`{"version":1}`), 0644); err != nil {
		t.Fatal(err)
	}
	report, err := Run(context.Background(), Options{Paths: Paths{StateDir: state, ConfigDir: config, ConfigFile: configFile}}, true)
	if err != nil {
		t.Fatal(err)
	}
	if report.ReadOnly || !report.Fix || len(report.Repairs) != 3 {
		t.Fatalf("unexpected fix report %#v", report)
	}
	for _, repair := range report.Repairs {
		if !repair.Applied || repair.Error != "" {
			t.Fatalf("unapplied repair %#v", repair)
		}
	}
	for path, want := range map[string]os.FileMode{state: 0700, config: 0700, configFile: 0600} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm() != want {
			t.Fatalf("%s mode %04o, want %04o", path, info.Mode().Perm(), want)
		}
	}
}

func TestDoctorInjectableProbeDoesNotRunFixUnlessNamed(t *testing.T) {
	called := 0
	probe := ProbeFunc(func(context.Context) ([]Finding, error) {
		return []Finding{{ID: "test.repair", Category: "test", Severity: SeverityWarning, Message: "safe test repair", Fixable: true, SafeFix: true, Fix: func(context.Context) (string, error) { called++; return "done", nil }}}, nil
	})
	readOnly, err := Run(context.Background(), Options{Probes: []Probe{probe}}, false)
	if err != nil || called != 0 || len(readOnly.Plan) != 1 || len(readOnly.Repairs) != 0 {
		t.Fatalf("read-only injected probe: %#v %v called=%d", readOnly, err, called)
	}
	fixed, err := Run(context.Background(), Options{Probes: []Probe{probe}}, true)
	if err != nil || called != 1 || len(fixed.Repairs) != 1 || !fixed.Repairs[0].Applied {
		t.Fatalf("fix injected probe: %#v %v called=%d", fixed, err, called)
	}
}

func TestDoctorRejectsRelativePathsBeforeProbingOrFixing(t *testing.T) {
	root := t.TempDir()
	t.Chdir(root)
	report, err := Run(context.Background(), Options{Paths: Paths{StateDir: "relative-state", ConfigDir: "relative-config", ConfigFile: "relative-config/config.json"}}, true)
	if err == nil {
		t.Fatal("relative paths unexpectedly accepted")
	}
	if len(report.Plan) != 0 || len(report.Repairs) != 0 {
		t.Fatalf("invalid path produced a repair plan/receipt: %#v", report)
	}
	for _, path := range []string{"relative-state", "relative-config"} {
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Fatalf("invalid path caused mutation at %s: %v", path, statErr)
		}
	}
}

func TestDoctorPermissionRepairRefusesSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "target")
	link := filepath.Join(root, "link")
	if err := os.WriteFile(target, []byte("private"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if _, err := chmodFix(link, 0600)(context.Background()); err == nil {
		t.Fatal("symlink permission repair unexpectedly succeeded")
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0644 {
		t.Fatalf("symlink repair changed target mode to %04o", info.Mode().Perm())
	}
}

func TestDoctorForeignOwnerIsNeverHealthyOrSafeFixable(t *testing.T) {
	root := t.TempDir()
	state := filepath.Join(root, "state")
	if err := os.Mkdir(state, 0700); err != nil {
		t.Fatal(err)
	}
	report, err := Run(context.Background(), Options{
		Paths: Paths{StateDir: state},
		Owner: func(os.FileInfo) bool { return false },
	}, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, finding := range report.Findings {
		if finding.ID == "state.directory" && (finding.Severity == SeverityOK || finding.SafeFix || finding.Fixable) {
			t.Fatalf("foreign-owned state was healthy/fixable: %#v", finding)
		}
	}
	if len(report.Repairs) != 0 {
		t.Fatalf("foreign-owned state produced repair receipts: %#v", report.Repairs)
	}
}

func TestDoctorMissingDirectoryRepairRejectsSubstitutedFinalSymlink(t *testing.T) {
	root := t.TempDir()
	target := filepath.Join(root, "unrelated")
	if err := os.Mkdir(target, 0o755); err != nil {
		t.Fatal(err)
	}
	requested := filepath.Join(root, "state")
	findings, err := probeDir(context.Background(), "state.directory", "state", requested)
	if err != nil || len(findings) != 1 || findings[0].Fix == nil {
		t.Fatalf("missing repair findings=%#v err=%v", findings, err)
	}
	if err := os.Symlink(target, requested); err != nil {
		t.Fatal(err)
	}
	if _, err := findings[0].Fix(context.Background()); err == nil {
		t.Fatal("repair followed a substituted final symlink")
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o755 {
		t.Fatalf("repair changed unrelated target mode to %04o", info.Mode().Perm())
	}
}
