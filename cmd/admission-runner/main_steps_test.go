package main

// The rules run carries in named steps, held at the step level; the input
// and loader rules are held through run and loadEnvironment beside this file.

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func completeRunnerFlags() runnerFlags {
	return runnerFlags{root: "/in", images: "/images.json", assets: "/assets", output: "/out/report.json", command: "/bin/manager",
		identityName: "probe", host: "127.0.0.1", metricsAddress: ":8080", healthAddress: ":8081",
		timeout: time.Minute, identityTTL: 2 * time.Minute, headerTimeout: time.Second, readinessInterval: time.Second}
}

func TestRunnerFlagsIncompleteRefusesEachMissingInput(t *testing.T) {
	if completeRunnerFlags().incomplete("ome", lockImage, lockRevision) {
		t.Fatal("a complete set of inputs was refused")
	}
	edits := map[string]func(f *runnerFlags){
		"root": func(f *runnerFlags) { f.root = "" }, "assets": func(f *runnerFlags) { f.assets = "" },
		"output": func(f *runnerFlags) { f.output = "" }, "command": func(f *runnerFlags) { f.command = "" },
		"identity name": func(f *runnerFlags) { f.identityName = "" }, "metrics address": func(f *runnerFlags) { f.metricsAddress = "" },
		"health address": func(f *runnerFlags) { f.healthAddress = "" }, "readiness interval": func(f *runnerFlags) { f.readinessInterval = 0 },
		"header timeout": func(f *runnerFlags) { f.headerTimeout = 0 }, "timeout": func(f *runnerFlags) { f.timeout = 0 },
		"identity lifetime equal to the timeout": func(f *runnerFlags) { f.identityTTL = f.timeout },
		"identity lifetime below the timeout":    func(f *runnerFlags) { f.identityTTL = f.timeout / 2 },
		"IPv6 loopback host":                     func(f *runnerFlags) { f.host = "::1" },
		"non-loopback host":                      func(f *runnerFlags) { f.host = "10.0.0.1" },
		"host name":                              func(f *runnerFlags) { f.host = "localhost" },
		"empty host":                             func(f *runnerFlags) { f.host = "" },
	}
	for name, edit := range edits {
		f := completeRunnerFlags()
		edit(&f)
		if !f.incomplete("ome", lockImage, lockRevision) {
			t.Errorf("%s: accepted", name)
		}
	}
	f := completeRunnerFlags()
	f.host = "127.0.0.2"
	if f.incomplete("ome", lockImage, lockRevision) {
		t.Error("a second IPv4 loopback address was refused")
	}
	for name, in := range map[string][3]string{
		"empty namespace":         {"", lockImage, lockRevision},
		"image without a digest":  {"ome", "repo/ome:v1", lockRevision},
		"image with a short hash": {"ome", "repo/ome@sha256:abc", lockRevision},
		"short revision":          {"ome", lockImage, "abc"},
		"revision not hex":        {"ome", lockImage, "g123456789"},
		"upper-case revision":     {"ome", lockImage, strings.ToUpper(lockRevision)},
	} {
		if !completeRunnerFlags().incomplete(in[0], in[1], in[2]) {
			t.Errorf("%s: accepted", name)
		}
	}
	if completeRunnerFlags().incomplete("ome", lockImage, lockRevision[:8]) {
		t.Error("an eight-character revision was refused")
	}
}

func TestRevisionAgreementNamesWhatTiesTheManagerToTheRevision(t *testing.T) {
	rev := lockRevision
	cases := []struct{ name, runner, embedded, want, wantErr string }{
		{"development runner without a logged commit", "", "", "development runner without embedded revision", ""},
		{"runner commit is a prefix of the revision", rev[:12], "", "matching embedded runner revision and immutable image digest", ""},
		{"runner commit equals the revision", rev, "", "matching embedded runner revision and immutable image digest", ""},
		{"runner commit differs", "ffffffffffff", "", "", "runner revision ffffffffffff does not match " + rev},
		{"logged commit is a prefix of the revision", "", rev[:10], "released revision tag, immutable digest, and matching embedded commit", ""},
		{"logged commit and runner commit both agree", rev[:12], rev[:10], "released revision tag, immutable digest, and matching embedded commit", ""},
		{"logged commit differs", "", "ffffffffff", "", `manager reports OME commit "ffffffffff"; expected ` + rev},
		{"logged commit extends the revision with non-hex characters", "", rev + "zz", "", `manager reports OME commit "` + rev + `zz"; expected ` + rev},
		{"logged commit shorter than eight characters", "", rev[:7], "", `manager reports OME commit "` + rev[:7] + `"; expected ` + rev},
		{"runner mismatch is reported before the log", "ffffffffffff", rev, "", "runner revision ffffffffffff does not match " + rev},
	}
	for _, c := range cases {
		got, err := revisionAgreement(c.runner, c.embedded, rev)
		if c.wantErr != "" {
			if err == nil || err.Error() != c.wantErr {
				t.Errorf("%s: err=%v want %q", c.name, err, c.wantErr)
			}
			continue
		}
		if err != nil || got != c.want {
			t.Errorf("%s: got %q err=%v want %q", c.name, got, err, c.want)
		}
	}
}

func TestEmbeddedRevisionReadsTheFirstInitializingLine(t *testing.T) {
	log := "not json\n{\"msg\":\"other\",\"gitCommit\":\"aaaaaaaa\"}\n{\"msg\":\"Initializing\",\"gitCommit\":\"bbbbbbbb\"}\n{\"msg\":\"Initializing\",\"gitCommit\":\"cccccccc\"}\n"
	if got := embeddedRevision([]byte(log)); got != "bbbbbbbb" {
		t.Fatalf("embedded revision %q, want the first Initializing line's commit", got)
	}
	if got := embeddedRevision([]byte("{\"msg\":\"Initializing\"}\n")); got != "" {
		t.Fatalf("an Initializing line without a commit read %q", got)
	}
	if got := embeddedRevision(nil); got != "" {
		t.Fatalf("an empty log read %q", got)
	}
}

func TestManagerArgumentsAndEnvironment(t *testing.T) {
	f := completeRunnerFlags()
	want := []string{"--webhook", "--webhook-only", "--webhook-cert-dir=/certs", "--webhook-port=8443", "--metrics-bind-address=:8080", "--health-probe-addr=:8081", "--zap-encoder=json"}
	if got := managerArguments(f, "", "/certs", 8443); !reflect.DeepEqual(got, want) {
		t.Fatalf("member arguments %v, want %v", got, want)
	}
	wantCP := append(append([]string{}, want...), "--multicluster-role=control-plane", "--allow-exec-credentials=false")
	if got := managerArguments(f, "control-plane", "/certs", 8443); !reflect.DeepEqual(got, wantCP) {
		t.Fatalf("control-plane arguments %v, want %v", got, wantCP)
	}
	t.Setenv("PATH", "/probe/bin")
	t.Setenv("TZ", "UTC")
	t.Setenv("EDITOR", "/probe/bin/ed")
	env := managerEnvironment("/k", "/t", "ome")
	if len(env) < 3 || env[0] != "KUBECONFIG=/k" || env[1] != "TMPDIR=/t" || env[2] != "POD_NAMESPACE=ome" {
		t.Fatalf("environment %v does not start with the kubeconfig, temp dir and namespace", env)
	}
	allowed := map[string]bool{"PATH": true, "SSL_CERT_FILE": true, "SSL_CERT_DIR": true, "LD_LIBRARY_PATH": true, "LANG": true, "TZ": true}
	seen := map[string]string{}
	for _, kv := range env[3:] {
		key, value, _ := strings.Cut(kv, "=")
		if !allowed[key] {
			t.Fatalf("environment carries %s, which the manager does not need", key)
		}
		seen[key] = value
	}
	if seen["PATH"] != "/probe/bin" || seen["TZ"] != "UTC" {
		t.Fatalf("environment %v does not carry the process PATH and TZ", env)
	}
}

func TestStartManagerReportsAManagerThatExited(t *testing.T) {
	dir := t.TempDir()
	f := completeRunnerFlags()
	f.command, f.output = "/bin/false", filepath.Join(dir, "report.json")
	port, err := freeLoopbackPort("127.0.0.1")
	if err != nil {
		t.Fatal(err)
	}
	exited, stop, err := startManager(context.Background(), f, "", dir, port, filepath.Join(dir, "kubeconfig"), dir, "ome")
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	if _, err := os.Stat(filepath.Join(dir, "manager.log")); err != nil {
		t.Fatalf("manager.log was not created beside the report: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	err = awaitManagerReady(ctx, "127.0.0.1", port, nil, 20*time.Millisecond, exited)
	if err == nil || !strings.HasPrefix(err.Error(), "prebuilt manager exited: ") || !strings.HasSuffix(err.Error(), "; see manager.log") {
		t.Fatalf("a manager that exited was not reported: %v", err)
	}
}

func TestWriteReportAndFailedProbe(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "report.json")
	r := report{OMERevision: lockRevision, ManagerImage: lockImage, Results: []result{
		{Case: "c1", File: "InferenceService/ns/a", Allowed: true, Passed: true},
		{Case: "c2", File: "ServingRuntime/ns/b", Passed: false, Error: "denied"},
		{Case: "c3", File: "BaseModel/ns/c", Passed: false, Error: "also denied"},
	}}
	if err := writeReport(path, r.OMERevision, r); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(data), "}\n") {
		t.Fatalf("report does not end with a newline: %q", data[len(data)-4:])
	}
	var back report
	if err := json.Unmarshal(data, &back); err != nil || back.OMERevision != lockRevision || len(back.Results) != 3 || back.Results[1].Error != "denied" {
		t.Fatalf("report read back %+v err=%v", back, err)
	}
	if err := failedProbe(r.Results); err == nil || err.Error() != "unexpected admission result c2/ServingRuntime/ns/b: denied" {
		t.Fatalf("the first failed probe was not reported: %v", err)
	}
	if err := failedProbe(r.Results[:1]); err != nil {
		t.Fatalf("a run of passed probes reported %v", err)
	}
	if err := failedProbe(nil); err != nil {
		t.Fatalf("a run without probes reported %v", err)
	}
	if err := writeReport(filepath.Join(dir, "missing", "report.json"), lockRevision, r); err == nil {
		t.Fatal("a report path in a missing directory was accepted")
	}
}

func TestClusterImageReadsTheClusterEntry(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "images.json")
	if err := os.WriteFile(path, []byte(`{"a":"img-a","b":""}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for cluster, want := range map[string]string{"a": "img-a", "b": "", "c": ""} {
		if got, err := clusterImage(path, cluster); err != nil || got != want {
			t.Errorf("cluster %s: image %q err=%v, want %q", cluster, got, err, want)
		}
	}
	if _, err := clusterImage(filepath.Join(dir, "absent.json"), "a"); err == nil {
		t.Error("an absent images file was accepted")
	}
	if err := os.WriteFile(path, []byte("{bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := clusterImage(path, "a"); err == nil {
		t.Error("a malformed images file was accepted")
	}
}
