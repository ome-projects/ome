package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
)

var helperBinary string

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "alfred-barrier-helper-")
	if err != nil {
		panic(err)
	}
	source := filepath.Join(dir, "main.go")
	if err := os.WriteFile(source, []byte(helperSource), 0o600); err != nil {
		panic(err)
	}
	helperBinary = filepath.Join(dir, "helper")
	cmd := exec.Command("go", "build", "-o", helperBinary, source)
	cmd.Stdout, cmd.Stderr = os.Stderr, os.Stderr
	if err := cmd.Run(); err != nil {
		panic(err)
	}
	code := m.Run()
	if err := os.RemoveAll(dir); err != nil && code == 0 {
		_, _ = fmt.Fprintf(os.Stderr, "remove helper directory: %v\n", err)
		code = 1
	}
	os.Exit(code)
}

const helperSource = `package main

import (
  "crypto/sha256"
  "encoding/hex"
  "encoding/json"
  "flag"
  "io"
  "os"
  "strings"
)

type config struct { Mode, ExpectedRequestSHA string; OversizeBytes int }
type request struct {
  SchemaVersion string
  RequestID string
  Profile json.RawMessage
  ReplacementPods []pod
  SnapshotID string
  SnapshotTime json.RawMessage
}
type pod struct { Metadata metadata }
type metadata struct { Namespace, Name, UID string }

func main() {
  fs := flag.NewFlagSet("helper", flag.ContinueOnError)
  fs.SetOutput(io.Discard)
  var backend, path, timeout string
  var printProfile bool
  fs.StringVar(&backend, "backend", "", "")
  fs.StringVar(&path, "scheduler-config", "", "")
  fs.StringVar(&timeout, "timeout", "", "")
  fs.BoolVar(&printProfile, "print-profile", false, "")
  if fs.Parse(os.Args[1:]) != nil || fs.NArg() != 0 || backend == "" || path == "" || timeout == "" || len(os.Environ()) != 0 { os.Exit(2) }
  rawConfig, err := os.ReadFile(path); if err != nil { os.Exit(3) }
  var cfg config; if json.Unmarshal(rawConfig, &cfg) != nil { os.Exit(4) }
  if printProfile { _, _ = io.WriteString(os.Stdout, " \n{\"profile\":true}\n"); return }
  input, err := io.ReadAll(os.Stdin); if err != nil { os.Exit(5) }
  sum := sha256.Sum256(input)
  if cfg.ExpectedRequestSHA != "" && cfg.ExpectedRequestSHA != hex.EncodeToString(sum[:]) { os.Exit(6) }
  switch cfg.Mode {
  case "failure": os.Exit(7)
  case "malformed": _, _ = io.WriteString(os.Stdout, "{not-json\n"); return
  case "oversize": _, _ = io.WriteString(os.Stdout, strings.Repeat("x", cfg.OversizeBytes)); return
  case "stderr-oversize": _, _ = io.WriteString(os.Stderr, strings.Repeat("x", cfg.OversizeBytes))
  }
  var request request
  if json.Unmarshal(input, &request) != nil { os.Exit(8) }
  result := map[string]any{
    "schemaVersion": request.SchemaVersion, "requestID": request.RequestID,
    "snapshotID": request.SnapshotID, "snapshotTime": request.SnapshotTime,
    "profile": request.Profile, "decision": "Feasible", "reason": "PlacementFound",
  }
  if cfg.Mode == "mismatch" { result["requestID"] = "different-request" }
  if cfg.Mode == "infeasible" {
    result["decision"] = "Infeasible"
    result["reason"] = "NoFeasiblePlacement"
  } else {
    placements := make([]map[string]any, 0, len(request.ReplacementPods))
    for _, pod := range request.ReplacementPods {
      identity := map[string]any{"namespace": pod.Metadata.Namespace, "name": pod.Metadata.Name}
      if pod.Metadata.UID != "" { identity["uid"] = pod.Metadata.UID }
      placements = append(placements, map[string]any{"pod": identity, "nodeName": "target-node"})
    }
    result["placements"] = placements
  }
  encoded, err := json.Marshal(result); if err != nil { os.Exit(9) }
  _, _ = os.Stdout.Write(append(append([]byte(" \n"), encoded...), []byte("\n\n")...))
}
`

type helperConfig struct {
	Mode               string `json:"Mode,omitempty"`
	ExpectedRequestSHA string `json:"ExpectedRequestSHA,omitempty"`
	OversizeBytes      int    `json:"OversizeBytes,omitempty"`
}

func profile() scheduling.ProfileIdentity {
	return scheduling.ProfileIdentity{
		SchedulerName: "alfred-default-scheduler", Backend: "test-backend",
		SchedulerVersion: "v1.35.4", ConfigurationID: "sha256:test-config",
	}
}

func validRequest(workload string, gang bool) scheduling.Request {
	count := 1
	if gang {
		count = 2
	}
	request := scheduling.Request{
		SchemaVersion:     scheduling.SimulationSchemaV1,
		RequestID:         "preflight",
		Profile:           profile(),
		SnapshotID:        "snapshot-1",
		SnapshotTime:      metav1.NewTime(time.Unix(1_700_000_000, 123).UTC()),
		RequireGang:       gang,
		ExcludedNodes:     []string{"source-node"},
		MigrationFromNode: "source-node",
		ClusterObjects: []runtime.RawExtension{
			{Object: &corev1.Node{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}, ObjectMeta: metav1.ObjectMeta{Name: "source-node"}}},
			{Object: &corev1.Node{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Node"}, ObjectMeta: metav1.ObjectMeta{Name: "target-node"}}},
		},
	}
	for i := 0; i < count; i++ {
		source := corev1.Pod{
			TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "alfred-e2e", Name: fmt.Sprintf("%s-source-%d", workload, i),
				UID: types.UID(fmt.Sprintf("uid-%d", i)), Labels: map[string]string{"ome.io/inferenceservice": workload},
			},
			Spec: corev1.PodSpec{NodeName: "source-node", SchedulerName: profile().SchedulerName},
		}
		replacement := corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Namespace: "alfred-e2e", Name: fmt.Sprintf("%s-replacement-%d", workload, i)},
			Spec: corev1.PodSpec{
				SchedulerName: profile().SchedulerName,
				Affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
					RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{
						MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"source-node"}}},
					}}},
				}},
			},
		}
		request.SourcePods = append(request.SourcePods, source)
		request.ReplacementPods = append(request.ReplacementPods, replacement)
		request.ClusterObjects = append(request.ClusterObjects, runtime.RawExtension{Object: source.DeepCopy()})
	}
	return request
}

func matchingResult(request scheduling.Request, feasible bool) scheduling.Result {
	result := scheduling.Result{
		SchemaVersion: request.SchemaVersion, RequestID: request.RequestID,
		SnapshotID: request.SnapshotID, SnapshotTime: request.SnapshotTime,
		Profile: request.Profile, Decision: scheduling.DecisionInfeasible,
		Reason: scheduling.SimulationReasonNoFeasiblePlacement,
	}
	if feasible {
		result.Decision = scheduling.DecisionFeasible
		result.Reason = scheduling.SimulationReasonPlacementFound
		for _, pod := range request.ReplacementPods {
			result.Placements = append(result.Placements, scheduling.Placement{
				Pod: scheduling.PodIdentity{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}, NodeName: "target-node",
			})
		}
	}
	return result
}

func exactRequestBytes(t *testing.T, request scheduling.Request) []byte {
	t.Helper()
	raw, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]byte(" \n"), raw...), '\n', '\n')
}

func exactResultBytes(t *testing.T, request scheduling.Request, feasible bool) []byte {
	t.Helper()
	result := map[string]any{
		"schemaVersion": request.SchemaVersion, "requestID": request.RequestID,
		"snapshotID": request.SnapshotID, "snapshotTime": request.SnapshotTime,
		"profile": request.Profile,
	}
	if feasible {
		result["decision"], result["reason"] = "Feasible", "PlacementFound"
		placements := make([]map[string]any, 0, len(request.ReplacementPods))
		for _, pod := range request.ReplacementPods {
			identity := map[string]any{"namespace": pod.Namespace, "name": pod.Name}
			if pod.UID != "" {
				identity["uid"] = pod.UID
			}
			placements = append(placements, map[string]any{"pod": identity, "nodeName": "target-node"})
		}
		result["placements"] = placements
	} else {
		result["decision"], result["reason"] = "Infeasible", "NoFeasiblePlacement"
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return append(append([]byte(" \n"), raw...), '\n', '\n')
}

func writeJSONFile(t *testing.T, name string, value any) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), name)
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func sha(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func testOptions(t *testing.T, requestBytes []byte, workload string, gang bool, child helperConfig) (options, []string) {
	t.Helper()
	if child.ExpectedRequestSHA == "" && requestBytes != nil {
		child.ExpectedRequestSHA = sha(requestBytes)
	}
	childConfig := writeJSONFile(t, "child.json", child)
	barrierConfig := writeJSONFile(t, "barrier.json", barrierConfig{
		Profile: profile(), Namespace: "alfred-e2e", Workload: workload,
	})
	opts := productionOptions()
	opts.childPath = helperBinary
	opts.configPath = barrierConfig
	opts.holdTimeout = time.Second
	return opts, []string{"--backend", "test-backend", "--scheduler-config", childConfig, "--timeout", "2s"}
}

func testListener(t *testing.T) (net.Listener, func(string, string) (net.Listener, error)) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener, func(network, address string) (net.Listener, error) {
		if network != "tcp" || address != defaultListenAddress {
			t.Fatalf("Listen(%q, %q), want tcp/%s", network, address, defaultListenAddress)
		}
		return listener, nil
	}
}

func TestPrintProfileForwardsExactBytesWithoutBarrierConfig(t *testing.T) {
	childConfig := writeJSONFile(t, "child.json", helperConfig{})
	opts := productionOptions()
	opts.childPath = helperBinary
	opts.configPath = filepath.Join(t.TempDir(), "must-not-be-read.json")
	var stdout, stderr bytes.Buffer
	err := run(context.Background(), []string{"--backend", "test-backend", "--scheduler-config", childConfig, "--timeout", "2s", "--print-profile"}, strings.NewReader("ignored"), &stdout, &stderr, opts)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := stdout.String(), " \n{\"profile\":true}\n"; got != want {
		t.Fatalf("stdout = %q, want exact %q", got, want)
	}
}

func controlOptions(baseURL string) options {
	opts := productionOptions()
	opts.controlBaseURL = baseURL
	opts.controlWaitTimeout = time.Second
	opts.controlRequestTimeout = 100 * time.Millisecond
	opts.controlRetryDelay = 10 * time.Millisecond
	opts.heldResponseLimit = 1024
	opts.receiptResponseLimit = 1024
	return opts
}

func TestWaitHeldRetriesRefusalAndForwardsExactBytes(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	if err := probe.Close(); err != nil {
		t.Fatal(err)
	}
	want := []byte(" \n{\"requestBytes\":\"AQI=\"}\n")
	server := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/held" {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(want)
	})}
	serverDone := make(chan error, 1)
	go func() {
		time.Sleep(40 * time.Millisecond)
		listener, err := net.Listen("tcp", address)
		if err != nil {
			serverDone <- err
			return
		}
		serverDone <- server.Serve(listener)
	}()

	opts := controlOptions("http://" + address)
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--wait-held"}, strings.NewReader(""), &stdout, &stderr, opts); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stdout.Bytes(), want) {
		t.Fatalf("stdout = %q, want exact %q", stdout.Bytes(), want)
	}
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-serverDone; err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Fatal(err)
	}
}

func TestWaitHeldHonorsParentCancellation(t *testing.T) {
	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := probe.Addr().String()
	_ = probe.Close()
	opts := controlOptions("http://" + address)
	opts.controlWaitTimeout = time.Minute
	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(30*time.Millisecond, cancel)
	var stdout, stderr bytes.Buffer
	started := time.Now()
	if err := run(ctx, []string{"--wait-held"}, strings.NewReader(""), &stdout, &stderr, opts); !errors.Is(err, context.Canceled) {
		t.Fatalf("run() error = %v, want context.Canceled", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("parent cancellation took %s", elapsed)
	}
	if stdout.Len() != 0 {
		t.Fatalf("canceled wait exposed stdout: %q", stdout.String())
	}
}

func TestWaitHeldRejectsOversizeAndNonOKResponses(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		limit  int64
	}{
		{name: "oversize", status: http.StatusOK, body: strings.Repeat("x", 129), limit: 128},
		{name: "non-ok", status: http.StatusServiceUnavailable, body: "not held", limit: 128},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = io.WriteString(w, tc.body)
			}))
			defer server.Close()
			opts := controlOptions(server.URL)
			opts.heldResponseLimit = tc.limit
			var stdout, stderr bytes.Buffer
			if err := run(context.Background(), []string{"--wait-held"}, strings.NewReader(""), &stdout, &stderr, opts); err == nil {
				t.Fatal("run() = nil, want failure")
			}
			if stdout.Len() != 0 {
				t.Fatalf("failed wait exposed stdout: %q", stdout.String())
			}
		})
	}
}

func TestReleaseHeldPostsOnceAndForwardsExactReceipt(t *testing.T) {
	input := []byte(" \n{\"nonce\":\"one-use-token\"}\n")
	want := []byte("\n{\"releasedAt\":\"2026-09-28T00:00:00Z\"}\n")
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		attempts++
		if r.Method != http.MethodPost || r.URL.Path != "/release" {
			t.Errorf("request = %s %s, want POST /release", r.Method, r.URL.Path)
		}
		got, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
		}
		if !bytes.Equal(got, input) {
			t.Errorf("release body = %q, want exact %q", got, input)
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(want)
	}))
	defer server.Close()
	opts := controlOptions(server.URL)
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--release-held"}, bytes.NewReader(input), &stdout, &stderr, opts); err != nil {
		t.Fatal(err)
	}
	if attempts != 1 {
		t.Fatalf("release attempts = %d, want 1", attempts)
	}
	if !bytes.Equal(stdout.Bytes(), want) {
		t.Fatalf("stdout = %q, want exact %q", stdout.Bytes(), want)
	}
}

func TestReleaseHeldNeverRetriesOrOutputsFailure(t *testing.T) {
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		http.Error(w, "uncertain", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	opts := controlOptions(server.URL)
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--release-held"}, strings.NewReader(`{"nonce":"one-use-token"}`), &stdout, &stderr, opts); err == nil {
		t.Fatal("run() = nil, want failure")
	}
	if attempts != 1 {
		t.Fatalf("release attempts = %d, want exactly 1", attempts)
	}
	if stdout.Len() != 0 {
		t.Fatalf("failed release exposed stdout: %q", stdout.String())
	}
}

func TestReleaseHeldRejectsOversizeInputBeforeRequest(t *testing.T) {
	var attempts int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts++
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	opts := controlOptions(server.URL)
	opts.releaseBodyLimit = 32
	var stdout, stderr bytes.Buffer
	if err := run(context.Background(), []string{"--release-held"}, strings.NewReader(strings.Repeat("x", 33)), &stdout, &stderr, opts); err == nil {
		t.Fatal("run() = nil, want oversized-input failure")
	}
	if attempts != 0 || stdout.Len() != 0 {
		t.Fatalf("oversize input made %d attempts and wrote %q", attempts, stdout.String())
	}
}

func TestMatchingRequestWaitsForReleaseAndPreservesChildBytes(t *testing.T) {
	request := validRequest("single", false)
	input := exactRequestBytes(t, request)
	opts, args := testOptions(t, input, "single", false, helperConfig{})
	listener, listen := testListener(t)
	opts.listen = listen

	var stdout, stderr bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- run(context.Background(), args, bytes.NewReader(input), &stdout, &stderr, opts) }()
	held := waitForHeld(t, listener.Addr().String())
	if stdout.Len() != 0 {
		t.Fatalf("stdout was exposed before release: %q", stdout.String())
	}
	if !bytes.Equal(held.RequestBytes, input) {
		t.Fatal("GET /held did not preserve exact request bytes")
	}
	wantResult := exactResultBytes(t, request, true)
	if !bytes.Equal(held.ResultBytes, wantResult) {
		t.Fatalf("GET /held result bytes differ\n got: %q\nwant: %q", held.ResultBytes, wantResult)
	}
	if held.RequestSHA256 != sha(input) || held.ResultSHA256 != sha(wantResult) {
		t.Fatal("GET /held hashes do not bind exact bytes")
	}
	if held.Nonce == "" || held.HeldAt == "" || held.Deadline == "" {
		t.Fatalf("GET /held omitted barrier evidence: %+v", held)
	}
	receipt := release(t, listener.Addr().String(), held.Nonce, http.StatusOK)
	if receipt.RequestSHA256 != sha(input) || receipt.ResultSHA256 != sha(wantResult) || receipt.ReleasedAt == "" {
		t.Fatalf("release receipt = %+v", receipt)
	}
	releasedAt, err := time.Parse(time.RFC3339Nano, receipt.ReleasedAt)
	if err != nil {
		t.Fatal(err)
	}
	deadline, err := time.Parse(time.RFC3339Nano, held.Deadline)
	if err != nil {
		t.Fatal(err)
	}
	if !releasedAt.Before(deadline) {
		t.Fatalf("release time %s is not before hard deadline %s", releasedAt, deadline)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stdout.Bytes(), wantResult) {
		t.Fatalf("stdout changed child bytes\n got: %q\nwant: %q", stdout.Bytes(), wantResult)
	}
}

func TestValidNonmatchesPassThroughWithoutListening(t *testing.T) {
	tests := []struct {
		name      string
		edit      func(*scheduling.Request)
		childMode string
		feasible  bool
	}{
		{name: "recommendation", edit: func(r *scheduling.Request) { r.MigrationFromNode = "" }, feasible: true},
		{name: "wrong workload", edit: func(r *scheduling.Request) {
			r.SourcePods[0].Labels["ome.io/inferenceservice"] = "other"
			r.ClusterObjects[2].Object.(*corev1.Pod).Labels["ome.io/inferenceservice"] = "other"
		}, feasible: true},
		{name: "wrong profile", edit: func(r *scheduling.Request) { r.Profile.ConfigurationID = "sha256:other" }, feasible: true},
		{name: "wrong request id", edit: func(r *scheduling.Request) { r.RequestID = "retry-1" }, feasible: true},
		{name: "negative decision", edit: func(*scheduling.Request) {}, childMode: "infeasible", feasible: false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			request := validRequest("single", false)
			tc.edit(&request)
			input := exactRequestBytes(t, request)
			opts, args := testOptions(t, input, "single", false, helperConfig{Mode: tc.childMode})
			opts.listen = func(string, string) (net.Listener, error) { t.Fatal("nonmatch tried to listen"); return nil, nil }
			var stdout, stderr bytes.Buffer
			if err := run(context.Background(), args, bytes.NewReader(input), &stdout, &stderr, opts); err != nil {
				t.Fatal(err)
			}
			if want := exactResultBytes(t, request, tc.feasible); !bytes.Equal(stdout.Bytes(), want) {
				t.Fatalf("stdout changed child bytes\n got: %q\nwant: %q", stdout.Bytes(), want)
			}
		})
	}
}

func TestMatchRequiresExactSingleOrGangShape(t *testing.T) {
	tests := []struct {
		name    string
		request scheduling.Request
		config  barrierConfig
		want    bool
	}{
		{name: "single", request: validRequest("single", false), config: barrierConfig{Profile: profile(), Namespace: "alfred-e2e", Workload: "single"}, want: true},
		{name: "gang", request: validRequest("gang", true), config: barrierConfig{Profile: profile(), Namespace: "alfred-e2e", Workload: "gang"}, want: true},
		{name: "single count drift", request: validRequest("gang", true), config: barrierConfig{Profile: profile(), Namespace: "alfred-e2e", Workload: "single"}},
		{name: "gang flag drift", request: validRequest("gang", false), config: barrierConfig{Profile: profile(), Namespace: "alfred-e2e", Workload: "gang"}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if got := matchesBarrier(tc.request, matchingResult(tc.request, true), tc.config); got != tc.want {
				t.Fatalf("matchesBarrier() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestFailuresNeverAuthorizeStdout(t *testing.T) {
	request := validRequest("single", false)
	input := exactRequestBytes(t, request)
	tests := []struct {
		name   string
		child  helperConfig
		mutate func(*options, *[]byte)
	}{
		{name: "child failure", child: helperConfig{Mode: "failure"}},
		{name: "malformed result", child: helperConfig{Mode: "malformed"}},
		{name: "mismatched result", child: helperConfig{Mode: "mismatch"}},
		{name: "oversize result", child: helperConfig{Mode: "oversize", OversizeBytes: 1024}, mutate: func(o *options, _ *[]byte) { o.resultLimit = 128 }},
		{name: "oversize stderr", child: helperConfig{Mode: "stderr-oversize", OversizeBytes: 1024}, mutate: func(o *options, _ *[]byte) { o.stderrLimit = 128 }},
		{name: "oversize input", mutate: func(o *options, b *[]byte) { o.requestLimit = 128; *b = bytes.Repeat([]byte("x"), 129) }},
		{name: "bind failure", mutate: func(o *options, _ *[]byte) {
			o.listen = func(string, string) (net.Listener, error) { return nil, errors.New("bind failed") }
		}},
		{name: "hold timeout", mutate: func(o *options, _ *[]byte) {
			listener, listen := testListener(t)
			_ = listener
			o.listen = listen
			o.holdTimeout = 40 * time.Millisecond
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			caseInput := append([]byte(nil), input...)
			opts, args := testOptions(t, caseInput, "single", false, tc.child)
			if tc.mutate != nil {
				tc.mutate(&opts, &caseInput)
			}
			var stdout, stderr bytes.Buffer
			if err := run(context.Background(), args, bytes.NewReader(caseInput), &stdout, &stderr, opts); err == nil {
				t.Fatal("run() = nil, want failure")
			}
			if stdout.Len() != 0 {
				t.Fatalf("failure exposed stdout: %q", stdout.String())
			}
		})
	}
}

func TestBadNonceAndOversizeReleaseFailClosed(t *testing.T) {
	for _, tc := range []struct{ name, body string }{
		{name: "bad nonce", body: `{"nonce":"wrong"}`},
		{name: "oversize body", body: strings.Repeat("x", 1025)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := validRequest("single", false)
			input := exactRequestBytes(t, request)
			opts, args := testOptions(t, input, "single", false, helperConfig{})
			opts.releaseBodyLimit = 1024
			listener, listen := testListener(t)
			opts.listen = listen
			var stdout, stderr bytes.Buffer
			done := make(chan error, 1)
			go func() { done <- run(context.Background(), args, bytes.NewReader(input), &stdout, &stderr, opts) }()
			held := waitForHeld(t, listener.Addr().String())
			if held.Nonce == "" {
				t.Fatal("empty nonce")
			}
			response, err := http.Post("http://"+listener.Addr().String()+"/release", "application/json", strings.NewReader(tc.body))
			if err != nil {
				t.Fatal(err)
			}
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				t.Fatal("invalid release returned 200")
			}
			if err := <-done; err == nil {
				t.Fatal("invalid release did not fail invocation")
			}
			if stdout.Len() != 0 {
				t.Fatalf("invalid release exposed stdout: %q", stdout.String())
			}
		})
	}
}

func TestReleaseNonceCanBeUsedOnlyOnce(t *testing.T) {
	evidence := heldDocument{RequestSHA256: strings.Repeat("a", 64), ResultSHA256: strings.Repeat("b", 64)}
	released := make(chan releaseReceipt, 1)
	failed := make(chan error, 1)
	handler := newBarrierHandler(evidence, "one-use-token", time.Now().Add(time.Hour), released, failed, 1024, time.Now)

	var wg sync.WaitGroup
	statuses := make(chan int, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			recorder := httptest.NewRecorder()
			request := httptest.NewRequest(http.MethodPost, "/release", strings.NewReader(`{"nonce":"one-use-token"}`))
			handler.ServeHTTP(recorder, request)
			statuses <- recorder.Code
		}()
	}
	wg.Wait()
	close(statuses)
	ok := 0
	for status := range statuses {
		if status == http.StatusOK {
			ok++
		}
	}
	if ok != 1 {
		t.Fatalf("successful releases = %d, want exactly 1", ok)
	}
	select {
	case <-released:
	default:
		t.Fatal("successful receipt was not delivered")
	}
}

func TestExpiredNonceFailsClosed(t *testing.T) {
	deadline := time.Unix(1_700_000_000, 0).UTC()
	evidence := heldDocument{
		RequestSHA256: strings.Repeat("a", 64), ResultSHA256: strings.Repeat("b", 64),
		Deadline: deadline.Format(time.RFC3339Nano),
	}
	released := make(chan releaseReceipt, 1)
	failed := make(chan error, 1)
	handler := newBarrierHandler(evidence, "expired-token", deadline, released, failed, 1024, func() time.Time { return deadline })
	recorder := httptest.NewRecorder()
	request := httptest.NewRequest(http.MethodPost, "/release", strings.NewReader(`{"nonce":"expired-token"}`))
	handler.ServeHTTP(recorder, request)
	if recorder.Code == http.StatusOK {
		t.Fatal("expired nonce authorized release")
	}
	select {
	case <-released:
		t.Fatal("expired nonce delivered a release receipt")
	default:
	}
	select {
	case <-failed:
	default:
		t.Fatal("expired nonce did not fail closed")
	}
}

func waitForHeld(t *testing.T, address string) heldDocument {
	t.Helper()
	client := &http.Client{Timeout: 200 * time.Millisecond}
	for deadline := time.Now().Add(750 * time.Millisecond); time.Now().Before(deadline); time.Sleep(5 * time.Millisecond) {
		response, err := client.Get("http://" + address + "/held")
		if err != nil {
			continue
		}
		var held heldDocument
		err = json.NewDecoder(response.Body).Decode(&held)
		_ = response.Body.Close()
		if err == nil && response.StatusCode == http.StatusOK {
			return held
		}
	}
	t.Fatal("barrier did not expose /held")
	return heldDocument{}
}

func release(t *testing.T, address, nonce string, wantStatus int) releaseReceipt {
	t.Helper()
	body, _ := json.Marshal(releaseRequest{Nonce: nonce})
	response, err := http.Post("http://"+address+"/release", "application/json", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != wantStatus {
		raw, _ := io.ReadAll(response.Body)
		t.Fatalf("release status = %d, want %d: %s", response.StatusCode, wantStatus, raw)
	}
	var receipt releaseReceipt
	if err := json.NewDecoder(response.Body).Decode(&receipt); err != nil {
		t.Fatal(err)
	}
	return receipt
}
