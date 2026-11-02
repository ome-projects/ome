package replay

import (
	"testing"
	"time"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/analysis"
)

// A staged answer is read once per consumption: it is fresh while it is
// newer than the gate's last read, bound to the step it was staged on, and
// absent for a member the scenario never named.
func TestSampleScriptAnswersOncePerFreshRead(t *testing.T) {
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	script := newSampleScript()
	script.samples["engine"] = scriptedSample{outcome: analysis.Fail, step: 1, at: start.Add(time.Minute)}
	req := canary.SampleRequest{
		Key:      canary.SampleKey{Component: "engine", Step: 1},
		Analysis: &v1beta1.RolloutAnalysis{Metrics: []v1beta1.AnalysisMetric{{Name: "err", Threshold: "0.05"}}},
	}

	res, at, ok := script.Get(req, time.Time{})
	if !ok || res.Outcome != analysis.Fail || !at.Equal(start.Add(time.Minute)) {
		t.Fatalf("first read = %v %v %v, want the staged breach at t+1m", res, at, ok)
	}
	if len(res.Metrics) != 1 || res.Metrics[0].Name != "err" || res.Metrics[0].Passed {
		t.Fatalf("metrics = %+v, want the step's one check, not passed", res.Metrics)
	}
	if _, _, ok := script.Get(req, at); ok {
		t.Fatal("a sample the gate consumed must not read as fresh")
	}
	other := req
	other.Key.Step = 2
	if _, _, ok := script.Get(other, time.Time{}); ok {
		t.Fatal("a sample staged for step 1 must not answer step 2")
	}
	other.Key = canary.SampleKey{Component: "decoder", Step: 1}
	if _, _, ok := script.Get(other, time.Time{}); ok {
		t.Fatal("a member the scenario never named has no fresh sample")
	}
}

// A sample needs a member, a known outcome and a unit with a record; the
// script is wired only when the timeline stages the event.
func TestAnalysisSampleIsStagedOnlyUnderARecord(t *testing.T) {
	const head = `scenario: sample
cells: ["run/example-cell"]
initial:
  service:
    components:
      engine: {replicas: 2, image: registry.example.com/runtime:v1}
    groups:
      - components: [engine]
        canary:
          steps:
            - {capacity: "50%", traffic: 10, analysis: {interval: 1m, failureLimit: 3, metrics: [{name: err, query: q, operator: LTE, threshold: "0.05"}]}}
            - {capacity: "100%", traffic: 100}
  config: {requeue: 10s}
timeline:
  - tick: 1
    events:
`
	s, err := Parse([]byte(head+"      - analysis.sample: {component: engine, outcome: pass}\n"), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(t.Context(), s, Options{}); err == nil || !contains([]string{err.Error()}, err.Error()) || !containsText(err.Error(), "carries no canary record") {
		t.Fatalf("a sample with no record to bind to must fail the run, got %v", err)
	}
	s, err = Parse([]byte(head+"      - analysis.sample: {component: engine, outcome: maybe}\n"), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Run(t.Context(), s, Options{}); err == nil || !containsText(err.Error(), "pass, fail or inconclusive") {
		t.Fatalf("an unknown outcome must fail the run, got %v", err)
	}
	plain, err := Parse([]byte(plainWalk), Vocabulary{})
	if err != nil {
		t.Fatal(err)
	}
	d, err := newDriver(t.Context(), plain, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if d.samples != nil {
		t.Fatal("a scenario that stages no sample must run with no sampler")
	}
}

func containsText(s, sub string) bool {
	return len(sub) > 0 && len(s) >= len(sub) && (s == sub || indexOf(s, sub) >= 0)
}

func indexOf(s, sub string) int {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return i
		}
	}
	return -1
}
