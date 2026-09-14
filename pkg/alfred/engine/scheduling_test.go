package engine

import (
	"context"
	"encoding/json"
	"testing"

	promtestutil "github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	"sigs.k8s.io/ome/pkg/alfred/testutil"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const schedulingProfilesYAML = `schemaVersion: 1
scheduling:
  profiles:
    default-scheduler:
      backend: kube-worker
      schedulerVersion: v1.31.2
      configurationID: sha256:default
    custom-gang:
      backend: gang-worker
      schedulerVersion: v0.30.1
      configurationID: sha256:gang
      gangScheduling: true
`

type schedulingRecommendation struct {
	Outcome        string `json:"outcome"`
	AdvisoryReason string `json:"advisoryReason"`
	Scheduling     *struct {
		SchedulerName    string `json:"schedulerName"`
		Backend          string `json:"backend"`
		SchedulerVersion string `json:"schedulerVersion"`
		ConfigurationID  string `json:"configurationID"`
		Status           string `json:"status"`
		Reason           string `json:"reason"`
	} `json:"scheduling"`
}

func readSchedulingRecommendation(t *testing.T, reporter *Reporter) schedulingRecommendation {
	t.Helper()
	var cm corev1.ConfigMap
	if err := reporter.Client.Get(context.Background(), types.NamespacedName{
		Namespace: "ome", Name: "alfred-recommendations",
	}, &cm); err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Recommendations []schedulingRecommendation `json:"recommendations"`
	}
	if err := json.Unmarshal([]byte(cm.Data[recommendationsKey]), &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Recommendations) != 1 {
		t.Fatalf("want one recommendation: %s", cm.Data[recommendationsKey])
	}
	return doc.Recommendations[0]
}

func schedulingScenario() (*snapshot.ClusterSnapshot, policy.Candidate) {
	snap := testutil.NewSnapshot().WithNode("node1", "h100", 8).
		WithNode("node2", "h100", 8).
		WithInstance("prod/a", v1beta1.EngineComponent, constants.OMENative, "node1", 2).Build()
	c := cand("prod/a", "node1")
	c.Mode = constants.OMENative
	component := snap.Workloads[c.Workload].Components[c.Component]
	component.IR.Status.ObservedGeneration = component.IR.Generation
	component.IR.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	return snap, c
}

// The real loop must withhold a GPU-heuristic candidate before arbitration
// and publish the reason in its actual recommendation ConfigMap.
func TestRunOnceOMENativeRequiresScheduling(t *testing.T) {
	snap, c := schedulingScenario()
	loop, reporter, _ := newTestLoop(t, snap, &stubPolicy{out: []policy.Candidate{c}})
	if _, err := loop.Store.Update([]byte(schedulingProfilesYAML)); err != nil {
		t.Fatal(err)
	}
	loop.RunOnce(context.Background())
	got := readSchedulingRecommendation(t, reporter)
	if got.Outcome != OutcomeAdvisory || got.AdvisoryReason != "SimulationUnavailable" {
		t.Fatalf("OMENative without simulation must be advisory: %+v", got)
	}
	if got.Scheduling == nil || got.Scheduling.Reason != "SimulationUnavailable" || got.Scheduling.Backend != "kube-worker" {
		t.Fatalf("missing selected-profile diagnostics: %+v", got.Scheduling)
	}
	if got := promtestutil.ToFloat64(reporter.Metrics.RecommendationsAccepted.WithLabelValues(c.Policy, c.Workload.String(), string(c.Component))); got != 0 {
		t.Fatalf("unverified candidate entered arbitration: accepted = %v", got)
	}
}

func TestRunOnceSchedulingDiagnostics(t *testing.T) {
	tests := []struct {
		name                                                             string
		mutate                                                           func(*snapshot.Component, *policy.Candidate)
		wantScheduler, wantBackend, wantStatus, wantReason, wantAdvisory string
	}{
		{name: "default", wantScheduler: "default-scheduler", wantBackend: "kube-worker", wantStatus: "Unavailable", wantReason: "SimulationUnavailable"},
		{name: "custom", mutate: func(comp *snapshot.Component, _ *policy.Candidate) {
			comp.IR.Spec.Runners[0].Template.Spec.SchedulerName = "custom-gang"
		}, wantScheduler: "custom-gang", wantBackend: "gang-worker", wantStatus: "Unavailable", wantReason: "SimulationUnavailable"},
		{name: "unknown", mutate: func(comp *snapshot.Component, _ *policy.Candidate) {
			comp.IR.Spec.Runners[0].Template.Spec.SchedulerName = "unknown-scheduler"
		}, wantScheduler: "unknown-scheduler", wantStatus: "Unavailable", wantReason: "ProfileNotConfigured"},
		{name: "mixed", mutate: func(comp *snapshot.Component, _ *policy.Candidate) {
			comp.IR.Spec.Runners = append(comp.IR.Spec.Runners, v1beta1.Runner{Size: 1, Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{SchedulerName: "custom-gang"}}})
		}, wantScheduler: "default-scheduler", wantStatus: "Unavailable", wantReason: "MixedSchedulers"},
		{name: "missing IR", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.IR = nil }, wantStatus: "Unavailable", wantReason: "OMENativeObservationInvalid"},
		{name: "stale IR", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.StatusFresh = false }, wantStatus: "Unavailable", wantReason: "OMENativeObservationInvalid"},
		{name: "stale generation", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.IR.Generation++ }, wantStatus: "Unavailable", wantReason: "OMENativeObservationInvalid"},
		{name: "invalid component observation", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.ObservationValid = false }, wantStatus: "Unavailable", wantReason: "OMENativeObservationInvalid"},
		{name: "invalid instance observation", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.Instances[0].ObservationValid = false }, wantStatus: "Unavailable", wantReason: "OMENativeObservationInvalid"},
		{name: "missing instance", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.Instances = nil }, wantStatus: "Unavailable", wantReason: "OMENativeObservationInvalid"},
		{name: "executable component-wide", mutate: func(_ *snapshot.Component, c *policy.Candidate) {
			c.Instance = policy.ComponentWideInstance
		}, wantStatus: "Unavailable", wantReason: "OMENativeObservationInvalid"},
		{name: "missing templates", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.IR.Spec.Runners = nil }, wantStatus: "Unavailable", wantReason: "NoTemplates"},
		{name: "invalid runner size", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.IR.Spec.Runners[0].Size = 0 }, wantStatus: "Unavailable", wantReason: "OMENativeObservationInvalid"},
		{name: "runner size needs gang", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.IR.Spec.Runners[0].Size = 2 }, wantScheduler: "default-scheduler", wantBackend: "kube-worker", wantStatus: "Unsupported", wantReason: "GangUnsupported"},
		{name: "runner sum needs gang", mutate: func(comp *snapshot.Component, _ *policy.Candidate) {
			comp.IR.Spec.Runners = append(comp.IR.Spec.Runners, v1beta1.Runner{Size: 1})
		}, wantScheduler: "default-scheduler", wantBackend: "kube-worker", wantStatus: "Unsupported", wantReason: "GangUnsupported"},
		{name: "runner sum cannot overflow int32", mutate: func(comp *snapshot.Component, _ *policy.Candidate) {
			comp.IR.Spec.Runners = []v1beta1.Runner{{Size: 2147483647}, {Size: 2147483647}, {Size: 2}}
		}, wantScheduler: "default-scheduler", wantBackend: "kube-worker", wantStatus: "Unsupported", wantReason: "GangUnsupported"},
		{name: "observed count needs gang", mutate: func(comp *snapshot.Component, _ *policy.Candidate) { comp.Instances[0].ObservedPods = 2 }, wantScheduler: "default-scheduler", wantBackend: "kube-worker", wantStatus: "Unsupported", wantReason: "GangUnsupported"},
		{name: "member list needs gang", mutate: func(comp *snapshot.Component, _ *policy.Candidate) {
			comp.Instances[0].Pods = append(comp.Instances[0].Pods, snapshot.PodInfo{Name: "second"})
		}, wantScheduler: "default-scheduler", wantBackend: "kube-worker", wantStatus: "Unsupported", wantReason: "GangUnsupported"},
		{name: "gang profile still needs worker", mutate: func(comp *snapshot.Component, _ *policy.Candidate) {
			comp.IR.Spec.Runners[0].Size = 2
			comp.IR.Spec.Runners[0].Template.Spec.SchedulerName = "custom-gang"
		}, wantScheduler: "custom-gang", wantBackend: "gang-worker", wantStatus: "Unavailable", wantReason: "SimulationUnavailable"},
		{name: "existing advisory preserved", mutate: func(_ *snapshot.Component, c *policy.Candidate) {
			c.Executable = false
			c.AdvisoryReason = policy.AdvisoryOMENativeUnavailable
		}, wantScheduler: "default-scheduler", wantBackend: "kube-worker", wantStatus: "Unavailable", wantReason: "SimulationUnavailable", wantAdvisory: policy.AdvisoryOMENativeUnavailable},
		{name: "existing advisory preserves profile failure", mutate: func(comp *snapshot.Component, c *policy.Candidate) {
			c.Executable = false
			c.AdvisoryReason = policy.AdvisoryOMENativeUnavailable
			comp.IR.Spec.Runners[0].Template.Spec.SchedulerName = "unknown-scheduler"
		}, wantScheduler: "unknown-scheduler", wantStatus: "Unavailable", wantReason: "ProfileNotConfigured", wantAdvisory: policy.AdvisoryOMENativeUnavailable},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snap, c := schedulingScenario()
			comp := snap.Workloads[c.Workload].Components[c.Component]
			if tc.mutate != nil {
				tc.mutate(comp, &c)
			}
			before, err := json.Marshal(comp)
			if err != nil {
				t.Fatal(err)
			}
			p := &stubPolicy{out: []policy.Candidate{c}}
			policyBefore, err := json.Marshal(p.out)
			if err != nil {
				t.Fatal(err)
			}
			loop, reporter, _ := newTestLoop(t, snap, p)
			if _, err := loop.Store.Update([]byte(schedulingProfilesYAML)); err != nil {
				t.Fatal(err)
			}
			loop.RunOnce(context.Background())
			got := readSchedulingRecommendation(t, reporter)
			wantAdvisory := tc.wantAdvisory
			if wantAdvisory == "" {
				wantAdvisory = tc.wantReason
			}
			if got.Outcome != OutcomeAdvisory || got.AdvisoryReason != wantAdvisory {
				t.Fatalf("advisory = %+v, want %q", got, wantAdvisory)
			}
			d := got.Scheduling
			if d == nil || d.SchedulerName != tc.wantScheduler || d.Backend != tc.wantBackend || d.Status != tc.wantStatus || d.Reason != tc.wantReason {
				t.Fatalf("diagnostics = %+v, want scheduler %q backend %q status %q reason %q", d, tc.wantScheduler, tc.wantBackend, tc.wantStatus, tc.wantReason)
			}
			if d.Backend == "kube-worker" && (d.SchedulerVersion != "v1.31.2" || d.ConfigurationID != "sha256:default") {
				t.Fatalf("profile identity missing: %+v", d)
			}
			policyAfter, err := json.Marshal(p.out)
			if err != nil {
				t.Fatal(err)
			}
			if string(policyBefore) != string(policyAfter) {
				t.Fatal("gate mutated policy-owned candidates")
			}
			after, err := json.Marshal(comp)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) {
				t.Fatal("gate mutated snapshot or runner templates")
			}
		})
	}
}

func TestRunOnceSchedulingReloadAndScope(t *testing.T) {
	snap, c := schedulingScenario()
	p := &stubPolicy{out: []policy.Candidate{c}}
	loop, reporter, _ := newTestLoop(t, snap, p)
	loop.RunOnce(context.Background())
	if got := readSchedulingRecommendation(t, reporter); got.Outcome != OutcomeAdvisory || got.AdvisoryReason != "ProfileNotConfigured" {
		t.Fatalf("empty default profile configuration must remain advisory: %+v", got)
	}
	if _, err := loop.Store.Update([]byte(schedulingProfilesYAML)); err != nil {
		t.Fatal(err)
	}
	loop.RunOnce(context.Background())
	if got := readSchedulingRecommendation(t, reporter); got.Scheduling == nil || got.Scheduling.Backend != "kube-worker" {
		t.Fatalf("first profile not selected: %+v", got.Scheduling)
	}
	if _, err := loop.Store.Update([]byte(`schemaVersion: 1
scheduling:
  profiles:
    default-scheduler:
      backend: replacement-worker
      schedulerVersion: v1.32.1
      configurationID: sha256:next
`)); err != nil {
		t.Fatal(err)
	}
	loop.RunOnce(context.Background())
	got := readSchedulingRecommendation(t, reporter)
	if got.Scheduling == nil || got.Scheduling.Backend != "replacement-worker" || got.Scheduling.ConfigurationID != "sha256:next" || got.AdvisoryReason != "SimulationUnavailable" {
		t.Fatalf("reload must select new profile without authorizing execution: %+v", got)
	}

	c.Instance = policy.ComponentWideInstance
	c.Executable = false
	c.AdvisoryReason = policy.AdvisoryVolumePinned
	p.out = []policy.Candidate{c}
	loop.RunOnce(context.Background())
	got = readSchedulingRecommendation(t, reporter)
	if got.Outcome != OutcomeAdvisory || got.Scheduling != nil || got.AdvisoryReason != policy.AdvisoryVolumePinned {
		t.Fatalf("component-wide advice changed: %+v", got)
	}

	c.Mode = constants.RawDeployment
	c.Instance = 0
	c.AdvisoryReason = policy.AdvisoryRawDeploymentMigrationUnsupported
	p.out = []policy.Candidate{c}
	loop.RunOnce(context.Background())
	got = readSchedulingRecommendation(t, reporter)
	if got.Scheduling != nil || got.AdvisoryReason != policy.AdvisoryRawDeploymentMigrationUnsupported {
		t.Fatalf("other deployment advice changed: %+v", got)
	}
}
