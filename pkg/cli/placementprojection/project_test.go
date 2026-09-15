package placementprojection

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	knapis "knative.dev/pkg/apis"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	c "sigs.k8s.io/ome/pkg/cli/placementcollection"
	"sigs.k8s.io/ome/pkg/cli/report"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

const statusFixture = `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"placement-demo","namespace":"cli-demo","uid":"fixture-isvc-uid","generation":7},"spec":{"placement":{"mode":"Split","requirements":"accelerator=cpu","clusterSelector":"provider=demo","split":{"replicas":10,"spread":true,"maxReplicasPerCluster":8}}},"status":{"url":"https://user:private-password@demo.invalid/private-path?private-token=x#private-fragment","placement":{"phase":"Placed","candidates":[{"cluster":"demo-a","phase":"Admitted","endpoint":"https://user:private-password@demo-a.invalid/private-path?private-token=x#private-fragment","admittedReplicas":5,"readyReplicas":4,"autoscaling":{"policies":[{"name":"demo-policy","portableDigest":"pv1:111111111111"}],"components":{"engine":{"resolvedDigest":"rv1:222222222222","ready":true}},"ready":true},"rollout":{"activeRunID":"private-run-uuid","activeGroups":[{"source":"Policy","policyName":"demo-rollout","portableDigest":"rp1:333333333333"},{"source":"Inline","portableDigest":"rp1:444444444444"}]}},{"cluster":"demo-b","phase":"Admitted","endpoint":"http://demo-b.invalid","admittedReplicas":2,"readyReplicas":0}]}}}`
const clusterFixture = `{"apiVersion":"ome.io/v1beta1","kind":"WorkloadCluster","metadata":{"name":"demo-a","generation":3,"labels":{"accelerator":"cpu","provider":"demo"}},"spec":{"clusterSource":{"clusterProfileRef":{"name":"private-profile"}}},"status":{"conditions":[{"type":"Ready","status":"True","reason":"ProbeFailedRetrying","observedGeneration":3,"lastTransitionTime":"2026-09-14T10:00:00Z","message":"private-condition-message"}]}}`
const mapFixture = `{"apiVersion":"ome.io/v1beta1","kind":"TrafficMap","metadata":{"name":"placement-demo","namespace":"cli-demo","uid":"fixture-map-uid","generation":4,"ownerReferences":[{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","name":"placement-demo","uid":"fixture-isvc-uid","controller":true}]},"spec":{"service":"placement-demo","mode":"Split","observedISVCGeneration":7,"entries":[{"cluster":"demo-a","endpoint":"https://demo-a.invalid","weight":1,"healthy":false,"capacity":{"allocated":5,"ready":0,"factor":"1","source":"ControlPlane"},"probe":{"result":"Passing","lastProbeTime":"2026-09-15T10:00:00Z","consecutiveFailures":0,"message":"https://private-user:private-token@private.invalid"}},{"cluster":"demo-b","endpoint":"http://demo-b.invalid","weight":1,"healthy":false}]},"status":{"conditions":[{"type":"Routable","status":"True","reason":"AllHomesUnready","observedGeneration":4,"lastTransitionTime":"2026-09-15T10:00:00Z","message":"private-map-message"}]}}`

var fixtureClock = v.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) })

func fixture(t *testing.T) c.Result {
	t.Helper()
	var parent ome.InferenceService
	var cluster ome.WorkloadCluster
	var tm ome.TrafficMap
	for _, x := range []struct {
		raw    string
		target any
	}{{statusFixture, &parent}, {clusterFixture, &cluster}, {mapFixture, &tm}} {
		if err := json.Unmarshal([]byte(x.raw), x.target); err != nil {
			t.Fatal(err)
		}
	}
	return c.Result{InferenceService: &parent, WorkloadClusters: []ome.WorkloadCluster{cluster}, Fleet: v.PlacementAcquisition{State: "Observed", Returned: 1, Admitted: 1, Pages: 1, Complete: true}, TrafficMap: &tm, TrafficMapAcquisition: v.PlacementAcquisition{State: "Observed", Returned: 1, Admitted: 1, Pages: 1, Complete: true}}
}

func TestSplitReportedNotFloorFulfillmentAndOriginPrivacy(t *testing.T) {
	snapshot := fixture(t)
	before := snapshot.InferenceService.DeepCopy()
	got, err := ProjectStatus(snapshot, fixtureClock)
	if err != nil {
		t.Fatal(err)
	}
	if got.Kind != "PlacementStatusReport" || got.Content.Placement.Phase != "Placed" || got.Content.Placement.Source.Freshness != "Unverifiable" {
		t.Fatalf("placement=%+v", got)
	}
	if got.Content.Inputs.Mode != "Split" || len(got.Content.Placement.Homes) != 2 {
		t.Fatalf("mode/homes=%+v", got.Content)
	}
	if *got.Content.Placement.Homes[0].AdmittedReplicas.Value != 5 || got.Content.Placement.Homes[1].ReadyReplicas.State != "Unknown" {
		t.Fatalf("counts=%+v", got.Content.Placement.Homes)
	}
	if got.Content.Placement.Homes[0].Address.EndpointOrigin != "https://demo-a.invalid" || !got.Content.Placement.Homes[0].Address.PathPresent {
		t.Fatalf("address=%+v", got.Content.Placement.Homes[0].Address)
	}
	if got.Content.Placement.Homes[0].Provenance.ActiveGroups[0].Ordinal != 0 || got.Content.Placement.Homes[0].Provenance.ActiveGroups[1].Source != "Inline" {
		t.Fatalf("atomic order=%+v", got.Content.Placement.Homes[0].Provenance)
	}
	if !reflect.DeepEqual(before, snapshot.InferenceService) {
		t.Fatal("source mutated")
	}
	for _, format := range []report.Format{report.FormatTable, report.FormatJSON, report.FormatYAML} {
		var out bytes.Buffer
		if err := report.Write(&out, format, got); err != nil {
			t.Fatal(err)
		}
		for _, private := range []string{"private-", "fixture-isvc-uid", "fixture-map-uid", "accelerator=cpu", "provider=demo", "fulfilled", "eligible"} {
			if strings.Contains(out.String(), private) {
				t.Fatalf("leak/unsupported verdict %q: %s", private, out.String())
			}
		}
		if format == report.FormatTable {
			for _, line := range strings.Split(out.String(), "\n") {
				if len(line) > 80 {
					t.Fatalf("overwide: %s", line)
				}
			}
		}
	}
}

func TestInputWholesalePrecedenceAndExactPresence(t *testing.T) {
	for _, tc := range []struct {
		name                                  string
		structured                            *ome.PlacementSpec
		requirements, selector                string
		wantSource, wantState, wantCompatible string
	}{
		{"empty-structured", &ome.PlacementSpec{}, "accelerator=cpu", "provider=demo", "Structured", "NoRequirements", "NotApplicable"},
		{"legacy-selector-only", nil, "", "provider=demo", "LegacyAnnotations", "Valid", "True"},
		{"structured-selector-only", &ome.PlacementSpec{ClusterSelector: "provider=demo"}, "accelerator=wrong", "provider=wrong", "Structured", "Valid", "True"},
		{"exact-empty", nil, "", "", "LegacyAnnotations", "NoRequirements", "NotApplicable"},
		{"whitespace-present", &ome.PlacementSpec{Requirements: " "}, "", "", "Structured", "Valid", "True"},
		{"malformed", &ome.PlacementSpec{Requirements: "provider in ("}, "", "", "Structured", "InvalidSelector", "Unknown"},
		{"budget", &ome.PlacementSpec{Requirements: strings.Repeat("a", 4097)}, "", "", "Structured", "BudgetExceeded", "Unknown"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t)
			s.InferenceService.Spec.Placement = tc.structured
			s.InferenceService.Annotations = map[string]string{"ome.io/accelerator-requirements": tc.requirements, "ome.io/cluster-selector": tc.selector}
			got, err := ProjectExplain(s, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			if string(got.Content.Status.Inputs.Source) != tc.wantSource || string(got.Content.Status.Inputs.RequirementsState) != tc.wantState || len(got.Content.Clusters) != 1 || string(got.Content.Clusters[0].ComputedSelectorCompatible) != tc.wantCompatible {
				t.Fatalf("inputs=%+v clusters=%+v", got.Content.Status.Inputs, got.Content.Clusters)
			}
		})
	}
}

func TestModesAndFailedSingleRetainedAddress(t *testing.T) {
	for _, mode := range []ome.PlacementMode{"", ome.PlacementModeSingle, ome.PlacementModeAll, "Future"} {
		s := fixture(t)
		s.InferenceService.Spec.Placement.Mode = mode
		s.InferenceService.Status.Placement.Phase = ome.PlacementPhaseFailed
		s.InferenceService.Status.Placement.Endpoint = &knapis.URL{Scheme: "https", Host: "demo.invalid"}
		got, err := ProjectStatus(s, fixtureClock)
		if err != nil {
			t.Fatal(err)
		}
		if got.Content.Placement.Phase != "Failed" || got.Content.Placement.Address.State != "Present" || got.Content.Placement.Homes[0].AdmittedReplicas.State != "NotApplicable" {
			t.Fatalf("failed/mode=%+v", got.Content)
		}
		if mode == "" && (got.Content.Inputs.Mode != "Single" || got.Content.Inputs.ModeEvidence != "Defaulted") {
			t.Fatalf("default=%+v", got.Content.Inputs)
		}
		if mode == "Future" && got.Content.Inputs.Mode != "Unknown" {
			t.Fatal("unknown mode inferred")
		}
	}
}

func TestSourceSpecificFreshnessAndNoAcknowledgement(t *testing.T) {
	s := fixture(t)
	s.TrafficMap.Spec.ObservedISVCGeneration = 6
	got, err := ProjectEndpoint(s, fixtureClock)
	if err != nil {
		t.Fatal(err)
	}
	if got.Content.Routing.Source.Freshness != "Stale" || got.Content.Routing.Routable.Source.Freshness != "Current" || got.Content.Status.Placement.Source.Freshness != "Unverifiable" || got.Content.Routing.Acknowledgement != "NoAcknowledgement" {
		t.Fatalf("independent evidence=%+v", got.Content)
	}
	if len(got.Content.Entries) != 2 || got.Content.Entries[0].Healthy || got.Content.Entries[0].Weight != 1 || got.Content.Entries[0].Probe.Result != "Passing" {
		t.Fatalf("probe not separate=%+v", got.Content.Entries)
	}
	for _, generation := range []int64{0, 6, 7, 8, -1} {
		s.TrafficMap.Spec.ObservedISVCGeneration = generation
		r, _ := ProjectEndpoint(s, fixtureClock)
		want := map[int64]v.PlacementValue{0: "Unverifiable", 6: "Stale", 7: "Current", 8: "Invalid", -1: "Invalid"}[generation]
		if r.Content.Routing.Source.Freshness != want {
			t.Fatalf("gen%d=%s want%s", generation, r.Content.Routing.Source.Freshness, want)
		}
	}
	for _, generation := range []int64{0, 2, 3, 4, -1} {
		s.WorkloadClusters[0].Status.Conditions[0].ObservedGeneration = generation
		r, _ := ProjectExplain(s, fixtureClock)
		want := map[int64]v.PlacementValue{0: "Unverifiable", 2: "Stale", 3: "Current", 4: "Invalid", -1: "Invalid"}[generation]
		if r.Content.Clusters[0].ReportedReady.Source.Freshness != want {
			t.Fatalf("WLCgen%d=%+v", generation, r.Content.Clusters[0])
		}
	}
}

func TestAcknowledgementFalseDefaultDoesNotContradictTrueCondition(t *testing.T) {
	for _, tc := range []struct {
		scalar     bool
		condition  metav1.ConditionStatus
		generation int64
		want       string
	}{
		{false, metav1.ConditionTrue, 4, "ReportedTrue"},
		{true, metav1.ConditionFalse, 4, "Invalid"},
		{true, metav1.ConditionTrue, 4, "ReportedTrue"},
		{false, metav1.ConditionFalse, 4, "ReportedFalse"},
		{true, metav1.ConditionTrue, 3, "Invalid"},
	} {
		s := fixture(t)
		s.TrafficMap.Status.Programmed = tc.scalar
		s.TrafficMap.Status.ObservedTrafficMapGeneration = tc.generation
		s.TrafficMap.Status.Conditions = append(s.TrafficMap.Status.Conditions, metav1.Condition{Type: "Programmed", Status: tc.condition, Reason: "Programmed", ObservedGeneration: 4, LastTransitionTime: metav1.NewTime(fixtureClock.Now().Add(-time.Hour))})
		got, _ := ProjectEndpoint(s, fixtureClock)
		if string(got.Content.Routing.Acknowledgement) != tc.want {
			t.Fatalf("ack=%+v want%s", got.Content.Routing, tc.want)
		}
	}
}

func TestStrictTrafficMapOwnershipUnavailableDoesNotAttribute(t *testing.T) {
	for _, mutate := range []func(*c.Result){
		func(s *c.Result) { s.TrafficMap.Spec.Service = "other" },
		func(s *c.Result) { s.TrafficMap.Name = "other" },
		func(s *c.Result) { s.TrafficMap.Namespace = "other" },
		func(s *c.Result) { s.TrafficMap.OwnerReferences[0].UID = "old-parent" },
		func(s *c.Result) { s.TrafficMap.OwnerReferences[0].APIVersion = "other.io/v1" },
		func(s *c.Result) {
			s.TrafficMap.OwnerReferences = append(s.TrafficMap.OwnerReferences, s.TrafficMap.OwnerReferences[0])
		},
		func(s *c.Result) { s.TrafficMap.DeletionTimestamp = &metav1.Time{Time: fixtureClock.Now()} },
	} {
		s := fixture(t)
		mutate(&s)
		got, err := ProjectEndpoint(s, fixtureClock)
		if err != nil {
			t.Fatal(err)
		}
		if len(got.Content.Entries) != 0 || got.Content.Routing.Acquisition.State != "Unavailable" {
			t.Fatalf("attributed unbound map=%+v", got.Content)
		}
	}
	s := fixture(t)
	s.TrafficMap.OwnerReferences[0].APIVersion = "ome.io/v1future"
	got, _ := ProjectEndpoint(s, fixtureClock)
	if len(got.Content.Entries) != 2 {
		t.Fatal("compatible group owner rejected")
	}
}

func TestCompleteLateConflictAndScanBudgetsBeforePreview(t *testing.T) {
	s := fixture(t)
	for i := 0; i < 70; i++ {
		h := s.InferenceService.Status.Placement.Candidates[0]
		h.Cluster = "home-" + strings.Repeat("a", i+1)
		s.InferenceService.Status.Placement.Candidates = append(s.InferenceService.Status.Placement.Candidates, h)
	}
	conflict := s.InferenceService.Status.Placement.Candidates[0]
	conflict.Endpoint = &knapis.URL{Scheme: "https", Host: "demo-a.invalid", Path: "/different-hidden-path"}
	s.InferenceService.Status.Placement.Candidates = append(s.InferenceService.Status.Placement.Candidates, conflict)
	got, _ := ProjectStatus(s, fixtureClock)
	if len(got.Content.Placement.Homes) != 0 || got.Content.Placement.HomePreview.State != "ConflictingDuplicates" {
		t.Fatalf("late conflict=%+v", got.Content.Placement)
	}
	s = fixture(t)
	s.InferenceService.Status.Placement.Candidates = make([]ome.CandidatePlacement, 257)
	got, _ = ProjectStatus(s, fixtureClock)
	if got.Content.Placement.HomePreview.State != "BudgetExceeded" {
		t.Fatal("overbudget prefix validated")
	}
	s = fixture(t)
	for i := 0; i < 20; i++ {
		s.TrafficMap.Status.Conditions = append(s.TrafficMap.Status.Conditions, metav1.Condition{Type: "Other" + strings.Repeat("a", i), Status: metav1.ConditionUnknown, Reason: "NotRecorded", LastTransitionTime: metav1.NewTime(fixtureClock.Now().Add(-time.Hour))})
	}
	condition := s.TrafficMap.Status.Conditions[0]
	condition.Status = metav1.ConditionFalse
	s.TrafficMap.Status.Conditions = append(s.TrafficMap.Status.Conditions, condition)
	ep, _ := ProjectEndpoint(s, fixtureClock)
	if ep.Content.Routing.Routable.Source.Freshness != "Invalid" || len(ep.Content.Entries) != 2 {
		t.Fatalf("condition conflict erased independent entries=%+v", ep.Content)
	}
}

func TestCanonicalMapOrderAndMalformedRequiredCondition(t *testing.T) {
	s := fixture(t)
	first, _ := ProjectEndpoint(s, fixtureClock)
	s.TrafficMap.Spec.Entries[0], s.TrafficMap.Spec.Entries[1] = s.TrafficMap.Spec.Entries[1], s.TrafficMap.Spec.Entries[0]
	s.InferenceService.Status.Placement.Candidates[0], s.InferenceService.Status.Placement.Candidates[1] = s.InferenceService.Status.Placement.Candidates[1], s.InferenceService.Status.Placement.Candidates[0]
	second, _ := ProjectEndpoint(s, fixtureClock)
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if !bytes.Equal(a, b) {
		t.Fatalf("not canonical:\n%s\n%s", a, b)
	}
	s.WorkloadClusters[0].Status.Conditions[0].LastTransitionTime = metav1.Time{}
	explain, _ := ProjectExplain(s, fixtureClock)
	if explain.Content.Clusters[0].ReportedReady.Source.Freshness != "Invalid" {
		t.Fatal("missing required time claimed ready")
	}
}
