package placementprojection

import (
	"bytes"
	"encoding/json"
	"fmt"
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

func TestProjectStatusPreservesAdmittingPhases(t *testing.T) {
	for _, tc := range []struct {
		name          string
		placement     ome.PlacementPhase
		candidate     ome.CandidatePlacementPhase
		wantPlacement v.PlacementValue
		wantCandidate v.PlacementValue
	}{
		{"admitting", ome.PlacementPhaseAdmitting, ome.CandidatePhaseAdmitting, "Admitting", "Admitting"},
		{"legacy-racing-admitted", "Racing", "Admitted", "Racing", "Admitted"},
		{"placed", ome.PlacementPhasePlaced, ome.CandidatePhasePlaced, "Placed", "Placed"}, //nolint:staticcheck // Deliberately exercise legacy wire compatibility.
		{"empty", "", "", "NotRecorded", "NotRecorded"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t)
			s.InferenceService.Status.Placement.Phase = tc.placement
			s.InferenceService.Status.Placement.Candidates = []ome.CandidatePlacement{{Cluster: "west", Phase: tc.candidate}}
			got, err := ProjectStatus(s, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			if got.Content.Placement.Phase != tc.wantPlacement {
				t.Errorf("placement phase=%q want %q", got.Content.Placement.Phase, tc.wantPlacement)
			}
			if len(got.Content.Placement.Homes) != 1 {
				t.Fatalf("homes=%+v", got.Content.Placement.Homes)
			}
			if got.Content.Placement.Homes[0].Phase != tc.wantCandidate {
				t.Errorf("candidate phase=%q want %q", got.Content.Placement.Homes[0].Phase, tc.wantCandidate)
			}
			if len(got.Content.Issues) != 0 {
				t.Errorf("unexpected issues=%+v", got.Content.Issues)
			}
		})
	}
}

func TestProjectStatusFlagsUnknownPhasesWithoutEchoingThem(t *testing.T) {
	for _, tc := range []struct {
		name      string
		placement string
		candidate string
		malformed bool
	}{
		{"future", "FuturePlacement", "FutureCandidate", false},
		{"placement-control-text", "FuturePlacement\nplacement-control\x1b[31m", "FutureCandidate", false},
		{"candidate-control-text", "FuturePlacement", "FutureCandidate\rcandidate-control\x1b[2J", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t)
			s.InferenceService.Status.Placement.Phase = ome.PlacementPhase(tc.placement)
			s.InferenceService.Status.Placement.Candidates = []ome.CandidatePlacement{
				{Cluster: "west", Phase: ome.CandidatePlacementPhase(tc.candidate)},
				{Cluster: "east", Phase: ome.CandidatePlacementPhase(tc.candidate)},
				{Cluster: "empty"},
			}
			got, err := ProjectStatus(s, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			if got.Content.Placement.Phase != "Unknown" {
				t.Errorf("placement phase=%q want Unknown", got.Content.Placement.Phase)
			}
			wantHomes := 3
			if tc.malformed {
				wantHomes = 0
				if got.Content.Placement.HomePreview.State != "MalformedPayload" {
					t.Errorf("home preview=%+v want MalformedPayload", got.Content.Placement.HomePreview)
				}
			}
			if len(got.Content.Placement.Homes) != wantHomes {
				t.Fatalf("homes=%+v", got.Content.Placement.Homes)
			}
			for _, home := range got.Content.Placement.Homes {
				want := v.PlacementValue("Unknown")
				if home.Cluster == "empty" {
					want = "NotRecorded"
				}
				if home.Phase != want {
					t.Errorf("home %s phase=%q want %q", home.Cluster, home.Phase, want)
				}
			}
			wantIssues := []v.PlacementIssue{
				{Group: "CandidatePhase", Code: "UnknownValue", Count: 2},
				{Group: "PlacementPhase", Code: "UnknownValue", Count: 1},
			}
			if tc.malformed {
				wantIssues = wantIssues[1:]
			}
			if !reflect.DeepEqual(got.Content.Issues, wantIssues) {
				t.Errorf("issues=%+v want %+v", got.Content.Issues, wantIssues)
			}
			candidates := s.InferenceService.Status.Placement.Candidates
			candidates[0], candidates[2] = candidates[2], candidates[0]
			reordered, err := ProjectStatus(s, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			for _, format := range []report.Format{report.FormatJSON, report.FormatYAML, report.FormatTable} {
				var out, reorderedOut bytes.Buffer
				if err := report.Write(&out, format, got); err != nil {
					t.Fatal(err)
				}
				if err := report.Write(&reorderedOut, format, reordered); err != nil {
					t.Fatal(err)
				}
				if out.String() != reorderedOut.String() {
					t.Errorf("%s output changed after reordering candidates", format)
				}
				for _, raw := range []string{tc.placement, tc.candidate, "FuturePlacement", "FutureCandidate", "placement-control", "candidate-control", "\x1b"} {
					if strings.Contains(out.String(), raw) {
						t.Errorf("%s output leaked %q: %s", format, raw, out.String())
					}
				}
			}
		})
	}
}

func TestProjectStatusDiscardsCandidatePhaseIssuesOnInvalidHomes(t *testing.T) {
	for _, tc := range []struct {
		name      string
		other     ome.CandidatePlacement
		wantState v.PlacementValue
	}{
		{"malformed", ome.CandidatePlacement{Cluster: "east", Phase: "FutureCandidate\ncontrol-text\x1b[2J"}, "MalformedPayload"},
		{"conflicting-duplicate", ome.CandidatePlacement{Cluster: "west", Phase: ome.CandidatePhasePlaced}, "ConflictingDuplicates"}, //nolint:staticcheck // Deliberately exercise legacy wire compatibility.
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t)
			s.InferenceService.Status.Placement.Phase = "FuturePlacement"
			unknown := ome.CandidatePlacement{Cluster: "west", Phase: "FutureCandidate"}
			outputs := map[report.Format]string{}
			for i, candidates := range [][]ome.CandidatePlacement{{unknown, tc.other}, {tc.other, unknown}} {
				s.InferenceService.Status.Placement.Candidates = candidates
				got, err := ProjectStatus(s, fixtureClock)
				if err != nil {
					t.Fatal(err)
				}
				if got.Content.Placement.HomePreview.State != tc.wantState || len(got.Content.Placement.Homes) != 0 {
					t.Errorf("order %d: placement=%+v", i, got.Content.Placement)
				}
				wantIssues := []v.PlacementIssue{{Group: "PlacementPhase", Code: "UnknownValue", Count: 1}}
				if !reflect.DeepEqual(got.Content.Issues, wantIssues) {
					t.Errorf("order %d: issues=%+v want %+v", i, got.Content.Issues, wantIssues)
				}
				for _, format := range []report.Format{report.FormatJSON, report.FormatYAML, report.FormatTable} {
					var out bytes.Buffer
					if err := report.Write(&out, format, got); err != nil {
						t.Fatal(err)
					}
					if i == 0 {
						outputs[format] = out.String()
					} else if out.String() != outputs[format] {
						t.Errorf("%s output changed after reordering invalid candidates", format)
					}
					for _, raw := range []string{"FuturePlacement", "FutureCandidate", "control-text", "\x1b"} {
						if strings.Contains(out.String(), raw) {
							t.Errorf("%s output leaked %q: %s", format, raw, out.String())
						}
					}
				}
			}
		})
	}
}

func TestProjectStatusDiscardsCandidateDerivedIssuesOnRejectedCandidateSet(t *testing.T) {
	provenanceCases := []struct {
		name       string
		candidate  ome.CandidatePlacement
		privateRaw string
	}{
		{
			name: "malformed-provenance",
			candidate: ome.CandidatePlacement{
				Cluster: "west",
				Phase:   "FutureCandidate",
				Autoscaling: &ome.CandidateAutoscalingStatus{Policies: []ome.CandidatePolicyDigest{{
					Name:           "safe-policy",
					PortableDigest: "pv1:malformed-provenance-control\n\x1b[31m",
				}}},
			},
			privateRaw: "malformed-provenance-control",
		},
		{
			name: "budget-exceeded-provenance",
			candidate: ome.CandidatePlacement{
				Cluster: "west",
				Phase:   "FutureCandidate",
				Autoscaling: &ome.CandidateAutoscalingStatus{
					Policies: make([]ome.CandidatePolicyDigest, 65),
				},
			},
			privateRaw: "budget-provenance-control",
		},
	}
	provenanceCases[1].candidate.Autoscaling.Policies[0] = ome.CandidatePolicyDigest{
		Name:           "budget-provenance-control\n\x1b[31m",
		PortableDigest: "pv1:hidden",
	}

	rejectionCases := []struct {
		name      string
		candidate ome.CandidatePlacement
		wantState v.PlacementValue
	}{
		{
			name:      "malformed-candidate",
			candidate: ome.CandidatePlacement{Cluster: "east", Phase: "RejectedCandidate\ncandidate-control\x1b[2J"},
			wantState: "MalformedPayload",
		},
		{
			name:      "conflicting-duplicate",
			candidate: ome.CandidatePlacement{Cluster: "west", Phase: ome.CandidatePhasePlaced}, //nolint:staticcheck // Deliberately exercise legacy wire compatibility.
			wantState: "ConflictingDuplicates",
		},
	}

	for _, provenanceCase := range provenanceCases {
		for _, rejectionCase := range rejectionCases {
			t.Run(provenanceCase.name+"/"+rejectionCase.name, func(t *testing.T) {
				outputs := map[report.Format]string{}
				orders := [][]ome.CandidatePlacement{
					{provenanceCase.candidate, rejectionCase.candidate},
					{rejectionCase.candidate, provenanceCase.candidate},
				}
				for i, candidates := range orders {
					s := fixture(t)
					s.InferenceService.Status.Placement.Phase = "FuturePlacement"
					s.InferenceService.Status.Placement.Candidates = candidates
					got, err := ProjectStatus(s, fixtureClock)
					if err != nil {
						t.Fatal(err)
					}
					if got.Content.Placement.HomePreview.State != rejectionCase.wantState ||
						got.Content.Placement.HomePreview.Total != len(candidates) ||
						got.Content.Placement.HomePreview.Kept != 0 ||
						got.Content.Placement.ProvenancePreview.State != "Unavailable" ||
						got.Content.Placement.ProvenancePreview.Total != len(candidates) ||
						got.Content.Placement.ProvenancePreview.Kept != 0 ||
						len(got.Content.Placement.Homes) != 0 {
						t.Errorf("order %d: rejected placement=%+v", i, got.Content.Placement)
					}
					wantIssues := []v.PlacementIssue{{Group: "PlacementPhase", Code: "UnknownValue", Count: 1}}
					if !reflect.DeepEqual(got.Content.Issues, wantIssues) {
						t.Errorf("order %d: issues=%+v want %+v", i, got.Content.Issues, wantIssues)
					}
					for _, issue := range got.Content.Issues {
						if issue.Group == "CandidateProvenance" || issue.Group == "CandidatePhase" {
							t.Errorf("order %d: provisional candidate issue survived rejection: %+v", i, issue)
						}
					}
					for _, format := range []report.Format{report.FormatJSON, report.FormatYAML, report.FormatTable} {
						var out bytes.Buffer
						if err := report.Write(&out, format, got); err != nil {
							t.Fatal(err)
						}
						if i == 0 {
							outputs[format] = out.String()
						} else if out.String() != outputs[format] {
							t.Errorf("%s output changed after reordering rejected candidates", format)
						}
						for _, raw := range []string{
							"FuturePlacement",
							"FutureCandidate",
							"RejectedCandidate",
							provenanceCase.privateRaw,
							"candidate-control",
							"\x1b",
						} {
							if strings.Contains(out.String(), raw) {
								t.Errorf("%s output leaked %q: %s", format, raw, out.String())
							}
						}
					}
				}
			})
		}
	}
}

func TestProjectStatusAggregatesUnknownCandidatePhasesAcrossBounds(t *testing.T) {
	for _, tc := range []struct {
		name           string
		unique         int
		unknownFrom    int
		duplicate      bool
		wantIssueCount int
		wantKept       int
		wantState      v.PlacementValue
		wantPhase      v.PlacementValue
	}{
		{"identical-duplicate", 1, 0, true, 1, 1, "Validated", "Unknown"},
		{"unknown-beyond-preview", 65, 64, false, 1, 64, "Validated", "Placed"},
		{"at-scan-budget", 256, 0, false, 256, 64, "Validated", "Unknown"},
		{"over-scan-budget", 257, 0, false, 0, 0, "BudgetExceeded", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := fixture(t)
			candidates := make([]ome.CandidatePlacement, tc.unique)
			for i := range candidates {
				phase := ome.CandidatePhasePlaced //nolint:staticcheck // Deliberately exercise legacy wire compatibility.
				if i >= tc.unknownFrom {
					phase = "FutureCandidate"
				}
				candidates[i] = ome.CandidatePlacement{Cluster: fmt.Sprintf("home-%03d", i), Phase: phase}
			}
			if tc.duplicate {
				candidates = append(candidates, candidates[0])
			}
			s.InferenceService.Status.Placement.Candidates = candidates
			got, err := ProjectStatus(s, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			wantIssues := []v.PlacementIssue{}
			if tc.wantIssueCount > 0 {
				wantIssues = append(wantIssues, v.PlacementIssue{Group: "CandidatePhase", Code: "UnknownValue", Count: tc.wantIssueCount})
			}
			if !reflect.DeepEqual(got.Content.Issues, wantIssues) {
				t.Errorf("issues=%+v want %+v", got.Content.Issues, wantIssues)
			}
			preview := got.Content.Placement.HomePreview
			if preview.State != tc.wantState || preview.Total != tc.unique || preview.Kept != tc.wantKept || len(got.Content.Placement.Homes) != tc.wantKept {
				t.Errorf("preview=%+v homes=%d", preview, len(got.Content.Placement.Homes))
			}
			for _, home := range got.Content.Placement.Homes {
				if home.Phase != tc.wantPhase {
					t.Errorf("home %s phase=%q want %q", home.Cluster, home.Phase, tc.wantPhase)
				}
			}
			for i, j := 0, len(candidates)-1; i < j; i, j = i+1, j-1 {
				candidates[i], candidates[j] = candidates[j], candidates[i]
			}
			reordered, err := ProjectStatus(s, fixtureClock)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, reordered) {
				t.Error("report changed after reversing candidate order")
			}
		})
	}
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
		wantCountState := v.PlacementValue("Reported")
		if mode == "Future" {
			wantCountState = "NotApplicable"
		}
		if got.Content.Placement.Phase != "Failed" || got.Content.Placement.Address.State != "Present" || got.Content.Placement.Homes[0].AdmittedReplicas.State != wantCountState {
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
		{false, metav1.ConditionFalse, 4, "Invalid"},
		{true, metav1.ConditionTrue, 3, "Invalid"},
	} {
		s := fixture(t)
		s.TrafficMap.Status.Published = tc.scalar
		s.TrafficMap.Status.ObservedTrafficMapGeneration = tc.generation
		s.TrafficMap.Status.Conditions = append(s.TrafficMap.Status.Conditions, metav1.Condition{Type: "Published", Status: tc.condition, Reason: "Published", ObservedGeneration: 4, LastTransitionTime: metav1.NewTime(fixtureClock.Now().Add(-time.Hour))})
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
