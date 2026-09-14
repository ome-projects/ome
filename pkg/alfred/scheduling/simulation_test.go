package scheduling

import (
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

func migrationRequest() Request {
	r := validRequest()
	r.MigrationFromNode = "gpu-a"
	r.ExcludedNodes = []string{"gpu-a"}
	for i := range r.SourcePods {
		r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Object: r.SourcePods[i].DeepCopy()})
	}
	for i := range r.ReplacementPods {
		r.ReplacementPods[i].Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"gpu-a"}}}}}}}}
	}
	return r
}

func TestMigrationSingleSourceExclusion(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*Request)
		want bool
	}{
		{"whole gang", func(*Request) {}, true},
		{"legacy still excludes every source", func(r *Request) { r.MigrationFromNode = "" }, false},
		{"not a source", func(r *Request) { r.MigrationFromNode = "gpu-c"; r.ExcludedNodes = []string{"gpu-c"} }, false},
		{"invalid name", func(r *Request) { r.MigrationFromNode = "GPU_A"; r.ExcludedNodes = []string{"GPU_A"} }, false},
		{"extra exclusion", func(r *Request) { r.ExcludedNodes = append(r.ExcludedNodes, "gpu-b") }, false},
		{"duplicate exclusion", func(r *Request) { r.ExcludedNodes = append(r.ExcludedNodes, "gpu-a") }, false},
		{"wrong exclusion", func(r *Request) { r.ExcludedNodes = []string{"gpu-b"} }, false},
		{"missing exclusion", func(r *Request) { r.ExcludedNodes = nil }, false},
		{"unknown source node", func(r *Request) { r.ClusterObjects = r.ClusterObjects[1:] }, false},
		{"deleting source node", func(r *Request) {
			now := metav1.Now()
			r.ClusterObjects[0].Object.(*corev1.Node).DeletionTimestamp = &now
		}, false},
		{"source occupancy omitted", func(r *Request) { r.ClusterObjects = r.ClusterObjects[:len(r.ClusterObjects)-1] }, false},
		{"source occupancy changed", func(r *Request) {
			r.ClusterObjects[len(r.ClusterObjects)-1].Object.(*corev1.Pod).Spec.NodeName = "gpu-c"
		}, false},
		{"missing overlay", func(r *Request) { r.ReplacementPods[1].Spec.Affinity = nil }, false},
		{"empty terms", func(r *Request) {
			r.ReplacementPods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms = nil
		}, false},
		{"unguarded OR branch", func(r *Request) {
			na := r.ReplacementPods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			na.NodeSelectorTerms = append(na.NodeSelectorTerms, corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"west"}}}})
		}, false},
		{"wrong operator", func(r *Request) {
			r.ReplacementPods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Operator = corev1.NodeSelectorOpIn
		}, false},
		{"safe existing superset", func(r *Request) {
			r.ReplacementPods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values = append([]string{"gpu-a"}, "gpu-z")
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := migrationRequest()
			tc.edit(&r)
			before := make([]runtime.RawExtension, len(r.ClusterObjects))
			for i := range r.ClusterObjects {
				before[i] = *r.ClusterObjects[i].DeepCopy()
			}
			_, _, err := validateRequest(r)
			if (err == nil) != tc.want {
				t.Fatalf("validateRequest()=%v, allowed want %v", err, tc.want)
			}
			if reason := map[string]string{"not a source": "does not host a source Pod", "invalid name": "DNS1123"}[tc.name]; reason != "" && (err == nil || !strings.Contains(err.Error(), reason)) {
				t.Fatalf("validateRequest()=%v, want %s", err, reason)
			}
			if !reflect.DeepEqual(before, r.ClusterObjects) {
				t.Fatal("validation changed occupied snapshot objects")
			}
		})
	}
	// A feasible response may use the other still-occupied source node; only the
	// worker's scheduler can establish whether its remaining capacity fits.
	r := migrationRequest()
	if err := ValidateResult(r, matchingResult(r, []Placement{{Pod: identity(r.ReplacementPods[0]), NodeName: "gpu-b"}, {Pod: identity(r.ReplacementPods[1]), NodeName: "gpu-c"}})); err != nil {
		t.Fatal(err)
	}
}

func TestValidateResultAcceptsFullFeasiblePlacement(t *testing.T) {
	req := validRequest()
	result := matchingResult(req, []Placement{
		{Pod: identity(req.ReplacementPods[0]), NodeName: "gpu-c"},
		{Pod: identity(req.ReplacementPods[1]), NodeName: "gpu-d"},
	})
	if err := ValidateResult(req, result); err != nil {
		t.Fatalf("ValidateResult() = %v, want nil", err)
	}
}

func TestValidateResponseAcceptsValidDecisions(t *testing.T) {
	req := validRequest()
	placements := []Placement{
		{Pod: identity(req.ReplacementPods[0]), NodeName: "gpu-c"},
		{Pod: identity(req.ReplacementPods[1]), NodeName: "gpu-d"},
	}
	tests := []struct {
		name       string
		decision   Decision
		reason     SimulationReason
		placements []Placement
	}{
		{name: "feasible", decision: DecisionFeasible, reason: SimulationReasonPlacementFound, placements: placements},
		{name: "infeasible", decision: DecisionInfeasible, reason: SimulationReasonNoFeasiblePlacement},
		{name: "unsupported", decision: DecisionUnsupported, reason: SimulationReasonUnsupported},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := matchingResult(req, tc.placements)
			result.Decision = tc.decision
			result.Reason = tc.reason
			if err := ValidateResponse(req, result); err != nil {
				t.Fatalf("ValidateResponse() = %v, want nil", err)
			}
		})
	}
}

func TestValidateResponseRejectsDecisionReasonAndPlacementMismatches(t *testing.T) {
	req := validRequest()
	placements := []Placement{
		{Pod: identity(req.ReplacementPods[0]), NodeName: "gpu-c"},
		{Pod: identity(req.ReplacementPods[1]), NodeName: "gpu-d"},
	}
	tests := []struct {
		name       string
		decision   Decision
		reason     SimulationReason
		placements []Placement
		wantErr    string
	}{
		{name: "feasible wrong reason", decision: DecisionFeasible, reason: SimulationReasonNoFeasiblePlacement, placements: placements, wantErr: "invalid reason"},
		{name: "infeasible wrong reason", decision: DecisionInfeasible, reason: SimulationReasonUnsupported, wantErr: "invalid reason"},
		{name: "unsupported wrong reason", decision: DecisionUnsupported, reason: SimulationReasonNoFeasiblePlacement, wantErr: "invalid reason"},
		{name: "infeasible placements", decision: DecisionInfeasible, reason: SimulationReasonNoFeasiblePlacement, placements: placements, wantErr: "placements"},
		{name: "unsupported placements", decision: DecisionUnsupported, reason: SimulationReasonUnsupported, placements: placements, wantErr: "placements"},
		{name: "unknown decision", decision: Decision("Maybe"), reason: SimulationReasonUnsupported, wantErr: "unknown decision"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := matchingResult(req, tc.placements)
			result.Decision = tc.decision
			result.Reason = tc.reason
			err := ValidateResponse(req, result)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateResponse() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateResponseRejectsStaleNegativeEnvelope(t *testing.T) {
	req := validRequest()
	result := matchingResult(req, nil)
	result.Decision = DecisionInfeasible
	result.Reason = SimulationReasonNoFeasiblePlacement
	result.Profile.ConfigurationID = "sha256:stale"
	if err := ValidateResponse(req, result); err == nil || !strings.Contains(err.Error(), "profile identity") {
		t.Fatalf("ValidateResponse() error = %v, want profile identity mismatch", err)
	}
}

func TestValidateResultRecognizesOnlyCoreV1NodeObjects(t *testing.T) {
	tests := []struct {
		name    string
		object  runtime.Object
		wantErr string
	}{
		{
			name:   "typed core node with empty type metadata",
			object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-c"}},
		},
		{
			name: "unstructured core v1 node",
			object: &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "v1", "kind": "Node",
				"metadata": map[string]interface{}{"name": "gpu-c"},
			}},
		},
		{
			name: "unstructured custom resource named Node",
			object: &unstructured.Unstructured{Object: map[string]interface{}{
				"apiVersion": "example.com/v1", "kind": "Node",
				"metadata": map[string]interface{}{"name": "gpu-c"},
			}},
			wantErr: "unknown node",
		},
		{
			name: "typed core node with contradictory type metadata",
			object: &corev1.Node{
				TypeMeta:   metav1.TypeMeta{APIVersion: "example.com/v1", Kind: "Node"},
				ObjectMeta: metav1.ObjectMeta{Name: "gpu-c"},
			},
			wantErr: "unknown node",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			req.ClusterObjects[2] = runtime.RawExtension{Object: tc.object}
			result := matchingResult(req, []Placement{
				{Pod: identity(req.ReplacementPods[0]), NodeName: "gpu-c"},
				{Pod: identity(req.ReplacementPods[1]), NodeName: "gpu-d"},
			})
			err := ValidateResult(req, result)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("ValidateResult() = %v, want nil", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateResult() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateResultRejectsInvalidPlacementSets(t *testing.T) {
	req := validRequest()
	first := identity(req.ReplacementPods[0])
	second := identity(req.ReplacementPods[1])
	tests := []struct {
		name       string
		placements []Placement
		wantErr    string
	}{
		{name: "missing gang member", placements: []Placement{{Pod: first, NodeName: "gpu-c"}}, wantErr: "missing placement"},
		{name: "duplicate member", placements: []Placement{{Pod: first, NodeName: "gpu-c"}, {Pod: first, NodeName: "gpu-d"}}, wantErr: "duplicate placement"},
		{name: "foreign member", placements: []Placement{{Pod: first, NodeName: "gpu-c"}, {Pod: PodIdentity{Namespace: "team-a", Name: "foreign"}, NodeName: "gpu-d"}}, wantErr: "unexpected pod"},
		{name: "unknown node", placements: []Placement{{Pod: first, NodeName: "gpu-c"}, {Pod: second, NodeName: "gpu-z"}}, wantErr: "unknown node"},
		{name: "excluded source node", placements: []Placement{{Pod: first, NodeName: "gpu-a"}, {Pod: second, NodeName: "gpu-d"}}, wantErr: "excluded node"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateResult(req, matchingResult(req, tc.placements))
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateResult() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateResultRejectsStaleOrMalformedEnvelopes(t *testing.T) {
	req := validRequest()
	placements := []Placement{
		{Pod: identity(req.ReplacementPods[0]), NodeName: "gpu-c"},
		{Pod: identity(req.ReplacementPods[1]), NodeName: "gpu-d"},
	}
	tests := []struct {
		name    string
		mutate  func(*Request, *Result)
		wantErr string
	}{
		{name: "schema", mutate: func(_ *Request, result *Result) { result.SchemaVersion = "v2" }, wantErr: "schema version"},
		{name: "request", mutate: func(_ *Request, result *Result) { result.RequestID = "request-older" }, wantErr: "request ID"},
		{name: "snapshot", mutate: func(_ *Request, result *Result) { result.SnapshotID = "snapshot-older" }, wantErr: "snapshot ID"},
		{name: "snapshot time", mutate: func(_ *Request, result *Result) {
			result.SnapshotTime = metav1.NewTime(result.SnapshotTime.Add(-time.Minute))
		}, wantErr: "snapshot time"},
		{name: "profile", mutate: func(_ *Request, result *Result) { result.Profile.ConfigurationID = "sha256:older" }, wantErr: "profile identity"},
		{name: "malformed request ID", mutate: func(req *Request, _ *Result) { req.RequestID = " padded " }, wantErr: "request ID"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			changedReq := req
			result := matchingResult(changedReq, placements)
			tc.mutate(&changedReq, &result)
			err := ValidateResult(changedReq, result)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateResult() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateResultRejectsResponsesThatCannotBeAccepted(t *testing.T) {
	req := validRequest()
	placements := []Placement{
		{Pod: identity(req.ReplacementPods[0]), NodeName: "gpu-c"},
		{Pod: identity(req.ReplacementPods[1]), NodeName: "gpu-d"},
	}

	t.Run("no live nodes", func(t *testing.T) {
		withoutNodes := req
		withoutNodes.ClusterObjects = nil
		if err := ValidateResult(withoutNodes, matchingResult(withoutNodes, placements)); err == nil || !strings.Contains(err.Error(), "live nodes") {
			t.Fatalf("ValidateResult() error = %v, want no live nodes", err)
		}
	})

	for _, decision := range []Decision{DecisionInfeasible, DecisionUnsupported} {
		t.Run(string(decision), func(t *testing.T) {
			result := matchingResult(req, nil)
			result.Decision = decision
			if decision == DecisionInfeasible {
				result.Reason = SimulationReasonNoFeasiblePlacement
			} else {
				result.Reason = SimulationReasonUnsupported
			}
			if err := ValidateResult(req, result); err == nil || !strings.Contains(err.Error(), "not feasible") {
				t.Fatalf("ValidateResult() error = %v, want non-feasible rejection", err)
			}
		})
	}

	t.Run("unknown decision", func(t *testing.T) {
		result := matchingResult(req, placements)
		result.Decision = Decision("Maybe")
		if err := ValidateResult(req, result); err == nil || !strings.Contains(err.Error(), "unknown decision") {
			t.Fatalf("ValidateResult() error = %v, want unknown decision rejection", err)
		}
	})

	t.Run("unknown feasible reason", func(t *testing.T) {
		result := matchingResult(req, placements)
		result.Reason = SimulationReason("TrustMe")
		if err := ValidateResult(req, result); err == nil || !strings.Contains(err.Error(), "invalid reason") {
			t.Fatalf("ValidateResult() error = %v, want invalid reason rejection", err)
		}
	})
}

func TestValidateResultRejectsInvalidRequestPods(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Request)
		wantErr string
	}{
		{
			name: "replacement scheduler differs from profile",
			mutate: func(req *Request) {
				req.ReplacementPods[1].Spec.SchedulerName = corev1.DefaultSchedulerName
			},
			wantErr: "scheduler name",
		},
		{
			name: "replacement is already bound",
			mutate: func(req *Request) {
				req.ReplacementPods[0].Spec.NodeName = "gpu-c"
			},
			wantErr: "already bound",
		},
		{
			name: "source node is not explicitly excluded",
			mutate: func(req *Request) {
				req.ExcludedNodes = []string{"gpu-a"}
			},
			wantErr: "source node",
		},
		{
			name: "source identity is not immutable",
			mutate: func(req *Request) {
				req.SourcePods[0].UID = ""
			},
			wantErr: "source pod identity",
		},
		{
			name: "source pods missing",
			mutate: func(req *Request) {
				req.SourcePods = nil
			},
			wantErr: "source pods",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			tc.mutate(&req)
			result := matchingResult(req, []Placement{
				{Pod: identity(req.ReplacementPods[0]), NodeName: "gpu-c"},
				{Pod: identity(req.ReplacementPods[1]), NodeName: "gpu-d"},
			})
			err := ValidateResult(req, result)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("ValidateResult() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func validRequest() Request {
	return Request{
		SchemaVersion: SimulationSchemaV1,
		RequestID:     "request-current",
		Profile: ProfileIdentity{
			SchedulerName: "custom-gang", Backend: "custom-gang-v030",
			SchedulerVersion: "v0.30.1", ConfigurationID: "sha256:current",
		},
		ReplacementPods: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "replacement-0", UID: types.UID("replacement-uid-0")}, Spec: corev1.PodSpec{SchedulerName: "custom-gang"}},
			{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "replacement-1", UID: types.UID("replacement-uid-1")}, Spec: corev1.PodSpec{SchedulerName: "custom-gang"}},
		},
		SourcePods: []corev1.Pod{
			{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "source-0", UID: types.UID("source-uid-0")}, Spec: corev1.PodSpec{NodeName: "gpu-a"}},
			{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "source-1", UID: types.UID("source-uid-1")}, Spec: corev1.PodSpec{NodeName: "gpu-b"}},
		},
		ClusterObjects: []runtime.RawExtension{
			{Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-a"}}},
			{Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-b"}}},
			{Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-c"}}},
			{Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "gpu-d"}}},
		},
		SnapshotID:   "snapshot-current",
		SnapshotTime: metav1.NewTime(time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)),
		RequireGang:  true,
		ExcludedNodes: []string{
			"gpu-a", "gpu-b",
		},
	}
}

func matchingResult(req Request, placements []Placement) Result {
	return Result{
		SchemaVersion: req.SchemaVersion,
		RequestID:     req.RequestID,
		SnapshotID:    req.SnapshotID,
		SnapshotTime:  req.SnapshotTime,
		Profile:       req.Profile,
		Decision:      DecisionFeasible,
		Reason:        SimulationReasonPlacementFound,
		Placements:    placements,
	}
}

func identity(pod corev1.Pod) PodIdentity {
	return PodIdentity{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}
}
