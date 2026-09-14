package scheduling

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

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
			result := matchingResult(req, placements)
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
