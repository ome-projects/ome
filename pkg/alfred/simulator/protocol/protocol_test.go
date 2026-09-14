package protocol

import (
	"bytes"
	"encoding/json"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"
)

func migrationRequest() Request {
	r := validRequest()
	r.MigrationFromNode = "source-node"
	r.RequireGang = true
	second := r.SourcePods[0].DeepCopy()
	second.Name = "source-two"
	second.UID = "source-two-uid"
	second.Spec.NodeName = "other-source"
	r.SourcePods = append(r.SourcePods, *second)
	r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "other-source"}}}, runtime.RawExtension{Object: second.DeepCopy()})
	replacement := r.ReplacementPods[0].DeepCopy()
	replacement.Name = "replacement-two"
	replacement.UID = "replacement-two-uid"
	r.ReplacementPods = append(r.ReplacementPods, *replacement)
	for i := range r.ReplacementPods {
		r.ReplacementPods[i].Spec.Affinity = &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"source-node"}}}}}}}}
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
		{"legacy exclusion", func(r *Request) { r.MigrationFromNode = "" }, false},
		{"not a source", func(r *Request) {
			r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "destination"}}})
			r.MigrationFromNode = "destination"
			r.ExcludedNodes = []string{"destination"}
		}, false},
		{"invalid DNS", func(r *Request) { r.MigrationFromNode = "SOURCE_NODE"; r.ExcludedNodes = []string{"SOURCE_NODE"} }, false},
		{"extra exclusion", func(r *Request) { r.ExcludedNodes = append(r.ExcludedNodes, "other-source") }, false},
		{"duplicate exclusion", func(r *Request) { r.ExcludedNodes = append(r.ExcludedNodes, "source-node") }, false},
		{"mismatched exclusion", func(r *Request) { r.ExcludedNodes = []string{"other-source"} }, false},
		{"missing exclusion", func(r *Request) { r.ExcludedNodes = nil }, false},
		{"unknown source node", func(r *Request) { r.ClusterObjects = r.ClusterObjects[1:] }, false},
		{"deleting source node", func(r *Request) {
			now := metav1.Now()
			r.ClusterObjects[0].Object.(*corev1.Node).DeletionTimestamp = &now
		}, false},
		{"missing occupancy", func(r *Request) { r.ClusterObjects = r.ClusterObjects[:len(r.ClusterObjects)-1] }, false},
		{"changed occupancy", func(r *Request) {
			r.ClusterObjects[len(r.ClusterObjects)-1].Object.(*corev1.Pod).Spec.NodeName = "source-node"
		}, false},
		{"missing overlay", func(r *Request) { r.ReplacementPods[1].Spec.Affinity = nil }, false},
		{"unguarded OR term", func(r *Request) {
			na := r.ReplacementPods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
			na.NodeSelectorTerms = append(na.NodeSelectorTerms, corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"west"}}}})
		}, false},
		{"wrong hostname key", func(r *Request) {
			r.ReplacementPods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Key = "other"
		}, false},
		{"wrong operator", func(r *Request) {
			r.ReplacementPods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Operator = corev1.NodeSelectorOpIn
		}, false},
		{"superset", func(r *Request) {
			r.ReplacementPods[0].Spec.Affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms[0].MatchExpressions[0].Values = []string{"source-node", "unrelated"}
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := migrationRequest()
			tc.edit(&r)
			before := make([]runtime.RawExtension, len(r.ClusterObjects))
			for i := range r.ClusterObjects {
				before[i] = *r.ClusterObjects[i].DeepCopy()
			}
			snapshot, err := Validate(r)
			if (err == nil) != tc.want {
				t.Fatalf("Validate()=%v, allowed want %v", err, tc.want)
			}
			if reason := map[string]string{"not a source": "does not host a source Pod", "invalid DNS": "DNS1123"}[tc.name]; reason != "" && (err == nil || !strings.Contains(err.Error(), reason)) {
				t.Fatalf("Validate()=%v, want %s", err, reason)
			}
			if !reflect.DeepEqual(before, r.ClusterObjects) {
				t.Fatal("validation changed snapshot occupancy")
			}
			if err == nil {
				for _, source := range r.SourcePods {
					actual := snapshot.Pods[types.NamespacedName{Namespace: source.Namespace, Name: source.Name}]
					if actual == nil || actual.UID != source.UID || actual.Spec.NodeName != source.Spec.NodeName {
						t.Fatal("source Pod absent from validated occupancy")
					}
				}
			}
		})
	}
}

func TestValidateRequiresSourceOccupancy(t *testing.T) {
	req := validRequest()
	req.ClusterObjects = req.ClusterObjects[:2] // Node and Namespace, but no source Pod.
	if _, err := Validate(req); err == nil {
		t.Fatal("missing occupied source was accepted")
	}
}

func TestValidateAcceptsCompleteSnapshot(t *testing.T) {
	req := validRequest()
	if len(req.ReplacementPods[0].Annotations) != 0 {
		t.Fatal("test request unexpectedly carries admission provenance")
	}
	snapshot, err := Validate(req)
	if err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
	if len(snapshot.Nodes) != 1 || len(snapshot.Namespaces) != 1 || len(snapshot.Pods) != 1 {
		t.Fatalf("Validate() snapshot = %+v, want one Node, Namespace, and Pod", snapshot)
	}
}

func TestResultForMapsDecisionReason(t *testing.T) {
	req := validRequest()
	tests := []struct {
		decision Decision
		want     Reason
	}{
		{decision: DecisionFeasible, want: ReasonPlacementFound},
		{decision: DecisionInfeasible, want: ReasonNoFeasiblePlacement},
		{decision: DecisionUnsupported, want: ReasonUnsupported},
	}
	for _, tc := range tests {
		t.Run(string(tc.decision), func(t *testing.T) {
			got := ResultFor(req, tc.decision)
			if got.SchemaVersion != req.SchemaVersion || got.RequestID != req.RequestID ||
				got.SnapshotID != req.SnapshotID || !got.SnapshotTime.Equal(&req.SnapshotTime) ||
				got.Profile != req.Profile || got.Decision != tc.decision || got.Reason != tc.want {
				t.Fatalf("ResultFor() = %+v, want matching envelope, decision %q and reason %q", got, tc.decision, tc.want)
			}
		})
	}
}

func TestResultForFailsClosedForUnknownDecision(t *testing.T) {
	got := ResultFor(validRequest(), Decision("Maybe"))
	if got.Decision != DecisionUnsupported || got.Reason != ReasonUnsupported {
		t.Fatalf("ResultFor(unknown) = %q/%q, want Unsupported/Unsupported", got.Decision, got.Reason)
	}
}

func TestValidateRejectsInvalidSnapshots(t *testing.T) {
	terminalTime := metav1.NewTime(time.Date(2026, 9, 14, 13, 0, 0, 0, time.UTC))
	tests := []struct {
		name    string
		mutate  func(*Request)
		wantErr string
	}{
		{name: "schema", mutate: func(r *Request) { r.SchemaVersion = "v2" }, wantErr: "schema version"},
		{name: "request ID", mutate: func(r *Request) { r.RequestID = " padded " }, wantErr: "request ID"},
		{name: "snapshot ID", mutate: func(r *Request) { r.SnapshotID = "" }, wantErr: "snapshot ID"},
		{name: "snapshot time", mutate: func(r *Request) { r.SnapshotTime = metav1.Time{} }, wantErr: "snapshot time"},
		{name: "profile scheduler", mutate: func(r *Request) { r.Profile.SchedulerName = "" }, wantErr: "scheduler name"},
		{name: "profile backend", mutate: func(r *Request) { r.Profile.Backend = "" }, wantErr: "backend"},
		{name: "profile version", mutate: func(r *Request) { r.Profile.SchedulerVersion = "" }, wantErr: "scheduler version"},
		{name: "profile configuration", mutate: func(r *Request) { r.Profile.ConfigurationID = "" }, wantErr: "configuration ID"},
		{name: "replacement scheduler", mutate: func(r *Request) { r.ReplacementPods[0].Spec.SchedulerName = "other" }, wantErr: "scheduler name"},
		{name: "replacement bound", mutate: func(r *Request) { r.ReplacementPods[0].Spec.NodeName = "source-node" }, wantErr: "already bound"},
		{name: "replacement deleting", mutate: func(r *Request) { r.ReplacementPods[0].DeletionTimestamp = &terminalTime }, wantErr: "deleting"},
		{name: "replacement terminal", mutate: func(r *Request) { r.ReplacementPods[0].Status.Phase = corev1.PodFailed }, wantErr: "terminal"},
		{name: "duplicate replacement name", mutate: func(r *Request) {
			duplicate := *r.ReplacementPods[0].DeepCopy()
			duplicate.UID = "other-uid"
			r.ReplacementPods = append(r.ReplacementPods, duplicate)
		}, wantErr: "duplicate name"},
		{name: "duplicate replacement UID", mutate: func(r *Request) {
			duplicate := *r.ReplacementPods[0].DeepCopy()
			duplicate.Name = "replacement-two"
			r.ReplacementPods = append(r.ReplacementPods, duplicate)
		}, wantErr: "duplicate UID"},
		{name: "source UID", mutate: func(r *Request) { r.SourcePods[0].UID = "" }, wantErr: "must have a UID"},
		{name: "source unbound", mutate: func(r *Request) { r.SourcePods[0].Spec.NodeName = "" }, wantErr: "not bound"},
		{name: "source node not excluded", mutate: func(r *Request) { r.ExcludedNodes = nil }, wantErr: "not explicitly excluded"},
		{name: "source UID mismatch", mutate: func(r *Request) { r.SourcePods[0].UID = "stale-uid" }, wantErr: "exactly match"},
		{name: "source spec mismatch", mutate: func(r *Request) { r.SourcePods[0].Spec.Containers[0].Image = "different.invalid/model" }, wantErr: "exactly match"},
		{name: "source node mismatch", mutate: func(r *Request) {
			r.SourcePods[0].Spec.NodeName = "other-node"
			r.ExcludedNodes = append(r.ExcludedNodes, "other-node")
			r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "other-node"}}})
		}, wantErr: "exactly match"},
		{name: "duplicate source name", mutate: func(r *Request) {
			duplicate := *r.SourcePods[0].DeepCopy()
			duplicate.UID = "other-source-uid"
			r.SourcePods = append(r.SourcePods, duplicate)
		}, wantErr: "duplicate name"},
		{name: "duplicate source UID", mutate: func(r *Request) {
			duplicate := *r.SourcePods[0].DeepCopy()
			duplicate.Name = "source-two"
			r.SourcePods = append(r.SourcePods, duplicate)
		}, wantErr: "duplicate UID"},
		{name: "replacement source collision", mutate: func(r *Request) { r.ReplacementPods[0].Name = "source" }, wantErr: "collides"},
		{name: "replacement UID collides with snapshot", mutate: func(r *Request) { r.ReplacementPods[0].UID = "source-uid" }, wantErr: "snapshot Pod"},
		{name: "duplicate snapshot Pod UID", mutate: func(r *Request) {
			pod := *r.SourcePods[0].DeepCopy()
			pod.Name = "same-uid-other-name"
			r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Object: &pod})
		}, wantErr: "duplicate Pod UID"},
		{name: "unknown bound Node", mutate: func(r *Request) {
			pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "foreign", UID: "foreign-uid"}, Spec: corev1.PodSpec{NodeName: "ghost"}}
			r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Object: &pod})
		}, wantErr: "unknown Node"},
		{name: "missing Namespace", mutate: func(r *Request) { r.ClusterObjects = append(r.ClusterObjects[:1], r.ClusterObjects[2:]...) }, wantErr: "no supplied Namespace"},
		{name: "ambiguous raw extension", mutate: func(r *Request) {
			r.ClusterObjects[0].Raw = []byte(`{"apiVersion":"v1","kind":"Node","metadata":{"name":"source-node"}}`)
		}, wantErr: "both Object and Raw"},
		{name: "empty raw extension", mutate: func(r *Request) { r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{}) }, wantErr: "empty"},
		{name: "unsupported object", mutate: func(r *Request) {
			r.ClusterObjects = append(r.ClusterObjects, rawObject(t, &corev1.ConfigMap{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ConfigMap"}, ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "config"}}))
		}, wantErr: "unsupported"},
		{name: "unknown raw field", mutate: func(r *Request) {
			r.ClusterObjects[0] = runtime.RawExtension{Raw: []byte(`{"apiVersion":"v1","kind":"Node","metadata":{"name":"source-node"},"unexpected":true}`)}
		}, wantErr: "unknown field"},
		{name: "duplicate Node", mutate: func(r *Request) {
			r.ClusterObjects = append(r.ClusterObjects, runtime.RawExtension{Object: &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "source-node"}}})
		}, wantErr: "duplicate Node"},
		{name: "nominated Pod", mutate: func(r *Request) { r.ReplacementPods[0].Status.NominatedNodeName = "source-node" }, wantErr: "nominated"},
		{name: "resource claim", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "claim"}}
		}, wantErr: "resource claims"},
		{name: "container resource claim", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Containers[0].Resources.Claims = []corev1.ResourceClaim{{Name: "claim"}}
		}, wantErr: "resource claims"},
		{name: "negative container request", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Containers[0].Resources.Requests = corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("-1")}
		}, wantErr: "negative"},
		{name: "negative container limit", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Containers[0].Resources.Limits = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-1")}
		}, wantErr: "negative"},
		{name: "negative init container request", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.InitContainers = []corev1.Container{{Name: "init", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-1")}}}}
		}, wantErr: "negative"},
		{name: "negative pod-level request", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Resources = &corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-1")}}
		}, wantErr: "negative"},
		{name: "negative overhead", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-1")}
		}, wantErr: "negative"},
		{name: "negative status allocation", mutate: func(r *Request) {
			r.SourcePods[0].Status.AllocatedResources = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("-1")}
		}, wantErr: "negative"},
		{name: "negative Node capacity", mutate: func(r *Request) {
			r.ClusterObjects[0].Object.(*corev1.Node).Status.Capacity[corev1.ResourceMemory] = resource.MustParse("-1")
		}, wantErr: "negative"},
		{name: "negative Node allocatable", mutate: func(r *Request) {
			r.ClusterObjects[0].Object.(*corev1.Node).Status.Allocatable[corev1.ResourceCPU] = resource.MustParse("-1")
		}, wantErr: "negative"},
		{name: "PVC", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "claim"}}}}
		}, wantErr: "persistent volume claim"},
		{name: "inline CSI", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{CSI: &corev1.CSIVolumeSource{Driver: "example.invalid"}}}}
		}, wantErr: "inline CSI"},
		{name: "ephemeral volume", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}}}
		}, wantErr: "ephemeral"},
		{name: "legacy translated volume", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{GCEPersistentDisk: &corev1.GCEPersistentDiskVolumeSource{PDName: "disk"}}}}
		}, wantErr: "storage driver"},
		{name: "network storage volume", mutate: func(r *Request) {
			r.ReplacementPods[0].Spec.Volumes = []corev1.Volume{{Name: "data", VolumeSource: corev1.VolumeSource{NFS: &corev1.NFSVolumeSource{Server: "example.invalid", Path: "/model"}}}}
		}, wantErr: "storage driver"},
		{name: "invalid PodGroup", mutate: func(r *Request) {
			r.ClusterObjects = append(r.ClusterObjects, rawObject(t, &schedulingv1alpha1.PodGroup{
				TypeMeta:   metav1.TypeMeta{APIVersion: "scheduling.x-k8s.io/v1alpha1", Kind: "PodGroup"},
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "gang", UID: "gang-uid"},
			}))
		}, wantErr: "minMember"},
		{name: "PodGroup without UID", mutate: func(r *Request) {
			r.ClusterObjects = append(r.ClusterObjects, rawObject(t, &schedulingv1alpha1.PodGroup{
				TypeMeta:   metav1.TypeMeta{APIVersion: "scheduling.x-k8s.io/v1alpha1", Kind: "PodGroup"},
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "gang"},
				Spec:       schedulingv1alpha1.PodGroupSpec{MinMember: 1},
			}))
		}, wantErr: "UID"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := cloneRequest(t, validRequest())
			tc.mutate(&req)
			if _, err := Validate(req); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidatePreservesTopologySelectorDependencies(t *testing.T) {
	req := validRequest()
	controller := true
	req.ReplacementPods[0].OwnerReferences = []metav1.OwnerReference{{
		APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "replacement-rs", UID: "replacement-rs-uid", Controller: &controller,
	}}
	dependencies := []runtime.Object{
		&corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-service", UID: "service-uid"}},
		&corev1.ReplicationController{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ReplicationController"}, ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-rc", UID: "rc-uid"}},
		&appsv1.ReplicaSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "ReplicaSet"}, ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "replacement-rs", UID: "replacement-rs-uid"}},
		&appsv1.StatefulSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"}, ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model-sts", UID: "sts-uid"}},
	}
	for _, dependency := range dependencies {
		req.ClusterObjects = append(req.ClusterObjects, rawObject(t, dependency))
	}
	snapshot, err := Validate(req)
	if err != nil {
		t.Fatalf("Validate() = %v, want topology selector dependencies accepted", err)
	}
	if len(snapshot.Objects) != len(req.ClusterObjects) {
		t.Fatalf("Validate() preserved %d objects, want %d", len(snapshot.Objects), len(req.ClusterObjects))
	}
}

func TestValidateRequiresReplacementControllerClosure(t *testing.T) {
	controller := true
	tests := []struct {
		name    string
		owner   metav1.OwnerReference
		object  runtime.Object
		wantErr string
	}{
		{
			name:    "missing ReplicaSet",
			owner:   metav1.OwnerReference{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: "missing", UID: "rs-uid", Controller: &controller},
			wantErr: "not supplied",
		},
		{
			name:    "mismatched StatefulSet UID",
			owner:   metav1.OwnerReference{APIVersion: "apps/v1", Kind: "StatefulSet", Name: "model", UID: "expected-uid", Controller: &controller},
			object:  &appsv1.StatefulSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "StatefulSet"}, ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model", UID: "stale-uid"}},
			wantErr: "UID",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			req.ReplacementPods[0].OwnerReferences = []metav1.OwnerReference{tc.owner}
			if tc.object != nil {
				req.ClusterObjects = append(req.ClusterObjects, rawObject(t, tc.object))
			}
			if _, err := Validate(req); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateRejectsInvalidTopologySelectorDependencies(t *testing.T) {
	tests := []struct {
		name    string
		objects []runtime.Object
		wantErr string
	}{
		{
			name: "duplicate Service",
			objects: []runtime.Object{
				&corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"}},
				&corev1.Service{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "Service"}, ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "model"}},
			},
			wantErr: "duplicate Service",
		},
		{
			name: "missing dependency Namespace",
			objects: []runtime.Object{
				&appsv1.ReplicaSet{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "ReplicaSet"}, ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "model"}},
			},
			wantErr: "no supplied Namespace",
		},
		{
			name: "empty controller name",
			objects: []runtime.Object{
				&corev1.ReplicationController{TypeMeta: metav1.TypeMeta{APIVersion: "v1", Kind: "ReplicationController"}, ObjectMeta: metav1.ObjectMeta{Namespace: "team-a"}},
			},
			wantErr: "name",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := validRequest()
			for _, object := range tc.objects {
				req.ClusterObjects = append(req.ClusterObjects, rawObject(t, object))
			}
			if _, err := Validate(req); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateAllowsDistinctUIDLessReplacements(t *testing.T) {
	req := validRequest()
	req.ReplacementPods[0].UID = ""
	second := *req.ReplacementPods[0].DeepCopy()
	second.Name = "replacement-two"
	req.ReplacementPods = append(req.ReplacementPods, second)
	if _, err := Validate(req); err != nil {
		t.Fatalf("Validate() = %v, want UID-less replacements with distinct names accepted", err)
	}
}

func TestValidateDeepCopiesSnapshot(t *testing.T) {
	req := validRequest()
	snapshot, err := Validate(req)
	if err != nil {
		t.Fatal(err)
	}
	req.ClusterObjects[0].Object.(*corev1.Node).Labels = map[string]string{"input": "changed"}
	if snapshot.Nodes["source-node"].Labels["input"] != "" {
		t.Fatal("snapshot aliases input Node")
	}
	snapshot.Pods[types.NamespacedName{Namespace: "team-a", Name: "source"}].Labels = map[string]string{"snapshot": "changed"}
	if req.ClusterObjects[2].Object.(*corev1.Pod).Labels["snapshot"] != "" {
		t.Fatal("input Pod aliases snapshot")
	}
}

func TestDecodeRejectsMalformedOrUnboundedInput(t *testing.T) {
	validPayload, err := json.Marshal(validRequest())
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name    string
		reader  *bytes.Reader
		limit   int64
		wantErr string
	}{
		{name: "oversized", reader: bytes.NewReader(validPayload), limit: int64(len(validPayload) - 1), wantErr: "exceeds"},
		{name: "trailing value", reader: bytes.NewReader(append(append([]byte{}, validPayload...), []byte(` {}`)...)), limit: int64(len(validPayload) + 3), wantErr: "trailing JSON value"},
		{name: "malformed trailing", reader: bytes.NewReader(append(append([]byte{}, validPayload...), byte('{'))), limit: int64(len(validPayload) + 1), wantErr: "trailing data"},
		{name: "unknown envelope field", reader: bytes.NewReader([]byte(`{"schemaVersion":"v1","unexpected":true}`)), limit: 128, wantErr: "unknown field"},
		{name: "nonpositive limit", reader: bytes.NewReader(validPayload), limit: 0, wantErr: "positive"},
		{name: "null is not an object", reader: bytes.NewReader([]byte(`null`)), limit: 4, wantErr: "JSON object"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := Decode(tc.reader, tc.limit); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Decode() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
	if _, err := Decode(bytes.NewReader(append(validPayload, '\n')), int64(len(validPayload)+1)); err != nil {
		t.Fatalf("Decode() rejected one bounded object with trailing whitespace: %v", err)
	}
}

func TestSharedWireFixture(t *testing.T) {
	payload, err := os.ReadFile("../../scheduling/testdata/simulator-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var fixture struct {
		Request json.RawMessage `json:"request"`
		Result  Result          `json:"result"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&fixture); err != nil {
		t.Fatal(err)
	}
	req, err := Decode(bytes.NewReader(fixture.Request), int64(len(fixture.Request)))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Validate(req); err != nil {
		t.Fatalf("fixture request rejected: %v", err)
	}
	if len(req.ReplacementPods) != 2 || req.ReplacementPods[0].UID == "" || req.ReplacementPods[1].UID != "" {
		t.Fatalf("fixture does not cover explicit and empty replacement UIDs: %+v", req.ReplacementPods)
	}
	if fixture.Result.Placements[0].Pod.UID == "" || fixture.Result.Placements[1].Pod.UID != "" {
		t.Fatalf("fixture placement identities do not mirror request UIDs: %+v", fixture.Result.Placements)
	}
	roundTrip, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Decode(bytes.NewReader(roundTrip), int64(len(roundTrip))); err != nil {
		t.Fatalf("fixture request did not round trip: %v", err)
	}
}

func rawObject(t *testing.T, object any) runtime.RawExtension {
	t.Helper()
	raw, err := json.Marshal(object)
	if err != nil {
		t.Fatal(err)
	}
	return runtime.RawExtension{Raw: raw}
}

func cloneRequest(t *testing.T, request Request) Request {
	t.Helper()
	copy := request
	copy.ReplacementPods = make([]corev1.Pod, len(request.ReplacementPods))
	for i := range request.ReplacementPods {
		copy.ReplacementPods[i] = *request.ReplacementPods[i].DeepCopy()
	}
	copy.SourcePods = make([]corev1.Pod, len(request.SourcePods))
	for i := range request.SourcePods {
		copy.SourcePods[i] = *request.SourcePods[i].DeepCopy()
	}
	copy.ClusterObjects = make([]runtime.RawExtension, len(request.ClusterObjects))
	for i := range request.ClusterObjects {
		copy.ClusterObjects[i] = *request.ClusterObjects[i].DeepCopy()
	}
	copy.ExcludedNodes = append([]string(nil), request.ExcludedNodes...)
	return copy
}

func validRequest() Request {
	source := corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "source", UID: types.UID("source-uid")},
		Spec: corev1.PodSpec{
			NodeName: "source-node",
			Containers: []corev1.Container{{
				Name: "model", Image: "example.invalid/model:latest",
				Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
					corev1.ResourceCPU: resource.MustParse("2"),
				}},
			}},
		},
	}
	return Request{
		SchemaVersion: SchemaVersion,
		RequestID:     "request-1",
		Profile: ProfileIdentity{
			SchedulerName: "ome-scheduler", Backend: "ome",
			SchedulerVersion: "v1.35.4", ConfigurationID: "sha256:config",
		},
		ReplacementPods: []corev1.Pod{{
			ObjectMeta: metav1.ObjectMeta{Namespace: "team-a", Name: "replacement", UID: types.UID("replacement-uid")},
			Spec: corev1.PodSpec{
				SchedulerName: "ome-scheduler",
				Containers:    []corev1.Container{{Name: "model", Image: "example.invalid/model:latest"}},
			},
		}},
		SourcePods: []corev1.Pod{source},
		ClusterObjects: []runtime.RawExtension{
			{Object: &corev1.Node{
				ObjectMeta: metav1.ObjectMeta{Name: "source-node"},
				Status: corev1.NodeStatus{Capacity: corev1.ResourceList{
					corev1.ResourceCPU:                    resource.MustParse("8"),
					corev1.ResourceMemory:                 resource.MustParse("32Gi"),
					corev1.ResourcePods:                   resource.MustParse("32"),
					corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
				}, Allocatable: corev1.ResourceList{
					corev1.ResourceCPU:                    resource.MustParse("8"),
					corev1.ResourceMemory:                 resource.MustParse("32Gi"),
					corev1.ResourcePods:                   resource.MustParse("32"),
					corev1.ResourceName("nvidia.com/gpu"): resource.MustParse("1"),
				}},
			}},
			{Object: &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team-a"}}},
			{Object: source.DeepCopy()},
		},
		SnapshotID:    "snapshot-1",
		SnapshotTime:  metav1.NewTime(time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)),
		ExcludedNodes: []string{"source-node"},
	}
}
