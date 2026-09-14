package snapshot

import (
	"context"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/alfred/config"
)

func TestObserveNodeMaintenance(t *testing.T) {
	empty, planned := "", "planned"
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"patch": "planned", "empty": ""}},
		Spec:       corev1.NodeSpec{Taints: []corev1.Taint{{Key: "patch", Value: "planned", Effect: corev1.TaintEffectNoSchedule}, {Key: "empty", Effect: corev1.TaintEffectNoExecute}}},
		Status:     corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: "Patching", Status: corev1.ConditionTrue}}},
	}
	tests := []struct {
		name    string
		trigger config.MaintenanceTrigger
		want    bool
	}{
		{"condition", config.MaintenanceTrigger{Condition: &config.MaintenanceCondition{Type: "Patching", Status: corev1.ConditionTrue}}, true},
		{"wrong condition type", config.MaintenanceTrigger{Condition: &config.MaintenanceCondition{Type: "Other", Status: corev1.ConditionTrue}}, false},
		{"wrong condition status", config.MaintenanceTrigger{Condition: &config.MaintenanceCondition{Type: "Patching", Status: corev1.ConditionFalse}}, false},
		{"label presence", config.MaintenanceTrigger{Label: &config.MaintenanceLabel{Key: "patch"}}, true},
		{"missing label", config.MaintenanceTrigger{Label: &config.MaintenanceLabel{Key: "missing"}}, false},
		{"label exact", config.MaintenanceTrigger{Label: &config.MaintenanceLabel{Key: "patch", Value: &planned}}, true},
		{"label exact empty", config.MaintenanceTrigger{Label: &config.MaintenanceLabel{Key: "empty", Value: &empty}}, true},
		{"label empty mismatch", config.MaintenanceTrigger{Label: &config.MaintenanceLabel{Key: "patch", Value: &empty}}, false},
		{"missing label not empty", config.MaintenanceTrigger{Label: &config.MaintenanceLabel{Key: "missing", Value: &empty}}, false},
		{"taint presence", config.MaintenanceTrigger{Taint: &config.MaintenanceTaint{Key: "patch"}}, true},
		{"missing taint", config.MaintenanceTrigger{Taint: &config.MaintenanceTaint{Key: "missing"}}, false},
		{"taint exact", config.MaintenanceTrigger{Taint: &config.MaintenanceTaint{Key: "patch", Value: &planned, Effect: corev1.TaintEffectNoSchedule}}, true},
		{"taint empty", config.MaintenanceTrigger{Taint: &config.MaintenanceTaint{Key: "empty", Value: &empty}}, true},
		{"taint value mismatch", config.MaintenanceTrigger{Taint: &config.MaintenanceTaint{Key: "patch", Value: &empty}}, false},
		{"taint effect mismatch", config.MaintenanceTrigger{Taint: &config.MaintenanceTaint{Key: "patch", Effect: corev1.TaintEffectNoExecute}}, false},
		{"fields cannot span taints", config.MaintenanceTrigger{Taint: &config.MaintenanceTaint{Key: "empty", Value: &planned, Effect: corev1.TaintEffectNoSchedule}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.trigger.Name = "maintenance"
			got := ObserveNodeMaintenance(node, []config.MaintenanceTrigger{tt.trigger})
			if got.Requested != tt.want {
				t.Fatalf("observation=%+v, want requested=%t", got, tt.want)
			}
			if tt.want && !reflect.DeepEqual(got.Triggers, []string{"maintenance"}) {
				t.Fatalf("triggers=%v", got.Triggers)
			}
		})
	}
	triggers := []config.MaintenanceTrigger{
		{Name: "z-label", Label: &config.MaintenanceLabel{Key: "patch"}},
		{Name: "a-condition", Condition: &config.MaintenanceCondition{Type: "Patching", Status: corev1.ConditionTrue}},
		{Name: "unmatched", Label: &config.MaintenanceLabel{Key: "missing"}},
	}
	got := ObserveNodeMaintenance(node, triggers)
	if !got.Requested || !reflect.DeepEqual(got.Triggers, []string{"a-condition", "z-label"}) {
		t.Fatalf("combined observation=%+v", got)
	}
	if got := ObserveNodeMaintenance(node, nil); got.Requested || len(got.Triggers) != 0 {
		t.Fatalf("disabled observation=%+v", got)
	}
	node.Labels = nil
	node.Status.Conditions = nil
	if got := ObserveNodeMaintenance(node, triggers); got.Requested || len(got.Triggers) != 0 {
		t.Fatalf("cleared observation=%+v", got)
	}
}

func TestNodeUnavailableAsTarget(t *testing.T) {
	var absent *Node
	if !absent.UnavailableAsTarget() {
		t.Fatal("absent node must be unavailable")
	}
	for _, n := range []Node{
		{Health: NodeHealthObservation{State: NodeHealthUnhealthy}},
		{Health: NodeHealthObservation{State: NodeHealthUnknown}},
		{Health: NodeHealthObservation{State: NodeHealthSuspect}},
		{Maintenance: NodeMaintenanceObservation{Requested: true}},
		{Cordoned: true}, {ScaleDownMarked: true},
	} {
		if !n.UnavailableAsTarget() {
			t.Fatalf("available: %+v", n)
		}
	}
	for _, n := range []Node{{}, {Health: NodeHealthObservation{State: NodeHealthClear}}, {ScaleDownDisabled: true}, {Preemptible: true}} {
		if n.UnavailableAsTarget() {
			t.Fatalf("unavailable: %+v", n)
		}
	}
}

func TestBuildMaintenanceDoesNotChangeHealth(t *testing.T) {
	conditions := []corev1.NodeCondition{{Type: "Patching", Status: corev1.ConditionTrue}}
	triggers := []config.MaintenanceTrigger{{Name: "patch", Condition: &config.MaintenanceCondition{Type: "Patching", Status: corev1.ConditionTrue}}}
	node := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "patching", UID: types.UID("uid-1")}, Status: corev1.NodeStatus{Conditions: conditions}}
	opts := Options{MaintenanceTriggers: triggers, Now: func() time.Time { return buildNow }}
	got := buildNode(node, &opts)
	if got.UID != node.UID || got.Health.State != NodeHealthClear || !got.Maintenance.Requested || !got.UnavailableAsTarget() {
		t.Fatalf("patching node=%+v", got)
	}
	node.Status.Conditions[0].Status = corev1.ConditionFalse
	node.Status.Conditions[0].LastTransitionTime = metav1.NewTime(buildNow)
	got = buildNode(node, &opts)
	if got.Health.State != NodeHealthClear || got.Maintenance.Requested || got.UnavailableAsTarget() {
		t.Fatalf("cleared node=%+v", got)
	}
}

func TestBuildNodeHealthUsesSnapshotClock(t *testing.T) {
	clockCalls := 0
	client := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "recovering"}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: "GpuUnhealthy", Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(buildNow.Add(-29 * time.Minute))}}}}).Build()
	snap, err := Build(context.Background(), client, Options{Now: func() time.Time { clockCalls++; return buildNow.Add(time.Duration(clockCalls-1) * time.Hour) }})
	if err != nil {
		t.Fatal(err)
	}
	if clockCalls != 1 || snap.Nodes["recovering"].Health.State != NodeHealthSuspect {
		t.Fatalf("clock calls=%d health=%+v", clockCalls, snap.Nodes["recovering"].Health)
	}
}
