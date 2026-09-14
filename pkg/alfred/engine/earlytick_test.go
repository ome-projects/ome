package engine

import (
	"testing"
	"time"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/alfred/config"
)

func TestEarlyTickMaintenanceMatchChanges(t *testing.T) {
	store := config.NewStore()
	_, err := store.Update([]byte(`
schemaVersion: 1
earlyTickOn: [NodeMaintenanceChange]
policies:
  nodeHealth:
    maintenance:
      triggers:
      - name: label
        label: {key: ops.example/patch, value: ""}
      - name: taint
        taint: {key: ops.example/patch, value: planned, effect: NoSchedule}
      - name: condition
        condition: {type: Patching, status: "True"}
`))
	if err != nil {
		t.Fatal(err)
	}
	ticker := &EarlyTicker{Store: store, Log: logr.Discard(), C: make(chan struct{}, 1)}
	base := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}}
	label := base.DeepCopy()
	label.Labels = map[string]string{"ops.example/patch": ""}
	labelWrong := base.DeepCopy()
	labelWrong.Labels = map[string]string{"ops.example/patch": "wrong"}
	taint := base.DeepCopy()
	taint.Spec.Taints = []corev1.Taint{{Key: "ops.example/patch", Value: "planned", Effect: corev1.TaintEffectNoSchedule}}
	taintWrong := taint.DeepCopy()
	taintWrong.Spec.Taints[0].Effect = corev1.TaintEffectNoExecute
	condition := base.DeepCopy()
	condition.Status.Conditions = []corev1.NodeCondition{{Type: "Patching", Status: corev1.ConditionTrue}}
	heartbeat := condition.DeepCopy()
	heartbeat.Status.Conditions[0].LastHeartbeatTime = metav1.Now()
	heartbeat.Status.Conditions[0].LastTransitionTime = metav1.NewTime(time.Now())
	conditionFalse := condition.DeepCopy()
	conditionFalse.Status.Conditions[0].Status = corev1.ConditionFalse
	unrelated := base.DeepCopy()
	unrelated.Labels = map[string]string{"unrelated": "value"}
	unrelated.Spec.Taints = []corev1.Taint{{Key: "unrelated", Effect: corev1.TaintEffectNoSchedule}}
	tests := []struct {
		name      string
		old, next *corev1.Node
		want      bool
	}{
		{"label match", base, label, true},
		{"label clear", label, base, true},
		{"label stops matching", label, labelWrong, true},
		{"label mismatch", base, labelWrong, false},
		{"taint match", base, taint, true},
		{"taint clear", taint, base, true},
		{"taint stops matching", taint, taintWrong, true},
		{"taint mismatch", base, taintWrong, false},
		{"condition match", base, condition, true},
		{"condition clear", condition, conditionFalse, true},
		{"condition removed", condition, base, true},
		{"condition mismatch", base, conditionFalse, false},
		{"heartbeat only", condition, heartbeat, false},
		{"unrelated metadata", base, unrelated, false},
		{"different evidence remains requested", label, taint, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ticker.observe(tt.old, tt.next)
			got := false
			select {
			case <-ticker.C:
				got = true
			default:
			}
			if got != tt.want {
				t.Fatalf("early tick=%t, want=%t", got, tt.want)
			}
		})
	}
	ticker.observe(base, label)
	ticker.observe(base, taint)
	ticker.observe(base, condition)
	if len(ticker.C) != 1 {
		t.Fatalf("signals not coalesced: %d", len(ticker.C))
	}
	<-ticker.C
	if _, err := store.Update([]byte("schemaVersion: 1\nearlyTickOn: []")); err != nil {
		t.Fatal(err)
	}
	ticker.observe(base, label)
	ticker.observe(base, condition)
	if len(ticker.C) != 0 {
		t.Fatal("explicit empty earlyTickOn did not disable events")
	}
	if _, err := store.Update([]byte("schemaVersion: 1\npolicies:\n  nodeHealth:\n    maintenance:\n      triggers: [{name: new-label, label: {key: ops.example/patch}}]")); err != nil {
		t.Fatal(err)
	}
	ticker.observe(base, labelWrong)
	if len(ticker.C) != 1 {
		t.Fatal("hot reload did not apply new maintenance matcher and default event")
	}
	<-ticker.C
	if _, err := store.Update([]byte("schemaVersion: 1")); err != nil {
		t.Fatal(err)
	}
	ticker.observe(base, label)
	if len(ticker.C) != 0 {
		t.Fatal("maintenance rules must be disabled by default")
	}
}

func TestEarlyTickHealthTransitionTimeChanges(t *testing.T) {
	ticker := &EarlyTicker{Store: config.NewStore(), Log: logr.Discard(), C: make(chan struct{}, 1)}
	old := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "node"}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: "GpuUnhealthy", Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(time.Unix(1, 0))}}}}
	next := old.DeepCopy()
	next.Status.Conditions[0].LastTransitionTime = metav1.NewTime(time.Unix(2, 0))
	ticker.observe(old, next)
	if len(ticker.C) != 1 {
		t.Fatal("recovery boundary change did not wake decision loop")
	}
}
