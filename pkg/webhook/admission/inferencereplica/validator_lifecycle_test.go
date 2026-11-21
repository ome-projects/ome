package inferencereplica

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// firstShapeRefusal runs the shape checks every writer passes, in their
// order, and returns the first refusal.
func firstShapeRefusal(ir *v1beta1.InferenceReplica) error {
	for _, check := range shapeChecks {
		if err := check(ir); err != nil {
			return err
		}
	}
	return nil
}

// TestShapeChecks_LeaderGoneGracePeriodMustBeAboveZero: the shape checks
// refuse a replica whose lifecycle.leaderGoneGracePeriod is zero or negative
// and admit an unset field and a value above zero. A single-pod replica may
// carry a value above zero: it has no leader, so nothing reads the field
// there.
func TestShapeChecks_LeaderGoneGracePeriodMustBeAboveZero(t *testing.T) {
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{
		Containers: []corev1.Container{{Name: "ome-container", Image: "registry.example.com/runtime:v1"}},
	}}
	gang := []v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: template},
		{Name: v1beta1.RunnerNameWorker, Size: 3, Template: template},
	}
	single := []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1, Template: template}}
	with := func(d time.Duration) *v1beta1.LifecycleSpec {
		return &v1beta1.LifecycleSpec{LeaderGoneGracePeriod: &metav1.Duration{Duration: d}}
	}
	cases := []struct {
		name      string
		runners   []v1beta1.Runner
		lifecycle *v1beta1.LifecycleSpec
		refused   bool
	}{
		{name: "a gang with no lifecycle block", runners: gang},
		{name: "a gang with the field unset", runners: gang, lifecycle: &v1beta1.LifecycleSpec{}},
		{name: "a gang with 30 s", runners: gang, lifecycle: with(30 * time.Second)},
		{name: "a gang with zero", runners: gang, lifecycle: with(0), refused: true},
		{name: "a gang with a negative value", runners: gang, lifecycle: with(-5 * time.Second), refused: true},
		{name: "a single-pod replica with 30 s", runners: single, lifecycle: with(30 * time.Second)},
		{name: "a single-pod replica with zero", runners: single, lifecycle: with(0), refused: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ir := &v1beta1.InferenceReplica{Spec: v1beta1.InferenceReplicaSpec{Runners: tc.runners, Lifecycle: tc.lifecycle}}
			err := firstShapeRefusal(ir)
			if !tc.refused {
				if err != nil {
					t.Fatalf("want the replica to pass the shape checks, got %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), "spec.lifecycle.leaderGoneGracePeriod is") || !strings.Contains(err.Error(), "must be above 0s") {
				t.Fatalf("want a refusal that names spec.lifecycle.leaderGoneGracePeriod, got %v", err)
			}
		})
	}
	if err := validateIRLifecycle(nil); err != nil {
		t.Fatalf("no replica, no refusal: %v", err)
	}
}
