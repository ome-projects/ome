package v1beta1convert

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestTemplateHash_LeaderGoneGracePeriodIsOutsideTheRevision: a replica's
// revision is the hash of its pod templates. Setting, changing or removing
// lifecycle.leaderGoneGracePeriod leaves that hash as it is, so an edit of
// the field starts no rollout and restarts no pod. A new image moves the
// hash, which shows the hash reads the templates at all.
func TestTemplateHash_LeaderGoneGracePeriodIsOutsideTheRevision(t *testing.T) {
	hashOf := func(image string, grace time.Duration) string {
		template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{
			Containers: []corev1.Container{{Name: "ome-container", Image: image}},
		}}
		replica := &v1beta1.InferenceReplica{Spec: v1beta1.InferenceReplicaSpec{
			Runners: []v1beta1.Runner{
				{Name: v1beta1.RunnerNameLeader, Size: 1, Template: template},
				{Name: v1beta1.RunnerNameWorker, Size: 3, Template: template},
			},
		}}
		if grace > 0 {
			replica.Spec.Lifecycle = &v1beta1.LifecycleSpec{LeaderGoneGracePeriod: &metav1.Duration{Duration: grace}}
		}
		return TemplateHash(replica, replica.Spec.Runners)
	}
	const image = "registry.example.com/runtime:v1"
	unset := hashOf(image, 0)
	if unset == "" {
		t.Fatalf("a gang replica with pod templates must hash to something")
	}
	for _, grace := range []time.Duration{30 * time.Second, 45 * time.Second} {
		if got := hashOf(image, grace); got != unset {
			t.Errorf("with leaderGoneGracePeriod at %s the hash is %s, want %s as with the field unset", grace, got, unset)
		}
	}
	if hashOf("registry.example.com/runtime:v2", 0) == unset {
		t.Fatalf("a new image must move the hash")
	}
}
