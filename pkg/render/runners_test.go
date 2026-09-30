package render

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestRunnersSinglePod(t *testing.T) {
	primary := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example.com/engine:v1"}}}
	meta := metav1.ObjectMeta{
		Name:        "isvc-engine",
		Namespace:   "team-a",
		Labels:      map[string]string{"rendered": "yes"},
		Annotations: map[string]string{"note": "rendered"},
	}

	got := Runners(Templates{ObjectMeta: meta, Primary: primary}, nil)

	want := []v1beta1.Runner{{
		Name: v1beta1.RunnerNameDefault,
		Size: 1,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{"rendered": "yes"},
				Annotations: map[string]string{"note": "rendered"},
			},
			Spec: *primary,
		},
	}}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("single-pod runners mismatch (-want +got):\n%s", diff)
	}
}

func TestRunnersLeaderWorker(t *testing.T) {
	leader := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example.com/engine:v1"}}}
	worker := &corev1.PodSpec{Containers: []corev1.Container{{Name: "worker", Image: "example.com/engine:v1"}}}
	meta := metav1.ObjectMeta{Labels: map[string]string{"role": "engine"}}

	got := Runners(Templates{ObjectMeta: meta, Primary: leader, Worker: worker, WorkerSize: 3, MultiPod: true}, nil)

	tmplMeta := metav1.ObjectMeta{Labels: map[string]string{"role": "engine"}}
	want := []v1beta1.Runner{
		{
			Name:     v1beta1.RunnerNameLeader,
			Size:     1,
			Template: corev1.PodTemplateSpec{ObjectMeta: tmplMeta, Spec: *leader},
		},
		{
			Name:     v1beta1.RunnerNameWorker,
			Size:     3,
			Template: corev1.PodTemplateSpec{ObjectMeta: tmplMeta, Spec: *worker},
		},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("leader/worker runners mismatch (-want +got):\n%s", diff)
	}

	// A multi-pod component without a worker template yields only the leader.
	leaderOnly := Runners(Templates{ObjectMeta: meta, Primary: leader, MultiPod: true}, nil)
	if diff := cmp.Diff(want[:1], leaderOnly); diff != "" {
		t.Fatalf("leader-only runners mismatch (-want +got):\n%s", diff)
	}
}

func TestTemplateObjectMetaComponentOverridesAndIdentityDropped(t *testing.T) {
	meta := metav1.ObjectMeta{
		Name:            "isvc-engine",
		Namespace:       "team-a",
		GenerateName:    "isvc-engine-",
		ResourceVersion: "7",
		UID:             "owner-uid",
		Labels:          map[string]string{"shared": "rendered", "only-rendered": "a"},
		Annotations:     map[string]string{"shared": "rendered", "only-rendered": "b"},
	}
	ext := &v1beta1.ComponentExtensionSpec{
		Labels:      map[string]string{"shared": "component", "only-component": "c"},
		Annotations: map[string]string{"shared": "component", "only-component": "d"},
	}

	got := TemplateObjectMeta(meta, ext)

	want := metav1.ObjectMeta{
		Labels:      map[string]string{"shared": "component", "only-rendered": "a", "only-component": "c"},
		Annotations: map[string]string{"shared": "component", "only-rendered": "b", "only-component": "d"},
	}
	if diff := cmp.Diff(want, got); diff != "" {
		t.Fatalf("template metadata mismatch (-want +got):\n%s", diff)
	}
	if meta.Labels["shared"] != "rendered" || meta.Annotations["shared"] != "rendered" {
		t.Fatalf("input metadata mutated: %v %v", meta.Labels, meta.Annotations)
	}

	// Absent maps stay absent; a component with no declarations adds nothing.
	empty := TemplateObjectMeta(metav1.ObjectMeta{Name: "isvc-engine"}, &v1beta1.ComponentExtensionSpec{})
	if empty.Labels != nil || empty.Annotations != nil {
		t.Fatalf("expected nil labels and annotations, got %v %v", empty.Labels, empty.Annotations)
	}

	// Absent maps are allocated when the component declares keys.
	allocated := TemplateObjectMeta(metav1.ObjectMeta{Name: "isvc-engine"}, ext)
	want = metav1.ObjectMeta{
		Labels:      map[string]string{"shared": "component", "only-component": "c"},
		Annotations: map[string]string{"shared": "component", "only-component": "d"},
	}
	if diff := cmp.Diff(want, allocated); diff != "" {
		t.Fatalf("template metadata from absent maps mismatch (-want +got):\n%s", diff)
	}
}
