package render

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

func TestRunnersSinglePod(t *testing.T) {
	primary := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main", Image: "example.com/engine:v1"}}}
	// The template keeps OME's own annotation; the service's own key stays
	// on the component metadata and off the pods.
	meta := metav1.ObjectMeta{
		Name:        "isvc-engine",
		Namespace:   "team-a",
		Labels:      map[string]string{"rendered": "yes"},
		Annotations: map[string]string{"ome.io/base-model-name": "model-a", "example.com/owner": "team-a"},
	}

	got := Runners(Templates{ObjectMeta: meta, Primary: primary}, nil)

	want := []v1beta1.Runner{{
		Name: v1beta1.RunnerNameDefault,
		Size: 1,
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{
				Labels:      map[string]string{"rendered": "yes"},
				Annotations: map[string]string{"ome.io/base-model-name": "model-a"},
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
		Annotations:     map[string]string{"shared": "rendered", "ome.io/only-rendered": "b", "example.com/only-service": "s"},
	}
	ext := &v1beta1.ComponentExtensionSpec{
		Labels:      map[string]string{"shared": "component", "only-component": "c"},
		Annotations: map[string]string{"shared": "component", "only-component": "d"},
	}

	got := TemplateObjectMeta(meta, ext)

	// A declared key takes the component's value, OME's own key is kept, and
	// the service's own key is left off the template.
	want := metav1.ObjectMeta{
		Labels:      map[string]string{"shared": "component", "only-rendered": "a", "only-component": "c"},
		Annotations: map[string]string{"shared": "component", "ome.io/only-rendered": "b", "only-component": "d"},
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

// TestRunnerTemplateCarriesDeclaredAndOMEAnnotationsOnly renders an engine
// whose service carries four kinds of annotation and reads what reaches the
// runner template: the key the Component declares, OME's own key and a key
// under a prefix OME lists as pod-level configuration do; a tool's key written
// on the service does not, while the component's object metadata, which its
// Service is built from, still carries it.
func TestRunnerTemplateCarriesDeclaredAndOMEAnnotationsOnly(t *testing.T) {
	piece := &Piece{Client: newRenderClient(t), InferenceServiceConfig: &controllerconfig.InferenceServicesConfig{}}
	service := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "service", Namespace: "team-a",
		Annotations: map[string]string{
			"editor.example.com/write-counter": "1",
			"ome.io/local-queue":               "queue-a",
			"prometheus.io/scrape":             "true",
			"example.com/declared":             "service",
		},
	}}
	specs := Specs{Engine: &v1beta1.EngineSpec{
		ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: map[string]string{"example.com/declared": "component"}},
		Runner:                 &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "example.com/serving:v1"}},
	}}
	rendered, err := RenderComponent(t.Context(), piece, service, specs, v1beta1.EngineComponent)
	if err != nil {
		t.Fatalf("render engine: %v", err)
	}
	runners := rendered.Runners()
	if len(runners) != 1 {
		t.Fatalf("runners = %d, want 1", len(runners))
	}
	got := runners[0].Template.Annotations
	for key, want := range map[string]string{
		"ome.io/local-queue":   "queue-a",
		"prometheus.io/scrape": "true",
		"example.com/declared": "component",
	} {
		if got[key] != want {
			t.Errorf("template annotation %s = %q, want %q", key, got[key], want)
		}
	}
	if value, ok := got["editor.example.com/write-counter"]; ok {
		t.Errorf("a service annotation the Component does not declare reached the pod template: editor.example.com/write-counter=%q", value)
	}
	if rendered.ObjectMeta.Annotations["editor.example.com/write-counter"] != "1" {
		t.Errorf("the component's object metadata must keep the service annotation, got %v", rendered.ObjectMeta.Annotations)
	}
}

// TestAnnotationReachesPods checks the three kinds of annotation a pod template
// carries and the kinds it does not.
func TestAnnotationReachesPods(t *testing.T) {
	declared := map[string]string{"note": "declared"}
	for key, want := range map[string]bool{
		"note":                    true,
		"ome.io/local-queue":      true,
		"rdma.ome.io/auto-inject": true,
		"internal.ome.io/in-place-image-transition":        true,
		"prometheus.io/scrape":                             true,
		"k8s.grafana.com/scrape":                           true,
		"networking.gke.io/interfaces":                     true,
		"editor.example.com/write-counter":                 false,
		"delivery.example.com/tracking-id":                 false,
		"kubectl.kubernetes.io/last-applied-configuration": false,
		"notome.io/key":                                    false,
		"unlisted":                                         false,
	} {
		if got := AnnotationReachesPods(key, declared); got != want {
			t.Errorf("AnnotationReachesPods(%q) = %v, want %v", key, got, want)
		}
	}
	if AnnotationReachesPods("note", nil) {
		t.Errorf("a bare key reaches the pods only when the Component declares it")
	}
}
