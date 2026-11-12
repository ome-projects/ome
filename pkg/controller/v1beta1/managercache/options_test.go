package managercache

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/cache"

	"sigs.k8s.io/ome/pkg/constants"
)

var writtenBy = []metav1.ManagedFieldsEntry{{Manager: "kubelet", Operation: metav1.ManagedFieldsOperationUpdate}}

func TestOptions_PodCacheHoldsOnlyPodsWithTheInferenceServiceLabel(t *testing.T) {
	opts, err := Options()
	if err != nil {
		t.Fatal(err)
	}
	if len(opts.ByObject) != 1 {
		t.Fatalf("ByObject has %d entries, want one for pods", len(opts.ByObject))
	}
	selector := podEntry(t, opts).Label
	if selector == nil {
		t.Fatal("the pod entry has no label selector; the cache would hold every pod in the cluster")
	}
	if !selector.Matches(labels.Set{constants.InferenceServicePodLabelKey: "example-service"}) {
		t.Errorf("selector %q does not match a pod carrying the label key", selector)
	}
	if selector.Matches(labels.Set{"app": "example"}) {
		t.Errorf("selector %q matches a pod without the label key", selector)
	}
	if selector.Matches(labels.Set{}) {
		t.Errorf("selector %q matches a pod with no labels", selector)
	}
}

func TestOptions_ACachedPodKeepsWhatTheControllersReadAndDropsTheRest(t *testing.T) {
	opts, err := Options()
	if err != nil {
		t.Fatal(err)
	}
	transform := podEntry(t, opts).Transform
	if transform == nil {
		t.Fatal("the pod entry has no transform; a cached pod would keep every field")
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "example", ManagedFields: writtenBy},
		Spec: corev1.PodSpec{
			NodeName: "node-a",
			Volumes:  []corev1.Volume{{Name: "scratch"}},
			Containers: []corev1.Container{{
				Name:  "serving",
				Image: "serving:v1",
				Env:   []corev1.EnvVar{{Name: "MODEL_PATH", Value: "/models/example"}},
			}},
		},
	}
	cached, err := transform(pod)
	if err != nil {
		t.Fatal(err)
	}
	got := cached.(*corev1.Pod)
	if got.Spec.NodeName != "node-a" || len(got.Spec.Containers) != 1 || got.Spec.Containers[0].Image != "serving:v1" {
		t.Errorf("the cache lost a part the controllers read: node %q, containers %+v", got.Spec.NodeName, got.Spec.Containers)
	}
	if len(got.Spec.Containers[0].Env) != 0 {
		t.Errorf("the cache keeps the container environment %+v; the controllers never read it", got.Spec.Containers[0].Env)
	}
	if len(got.Spec.Volumes) != 0 {
		t.Errorf("the cache keeps the volumes %+v; the controllers never read them", got.Spec.Volumes)
	}
	if len(got.ManagedFields) != 0 {
		t.Errorf("the cache keeps %d managedFields entries, want none", len(got.ManagedFields))
	}
}

func TestOptions_EveryCachedObjectLosesItsManagedFields(t *testing.T) {
	opts, err := Options()
	if err != nil {
		t.Fatal(err)
	}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "example", ManagedFields: writtenBy}}
	configMap := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "example", ManagedFields: writtenBy}}
	for _, object := range []any{pod, configMap} {
		cached, err := opts.DefaultTransform(object)
		if err != nil {
			t.Fatal(err)
		}
		if fields := cached.(interface {
			GetManagedFields() []metav1.ManagedFieldsEntry
		}).GetManagedFields(); len(fields) != 0 {
			t.Errorf("%T keeps %d managedFields entries in the cache, want none", object, len(fields))
		}
	}
}

// podEntry is the pod entry of opts.ByObject.
func podEntry(t *testing.T, opts cache.Options) cache.ByObject {
	t.Helper()
	for object, byObject := range opts.ByObject {
		if _, ok := object.(*corev1.Pod); ok {
			return byObject
		}
	}
	t.Fatal("no pod entry in ByObject")
	return cache.ByObject{}
}
