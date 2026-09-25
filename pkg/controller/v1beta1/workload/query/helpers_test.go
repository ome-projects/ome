package query

import (
	"context"
	"fmt"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func newDiscoverTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
}

// testPodLabels reproduces the label set Render stamps on every
// OMENative pod. Duplicated here (rather than importing render.go's
// podLabels) because that would create a cycle: render.go imports
// query/.
func testPodLabels(isvc string, component workload.ComponentType, idx int32, runner string, incarnation int64) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: isvc,
		constants.OMEComponentLabel:           string(component),
		LabelInstanceIdx:                      fmt.Sprintf("%d", idx),
		LabelInstanceIncarnation:              fmt.Sprintf("%d", incarnation),
		LabelRunner:                           runner,
		LabelManagedBy:                        ManagedByOMENative,
	}
}

func newDiscoverPod(name string, instanceIdx int32, incarnation int64, withDeletionTimestamp bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels:    testPodLabels("isvc", workload.ComponentEngine, instanceIdx, "default", incarnation),
		},
	}
	if withDeletionTimestamp {
		t := metav1.Now()
		pod.DeletionTimestamp = &t
		// fake client requires finalizers on objects with DeletionTimestamp
		pod.Finalizers = []string{"keep"}
	}
	return pod
}

func podNames(pods []*corev1.Pod) []string {
	out := make([]string, 0, len(pods))
	for _, p := range pods {
		out = append(out, p.Name)
	}
	return out
}

// newIndexedDiscoverTestClient mirrors newDiscoverTestClient but also
// registers the OMENativePodIndexField on Pods — the same index
// cmd/manager installs on the manager cache. Lets the test exercise the
// MatchingFields fast path in ListOMENativePodsByName.
func newIndexedDiscoverTestClient(t *testing.T, objs ...client.Object) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatalf("add corev1: %v", err)
	}
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add v1beta1: %v", err)
	}
	return fake.NewClientBuilder().
		WithScheme(scheme).
		WithIndex(&corev1.Pod{}, OMENativePodIndexField, OMENativePodIndexExtractor).
		WithObjects(objs...).
		Build()
}

// pod with arbitrary isvc/component labels — used to prove the index
// excludes pods from a different (isvc, component) tuple.
func newDiscoverPodFor(name, isvc string, component workload.ComponentType) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: "ns",
			Labels:    testPodLabels(isvc, component, 0, "default", 1),
		},
	}
}

func assertPodSet(t *testing.T, label string, pods []*corev1.Pod, want map[string]bool) {
	t.Helper()
	if len(pods) != len(want) {
		t.Fatalf("%s: got %v, want keys %v", label, podNames(pods), want)
	}
	for _, p := range pods {
		if !want[p.Name] {
			t.Errorf("%s: unexpected pod %q in result set %v", label, p.Name, podNames(pods))
		}
	}
}

// probeCountingReader wraps a client.Reader and records, per PodList List
// call, whether the caller passed a MatchingFields option (the index
// probe). Lets a test prove the live-reader path skips the doomed probe.
type probeCountingReader struct {
	client.Reader
	podListCalls       int
	matchingFieldsHits int
}

func (r *probeCountingReader) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		r.podListCalls++
		for _, o := range opts {
			if _, ok := o.(client.MatchingFields); ok {
				r.matchingFieldsHits++
			}
		}
	}
	return r.Reader.List(ctx, list, opts...)
}
