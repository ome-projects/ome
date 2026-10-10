package snapshot

import (
	"context"
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/constants"
)

func TestBuildRecordsTPUSliceProvisioning(t *testing.T) {
	tpuNode := &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "tpu-1", Labels: map[string]string{
		"cloud.google.com/gke-tpu-partition-2x2x2-id": "p-1",
	}}}
	tpuPod := omePod("prod", "svc-t-engine-0", "tpu-1", "svc-t", "engine", 0, true)
	tpuPod.Annotations = map[string]string{constants.TPUSliceProvisioningAnnotationKey: "true"}
	tpuPod.Spec.NodeSelector = map[string]string{"cloud.google.com/gke-tpu-topology": "2x2x2", "cloud.google.com/gke-tpu-slice": "svc-t-0-0-abc"}
	tpuPod.Spec.Containers = []corev1.Container{{Name: "main", Resources: corev1.ResourceRequirements{
		Limits: corev1.ResourceList{"google.com/tpu": resource.MustParse("4")},
	}}}
	plainTPU := omePod("prod", "svc-p-engine-0", "tpu-1", "svc-p", "engine", 0, true)
	plainTPU.Spec.Containers = tpuPod.Spec.Containers
	gpuPod := omePod("prod", "svc-g-engine-0", "tpu-1", "svc-g", "engine", 1, true)
	gpuPod.Annotations = map[string]string{constants.TPUSliceProvisioningAnnotationKey: "true"}
	gpuPod.Spec.NodeSelector = map[string]string{"example.com/any": "x"}

	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(tpuNode, tpuPod, plainTPU, gpuPod).Build()
	snap, err := Build(context.Background(), c, Options{Now: func() time.Time { return buildNow }})
	if err != nil {
		t.Fatal(err)
	}
	pods := map[string]PodInfo{}
	for _, p := range snap.Nodes["tpu-1"].OMEPods {
		pods[p.Name] = p
	}
	got := pods["svc-t-engine-0"]
	if !got.TPUSliceProvisioned || !reflect.DeepEqual(got.NodeSelector, tpuPod.Spec.NodeSelector) {
		t.Fatalf("slice-provisioned TPU pod = %+v", got)
	}
	if p := pods["svc-p-engine-0"]; p.TPUSliceProvisioned || p.NodeSelector != nil {
		t.Fatalf("TPU pod without the annotation = %+v", p)
	}
	if p := pods["svc-g-engine-0"]; p.TPUSliceProvisioned || p.NodeSelector != nil {
		t.Fatalf("GPU pod must not carry TPU slice fields: %+v", p)
	}
}
