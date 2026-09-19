package components

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrlclientfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestReconcileOMENativeSubresources_SkipsPodMonitorWhenSchemeAbsent pins
// that an OMENative ISVC reconciles on a cluster without the Prometheus
// operator: creating the per-component PodMonitor returns "no kind is
// registered for the type v1.PodMonitor in scheme" (OME registers that
// scheme only when the CRD is present), which must not abort the reconcile
// before status is written. The stable Service must still be created; the
// PodMonitor is skipped, not fatal.
func TestReconcileOMENativeSubresources_SkipsPodMonitorWhenSchemeAbsent(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	// NOTE: monitoringv1 (PodMonitor) is deliberately NOT registered.

	cl := ctrlclientfake.NewClientBuilder().WithScheme(scheme).Build()
	b := &BaseComponentFields{Client: cl, Scheme: scheme, Log: logr.Discard()}

	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "dummy", Namespace: "inf-prod"}}
	objectMeta := metav1.ObjectMeta{Name: "dummy-engine", Namespace: "inf-prod"}
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{
		Name:  "ome-container",
		Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: 8000}},
	}}}

	err := ReconcileOMENativeSubresources(context.Background(), b, isvc,
		v1beta1.EngineComponent, &v1beta1.ComponentExtensionSpec{}, objectMeta, podSpec)
	require.NoError(t, err, "reconcile must not fail when the PodMonitor CRD/scheme is absent")

	// The stable Service (which runs before the PodMonitor) must still exist.
	svc := &corev1.Service{}
	require.NoError(t, cl.Get(context.Background(),
		types.NamespacedName{Name: "dummy-engine", Namespace: "inf-prod"}, svc),
		"stable Service should be created even when PodMonitor is skipped")
}
