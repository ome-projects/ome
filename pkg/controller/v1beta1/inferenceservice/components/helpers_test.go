package components

import (
	"testing"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// componentClient is the fake client the component tests share: the
// builder's scheme plus the KEDA and Prometheus Operator groups, which the
// autoscaler dispatch and the PodMonitor path write. A reconciler built on it
// reads its scheme from the client.
func componentClient(t testing.TB, opts ...fakeclient.Option) client.WithWatch {
	t.Helper()
	return fakeclient.New(t, append([]fakeclient.Option{fakeclient.WithTypes(kedav1.AddToScheme, monitoringv1.AddToScheme)}, opts...)...)
}
