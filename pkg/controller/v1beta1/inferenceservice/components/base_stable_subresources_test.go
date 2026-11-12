package components

import (
	"context"
	"errors"
	"testing"

	"github.com/go-logr/logr"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/render"
	testingutils "sigs.k8s.io/ome/pkg/utils/testing"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// stableSubresourcesPod is a one-container pod whose ports carry the given
// names in order; an empty name is an unnamed port.
func stableSubresourcesPod(portNames ...string) *corev1.PodSpec {
	ports := make([]corev1.ContainerPort, 0, len(portNames))
	for i, name := range portNames {
		ports = append(ports, corev1.ContainerPort{Name: name, ContainerPort: int32(8000 + i)})
	}
	return &corev1.PodSpec{Containers: []corev1.Container{{Name: "engine", Image: "engine:v1", Ports: ports}}}
}

func stableSubresourcesService() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "ns", UID: types.UID("svc-uid")}}
}

// chartScrapeValues is the operator configuration whose podMonitor block is
// the one the chart ships, read from charts/ome-resources/values.yaml: the
// scrape path, interval and port names have no in-code default, so every
// test that expects OME's own endpoint supplies them the way a cluster does.
func chartScrapeValues(t testing.TB) *controllerconfig.InferenceServicesConfig {
	t.Helper()
	cfg := &controllerconfig.InferenceServicesConfig{}
	require.NoError(t, testingutils.ShippedChartValue("ome.podMonitor", &cfg.PodMonitor))
	return cfg
}

// extraEndpointsAnnotation declares one more scrape endpoint on the http
// port, the way a Component's extra-endpoints annotation does.
func extraEndpointsAnnotation() map[string]string {
	return map[string]string{constants.ExtraPodMetricsEndpointsAnnotationKey: "http:/engine_metrics"}
}

// reconcileEngineSubresources writes the stable Service and the PodMonitor
// of svc's engine over cl for the given pod under the given operator
// configuration and annotations, and returns the write's error.
func reconcileEngineSubresources(t *testing.T, cl client.Client, cfg *controllerconfig.InferenceServicesConfig, svc *v1beta1.InferenceService, pod *corev1.PodSpec, annotations map[string]string) error {
	t.Helper()
	b := &BaseComponentFields{Piece: render.Piece{Log: logr.Discard(), InferenceServiceConfig: cfg}, Client: cl, Scheme: cl.Scheme()}
	meta := metav1.ObjectMeta{Name: svc.Name + "-engine", Namespace: svc.Namespace, Annotations: annotations}
	return ReconcileOMENativeSubresources(context.Background(), b, svc, v1beta1.EngineComponent, &v1beta1.ComponentExtensionSpec{}, meta, pod, false)
}

// enginePodMonitor reads the PodMonitor reconcileEngineSubresources wrote.
func enginePodMonitor(t *testing.T, cl client.Client) *monitoringv1.PodMonitor {
	t.Helper()
	monitor := &monitoringv1.PodMonitor{}
	require.NoError(t, cl.Get(context.Background(), types.NamespacedName{Name: "svc-engine", Namespace: "ns"}, monitor))
	return monitor
}

// The test: the PodMonitor scrapes the port the podMonitor block names (the
// chart's metrics) wherever it stands among the first container's ports,
// else the first port when it is named, else the block's fallback port (the
// chart's http).
func TestReconcileOMENativeSubresources_PodMonitorScrapePort(t *testing.T) {
	cases := []struct {
		name string
		pod  *corev1.PodSpec
		want string
	}{
		{"metrics after a named port", stableSubresourcesPod("http", "metrics"), "metrics"},
		{"metrics after an unnamed port", stableSubresourcesPod("", "metrics"), "metrics"},
		{"first port named, no metrics port", stableSubresourcesPod("grpc", "http"), "grpc"},
		{"first port unnamed, no metrics port", stableSubresourcesPod("", "grpc"), "http"},
		{"no ports", stableSubresourcesPod(), "http"},
		{"no containers", &corev1.PodSpec{}, "http"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cl := componentClient(t)
			require.NoError(t, reconcileEngineSubresources(t, cl, chartScrapeValues(t), stableSubresourcesService(), c.pod, nil))
			require.Equal(t, c.want, *enginePodMonitor(t, cl).Spec.PodMetricsEndpoints[0].Port)
		})
	}
}

// The test: with the chart's podMonitor block the PodMonitor carries OME's
// own endpoint first, on the scrape port at /metrics every 10s, then the
// endpoints the extra-endpoints annotation declares at the same interval.
func TestReconcileOMENativeSubresources_PodMonitorScrapeEndpoints(t *testing.T) {
	cl := componentClient(t)
	require.NoError(t, reconcileEngineSubresources(t, cl, chartScrapeValues(t), stableSubresourcesService(), stableSubresourcesPod("http", "metrics"), extraEndpointsAnnotation()))
	require.Equal(t, []monitoringv1.PodMetricsEndpoint{
		{Port: ptr.To("metrics"), Path: "/metrics", Interval: "10s"},
		{Port: ptr.To("http"), Path: "/engine_metrics", Interval: "10s"},
	}, enginePodMonitor(t, cl).Spec.PodMetricsEndpoints)
}

// The test: a podMonitor block without the scrape values writes no endpoint
// of OME's own. A Component with an extra-endpoints annotation keeps those
// endpoints; one without gets a PodMonitor with no endpoint, and the pass
// does not fail.
func TestReconcileOMENativeSubresources_PodMonitorWithoutScrapeValues(t *testing.T) {
	for _, cfg := range []*controllerconfig.InferenceServicesConfig{nil, {}} {
		cl := componentClient(t)
		require.NoError(t, reconcileEngineSubresources(t, cl, cfg, stableSubresourcesService(), stableSubresourcesPod("http", "metrics"), extraEndpointsAnnotation()))
		require.Equal(t, []monitoringv1.PodMetricsEndpoint{{Port: ptr.To("http"), Path: "/engine_metrics"}}, enginePodMonitor(t, cl).Spec.PodMetricsEndpoints)

		cl = componentClient(t)
		require.NoError(t, reconcileEngineSubresources(t, cl, cfg, stableSubresourcesService(), stableSubresourcesPod("http", "metrics"), nil))
		require.Empty(t, enginePodMonitor(t, cl).Spec.PodMetricsEndpoints)
	}
}

// The test: the service controls the stable Service and the PodMonitor, so
// both go when the service goes.
func TestReconcileOMENativeSubresources_ObjectsAreControlledByTheService(t *testing.T) {
	cl := componentClient(t)
	svc := stableSubresourcesService()
	require.NoError(t, reconcileEngineSubresources(t, cl, chartScrapeValues(t), svc, stableSubresourcesPod("http"), nil))
	key := types.NamespacedName{Name: "svc-engine", Namespace: "ns"}
	service := &corev1.Service{}
	require.NoError(t, cl.Get(context.Background(), key, service))
	require.True(t, metav1.IsControlledBy(service, svc), "the service controls its stable Service")
	require.True(t, metav1.IsControlledBy(enginePodMonitor(t, cl), svc), "the service controls its PodMonitor")
}

// The test: a refused stable Service write is reported, not swallowed, so
// the pass retries instead of writing a PodMonitor for a Service that does
// not exist.
func TestReconcileOMENativeSubresources_ReportsARefusedServiceWrite(t *testing.T) {
	refused := errors.New("refused by the test")
	cl := componentClient(t, fakeclient.FailingVerb(fakeclient.Create, fakeclient.EveryCall, refused).OnKind(&corev1.Service{}))
	err := reconcileEngineSubresources(t, cl, chartScrapeValues(t), stableSubresourcesService(), stableSubresourcesPod("http"), nil)
	require.ErrorContains(t, err, "refused by the test")
	require.ErrorContains(t, err, "stable service")
	require.Error(t, cl.Get(context.Background(), types.NamespacedName{Name: "svc-engine", Namespace: "ns"}, &monitoringv1.PodMonitor{}), "no PodMonitor is written for a Service that was not")
}
