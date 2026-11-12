package pod

import (
	"os"
	"testing"

	v1 "k8s.io/api/core/v1"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	pkgtest "sigs.k8s.io/ome/pkg/utils/testing"
)

var cfg *rest.Config
var c client.Client
var clientset kubernetes.Interface

func TestMain(m *testing.M) {
	t := pkgtest.SetupEnvTest()

	err := v1beta1.AddToScheme(scheme.Scheme)
	if err != nil {
		klog.Error(err, "Failed to add v1beta1 to scheme")
	}

	if cfg, err = t.Start(); err != nil {
		klog.Error(err, "Failed to start testing panel")
	}

	if c, err = client.New(cfg, client.Options{Scheme: scheme.Scheme}); err != nil {
		klog.Error(err, "Failed to start client")
	}

	if clientset, err = kubernetes.NewForConfig(cfg); err != nil {
		klog.Error(err, "Failed to create clientset")
	}

	code := m.Run()
	_ = t.Stop()
	os.Exit(code)
}

func ptrTo[T any](v T) *T { return &v }

// devInfVolumeName is the device volume the fixture's default RDMA profile mounts.
const devInfVolumeName = "devinf"

// shippedConfigMap is the inferenceservice-config the kustomize profile
// installs, the one copy of the webhook's tables outside the chart.
func shippedConfigMap(t testing.TB) *v1.ConfigMap {
	t.Helper()
	configMap, err := pkgtest.ShippedInferenceServiceConfigMap()
	if err != nil {
		t.Fatal(err)
	}
	return configMap
}

// configMapWithRDMAFixture returns a copy of configMap whose rdma block is
// the fixture table, the way a cluster's values file fills the shipped
// ConfigMap: a chart install ships no profile.
func configMapWithRDMAFixture(t testing.TB, configMap *v1.ConfigMap) *v1.ConfigMap {
	t.Helper()
	withFixture := configMap.DeepCopy()
	if withFixture.Data == nil {
		withFixture.Data = map[string]string{}
	}
	withFixture.Data[RDMAConfigKeyName] = pkgtest.RDMAProfileFixtureJSON
	return withFixture
}

// fixtureRDMA is the RDMA profile table the test seeds carry.
func fixtureRDMA(t testing.TB) *RDMAConfig {
	t.Helper()
	table, err := newRDMAConfig(configMapWithRDMAFixture(t, &v1.ConfigMap{}))
	if err != nil || !table.hasProfiles() {
		t.Fatalf("fixture rdma block: table=%v err=%v", table, err)
	}
	return table
}

// shippedRuntimeProfiles is the runtimeProfiles block the shipped ConfigMap carries.
func shippedRuntimeProfiles(t testing.TB) *RuntimeProfilesConfig {
	t.Helper()
	profiles, err := newRuntimeProfilesConfig(shippedConfigMap(t))
	if err != nil || profiles == nil {
		t.Fatalf("shipped runtimeProfiles block: profiles=%v err=%v", profiles, err)
	}
	return profiles
}

// shippedMetrics is the metricsAggregator block the shipped ConfigMap carries.
func shippedMetrics(t testing.TB) *MetricsAggregator {
	t.Helper()
	metrics, err := newMetricsAggregator(shippedConfigMap(t))
	if err != nil || !metrics.portsConfigured() {
		t.Fatalf("shipped metricsAggregator block: ports configured=%v err=%v", metrics.portsConfigured(), err)
	}
	return metrics
}
