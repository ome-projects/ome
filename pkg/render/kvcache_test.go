package render

import (
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/kvcache"
)

func TestRenderEngineWithNodeLocalPool(t *testing.T) {
	raw := kvcache.ConnectionJSON("123", "team")
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "ome-kv-123-connection", Namespace: "team", Labels: map[string]string{kvcache.ServerPoolUIDLabel: "123"}},
		Data:       map[string]string{kvcache.ConnectionKey: raw},
	}
	pool := &ome.KVCachePool{
		ObjectMeta: metav1.ObjectMeta{Name: "shared", Namespace: "team", UID: "123", Generation: 3},
		Spec:       ome.KVCachePoolSpec{Provider: ome.KVCacheProviderSpec{Name: ome.KVCacheProviderLMCache}, DeploymentMode: ome.KVCachePoolNodeLocal, Workloads: []ome.KVCachePoolWorkload{{Name: "server", NodeSelector: map[string]string{"group": "a"}}}},
		Status: ome.KVCachePoolStatus{
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 3}},
			Connection: &ome.KVCachePoolConnectionStatus{ConfigMapRef: corev1.LocalObjectReference{Name: cm.Name}},
		},
	}
	connector := ome.KVCacheConnectorConfig{ConnectorClass: kvcache.ConnectorClass}
	runtime := &ome.ServingRuntimeSpec{KVCacheConnectors: []ome.KVCacheConnectorSpec{{
		Provider: ome.KVCacheProviderLMCache, DeploymentModes: []ome.KVCachePoolDeploymentMode{ome.KVCachePoolNodeLocal},
		Components: ome.KVCacheConnectorComponents{Engine: ome.KVCacheEngineConnector{ConnectorConfig: connector}},
	}}}
	service := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "llama", Namespace: "team"}, Spec: ome.InferenceServiceSpec{KVCachePool: &ome.KVCachePoolRef{Name: "shared"}}}
	engine := &ome.EngineSpec{PodSpec: ome.PodSpec{HostIPC: true}, Runner: &ome.RunnerSpec{Container: corev1.Container{Name: "vllm", Image: "example.invalid/vllm", Command: []string{"vllm", "serve"}, Args: []string{"model"}}}}
	piece := &Piece{Client: newRenderClient(t, pool, cm), DeploymentMode: constants.RawDeployment, Runtime: runtime, InferenceServiceConfig: &controllerconfig.InferenceServicesConfig{}}
	rendered, err := RenderEngine(t.Context(), piece, service, engine)
	require.NoError(t, err)
	require.Equal(t, []string{"model", "--kv-transfer-config", raw}, rendered.Primary.Containers[0].Args)
	require.Contains(t, rendered.Primary.Containers[0].Env, corev1.EnvVar{Name: "PYTHONHASHSEED", Value: "0"})
	require.Equal(t, "a", rendered.Primary.NodeSelector["group"])
	require.Equal(t, "123", rendered.ObjectMeta.Labels[kvcache.PoolUIDLabel])
	require.Equal(t, []string{"model"}, engine.Runner.Args)
	for _, tc := range []struct {
		name   string
		edit   func(*ome.EngineSpec)
		reason string
	}{
		{"incompatible node", func(e *ome.EngineSpec) { e.NodeSelector = map[string]string{"group": "b"} }, "nodeSelector"},
		{"shell entrypoint", func(e *ome.EngineSpec) { e.Runner.Command = []string{"sh", "-c"} }, "direct vLLM"},
		{"duplicate connector", func(e *ome.EngineSpec) { e.Runner.Args = []string{"--kv-transfer-config={}"} }, "already configures"},
		{"private IPC", func(e *ome.EngineSpec) { e.HostIPC = false }, "hostIPC"},
		{"private shm", func(e *ome.EngineSpec) {
			e.Runner.VolumeMounts = []corev1.VolumeMount{{Name: "scratch", MountPath: "/dev/shm"}}
		}, "/dev/shm"},
		{"shm webhook", func(e *ome.EngineSpec) {
			e.Annotations = map[string]string{constants.RuntimeShmProfileAnnotationKey: "default"}
		}, "shm-profile"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			spec := engine.DeepCopy()
			tc.edit(spec)
			_, err := RenderEngine(t.Context(), piece, service, spec)
			require.ErrorContains(t, err, tc.reason)
		})
	}
	tampered := cm.DeepCopy()
	tampered.Data[kvcache.ConnectionKey] = kvcache.ConnectionJSON("another-pool", "team")
	piece.Client = newRenderClient(t, pool, tampered)
	_, err = RenderEngine(t.Context(), piece, service, engine)
	require.ErrorContains(t, err, "node-local LMCache endpoint")
	foreign := cm.DeepCopy()
	foreign.Labels = nil
	piece.Client = newRenderClient(t, pool, foreign)
	_, err = RenderEngine(t.Context(), piece, service, engine)
	require.ErrorContains(t, err, "does not belong")
	stale := pool.DeepCopy()
	stale.Generation++
	piece.Client = newRenderClient(t, stale, cm)
	_, err = RenderEngine(t.Context(), piece, service, engine)
	require.ErrorContains(t, err, "current connection")
	service.Spec.KVCachePool = nil
	rendered, err = RenderEngine(t.Context(), piece, service, engine)
	require.NoError(t, err)
	require.Equal(t, []string{"model"}, rendered.Primary.Containers[0].Args)
}
