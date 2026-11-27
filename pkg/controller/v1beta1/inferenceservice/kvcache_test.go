package inferenceservice

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/go-logr/logr"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	monitoringv1 "github.com/prometheus-operator/prometheus-operator/pkg/apis/monitoring/v1"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	lws "sigs.k8s.io/lws/api/leaderworkerset/v1"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/acceleratorclassselector"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/kvcache"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestKVCacheSampleReconciles(t *testing.T) {
	read := func(path string, dst client.Object) {
		t.Helper()
		b, err := os.ReadFile(filepath.Join("../../../..", "config", path))
		require.NoError(t, err)
		require.NoError(t, yaml.UnmarshalStrict(b, dst))
	}
	service := &ome.InferenceService{}
	rt := &ome.ServingRuntime{}
	pool := &ome.KVCachePool{}
	model := &ome.BaseModel{}
	read("samples/kvcachepool/inferenceservice.yaml", service)
	read("samples/kvcachepool/vllm-runtime.yaml", rt)
	read("samples/kvcachepool/lmcache-node-local.yaml", pool)
	read("samples/kvcachepool/model.yaml", model)
	for _, obj := range []client.Object{pool, rt, service, model} {
		obj.SetNamespace("default")
	}
	service.Finalizers = []string{inferenceServiceFinalizer}
	service.UID = "test-service"
	pool.UID = "pool-id"
	pool.Generation = 1
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: kvcache.ServerName(pool.UID) + "-connection", Namespace: pool.Namespace, Labels: map[string]string{kvcache.ServerPoolUIDLabel: string(pool.UID)}},
		Data:       map[string]string{kvcache.ConnectionKey: kvcache.ConnectionJSON(pool.UID, pool.Namespace)},
	}
	cl := fakeclient.New(t, fakeclient.WithTypes(kedav1.AddToScheme, monitoringv1.AddToScheme, lws.AddToScheme), fakeclient.WithObjects(service, rt, pool, model, cm), fakeclient.WithStatusSubresource(service, pool))
	cfg := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace}, Data: map[string]string{"deploy": `{"defaultDeploymentMode":"RawDeployment"}`}}
	r := &InferenceServiceReconciler{Client: cl, APIReader: cl, Clientset: fake.NewClientset(cfg), Scheme: cl.Scheme(), Recorder: record.NewFakeRecorder(128), Log: logr.Discard(), RuntimeSelector: runtimeselector.New(cl), AcceleratorClassSelector: acceleratorclassselector.New(cl)}
	ctx := context.Background()
	reconcile := func() {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: client.ObjectKeyFromObject(service)})
		require.NoError(t, err)
	}
	state := func() *ome.InferenceService {
		var s ome.InferenceService
		require.NoError(t, cl.Get(ctx, client.ObjectKeyFromObject(service), &s))
		return &s
	}
	reconcile()
	status := state().Status
	require.NotNil(t, status.GetCondition(ome.KVCacheConfigured))
	require.Equal(t, corev1.ConditionFalse, status.GetCondition(ome.KVCacheConfigured).Status)
	var dep appsv1.DeploymentList
	require.NoError(t, cl.List(ctx, &dep))
	require.Empty(t, dep.Items)
	pool.Status.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 1}}
	pool.Status.Connection = &ome.KVCachePoolConnectionStatus{ConfigMapRef: corev1.LocalObjectReference{Name: cm.Name}}
	require.NoError(t, cl.Status().Update(ctx, pool))
	reconcile()
	require.NoError(t, cl.List(ctx, &dep))
	require.Len(t, dep.Items, 1)
	tpl := dep.Items[0].Spec.Template
	require.Equal(t, string(pool.UID), tpl.Labels[kvcache.PoolUIDLabel])
	require.Contains(t, tpl.Spec.Containers[0].Args, cm.Data[kvcache.ConnectionKey])
	status = state().Status
	require.Equal(t, corev1.ConditionTrue, status.GetCondition(ome.KVCacheConfigured).Status)
	s := state()
	s.Spec.KVCachePool = nil
	require.NoError(t, cl.Update(ctx, s))
	reconcile()
	require.NoError(t, cl.List(ctx, &dep))
	require.Len(t, dep.Items, 1)
	require.NotContains(t, dep.Items[0].Spec.Template.Spec.Containers[0].Args, "--kv-transfer-config")
	status = state().Status
	require.Nil(t, status.GetCondition(ome.KVCacheConfigured))
}
