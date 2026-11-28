package kvcachepool

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/kvcache"
	ometesting "sigs.k8s.io/ome/pkg/utils/testing"
)

func TestLMCachePoolLifecycle(t *testing.T) {
	if os.Getenv("KUBEBUILDER_ASSETS") == "" {
		t.Skip("set KUBEBUILDER_ASSETS to run the local API server")
	}
	scheme := runtime.NewScheme()
	require.NoError(t, ome.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	root := filepath.Join("../../../..")
	_, cfg := ometesting.StartOnBusyHost(t, func() *envtest.Environment {
		return &envtest.Environment{
			UseExistingCluster: ptr.To(false),
			CRDDirectoryPaths: []string{
				filepath.Join(root, "config/crd/full/ome.io_kvcachepools.yaml"),
				filepath.Join(root, "config/crd/full/ome.io_inferenceservices.yaml"),
			},
			ErrorIfCRDPathMissing: true,
		}
	})
	cl, err := client.New(cfg, client.Options{Scheme: scheme})
	require.NoError(t, err)
	ctx := t.Context()
	pool := &ome.KVCachePool{}
	sample, err := os.ReadFile(filepath.Join(root, "config/samples/kvcachepool/lmcache-node-local.yaml"))
	require.NoError(t, err)
	require.NoError(t, yaml.UnmarshalStrict(sample, pool))
	pool.Namespace = "default"
	unsupported := pool.DeepCopy()
	unsupported.Name = "unsupported-mode"
	unsupported.Spec.DeploymentMode = "Unsupported"
	require.True(t, apierrors.IsInvalid(cl.Create(ctx, unsupported)))
	sampleImage := pool.Spec.Workloads[0].Runner.Image
	pool.Spec.Workloads[0].Runner.Image = "example.invalid/LMCache:test"
	require.NoError(t, cl.Create(ctx, pool))
	key := client.ObjectKeyFromObject(pool)
	r := Reconciler{cl, cl}
	reconcile := func() {
		t.Helper()
		_, err := r.Reconcile(ctx, ctrl.Request{NamespacedName: key})
		require.NoError(t, err)
	}
	read := func() { t.Helper(); require.NoError(t, cl.Get(ctx, key, pool)) }
	reconcile()
	reconcile()
	read()
	require.False(t, meta.IsStatusConditionTrue(pool.Status.Conditions, "Ready"))
	childKey := client.ObjectKey{Namespace: pool.Namespace, Name: kvcache.ServerName(pool.UID)}
	cmKey := client.ObjectKey{Namespace: pool.Namespace, Name: childKey.Name + "-connection"}
	ds := &appsv1.DaemonSet{}
	svc := &corev1.Service{}
	cm := &corev1.ConfigMap{}
	ready := meta.FindStatusCondition(pool.Status.Conditions, "Ready")
	require.NotNil(t, ready)
	require.Equal(t, "InvalidSpec", ready.Reason)
	require.Contains(t, ready.Message, "runner.image")
	require.True(t, apierrors.IsNotFound(cl.Get(ctx, childKey, ds)))
	pool.Spec.Workloads[0].Runner.Image = sampleImage
	require.NoError(t, cl.Update(ctx, pool))
	reconcile()
	read()
	require.NoError(t, cl.Get(ctx, childKey, ds))
	require.NoError(t, cl.Get(ctx, childKey, svc))
	require.NoError(t, cl.Get(ctx, cmKey, cm))
	require.Empty(t, ds.OwnerReferences)
	require.Empty(t, svc.OwnerReferences)
	require.Empty(t, cm.OwnerReferences)
	require.Equal(t, corev1.ServiceInternalTrafficPolicyLocal, *svc.Spec.InternalTrafficPolicy)
	require.Equal(t, svc.Spec.Selector, ds.Spec.Template.Labels)
	require.NotContains(t, svc.Spec.Selector, kvcache.PoolUIDLabel)
	require.Equal(t, pool.Spec.Workloads[0].NodeSelector, ds.Spec.Template.Spec.NodeSelector)
	require.Equal(t, defaultServerImage, ds.Spec.Template.Spec.Containers[0].Image)
	sc := ds.Spec.Template.Spec.Containers[0].SecurityContext
	require.False(t, sc != nil && ptr.Deref(sc.Privileged, false), "the LMCache server must run without privileged mode")
	require.True(t, ds.Spec.Template.Spec.HostIPC)
	require.False(t, ds.Spec.Template.Spec.HostNetwork)
	require.Equal(t, "nvidia", *ds.Spec.Template.Spec.RuntimeClassName)
	require.Contains(t, ds.Spec.Template.Spec.Containers[0].Args, "60")
	require.Equal(t, "/healthcheck", ds.Spec.Template.Spec.Containers[0].ReadinessProbe.HTTPGet.Path)
	raw, err := kvcache.Connection(cm, pool.UID)
	require.NoError(t, err)
	require.Contains(t, raw, "tcp://"+childKey.Name+".default")
	// envtest has no scheduler or DaemonSet controller. Drive the observed rollout, including a stale revision.
	ds.Status = appsv1.DaemonSetStatus{ObservedGeneration: ds.Generation, DesiredNumberScheduled: 2, NumberReady: 2, NumberAvailable: 2, UpdatedNumberScheduled: 1}
	require.NoError(t, cl.Status().Update(ctx, ds))
	// An independent writer changes a pool-owned key and adds its own key.
	// Reconcile must reclaim the first without removing the second.
	before := cm.DeepCopy()
	cm.Data[kvcache.ConnectionKey] = "stale"
	cm.Data["independent-key"] = "keep"
	require.NoError(t, cl.Patch(ctx, cm, client.MergeFrom(before), client.FieldOwner("test-writer")))
	reconcile()
	require.NoError(t, cl.Get(ctx, cmKey, cm))
	require.Equal(t, raw, cm.Data[kvcache.ConnectionKey])
	require.Equal(t, "keep", cm.Data["independent-key"])
	read()
	require.False(t, meta.IsStatusConditionTrue(pool.Status.Conditions, "Ready"))
	require.Nil(t, pool.Status.Connection)
	require.NoError(t, cl.Get(ctx, childKey, ds))
	ds.Status.UpdatedNumberScheduled = 2
	require.NoError(t, cl.Status().Update(ctx, ds))
	reconcile()
	read()
	require.True(t, meta.IsStatusConditionTrue(pool.Status.Conditions, "Ready"))
	require.Equal(t, cm.Name, pool.Status.Connection.ConfigMapRef.Name)
	generation := ds.Generation
	rv := pool.ResourceVersion
	require.NoError(t, cl.Get(ctx, childKey, ds))
	dsRV := ds.ResourceVersion
	reconcile()
	read()
	require.Equal(t, rv, pool.ResourceVersion)
	require.NoError(t, cl.Get(ctx, childKey, ds))
	require.Equal(t, dsRV, ds.ResourceVersion)
	pool.Spec.Workloads[0].Runner.Image = "example.invalid/lmcache:test"
	require.NoError(t, cl.Update(ctx, pool))
	reconcile()
	read()
	require.False(t, meta.IsStatusConditionTrue(pool.Status.Conditions, "Ready"))
	require.NoError(t, cl.Get(ctx, childKey, ds))
	require.Greater(t, ds.Generation, generation)
	require.Equal(t, "example.invalid/lmcache:test", ds.Spec.Template.Spec.Containers[0].Image)
	// The DaemonSet Pod shares the pool's namespace with the consumers. Its
	// presence must not keep the consumer-drain finalizer waiting forever.
	serverPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "cache-server", Namespace: pool.Namespace, Labels: ds.Spec.Template.Labels},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "server", Image: "example.invalid/server"}}},
	}
	require.NoError(t, cl.Create(ctx, serverPod))
	// Removing either consumer must leave the shared provider available.
	consumers := []ome.InferenceService{{ObjectMeta: metav1.ObjectMeta{Name: "a", Namespace: "default"}}, {ObjectMeta: metav1.ObjectMeta{Name: "b", Namespace: "default"}}}
	for i := range consumers {
		svc := &consumers[i]
		svc.Spec.KVCachePool = &ome.KVCachePoolRef{Name: pool.Name}
		require.NoError(t, cl.Create(ctx, svc))
	}
	require.NoError(t, cl.Delete(ctx, pool, client.PropagationPolicy(metav1.DeletePropagationForeground)))
	reconcile()
	read()
	require.Equal(t, "InUse", meta.FindStatusCondition(pool.Status.Conditions, "Ready").Reason)
	require.NoError(t, cl.Delete(ctx, &consumers[0]))
	reconcile()
	require.NoError(t, cl.Get(ctx, childKey, &appsv1.DaemonSet{}))
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "draining", Namespace: "default", Labels: map[string]string{kvcache.PoolUIDLabel: string(pool.UID)}}, Spec: corev1.PodSpec{Containers: []corev1.Container{{Name: "engine", Image: "example.invalid/vllm"}}}}
	require.NoError(t, cl.Create(ctx, pod))
	require.NoError(t, cl.Delete(ctx, &consumers[1]))
	reconcile()
	require.NoError(t, cl.Get(ctx, childKey, &appsv1.DaemonSet{}))
	require.NoError(t, cl.Delete(ctx, pod, client.GracePeriodSeconds(0)))
	// An unbound service's old ReplicaSet may replace a pod until it scales to zero.
	labels := map[string]string{kvcache.PoolUIDLabel: string(pool.UID)}
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{Name: "rolling", Namespace: pool.Namespace, Labels: labels},
		Spec: appsv1.ReplicaSetSpec{
			Replicas: ptr.To[int32](1), Selector: &metav1.LabelSelector{MatchLabels: labels},
			Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: labels}, Spec: pod.Spec},
		},
	}
	require.NoError(t, cl.Create(ctx, rs))
	reconcile()
	require.NoError(t, cl.Get(ctx, childKey, &appsv1.DaemonSet{}))
	rs.Spec.Replicas = ptr.To[int32](0)
	require.NoError(t, cl.Update(ctx, rs))
	reconcile()
	// envtest does not run Kubernetes garbage collection. Observe the foreground
	// request and complete only the GC finalizers the real cluster would remove.
	finishGC := func(obj client.Object, key client.ObjectKey) {
		t.Helper()
		err := cl.Get(ctx, key, obj)
		if apierrors.IsNotFound(err) {
			return
		}
		require.NoError(t, err)
		require.NotNil(t, obj.GetDeletionTimestamp())
		require.Equal(t, []string{metav1.FinalizerDeleteDependents}, obj.GetFinalizers())
		obj.SetFinalizers(nil)
		require.NoError(t, cl.Update(ctx, obj))
	}
	require.NoError(t, cl.Delete(ctx, serverPod, client.GracePeriodSeconds(0)))
	finishGC(ds, childKey)
	for range 4 {
		reconcile()
	}
	finishGC(pool, key)
	for _, obj := range []client.Object{ds, svc, cm, pool} {
		require.True(t, apierrors.IsNotFound(cl.Get(ctx, client.ObjectKeyFromObject(obj), obj)))
	}
	replacement := &ome.KVCachePool{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, Spec: pool.Spec}
	require.NoError(t, cl.Create(ctx, replacement))
	require.NotEqual(t, pool.UID, replacement.UID)
	reconcile()
	// A collision at the new UID-derived name must never be adopted or deleted.
	collision := serverService(replacement)
	collision.Labels = nil
	collision.Annotations = nil
	require.NoError(t, cl.Create(ctx, collision))
	reconcile()
	require.NoError(t, cl.Get(ctx, key, replacement))
	require.Equal(t, "OwnershipConflict", meta.FindStatusCondition(replacement.Status.Conditions, "Ready").Reason)
	require.NoError(t, cl.Delete(ctx, collision))
	reconcile()
	require.NoError(t, cl.Get(ctx, client.ObjectKey{Namespace: key.Namespace, Name: kvcache.ServerName(replacement.UID)}, ds))
	require.True(t, apierrors.IsNotFound(cl.Get(ctx, childKey, &appsv1.DaemonSet{})))
}

func TestNodeLocalBudgetAndOwnership(t *testing.T) {
	pool := &ome.KVCachePool{
		ObjectMeta: metav1.ObjectMeta{Name: "cache", Namespace: "team", UID: "pool-id"},
		Spec: ome.KVCachePoolSpec{
			DeploymentMode: ome.KVCachePoolNodeLocal,
			Provider: ome.KVCacheProviderSpec{Name: ome.KVCacheProviderLMCache,
				Config: ome.KVCacheProviderConfig{L1CapacityPerNode: resource.MustParse("512Mi")}},
			Workloads: []ome.KVCachePoolWorkload{{
				Name: "server", NodeSelector: map[string]string{"pool": "test"}, RuntimeClassName: "gpu-runtime",
			}},
		},
	}
	ds, err := serverDaemonSet(pool)
	require.NoError(t, err)
	c := ds.Spec.Template.Spec.Containers[0]
	require.Equal(t, defaultServerImage, c.Image)
	if c.SecurityContext != nil {
		require.Nil(t, c.SecurityContext.Privileged, "the server template should omit privileged")
	}
	require.True(t, ds.Spec.Template.Spec.HostIPC)
	require.Equal(t, "gpu-runtime", ptr.Deref(ds.Spec.Template.Spec.RuntimeClassName, ""))
	require.Contains(t, c.Args, "0.5")
	require.Greater(t, c.Resources.Requests.Memory().Cmp(pool.Spec.Provider.Config.L1CapacityPerNode), 0)
	require.True(t, belongsToPool(ds, pool))
	ds.Labels[kvcache.ServerPoolUIDLabel] = "other"
	require.False(t, belongsToPool(ds, pool))
	pool.Spec.Workloads[0].Runner = &ome.KVCacheServerRunner{Resources: &corev1.ResourceRequirements{Limits: corev1.ResourceList{corev1.ResourceMemory: resource.MustParse("32Mi")}}}
	_, err = serverDaemonSet(pool)
	require.ErrorContains(t, err, "memory limit")
}
