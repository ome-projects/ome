package pernode

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/go-logr/logr"
	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/basemodel/shared"
)

func orphanConfigMap() *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node", Namespace: constants.OMENamespace, UID: "original", ResourceVersion: "1",
			Labels: map[string]string{constants.ModelStatusConfigMapLabel: "true"},
		},
		Data: map[string]string{constants.GetModelConfigMapKey("default", "model", false): `{"status":"Ready"}`},
	}
}

func TestCleanupOrphanedNodeConfigMap(t *testing.T) {
	for _, tc := range []struct {
		name       string
		nodeExists bool
		cachedNode bool
		cacheError error
		readError  error
		noReader   bool
	}{
		{name: "missing node"},
		{name: "existing not-ready node overrides cached miss", nodeExists: true},
		{name: "healthy node cache-only fast path", nodeExists: true, cachedNode: true},
		{name: "stale cached presence delays cleanup", cachedNode: true},
		{name: "cached lookup failure", cacheError: apierrors.NewTimeoutError("cached lookup", 1)},
		{name: "forbidden node lookup", readError: apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, "node", fmt.Errorf("denied"))},
		{name: "timed out node lookup", readError: apierrors.NewTimeoutError("node lookup", 1)},
		{name: "cancelled node lookup", readError: context.Canceled},
		{name: "missing authoritative reader", noReader: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()
			scheme := runtime.NewScheme()
			g.Expect(corev1.AddToScheme(scheme)).To(gomega.Succeed())
			cm := orphanConfigMap()
			live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()
			if tc.nodeExists {
				g.Expect(live.Create(ctx, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: cm.Name}, Status: corev1.NodeStatus{Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionFalse}}}})).To(gomega.Succeed())
			}
			deletes := 0
			liveReads := 0
			cached := interceptor.NewClient(live, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if _, node := obj.(*corev1.Node); node {
						if tc.cacheError != nil {
							return tc.cacheError
						}
						if tc.cachedNode {
							return nil
						}
						return apierrors.NewNotFound(schema.GroupResource{Resource: "nodes"}, key.Name)
					}
					return c.Get(ctx, key, obj, opts...)
				},
				Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
					deletes++
					options := (&client.DeleteOptions{}).ApplyOptions(opts)
					g.Expect(options.Preconditions).NotTo(gomega.BeNil())
					g.Expect(options.Preconditions.UID).To(gomega.Equal(&cm.UID))
					g.Expect(options.Preconditions.ResourceVersion).To(gomega.Equal(&cm.ResourceVersion))
					return c.Delete(ctx, obj, opts...)
				},
			})
			var reader client.Reader = interceptor.NewClient(live, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					liveReads++
					if tc.readError != nil {
						return tc.readError
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			if tc.noReader {
				reader = nil
			}
			orphaned, err := cleanupOrphanedNodeConfigMap(ctx, cached, reader, cm)
			if tc.readError != nil || tc.noReader || tc.cacheError != nil {
				g.Expect(err).To(gomega.HaveOccurred())
				if tc.readError != nil {
					g.Expect(err).To(gomega.MatchError(gomega.ContainSubstring(tc.readError.Error())))
				}
			} else {
				g.Expect(err).NotTo(gomega.HaveOccurred())
			}
			wantOrphan := !tc.nodeExists && !tc.cachedNode && tc.readError == nil && tc.cacheError == nil && !tc.noReader
			if tc.cachedNode || tc.noReader || tc.cacheError != nil {
				g.Expect(liveReads).To(gomega.BeZero())
			} else {
				g.Expect(liveReads).To(gomega.Equal(1))
			}
			g.Expect(orphaned).To(gomega.Equal(wantOrphan))
			getErr := live.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{})
			if wantOrphan {
				g.Expect(apierrors.IsNotFound(getErr)).To(gomega.BeTrue())
				g.Expect(deletes).To(gomega.Equal(1))
				// Retrying a stale cache entry after a successful deletion is harmless.
				orphaned, err = cleanupOrphanedNodeConfigMap(ctx, cached, reader, cm)
				g.Expect(err).NotTo(gomega.HaveOccurred())
				g.Expect(orphaned).To(gomega.BeTrue())
			} else {
				g.Expect(getErr).NotTo(gomega.HaveOccurred())
				g.Expect(deletes).To(gomega.BeZero())
			}
		})
	}
}

func TestOrphanCleanupRejectsUnfencedOrUnrelatedConfigMaps(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.ConfigMap)
	}{
		{"wrong namespace", func(cm *corev1.ConfigMap) { cm.Namespace = "default" }},
		{"unlabeled", func(cm *corev1.ConfigMap) { cm.Labels = nil }},
		{"PVC status", func(cm *corev1.ConfigMap) { cm.Labels = map[string]string{constants.PVCStorageConfigMapLabel: "true"} }},
		{"no UID", func(cm *corev1.ConfigMap) { cm.UID = "" }},
		{"no resourceVersion", func(cm *corev1.ConfigMap) { cm.ResourceVersion = "" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			scheme := runtime.NewScheme()
			g.Expect(corev1.AddToScheme(scheme)).To(gomega.Succeed())
			cm := orphanConfigMap()
			live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm.DeepCopy()).Build()
			c := interceptor.NewClient(live, interceptor.Funcs{
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
					t.Fatal("unsafe ConfigMap reached Delete")
					return nil
				},
			})
			tc.mutate(cm)
			orphaned, err := cleanupOrphanedNodeConfigMap(context.Background(), c, live, cm)
			g.Expect(err).To(gomega.HaveOccurred())
			g.Expect(orphaned).To(gomega.BeFalse())
		})
	}
}

func TestOrphanCleanupErrorsPreserveFinalizerAndStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		onRead bool
		err    error
	}{
		{"node read failure", true, apierrors.NewTimeoutError("lookup", 1)},
		{"changed ConfigMap", false, apierrors.NewConflict(schema.GroupResource{Resource: "configmaps"}, "node", fmt.Errorf("changed"))},
		{"forbidden delete", false, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "node", fmt.Errorf("denied"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()
			scheme := runtime.NewScheme()
			g.Expect(corev1.AddToScheme(scheme)).To(gomega.Succeed())
			g.Expect(v1beta1.AddToScheme(scheme)).To(gomega.Succeed())
			cm := orphanConfigMap()
			now := metav1.Now()
			model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", Finalizers: []string{constants.BaseModelFinalizer}, DeletionTimestamp: &now}}
			live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm, model).Build()
			c := interceptor.NewClient(live, interceptor.Funcs{
				Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error { return tc.err },
			})
			reader := interceptor.NewClient(live, interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if tc.onRead {
						return tc.err
					}
					return c.Get(ctx, key, obj, opts...)
				},
			})
			_, err := HandleModelDeletion(ctx, c, reader, model, constants.BaseModelFinalizer)
			g.Expect(err).To(gomega.HaveOccurred())
			g.Expect(live.Get(ctx, client.ObjectKeyFromObject(model), model)).To(gomega.Succeed())
			g.Expect(controllerutil.ContainsFinalizer(model, constants.BaseModelFinalizer)).To(gomega.BeTrue())
			g.Expect(live.Get(ctx, client.ObjectKeyFromObject(cm), &corev1.ConfigMap{})).To(gomega.Succeed())
			updated := false
			err = processModelStatus(ctx, c, reader, logr.Discard(), "default", "model", false,
				func(context.Context, *shared.ModelConfig) error { updated = true; return nil },
				func(context.Context, []string, []string) error { updated = true; return nil })
			g.Expect(err).To(gomega.HaveOccurred())
			g.Expect(updated).To(gomega.BeFalse())
		})
	}
}

func TestDeletedAcknowledgementDoesNotRequireNodeLookup(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	scheme := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(scheme)).To(gomega.Succeed())
	g.Expect(v1beta1.AddToScheme(scheme)).To(gomega.Succeed())
	cm := orphanConfigMap()
	cm.Data[constants.GetModelConfigMapKey("default", "model", false)] = `{"status":"Deleted"}`
	now := metav1.Now()
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", Finalizers: []string{constants.BaseModelFinalizer}, DeletionTimestamp: &now}}
	live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm, model).Build()
	nodeReads, deletes := 0, 0
	c := interceptor.NewClient(live, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if _, node := obj.(*corev1.Node); node {
				nodeReads++
				return apierrors.NewForbidden(schema.GroupResource{Resource: "nodes"}, key.Name, fmt.Errorf("denied"))
			}
			return c.Get(ctx, key, obj, opts...)
		},
		Delete: func(context.Context, client.WithWatch, client.Object, ...client.DeleteOption) error {
			deletes++
			return fmt.Errorf("acknowledged ConfigMap must not be deleted")
		},
	})
	result, err := HandleModelDeletion(ctx, c, c, model, constants.BaseModelFinalizer)
	g.Expect(nodeReads).To(gomega.BeZero())
	g.Expect(deletes).To(gomega.BeZero())
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(ctrl.Result{}))
	g.Expect(apierrors.IsNotFound(live.Get(ctx, client.ObjectKeyFromObject(model), &v1beta1.BaseModel{}))).To(gomega.BeTrue())
	current := &corev1.ConfigMap{}
	g.Expect(live.Get(ctx, client.ObjectKeyFromObject(cm), current)).To(gomega.Succeed())
	g.Expect(current.Data).To(gomega.Equal(cm.Data))
	g.Expect(current.DeletionTimestamp.IsZero()).To(gomega.BeTrue())
}

func TestModelDeletionWaitsForLiveNodeAfterOrphanCleanup(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	scheme := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(scheme)).To(gomega.Succeed())
	g.Expect(v1beta1.AddToScheme(scheme)).To(gomega.Succeed())
	orphan, liveCM := orphanConfigMap(), orphanConfigMap()
	liveCM.Name, liveCM.UID = "live-node", "live-configmap"
	now := metav1.Now()
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", Finalizers: []string{constants.BaseModelFinalizer}, DeletionTimestamp: &now}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model, orphan, liveCM, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: liveCM.Name}}).Build()

	result, err := HandleModelDeletion(ctx, c, c, model, constants.BaseModelFinalizer)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result.RequeueAfter).To(gomega.Equal(30 * time.Second))
	g.Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(orphan), &corev1.ConfigMap{}))).To(gomega.BeTrue())
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(model), model)).To(gomega.Succeed())
	g.Expect(controllerutil.ContainsFinalizer(model, constants.BaseModelFinalizer)).To(gomega.BeTrue())
	current := &corev1.ConfigMap{}
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(liveCM), current)).To(gomega.Succeed())
	g.Expect(current.Data).To(gomega.Equal(liveCM.Data))
	current.Data[constants.GetModelConfigMapKey("default", "model", false)] = `{"status":"Deleted"}`
	g.Expect(c.Update(ctx, current)).To(gomega.Succeed())

	result, err = HandleModelDeletion(ctx, c, c, model, constants.BaseModelFinalizer)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(ctrl.Result{}))
	g.Expect(apierrors.IsNotFound(c.Get(ctx, client.ObjectKeyFromObject(model), &v1beta1.BaseModel{}))).To(gomega.BeTrue())
	g.Expect(c.Get(ctx, client.ObjectKeyFromObject(liveCM), &corev1.ConfigMap{})).To(gomega.Succeed())
}

func TestPartialOrphanCleanupFailureConvergesOnRetry(t *testing.T) {
	g := gomega.NewWithT(t)
	ctx := context.Background()
	scheme := runtime.NewScheme()
	g.Expect(corev1.AddToScheme(scheme)).To(gomega.Succeed())
	g.Expect(v1beta1.AddToScheme(scheme)).To(gomega.Succeed())
	first, second := orphanConfigMap(), orphanConfigMap()
	first.Name, first.UID = "a-first", "first-configmap"
	second.Name, second.UID = "b-second", "second-configmap"
	now := metav1.Now()
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "default", Finalizers: []string{constants.BaseModelFinalizer}, DeletionTimestamp: &now}}
	live := fake.NewClientBuilder().WithScheme(scheme).WithObjects(model, first, second).Build()
	failOnce := true
	var deletes []string
	c := interceptor.NewClient(live, interceptor.Funcs{
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			if cms, ok := list.(*corev1.ConfigMapList); ok {
				slices.SortFunc(cms.Items, func(a, b corev1.ConfigMap) int { return cmp.Compare(a.Name, b.Name) })
			}
			return nil
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			deletes = append(deletes, obj.GetName())
			if obj.GetName() == second.Name && failOnce {
				failOnce = false
				return apierrors.NewTimeoutError("second ConfigMap deletion failed", 1)
			}
			return c.Delete(ctx, obj, opts...)
		},
	})

	_, err := HandleModelDeletion(ctx, c, live, model, constants.BaseModelFinalizer)
	g.Expect(apierrors.IsTimeout(err)).To(gomega.BeTrue())
	g.Expect(deletes).To(gomega.Equal([]string{first.Name, second.Name}))
	g.Expect(apierrors.IsNotFound(live.Get(ctx, client.ObjectKeyFromObject(first), &corev1.ConfigMap{}))).To(gomega.BeTrue())
	g.Expect(live.Get(ctx, client.ObjectKeyFromObject(second), &corev1.ConfigMap{})).To(gomega.Succeed())
	g.Expect(live.Get(ctx, client.ObjectKeyFromObject(model), model)).To(gomega.Succeed())
	g.Expect(controllerutil.ContainsFinalizer(model, constants.BaseModelFinalizer)).To(gomega.BeTrue())

	result, err := HandleModelDeletion(ctx, c, live, model, constants.BaseModelFinalizer)
	g.Expect(err).NotTo(gomega.HaveOccurred())
	g.Expect(result).To(gomega.Equal(ctrl.Result{}))
	g.Expect(deletes).To(gomega.Equal([]string{first.Name, second.Name, second.Name}))
	g.Expect(apierrors.IsNotFound(live.Get(ctx, client.ObjectKeyFromObject(second), &corev1.ConfigMap{}))).To(gomega.BeTrue())
	g.Expect(apierrors.IsNotFound(live.Get(ctx, client.ObjectKeyFromObject(model), &v1beta1.BaseModel{}))).To(gomega.BeTrue())
}
