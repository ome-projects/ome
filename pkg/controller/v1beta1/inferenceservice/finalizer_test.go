package inferenceservice

import (
	"context"
	"testing"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	kubernetesfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func TestReconcileRetriesFinalizerConflict(t *testing.T) {
	const finalizerName = "inferenceservice.finalizers"

	tests := []struct {
		name     string
		deleting bool
	}{
		{name: "add finalizer", deleting: false},
		{name: "remove finalizer", deleting: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			g := gomega.NewWithT(t)
			ctx := context.Background()
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "finalizer-conflict",
					Namespace: "test",
					Annotations: map[string]string{
						constants.DeploymentMode: string(constants.VirtualDeployment),
					},
				},
			}
			if test.deleting {
				isvc.Finalizers = []string{finalizerName}
			}

			live := fakeclient.New(t, fakeclient.WithObjects(isvc), fakeclient.WithStatusSubresource(isvc))
			key := client.ObjectKeyFromObject(isvc)
			if test.deleting {
				g.Expect(live.Delete(ctx, isvc)).To(gomega.Succeed())
			}

			stale := &v1beta1.InferenceService{}
			g.Expect(live.Get(ctx, key, stale)).To(gomega.Succeed())
			latest := stale.DeepCopy()
			latest.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.EngineComponent: {},
			}
			g.Expect(live.Status().Update(ctx, latest)).To(gomega.Succeed())

			staleGet := true
			updates := fakeclient.CountingVerb(fakeclient.Update)
			cached := fakeclient.Wrap(t, live, updates, fakeclient.WithInterceptors(interceptor.Funcs{
				Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
					if staleGet && key == client.ObjectKeyFromObject(stale) {
						if out, ok := obj.(*v1beta1.InferenceService); ok {
							staleGet = false
							stale.DeepCopyInto(out)
							return nil
						}
					}
					return c.Get(ctx, key, obj, opts...)
				},
			}))

			configMap := &corev1.ConfigMap{
				ObjectMeta: metav1.ObjectMeta{
					Name:      constants.InferenceServiceConfigMapName,
					Namespace: constants.OMENamespace,
				},
				Data: map[string]string{
					controllerconfig.DeployConfigName: `{"defaultDeploymentMode":"RawDeployment"}`,
				},
			}
			reconciler := &InferenceServiceReconciler{
				Client:    cached,
				APIReader: live,
				Clientset: kubernetesfake.NewSimpleClientset(configMap),
				Log:       ctrl.Log.WithName("test"),
				Recorder:  record.NewFakeRecorder(10),
			}
			request := ctrl.Request{NamespacedName: types.NamespacedName(key)}

			result, err := reconciler.Reconcile(ctx, request)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(result.Requeue).To(gomega.BeTrue())
			g.Expect(updates.Count()).To(gomega.Equal(1))

			stored := &v1beta1.InferenceService{}
			g.Expect(live.Get(ctx, key, stored)).To(gomega.Succeed())
			g.Expect(controllerutil.ContainsFinalizer(stored, finalizerName)).To(gomega.Equal(test.deleting))

			result, err = reconciler.Reconcile(ctx, request)
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(result.Requeue).To(gomega.BeFalse())

			stored = &v1beta1.InferenceService{}
			err = live.Get(ctx, key, stored)
			if test.deleting {
				g.Expect(apierrors.IsNotFound(err)).To(gomega.BeTrue())
				return
			}
			g.Expect(err).NotTo(gomega.HaveOccurred())
			g.Expect(controllerutil.ContainsFinalizer(stored, finalizerName)).To(gomega.BeTrue())
		})
	}
}
