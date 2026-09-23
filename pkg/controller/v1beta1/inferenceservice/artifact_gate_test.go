package inferenceservice

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestPendingArtifactGateCreatesNoWorkloads(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	require.NoError(t, appsv1.AddToScheme(scheme))
	require.NoError(t, corev1.AddToScheme(scheme))
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "pending", Namespace: "ns", Annotations: map[string]string{constants.ArtifactStartupGateAnnotation: constants.ArtifactStartupGatePending}}}
	c := fake.NewClientBuilder().WithScheme(scheme).WithStatusSubresource(isvc).WithObjects(isvc).Build()
	r := &InferenceServiceReconciler{Client: c, APIReader: c, Scheme: scheme, Log: logr.Discard()}
	result, err := r.Reconcile(context.Background(), ctrl.Request{NamespacedName: client.ObjectKeyFromObject(isvc)})
	require.NoError(t, err)
	require.Positive(t, result.RequeueAfter)
	list := &appsv1.DeploymentList{}
	require.NoError(t, c.List(context.Background(), list))
	require.Empty(t, list.Items)
}
