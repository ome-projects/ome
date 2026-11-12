package inferenceservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// The test: the sweep after a Component is removed leaves an owned object
// that names no Component alone; the model ConfigMap is one.
func TestCleanupRemovedComponentsKeepsAnOwnedObjectOfNoComponent(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default", UID: "test-uid"}}
	modelConfig := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name:      constants.ModelConfigName(isvc.Name),
		Namespace: isvc.Namespace,
		Labels:    map[string]string{constants.InferenceServicePodLabelKey: isvc.Name},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
			Name: isvc.Name, UID: isvc.UID, Controller: ptr.To(true),
		}},
	}}
	decoder := createDeployment("test-isvc-decoder", "default", "test-isvc", "test-uid", v1beta1.DecoderComponent)
	cl := fakeclient.New(t, fakeclient.WithObjects(isvc, modelConfig, decoder))
	r := &InferenceServiceReconciler{Client: cl, Clientset: fake.NewSimpleClientset()}
	ctx := log.IntoContext(context.Background(), log.Log)

	require.NoError(t, r.cleanupRemovedComponents(ctx, isvc, v1beta1.RoleSpecs{Engine: &v1beta1.EngineSpec{}}))
	require.NoError(t, cl.Get(ctx, types.NamespacedName{Name: modelConfig.Name, Namespace: modelConfig.Namespace}, &corev1.ConfigMap{}), "an owned object of no Component survives the sweep")
	require.Error(t, cl.Get(ctx, client.ObjectKeyFromObject(decoder), decoder.DeepCopy()), "the removed decoder's Deployment is deleted")
}
