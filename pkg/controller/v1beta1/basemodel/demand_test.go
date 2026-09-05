package basemodel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestReconcileModelDownloadDemandProjectsBoundedReferenceCount(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	model := &v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "models"}}
	baseModelKind := constants.BaseModel
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "models"},
		Spec: v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{
			Name: model.Name,
			Kind: &baseModelKind,
		}},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.BaseModel{}).
		WithIndex(&v1beta1.InferenceService{}, modelDemandIndexField, testModelDemandIndex).
		WithObjects(model, isvc).
		Build()

	changed, err := reconcileModelDownloadDemand(context.Background(), c, model, false)
	require.NoError(t, err)
	assert.True(t, changed)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), model))
	require.NotNil(t, model.Status.DownloadScheduling)
	assert.True(t, model.Status.DownloadScheduling.ServingDemand)
	assert.Equal(t, int32(1), model.Status.DownloadScheduling.ReferenceCount)

	require.NoError(t, c.Delete(context.Background(), isvc))
	changed, err = reconcileModelDownloadDemand(context.Background(), c, model, false)
	require.NoError(t, err)
	assert.True(t, changed)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), model))
	assert.Nil(t, model.Status.DownloadScheduling)
}

func TestModelReferenceUpdateIndexesOldAndNewModels(t *testing.T) {
	baseModelKind := constants.BaseModel
	oldISVC := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "endpoint", Namespace: "models"},
		Spec:       v1beta1.InferenceServiceSpec{Model: &v1beta1.ModelRef{Name: "old", Kind: &baseModelKind}},
	}
	newISVC := oldISVC.DeepCopy()
	newISVC.Spec.Model.Name = "new"

	oldRef, oldOK := modelReferenceForInferenceService(oldISVC)
	newRef, newOK := modelReferenceForInferenceService(newISVC)
	require.True(t, oldOK)
	require.True(t, newOK)
	assert.Equal(t, "models/old", oldRef.key.String())
	assert.Equal(t, "models/new", newRef.key.String())
	assert.NotEqual(t, modelDemandIndexValue(oldRef), modelDemandIndexValue(newRef))

	queue := workqueue.NewTypedRateLimitingQueue(workqueue.DefaultTypedControllerRateLimiter[reconcile.Request]())
	t.Cleanup(queue.ShutDown)
	modelDemandEventHandler(constants.BaseModel).Update(
		context.Background(),
		event.UpdateEvent{ObjectOld: oldISVC, ObjectNew: newISVC},
		queue,
	)
	requests := make(map[string]struct{}, 2)
	for queue.Len() > 0 {
		request, _ := queue.Get()
		requests[request.NamespacedName.String()] = struct{}{}
		queue.Done(request)
	}
	assert.Contains(t, requests, "models/old")
	assert.Contains(t, requests, "models/new")
}

func TestDisabledServingDemandPriorityClearsBaseModelScheduling(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	model := &v1beta1.BaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model", Namespace: "models"},
		Status: v1beta1.ModelStatusSpec{
			State:      v1beta1.LifeCycleStateReady,
			NodesReady: []string{"node-a"},
			DownloadScheduling: &v1beta1.ModelDownloadSchedulingStatus{
				ServingDemand:  true,
				ReferenceCount: 1,
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.BaseModel{}).
		WithObjects(model).
		Build()

	changed, err := reconcileModelDownloadScheduling(context.Background(), c, model, false, false)
	require.NoError(t, err)
	assert.True(t, changed)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), model))
	assert.Nil(t, model.Status.DownloadScheduling)
	assert.Equal(t, v1beta1.LifeCycleStateReady, model.Status.State)
	assert.Equal(t, []string{"node-a"}, model.Status.NodesReady)

	changed, err = reconcileModelDownloadScheduling(context.Background(), c, model, false, false)
	require.NoError(t, err)
	assert.False(t, changed)
}

func TestDisabledServingDemandPriorityClearsClusterBaseModelScheduling(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1beta1.AddToScheme(scheme))
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model"},
		Status: v1beta1.ModelStatusSpec{
			State:       v1beta1.LifeCycleStateReady,
			NodesFailed: []string{"node-b"},
			DownloadScheduling: &v1beta1.ModelDownloadSchedulingStatus{
				ServingDemand:  true,
				ReferenceCount: 2,
			},
		},
	}
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.ClusterBaseModel{}).
		WithObjects(model).
		Build()

	changed, err := reconcileModelDownloadScheduling(context.Background(), c, model, true, false)
	require.NoError(t, err)
	assert.True(t, changed)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(model), model))
	assert.Nil(t, model.Status.DownloadScheduling)
	assert.Equal(t, v1beta1.LifeCycleStateReady, model.Status.State)
	assert.Equal(t, []string{"node-b"}, model.Status.NodesFailed)

	changed, err = reconcileModelDownloadScheduling(context.Background(), c, model, true, false)
	require.NoError(t, err)
	assert.False(t, changed)
}

func testModelDemandIndex(obj client.Object) []string {
	ref, ok := modelReferenceForInferenceService(obj)
	if !ok {
		return nil
	}
	return []string{modelDemandIndexValue(ref)}
}
