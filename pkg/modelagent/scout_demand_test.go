package modelagent

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/tools/cache"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omev1beta1lister "sigs.k8s.io/ome/pkg/client/listers/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

type blockingInferenceServiceLister struct {
	items       []*v1beta1.InferenceService
	listEntered chan<- struct{}
	releaseList <-chan struct{}
	once        sync.Once
}

func (l *blockingInferenceServiceLister) List(labels.Selector) ([]*v1beta1.InferenceService, error) {
	l.once.Do(func() { close(l.listEntered) })
	<-l.releaseList
	return l.items, nil
}

func (l *blockingInferenceServiceLister) InferenceServices(string) omev1beta1lister.InferenceServiceNamespaceLister {
	return nil
}

func TestScoutTracksSharedEndpointDemandByReferenceCount(t *testing.T) {
	scout := &Scout{
		endpointDemands: make(map[string]string),
		demandCounts:    make(map[string]int),
	}
	modelKey := modelDemandKey(constants.ClusterBaseModel, "", "model")

	assert.True(t, scout.setEndpointDemand("ns/first", modelKey))
	assert.True(t, scout.setEndpointDemand("ns/second", modelKey))
	assert.True(t, scout.isModelDemanded(modelKey))

	assert.False(t, scout.setEndpointDemand("ns/first", ""))
	assert.True(t, scout.isModelDemanded(modelKey))
	assert.False(t, scout.setEndpointDemand("ns/second", ""))
	assert.False(t, scout.isModelDemanded(modelKey))
}

func TestInferenceServiceModelReferenceValidation(t *testing.T) {
	baseKind := "basemodel"
	clusterKind := constants.ClusterBaseModel
	omeAPIGroup := constants.OMEAPIGroupName
	unsupportedKind := "CustomModel"
	unsupportedAPIGroup := "example.com"

	tests := []struct {
		name          string
		model         *v1beta1.ModelRef
		expectedKind  string
		expectedName  string
		expectedValid bool
	}{
		{name: "default cluster model", model: &v1beta1.ModelRef{Name: " model "}, expectedKind: constants.ClusterBaseModel, expectedName: "model", expectedValid: true},
		{name: "case insensitive base model", model: &v1beta1.ModelRef{Name: "model", Kind: &baseKind}, expectedKind: constants.BaseModel, expectedName: "model", expectedValid: true},
		{name: "explicit OME API group", model: &v1beta1.ModelRef{Name: "model", Kind: &clusterKind, APIGroup: &omeAPIGroup}, expectedKind: constants.ClusterBaseModel, expectedName: "model", expectedValid: true},
		{name: "empty name", model: &v1beta1.ModelRef{}, expectedValid: false},
		{name: "unsupported kind", model: &v1beta1.ModelRef{Name: "model", Kind: &unsupportedKind}, expectedValid: false},
		{name: "unsupported API group", model: &v1beta1.ModelRef{Name: "model", APIGroup: &unsupportedAPIGroup}, expectedValid: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			kind, name, valid := inferenceServiceModelReference(test.model)
			assert.Equal(t, test.expectedValid, valid)
			if test.expectedValid {
				require.NotEmpty(t, kind)
				assert.Equal(t, test.expectedKind, kind)
				assert.Equal(t, test.expectedName, name)
			}
		})
	}
}

func TestRefreshAllEndpointDemandsDoesNotOverwriteConcurrentUpdate(t *testing.T) {
	oldInferenceService := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: "endpoint"},
		Spec: v1beta1.InferenceServiceSpec{
			Model: &v1beta1.ModelRef{Name: "old-model"},
		},
	}
	newInferenceService := oldInferenceService.DeepCopy()
	newInferenceService.Spec.Model.Name = "new-model"

	listEntered := make(chan struct{})
	releaseList := make(chan struct{})
	scout := &Scout{
		inferenceServiceLister: &blockingInferenceServiceLister{
			items:       []*v1beta1.InferenceService{oldInferenceService},
			listEntered: listEntered,
			releaseList: releaseList,
		},
		clusterBaseModelLister: omev1beta1lister.NewClusterBaseModelLister(
			cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})),
		logger:          zap.NewNop().Sugar(),
		endpointDemands: make(map[string]string),
		demandCounts:    make(map[string]int),
	}

	refreshDone := make(chan struct{})
	go func() {
		scout.refreshAllEndpointDemands()
		close(refreshDone)
	}()
	<-listEntered

	// On the broken implementation, refresh does not own demandMu while List is
	// blocked. Complete the newer update before releasing the stale snapshot so
	// the rebuild deterministically overwrites it. On the fixed implementation,
	// the update waits for the atomic snapshot replay and is applied last.
	refreshOwnsDemandLock := !scout.demandMu.TryLock()
	if !refreshOwnsDemandLock {
		scout.demandMu.Unlock()
	}
	updateDone := make(chan struct{})
	go func() {
		scout.addOrUpdateInferenceService(newInferenceService)
		close(updateDone)
	}()
	if !refreshOwnsDemandLock {
		<-updateDone
	}
	close(releaseList)
	<-refreshDone
	<-updateDone

	oldModelKey := modelDemandKey(constants.ClusterBaseModel, "", "old-model")
	newModelKey := modelDemandKey(constants.ClusterBaseModel, "", "new-model")
	assert.Equal(t, newModelKey, scout.endpointDemands["ns/endpoint"])
	assert.False(t, scout.isModelDemanded(oldModelKey))
	assert.True(t, scout.isModelDemanded(newModelKey))
}
