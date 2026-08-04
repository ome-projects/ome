package modelagent

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

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
