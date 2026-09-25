package isvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestValidateCreateRejectsInvalidRouting(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
			Routing: &v1beta1.RoutingSpec{Probe: &v1beta1.RoutingProbeSpec{}},
		},
	}

	_, err := (&InferenceServiceValidator{}).ValidateCreate(context.Background(), isvc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "spec.routing.probe.path is required")
}
