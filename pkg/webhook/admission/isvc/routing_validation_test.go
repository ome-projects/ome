package isvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
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

func TestValidateCreateRejectsRoutingPublisherOptionSetTwice(t *testing.T) {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default", Annotations: map[string]string{
			constants.RoutingPublisherOptionAnnotationPrefix + "routingClass": "premium",
		}},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
			Routing: &v1beta1.RoutingSpec{Publisher: &v1beta1.RoutingPublisherSpec{
				Options: map[string]string{"routingClass": "standard"},
			}},
		},
	}

	_, err := (&InferenceServiceValidator{}).ValidateCreate(context.Background(), isvc)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "is set both in spec.routing.publisher.options and in annotation")
}

func TestValidateUpdateRejectsBlankRoutingPublisherOptionAnnotation(t *testing.T) {
	old := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
		Spec:       v1beta1.InferenceServiceSpec{Runtime: &v1beta1.ServingRuntimeRef{Name: "test-runtime"}},
	}
	updated := old.DeepCopy()
	updated.Annotations = map[string]string{constants.RoutingPublisherOptionAnnotationPrefix + "routingClass": " "}

	_, err := (&InferenceServiceValidator{}).ValidateUpdate(context.Background(), old, updated)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "must not be empty or whitespace-only")
}
