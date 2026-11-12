package endpoint

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// The service's publisher options: spec.routing.publisher.options together
// with the ome.io/routing-publisher.<option> annotations.

func TestRoutingPublisherOptions_AnnotationsJoinSpecOptions(t *testing.T) {
	annotationFor := func(option string) string {
		return constants.RoutingPublisherOptionAnnotationPrefix + option
	}
	tooMany := make(map[string]string, v1beta1.MaxRoutingPublisherOptions+1)
	for i := 0; i <= v1beta1.MaxRoutingPublisherOptions; i++ {
		tooMany[annotationFor(fmt.Sprintf("key-%d", i))] = "value"
	}
	tests := []struct {
		name        string
		spec        map[string]string
		annotations map[string]string
		want        map[string]string
		wantErr     string
	}{
		{name: "neither", want: map[string]string{}},
		{
			name: "spec only",
			spec: map[string]string{"routingClass": "standard"},
			want: map[string]string{"routingClass": "standard"},
		},
		{
			name:        "annotation only",
			annotations: map[string]string{annotationFor("routingClass"): "premium"},
			want:        map[string]string{"routingClass": "premium"},
		},
		{
			name:        "distinct options from both",
			spec:        map[string]string{"tier": "a"},
			annotations: map[string]string{annotationFor("routingClass"): "premium"},
			want:        map[string]string{"tier": "a", "routingClass": "premium"},
		},
		{
			name: "other annotations are not options",
			annotations: map[string]string{
				constants.TrafficDrainAnnotation:                          `{}`,
				constants.OMEAPIGroupName + "/routing-publisher-tier":     "a",
				"example.com/routing-publisher.tier":                      "a",
				constants.RoutingPublisherOptionAnnotationPrefix + "tier": "b",
			},
			want: map[string]string{"tier": "b"},
		},
		{
			name:        "the same option in both places",
			spec:        map[string]string{"routingClass": "standard"},
			annotations: map[string]string{annotationFor("routingClass"): "premium"},
			wantErr:     `publisher option "routingClass" is set both in spec.routing.publisher.options and in annotation`,
		},
		{
			name:        "a blank value",
			annotations: map[string]string{annotationFor("routingClass"): " "},
			wantErr:     "must not be empty or whitespace-only",
		},
		{
			name:        "a value past the limit",
			annotations: map[string]string{annotationFor("routingClass"): strings.Repeat("v", v1beta1.MaxRoutingPublisherOptionValueBytes+1)},
			wantErr:     "byte value limit",
		},
		{
			name:        "more options than the limit",
			annotations: tooMany,
			wantErr:     "at most",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, _ := publisherTestObjects()
			owner.Annotations = tt.annotations
			if tt.spec != nil {
				owner.Spec.Routing = &v1beta1.RoutingSpec{Publisher: &v1beta1.RoutingPublisherSpec{Options: tt.spec}}
			}
			got, err := routingPublisherOptions(owner)
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
			got["mutated"] = "caller"
			if tt.spec != nil {
				assert.NotContains(t, owner.Spec.Routing.Publisher.Options, "mutated",
					"the options must be a copy, not the spec's map")
			}
		})
	}

	got, err := routingPublisherOptions(nil)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{}, got)
}

func TestTrafficMapPublisher_AnnotationOption_ReachesResolverAndOptionsDigest(t *testing.T) {
	owner, trafficMap := publisherTestObjects()
	owner.Annotations = map[string]string{
		constants.RoutingPublisherOptionAnnotationPrefix + "routingClass": "premium",
	}
	trafficMap.Finalizers = []string{TrafficMapPublisherFinalizer}
	trafficMap.Status.Publisher = &v1beta1.TrafficMapPublisherStatus{PublisherName: "example-publisher"}
	trafficMap.Status.Conditions = []metav1.Condition{{
		Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue,
		Reason: "Routable", ObservedGeneration: trafficMap.Generation,
	}}
	publisher := &recordingTrafficMapPublisher{
		name: "example-publisher", stateful: true, claims: []string{"target-a"},
		resolve: func(global, inline map[string]string) (map[string]string, error) {
			assert.Equal(t, map[string]string{"routingClass": "premium"}, inline)
			effective := clonePublisherOptions(global)
			effective["routingClass"] = inline["routingClass"]
			return effective, nil
		},
	}
	reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)
	reconciler.GlobalOptions = map[string]string{"routingClass": "standard"}
	reconciler.RequeueAfter = time.Minute

	_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
	require.NoError(t, err)
	assert.Equal(t, []string{"apply"}, publisher.events)
	current := getPublisherTrafficMap(t, kubeClient, trafficMap)
	require.NotNil(t, current.Status.Publisher)
	wantDigest, err := publisherOptionsDigest("example-publisher", map[string]string{"routingClass": "premium"})
	require.NoError(t, err)
	assert.Equal(t, wantDigest, current.Status.Publisher.ObservedOptionsDigest,
		"the status records the options the annotation chose")
	assert.True(t, current.Status.Published)
}

func TestTrafficMapPublisher_InvalidOptionAnnotation_ReportsInvalidOptionsNoEffects(t *testing.T) {
	tests := []struct {
		name        string
		spec        map[string]string
		annotations map[string]string
	}{
		{
			name:        "the same option in the spec",
			spec:        map[string]string{"routingClass": "standard"},
			annotations: map[string]string{constants.RoutingPublisherOptionAnnotationPrefix + "routingClass": "premium"},
		},
		{
			name:        "a blank value",
			annotations: map[string]string{constants.RoutingPublisherOptionAnnotationPrefix + "routingClass": ""},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			owner, trafficMap := publisherTestObjects()
			if tt.spec != nil {
				owner.Spec.Routing = &v1beta1.RoutingSpec{Publisher: &v1beta1.RoutingPublisherSpec{Options: tt.spec}}
			}
			owner.Annotations = tt.annotations
			publisher := &recordingTrafficMapPublisher{
				name: "example-publisher", stateful: true, claims: []string{"target-a"},
				resolve: func(map[string]string, map[string]string) (map[string]string, error) {
					t.Fatal("an invalid option annotation must be refused before the publisher resolves options")
					return nil, nil
				},
			}
			reconciler, kubeClient := newTrafficMapPublisherReconciler(t, publisher, owner, trafficMap)

			_, err := reconcileTrafficMapPublisher(t, reconciler, trafficMap)
			require.NoError(t, err)
			assert.Empty(t, publisher.events)
			current := getPublisherTrafficMap(t, kubeClient, trafficMap)
			assert.Nil(t, current.Status.Publisher)
			condition := apimeta.FindStatusCondition(current.Status.Conditions, v1beta1.TrafficMapPublished)
			require.NotNil(t, condition)
			assert.Equal(t, metav1.ConditionFalse, condition.Status)
			assert.Equal(t, trafficMapPublisherReasonInvalidOptions, condition.Reason)
		})
	}
}
