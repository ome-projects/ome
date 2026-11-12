package splitplan

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

var splitMembers = []string{"member-a", "member-b", "member-c"}

// splitService is a Split service with 6 engines, the given decoder minimum
// and 3 routers, divided equally over member-a, member-b and member-c.
func splitService(decoders int) *v1beta1.InferenceService {
	var terms []v1beta1.ClusterAffinityTerm
	for _, name := range splitMembers {
		terms = append(terms, v1beta1.ClusterAffinityTerm{
			Weights: &v1beta1.ClusterAffinityWeights{
				Default: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1), Max: ptr.To[int32](1)},
				Decoder: &v1beta1.ReplicaRatio{Min: ptr.To[int32](1), Max: ptr.To[int32](1)},
				Router:  &v1beta1.ReplicaRatio{Min: ptr.To[int32](1), Max: ptr.To[int32](1)},
			},
			MatchFields: []v1beta1.ClusterSelectorRequirement{{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{name}}},
		})
	}
	runner := func() *v1beta1.RunnerSpec {
		return &v1beta1.RunnerSpec{Container: corev1.Container{Name: "ome-container", Image: "img"}}
	}
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "svc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Placement: &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: v1beta1.PlacementModeSplit, ClusterAffinity: terms},
			Engine:    &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(6)}, Runner: runner()},
			Decoder:   &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(decoders)}, Runner: runner()},
			Router:    &v1beta1.RouterSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(3)}, Runner: runner()},
		},
	}
}

// splitPlanValidator reads the named WorkloadCluster registrations.
func splitPlanValidator(t *testing.T, members ...string) *Validator {
	t.Helper()
	var objects []client.Object
	for _, name := range members {
		objects = append(objects, &v1beta1.WorkloadCluster{ObjectMeta: metav1.ObjectMeta{Name: name}})
	}
	return &Validator{Reader: fakeclient.New(t, fakeclient.WithObjects(objects...))}
}

// Two decoders cannot give each of the three clusters holding engines one.
const heldPlan = "decoder minimum 2 cannot give each of the 3 "

func TestValidatorCreate(t *testing.T) {
	ctx := context.Background()
	v := splitPlanValidator(t, splitMembers...)

	_, err := v.ValidateCreate(ctx, splitService(5))
	require.NoError(t, err, "a plan every cluster can carry is admitted")

	_, err = v.ValidateCreate(ctx, splitService(2))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "the placement planner would hold this Split plan across the 3 registered WorkloadClusters")
	assert.Contains(t, err.Error(), heldPlan)
	assert.Equal(t, 1, strings.Count(err.Error(), heldPlan), "an error every cluster repeats is reported once")

	_, err = splitPlanValidator(t).ValidateCreate(ctx, splitService(2))
	require.NoError(t, err, "without registrations there is nothing to plan against")

	_, err = splitPlanValidator(t, "member-x").ValidateCreate(ctx, splitService(2))
	require.NoError(t, err, "replicas no registration matches stay unassigned until a matching cluster registers")

	_, err = (&Validator{}).ValidateCreate(ctx, splitService(2))
	require.NoError(t, err, "a validator without a Reader admits every service")

	notSplit := splitService(2)
	notSplit.Spec.Placement.Mode = v1beta1.PlacementModeAll
	_, err = v.ValidateCreate(ctx, notSplit)
	require.NoError(t, err, "only mode Split is planned")
}

func TestValidatorAdmitsWhenRegistrationsCannotBeRead(t *testing.T) {
	v := &Validator{Reader: fakeclient.New(t, fakeclient.WithInterceptors(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("connection refused")
		},
	}))}
	warnings, err := v.ValidateCreate(context.Background(), splitService(2))
	require.NoError(t, err)
	require.Len(t, warnings, 1)
	assert.Contains(t, warnings[0], "the Split plan was not checked")
}

func TestValidatorUpdate(t *testing.T) {
	ctx := context.Background()
	v := splitPlanValidator(t, splitMembers...)

	_, err := v.ValidateUpdate(ctx, splitService(5), splitService(2))
	require.Error(t, err, "an update that breaks a plan the planner could carry is refused")
	assert.Contains(t, err.Error(), heldPlan)

	held := splitService(2)
	edited := splitService(2)
	edited.Spec.Engine.MaxReplicas = 12
	_, err = v.ValidateUpdate(ctx, held, edited)
	require.NoError(t, err, "a service the planner already holds keeps accepting edits")

	relabeled := splitService(2)
	relabeled.Labels = map[string]string{"team": "a"}
	_, err = splitPlanValidator(t, splitMembers...).ValidateUpdate(ctx, splitService(2), relabeled)
	require.NoError(t, err, "an update that leaves the spec alone is admitted")

	deleting := splitService(5)
	deleting.DeletionTimestamp = ptr.To(metav1.Now())
	_, err = v.ValidateUpdate(ctx, deleting, splitService(2))
	require.NoError(t, err, "a service under deletion is admitted")
}
