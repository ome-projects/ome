package waitir_test

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitir"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func readySourceFixture(count int32) (*ome.InferenceService, *ome.InferenceReplica) {
	parent := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("parent-uid"), ResourceVersion: "11", Generation: 3,
	}}
	controller := true
	ir := &ome.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat-engine", Namespace: "prod", UID: types.UID("engine-uid"), ResourceVersion: "12", Generation: 2,
			Labels:          map[string]string{constants.InferenceServiceLabel: "chat", constants.OMEComponentLabel: "engine"},
			Annotations:     map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: strconv.FormatInt(parent.Generation, 10)},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: ome.SchemeGroupVersion.String(), Kind: "InferenceService", Name: "chat", UID: parent.UID, Controller: &controller}},
		},
		Spec: ome.InferenceReplicaSpec{ParentRef: ome.ParentReference{Name: "chat"}, Component: ome.EngineComponent},
		Status: ome.InferenceReplicaStatus{
			ObservedGeneration: 2, Replicas: count, ReadyReplicas: count, ServingReplicas: count, AvailableReplicas: count,
		},
	}
	for i := int32(0); i < count; i++ {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, ome.OMENativeInstanceStatus{
			Index: i, Incarnation: 1, Phase: ome.OMENativeInstanceReady,
			PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true,
		})
	}
	return parent, ir
}

func TestReadySourceReadsCurrentOwnedIRAndBindsParent(t *testing.T) {
	parent, ir := readySourceFixture(2)
	client := omefake.NewSimpleClientset(parent, ir)
	source := waitir.NewSource(client.OmeV1beta1(), "prod", "chat", func() time.Time { return time.Unix(3000, 0) })
	snapshot, err := source.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, parent.UID, snapshot.UID)
	require.Equal(t, parent.ResourceVersion, snapshot.ResourceVersion)
	require.False(t, snapshot.Deleting)
	decision, observation := new(waitir.Evaluator).Evaluate(snapshot.Value, v1alpha1.RuntimeComponentEngine, 2)
	require.True(t, decision.Matched)
	require.Equal(t, "Valid", observation.Validity)
	require.NotNil(t, observation.ReadyReplicas)
	require.Equal(t, int32(2), *observation.ReadyReplicas)

	actions := client.Actions()
	require.Len(t, actions, 2)
	get := actions[0].(ktesting.GetAction)
	require.Equal(t, "inferenceservices", get.GetResource().Resource)
	require.Equal(t, "prod", get.GetNamespace())
	require.Equal(t, "chat", get.GetName())
	list := actions[1].(ktesting.ListAction)
	require.Equal(t, "inferencereplicas", list.GetResource().Resource)
	require.Equal(t, "prod", list.GetNamespace())
	value, exact := list.GetListRestrictions().Labels.RequiresExactMatch(constants.InferenceServiceLabel)
	require.True(t, exact)
	require.Equal(t, "chat", value)
}

func TestReadySourceNormalizesColumnarV2BeforeCountDecision(t *testing.T) {
	parent, ir := readySourceFixture(2)
	columns, err := irstatus.EncodeColumns(ir.Status.InstanceStatuses, 2)
	require.NoError(t, err)
	encoding := ome.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatuses = nil
	ir.Status.InstanceStatusEncoding = &encoding
	ir.Status.InstanceStatusColumns = columns
	client := omefake.NewSimpleClientset(parent, ir)
	source := waitir.NewSource(client.OmeV1beta1(), "prod", "chat", time.Now)
	snapshot, err := source.Get(context.Background())
	require.NoError(t, err)
	decision, observation := new(waitir.Evaluator).Evaluate(snapshot.Value, v1alpha1.RuntimeComponentEngine, 2)
	require.True(t, decision.Matched)
	require.Equal(t, "Valid", observation.Validity)
	require.Equal(t, 2, snapshot.Value.Content.Summary.Instances)
}

func TestReadySourceUnobservedZeroNeverMatches(t *testing.T) {
	parent, ir := readySourceFixture(0)
	ir.Status.ObservedGeneration = 0
	client := omefake.NewSimpleClientset(parent, ir)
	snapshot, err := waitir.NewSource(client.OmeV1beta1(), "prod", "chat", time.Now).Get(context.Background())
	require.NoError(t, err)
	decision, observation := new(waitir.Evaluator).Evaluate(snapshot.Value, v1alpha1.RuntimeComponentEngine, 0)
	require.False(t, decision.Matched)
	require.Nil(t, observation.ReadyReplicas)
}
