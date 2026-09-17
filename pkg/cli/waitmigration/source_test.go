package waitmigration

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestSourceReadsBoundedOwnedIRsAndKeepsParentBinding(t *testing.T) {
	parent := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("parent-uid"), ResourceVersion: "rv-1", Generation: 3}}
	controller := true
	ir := &ome.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: types.UID("ir-uid"), Generation: 2, Labels: map[string]string{constants.InferenceServicePodLabelKey: "chat"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: parent.UID, Controller: &controller}}},
		Spec:       ome.InferenceReplicaSpec{ParentRef: ome.ParentReference{Name: "chat"}, Component: ome.EngineComponent},
		Status:     ome.InferenceReplicaStatus{ObservedGeneration: 2, Migrations: []ome.MigrationStatus{{RequestUUID: requestID, Trigger: ome.MigrationTriggerManual, SourceInstance: 1, Phase: ome.MigrationPhaseFailed, StartedAt: metav1.NewTime(time.Unix(1000, 0)), Deadline: metav1.NewTime(time.Unix(2000, 0)), CompletedAt: ptrTime(time.Unix(1500, 0))}}},
	}
	client := omefake.NewSimpleClientset(parent, ir)
	source := NewSource(client.OmeV1beta1(), "prod", "chat", func() time.Time { return time.Unix(3000, 0) })
	snapshot, err := source.Get(context.Background())
	require.NoError(t, err)
	require.Equal(t, parent.UID, snapshot.UID)
	require.Equal(t, parent.ResourceVersion, snapshot.ResourceVersion)
	decision, observed := Evaluate(snapshot.Value, requestID)
	require.True(t, decision.Matched)
	require.Equal(t, ome.MigrationPhaseFailed, ome.MigrationPhase(observed.Phase))
	actions := client.Actions()
	require.Len(t, actions, 2)
	get := actions[0].(ktesting.GetAction)
	require.Equal(t, "inferenceservices", get.GetResource().Resource)
	require.Equal(t, "prod", get.GetNamespace())
	require.Equal(t, "chat", get.GetName())
	list := actions[1].(ktesting.ListAction)
	require.Equal(t, "inferencereplicas", list.GetResource().Resource)
	value, exact := list.GetListRestrictions().Labels.RequiresExactMatch(constants.InferenceServicePodLabelKey)
	require.True(t, exact)
	require.Equal(t, "chat", value)
}

func ptrTime(value time.Time) *metav1.Time { v := metav1.NewTime(value); return &v }
