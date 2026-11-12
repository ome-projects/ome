package inferenceservice

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// A decoder removed from the spec takes its whole status entry with it. The
// flush keeps the live lifecycle of every Component the pass still reports,
// and never recreates an entry the pass removed.
func TestUpdateStatus_ComponentRemovedFromTheSpec_LeavesNoStatusEntry(t *testing.T) {
	live := modeChangeISVCObject()
	live.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent:  {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 12, ReadyReplicas: 12}},
		v1beta1.DecoderComponent: {Lifecycle: &v1beta1.LifecycleStatus{Replicas: 8, ReadyReplicas: 8}},
	}
	r, c, _ := newModeChangeReconciler(t, live)
	ctx := modeChangeContext()
	key := client.ObjectKeyFromObject(live)

	// The pass's copy after cleanupUndeclaredComponents: the decoder is gone
	// from the spec and from the status.
	desired := live.DeepCopy()
	delete(desired.Status.Components, v1beta1.DecoderComponent)

	// An engine write that landed between the pass's read and its flush.
	fresh := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(ctx, key, fresh))
	engine := fresh.Status.Components[v1beta1.EngineComponent]
	engine.Lifecycle.ReadyReplicas = 11
	fresh.Status.Components[v1beta1.EngineComponent] = engine
	require.NoError(t, c.Status().Update(ctx, fresh))

	require.NoError(t, r.updateStatus(desired, constants.OMENative, nil, groupEntriesComputed))

	after := &v1beta1.InferenceService{}
	require.NoError(t, c.Get(ctx, key, after))
	assert.NotContains(t, after.Status.Components, v1beta1.DecoderComponent, "the removed decoder keeps no status entry")
	require.Contains(t, after.Status.Components, v1beta1.EngineComponent)
	assert.Equal(t, int32(11), after.Status.Components[v1beta1.EngineComponent].Lifecycle.ReadyReplicas, "the engine keeps the lifecycle written after the pass's read")
}
