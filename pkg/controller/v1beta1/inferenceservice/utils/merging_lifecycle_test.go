package utils

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// TestMergeSpecs_LeaderGoneGracePeriod: a component takes
// lifecycle.leaderGoneGracePeriod from its ServingRuntime unless the
// InferenceService sets one, as the other fields of the lifecycle block do.
// With neither setting it the merged component carries none.
func TestMergeSpecs_LeaderGoneGracePeriod(t *testing.T) {
	grace := func(d time.Duration) *metav1.Duration { return &metav1.Duration{Duration: d} }
	with := func(d time.Duration) v1beta1.ComponentExtensionSpec {
		return v1beta1.ComponentExtensionSpec{Lifecycle: &v1beta1.LifecycleSpec{LeaderGoneGracePeriod: grace(d)}}
	}
	runtimeEngine := &v1beta1.EngineSpec{ComponentExtensionSpec: with(30 * time.Second)}

	t.Run("a service that sets none takes the runtime's", func(t *testing.T) {
		merged, err := MergeEngineSpec(runtimeEngine, &v1beta1.EngineSpec{})
		assert.NoError(t, err)
		if assert.NotNil(t, merged.Lifecycle) {
			assert.Equal(t, grace(30*time.Second), merged.Lifecycle.LeaderGoneGracePeriod)
		}
	})

	t.Run("the service's value wins", func(t *testing.T) {
		merged, err := MergeEngineSpec(runtimeEngine, &v1beta1.EngineSpec{ComponentExtensionSpec: with(45 * time.Second)})
		assert.NoError(t, err)
		if assert.NotNil(t, merged.Lifecycle) {
			assert.Equal(t, grace(45*time.Second), merged.Lifecycle.LeaderGoneGracePeriod)
		}
	})

	t.Run("a service that sets another field of the block keeps the runtime's grace", func(t *testing.T) {
		policy := v1beta1.InstanceRestartPolicyRecreateInstance
		service := &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			Lifecycle: &v1beta1.LifecycleSpec{RestartPolicy: &policy},
		}}
		merged, err := MergeEngineSpec(runtimeEngine, service)
		assert.NoError(t, err)
		if assert.NotNil(t, merged.Lifecycle) {
			assert.Equal(t, grace(30*time.Second), merged.Lifecycle.LeaderGoneGracePeriod)
			assert.Equal(t, &policy, merged.Lifecycle.RestartPolicy)
		}
	})

	t.Run("with neither setting it there is none", func(t *testing.T) {
		merged, err := MergeEngineSpec(&v1beta1.EngineSpec{}, &v1beta1.EngineSpec{})
		assert.NoError(t, err)
		assert.True(t, merged.Lifecycle == nil || merged.Lifecycle.LeaderGoneGracePeriod == nil)
	})

	t.Run("a decoder takes the runtime's the same way", func(t *testing.T) {
		merged, err := MergeDecoderSpec(&v1beta1.DecoderSpec{ComponentExtensionSpec: with(30 * time.Second)}, &v1beta1.DecoderSpec{})
		assert.NoError(t, err)
		if assert.NotNil(t, merged.Lifecycle) {
			assert.Equal(t, grace(30*time.Second), merged.Lifecycle.LeaderGoneGracePeriod)
		}
	})
}
