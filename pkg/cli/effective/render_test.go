package effective

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

func TestRenderSpecReturnsDeepCopies(t *testing.T) {
	isvc := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
		Engine: &v1beta1.EngineSpec{}, Decoder: &v1beta1.DecoderSpec{}, Router: &v1beta1.RouterSpec{},
	}}
	runtimeSpec := &v1beta1.ServingRuntimeSpec{
		EngineConfig:  &v1beta1.EngineSpec{Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "engine"}}},
		DecoderConfig: &v1beta1.DecoderSpec{}, RouterConfig: &v1beta1.RouterSpec{},
	}
	components, err := MergeEffectiveComponents(isvc, runtimeSpec, nil)
	require.NoError(t, err)
	require.Len(t, components, 3)

	engine, ok := components[0].RenderSpec().(*v1beta1.EngineSpec)
	require.True(t, ok)
	require.NotNil(t, engine.Runner)
	assert.Equal(t, "engine", engine.Runner.Image)
	engine.Runner.Image = "changed"
	assert.Equal(t, "engine", components[0].engine.Runner.Image)

	_, ok = components[1].RenderSpec().(*v1beta1.DecoderSpec)
	assert.True(t, ok)
	_, ok = components[2].RenderSpec().(*v1beta1.RouterSpec)
	assert.True(t, ok)
	assert.Nil(t, EffectiveComponent{}.RenderSpec())
}
