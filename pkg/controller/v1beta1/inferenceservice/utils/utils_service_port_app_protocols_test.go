package utils

import (
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// The per-revision Services read each Component's appProtocol declarations
// from the merged spec its stable Service is built from, so a runtime's
// declarations count and every role keeps its own under its own type. The
// usual shape is a service block that declares nothing over a runtime that
// declares `http`. A service may blank a runtime value with an empty string;
// the empty value stays in the map and the builders publish nothing for it.
func TestMergedServicePortAppProtocols(t *testing.T) {
	type declarations = map[v1beta1.ComponentType]map[string]string
	const h2c, ws = "kubernetes.io/h2c", "kubernetes.io/ws"
	declare := func(ports map[string]string) v1beta1.ComponentExtensionSpec {
		return v1beta1.ComponentExtensionSpec{ServicePortAppProtocols: ports}
	}
	servingRuntime := &v1beta1.ServingRuntimeSpec{
		EngineConfig:  &v1beta1.EngineSpec{ComponentExtensionSpec: declare(map[string]string{"engine1": ws})},
		DecoderConfig: &v1beta1.DecoderSpec{},
		EncoderConfig: &v1beta1.EncoderSpec{ComponentExtensionSpec: declare(map[string]string{"grpc": h2c})},
		RouterConfig:  &v1beta1.RouterSpec{ComponentExtensionSpec: declare(map[string]string{"http": h2c})},
	}
	merged := func(spec v1beta1.InferenceServiceSpec) v1beta1.RoleSpecs {
		specs, err := MergeRuntimeRoleSpecs(&v1beta1.InferenceService{Spec: spec}, servingRuntime, logr.Discard())
		require.NoError(t, err)
		return specs
	}
	allRoles := v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Decoder: &v1beta1.DecoderSpec{}, Encoder: &v1beta1.EncoderSpec{}, Router: &v1beta1.RouterSpec{}}
	cases := []struct {
		name  string
		specs v1beta1.RoleSpecs
		want  declarations
	}{
		{"a runtime declaration reaches a service block that declares nothing; a role with none is left out",
			merged(v1beta1.InferenceServiceSpec{Decoder: &v1beta1.DecoderSpec{}, Router: &v1beta1.RouterSpec{}}),
			declarations{v1beta1.RouterComponent: {"http": h2c}}},
		{"every role keeps its own declarations", merged(allRoles),
			declarations{v1beta1.EngineComponent: {"engine1": ws}, v1beta1.EncoderComponent: {"grpc": h2c}, v1beta1.RouterComponent: {"http": h2c}}},
		{"a service blanks a runtime value",
			merged(v1beta1.InferenceServiceSpec{Router: &v1beta1.RouterSpec{ComponentExtensionSpec: declare(map[string]string{"http": ""})}}),
			declarations{v1beta1.RouterComponent: {"http": ""}}},
		{"the service's own block alone declares nothing", v1beta1.RoleSpecs{Router: &v1beta1.RouterSpec{}}, declarations{}},
		{"no roles", v1beta1.RoleSpecs{}, declarations{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, MergedServicePortAppProtocols(tc.specs))
		})
	}
}
