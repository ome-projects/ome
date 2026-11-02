package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestDetermineDeploymentModes_PerComponentAnnotations(t *testing.T) {
	omeAnnot := map[string]string{
		constants.DeploymentMode: string(constants.OMENative),
	}
	rawAnnot := map[string]string{
		constants.DeploymentMode: string(constants.RawDeployment),
	}

	tests := []struct {
		name           string
		engine         *v1beta1.EngineSpec
		decoder        *v1beta1.DecoderSpec
		router         *v1beta1.RouterSpec
		wantEngineMode constants.DeploymentModeType
		wantDecoder    constants.DeploymentModeType
		wantRouter     constants.DeploymentModeType
	}{
		{
			name: "Engine-only OMENative; absent Decoder/Router default to Raw",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "Decoder annotation = OMENative is respected (foundation for PD)",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.OMENative,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "Router annotation = OMENative is respected",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			router: &v1beta1.RouterSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.OMENative,
		},
		{
			name: "Three-way PD: Engine + Decoder + Router all opt into OMENative",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			router: &v1beta1.RouterSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.OMENative,
			wantRouter:     constants.OMENative,
		},
		{
			name: "Mixed: Engine OMENative, Decoder Raw (explicit)",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: rawAnnot},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "Decoder with Leader/Worker but no annotation falls back to OMENative",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: rawAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{},
			},
			wantEngineMode: constants.RawDeployment,
			wantDecoder:    constants.OMENative,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "Decoder annotation takes precedence over Leader/Worker presence",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
				Leader:                 &v1beta1.LeaderSpec{},
				Worker:                 &v1beta1.WorkerSpec{},
			},
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.OMENative,
			wantRouter:     constants.RawDeployment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modes, err := DetermineRoleDeploymentModes(v1beta1.RoleSpecs{Engine: tt.engine, Decoder: tt.decoder, Router: tt.router}, nil)
			assert.NoError(t, err)
			engineMode, decoderMode, routerMode := modeOrRaw(modes, v1beta1.EngineComponent), modeOrRaw(modes, v1beta1.DecoderComponent), modeOrRaw(modes, v1beta1.RouterComponent)
			assert.Equal(t, tt.wantEngineMode, engineMode, "engine mode mismatch")
			assert.Equal(t, tt.wantDecoder, decoderMode, "decoder mode mismatch")
			assert.Equal(t, tt.wantRouter, routerMode, "router mode mismatch")

			// The three-role signature resolves every role the same way.
			engineMode, decoderMode, routerMode, err = DetermineDeploymentModes(tt.engine, tt.decoder, tt.router, nil, nil)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantEngineMode, engineMode, "three-role engine mode mismatch")
			assert.Equal(t, tt.wantDecoder, decoderMode, "three-role decoder mode mismatch")
			assert.Equal(t, tt.wantRouter, routerMode, "three-role router mode mismatch")
		})
	}
}

func TestDetermineDeploymentModes_NilEngineRejected(t *testing.T) {
	_, err := DetermineRoleDeploymentModes(v1beta1.RoleSpecs{}, nil)
	assert.Error(t, err)
	_, _, _, err = DetermineDeploymentModes(nil, &v1beta1.DecoderSpec{}, nil, nil, nil)
	assert.Error(t, err)
}

// modeOrRaw reads a role's mode, RawDeployment for a role the specs do not
// declare: the mode an undeclared role's dispatch would resolve to.
func modeOrRaw(modes map[v1beta1.ComponentType]constants.DeploymentModeType, c v1beta1.ComponentType) constants.DeploymentModeType {
	if mode, ok := modes[c]; ok {
		return mode
	}
	return constants.RawDeployment
}

// TestDetermineDeploymentModes_SpecDeploymentModeField covers the
// precedence chain that includes the new typed spec.deploymentMode
// field: per-Component annotation > spec.deploymentMode > Leader/Worker
// shape (OMENative) > RawDeployment default.
func TestDetermineDeploymentModes_SpecDeploymentModeField(t *testing.T) {
	omeNativePtr := func() *constants.DeploymentModeType {
		m := constants.OMENative
		return &m
	}
	rawDeploymentPtr := func() *constants.DeploymentModeType {
		m := constants.RawDeployment
		return &m
	}
	omeAnnot := map[string]string{
		constants.DeploymentMode: string(constants.OMENative),
	}
	rawAnnot := map[string]string{
		constants.DeploymentMode: string(constants.RawDeployment),
	}

	tests := []struct {
		name           string
		engine         *v1beta1.EngineSpec
		decoder        *v1beta1.DecoderSpec
		router         *v1beta1.RouterSpec
		specMode       *constants.DeploymentModeType
		wantEngineMode constants.DeploymentModeType
		wantDecoder    constants.DeploymentModeType
		wantRouter     constants.DeploymentModeType
	}{
		{
			name:           "spec.deploymentMode=OMENative propagates to bare Engine",
			engine:         &v1beta1.EngineSpec{},
			specMode:       omeNativePtr(),
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.RawDeployment,
		},
		{
			name:           "spec.deploymentMode=OMENative propagates to PD shape (Engine + Decoder)",
			engine:         &v1beta1.EngineSpec{},
			decoder:        &v1beta1.DecoderSpec{},
			specMode:       omeNativePtr(),
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.OMENative,
			wantRouter:     constants.RawDeployment,
		},
		{
			name:           "spec.deploymentMode=OMENative propagates to all three Components",
			engine:         &v1beta1.EngineSpec{},
			decoder:        &v1beta1.DecoderSpec{},
			router:         &v1beta1.RouterSpec{},
			specMode:       omeNativePtr(),
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.OMENative,
			wantRouter:     constants.OMENative,
		},
		{
			name: "per-Component annotation overrides spec.deploymentMode (Decoder pinned Raw)",
			engine: &v1beta1.EngineSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: omeAnnot},
			},
			decoder: &v1beta1.DecoderSpec{
				ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{Annotations: rawAnnot},
			},
			specMode:       omeNativePtr(),
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "spec.deploymentMode wins over Leader/Worker shape",
			engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{},
			},
			specMode:       omeNativePtr(),
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.RawDeployment,
		},
		{
			name: "spec.deploymentMode=RawDeployment overrides Leader/Worker shape (operator opt-out)",
			engine: &v1beta1.EngineSpec{
				Leader: &v1beta1.LeaderSpec{},
				Worker: &v1beta1.WorkerSpec{},
			},
			specMode:       rawDeploymentPtr(),
			wantEngineMode: constants.RawDeployment,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.RawDeployment,
		},
		{
			name:           "nil spec.deploymentMode → Leader/Worker shape still wins (OMENative)",
			engine:         &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}},
			specMode:       nil,
			wantEngineMode: constants.OMENative,
			wantDecoder:    constants.RawDeployment,
			wantRouter:     constants.RawDeployment,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			modes, err := DetermineRoleDeploymentModes(v1beta1.RoleSpecs{Engine: tt.engine, Decoder: tt.decoder, Router: tt.router}, tt.specMode)
			assert.NoError(t, err)
			engineMode, decoderMode, routerMode := modeOrRaw(modes, v1beta1.EngineComponent), modeOrRaw(modes, v1beta1.DecoderComponent), modeOrRaw(modes, v1beta1.RouterComponent)
			assert.Equal(t, tt.wantEngineMode, engineMode, "engine mode mismatch")
			assert.Equal(t, tt.wantDecoder, decoderMode, "decoder mode mismatch")
			assert.Equal(t, tt.wantRouter, routerMode, "router mode mismatch")
		})
	}
}

func TestInferenceServiceDeploymentMode(t *testing.T) {
	one := 1
	omeNative := constants.OMENative
	withAnnotation := func(mode string) metav1.ObjectMeta {
		return metav1.ObjectMeta{Annotations: map[string]string{constants.DeploymentMode: mode}}
	}
	tests := []struct {
		name string
		isvc *v1beta1.InferenceService
		def  constants.DeploymentModeType
		want constants.DeploymentModeType
	}{
		{name: "nil object resolves to the operator default", isvc: nil, def: constants.RawDeployment, want: constants.RawDeployment},
		{
			name: "top-level annotation wins over the spec field",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: withAnnotation(string(constants.VirtualDeployment)),
				Spec:       v1beta1.InferenceServiceSpec{DeploymentMode: &omeNative, Engine: &v1beta1.EngineSpec{}},
			},
			def:  constants.RawDeployment,
			want: constants.VirtualDeployment,
		},
		{
			name: "spec field wins over the declared shape",
			isvc: &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
				DeploymentMode: &omeNative,
				Engine:         &v1beta1.EngineSpec{},
				Decoder:        &v1beta1.DecoderSpec{},
			}},
			def:  constants.RawDeployment,
			want: constants.OMENative,
		},
		{
			name: "engine plus decoder is PD-disaggregated",
			isvc: &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
				Engine:  &v1beta1.EngineSpec{},
				Decoder: &v1beta1.DecoderSpec{},
			}},
			def:  constants.RawDeployment,
			want: constants.PDDisaggregated,
		},
		{
			name: "leader plus worker without a size is OMENative",
			isvc: &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{}},
			}},
			def:  constants.RawDeployment,
			want: constants.OMENative,
		},
		{
			name: "sized leader plus worker is OMENative",
			isvc: &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{Leader: &v1beta1.LeaderSpec{}, Worker: &v1beta1.WorkerSpec{Size: &one}},
			}},
			def:  constants.RawDeployment,
			want: constants.OMENative,
		},
		{
			name: "single-pod engine falls to the operator default",
			isvc: &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}}},
			def:  constants.RawDeployment,
			want: constants.RawDeployment,
		},
		{
			name: "invalid annotation is ignored",
			isvc: &v1beta1.InferenceService{
				ObjectMeta: withAnnotation("Bogus"),
				Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
			},
			def:  constants.RawDeployment,
			want: constants.RawDeployment,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, InferenceServiceDeploymentMode(tc.isvc, tc.def))
		})
	}
}
