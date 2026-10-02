package modelconfig

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNemotronSuperVLCapabilities(t *testing.T) {
	vl := []Capability{CapabilityTextToText, CapabilityImageTextToText, CapabilityVideoTextToText}
	omni := []Capability{CapabilityTextToAudio, CapabilityImageTextToAudio, CapabilityVideoTextToAudio, CapabilityAudioToText, CapabilityAudioToAudio}
	nano := []Capability{CapabilityImageTextToText, CapabilityTextToText, CapabilityAudioTextToText, CapabilityVideoTextToText}
	cases := []struct {
		name      string
		arch      string
		modelType string
		sound     json.RawMessage // nil means absent; []byte("null") means explicit null.
		vision    bool
		want      []Capability
	}{
		{name: "explicit_null", sound: json.RawMessage(`null`), vision: true, want: vl},
		{name: "null_with_whitespace", sound: json.RawMessage(" \n null \t"), vision: true, want: vl},
		{name: "missing_sound_config", vision: true, want: omni},
		{name: "empty_sound_config", sound: json.RawMessage(`{}`), vision: true, want: omni},
		{name: "audio_encoder", sound: json.RawMessage(`{"model_type":"synthetic_audio_encoder"}`), vision: true, want: omni},
		{name: "false_is_not_null", sound: json.RawMessage(`false`), vision: true, want: omni},
		{name: "string_is_not_null", sound: json.RawMessage(`"null"`), vision: true, want: omni},
		{name: "array_is_not_null", sound: json.RawMessage(`[]`), vision: true, want: omni},
		{name: "no_vision_signal", sound: json.RawMessage(`null`), want: omni},
		{name: "similar_prefix", arch: "Other_NemotronH_Omni_Reasoning_V3", sound: json.RawMessage(`null`), vision: true, want: omni},
		{name: "similar_suffix", arch: "NemotronH_Omni_Reasoning_V3Other", sound: json.RawMessage(`null`), vision: true, want: omni},
		{name: "qwen_omni", arch: "Qwen3OmniMoeForConditionalGeneration", sound: json.RawMessage(`null`), vision: true, want: omni},
		{name: "nano_omni", arch: "NemotronH_Nano_Omni_Reasoning_V3", modelType: "NemotronH_Nano_Omni_Reasoning_V3", sound: json.RawMessage(`null`), vision: true, want: nano},
		{name: "nano_rule_keeps_precedence", modelType: "NemotronH_Nano_Omni_Reasoning_V3", sound: json.RawMessage(`null`), vision: true, want: nano},
		{name: "ordinary_vision", arch: "Qwen2VLForConditionalGeneration", sound: json.RawMessage(`null`), vision: true, want: []Capability{CapabilityImageTextToText}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			arch, modelType := tc.arch, tc.modelType
			if arch == "" {
				arch = "NemotronH_Omni_Reasoning_V3"
			}
			if modelType == "" {
				modelType = "synthetic_super_vl_regression"
			}
			// This is a minimal synthetic input, not a downloaded checkpoint.
			input := map[string]interface{}{
				"model_type": modelType, "architectures": []string{arch},
				"llm_config": map[string]interface{}{"max_position_embeddings": 1048576},
			}
			if tc.sound != nil {
				input["sound_config"] = tc.sound
			}
			if tc.vision {
				input["vision_config"] = map[string]interface{}{"model_type": "synthetic_vision_encoder"}
			}
			data, err := json.Marshal(input)
			require.NoError(t, err)
			model, err := ParseModelConfig(ModelConfigInput{Path: "config.json", Data: data})
			require.NoError(t, err)
			require.Equal(t, tc.want, model.GetCapabilities())
			require.Equal(t, 1048576, model.GetContextLength())
		})
	}
}

func TestNemotronSuperVLFileLoader(t *testing.T) {
	// Synthetic fixture: real checkpoint provenance is validated separately.
	data := []byte(`{"model_type":"synthetic_super_vl_regression","architectures":["NemotronH_Omni_Reasoning_V3"],"vision_config":{},"sound_config": null}`)
	path := filepath.Join(t.TempDir(), "config.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	model, err := LoadModelConfig(path)
	require.NoError(t, err)
	require.Equal(t, []Capability{CapabilityTextToText, CapabilityImageTextToText, CapabilityVideoTextToText}, model.GetCapabilities())
}

func TestNemotronSuperVLArchitectureAloneKeepsOmni(t *testing.T) {
	model := &stubModel{modelType: "synthetic_super_vl_regression", architecture: "NemotronH_Omni_Reasoning_V3", hasVision: true}
	require.Equal(t, []Capability{CapabilityTextToAudio, CapabilityImageTextToAudio, CapabilityVideoTextToAudio, CapabilityAudioToText, CapabilityAudioToAudio}, classifyCapabilities(model))
}
