package modelconfig

import "testing"

func TestHasArchToken(t *testing.T) {
	cases := []struct {
		arch  string
		token string
		want  bool
	}{
		// CamelCase splitting
		{"Qwen3OmniMoeForConditionalGeneration", "Omni", true},
		{"Qwen3OmniMoeForConditionalGeneration", "Moe", true},
		{"Qwen3OmniMoeForConditionalGeneration", "ForCausalLM", false}, // no such token
		// Substring trap that whole-token matching avoids
		{"Demonic", "Omni", false}, // would match as substring; must NOT match as token
		{"Geometric", "metric", false},
		// Underscore-separated also splits
		{"some_omni_model", "omni", true},
		// Empty / edge cases
		{"", "Omni", false},
		{"Omni", "Omni", true},
		// Numbers stay attached to adjacent letters (Qwen3 is one token)
		{"Qwen3", "Qwen3", true},
		{"Qwen3", "Qwen", false},
		{"Qwen3", "3", false},
	}
	for _, tc := range cases {
		t.Run(tc.arch+"/"+tc.token, func(t *testing.T) {
			if got := hasArchToken(tc.arch, tc.token); got != tc.want {
				t.Errorf("hasArchToken(%q, %q) = %v, want %v", tc.arch, tc.token, got, tc.want)
			}
		})
	}
}

func TestHasAnySuffix(t *testing.T) {
	cases := []struct {
		arch     string
		suffixes []string
		want     bool
	}{
		// Single-suffix matches
		{"Wav2Vec2ForCTC", []string{"ForCTC"}, true},
		{"HubertForCTC", []string{"ForCTC"}, true},
		// Multiple-suffix list, any match wins
		{"SeamlessM4TForSpeechToText", []string{"ForCTC", "ForSpeechToText"}, true},
		// Suffix in the MIDDLE of the string must NOT match
		{"ForCTCExtra", []string{"ForCTC"}, false},
		// Empty inputs
		{"", []string{"ForCTC"}, false},
		{"WhisperForConditionalGeneration", nil, false},
		// Case-sensitive
		{"WhisperForConditionalGeneration", []string{"forconditionalgeneration"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.arch, func(t *testing.T) {
			if got := hasAnySuffix(tc.arch, tc.suffixes...); got != tc.want {
				t.Errorf("hasAnySuffix(%q, %v) = %v, want %v",
					tc.arch, tc.suffixes, got, tc.want)
			}
		})
	}
}

func TestContainsAny(t *testing.T) {
	cases := []struct {
		s    string
		subs []string
		want bool
	}{
		// Direct substring matches
		{"StableDiffusionImg2ImgPipeline", []string{"Img2Img"}, true},
		{"StableDiffusionImg2ImgPipeline", []string{"Pix2Pix", "Img2Img"}, true},
		// No match
		{"FluxPipeline", []string{"Img2Img", "Pix2Pix"}, false},
		// Empty inputs
		{"", []string{"Image"}, false},
		{"FluxPipeline", nil, false},
		// Substring in the middle counts
		{"FooImageBar", []string{"Image"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.s, func(t *testing.T) {
			if got := containsAny(tc.s, tc.subs...); got != tc.want {
				t.Errorf("containsAny(%q, %v) = %v, want %v",
					tc.s, tc.subs, got, tc.want)
			}
		})
	}
}

// stubModel is a minimal HuggingFaceModel for unit-testing rules.
type stubModel struct {
	modelType    string
	architecture string
	hasVision    bool
	isEmbedding  bool
}

func (m *stubModel) GetParameterCount() int64         { return 0 }
func (m *stubModel) GetTransformerVersion() string    { return "" }
func (m *stubModel) GetQuantizationType() string      { return "" }
func (m *stubModel) GetArchitecture() string          { return m.architecture }
func (m *stubModel) GetModelType() string             { return m.modelType }
func (m *stubModel) GetContextLength() int            { return 0 }
func (m *stubModel) GetModelSizeBytes() int64         { return 0 }
func (m *stubModel) GetTorchDtype() string            { return "" }
func (m *stubModel) HasVision() bool                  { return m.hasVision }
func (m *stubModel) IsEmbedding() bool                { return m.isEmbedding }
func (m *stubModel) GetCapabilities() []Capability    { return classifyCapabilities(m) }
func (m *stubModel) GetHFQuantConfig() *HFQuantConfig { return nil }

// stubDiffusionModel adds the diffusion sub-interface.
type stubDiffusionModel struct {
	stubModel
	pipeline *DiffusionPipelineSpec
}

func (m *stubDiffusionModel) GetDiffusionModel() *DiffusionPipelineSpec {
	return m.pipeline
}

func TestClassifyCapabilities_Diffusion(t *testing.T) {
	cases := []struct {
		name     string
		pipeline *DiffusionPipelineSpec
		want     []Capability
	}{
		{
			name:     "image-edit",
			pipeline: &DiffusionPipelineSpec{ClassName: "StableDiffusionImg2ImgPipeline"},
			want:     []Capability{CapabilityImageTextToImage},
		},
		{
			name:     "pix2pix",
			pipeline: &DiffusionPipelineSpec{ClassName: "InstructPix2PixPipeline"},
			want:     []Capability{CapabilityImageTextToImage},
		},
		{
			name:     "qwen image edit",
			pipeline: &DiffusionPipelineSpec{ClassName: "QwenImageEditPipeline"},
			want:     []Capability{CapabilityImageTextToImage},
		},
		{
			name:     "text-to-video",
			pipeline: &DiffusionPipelineSpec{ClassName: "TextToVideoSDPipeline"},
			want:     []Capability{CapabilityTextToVideo},
		},
		{
			name:     "t2v abbreviation",
			pipeline: &DiffusionPipelineSpec{ClassName: "T2VPipeline"},
			want:     []Capability{CapabilityTextToVideo},
		},
		{
			name:     "image-to-video catch-all",
			pipeline: &DiffusionPipelineSpec{ClassName: "ImageToVideoPipeline"},
			want:     []Capability{CapabilityImageTextToVideo},
		},
		{
			name:     "stable diffusion text-to-image",
			pipeline: &DiffusionPipelineSpec{ClassName: "StableDiffusionPipeline"},
			want:     []Capability{CapabilityTextToImage},
		},
		{
			name:     "flux pipeline",
			pipeline: &DiffusionPipelineSpec{ClassName: "FluxPipeline"},
			want:     []Capability{CapabilityTextToImage},
		},
		{
			name:     "qwen image",
			pipeline: &DiffusionPipelineSpec{ClassName: "QwenImagePipeline"},
			want:     []Capability{CapabilityTextToImage},
		},
		{
			name:     "unknown diffusion class returns Unknown, not silent fallthrough",
			pipeline: &DiffusionPipelineSpec{ClassName: "FooBarPipeline"},
			want:     []Capability{CapabilityUnknown},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &stubDiffusionModel{pipeline: tc.pipeline}
			got := classifyCapabilities(m)
			if !equalCaps(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClassifyCapabilities_NemotronH_Nano(t *testing.T) {
	m := &stubModel{
		modelType:    "NemotronH_Nano_Omni_Reasoning_V3",
		architecture: "NemotronH_Nano_Omni_Reasoning_V3",
		hasVision:    true,
	}
	want := []Capability{
		CapabilityImageTextToText,
		CapabilityTextToText,
		CapabilityAudioTextToText,
		CapabilityVideoTextToText,
	}
	if got := classifyCapabilities(m); !equalCaps(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestClassifyCapabilities_NemotronH_NanoNonOmni(t *testing.T) {
	m := &stubModel{
		modelType:    "NemotronH_Nano_VL_V2",
		architecture: "NemotronH_Nano_VL_V2",
		hasVision:    true,
	}
	want := []Capability{
		CapabilityImageTextToText,
		CapabilityTextToText,
	}
	if got := classifyCapabilities(m); !equalCaps(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestClassifyCapabilities_Omni(t *testing.T) {
	m := &stubModel{
		modelType:    "qwen",
		architecture: "Qwen3OmniMoeForConditionalGeneration",
	}
	want := []Capability{
		CapabilityTextToAudio,
		CapabilityImageTextToAudio,
		CapabilityVideoTextToAudio,
		CapabilityAudioToText,
		CapabilityAudioToAudio,
	}
	if got := classifyCapabilities(m); !equalCaps(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestClassifyCapabilities_OmniBeatsVision(t *testing.T) {
	// Deliberate behavior change vs today's modelparser: a model
	// with both an Omni token AND HasVision=true classifies as the
	// 5-cap omni list, NOT [ImageTextToText]. Spec calls this out.
	m := &stubModel{
		modelType:    "qwen",
		architecture: "Qwen3OmniVisionForConditionalGeneration",
		hasVision:    true,
	}
	got := classifyCapabilities(m)
	if len(got) != 5 || got[0] != CapabilityTextToAudio {
		t.Errorf("expected omni 5-cap list, got %v", got)
	}
}

func TestClassifyCapabilities_Vision(t *testing.T) {
	m := &stubModel{
		modelType:    "llava",
		architecture: "LlavaForConditionalGeneration",
		hasVision:    true,
	}
	want := []Capability{CapabilityImageTextToText}
	if got := classifyCapabilities(m); !equalCaps(got, want) {
		t.Errorf("got %v, want %v", got, want)
	}
}

func TestClassifyCapabilities_ASR(t *testing.T) {
	cases := []struct {
		name      string
		modelType string
		arch      string
		want      []Capability
	}{
		{
			name: "Cohere ASR architecture",
			arch: "CohereAsrForConditionalGeneration",
			want: []Capability{CapabilityAudioToText},
		},
		{
			name:      "Cohere ASR model type fallback",
			modelType: "cohere_asr",
			arch:      "CustomConditionalGeneration",
			want:      []Capability{CapabilityAudioToText},
		},
		{
			name: "Whisper architecture",
			arch: "WhisperForConditionalGeneration",
			want: []Capability{CapabilityAudioToText},
		},
		{
			name: "Wav2Vec2 architecture",
			arch: "Wav2Vec2ForCTC",
			want: []Capability{CapabilityAudioToText},
		},
		{
			name: "HuBERT architecture",
			arch: "HubertForCTC",
			want: []Capability{CapabilityAudioToText},
		},
		{
			name: "speech-to-text suffix",
			arch: "SeamlessM4TForSpeechToText",
			want: []Capability{CapabilityAudioToText},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := &stubModel{modelType: tc.modelType, architecture: tc.arch}
			if got := classifyCapabilities(m); !equalCaps(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClassifyCapabilities_Embedding(t *testing.T) {
	cases := []struct {
		name string
		m    *stubModel
		want []Capability
	}{
		{
			name: "explicit IsEmbedding flag",
			m:    &stubModel{modelType: "bert", architecture: "BertModel", isEmbedding: true},
			want: []Capability{CapabilityEmbedding},
		},
		{
			name: "Sentence token in architecture",
			m:    &stubModel{modelType: "sentence-transformer", architecture: "SentenceTransformerModel"},
			want: []Capability{CapabilityEmbedding},
		},
		{
			name: "e5-mistral special case (MistralModel without CausalLM head)",
			m:    &stubModel{modelType: "mistral", architecture: "MistralModel"},
			want: []Capability{CapabilityEmbedding},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyCapabilities(tc.m); !equalCaps(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

func TestClassifyCapabilities_Default(t *testing.T) {
	cases := []struct {
		name string
		m    *stubModel
		want []Capability
	}{
		{
			name: "unknown model with generative-text suffix => TextToText",
			m:    &stubModel{modelType: "falcon", architecture: "FalconForCausalLM"},
			want: []Capability{CapabilityTextToText},
		},
		{
			name: "unknown model with no recognized signal => Unknown",
			m:    &stubModel{modelType: "foo", architecture: "Foo"},
			want: []Capability{CapabilityUnknown},
		},
		{
			name: "Cohere text generation remains TextToText",
			m:    &stubModel{modelType: "cohere", architecture: "CohereForCausalLM"},
			want: []Capability{CapabilityTextToText},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := classifyCapabilities(tc.m); !equalCaps(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestClassifyCapabilities_RegressionSet carries forward every case
// from the previous pkg/modelparser/capability_test.go's
// TestDetermineCapabilities, retargeted at modelconfig.Capability.
// Each case must produce the same classification outcome as the
// pre-migration modelparser code (with two intentional changes
// flagged in subtest comments).
func TestClassifyCapabilities_RegressionSet(t *testing.T) {
	cases := []struct {
		name string
		m    HuggingFaceModel
		want []Capability
	}{
		{
			name: "Text Generation Model",
			m:    &stubModel{modelType: "llama", architecture: "LlamaForCausalLM"},
			want: []Capability{CapabilityTextToText},
		},
		{
			name: "Vision Model",
			m:    &stubModel{modelType: "clip", architecture: "CLIPModel", hasVision: true},
			want: []Capability{CapabilityImageTextToText},
		},
		{
			name: "Text Embedding Model",
			m:    &stubModel{modelType: "bert", architecture: "BertModel", isEmbedding: true},
			want: []Capability{CapabilityEmbedding},
		},
		{
			name: "Sentence Transformer Model",
			m:    &stubModel{modelType: "sentence-transformer", architecture: "SentenceTransformerModel"},
			want: []Capability{CapabilityEmbedding},
		},
		{
			name: "Special Case Mistral Embedding Model",
			m:    &stubModel{modelType: "mistral", architecture: "MistralModel"},
			want: []Capability{CapabilityEmbedding},
		},
		{
			name: "Vision-capable LLM",
			m:    &stubModel{modelType: "gemma", architecture: "GemmaForCausalLM", hasVision: true},
			want: []Capability{CapabilityImageTextToText},
		},
		{
			name: "Diffusion Model",
			m: &stubDiffusionModel{
				stubModel: stubModel{modelType: "diffusers", architecture: "StableDiffusionPipeline", hasVision: true},
				pipeline:  &DiffusionPipelineSpec{ClassName: "StableDiffusionPipeline"},
			},
			want: []Capability{CapabilityTextToImage},
		},
		{
			name: "Diffusion QwenImagePipeline",
			m: &stubDiffusionModel{
				stubModel: stubModel{modelType: "diffusers", architecture: "QwenImagePipeline", hasVision: true},
				pipeline:  &DiffusionPipelineSpec{ClassName: "QwenImagePipeline"},
			},
			want: []Capability{CapabilityTextToImage},
		},
		{
			name: "Diffusion QwenImageEditPlus",
			m: &stubDiffusionModel{
				stubModel: stubModel{modelType: "diffusers", architecture: "QwenImageEditPlus", hasVision: true},
				pipeline:  &DiffusionPipelineSpec{ClassName: "QwenImageEditPlus"},
			},
			want: []Capability{CapabilityImageTextToImage},
		},
		{
			name: "Transformer Qwen3 Omni",
			m:    &stubModel{modelType: "qwen", architecture: "Qwen3OmniMoeForConditionalGeneration"},
			want: []Capability{
				CapabilityTextToAudio,
				CapabilityImageTextToAudio,
				CapabilityVideoTextToAudio,
				CapabilityAudioToText,
				CapabilityAudioToAudio,
			},
		},
		{
			name: "NemotronH_Nano Omni Model - falls through to vision due to case mismatch",
			m: &stubModel{
				modelType:    "NemotronH_Nano_Omni_Reasoning_V3",
				architecture: "NemotronH_Nano_Omni_Reasoning_V3",
				hasVision:    true,
			},
			want: []Capability{
				CapabilityImageTextToText,
				CapabilityTextToText,
				CapabilityAudioTextToText,
				CapabilityVideoTextToText,
			},
		},
		{
			name: "Whisper ASR Model",
			m:    &stubModel{modelType: "whisper", architecture: "WhisperForConditionalGeneration"},
			want: []Capability{CapabilityAudioToText},
		},
		{
			name: "Wav2Vec2 ASR Model",
			m:    &stubModel{modelType: "wav2vec2", architecture: "Wav2Vec2ForCTC"},
			want: []Capability{CapabilityAudioToText},
		},
		{
			name: "HuBERT ASR Model",
			m:    &stubModel{modelType: "hubert", architecture: "HubertForCTC"},
			want: []Capability{CapabilityAudioToText},
		},
		{
			name: "Generic ForSpeechToText Model",
			m:    &stubModel{modelType: "seamless_m4t", architecture: "SeamlessM4TForSpeechToText"},
			want: []Capability{CapabilityAudioToText},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyCapabilities(tc.m)
			if !equalCaps(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestClassifyCapabilities_MistralRegression covers the bug fixed
// upstream in PR ome-projects/ome#602 (Mistral models misclassified
// as EMBEDDING). Our refactor naturally avoids the bug because
// (a) we deleted the per-model MistralConfig that had a dangerous
// "MistralModel" GetArchitecture fallback, and (b) GenericModelConfig
// returns "" when Architectures is empty. These tests pin the
// correct behavior across the three real-world Mistral shapes.
func TestClassifyCapabilities_MistralRegression(t *testing.T) {
	cases := []struct {
		name string
		m    *stubModel
		want []Capability
	}{
		{
			// Regular Mistral causal LM (e.g. mistralai/Mistral-7B-Instruct-v0.3).
			// Must NOT classify as embedding.
			name: "Mistral-7B-Instruct (architectures=MistralForCausalLM)",
			m:    &stubModel{modelType: "mistral", architecture: "MistralForCausalLM"},
			want: []Capability{CapabilityTextToText},
		},
		{
			// The original bug case: a Mistral model whose config.json
			// lacks the architectures field. Our old per-model parser
			// would have returned "MistralModel" via fallback and matched
			// the embedding special case. Our new code returns Unknown
			// (no architecture info to classify) — the gap surfaces as
			// a logged warning instead of a silent misclassification.
			name: "Mistral with no architectures field => Unknown (not Embedding)",
			m:    &stubModel{modelType: "mistral", architecture: ""},
			want: []Capability{CapabilityUnknown},
		},
		{
			// Genuine e5-mistral embedding checkpoint: explicitly sets
			// architectures=["MistralModel"] in config.json. The
			// (model_type=mistral && arch=="MistralModel") special case
			// in embeddingRule still fires correctly.
			name: "e5-mistral (architectures=MistralModel) => Embedding",
			m:    &stubModel{modelType: "mistral", architecture: "MistralModel"},
			want: []Capability{CapabilityEmbedding},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := classifyCapabilities(tc.m)
			if !equalCaps(got, tc.want) {
				t.Errorf("got %v, want %v", got, tc.want)
			}
		})
	}
}

// TestGetCapabilitiesFromFixture is a smoke test that confirms
// the GetCapabilities methods on the real impls dispatch through
// classifyCapabilities (and don't accidentally use the
// BaseModelConfig default).
func TestGetCapabilitiesFromFixture(t *testing.T) {
	// llama3.json is a plain text LLM → falls through rules to
	// the generative-text-suffix default → [TextToText].
	model, err := LoadModelConfig("testdata/llama3.json")
	if err != nil {
		t.Fatalf("LoadModelConfig: %v", err)
	}
	got := model.GetCapabilities()
	want := []Capability{CapabilityTextToText}
	if !equalCaps(got, want) {
		t.Errorf("llama3 GetCapabilities = %v, want %v", got, want)
	}
}

// equalCaps compares two capability slices order-sensitively.
func equalCaps(a, b []Capability) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
