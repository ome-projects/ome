package modelconfig

import (
	"strings"
	"unicode"
)

// Capability identifies the kind of inference task a model serves.
// Values mirror v1beta1.ModelCapability so consumers can map 1:1
// without modelconfig taking a dependency on the OME API.
type Capability string

const (
	CapabilityUnknown          Capability = ""
	CapabilityTextToText       Capability = "TEXT_TO_TEXT"
	CapabilityImageTextToText  Capability = "IMAGE_TEXT_TO_TEXT"
	CapabilityTextToImage      Capability = "TEXT_TO_IMAGE"
	CapabilityImageTextToImage Capability = "IMAGE_TEXT_TO_IMAGE"
	CapabilityTextToVideo      Capability = "TEXT_TO_VIDEO"
	CapabilityImageTextToVideo Capability = "IMAGE_TEXT_TO_VIDEO"
	CapabilityTextToAudio      Capability = "TEXT_TO_AUDIO"
	CapabilityImageTextToAudio Capability = "IMAGE_TEXT_TO_AUDIO"
	CapabilityVideoTextToAudio Capability = "VIDEO_TEXT_TO_AUDIO"
	CapabilityVideoTextToText  Capability = "VIDEO_TEXT_TO_TEXT"
	CapabilityAudioToText      Capability = "AUDIO_TO_TEXT"
	CapabilityAudioToAudio     Capability = "AUDIO_TO_AUDIO"
	CapabilityAudioTextToText  Capability = "AUDIO_TEXT_TO_TEXT"
	CapabilityEmbedding        Capability = "EMBEDDING"
)

// hasArchToken reports whether arch contains token as a distinct
// CamelCase or underscore-separated segment. Unlike strings.Contains,
// "Demonic" does not match "Omni". Case-sensitive.
func hasArchToken(arch, token string) bool {
	if arch == "" || token == "" {
		return false
	}
	for _, segment := range splitArchTokens(arch) {
		if segment == token {
			return true
		}
	}
	return false
}

// splitArchTokens splits arch on underscores and before each
// uppercase letter — except when the previous rune is also uppercase,
// so consecutive caps like "BERT" stay one token. "Qwen3Omni" yields
// ["Qwen3", "Omni"].
func splitArchTokens(arch string) []string {
	var out []string
	var current []rune
	flush := func() {
		if len(current) > 0 {
			out = append(out, string(current))
			current = current[:0]
		}
	}
	runes := []rune(arch)
	for i, r := range runes {
		if r == '_' {
			flush()
			continue
		}
		if i > 0 && unicode.IsUpper(r) && !unicode.IsUpper(runes[i-1]) {
			flush()
		}
		current = append(current, r)
	}
	flush()
	return out
}

// hasAnySuffix reports whether arch ends with any suffix.
// Case-sensitive; HF architecture names are stable in their casing.
func hasAnySuffix(arch string, suffixes ...string) bool {
	for _, suffix := range suffixes {
		if strings.HasSuffix(arch, suffix) {
			return true
		}
	}
	return false
}

// containsAny reports whether s contains any of the substrings.
// Reserved for diffusion pipeline class names, which are composite
// ("StableDiffusionImg2ImgPipeline") and need substring matching.
func containsAny(s string, substrings ...string) bool {
	for _, sub := range substrings {
		if strings.Contains(s, sub) {
			return true
		}
	}
	return false
}

// capabilityRule classifies a model. Returning nil means "skip me";
// the dispatcher moves on to the next rule.
type capabilityRule func(hf HuggingFaceModel) []Capability

// capabilityRules. First non-nil result wins. Order is load-bearing.
var capabilityRules = []capabilityRule{
	// Diffusion first: the pipeline-class signal beats any
	// architecture-suffix coincidence.
	diffusionRule,
	// NemotronH-Nano before vision: it is genuinely multi-cap, and
	// the vision short-circuit would otherwise drop the text and
	// audio outputs.
	nemotronHNanoRule,
	// The VL variant explicitly disables sound. Check its config before
	// inferring audio capabilities from the Omni architecture token.
	nemotronSuperVLRule,
	// Omni before vision: omni implies audio-output capabilities
	// that the vision short-circuit alone would miss.
	omniRule,
	// Inkling before vision: it is genuinely multi-cap (text and
	// audio-text), and the vision short-circuit would otherwise drop
	// the audio-text output.
	inklingRule,
	// Vision before ASR: a vision LLM with "ForConditionalGeneration"
	// shouldn't classify as AudioToText via suffix coincidence.
	visionRule,
	asrRule,
	embeddingRule,
}

// generativeTextSuffixes is the catch-all for unrecognized models
// whose architecture name signals a generative-text head.
var generativeTextSuffixes = []string{
	"ForCausalLM",
	"ForConditionalGeneration",
	"LMHeadModel",
}

// classifyCapabilities runs the rule dispatcher. First match wins.
// No match + recognized generative-text suffix => TextToText.
// Otherwise => Unknown (callers should log).
func classifyCapabilities(hf HuggingFaceModel) []Capability {
	for _, rule := range capabilityRules {
		if caps := rule(hf); caps != nil {
			return caps
		}
	}
	if hasAnySuffix(hf.GetArchitecture(), generativeTextSuffixes...) {
		return []Capability{CapabilityTextToText}
	}
	return []Capability{CapabilityUnknown}
}

func diffusionRule(hf HuggingFaceModel) []Capability {
	dm, ok := hf.(HuggingFaceDiffusionModel)
	if !ok || dm.GetDiffusionModel() == nil {
		return nil
	}
	cls := dm.GetDiffusionModel().ClassName
	switch {
	case containsAny(cls, "ImageEdit", "Pix2Pix", "Img2Img", "Inpaint"):
		return []Capability{CapabilityImageTextToImage}
	case containsAny(cls, "TextToVideo", "T2V"):
		return []Capability{CapabilityTextToVideo}
	case strings.Contains(cls, "Video"):
		// Image-to-video catch-all after the T2V check above.
		return []Capability{CapabilityImageTextToVideo}
	case containsAny(cls, "Image", "Pix", "StableDiffusion", "Flux"):
		return []Capability{CapabilityTextToImage}
	}
	// Diffusion sub-interface but no recognized pipeline class —
	// return Unknown so the warning fires and we can add a rule.
	return []Capability{CapabilityUnknown}
}

func nemotronHNanoRule(hf HuggingFaceModel) []Capability {
	modelType := strings.ToLower(hf.GetModelType())
	if !strings.Contains(modelType, "nemotronh_nano") {
		return nil
	}
	capabilities := []Capability{
		CapabilityImageTextToText,
		CapabilityTextToText,
	}
	if strings.Contains(modelType, "nemotronh_nano_omni") {
		capabilities = append(capabilities,
			CapabilityAudioTextToText,
			CapabilityVideoTextToText,
		)
	}
	return capabilities
}

// nemotronSuperVLRule recognizes the VL-only configuration of this
// architecture. Missing sound metadata is not evidence of disabled audio;
// object-valued sound configs and other architectures retain existing rules.
func nemotronSuperVLRule(hf HuggingFaceModel) []Capability {
	if hf.GetArchitecture() != "NemotronH_Omni_Reasoning_V3" || !hf.HasVision() {
		return nil
	}
	config, ok := hf.(*GenericModelConfig)
	if !ok || strings.TrimSpace(string(config.soundConfig)) != "null" {
		return nil
	}
	return []Capability{
		CapabilityTextToText,
		CapabilityImageTextToText,
		CapabilityVideoTextToText,
	}
}

func omniRule(hf HuggingFaceModel) []Capability {
	if !hasArchToken(hf.GetArchitecture(), "Omni") {
		return nil
	}
	return []Capability{
		CapabilityTextToAudio,
		CapabilityImageTextToAudio,
		CapabilityVideoTextToAudio,
		CapabilityAudioToText,
		CapabilityAudioToAudio,
	}
}

func visionRule(hf HuggingFaceModel) []Capability {
	if !hf.HasVision() {
		return nil
	}
	return []Capability{CapabilityImageTextToText}
}

func asrRule(hf HuggingFaceModel) []Capability {
	arch := hf.GetArchitecture()
	if hasAnySuffix(arch, "ForCTC", "ForSpeechToText") ||
		arch == "WhisperForConditionalGeneration" {
		return []Capability{CapabilityAudioToText}
	}
	return nil
}

func embeddingRule(hf HuggingFaceModel) []Capability {
	if hf.IsEmbedding() {
		return []Capability{CapabilityEmbedding}
	}
	arch := hf.GetArchitecture()
	// BertModel, BertForMaskedLM, BertForSequenceClassification, ...
	// IsEmbedding is rarely set on real configs, so the architecture
	// token is the practical detector for BERT-family encoders.
	if hasArchToken(arch, "Bert") {
		return []Capability{CapabilityEmbedding}
	}
	if hasArchToken(arch, "Embedding") || hasArchToken(arch, "Sentence") {
		return []Capability{CapabilityEmbedding}
	}
	// e5-mistral and similar use MistralModel (no CausalLM head)
	// for embedding output.
	if strings.ToLower(hf.GetModelType()) == "mistral" && arch == "MistralModel" {
		return []Capability{CapabilityEmbedding}
	}
	return nil
}

func inklingRule(hf HuggingFaceModel) []Capability {
	if !strings.Contains(strings.ToLower(hf.GetModelType()), "inkling_mm_model") {
		return nil
	}
	return []Capability{
		CapabilityTextToText,
		CapabilityAudioTextToText,
	}
}
