package modelconfig

import (
	"fmt"
	"regexp"
	"strings"
)

// AutoMap defines the mapping of model classes for custom Hugging Face
// models. Used when models require custom code (e.g., trust_remote_code=True).
type AutoMap struct {
	AutoConfig           string `json:"AutoConfig,omitempty"`
	AutoModel            string `json:"AutoModel,omitempty"`
	AutoModelForCausalLM string `json:"AutoModelForCausalLM,omitempty"`
}

// BaseModelConfig defines fields shared across all Hugging Face model
// configurations and provides default HuggingFaceModel methods that
// concrete config types can rely on by embedding.
type BaseModelConfig struct {
	ModelType          string   `json:"model_type"`
	Architectures      []string `json:"architectures"`
	TorchDtype         string   `json:"torch_dtype"`
	TransformerVersion string   `json:"transformers_version"`

	// Internal field; not in JSON.
	ConfigPath string `json:"-"`
}

func (c *BaseModelConfig) GetModelType() string {
	return c.ModelType
}

func (c *BaseModelConfig) GetTransformerVersion() string {
	return c.TransformerVersion
}

func (c *BaseModelConfig) GetArchitecture() string {
	if len(c.Architectures) > 0 {
		return c.Architectures[0]
	}
	return ""
}

func (c *BaseModelConfig) GetTorchDtype() string {
	return c.TorchDtype
}

// HasVision defaults to false. GenericModelConfig overrides this with
// JSON-shape detection (vision_config / mm_vision_tower / image_token_*
// / img_processor); GenericDiffusionModelConfig overrides to true.
func (c *BaseModelConfig) HasVision() bool {
	return false
}

// IsEmbedding defaults to false. GenericModelConfig overrides this from
// config_sentence_transformers.json; the capability classifier in this
// package also detects embedding models from architecture name
// (BertModel, SentenceTransformerModel, etc.).
func (c *BaseModelConfig) IsEmbedding() bool {
	return false
}

// GetCapabilities defaults to CapabilityUnknown. GenericModelConfig
// and GenericDiffusionModelConfig override this with the real
// classifier; types that embed BaseModelConfig without overriding
// will surface as Unknown so the gap is visible in logs.
func (c *BaseModelConfig) GetCapabilities() []Capability {
	return []Capability{CapabilityUnknown}
}

// QuantizationConfig is the embedded "quantization_config" block from
// config.json — the HF-native convention used by mxfp4, fbgemm_fp8,
// gptq, awq, fp8, compressed-tensors. Distinct from HFQuantConfig
// (ModelOpt's separate hf_quant_config.json in quant_config.go).
//
// The excluded-modules list has three field names in the wild —
// modules_to_not_convert (HF-native), ignore (compressed-tensors),
// ignored_layers (neuralmagic) — surfaced uniformly via ExcludePatterns.
type QuantizationConfig struct {
	// QuantMethod is the algorithm OR producer name. For most
	// producers it IS the algorithm; ModelOpt distinguishes producer
	// ("modelopt") from algorithm (under QuantAlgo).
	QuantMethod string `json:"quant_method"`
	// QuantAlgo is the algorithm name when QuantMethod is just the
	// producer. ModelOpt: quant_method="modelopt" + quant_algo="NVFP4".
	// Wins over QuantMethod for downstream mapping when set.
	QuantAlgo string `json:"quant_algo,omitempty"`
	// Bits is the weight bit width on GPTQ/AWQ ({"quant_method":"gptq",
	// "bits":4}). Without folding this in, "gptq" alone misses every
	// "int4"/"w4a" matcher and packed INT4 GPTQ undercounts 8x.
	Bits                int      `json:"bits,omitempty"`
	ModulesToNotConvert []string `json:"modules_to_not_convert,omitempty"`
	Ignore              []string `json:"ignore,omitempty"`
	IgnoredLayers       []string `json:"ignored_layers,omitempty"`
	ActivationScheme    string   `json:"activation_scheme,omitempty"`
	Format              string   `json:"fmt,omitempty"`
	WeightBlockSize     []int    `json:"weight_block_size,omitempty"`
}

// ExcludePatterns returns the first non-empty excluded-modules list
// across the three known field-name conventions, in order of
// production frequency: HF-native → compressed-tensors → neuralmagic.
func (c *QuantizationConfig) ExcludePatterns() []string {
	if c == nil {
		return nil
	}
	if len(c.ModulesToNotConvert) > 0 {
		return c.ModulesToNotConvert
	}
	if len(c.Ignore) > 0 {
		return c.Ignore
	}
	if len(c.IgnoredLayers) > 0 {
		return c.IgnoredLayers
	}
	return nil
}

// ToHFQuantConfig synthesizes a standalone HFQuantConfig from the
// embedded block so the safetensors counting pipeline sees one type
// regardless of producer convention. Returns nil for unquantized
// models (no quant_method AND no quant_algo).
func (c *QuantizationConfig) ToHFQuantConfig() *HFQuantConfig {
	if c == nil {
		return nil
	}
	algo := c.resolveAlgo()
	if algo == "" {
		return nil
	}
	return &HFQuantConfig{
		QuantMethod: c.QuantMethod,
		Quantization: HFQuantConfigDetails{
			QuantAlgo:      algo,
			ExcludeModules: c.ExcludePatterns(),
		},
	}
}

// resolveAlgo returns the algorithm name to surface: QuantAlgo if
// set (ModelOpt), else QuantMethod normalized to "INT<N>_<METHOD>"
// when GPTQ/AWQ + Bits is set (so "gptq"+bits:4 doesn't slip past
// the "int4"/"w4a" matchers and undercount 8x).
func (c *QuantizationConfig) resolveAlgo() string {
	if c == nil {
		return ""
	}
	if v := strings.TrimSpace(c.QuantAlgo); v != "" {
		return v
	}
	method := strings.TrimSpace(c.QuantMethod)
	if c.Bits > 0 && (method == "gptq" || method == "awq") {
		return fmt.Sprintf("INT%d_%s", c.Bits, strings.ToUpper(method))
	}
	return method
}

// RopeScalingConfig captures RoPE (Rotary Position Embedding) scaling
// configuration. Several variants of fields exist across model
// families; the union here covers what's been observed in fixtures.
type RopeScalingConfig struct {
	Type                          string  `json:"type,omitempty"`
	RopeType                      string  `json:"rope_type,omitempty"`
	Factor                        float64 `json:"factor"`
	LowFreqFactor                 float64 `json:"low_freq_factor,omitempty"`
	HighFreqFactor                float64 `json:"high_freq_factor,omitempty"`
	BetaFast                      float64 `json:"beta_fast,omitempty"`
	BetaSlow                      float64 `json:"beta_slow,omitempty"`
	MScale                        float64 `json:"mscale,omitempty"`
	MScaleAllDim                  float64 `json:"mscale_all_dim,omitempty"`
	OriginalMaxPositionEmbeddings int     `json:"original_max_position_embeddings"`
}

// DtypeSizeBytes maps torch data types to their size in bytes per parameter.
var DtypeSizeBytes = map[string]float64{
	"float32":  4.0,
	"float":    4.0,
	"bfloat16": 2.0,
	"bf16":     2.0,
	"float16":  2.0,
	"fp16":     2.0,
	"half":     2.0,
	"int8":     1.0,
	"fp8":      1.0,
	"float8":   1.0,
	"e4m3":     1.0,
	"int4":     0.5,
	"4bit":     0.5,
}

// FormatSize formats a byte size in a human-readable form (B, KB, MB, GB, TB).
func FormatSize(size int64) string {
	const (
		kb = 1024
		mb = 1024 * kb
		gb = 1024 * mb
		tb = 1024 * gb
	)

	switch {
	case size < kb:
		return fmt.Sprintf("%d B", size)
	case size < mb:
		return fmt.Sprintf("%.2f KB", float64(size)/float64(kb))
	case size < gb:
		return fmt.Sprintf("%.2f MB", float64(size)/float64(mb))
	case size < tb:
		return fmt.Sprintf("%.2f GB", float64(size)/float64(gb))
	default:
		return fmt.Sprintf("%.2f TB", float64(size)/float64(tb))
	}
}

// FormatParamCount converts a parameter count to a human-readable string.
// Examples: 1000 -> "1K", 1500000 -> "1.5M", 685000000000 -> "685B".
func FormatParamCount(count int64) string {
	const (
		thousand = 1_000
		million  = 1_000_000
		billion  = 1_000_000_000
		trillion = 1_000_000_000_000
	)

	var value float64
	var suffix string

	switch {
	case count >= trillion:
		value = float64(count) / float64(trillion)
		suffix = "T"
	case count >= billion:
		value = float64(count) / float64(billion)
		suffix = "B"
	case count >= million:
		value = float64(count) / float64(million)
		suffix = "M"
	case count >= thousand:
		value = float64(count) / float64(thousand)
		suffix = "K"
	default:
		return fmt.Sprintf("%d", count)
	}

	if value == float64(int64(value)) {
		return fmt.Sprintf("%d%s", int64(value), suffix)
	}
	if value*100 == float64(int64(value*100)) {
		return fmt.Sprintf("%.1f%s", value, suffix)
	}
	return fmt.Sprintf("%.2f%s", value, suffix)
}

// EstimateModelSizeBytes estimates model size in bytes from parameter
// count and torch dtype. Falls back to float32 (4 bytes) for unknown
// dtypes.
func EstimateModelSizeBytes(paramCount int64, dtype string) int64 {
	sizePerParam, ok := DtypeSizeBytes[strings.ToLower(dtype)]
	if !ok {
		sizePerParam = 4.0
	}
	return int64(float64(paramCount) * sizePerParam)
}

// Regex patterns for sanitizing non-standard JSON values.
var (
	infinityRegex    = regexp.MustCompile(`([:,\[]\s*)Infinity(\s*[,\]\}])`)
	negInfinityRegex = regexp.MustCompile(`([:,\[]\s*)-Infinity(\s*[,\]\}])`)
	nanRegex         = regexp.MustCompile(`([:,\[]\s*)NaN(\s*[,\]\}])`)
)

// SanitizeJSONBytes replaces JavaScript/Python-style special float
// values (Infinity, -Infinity, NaN) with JSON-compatible substitutes.
// Some configs (e.g., NVIDIA Nemotron) emit these; Python's json
// module accepts them but Go's does not.
func SanitizeJSONBytes(data []byte) []byte {
	s := string(data)
	s = infinityRegex.ReplaceAllString(s, "${1}1e308${2}")
	s = negInfinityRegex.ReplaceAllString(s, "${1}-1e308${2}")
	s = nanRegex.ReplaceAllString(s, "${1}null${2}")
	return []byte(s)
}
