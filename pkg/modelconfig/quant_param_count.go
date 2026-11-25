package modelconfig

import (
	"math"
	"regexp"
	"strings"
	"sync"
)

// IsScaleTensor reports whether the tensor is a quantization scale,
// zero-point, or other calibration tensor — NOT a real model parameter
// and should be excluded from parameter-count totals.
//
// Two naming conventions coexist on production models, both matched
// on the basename (last dot-segment) so weights with "scale" elsewhere
// in their dotted path don't false-positive:
//
//   - dot-convention (ModelOpt / AWQ / GPTQ / FP8): scale is its own
//     dot-segment, e.g. "...gate_proj.weight_scale" → exact match.
//   - underscore-convention (mxfp4): scale name is concatenated with
//     "_", e.g. "...experts.gate_up_proj_scales" → suffix match. Each
//     scaleSuffixes entry starts with "_" so "model.scaled_weight"
//     does NOT match "_scale".
func IsScaleTensor(tensorName string) bool {
	basename := tensorName
	if idx := strings.LastIndex(tensorName, "."); idx >= 0 {
		basename = tensorName[idx+1:]
	}
	if _, ok := scaleExactBasenames[basename]; ok {
		return true
	}
	for _, suffix := range scaleSuffixes {
		if strings.HasSuffix(basename, suffix) {
			return true
		}
	}
	return false
}

// Notably NOT in this set: "qweight" (AWQ/GPTQ packed weight — IS a
// real parameter, just stored in I32-packed form; counted via
// QuantPackingFactor). New entries should be
// driven by real fixtures in testdata/quant/.
var scaleExactBasenames = map[string]struct{}{
	"weight_scale":            {},
	"weight_scale_2":          {},
	"weight_scale_inv":        {},
	"input_scale":             {},
	"k_scale":                 {},
	"v_scale":                 {},
	"kv_scale":                {},
	"kv_cache_scaling_factor": {},
	"weight_zero_point":       {},
	"scales":                  {},
	"zeros":                   {},
	"qzeros":                  {},
	"g_idx":                   {},
}

// "_blocks" is intentionally NOT here — mxfp4 uses "*_blocks" for the
// actual packed weights (paired with "*_scales" for scales).
var scaleSuffixes = []string{
	"_scale",
	"_scales",
	"_scale_2",
	"_scale_inv",
	"_input_scale",
	"_weight_scale",
	"_weight_scale_2",
	"_weight_scale_inv",
	"_weight_zero_point",
	"_kv_scale",
	"_kv_cache_scaling_factor",
}

// IsExcludedTensor reports whether the tensor matches any pattern in
// the model's exclude/ignore list. Excluded modules stay at higher
// precision (FP16/BF16) and are NOT packed-quantized — their naive
// numel IS the real parameter count.
//
// Pattern semantics differ across producers (all observed in
// testdata/quant/):
//
//   - ModelOpt exclude_modules: substring patterns ("self_attn", "lm_head")
//   - fbgemm_fp8 modules_to_not_convert: full module paths (also substring)
//   - mxfp4 modules_to_not_convert: glob patterns ("model.layers.*.self_attn")
//
// A pattern containing "*" is treated as a glob (one "*" matches one
// dot-segment). Otherwise substring — the cheap default that handles
// the first two conventions.
func IsExcludedTensor(tensorName string, excludeModules []string) bool {
	for _, pattern := range excludeModules {
		if pattern == "" {
			continue
		}
		if strings.ContainsRune(pattern, '*') {
			if matchExcludeGlob(tensorName, pattern) {
				return true
			}
			continue
		}
		if strings.Contains(tensorName, pattern) {
			return true
		}
	}
	return false
}

// matchExcludeGlob compiles `pattern` (with "*" → "[^.]+" for one
// dot-segment) and substring-matches it against `tensorName`.
// Compiled regexes are cached because the same pattern fires for
// every tensor in a multi-shard model.
func matchExcludeGlob(tensorName, pattern string) bool {
	rx, ok := excludeGlobCache.Load(pattern)
	if !ok {
		parts := strings.Split(pattern, "*")
		quoted := make([]string, len(parts))
		for i, p := range parts {
			quoted[i] = regexp.QuoteMeta(p)
		}
		compiled, err := regexp.Compile(strings.Join(quoted, `[^.]+`))
		if err != nil {
			// Swallow compile failure: misclassify as "doesn't match"
			// rather than crash the controller on a malformed pattern.
			return false
		}
		excludeGlobCache.Store(pattern, compiled)
		rx = compiled
	}
	return rx.(*regexp.Regexp).MatchString(tensorName)
}

var excludeGlobCache sync.Map

// QuantPackingFactor returns the number of logical parameters held
// per safetensors storage element for the given (algo, dtype) pair.
// Returns 1 for "no packing" or unrecognized combinations — defaults
// conservatively to under-count rather than over-count.
//
// Coverage:
//
//	NVFP4 / MXFP4 / FP4 in U8/I8 → 2
//	INT4_* / W4A* in I32 → 8, in I16 → 4, in U8/I8 → 2
//	INT8_GPTQ in I32 → 4, in I16 → 2
//	FP8 → 1   (already 8 bits, no packing)
//	other → 1
func QuantPackingFactor(quantAlgo, dtype string) int64 {
	algo := strings.ToLower(strings.TrimSpace(quantAlgo))
	dt := strings.ToUpper(strings.TrimSpace(dtype))

	if algo == "" {
		return 1
	}

	switch {
	case algo == "int8_gptq":
		switch dt {
		case "I32", "U32", "INT32", "UINT32":
			return 4
		case "I16", "U16", "INT16", "UINT16":
			return 2
		}
	case algo == "nvfp4" || algo == "mxfp4" || algo == "fp4":
		switch dt {
		case "U8", "I8", "UINT8", "INT8":
			return 2
		}
	case strings.Contains(algo, "int4") || strings.Contains(algo, "w4a"):
		switch dt {
		case "I32", "U32", "INT32", "UINT32":
			return 8
		case "I16", "U16", "INT16", "UINT16":
			return 4
		case "U8", "I8", "UINT8", "INT8":
			return 2
		}
	}
	return 1
}

// CountTensorParams returns the logical parameter count for one tensor.
//
// nil quantConfig preserves backward-compat: returns the naive shape
// product like the legacy ParseSafetensorsHeader, INCLUDING any scale
// tensors a model happens to ship. Scale-skip is gated on quantConfig
// presence so unquantized model behavior doesn't change.
func CountTensorParams(name string, shape []int64, dtype string, quantConfig *HFQuantConfig) int64 {
	if len(shape) == 0 {
		return 0
	}

	count := int64(1)
	for _, dim := range shape {
		if dim <= 0 {
			return 0
		}
		// Defensive overflow guard: corrupted shapes wrap to negative
		// otherwise, poisoning the running total downstream.
		if count > math.MaxInt64/dim {
			return 0
		}
		count *= dim
	}

	if quantConfig == nil {
		return count
	}
	if IsScaleTensor(name) {
		return 0
	}
	if IsExcludedTensor(name, quantConfig.Quantization.ExcludeModules) {
		return count
	}
	factor := QuantPackingFactor(quantConfig.Quantization.QuantAlgo, dtype)
	if factor > 1 && count > math.MaxInt64/factor {
		return 0
	}
	return count * factor
}
