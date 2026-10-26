# Hugging Face Model Configuration Parser

This package parses Hugging Face model configurations (`config.json` and
`model_index.json`) into a uniform Go interface so callers can extract
metadata — parameter count, context length, architecture, vision
capability, quantization — without knowing the model type in advance.

## How it works

A single `GenericModelConfig` parses every transformer `config.json`,
and a single `GenericDiffusionModelConfig` parses every diffusers
`model_index.json`. There is no per-`model_type` registry, no
per-family struct.

What the resolver derives:

- **Architecture / model_type / dtype / transformers version**: read
  from top-level JSON, with a fallback to nested `text_config`,
  `llm_config`, or `language_config` for multimodal configs that
  push these fields down.
- **Context length**: cascade through `max_position_embeddings`,
  `max_sequence_length`, `max_seq_len` (DBRX), `seq_length`
  (ChatGLM), `model_max_length` (Baichuan). First non-zero wins.
  Nested LLM-config values are folded into the top-level fields by
  the same probe.
- **Parameter count**: try sibling safetensors via
  `FindAndParseSafetensors` first; fall back to architecture-based
  estimation (MoE-aware when `n_routed_experts` /
  `num_local_experts` / `num_experts` is present).
- **Vision capability**: detected from JSON shape — any of
  `vision_config`, `mm_vision_tower`, `image_token_id`,
  `image_token_index`, or `img_processor` flips it on. No
  per-`model_type` allowlist.
- **Embedding**: a `config_sentence_transformers.json` beside the
  config that names a `similarity_fn_name` marks the model as an
  embedding model. Decoder-based embedders such as Qwen3-Embedding
  have the `config.json` of a causal LM, so this file is their only
  signal.
- **Quantization**: read from `quantization_config.quant_method`.

`pkg/modelparser` consumes this interface and maps the result onto
OME's `ModelCapability` enum.

## Usage

```go
import "sigs.k8s.io/ome/pkg/modelconfig"

// From a file path:
model, err := modelconfig.LoadModelConfig("/path/to/config.json")
if err != nil {
    return err
}
fmt.Println("Parameters:", model.GetParameterCount())
fmt.Println("Context:   ", model.GetContextLength())
fmt.Println("Vision:    ", model.HasVision())

// From in-memory bytes (e.g., when serving from object storage):
model, err = modelconfig.ParseModelConfig(modelconfig.ModelConfigInput{
    Path: "config.json",
    Data: bytes,
})
```

`LoadModelConfig` and `ParseModelConfig` both return
`HuggingFaceModel`. Diffusion pipelines additionally satisfy
`HuggingFaceDiffusionModel` — type-assert to it when you need
pipeline metadata:

```go
if dm, ok := model.(modelconfig.HuggingFaceDiffusionModel); ok {
    pipeline := dm.GetDiffusionModel()
    // ... access pipeline.Scheduler, .TextEncoder, etc.
}
```

See `examples/` for end-to-end runnable code.

## Files

| File | Responsibility |
|---|---|
| `interface.go` | Public contract: `HuggingFaceModel`, `HuggingFaceDiffusionModel`, `ModelConfigInput`. |
| `base.go` | Shared data types (`BaseModelConfig`, `AutoMap`, `QuantizationConfig`, `RopeScalingConfig`) and pure utilities (`FormatSize`, `FormatParamCount`, `EstimateModelSizeBytes`, `SanitizeJSONBytes`). |
| `config.go` | `GenericModelConfig`, the JSON-shape vision detector, nested-config probing, MoE-aware parameter estimation. |
| `diffusion.go` | Diffusion pipeline parsing (`DiffusionPipelineSpec`) and `GenericDiffusionModelConfig` — sums component params via safetensors. |
| `loader.go` | Entry points: `LoadModelConfig` (path), `ParseModelConfig` (bytes). |
| `safetensors.go` | Safetensors header parsing for accurate parameter counts. |
| `testdata/` | 70+ real model configs used by the test suite. |
| `examples/` | Runnable examples. |

## Adding a new model

Most of the time, no code change is needed. Drop the model's
`config.json` into `testdata/` and run the test suite:

```bash
go test ./pkg/modelconfig/ -run TestAllFixturesParse -v
```

The fixture walker (`fixtures_test.go`) loads every config under
`testdata/` and asserts a Loose contract: parses without error,
non-empty `model_type` and architecture, positive context length,
diffusion fixtures satisfy `HuggingFaceDiffusionModel`. If your
fixture passes, the parser already handles it.

If a fixture fails:

- **Vision not detected**: add the relevant JSON-shape key to the
  `visionShapeKeys` list in `config.go` (only if the existing
  signals genuinely don't appear).
- **Context length zero**: check whether the JSON exposes an
  unrecognized field name, then add it to the cascade in
  `GetContextLength` and to the `GenericModelConfig` struct.
- **Truly sparse config** (no architecture anywhere, or an
  exotic derived-context formula): add the fixture to
  `fixturesSkipped` in `fixtures_test.go` with a one-line reason.
  The walker is intentionally strict; skips are documented.

Parameter counts derived from `config.json` alone are estimates —
they're inherently unreliable across architectures. The walker does
not assert on them; if you need accurate counts, place the model's
`.safetensors` files (or a `model.safetensors.index.json`) next to
the config so `FindAndParseSafetensors` can read the real counts.

## Known fixture skips

Two fixtures are excluded from the walker, with documented reasons in
`fixtures_test.go`:

- `clip_vision_model.json` — a CLIP vision-encoder sub-component, not
  a complete model. No text context length by design.
- `janus_1.3b.json` — `multi_modality` config with no `architectures`
  field at top level or in any nested config. The previous per-model
  parser hardcoded `JanusMultiModalityCausalLM`; we don't synthesize
  data the JSON doesn't have.
- `gemma3.json` — context length only derivable as
  `text_config.sliding_window * rope_scaling.factor`, an exotic formula
  we deliberately don't carry in the generic resolver.
