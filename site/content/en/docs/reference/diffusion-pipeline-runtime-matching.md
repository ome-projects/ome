---
title: Diffusion Pipeline Runtime Matching
linkTitle: Diffusion Pipeline Matching
weight: 3
description: >
  How `supportedModelFormats[].diffusionPipeline` constrains runtime selection to specific diffusers pipelines and their components.
---

Diffusion models built with the Hugging Face diffusers library all share the same coarse
metadata: model format `diffusers` and model framework `diffusers`. Format and framework
alone therefore cannot distinguish a Qwen-Image deployment from a Stable Diffusion XL
deployment. The `diffusionPipeline` field on a `supportedModelFormats` entry lets a
ServingRuntime declare exactly which diffusers pipeline — and optionally which pipeline
components — it can serve, so [runtime selection](/ome/docs/concepts/serving_runtime/#runtime-selection-logic)
never auto-selects a runtime for a pipeline it was not built for.

## Where the model-side metadata comes from

Matching compares the runtime's `diffusionPipeline` constraint against the
`spec.diffusionPipeline` field of the BaseModel or ClusterBaseModel. That field mirrors
the model's `model_index.json` file, and the model-agent fills it automatically after
downloading the model if you leave it unset (fields you set explicitly are never
overwritten). Given this `model_index.json`:

```json
{
  "_class_name": "QwenImagePipeline",
  "_diffusers_version": "0.34.0.dev0",
  "scheduler": ["diffusers", "FlowMatchEulerDiscreteScheduler"],
  "text_encoder": ["transformers", "Qwen2_5_VLForConditionalGeneration"],
  "tokenizer": ["transformers", "Qwen2Tokenizer"],
  "transformer": ["diffusers", "QwenImageTransformer2DModel"],
  "vae": ["diffusers", "AutoencoderKLQwenImage"]
}
```

the resulting model spec is:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: qwen-image
spec:
  modelFormat:
    name: diffusers
    version: "0.34.0.dev0"
  modelFramework:
    name: diffusers
    version: "0.34.0.dev0"
  diffusionPipeline:
    className: QwenImagePipeline
    scheduler:
      library: diffusers
      type: FlowMatchEulerDiscreteScheduler
    textEncoder:
      library: transformers
      type: Qwen2_5_VLForConditionalGeneration
    tokenizer:
      library: transformers
      type: Qwen2Tokenizer
    transformer:
      library: diffusers
      type: QwenImageTransformer2DModel
    vae:
      library: diffusers
      type: AutoencoderKLQwenImage
  storage:
    storageUri: hf://Qwen/Qwen-Image
    path: /raid/models/Qwen/Qwen-Image
```

A `model_index.json` component entry is typically a `[library, class]` pair that maps to
a component's `library` and `type` fields. The well-known keys `scheduler`,
`text_encoder`, `tokenizer`, `transformer`, and `vae` map to the typed fields of the same
names; the legacy `unet` key from older Stable Diffusion 1.x/2.x checkpoints also maps to
`transformer`. Any other component entry (for example `image_encoder`) lands in
`additionalComponents` under its original key; non-component entries such as boolean
flags are ignored.

The model-agent also records the pipeline class name as the model's
`spec.modelArchitecture`, so for diffusion models `modelArchitecture` and
`diffusionPipeline.className` carry the same value.

## Declaring supported pipelines in a ServingRuntime

A runtime constrains itself to a pipeline by adding `diffusionPipeline` to a
`supportedModelFormats` entry. Every field inside the constraint is optional; anything
you leave out is a wildcard:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-qwen-image
spec:
  supportedModelFormats:
    - modelFormat:
        name: diffusers
        version: "0.34.0.dev0"
      modelFramework:
        name: diffusers
        version: "0.34.0.dev0"
      diffusionPipeline:
        className: QwenImagePipeline
        scheduler:
          library: diffusers
          type: FlowMatchEulerDiscreteScheduler
        transformer:
          library: diffusers
          type: QwenImageTransformer2DModel
        vae:
          library: diffusers
          type: AutoencoderKLQwenImage
      autoSelect: true
      priority: 1
  protocolVersions:
    - openAI
```

The constraint is scoped to its `supportedModelFormats` entry, so a runtime that serves
several pipelines lists one entry per pipeline, each with its own `diffusionPipeline`
block.

Because the pipeline class doubles as the model architecture, a runtime that only needs
pipeline-level filtering can instead set `modelArchitecture: QwenImagePipeline` on the
entry — the pre-configured `srt-qwen-image` runtime in `config/runtimes/srt/Qwen/` does
exactly that. Use `diffusionPipeline` when you additionally need to pin individual
components, for example a runtime that only supports a specific scheduler or VAE
implementation.

## Matching rules

The diffusion pipeline check is one of the per-entry gates evaluated during runtime
selection, alongside model format name/version, framework, architecture, and
quantization. It is strictly pass/fail: it can disqualify a `supportedModelFormats`
entry, but it contributes no score. Weighting still comes only from
`modelFormat.weight`, `modelFramework.weight`, and `priority`.

The comparison walks the runtime constraint and checks each part against the model's
`spec.diffusionPipeline`:

| Runtime declares | Model declares | Result |
|---|---|---|
| No `diffusionPipeline` | Anything (or nothing) | Match — no constraint |
| `diffusionPipeline` (even empty `{}`) | No `diffusionPipeline` | Mismatch |
| `className` | Equal `className` | Match |
| `className` | Different or unset `className` | Mismatch |
| A component (even empty `{}`) | That component absent | Mismatch |
| Component `library` and/or `type` | Equal values on the same component | Match |
| Component `library` or `type` | Different value | Mismatch |
| `additionalComponents` key | Same key with matching `library`/`type` | Match |
| `additionalComponents` key | Key absent | Mismatch |

In detail:

- **A runtime without `diffusionPipeline` accepts everything.** It matches diffusion
  models and non-diffusion models alike (subject to the other format gates). The
  constraint only ever narrows what a runtime accepts; a model declaring
  `diffusionPipeline` never disqualifies a runtime that doesn't mention it.
- **`className` requires exact equality when set.** If the runtime sets `className`, the
  model must declare the identical class name. An unset runtime `className` matches any
  pipeline class.
- **Declaring a component makes it required.** For each of `scheduler`, `textEncoder`,
  `tokenizer`, `transformer`, and `vae` that the runtime declares — even as an empty
  object — the model must declare that component. Within a declared component, a
  non-empty `library` must equal the model's library and a non-empty `type` must equal
  the model's type; empty strings are wildcards. So `scheduler: {}` means "the model
  must have a scheduler, any implementation", while
  `scheduler: {library: diffusers}` additionally pins the library.
- **`additionalComponents` are matched by key.** Every key in the runtime's map must
  exist in the model's `additionalComponents` and pass the same library/type check.
  Model components that the runtime does not mention are ignored — a model with extra
  components still matches.
- **All comparisons are exact string equality.** Unlike `modelFormat.version` and
  `modelFramework.version`, there are no version operators for pipeline metadata.

## Troubleshooting

When no compatible runtime is found (or an explicitly pinned runtime is incompatible),
the selection error and the controller logs include the first mismatch reason per
runtime. The diffusion pipeline gate produces these reason strings:

| Reason | Cause |
|---|---|
| `diffusion pipeline required by runtime but not specified in model` | The runtime entry sets `diffusionPipeline` but the model's `spec.diffusionPipeline` is unset — commonly a model whose metadata the model-agent has not parsed yet |
| `pipeline class mismatch (model=..., runtime=...)` | `className` values differ, or the runtime sets one and the model does not (`model=<nil>`) |
| `component <name> required by runtime but not specified in model` | The runtime declares a named component the model omits |
| `<name> library mismatch (model=..., runtime=...)` | A component's `library` differs |
| `<name> type mismatch (model=..., runtime=...)` | A component's `type` differs |
| `diffusion pipeline missing required additional components` | The runtime lists `additionalComponents` but the model has none |
| `diffusion component <key> missing in model` | A specific `additionalComponents` key is absent from the model |
