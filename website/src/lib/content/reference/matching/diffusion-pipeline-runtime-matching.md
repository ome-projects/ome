---
title: Diffusion pipeline runtime matching
description: "The diffusionPipeline field on a runtime's supportedModelFormats limits auto-selection to the diffusers pipelines and components it can serve."
---

Set `diffusionPipeline` on an entry in a [serving runtime](../../concepts/runtimes/serving-runtimes.md)'s `supportedModelFormats` to limit the entry to the diffusion pipelines the runtime can serve. OME compares it with the pipeline recorded on the [BaseModel](../../concepts/models/base-models.md) or ClusterBaseModel, and the entry matches only models that pass. The entry's `modelArchitecture` already checks the pipeline class, and `diffusionPipeline` can also require components, such as a transformer from the `diffusers` library. The check is pass or fail: it decides which runtimes OME can pick, and adds nothing to their [score](runtime-selection-scoring.md). The [catalog](../../concepts/runtimes/serving-runtimes.md#the-runtime-catalog)'s diffusion runtimes, `srt-qwen-image` and `srt-qwen-image-edit`, leave it unset and match on the class through `modelArchitecture`.

## The matching rule

An entry's `diffusionPipeline` has the same fields as the model's: `className`, the components `scheduler`, `textEncoder`, `tokenizer`, `transformer` and `vae`, and `additionalComponents`, keyed by the component's name in `model_index.json`. Each component has a `library` and a `type`. Every field is optional, so set only what the runtime depends on. Each field you set adds a requirement:

| The entry sets | The check fails when the model has |
| --- | --- |
| `diffusionPipeline`, even an empty one (`{}`) | No `diffusionPipeline`. |
| `className` | A different class, or none. |
| `scheduler`, `textEncoder`, `tokenizer`, `transformer` or `vae`, even an empty one | No such component. |
| `library` in a component | A different library in that component, or none. |
| `type` in a component | A different type in that component, or none. |
| A key in `additionalComponents` | No component under that key, or one whose `library` or `type` differs from what the key sets. |

Values are compared as exact, case-sensitive strings. An entry without `diffusionPipeline` passes for every model. [`DiffusionPipelineSpec`](../api/ome.v1beta1.md#ome-io-v1beta1-DiffusionPipelineSpec) in the API reference describes each field.

## Declare a pipeline in a runtime

The rest of the entry must match the model too:

- Set `modelArchitecture` to the pipeline class. OME records the class as the model's architecture, and an entry needs an architecture when the model has one.
- Set `modelFormat` and `modelFramework` to `diffusers`, each with the model's diffusers version. A dev or pre-release version, such as `0.34.0.dev0`, matches only the same version, whatever the entry's `operator`. See [Model version matching](model-version-matching.md).

This ClusterServingRuntime serves `QwenImagePipeline` models at diffusers version `0.34.0.dev0` whose transformer comes from the `diffusers` library:

```yaml title="runtime.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-qwen-image-pipeline
spec:
  supportedModelFormats:
    - modelFormat:
        name: diffusers
        version: "0.34.0.dev0"
      modelFramework:
        name: diffusers
        version: "0.34.0.dev0"
      modelArchitecture: QwenImagePipeline
      diffusionPipeline:
        className: QwenImagePipeline
        transformer:
          library: diffusers
      autoSelect: true
      priority: 2
  engineConfig:
    runner:
      name: ome-container
      image: sglang:0.5.6-diffusion-cuda129-amd64
      ports:
        - containerPort: 8080
          name: http1
          protocol: TCP
      command:
        - sglang
        - serve
        - --host
        - "0.0.0.0"
        - --port
        - "8080"
        - --model-path
        - $(MODEL_PATH)
        - --served-model-name
        - Qwen/Qwen-Image
      resources:
        requests:
          cpu: "32"
          memory: 100Gi
          nvidia.com/gpu: 1
        limits:
          cpu: "32"
          memory: 100Gi
          nvidia.com/gpu: 1
```

```bash
kubectl apply -f runtime.yaml
```

```output
clusterservingruntime.ome.io/srt-qwen-image-pipeline created
```

`priority: 2` makes OME prefer this runtime to `srt-qwen-image` when both match a model. See [Runtime selection scoring](runtime-selection-scoring.md). A runtime that serves several pipelines has an entry for each, as `srt-qwen-image-edit` does for `QwenImageEditPlusPipeline` and `QwenImageEditPipeline`.

Adding `diffusionPipeline` to a runtime that OME already picks for `pvc://` models stops it from matching them, because they have [no recorded pipeline](#models-without-a-recorded-pipeline). Set `spec.diffusionPipeline` on those models first.

## What OME records for the model

When the [model agent](../../guides/operate-ome/model-agent.md) downloads a diffusion model, it reads `model_index.json` instead of `config.json`, and OME fills in these fields:

| Field | What OME records |
| --- | --- |
| `diffusionPipeline` | The pipeline class and its components, from `model_index.json`. |
| `modelArchitecture` | The pipeline class, such as `QwenImagePipeline`. |
| `modelFormat` | `diffusers`, with the version from `_diffusers_version`. |
| `modelFramework` | `diffusers`, with the same version. |

OME fills in only empty fields, so a `diffusionPipeline` that you set stays as written, even one that lists only some components.

The keys of `model_index.json` become these `diffusionPipeline` fields:

| `model_index.json` key | `diffusionPipeline` field |
| --- | --- |
| `_class_name` | `className` |
| `text_encoder` | `textEncoder` |
| `transformer` or `unet` | `transformer` |
| `scheduler`, `tokenizer` and `vae` | The same name |
| Any other key whose value is a component | `additionalComponents`, under the same key |

A component's value lists its `library` and then its `type`, as in `["diffusers", "AutoencoderKL"]`. For example, OME reads this `model_index.json`:

```json title="model_index.json"
{
  "_class_name": "StableDiffusionPipeline",
  "_diffusers_version": "0.24.0",
  "scheduler": ["diffusers", "EulerDiscreteScheduler"],
  "text_encoder": ["transformers", "CLIPTextModel"],
  "tokenizer": ["transformers", "CLIPTokenizer"],
  "unet": ["diffusers", "UNet2DConditionModel"],
  "vae": ["diffusers", "AutoencoderKL"],
  "safety_checker": ["diffusers", "StableDiffusionSafetyChecker"]
}
```

and records this pipeline:

```yaml
diffusionPipeline:
  className: StableDiffusionPipeline
  scheduler:
    library: diffusers
    type: EulerDiscreteScheduler
  textEncoder:
    library: transformers
    type: CLIPTextModel
  tokenizer:
    library: transformers
    type: CLIPTokenizer
  transformer:
    library: diffusers
    type: UNet2DConditionModel
  vae:
    library: diffusers
    type: AutoencoderKL
  additionalComponents:
    safety_checker:
      library: diffusers
      type: StableDiffusionSafetyChecker
```

`unet` becomes `transformer`, and `safety_checker`, which has no field of its own, goes under `additionalComponents`.

## Models without a recorded pipeline

OME records no pipeline for a [`pvc://` model](../../guides/deploy-models/serve-models-from-pvc.md): its metadata Job fills in the architecture, format and framework, but not `diffusionPipeline`. Other models get none of these fields if they're `vendor://` models, if the agent can't read their files, or if they have the annotation `ome.oracle.com/skip-config-parsing: "true"`, so [set them yourself](../../concepts/models/base-models.md#set-the-metadata-yourself).

To check a model, print its architecture and pipeline. For `qwen-image-pvc`, a ClusterBaseModel served from a PVC at diffusers version `0.34.0.dev0`, the pipeline is empty:

```bash
kubectl get clusterbasemodel qwen-image-pvc \
  -o 'custom-columns=ARCHITECTURE:.spec.modelArchitecture,PIPELINE:.spec.diffusionPipeline'
```

```output
ARCHITECTURE        PIPELINE
QwenImagePipeline   <none>
```

For a BaseModel, use `basemodel` and add `-n` with its namespace.

To match an entry that sets `diffusionPipeline`, set one on the model yourself, with at least the fields that the entry requires. This patch makes `qwen-image-pvc` match `srt-qwen-image-pipeline`:

```bash
kubectl patch clusterbasemodel qwen-image-pvc --type merge \
  -p '{"spec":{"diffusionPipeline":{"className":"QwenImagePipeline","transformer":{"library":"diffusers"}}}}'
```

```output
clusterbasemodel.ome.io/qwen-image-pvc patched
```

OME doesn't check InferenceServices again when a model changes. Apply a rejected InferenceService again, or change an existing one, as [Step 5: Verify recovery](../../guides/deploy-models/troubleshoot-runtime-selection.md#step-5-verify-recovery) shows.

## Where a rejection shows up {#exclusion-reasons}

An entry that fails the pipeline check, or sets no `modelArchitecture`, gets one of these reasons. The pipeline check names only its first mismatch, so fix it and check again.

| Reason | Cause |
| --- | --- |
| `diffusion pipeline required by runtime but not specified in model` | The model has no pipeline. See [Models without a recorded pipeline](#models-without-a-recorded-pipeline). |
| `pipeline class mismatch (model=QwenImageEditPipeline, runtime=QwenImagePipeline)` | The classes differ, or the model's pipeline has none (`model=<nil>`). |
| `component transformer required by runtime but not specified in model` | The model's pipeline has no such component. |
| `transformer library mismatch (model=transformers, runtime=diffusers)` | The libraries differ, or the model's component has none (an empty `model=`). |
| `scheduler type mismatch (model=EulerDiscreteScheduler, runtime=FlowMatchEulerDiscreteScheduler)` | The types differ, or the model's component has none. |
| `diffusion pipeline missing required additional components` | The entry sets `additionalComponents`, and the model has none. |
| `diffusion component safety_checker missing in model` | The model's `additionalComponents` has no such key. |
| `model has architecture QwenImagePipeline but runtime has no architecture requirement` | The entry doesn't set `modelArchitecture`. Set it to the pipeline class. |

When no runtime matches `qwen-image-pvc`, OME lists the example runtime among the runtimes it excluded:

```text
srt-qwen-image-pipeline (model format 'mt:diffusers:0.34.0.dev0:QwenImagePipeline:diffusers:0.34.0.dev0' not in supported formats: diffusion pipeline required by runtime but not specified in model)
```

| When | What you see |
| --- | --- |
| Another runtime matches | OME picks from the runtimes that match, with no event. Since v1.3, [`kubectl ome runtime explain --model`](../kubectl-ome/runtime.md#explain) shows this one as `No`, with the reason. |
| No runtime matches | The webhook rejects the InferenceService if it has an `engine` section and no runner image of its own. After admission, OME sets `RuntimeReady` to `False` and records a Warning event, both with reason `RuntimeNotFound` (since v1.3). Running pods keep serving. See [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md). |
| The InferenceService names the runtime | OME uses it anyway. Since v1.3, the webhook warns `does not declare support for model` when the InferenceService has an `engine`. OME records a `RuntimeCompatibilityAdvisory` Warning event, unless `spec.runtime.autoSync` is `false`. See [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md#what-ome-checks-when-you-apply). |

## Related pages

- [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md): find out why OME can't choose a runtime for a model.
- [Serving runtimes](../../concepts/runtimes/serving-runtimes.md): how a runtime declares the models it supports.
- [Runtime selection scoring](runtime-selection-scoring.md): how OME ranks the runtimes that match.
- [Model version matching](model-version-matching.md): how OME compares format and framework versions.
- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md): name a runtime on an InferenceService instead of letting OME choose one.
- [`kubectl ome runtime`](../kubectl-ome/runtime.md): see why a runtime was skipped.
