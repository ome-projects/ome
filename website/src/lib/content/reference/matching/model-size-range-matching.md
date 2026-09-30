---
title: Model size range matching
description: Auto-selection skips a runtime when the model's parameter count is outside the runtime's modelSizeRange.
---

A [serving runtime](../../concepts/runtimes/serving-runtimes.md) can limit the models it supports by size, with `modelSizeRange`. When OME chooses a runtime for a model, it skips any runtime whose range excludes the model's `modelParameterSize`. Both are parameter counts, such as `3.21B`, not bytes. Set a range on a runtime whose GPUs and engine settings suit models of one size, so that OME picks it only for those. A runtime that the InferenceService names is used anyway, with a warning. [Runtime selection scoring](runtime-selection-scoring.md#the-ranking-order) lists the other checks.

## The matching rule

| Field | On | What runtime selection does with it |
| --- | --- | --- |
| `spec.modelSizeRange.min` | ServingRuntime, ClusterServingRuntime | Rejects a model whose size is below it. |
| `spec.modelSizeRange.max` | ServingRuntime, ClusterServingRuntime | Rejects a model whose size is above it. |
| `spec.modelParameterSize` | [BaseModel](../../concepts/models/base-models.md), ClusterBaseModel | Compares it with the runtime's range. |

The catalog runtime [`srt-llama-3-2-3b-instruct`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-2-3b-instruct-rt.yaml) supports 2 to 4 billion parameters:

```yaml
spec:
  modelSizeRange:
    min: 2B
    max: 4B
```

The BaseModel `llama-3-2-3b-instruct` from [Model version matching](model-version-matching.md) sets a size inside that range:

```yaml
spec:
  modelParameterSize: "3.21B"
```

The bounds are inclusive. Since v1.3, either bound is optional; on v1.2.2, set both or leave out `modelSizeRange`. A range with `min` above `max` rejects any model with a size.

One range covers all the runtime's `supportedModelFormats` entries. A runtime without a range passes every model, and a model passes every range until it has a `modelParameterSize`. OME fills it in when it reads the model's files, and keeps a size that you set; see [What OME learns from the model](../../concepts/models/base-models.md#what-ome-learns-from-the-model).

When runtimes of the same kind tie on score, OME prefers the range closest to the model's size, and a runtime without a range counts as closest. See [Tie-breaks](runtime-selection-scoring.md#tie-breaks).

## Units and format

OME reads `min`, `max` and `modelParameterSize` the same way: a number, which can have a decimal point, then an optional uppercase suffix. It compares the counts, so `0.5B` equals `500M`, and a `1500M` model passes `min: 1B`.

| Suffix | Multiplies by | Example | Parameters |
| --- | --- | --- | --- |
| `M` | 1,000,000 | `500M` | 500 million |
| `B` | 1,000,000,000 | `3.21B` | 3.21 billion |
| `T` | 1,000,000,000,000 | `1.5T` | 1.5 trillion |
| None | 1 | `999` | 999 |

OME writes the sizes it fills in the same way, such as `3.21B` or `685B`.

!!! warning "OME reads a value it can't parse as 0"
    Write the suffix in upper case. OME reads a value it can't parse, such as `4b`, as 0, and nothing checks the values when you apply a runtime or a model. So a `max` of `4b` rejects every model with a valid size, and a `min` of `2b` sets no lower limit. The rejection reason shows the values as written, as in `[2B, 4b]`.

## Examples

Each row pairs a runtime's range with a model's size:

| `min` | `max` | `modelParameterSize` | Passes | Why |
| --- | --- | --- | --- | --- |
| `2B` | `4B` | `3.21B` | Yes | 3.21 billion is between 2 and 4 billion. |
| `2B` | `4B` | `4B` | Yes | The bounds are inclusive. |
| `500M` | `2B` | `3.21B` | No | The model's size is above `max`. |
| `1B` | `4B` | `1500M` | Yes | The suffixes can differ. |
| `70B` | Not set | `405B` | Yes | No upper limit. |
| Not set | `13B` | `3.21B` | Yes | No lower limit. |
| `2B` | `4B` | Not set | Yes | A model without a size always passes. |

## Where a rejection shows up

For the BaseModel above, the catalog runtime [`srt-llama-3-2-1b-instruct`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-2-1b-instruct-rt.yaml), with a range of `500M` to `2B`, gets this reason:

```text
model size 3.21B is outside supported range [500M, 2B]
```

Since v1.3, a bound that the runtime leaves out shows as `(-inf` or `inf)`, as in `[70B, inf)`. OME checks the size after the model format, so this reason shows only for a runtime with an entry that matches the model.

| When | What you see |
| --- | --- |
| Another runtime passes | OME picks from the runtimes that pass, with no event. Since v1.3, [`kubectl ome runtime explain --model`](../kubectl-ome/runtime.md#explain) shows this runtime as `No`, with the reason. |
| No runtime passes | The webhook rejects the InferenceService, and its message gives each failing runtime's reason. After admission, for example once OME fills in the model's size, OME sets `RuntimeReady` to `False` and records a Warning event, both with reason `RuntimeNotFound` (since v1.3). Running pods keep serving. See [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md). |
| The InferenceService names the runtime | OME uses it anyway. Since v1.3, the webhook warns `does not declare support for model`. OME records a `RuntimeCompatibilityAdvisory` Warning event, unless `spec.runtime.autoSync` is `false`. Both include the size reason. See [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md). |

The webhook checks only InferenceServices with an `engine` section, and leaves auto-selection until after admission when the engine sets `engine.runner.image`, or both `engine.leader.runner.image` and `engine.worker.runner.image`.

Both catalog runtimes above set `autoSelect: false`, so OME uses them only when an InferenceService names them.

## Resolve a rejection

- Check the case and suffix of the values in the reason: OME reads `4b` as 0.
- If the model's size is wrong, set the right one in `modelParameterSize`, and OME keeps it. The SIZE column of `kubectl get basemodels` or `kubectl get clusterbasemodels` shows it.
- Use a runtime whose range covers the model, or widen the range of a runtime whose resources fit the model.
- Or [name the runtime](../../guides/deploy-models/reference-a-runtime-explicitly.md) in `spec.runtime.name`: OME uses it with a warning. [Step 4: Fix the runtime or name one](../../guides/deploy-models/troubleshoot-runtime-selection.md#step-4-fix-the-runtime-or-name-one) shows how.

## Related pages

- [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md): find out why OME can't choose a runtime.
- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md): name the runtime instead of letting OME pick one.
- [Runtime selection scoring](runtime-selection-scoring.md): how OME orders the runtimes that pass.
- [Serving runtimes](../../concepts/runtimes/serving-runtimes.md): how a runtime declares the models it supports.
- [Base models](../../concepts/models/base-models.md): what OME learns from a model's files, its size included.
- [`kubectl ome runtime`](../kubectl-ome/runtime.md): see why OME skipped a runtime.
- [OME API](../api/ome.v1beta1.md#ome-io-v1beta1-ModelSizeRangeSpec): the fields of `ModelSizeRangeSpec`.
