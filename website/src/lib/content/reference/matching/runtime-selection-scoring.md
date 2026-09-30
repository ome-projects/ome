---
title: Runtime selection scoring
description: "How OME picks a runtime for a model: namespace runtimes first, then by score, size range and name."
---

When an [InferenceService](../../concepts/serving/inference-services.md) names a model but no runtime, OME picks one of the [serving runtimes](../../concepts/runtimes/serving-runtimes.md) that support the model. A ServingRuntime in the InferenceService's namespace comes before any ClusterServingRuntime, and among runtimes of the same kind the highest score wins: `(format weight + framework weight) × priority`. To prefer a runtime, raise its entry's `priority`. To fix the choice, [name the runtime](../../guides/deploy-models/reference-a-runtime-explicitly.md).

## The ranking order

OME ranks the runtimes that qualify for the model. A runtime qualifies when:

- it isn't disabled
- it passes the [accelerator-class](runtime-accelerator-class-matching.md) and [deployment-mode](runtime-deployment-mode-matching.md) checks
- one of its `supportedModelFormats` entries [matches the model](../../concepts/runtimes/serving-runtimes.md#supported-model-formats), and the model's size fits its [`modelSizeRange`](model-size-range-matching.md)
- one of its entries sets `autoSelect: true`
- its score is above 0

OME picks the first runtime in this order:

1. ServingRuntimes in the InferenceService's namespace come before ClusterServingRuntimes, whatever their scores.
2. A higher [score](#how-the-score-is-computed) comes first.
3. For a model with a `modelParameterSize`, a [size range](#tie-breaks) closer to the model's size comes first.
4. Runtimes that still tie come in [name](#tie-breaks) order.

[Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md#how-a-selection-failure-shows-up) shows what you see when no runtime qualifies.

## Steer the choice

To make OME pick the runtime you want:

- Raise the `priority` or the `weight`s of the runtime's entry for the model's format and framework.
- Override the cluster's runtimes in one namespace with a ServingRuntime there that supports the model and sets `autoSelect: true` on an entry.
- Name the runtime in `spec.runtime.name`. OME then uses it, so the choice holds when you add or change other runtimes. See [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md).

When OME picks a runtime, it matches and renders the runtime's own spec, without [inheritance](../../concepts/runtimes/runtime-inheritance.md#when-the-merged-spec-applies), so name a child runtime instead of setting `autoSelect: true` on it.

## How the score is computed

A runtime's score is the highest score among its `supportedModelFormats` entries. An entry that matches the model's format and framework, [versions](model-version-matching.md) included, scores `(modelFormat.weight + modelFramework.weight) × priority`:

| Field | Default | Notes |
| --- | --- | --- |
| `modelFormat.weight` | 1 | `weight: 0` counts as 10. |
| `modelFramework.weight` | 1 | `weight: 0` counts as 5. |
| `priority` | 1 | Must be at least 1. |
| `autoSelect` | Not set | `false` leaves the entry out of the score; unset counts. |

Writing `weight: 0` raises the score rather than switching the weight off. A weight can be negative, which lowers the score.

This entry, from `vllm-llama` in the [worked example](#worked-example), scores `(2 + 1) × 2 = 6`:

```yaml
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
        version: "1.0.0"
        weight: 2
      modelFramework:
        name: transformers
        version: "4.45.0.dev0"
      modelArchitecture: LlamaForCausalLM
      autoSelect: true
      priority: 2
```

The score ignores `modelArchitecture`, `quantization` and `diffusionPipeline`. So when another entry with the same format and framework sets `autoSelect: true`, OME can still pick the runtime for a model that only an `autoSelect: false` entry matches.

Within one runtime, `autoSelect: true` entries that share an entry `name`, or that leave it empty, can't set different priorities. The admission webhook rejects such a runtime with `different priorities assigned for the model format`.

## Tie-breaks

For a model with a `modelParameterSize`, OME adds up how far each bound of a runtime's `modelSizeRange` is from the model's size. The smaller distance comes first:

- With both bounds, the distance is the range's width, so the narrowest range comes first.
- A runtime without a `modelSizeRange` has a distance of 0, the smallest possible.
- Since v1.3, a range with one bound counts the distance to that bound. On v1.2.2, set both bounds.

[Model size range matching](model-size-range-matching.md#units-and-format) says how OME reads the sizes.

Runtimes that still tie come in ascending order of their names, so `runtime-a` comes before `runtime-b`.

To make a size-specific runtime win over one without a range, or to settle a name tie, give the runtime you want a higher `priority`.

## Worked example

The BaseModel `llama-3-2-3b-instruct` from [Model version matching](model-version-matching.md) lives in `llama-demo`. It has the format `safetensors` `1.0.0`, the framework `transformers` `4.45.0.dev0`, the architecture `LlamaForCausalLM` and `modelParameterSize: "3.21B"`. The cluster has four runtimes, each with one entry that matches the model and sets `autoSelect: true`:

| Runtime | Kind | `modelSizeRange` | Score | Distance |
| --- | --- | --- | --- | --- |
| `team-llama` | ServingRuntime in `llama-demo` | Not set | (1 + 1) × 1 = 2 | 0 |
| `vllm-llama` | ClusterServingRuntime | Not set | (2 + 1) × 2 = 6 | 0 |
| `srt-llama-small` | ClusterServingRuntime | `1B` to `8B` | (1 + 1) × 3 = 6 | 7B |
| `srt-llama-large` | ClusterServingRuntime | `1B` to `70B` | (1 + 1) × 3 = 6 | 69B |

OME ranks them in this order:

1. `team-llama`, the only ServingRuntime, even though it has the lowest score.
2. `vllm-llama`. The three ClusterServingRuntimes tie at 6, and it has no size range, so its distance is 0.
3. `srt-llama-small`, whose range is narrower than `srt-llama-large`'s.
4. `srt-llama-large`.

To make `srt-llama-small` the first ClusterServingRuntime, raise its `priority` to 4. It then scores (1 + 1) × 4 = 8, above `vllm-llama`'s 6, but `team-llama` still wins in `llama-demo`. [When a runtime changes](../../concepts/runtimes/serving-runtimes.md#when-a-runtime-changes) says when OME picks again.

## Check the ranking

Since v1.3, `kubectl ome runtime explain` lists the runtimes in the order OME ranks them. Pass `--isvc` instead of `--model` to also apply an InferenceService's accelerator class and deployment mode; see [kubectl ome runtime](../kubectl-ome/runtime.md#explain). In the cluster from the worked example:

```bash
kubectl ome runtime explain --model llama-3-2-3b-instruct -n llama-demo
```

```output
RUNTIME           SCOPE        COMPATIBLE   PRIORITY   WEIGHT   REASON
team-llama        Namespaced   Yes          1          2        -
vllm-llama        Cluster      Yes          2          6        -
srt-llama-small   Cluster      Yes          3          6        -
srt-llama-large   Cluster      Yes          3          6        -
```

The first `Yes` row is the runtime OME picks. `WEIGHT` comes from the runtime's first matching entry, so it can differ from the score; [Output fields](../kubectl-ome/runtime.md#explain-output-fields) explains each column.

OME also shows the runtime it picked in these places:

| Where | What it shows |
| --- | --- |
| The warning from `kubectl apply` | `Runtime <runtime> will be auto-selected for model <model>` |
| The `serving-runtime` label on the serving workload | The runtime's name, as [How OME selects a runtime](../../concepts/runtimes/serving-runtimes.md#how-ome-selects-a-runtime) shows |
| `Selected runtime` lines in the manager's logs | The runtime in `runtime`, its `score`, and `isCluster`, which is `true` for a ClusterServingRuntime |

The InferenceService's status doesn't record the choice.

## Related pages

- [Serving runtimes](../../concepts/runtimes/serving-runtimes.md#how-ome-selects-a-runtime): how OME uses the runtime it picks, and when it picks again.
- [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md): find out why OME can't pick a runtime for a model.
- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md): name the runtime instead of letting OME pick one.
- [Model size range matching](model-size-range-matching.md): how OME parses a runtime's `modelSizeRange` and compares it with the model's size.
- [Model version matching](model-version-matching.md): how an entry's versions and operators match the model's.
- [kubectl ome runtime](../kubectl-ome/runtime.md): list the runtimes that match a model, in the order that OME ranks them.
- [OME API](../api/ome.v1beta1.md#ome-io-v1beta1-SupportedModelFormat): the fields of `SupportedModelFormat`.
