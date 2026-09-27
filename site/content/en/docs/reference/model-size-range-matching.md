---
title: Model Size-Range Matching
linkTitle: Model Size-Range Matching
weight: 3
description: >
  How a runtime's modelSizeRange bounds are parsed — parameter counts with M/B/T suffixes, not bytes — and compared against a model's modelParameterSize during runtime selection.
---

When an InferenceService names no runtime (`spec.runtime` is unset), OME auto-selects one by scoring every enabled ServingRuntime and ClusterServingRuntime against the model and the service (see [Runtime Selection Logic](/ome/docs/concepts/serving_runtime#runtime-selection-logic) for the other dimensions). One of the compatibility checks compares **model size**: a runtime can declare a `modelSizeRange`, and a model whose parameter count falls outside it is rejected — so a runtime tuned for 7B-class models is kept away from 70B models even when its `supportedModelFormats` would otherwise match.

Both sides of this comparison are **parameter counts, not bytes**. `min: 7B` means seven billion parameters, not seven bytes and not seven gigabytes on disk.

## Where the two sides are declared

The runtime declares the range at the top level of its spec — one range per runtime, not per `supportedModelFormats` entry:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-mistral-7b-instruct
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
        version: "1.0.0"
      autoSelect: true
      priority: 1
  modelSizeRange:
    min: 5B
    max: 9B
```

Both `min` and `max` are optional strings; setting only one bounds only that side.

The model side is the `spec.modelParameterSize` field of the BaseModel or ClusterBaseModel:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
spec:
  modelFormat:
    name: safetensors
  modelParameterSize: "7.62B"
```

When you leave `modelParameterSize` unset, the model-agent fills it automatically from the downloaded weights, in the same human-readable form (for example `7.62B` or `400M`); a value you set explicitly is never overwritten.

## Units and accepted format

Each bound — and the model's `modelParameterSize` — is parsed with the same rule: a decimal number with an optional uppercase suffix.

| Suffix | Multiplier | Example | Parsed value |
|--------|------------|---------|--------------|
| `M` | ×1,000,000 | `400M` | 4×10⁸ |
| `B` | ×1,000,000,000 | `7.62B` | 7.62×10⁹ |
| `T` | ×1,000,000,000,000 | `1.5T` | 1.5×10¹² |
| (none) | ×1 | `92564` | 92,564 |

The number part may be fractional, so `0.4B` and `400M` parse to the same count. Only these three suffixes are recognized, and only in uppercase.

### Malformed values silently become 0

Neither the CRD schema nor the admission webhook validates these strings — any value is accepted at admission, and a value that fails to parse is **silently treated as 0** at match time. That includes lowercase suffixes (`7b`), unrecognized suffixes (`524K`), and compound MoE notations (`8x7B`). The consequences differ by field:

- A malformed `min` collapses to 0, which effectively removes the lower bound.
- A malformed `max` collapses to 0, which rejects every model whose own size parses above 0 — the runtime silently drops out of auto-selection for virtually all models.
- A malformed `modelParameterSize` makes the model's size 0, so the model fails any range with a positive `min` and passes any range without one.

## The matching rule

> A runtime is rejected when the model's parsed parameter count is **below `min` or above `max`**. Both bounds are inclusive — a `7B` model passes a `min: 7B` bound. An unset `min` or `max` leaves that side unbounded. The check is skipped entirely when the model declares no `modelParameterSize` or the runtime declares no `modelSizeRange`.

Details of the comparison:

- **The size check runs only after a supported format matches.** The compatibility checks run in order — disabled, accelerator class, deployment mode, supported model formats, then model size — so a size rejection means the runtime's formats did match. A runtime whose formats don't match reports a format mismatch, never a size mismatch.
- **One range covers all formats.** Because `modelSizeRange` sits at the runtime level, the same bounds apply no matter which `supportedModelFormats` entry matched.
- **A model with no declared size always passes.** A runtime with size constraints still matches a model that carries no `modelParameterSize`; the compatibility report only records a warning ("model does not specify size, but runtime has size constraints"), it does not reject.
- **Passing is pass/fail, but size is consulted again for ranking.** Unlike the other matching dimensions, the model size also influences how the *surviving* candidates are ordered: when two candidates score the same on format and framework match, the one whose range fits the model more tightly ranks higher. This page covers only the pass/fail filter; see [Runtime Selection Logic](/ome/docs/concepts/serving_runtime#runtime-selection-logic) for ranking.

## Examples

| Runtime `min` | Runtime `max` | Model `modelParameterSize` | Outcome | Why |
|---------------|---------------|----------------------------|---------|-----|
| `5B` | `9B` | `7.62B` | candidate | 7.62×10⁹ is within both bounds |
| `2B` | `4B` | `7.62B` | **rejected** | above `max` |
| `7B` | `9B` | `7B` | candidate | bounds are inclusive |
| unset | `13B` | `350M` | candidate | no lower bound |
| `1B` | unset | `350M` | **rejected** | 3.5×10⁸ is below 10⁹ |
| `0.4B` | unset | `400M` | candidate | `0.4B` and `400M` are the same count |
| `1B` | `13B` | `8x7B` | **rejected** | `8x7B` does not parse, model size becomes 0 |
| `7b` | `13B` | `3B` | candidate | lowercase `7b` does not parse; `min` becomes 0 |
| `1B` | `13b` | `7B` | **rejected** | lowercase `13b` does not parse; `max` becomes 0 |

## Where a rejection surfaces

During auto-selection, a size rejection appears in the `RuntimeNotFound` diagnostic (event and `RuntimeReady` condition) as that runtime's exclusion reason:

```
model size 7.62B is outside supported range [22B, 40B]
```

The message quotes the strings exactly as written on the model and the runtime. An unset bound renders as an open interval: `[1B, inf)` when only `min` is set, `(-inf, 13B]` when only `max` is set. See [Troubleshoot Runtime Selection Failures](/ome/docs/tasks/run-workloads/troubleshoot-runtime-selection/) for reading the full diagnostic.

An explicitly named runtime (`spec.runtime.name`) goes through the same check, but a size mismatch there is downgraded to a `RuntimeCompatibilityAdvisory` warning event and reconciliation proceeds with the named runtime — the deliberate choice wins. (Sharded base models are the exception: validation failures on them remain hard errors.)
