---
title: Runtime Selection Scoring
linkTitle: Runtime Selection Scoring
weight: 3
description: >
  How runtime auto-selection ranks compatible runtimes: the supportedModelFormats weight × priority score, the model-size-range tie-break, and why namespace-scoped runtimes always outrank cluster-scoped ones.
---

When an InferenceService names no runtime (`spec.runtime` is unset), OME
auto-selects one: every enabled ServingRuntime and ClusterServingRuntime is
first checked for **compatibility** with the model, and the surviving
candidates are then **ranked**. This page documents the ranking — what
determines which compatible runtime actually wins.

For the compatibility filters that run before ranking (format, framework and
version matching, architecture, quantization, size range, deployment mode,
accelerator class), see
[Runtime Selection Logic](/ome/docs/concepts/serving_runtime#runtime-selection-logic),
[Model Version Matching](/ome/docs/reference/model-version-matching/),
[Deployment-Mode Matching](/ome/docs/reference/runtime-deployment-mode-matching/), and
[Accelerator-Class Matching](/ome/docs/reference/runtime-accelerator-class-matching/).
A runtime named explicitly via `spec.runtime.name` bypasses ranking entirely —
it is only validated, never scored.

## The ranking order

Compatible runtimes are ordered by four criteria, each consulted only when the
previous one ties:

1. **Runtime scope** — every compatible namespace-scoped ServingRuntime ranks
   ahead of every compatible ClusterServingRuntime, *regardless of score*.
2. **Score**, highest first — computed from the matching
   `supportedModelFormats` entries' `weight` and `priority`, as described
   below.
3. **Model-size-range distance**, smallest first — how tightly the runtime's
   `modelSizeRange` hugs the model's `modelParameterSize`. Skipped when the
   model declares no `modelParameterSize`.
4. **Name**, alphabetical — the lexicographically smallest name wins.

The name criterion makes selection fully deterministic: the same set of
runtimes and the same model always produce the same choice.

To be ranked at all, a runtime must be enabled (`disabled` unset or `false`),
pass every compatibility check, have `autoSelect: true` on at least one
`supportedModelFormats` entry, and score greater than zero. A compatible
runtime whose score is zero (for example because its only matching entry sets
`autoSelect: false`) is dropped from auto-selection.

### Scope beats score

Namespace-scoped and cluster-scoped candidates are ranked as two separate
lists and then concatenated, namespace list first. A ServingRuntime in the
InferenceService's namespace therefore always outranks a
ClusterServingRuntime, even one with a much higher score. Namespace runtimes
act as tenant-level overrides: to beat a cluster-wide runtime in one
namespace, it is enough to create any compatible, auto-selectable
ServingRuntime there — no weight or priority tuning required.

## How the score is computed

Each entry in the runtime's
[`supportedModelFormats`](/ome/docs/reference/ome.v1beta1/#ome-io-v1beta1-SupportedModelFormat)
is scored independently, and the **runtime's score is the best single entry's
score** — entries are not summed. Per entry:

```
score = (modelFormat.weight  [if the model's format matches]
       + modelFramework.weight [if the model's framework matches])
       × priority
```

An entry contributes a score only when it matches the model:

- The entry's `modelFormat.name` must equal the model's `modelFormat.name`.
  When both sides declare a `version`, the versions must also match under the
  entry's `operator` (see
  [Model Version Matching](/ome/docs/reference/model-version-matching/)); a
  version declared on only one side does not block scoring, although such an
  entry cannot by itself make the runtime *compatible*.
- The entry and the model must either **both** declare a `modelFramework` or
  both omit it, and when both declare one the names (and versions, per the
  entry's `operator`) must match. Since the CRD requires every
  `supportedModelFormats` entry to declare `modelFormat` and `modelFramework`,
  in practice a model needs `modelFramework` metadata to score.
- An entry with `autoSelect: false` is skipped when scoring. An entry that
  *omits* `autoSelect` still contributes to the score, but cannot be the
  entry that qualifies the runtime for auto-selection — at least one entry
  must set `autoSelect: true` explicitly.

A name mismatch on either dimension, or a failed version comparison, makes
the entry score zero.

### Weights

`weight` is declared on the entry's `modelFormat` and `modelFramework`
objects, on the **runtime** side. The CRD defaults each `weight` to **1**
when omitted, so an entry that sets no weights and no priority scores
`(1 + 1) × 1 = 2` when both format and framework match. A `weight` set on a
BaseModel's own `modelFormat`/`modelFramework` is never consulted.

Setting `weight: 0` explicitly does *not* zero out the contribution: the
selector treats a zero weight as unset and substitutes its built-in fallbacks
of **10** for `modelFormat` and **5** for `modelFramework`. To rank one
runtime above another, use weights greater than 1 (or `priority`) rather
than zeros.

### Priority

`priority` is declared per `supportedModelFormats` entry and multiplies the
summed weights. It must be at least 1 and defaults to **1** when omitted.
Because it scales both weights at once, it is the simplest knob for
preferring one runtime over another for the same model.

Two admission-webhook rules keep priorities meaningful:

- Within one runtime, auto-selectable entries with the same `name` must not
  declare different priorities.
- Two enabled runtimes of the same scope may not both auto-select on an
  identical `supportedModelFormats` entry (same name, versions, framework,
  architecture, quantization) with an identical `modelSizeRange`,
  overlapping protocol versions, and the same explicit `priority` — the
  second runtime is rejected at admission so that scores stay distinguishable.

### Example

For a model declaring `modelFormat: safetensors` and
`modelFramework: transformers` (both matching in every entry below):

| Runtime entry | Score |
|---------------|-------|
| no weights, no priority | (1 + 1) × 1 = **2** |
| no weights, `priority: 2` | (1 + 1) × 2 = **4** |
| `modelFormat.weight: 10`, `modelFramework.weight: 5`, `priority: 1` | (10 + 5) × 1 = **15** |
| `modelFormat.weight: 10`, `modelFramework.weight: 5`, `priority: 2` | (10 + 5) × 2 = **30** |

A runtime carrying both the first and the last entry scores 30 — the best
entry wins, and the weaker entry is ignored.

## Tie-break 1: model-size-range distance

When two runtimes in the same scope score identically and the model declares
`modelParameterSize`, the runtime whose
[`modelSizeRange`](/ome/docs/reference/ome.v1beta1/#ome-io-v1beta1-ModelSizeRangeSpec)
sits **closest** to the model size wins. The distance is the sum of
`|min − modelSize|` and `|max − modelSize|` over the bounds the runtime
declares; a bound that is unset contributes nothing.

Runtimes whose range excludes the model size were already rejected as
incompatible, so for a candidate that declares both bounds the model size
lies inside the range and the distance equals the **width of the range** —
the narrowest range wins. A 7B model choosing between a `1B–8B` runtime and a
`1B–70B` runtime at equal scores gets the `1B–8B` one.

Two consequences worth knowing:

- A runtime that declares **no** `modelSizeRange` has distance 0 and
  therefore wins this tie-break against any runtime whose declared range has
  nonzero width.
- The tie-break is skipped entirely when the model has no
  `modelParameterSize`; ranking then falls through to the name comparison.

Sizes on both sides are parsed with `M`/`B`/`T` suffixes (10⁶ / 10⁹ / 10¹²),
so `"700M"`, `"7B"`, and `"1T"` all compare on one scale. An unparsable size
string is treated as 0.

## Tie-break 2: name

If score and size distance both tie, the runtime whose name sorts first
alphabetically is selected. The admission rules above make exact ties
uncommon, but when they happen the outcome is still deterministic — renaming
a runtime can change which one wins a dead heat.

## Worked example

A ClusterBaseModel declares `safetensors` / `transformers` and
`modelParameterSize: "7B"`. Four runtimes are compatible and auto-selectable:

| Runtime | Scope | Entry | Score | Size distance |
|---------|-------|-------|-------|---------------|
| `srt-llama-large` | cluster | weights 10/5, `priority: 2` | 30 | `1B–70B` → 69B |
| `srt-llama-small` | cluster | weights 10/5, `priority: 2` | 30 | `1B–8B` → 7B |
| `srt-generic` | cluster | defaults | 2 | none → 0 |
| `team-runtime` | namespace | defaults | 2 | none → 0 |

Ranking: `team-runtime` first (namespace scope beats every cluster runtime
despite its low score), then `srt-llama-small` (ties `srt-llama-large` on
score 30, wins on the tighter size range), then `srt-llama-large`, then
`srt-generic`. The InferenceService gets `team-runtime`; deleting it would
hand selection to `srt-llama-small`.

## Observing a selection

The controller logs the winner with its score
(`"Selected runtime" runtime=... score=... isCluster=...`) when it
auto-selects, and the `kubectl ome runtime explain --model NAME` plugin
command lists which runtimes match a model and why. When *no* runtime
survives filtering, see
[Troubleshooting Runtime Selection](/ome/docs/tasks/run-workloads/troubleshoot-runtime-selection/)
for reading the resulting event and status condition.
