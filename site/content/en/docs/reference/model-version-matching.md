---
title: Model Version Matching
linkTitle: Model Version Matching
weight: 3
description: >
  How version constraints in a runtime's supportedModelFormats are matched
  against a model's modelFormat and modelFramework versions.
---

When OME matches a model to a serving runtime, each entry in the runtime's
`supportedModelFormats` is compared against the model's `modelFormat` and
`modelFramework`. Names must match first; this page describes what happens
next — how the optional `version` fields are compared, what the `operator`
field does, and when ordering comparisons are refused.

For the full list of compatibility checks (architecture, quantization, size
range, and so on), see the
[Serving Runtime concept page](/ome/docs/concepts/serving_runtime).

## Where versions and operators are declared

A BaseModel (or ClusterBaseModel) declares its versions:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
spec:
  modelFormat:
    name: safetensors
    version: "1.0.0"
  modelFramework:
    name: transformers
    version: "4.36.2"
```

A ServingRuntime (or ClusterServingRuntime) declares the versions it supports,
each with an optional `operator`:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
        version: "1.0.0"
        operator: Equal
      modelFramework:
        name: transformers
        version: "4.50.0"
        operator: GreaterThanOrEqual
      autoSelect: true
```

The `operator` field accepts `Equal`, `GreaterThan`, or `GreaterThanOrEqual`
(see [`RuntimeSelectorOperator`](/ome/docs/reference/ome.v1beta1/#ome-io-v1beta1-RuntimeSelectorOperator)).
When omitted it defaults to `Equal`.

Although the shared `ModelFormat` and `ModelFrameworkSpec` schemas also expose
an `operator` field on the model side, only the operator declared on the
**runtime's** `supportedModelFormats` entry is consulted during matching. An
`operator` set on a BaseModel has no effect.

## When versions are compared

Version comparison runs independently for `modelFormat` and `modelFramework`,
and only after the corresponding names match:

- **Both sides declare a version**: the versions are compared using the
  runtime's operator, as described below.
- **Neither side declares a version**: the versions match.
- **Only one side declares a version**: the entry does not match. A runtime
  that pins `version: "1.0.0"` never matches a model that omits the version,
  and vice versa.

For `modelFramework` the same presence rule applies to the whole block: the
model and the runtime entry must either both declare a `modelFramework` or
both omit it.

## Accepted version formats

Versions are parsed with OME's model-version parser, which accepts one, two,
or three dot-separated numeric parts. The number of parts is called the
version's **precision**:

| Example | Precision |
|---------|-----------|
| `1`, `v1` | 1 (major) |
| `1.12`, `v1.12` | 2 (major.minor) |
| `0.6.0`, `v0.8.0` | 3 (major.minor.patch) |

Additional rules:

- A lowercase `v` prefix is allowed on the major part; `V1.0.0` does not parse.
- Each numeric part must not have leading zeroes (`1.08.0` does not parse).
- Three-part versions may carry a pre-release suffix (`4.51.3-SAM-HQ-preview`),
  build metadata (`4.43.0+build`), or a dev segment (`4.43.0.dev0`). These
  suffixes are only recognized after the patch part, so a two-part version
  like `1.2-beta` does not parse.
- A version that fails to parse — on either side — never matches. The
  `supportedModelFormats` entry is simply treated as incompatible.

## Operators

The comparison always places the **runtime's declared version on the left**
and the model's version on the right:

| Operator | Matches when |
|----------|--------------|
| `Equal` (default) | runtime version = model version |
| `GreaterThan` | runtime version > model version |
| `GreaterThanOrEqual` | runtime version ≥ model version |

In other words, the runtime declares the version it ships, and the ordering
operators express backward compatibility: a runtime with
`version: "4.50.0"` and `operator: GreaterThanOrEqual` matches models that
declare version `4.50.0` **or older** — not newer ones.

`Equal` compares the numeric parts with missing parts treated as zero, and it
ignores the `v` prefix — so `1.0` equals `1.0.0`, and `v1.0.0` equals
`1.0.0`. Any pre-release, build, or dev identifiers must be identical strings
on both sides.

## When ordering comparisons are refused

`GreaterThan` and `GreaterThanOrEqual` fall back to stricter behavior in two
situations:

- **Pre-release, build, or dev suffixes force equality.** If either version
  carries such a suffix (for example `1.8.0-dev` or `4.43.0.dev0`), the
  operator is ignored and the versions must be exactly equal. Ordering
  semantics on dev/alpha tags aren't well-defined, so OME doesn't guess.
- **Precision and prefix must agree.** Ordering is only attempted when both
  versions have the same precision (both `1.2`, or both `1.2.3`) and the same
  `v`-prefix state (both `v`-prefixed or both bare). Otherwise the entry does
  not match — comparing `1.0` against `1.0.0` with `GreaterThan` is refused
  rather than resolved.

## Examples

| Runtime declares | Operator | Model version | Match | Why |
|------------------|----------|---------------|-------|-----|
| `1.0.0` | `Equal` | `1.0.0` | Yes | Exact match |
| `1.8.0` | `GreaterThan` | `1.7.0` | Yes | Runtime version is greater |
| `1.8.0` | `GreaterThan` | `1.8.0` | No | Not strictly greater |
| `1.7.0` | `GreaterThan` | `1.8.0` | No | Runtime version is older than the model's |
| `1.8.0` | `GreaterThanOrEqual` | `1.8.0` | Yes | Equal satisfies ≥ |
| `1.9.0` | `GreaterThanOrEqual` | `1.8.0` | Yes | Runtime version is greater |
| `1.7.0` | `GreaterThanOrEqual` | `1.8.0` | No | Runtime version is older than the model's |
| `1.8.0-dev` | `GreaterThan` | `1.8.0-dev` | Yes | Pre-release forces equality; identical |
| `1.8.0-dev` | `GreaterThan` | `1.8.0-alpha` | No | Pre-release forces equality; different tags |
| `1.0` | `GreaterThan` | `1.0.0` | No | Precision differs (2 vs 3 parts) |
| `v1.0.0` | `GreaterThan` | `1.0.0` | No | One side is `v`-prefixed, the other is not |
| `1` | (omitted) | `1` | Yes | Omitted operator defaults to `Equal` |
| `v2.0.0` | (omitted) | `v2.0.0` | Yes | Omitted operator defaults to `Equal` |
