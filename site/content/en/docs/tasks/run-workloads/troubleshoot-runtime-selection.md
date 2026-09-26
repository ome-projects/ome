---
title: "Troubleshoot Runtime Selection Failures"
linkTitle: "Troubleshoot Runtime Selection"
weight: 20
date: 2026-09-26
description: >
  Learn how to read the RuntimeReady condition and the RuntimeNotFound event when OME cannot auto-select a serving runtime for your model.
---

This page shows you how to diagnose an InferenceService that reports **no runtime found** for its model. When an InferenceService specifies `spec.model` without `spec.runtime`, the controller auto-selects a runtime by scoring every ServingRuntime and ClusterServingRuntime against the model (see [Runtime Selection Logic](/ome/docs/concepts/serving_runtime/#runtime-selection-logic)). When that search comes up empty, OME tells you exactly how many runtimes it checked and why each incompatible one was excluded — you just need to know where to look and how to read the output.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- `kubectl` configured to communicate with your cluster
- An InferenceService whose `spec.model` references a **Ready** BaseModel or ClusterBaseModel

## How a selection failure surfaces

When auto-selection finds no compatible runtime, the controller deliberately does **not** treat it as a hard error:

- It records a **Warning event** with reason `RuntimeNotFound` on the InferenceService, carrying the full diagnostic message.
- It sets the **`RuntimeReady` condition to `False`** with reason `RuntimeNotFound` and the same message.
- It stops the reconcile **before touching any workloads**. A brand-new InferenceService creates no pods; an already-serving one keeps its existing pods running unchanged.
- `RuntimeReady` is advisory — it is **not** an input to the aggregate `Ready` condition, so a currently-serving InferenceService stays `Ready=True` (and the `READY` column in `kubectl get inferenceservice` stays `True`) even while its edited spec cannot resolve a runtime.
- The controller does not requeue with an error. It waits for the ServingRuntime / ClusterServingRuntime watch to re-trigger reconciliation, so the problem **self-heals** as soon as a compatible runtime is created or an existing one is fixed — you never need to touch the InferenceService itself.

## Step 1: Read the RuntimeReady condition

```bash
kubectl get inferenceservice qwen2-5-7b -n demo \
  -o jsonpath='{.status.conditions[?(@.type=="RuntimeReady")]}' | jq
```

Example output:

```json
{
  "type": "RuntimeReady",
  "status": "False",
  "severity": "Info",
  "reason": "RuntimeNotFound",
  "message": "no runtime found to support model safetensors with format safetensors in namespace demo. Checked 3 runtimes (0 namespace-scoped, 3 cluster-scoped). Excluded runtimes: srt-mistral-7b (model format 'mt:safetensors:1.0.0:Qwen2ForCausalLM:transformers:4.40.1' not in supported formats: architecture mismatch (model=Qwen2ForCausalLM, runtime=MistralForCausalLM)); srt-qwen2-5-32b (model size 7.62B is outside supported range [22B, 40B])",
  "lastTransitionTime": "2026-09-26T08:14:02Z"
}
```

If the condition is absent, the InferenceService has never hit a runtime-resolution problem — the controller only adds `RuntimeReady` on the first failure.

## Step 2: Read the RuntimeNotFound event

The same message is emitted as a Warning event, which is often the quickest place to spot it:

```bash
kubectl describe inferenceservice qwen2-5-7b -n demo
```

```
Events:
  Type     Reason           Age   From                 Message
  ----     ------           ----  ----                 -------
  Warning  RuntimeNotFound  12s   v1beta1Controllers   no runtime found to support model safetensors with format safetensors in namespace demo. Checked 3 runtimes (0 namespace-scoped, 3 cluster-scoped). Excluded runtimes: ...
```

Or filter events directly:

```bash
kubectl get events -n demo \
  --field-selector involvedObject.name=qwen2-5-7b,reason=RuntimeNotFound
```

## Anatomy of the diagnostic message

The message has three parts:

```
no runtime found to support model <format> with format <format> in namespace <namespace>.
Checked <N> runtimes (<X> namespace-scoped, <Y> cluster-scoped).
Excluded runtimes: <name> (<reason>); <name> (<reason>); ...
```

**The header.** Both the "model" and "format" placeholders render the model's *format name* (for example `safetensors`), not the BaseModel resource name — so `model safetensors with format safetensors` is normal, not a bug.

**The counts.** `Checked N runtimes` counts every ServingRuntime in the InferenceService's namespace (`X namespace-scoped`) plus every ClusterServingRuntime in the cluster (`Y cluster-scoped`), *including disabled ones*. If no runtimes exist at all, this clause is omitted entirely — a message with no counts means the search space was empty, and your first fix is to install runtimes.

**The excluded list.** One entry per runtime that failed a compatibility check, in no particular order. Each entry shows only the **first** check that runtime failed; the checks run in this order:

1. runtime is disabled
2. accelerator class
3. component deployment mode
4. supported model formats
5. model size range

So a runtime listed as `(runtime is disabled)` may have other problems too — re-enable it and re-read the message to see the next failure, if any.

## Reading the exclusion reasons

| Reason pattern | Meaning |
|----------------|---------|
| `runtime is disabled` | The runtime has `spec.disabled: true`. |
| `runtime does not support the required accelerator class` | The InferenceService requests an accelerator class (via `spec.acceleratorSelector.acceleratorClass`, a per-component `acceleratorOverride`, or the `ome.io/accelerator-class` annotation) that is not in the runtime's `acceleratorRequirements.acceleratorClasses`. |
| `runtime engine deployment mode <A> does not match requested engine deployment mode <B>` | Both the InferenceService and the runtime component explicitly declare deployment modes (e.g. `RawDeployment`, `MultiNode`) and they differ. The same pattern exists for `decoder`. |
| `model format 'mt:...' not in supported formats: <details>` | No entry in the runtime's `supportedModelFormats` matched the model. See below. |
| `model format 'mt:...' not in supported formats: no supported formats defined` | The runtime declares an empty `supportedModelFormats` list. |
| `model size <S> is outside supported range [<min>, <max>]` | A format matched, but the model's `modelParameterSize` falls outside the runtime's `modelSizeRange`. An unset bound renders as an open interval, e.g. `[1B, inf)` or `(-inf, 13B]`. |

### The format mismatch details

The `mt:` label identifies the model's matching signature: `mt:<format-name>[:<format-version>][:<architecture>][:<quantization>][:<framework-name>[:<framework-version>]]`, with unset fields omitted. Everything after `not in supported formats:` explains why each entry of the runtime's `supportedModelFormats` was rejected — one clause per entry, separated by `;`, and if a single entry mismatches on several attributes, those are separated by `,`.

The per-attribute reasons are:

- `architecture mismatch (model=LlamaForCausalLM, runtime=MistralForCausalLM)`
- `quantization mismatch (model=fp8, runtime=int4)`
- `format name mismatch (model=pytorch, runtime=safetensors)`
- `format version mismatch (model=1.0.0, runtime=2.0.0)`
- `framework name mismatch (model=transformers, runtime=pytorch)`
- `framework version mismatch (model=4.48.3, runtime=4.40.1)`

Matching on the optional attributes (architecture, quantization, framework, versions) is **strict on presence**: a field declared on only one side is a mismatch, not a wildcard. Those cases get their own phrasing, for example:

- `model has no architecture but runtime requires Qwen2ForCausalLM`
- `model has architecture Qwen2ForCausalLM but runtime has no architecture requirement`
- `model has no framework but runtime requires transformers`
- `model has no format version but runtime requires 1.0.0`

This is the most common surprise: a runtime whose supported format omits `modelArchitecture` does *not* accept every architecture — it only accepts models that also declare no architecture. Since the model agent normally fills in `modelArchitecture`, `modelFramework`, and format details when it parses a downloaded model, the fix is usually to make the runtime's `supportedModelFormats` entry declare the same attributes the model carries.

## Compatible but still not selected: check autoSelect

A runtime can pass **every** compatibility check and still not be auto-selected: at least one of its matching `supportedModelFormats` entries must set `autoSelect: true`. Such a runtime is counted in `Checked N runtimes` but does **not** appear in the excluded list — it was not incompatible, just not opted in to auto-selection.

So when the counts say 3 runtimes were checked but only 2 are listed as excluded, inspect the missing one:

```bash
kubectl get clusterservingruntime srt-qwen2-5-7b \
  -o jsonpath='{.spec.supportedModelFormats}' | jq
```

```json
[
  {
    "modelFormat": { "name": "safetensors", "version": "1.0.0" },
    "modelFramework": { "name": "transformers", "version": "4.40.1" },
    "modelArchitecture": "Qwen2ForCausalLM",
    "autoSelect": false,
    "priority": 1
  }
]
```

Either set `autoSelect: true` on the matching format, or bypass auto-selection by naming the runtime explicitly in the InferenceService's `spec.runtime.name`.

## Verify recovery

After you create a compatible runtime (or fix an excluded one), the controller re-reconciles automatically — the ServingRuntime and ClusterServingRuntime watches wake every InferenceService parked on `RuntimeReady=False`. On success the condition flips:

```json
{
  "type": "RuntimeReady",
  "status": "True",
  "severity": "Info",
  "reason": "RuntimeResolved"
}
```

and the service proceeds through its normal deployment flow. If `RuntimeReady` stays `False`, re-read the refreshed message — the excluded-runtimes list now reflects the updated runtimes, so the remaining reasons tell you what still doesn't match.

> **Note**: An InferenceService that names a runtime explicitly via `spec.runtime.name` reports a missing runtime through the same `RuntimeReady=False` condition and `RuntimeNotFound` event, but its message is a simple `runtime <name> not found ...` lookup failure — the checked/excluded breakdown on this page is specific to auto-selection.

## Next steps

- [Runtime Selection Logic](/ome/docs/concepts/serving_runtime/#runtime-selection-logic) — how compatible runtimes are scored and ranked once selection succeeds
- [Deploy a Simple Inference Service](/ome/docs/tasks/run-workloads/deploy-inference-service/) — the end-to-end deployment flow
