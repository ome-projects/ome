---
title: Troubleshoot runtime selection
description: Find out why OME can't auto-select a runtime for your model, from kubectl's error or the RuntimeReady condition, then fix the runtime or name one.
---

When an [InferenceService](../../concepts/serving/inference-services.md) names no runtime and OME can't pick a [serving runtime](../../concepts/runtimes/serving-runtimes.md) for its model, OME lists the runtimes it excluded and why. Read that message, then fix the runtime or name one yourself.

The examples use an InferenceService `qwen2-5-7b` in the namespace `demo`, which serves the ClusterBaseModel `qwen2-5-7b`. The cluster has two ClusterServingRuntimes from the OME catalog, `srt-mistral-7b-instruct` and `srt-qwen2-5-7b`.

<div class="prerequisites" markdown>

- `kubectl` access to read and patch InferenceServices and runtimes, and to read models.
- `jq`, to format JSON output.
- Optionally, the [`kubectl ome`](../../reference/kubectl-ome/overview.md#install) plugin, for Step 3. Until v1.3 is released, build it from source.
- A model that's `Ready`, so that OME has read its format from its files; see [What OME learns from the model](../../concepts/models/base-models.md#what-ome-learns-from-the-model). If the model is a BaseModel, the InferenceService sets `spec.model.kind: BaseModel`.

</div>

## How a selection failure shows up

When you apply an InferenceService that has an `engine` section and no [runner image of its own](../../reference/matching/model-version-matching.md#where-a-mismatch-shows-up), the admission webhook rejects it if no runtime matches. The error begins with `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:` and goes on with a message like this one:

```text
no supporting runtime found for model qwen2-5-7b and engine does not have complete runner configuration: no runtime found to support model safetensors with format safetensors in namespace demo. Checked 2 runtimes (0 namespace-scoped, 2 cluster-scoped). Excluded runtimes: srt-mistral-7b-instruct (model format 'mt:safetensors:1.0.0:Qwen2ForCausalLM:transformers:4.40.1' not in supported formats: architecture mismatch (model=Qwen2ForCausalLM, runtime=MistralForCausalLM), framework version mismatch (model=4.40.1, runtime=4.36.0))
```

Ignore the part about runner configuration: OME needs a runtime for the model either way. A new InferenceService isn't created, so go to [Step 2](#step-3-read-the-exclusion-reasons). One that already exists gets the same rejection on every update, even a label change, until a runtime matches or you name one.

OME catches the other failures later, with the same message. That covers an InferenceService with no `engine` section or with its own runner image, and one that stops matching, for example after you edit or delete a runtime. A new InferenceService then gets no pods, and one that's serving keeps its pods and stays `Ready`. Since v1.3, OME also sets the `RuntimeReady` condition to `False` and records a `RuntimeNotFound` Warning event, which Step 1 reads.

## Step 1: Read the RuntimeReady condition {since=v1.3}

The condition holds OME's message:

```bash
kubectl get inferenceservice qwen2-5-7b -n demo \
  -o jsonpath='{.status.conditions[?(@.type=="RuntimeReady")]}' | jq
```

```output
{
  "lastTransitionTime": "2026-09-28T08:14:02Z",
  "message": "no runtime found to support model safetensors with format safetensors in namespace demo. Checked 2 runtimes (0 namespace-scoped, 2 cluster-scoped). Excluded runtimes: srt-mistral-7b-instruct (model format 'mt:safetensors:1.0.0:Qwen2ForCausalLM:transformers:4.40.1' not in supported formats: architecture mismatch (model=Qwen2ForCausalLM, runtime=MistralForCausalLM), framework version mismatch (model=4.40.1, runtime=4.36.0))",
  "reason": "RuntimeNotFound",
  "severity": "Info",
  "status": "False",
  "type": "RuntimeReady"
}
```

The same message is in a Warning event, which Kubernetes deletes after a while. On v1.2.2, which sets no condition, the event's message starts with `Failed to find runtime for model qwen2-5-7b: `. Its reason is `RuntimeSelectionError`, so put that reason in the command:

```bash
kubectl get events -n demo \
  --field-selector involvedObject.name=qwen2-5-7b,reason=RuntimeNotFound \
  -o 'custom-columns=TYPE:.type,REASON:.reason,FROM:.source.component,MESSAGE:.message'
```

```output
TYPE      REASON            FROM                 MESSAGE
Warning   RuntimeNotFound   v1beta1Controllers   no runtime found to support model safetensors with format safetensors in namespace demo. Checked 2 runtimes (0 namespace-scoped, 2 cluster-scoped). Excluded runtimes: srt-mistral-7b-instruct (model format 'mt:safetensors:1.0.0:Qwen2ForCausalLM:transformers:4.40.1' not in supported formats: architecture mismatch (model=Qwen2ForCausalLM, runtime=MistralForCausalLM), framework version mismatch (model=4.40.1, runtime=4.36.0))
```

If neither command prints anything, the model may have no format: see [The message says the model format name is required](#the-message-says-the-model-format-name-is-required).

## Step 2: Read the exclusion reasons {#step-3-read-the-exclusion-reasons}

The message has this form:

```text
no runtime found to support model <format> with format <format> in namespace <namespace>. Checked <total> runtimes (<namespaced> namespace-scoped, <cluster> cluster-scoped). Excluded runtimes: <runtime> (<reason>); <runtime> (<reason>)
```

The message names the model by its format, such as `safetensors`. It counts every runtime that OME checked, then lists each one it excluded, with the first check that the runtime failed. A runtime that's counted but not listed passed every check, yet OME skipped it. In the example, that's `srt-qwen2-5-7b`: see [Step 3](#step-3-check-autoselect). When there are no runtimes at all, the message ends after the namespace: install a runtime for the model.

OME runs these checks in order:

| Reason | Fix |
| --- | --- |
| `runtime is disabled` | Set `disabled: false` on the runtime, or use another. |
| `runtime does not support the required accelerator class` | Use a runtime that supports the InferenceService's accelerator class. See [Runtime accelerator-class matching](../../reference/matching/runtime-accelerator-class-matching.md). |
| `runtime engine deployment mode <runtime mode> does not match requested engine deployment mode <mode>` | The runtime and the InferenceService set different deployment modes. See [Runtime deployment-mode matching](../../reference/matching/runtime-deployment-mode-matching.md). |
| `model format '<label>' not in supported formats: <entries>` | No entry matches the model. Use a runtime made for it, such as one from [the runtime catalog](../../concepts/runtimes/serving-runtimes.md#the-runtime-catalog), or change the entry. |
| `model size <size> is outside supported range <range>` | Use a runtime whose `modelSizeRange` covers the model's `modelParameterSize`. On v1.2.2, set both bounds of the range. |

In the format reason, `<label>` holds the values OME recorded for the model. `<entries>` has one part for each of the runtime's `supportedModelFormats` entries, separated by `; `. Each part names every way that the entry differs from the model. A value that only one side sets counts as a mismatch, so an entry needs `modelArchitecture` and `quantization` exactly when the model has them; see [Supported model formats](../../concepts/runtimes/serving-runtimes.md#supported-model-formats). For the version and pipeline reasons, see [Model version matching](../../reference/matching/model-version-matching.md#mismatch-reasons) and [Diffusion pipeline runtime matching](../../reference/matching/diffusion-pipeline-runtime-matching.md#exclusion-reasons).

To see which value in the label is which, show the model's fields. For a BaseModel, use `basemodel` and add `-n demo`:

```bash
kubectl get clusterbasemodel qwen2-5-7b \
  -o 'custom-columns=FORMAT:.spec.modelFormat.name,FORMAT-VERSION:.spec.modelFormat.version,ARCHITECTURE:.spec.modelArchitecture,QUANTIZATION:.spec.quantization,FRAMEWORK:.spec.modelFramework.name,FRAMEWORK-VERSION:.spec.modelFramework.version'
```

```output
FORMAT        FORMAT-VERSION   ARCHITECTURE       QUANTIZATION   FRAMEWORK      FRAMEWORK-VERSION
safetensors   1.0.0            Qwen2ForCausalLM   <none>         transformers   4.40.1
```

OME reads these values from the model's files unless you set them, so the fix is usually in the runtime. In the example, `srt-mistral-7b-instruct` is made for `MistralForCausalLM` models and transformers 4.36.0.

If the runtime you expected is excluded, fix what its reason shows, and go to [Step 5](#step-5-verify-recovery).

## Step 3: Check autoSelect

OME auto-selects a runtime only when one of its entries sets `autoSelect: true`. `autoSelect` is unset by default, and unset doesn't count.

Since v1.3, [`kubectl ome runtime explain`](../../reference/kubectl-ome/runtime.md#explain) gives the reason for each runtime, including the ones the message leaves out:

```bash
kubectl ome runtime explain --model qwen2-5-7b -n demo
```

```output
RUNTIME              SCOPE     COMPATIBLE   PRIORITY   WEIGHT   REASON
srt-mistral-7b-      Cluster   No           -          -        model format
instruct                                                        'mt:safetensors:
                                                                1.0.0:
                                                                Qwen2ForCausalLM
                                                                :transformers:4.
                                                                40.1' not in
                                                                supported
                                                                formats:
                                                                architecture
                                                                mismatch
                                                                (model=Qwen2ForC
                                                                ausalLM,
                                                                runtime=MistralF
                                                                orCausalLM),
                                                                framework
                                                                version mismatch
                                                                (model=4.40.1,
                                                                runtime=4.36.0)
srt-qwen2-5-7b       Cluster   No           -          -        supports the
                                                                model but has no
                                                                supportedModelFo
                                                                rmats[].
                                                                autoSelect=true
                                                                entry, so
                                                                automatic
                                                                selection skips
                                                                it (pin it
                                                                explicitly via
                                                                spec.runtime.
                                                                name instead)
```

With an existing InferenceService, `--isvc qwen2-5-7b` also checks its accelerator class and deployment mode.

To find the entry to change, list the runtime's entries. For a ServingRuntime, use `servingruntime` and add `-n demo`:

```bash
kubectl get clusterservingruntime srt-qwen2-5-7b \
  -o jsonpath='{.spec.supportedModelFormats}' | jq
```

```output
[
  {
    "autoSelect": false,
    "modelArchitecture": "Qwen2ForCausalLM",
    "modelFormat": {
      "name": "safetensors",
      "operator": "Equal",
      "version": "1.0.0",
      "weight": 1
    },
    "modelFramework": {
      "name": "transformers",
      "operator": "Equal",
      "version": "4.40.1",
      "weight": 1
    },
    "priority": 1
  }
]
```

## Step 4: Fix the runtime or name one

To let OME auto-select the runtime, set `autoSelect: true` on the entry that matches the model. Here that's the first entry, index `0`:

```bash
kubectl patch clusterservingruntime srt-qwen2-5-7b --type json \
  -p '[{"op":"add","path":"/spec/supportedModelFormats/0/autoSelect","value":true}]'
```

```output
clusterservingruntime.ome.io/srt-qwen2-5-7b patched
```

!!! warning "Runtimes that inherit"
    Since v1.3, name a runtime that inherits from another instead of setting `autoSelect: true` on it. An auto-selected runtime runs without anything it inherits. See [How OME selects a runtime](../../concepts/runtimes/serving-runtimes.md#how-ome-selects-a-runtime).

Because it's a ClusterServingRuntime, OME can now pick it for matching InferenceServices in every namespace. If you apply the runtime from a manifest, such as the catalog file, set `autoSelect: true` there too, or the next apply sets it back to `false`.

To leave auto-selection as it is, [reference the runtime explicitly](reference-a-runtime-explicitly.md) instead. OME uses a runtime you name even when it doesn't declare the model, and [only warns](reference-a-runtime-explicitly.md#what-ome-checks-when-you-apply), so name one made for the model. If the webhook rejected the InferenceService, add `spec.runtime.name` to its manifest and create it again. Otherwise, patch it:

```bash
kubectl patch inferenceservice qwen2-5-7b -n demo --type merge \
  -p '{"spec":{"runtime":{"name":"srt-qwen2-5-7b"}}}'
```

```output
inferenceservice.ome.io/qwen2-5-7b patched
```

## Step 5: Verify recovery

If the webhook rejected a new InferenceService, apply it again. If you fixed the runtime, kubectl prints the runtime that OME will auto-select:

```bash
kubectl apply -f isvc.yaml
```

```output
Warning: Runtime srt-qwen2-5-7b will be auto-selected for model qwen2-5-7b
inferenceservice.ome.io/qwen2-5-7b created
```

Then skip to the last command.

Since v1.3, OME tries again for an existing InferenceService when you create or change a ServingRuntime in its namespace, or any ClusterServingRuntime. It also tries again when you change the InferenceService, as naming a runtime does. OME doesn't watch models: after you fix a model, change the InferenceService's spec, labels or annotations. On v1.2.2, OME keeps retrying a failed selection on its own, with a growing delay.

Read the condition again. On v1.2.2, skip to the last command.

```bash
kubectl get inferenceservice qwen2-5-7b -n demo \
  -o jsonpath='{.status.conditions[?(@.type=="RuntimeReady")]}' | jq
```

```output
{
  "lastTransitionTime": "2026-09-28T08:21:37Z",
  "reason": "RuntimeResolved",
  "severity": "Info",
  "status": "True",
  "type": "RuntimeReady"
}
```

If the status stays `False`, read the new message and go back to [Step 2](#step-3-read-the-exclusion-reasons). Otherwise, wait for the InferenceService to become `Ready`:

```bash
kubectl wait --for=condition=Ready inferenceservice/qwen2-5-7b -n demo --timeout=30m
```

```output
inferenceservice.ome.io/qwen2-5-7b condition met
```

## Troubleshooting

### The message says the model format name is required

```text
invalid model specification: modelFormat.name: model format name is required
```

OME needs the model's format to match a runtime, and this model has none yet. The webhook rejects the InferenceService with this message. If the webhook admitted it, OME records a `RuntimeSelectionError` Warning event whose message ends with this text instead, sets no condition, and retries on its own.

Wait for the model to become `Ready`. `vendor://` models, and models whose files OME can't read, become `Ready` without a format, so set `modelFormat` and `modelFramework` on them yourself. See [Set the metadata yourself](../../concepts/models/base-models.md#set-the-metadata-yourself).

### The message mentions a model cache provider {since=v1.3}

A message with `no model cache provider is configured for sharded model loading` means that the model uses `Sharded` distribution. OME can't serve such models yet, even with a runtime you name. Use a model without `distribution: Sharded`. See [Distribution](../../concepts/models/base-models.md#distribution).

### The runtime you named isn't found

Since v1.3, the reason `RuntimeNotFound` also appears when the InferenceService names a runtime that doesn't exist. The message is `runtime srt-qwen2-5-7b not found in namespace demo or at cluster scope`, or `ServingRuntime srt-qwen2-5-7b not found in namespace demo` when `spec.runtime.kind` is `ServingRuntime`. On v1.2.2, it comes in a `RuntimeValidationError` event.

Create the runtime, or fix `spec.runtime.name`. If the runtime went missing while the InferenceService used it, see [The runtime was deleted or disabled](reference-a-runtime-explicitly.md#if-the-runtime-goes-missing).

## Next steps

- [Reference a runtime explicitly](reference-a-runtime-explicitly.md): name a runtime and skip auto-selection.
- [Runtime selection scoring](../../reference/matching/runtime-selection-scoring.md): how OME ranks the runtimes that match a model.
- [`kubectl ome runtime`](../../reference/kubectl-ome/runtime.md): the flags and output of `explain`.
- [Model version matching](../../reference/matching/model-version-matching.md): how an entry's `version` and `operator` match the model's versions.
