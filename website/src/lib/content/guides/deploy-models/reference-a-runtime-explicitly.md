---
title: Reference a runtime explicitly
description: Name the ServingRuntime or ClusterServingRuntime that an InferenceService uses instead of letting OME choose one, then check that OME used it.
---

When an [InferenceService](../../concepts/serving/inference-services.md) names a runtime in `spec.runtime.name`, OME serves it with that runtime instead of choosing one. Name a runtime when:

- OME wouldn't choose it on its own, for example because it sets `autoSelect: false`.
- Several runtimes match the model, and you want a particular one.
- You want the choice to stay fixed, because a new or changed runtime can replace one that OME chose.
- Since v1.3, the runtime [inherits](../../concepts/runtimes/runtime-inheritance.md) from a parent runtime. OME uses the inherited settings only for InferenceServices that name the runtime.
- Since v1.3, the runtime loads the weights itself, so the InferenceService needs no model. See [Step 2](#step-2-name-the-runtime-in-the-inferenceservice).

OME uses the runtime you name even when the runtime doesn't declare support for the model, and [warns you](#what-ome-checks-when-you-apply).

The steps below serve the ClusterBaseModel `llama-3-2-1b-instruct` with the [ClusterServingRuntime](../../concepts/runtimes/serving-runtimes.md) `srt-llama-3-2-1b-instruct`, then check which runtime OME used.

<div class="prerequisites" markdown>

- OME installed with the model agent turned on, as [Install OME](../../getting-started/install.md#step-3-install-ome) shows, and `kubectl` access to the cluster.
- A namespace `llama-demo`. To create it, run `kubectl create namespace llama-demo`.
- The ClusterBaseModel `llama-3-2-1b-instruct` from the [pre-configured models](../../getting-started/pre-configured-models.md), with `Ready` in the `READY` column of `kubectl get clusterbasemodel llama-3-2-1b-instruct`. It's gated on Hugging Face, so the model agent needs your token: see [Credentials](../../concepts/models/base-models.md#credentials).
- The ClusterServingRuntime `srt-llama-3-2-1b-instruct`, which `kubectl get clusterservingruntime srt-llama-3-2-1b-instruct` lists. If it's missing, apply [`config/runtimes/srt/meta/llama-3-2-1b-instruct-rt.yaml`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-2-1b-instruct-rt.yaml) from a clone of the OME repository.
- A node with an NVIDIA GPU, 10 CPUs and 30 GiB of memory free, for the runtime's requests. The runtime's SGLang image is built for amd64.

</div>

## Step 1: Check that the runtime supports the model

OME uses the runtime without a warning when:

- An entry in the runtime's `supportedModelFormats` has the model's architecture, format, framework and quantization, at [matching versions](../../reference/matching/model-version-matching.md).
- The model's `spec.modelParameterSize`, when OME has recorded it, is inside the runtime's `modelSizeRange`.
- The runtime accepts the InferenceService's [accelerator class](../../reference/matching/runtime-accelerator-class-matching.md) and [deployment mode](../../reference/matching/runtime-deployment-mode-matching.md).

[Supported model formats](../../concepts/runtimes/serving-runtimes.md#supported-model-formats) has the full rules.

`srt-llama-3-2-1b-instruct` sets `autoSelect: false`, so OME never picks it on its own, and you have to name it. Since v1.3, [`kubectl ome runtime explain`](../../reference/kubectl-ome/runtime.md#explain) lists the runtimes that OME considers for a model, whether it can choose each one, and why.

Compare what OME recorded for the model with what the runtime declares. The commands leave out the format version, which is `1.0.0` on both sides:

```bash
kubectl get clusterbasemodel llama-3-2-1b-instruct \
  -o custom-columns=ARCHITECTURE:.spec.modelArchitecture,FORMAT:.spec.modelFormat.name,FRAMEWORK:.spec.modelFramework.name,VERSION:.spec.modelFramework.version
```

```output
ARCHITECTURE       FORMAT        FRAMEWORK      VERSION
LlamaForCausalLM   safetensors   transformers   4.45.0.dev0
```

```bash
kubectl get clusterservingruntime srt-llama-3-2-1b-instruct \
  -o 'custom-columns=NAME:.metadata.name,DISABLED:.spec.disabled,ARCHITECTURE:.spec.supportedModelFormats[*].modelArchitecture,FORMAT:.spec.supportedModelFormats[*].modelFormat.name,FRAMEWORK:.spec.supportedModelFormats[*].modelFramework.name,VERSION:.spec.supportedModelFormats[*].modelFramework.version,MIN:.spec.modelSizeRange.min,MAX:.spec.modelSizeRange.max'
```

```output
NAME                        DISABLED   ARCHITECTURE       FORMAT        FRAMEWORK      VERSION       MIN    MAX
srt-llama-3-2-1b-instruct   false      LlamaForCausalLM   safetensors   transformers   4.45.0.dev0   500M   2B
```

The two match. To compare other runtimes, leave out the runtime's name, or use `servingruntimes -n llama-demo` for the ServingRuntimes in the namespace.

## Step 2: Name the runtime in the InferenceService

Set `spec.runtime.name` to the runtime's name:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  runtime:
    name: srt-llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
```

To serve a BaseModel in `llama-demo` instead, add `kind: BaseModel` under `spec.model`, and run Step 1's first command with `basemodel -n llama-demo`. To move an InferenceService that already exists to the runtime, add the same `runtime` field to its manifest.

`spec.runtime` also takes these fields:

| Field | Default | What it does |
| --- | --- | --- |
| `kind` | `ClusterServingRuntime` | Set `ServingRuntime` to name a ServingRuntime in the InferenceService's namespace. See [How OME resolves the name](#how-ome-resolves-the-name). |
| `autoSync` | `true` | `false` pins the InferenceService to a snapshot of the runtime, so later changes to the runtime don't roll out to it. See [Runtime revisions and pinning](../../concepts/runtimes/runtime-revisions.md). |
| `revision` | unset | The snapshot to pin to. Needs `autoSync: false`. |

Since v1.3, you can leave out `spec.model` when the runtime loads the weights itself, from its container or from a volume or init container under `spec.engine`. OME then skips all model work, and sets no `MODEL_PATH`. A runtime that passes `$(MODEL_PATH)` to the engine, as this one does, then needs the variable in its runner's `env`. See [The runtime](../../concepts/serving/inference-services.md#the-runtime).

Apply the InferenceService:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct created
```

!!! tip
    When you apply a runtime and the InferenceServices that name it together, apply the runtime first. Since v1.3, the webhook rejects an InferenceService like this one if its runtime doesn't exist yet.

Wait for the InferenceService to become ready:

```bash
kubectl wait --for=condition=Ready inferenceservice/llama-3-2-1b-instruct -n llama-demo --timeout=30m
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct condition met
```

## Step 3: Check the runtime OME used

The `RUNTIME` column of `kubectl get inferenceservice` shows the name you asked for. To see the runtime that OME built the serving pods from, read their `serving-runtime` label:

```bash
kubectl get pods -n llama-demo -l ome.io/inferenceservice=llama-3-2-1b-instruct \
  -o custom-columns=COMPONENT:.metadata.labels.component,RUNTIME:.metadata.labels.serving-runtime
```

```output
COMPONENT   RUNTIME
engine      srt-llama-3-2-1b-instruct
```

The label holds the runtime's name, not its kind. Since v1.3, `kubectl ome runtime effective llama-3-2-1b-instruct -n llama-demo` shows the kind too, as `CSR/srt-llama-3-2-1b-instruct`. It also shows the pin and drift, and with `-o json` or `-o yaml`, the runtime selection and inheritance. See [kubectl ome runtime](../../reference/kubectl-ome/runtime.md#effective).

Last, check for `RuntimeCompatibilityAdvisory` events, which OME records when the runtime doesn't declare support for the model. This runtime does, so there are none:

```bash
kubectl get events -n llama-demo --field-selector involvedObject.name=llama-3-2-1b-instruct,reason=RuntimeCompatibilityAdvisory
```

```output
No resources found in llama-demo namespace.
```

## How OME resolves the name

OME looks for the name in the InferenceService's namespace and at cluster scope. Since v1.3, `spec.runtime.kind` decides the order:

| `kind` | Where OME looks |
| --- | --- |
| `ClusterServingRuntime`, the default | The ClusterServingRuntime with the name, then a ServingRuntime with the name in the InferenceService's namespace |
| `ServingRuntime` | Only a ServingRuntime with the name in the InferenceService's namespace |

When a ClusterServingRuntime and a ServingRuntime in the namespace share a name, set `kind: ServingRuntime` to get the ServingRuntime. On v1.2.2, OME ignores `kind`: it looks for a ServingRuntime in the namespace first, then for a ClusterServingRuntime.

## Troubleshooting

Since v1.3, the admission webhook checks the runtime and the model when you create or update an InferenceService that has an `engine` section. On v1.2.2, or without an `engine` section, the same problems show up as events after `kubectl apply` succeeds.

### kubectl prints a `does not declare support` warning {#what-ome-checks-when-you-apply}

The runtime doesn't declare support for the model, or doesn't accept the InferenceService's accelerator class or deployment mode. The text in parentheses says what differs. For example, if the model's `config.json` had `"transformers_version": "4.46.0"`, OME would record the framework version `4.46.0`, while this runtime declares `4.45.0.dev0`:

```bash
kubectl apply -f isvc.yaml
```

```output
Warning: runtime "srt-llama-3-2-1b-instruct" does not declare support for model "llama-3-2-1b-instruct" (runtime srt-llama-3-2-1b-instruct does not support model safetensors: model format 'mt:safetensors:1.0.0:LlamaForCausalLM:transformers:4.46.0' not in supported formats: framework version mismatch (model=4.46.0, runtime=4.45.0.dev0)); proceeding because the runtime was named explicitly
inferenceservice.ome.io/llama-3-2-1b-instruct created
```

OME serves the model with the runtime anyway. If the runtime can't load the model, the serving pods don't become ready: name a runtime that declares the model instead. While the InferenceService follows the live runtime, OME also records a `RuntimeCompatibilityAdvisory` event with the same details, which the last command in [Step 3](#step-3-check-the-runtime-ome-used) lists.

### The webhook rejects the InferenceService

`kubectl` prints `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:`, then a message:

| The message includes | Cause and fix |
| --- | --- |
| `srt-llama-3-2-1b-instruct not found in namespace llama-demo` | No runtime has the name, or with `kind: ServingRuntime`, no ServingRuntime in `llama-demo` has it. Create the runtime first, or fix `spec.runtime`. |
| `is disabled` | The runtime or the model sets `disabled: true`. Set it to `false`, or name another. |
| `no model cache provider is configured for sharded model loading` | The model sets `distribution: Sharded`, which OME can't serve yet. See [Distribution](../../concepts/models/base-models.md#distribution). |

[The webhook rejects the InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md#the-webhook-rejects-the-inferenceservice) lists other messages.

### The runtime was deleted or disabled {#if-the-runtime-goes-missing}

Since v1.3, if you delete the runtime while the InferenceService follows it, OME records a `RuntimeNotFound` event and sets the `RuntimeReady` condition to `False`. It stops updating the serving workload, but running pods keep serving. `RuntimeReady` doesn't count toward `Ready`, so the InferenceService stays `Ready` while they serve:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-demo \
  -o 'custom-columns=STATUS:.status.conditions[?(@.type=="RuntimeReady")].status,REASON:.status.conditions[?(@.type=="RuntimeReady")].reason,MESSAGE:.status.conditions[?(@.type=="RuntimeReady")].message'
```

```output
STATUS   REASON            MESSAGE
False    RuntimeNotFound   runtime srt-llama-3-2-1b-instruct not found in namespace llama-demo or at cluster scope
```

To recover, recreate the runtime, or point `spec.runtime.name` at one that exists. OME then resumes updating the workload, and sets `RuntimeReady` to `True` with the reason `RuntimeResolved`. Until then, the webhook rejects every update to the InferenceService, even a label change, unless the update names a runtime that exists. You can still delete the InferenceService.

With `autoSync: false`, the InferenceService keeps serving from its pinned snapshot instead, and its `RuntimeDrifted` condition turns `True` with the reason `SourceRuntimeMissing`. See [The RuntimeDrifted condition](../../concepts/runtimes/runtime-revisions.md#the-runtimedrifted-condition).

On v1.2.2, a deleted runtime gives a `RuntimeValidationError` event, or `RuntimePinError` with `autoSync: false`.

If you disable a runtime that the InferenceService follows, OME records a `RuntimeValidationError` event that says the runtime is disabled, and stops updating the serving workload. Running pods keep serving. To recover, set `disabled: false`, or name another runtime.

## Clean up

Delete the InferenceService:

```bash
kubectl delete inferenceservice llama-3-2-1b-instruct -n llama-demo
```

```output
inferenceservice.ome.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
```

The model and the runtime stay, for other InferenceServices to use.

## Next steps

- [Troubleshoot runtime selection](troubleshoot-runtime-selection.md): find out why OME passes over a runtime when it selects one itself.
- [Serving runtimes](../../concepts/runtimes/serving-runtimes.md): what a runtime declares, and how OME builds pods from it.
- [Runtime revisions and pinning](../../concepts/runtimes/runtime-revisions.md): pin an InferenceService to a snapshot of its runtime.
- [Runtime inheritance](../../concepts/runtimes/runtime-inheritance.md): keep the settings that runtimes share in one parent runtime.
