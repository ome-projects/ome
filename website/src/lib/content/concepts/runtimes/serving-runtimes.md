---
title: Serving runtimes
description: "A ServingRuntime or ClusterServingRuntime describes how to run a model server: its pods, and optionally the models it supports, so OME can choose it for an InferenceService."
---

A serving runtime describes how to run a model server: its container, the pods around it, and optionally the models it supports. It can target one model, or any model its engine loads, with the differences set in each InferenceService or, since v1.3, in [child runtimes](runtime-inheritance.md). An [InferenceService](../serving/inference-services.md) names its runtime, or lets OME choose one that supports its [base model](../models/base-models.md). Since v1.3, an InferenceService that names a runtime needs no base model. OME builds the InferenceService's pods from the runtime, with the InferenceService's own settings on top.

## ServingRuntime and ClusterServingRuntime

Runtimes come in two kinds, which differ only in scope: a ClusterServingRuntime serves InferenceServices in every namespace, and a ServingRuntime only those in its own.

An InferenceService names its runtime in `spec.runtime.name`, and `spec.runtime.kind` says where OME looks for it:

| `spec.runtime.kind` | Where OME looks |
| --- | --- |
| `ClusterServingRuntime`, the default | For a ClusterServingRuntime with that name, then for a ServingRuntime in the InferenceService's namespace |
| `ServingRuntime` | Only for a ServingRuntime in the InferenceService's namespace |

## What's in a runtime {#anatomy-of-a-runtime}

A runtime lists the models that OME can choose it for in `supportedModelFormats`, and describes each component's pods in `engineConfig`, `decoderConfig` and `routerConfig`. The [ServingRuntimeSpec](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-ServingRuntimeSpec) reference lists every field.

The examples use the `llama-3-2-3b-instruct` ClusterBaseModel from [`config/models/meta/Llama-3.2-3B-Instruct.yaml`](https://github.com/ome-projects/ome/blob/main/config/models/meta/Llama-3.2-3B-Instruct.yaml). To download it, turn on the [model agent](../../guides/operate-ome/model-agent.md) with the chart values `modelAgent.enabled=true` and `modelAgent.hostPath=/raid/models`. It's gated, so the agent also needs a Hugging Face token in the Secret `hf-token` in the `ome` namespace; see [Credentials](../models/base-models.md#credentials). Apply the model, wait until it's `Ready`, and create a namespace for the examples:

```bash
kubectl create namespace llama-demo
```

```output
namespace/llama-demo created
```

This ServingRuntime serves Llama 3.2 3B models with SGLang in that namespace:

```yaml title="runtime.yaml"
apiVersion: ome.io/v1beta1
kind: ServingRuntime
metadata:
  name: sglang-llama-3-2-3b
  namespace: llama-demo
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
        version: "1.0.0"
      modelFramework:
        name: transformers
        version: "4.45.0.dev0"
      modelArchitecture: LlamaForCausalLM
      autoSelect: true
      priority: 1
  modelSizeRange:
    min: 2B
    max: 4B
  engineConfig:
    tolerations:
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
    volumes:
      - name: dshm
        emptyDir:
          medium: Memory
    runner:
      name: ome-container
      image: docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64
      command:
        - python3
        - -m
        - sglang.launch_server
      args:
        - --host=0.0.0.0
        - --port=8080
        - --model-path=$(MODEL_PATH)
        - "--served-model-name={{.Name}}"
        - --tp-size=1
        - --mem-frac=0.9
      ports:
        - containerPort: 8080
          name: http1
          protocol: TCP
      volumeMounts:
        - name: dshm
          mountPath: /dev/shm
      resources:
        requests:
          cpu: "10"
          memory: 30Gi
          nvidia.com/gpu: "1"
        limits:
          cpu: "10"
          memory: 30Gi
          nvidia.com/gpu: "1"
      readinessProbe:
        httpGet:
          path: /health_generate
          port: 8080
        periodSeconds: 60
        timeoutSeconds: 200
      startupProbe:
        httpGet:
          path: /health_generate
          port: 8080
        initialDelaySeconds: 60
        periodSeconds: 6
        timeoutSeconds: 30
        failureThreshold: 150
```

Apply the file:

```bash
kubectl apply -f runtime.yaml
```

```output
servingruntime.ome.io/sglang-llama-3-2-3b created
```

List the runtimes in the namespace with the formats and sizes they support:

```bash
kubectl get servingruntimes -n llama-demo \
  -o 'custom-columns=NAME:.metadata.name,FORMAT:.spec.supportedModelFormats[*].modelFormat.name,ARCHITECTURE:.spec.supportedModelFormats[*].modelArchitecture,MIN:.spec.modelSizeRange.min,MAX:.spec.modelSizeRange.max'
```

```output
NAME                  FORMAT        ARCHITECTURE       MIN   MAX
sglang-llama-3-2-3b   safetensors   LlamaForCausalLM   2B    4B
```

### Supported model formats

Each entry in `supportedModelFormats` describes one kind of model that the runtime supports. Only `modelFormat` and `modelFramework` are required, each with a `name`.

| Field | How OME uses it |
| --- | --- |
| `modelFormat` | The format of the weights, such as `safetensors`, and its `version`. |
| `modelFramework` | The framework, such as `transformers`, and its `version`. |
| `modelArchitecture` | The architecture, such as `LlamaForCausalLM`. |
| `quantization` | The quantization, such as `fp8`. |
| `diffusionPipeline` | The pipeline of a diffusion model; see [Diffusion pipeline runtime matching](../../reference/matching/diffusion-pipeline-runtime-matching.md). |
| `autoSelect` | `true` lets OME choose the runtime for InferenceServices that don't name one. |
| `priority` | Multiplies the entry's score when OME chooses a runtime. At least 1; unset counts as 1. |
| `acceleratorConfig` | Settings for each accelerator class; see [Per-accelerator configuration](#per-accelerator-configuration). |
| `modelCacheProviders` | Since v1.3. Caches to load `Sharded` models from. None can be configured yet, so leave it out; see [Distribution](../models/base-models.md#distribution). |
| `name` | Used only by the webhook's priority check; see [Common errors](#what-the-admission-webhook-checks). |
| `version` | Not used. |

A model matches an entry when their format, framework, architecture and quantization agree, and a value that only one side sets doesn't match. The model agent records each model's architecture, and the quantization of quantized ones, so set `modelArchitecture` in every entry, and `quantization` only for quantized models.

`modelFormat` and `modelFramework` also take a `weight` for the score, 1 by default, and an `operator` for versions. With `GreaterThan` or `GreaterThanOrEqual`, the entry's version is an upper limit on the model's, not a lower one; see [Model version matching](../../reference/matching/model-version-matching.md).

### Runtime-wide fields

| Field | How OME uses it |
| --- | --- |
| `modelSizeRange` | The model sizes the runtime supports, from `min` to `max`, both optional and inclusive. Write `M`, `B` or `T` in upper case: OME reads `4b` as 0. On v1.2.2, set both. See [Model size range matching](../../reference/matching/model-size-range-matching.md). |
| `acceleratorRequirements.acceleratorClasses` | The [accelerator classes](accelerator-classes.md) the runtime runs on. OME picks a class for a component only when its runtime lists some. The other fields of `acceleratorRequirements` have no effect. |
| `disabled` | `true` stops new use: OME never auto-selects the runtime, and since v1.3 the webhook rejects InferenceServices that name it. Running pods keep serving. A disabled runtime can still be a [profile](runtime-inheritance.md) for others. |
| `nodeSelector` | Added to the engine's and the decoder's pods. It wins over `engineConfig.nodeSelector` and `decoderConfig.nodeSelector`, and loses to the accelerator class's node selector and to the InferenceService's. |
| `schedulerName` | The default scheduler for every component's pods. |
| `protocolVersions` | No effect. The catalog runtimes set it; you can leave it out. |
| `volumes`, `tolerations`, `affinity` and most other pod fields | Not used. Set them in `engineConfig`, `decoderConfig` or `routerConfig`. |

### The engine, decoder and router

`engineConfig`, `decoderConfig` and `routerConfig` take the fields of the InferenceService's `engine`, `decoder` and `router`, including a `runner`: the model server container, which OME names `ome-container` unless you name it. A multi-node component has a `leader` and a `worker`, each with a `runner`, and `worker.size` sets the number of worker pods for each leader; see [Deployment modes and OMENative](../architecture/deployment-modes.md). On v1.2.2, runtimes also have a top-level `workers` field, which v1.3 drops; use `engineConfig.worker.size` instead.

OME uses a config only for the components that the InferenceService declares: an InferenceService with only an `engine` gets no decoder or router.

Since v1.3, the annotation `ome.io/engine: sglang-pd` on a runtime fills in an SGLang prefill-decode engine, decoder and router wherever the runtime leaves fields unset. Its images use the `latest` tag, so set each `image` yourself; see [Labels and annotations](../../reference/api/labels-and-annotations.md#runtime-annotations).

Where the runtime and the InferenceService both set a value, the InferenceService's wins, except for the [per-accelerator overrides](#per-accelerator-configuration):

- Maps merge by key: `labels`, `annotations`, `nodeSelector`, and the runner's resource requests and limits.
- Lists merge item by item: `env` and `volumes` by name, `ports` by `containerPort`, and `volumeMounts` by `mountPath`.
- `args`, `command` and `tolerations` replace the runtime's whole list. To add a flag with `args`, repeat the runtime's other flags. Most runtimes in the catalog put their flags in `command`, so an InferenceService's `args` adds flags to theirs.
- Single values, such as the runner's `image`, replace the runtime's.

Attach an [autoscaler policy](../serving/autoscaler-policy.md) and set `acceleratorOverride` in the InferenceService: since v1.3 the webhook rejects `autoscalerPolicyRef` in a runtime, and `acceleratorOverride` has no effect there.

OME sets two environment variables in the engine's and the decoder's runner:

- `MODEL_PATH`, the path of the model's weights, when the InferenceService names a base model and the runner doesn't set it. Without a base model, a runner that passes `$(MODEL_PATH)`, as the catalog runtimes do, gets that text literally. Set `MODEL_PATH` in `env`, or load the weights another way; see [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md#step-2-name-the-runtime-in-the-inferenceservice).
- `PARALLELISM_SIZE`, the GPUs in each pod times the pods in each replica, which is one plus `worker.size`. It replaces any value you set. OME leaves it unset for a runner without GPUs, or when `acceleratorConfig` sets `tensorParallelismOverride` for the chosen class.

### Placeholders

Values in each component's runner, leaders and workers included, can hold placeholders that OME fills in from the InferenceService's metadata:

| Placeholder | Value |
| --- | --- |
| `{{.Name}}` | The InferenceService's name, such as `llama-3-2-3b-instruct` |
| `{{.Namespace}}` | Its namespace, such as `llama-demo` |
| `{{.Labels.team}}` | The value of its `team` label |
| `{{.Annotations.owner}}` | The value of its `owner` annotation |

For a key with a dash, a dot or a slash, use `index` with the key in backquotes: ``{{index .Labels `app.kubernetes.io/name`}}``. A missing label or annotation gives `<no value>` in the field form, and an empty value with `index`.

### Scaling policy {since=v1.3}

`scalingPolicy` is an alpha field for how the engine, decoder and router scale together. No controller acts on it yet, and the webhook rejects any mode but `Independent`, the default, with `ScalingModeNotImplemented`. An InferenceService's `spec.scalingPolicy` replaces the runtime's.

## How OME selects a runtime

When an InferenceService names a base model but no runtime, OME chooses one from the ServingRuntimes in its namespace and all ClusterServingRuntimes. An InferenceService without `spec.model` must name its runtime. OME considers only the runtimes that:

- Support the model: an entry [matches](#supported-model-formats) it, and the model's size, if known, is in `modelSizeRange`.
- Have an entry with `autoSelect: true`.
- Aren't disabled.
- List every accelerator class that the InferenceService names; see [Runtime accelerator-class matching](../../reference/matching/runtime-accelerator-class-matching.md).
- Don't declare a deployment mode that differs from the InferenceService's; see [Runtime deployment-mode matching](../../reference/matching/runtime-deployment-mode-matching.md).

OME takes a ServingRuntime over a ClusterServingRuntime, and then the highest score: the runtime's best entry's format and framework `weight`s added together, times its `priority`. Ties go to the size range closest to the model's size, where no range counts as closest, and then to the first name alphabetically. [Runtime selection scoring](../../reference/matching/runtime-selection-scoring.md) has the full rules.

OME renders a runtime that it chose from that runtime's own spec, without what it [inherits](runtime-inheritance.md), so name an inheriting runtime instead of setting `autoSelect: true` on it.

!!! tip "Keep the choice fixed"
    Name the runtime in `spec.runtime.name` when the choice matters. Otherwise a new or changed runtime can later replace the one OME chose, for example when the InferenceService or its Deployment changes.

This InferenceService leaves out `spec.runtime`:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-3b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-3b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
```

Apply the file. The admission webhook tells you which runtime OME will choose:

```bash
kubectl apply -f isvc.yaml
```

```output
Warning: Runtime sglang-llama-3-2-3b will be auto-selected for model llama-3-2-3b-instruct
inferenceservice.ome.io/llama-3-2-3b-instruct created
```

The `serving-runtime` label on the InferenceService's Deployments shows the runtime OME chose:

```bash
kubectl get deployments -n llama-demo -l ome.io/inferenceservice=llama-3-2-3b-instruct \
  -o 'custom-columns=NAME:.metadata.name,RUNTIME:.metadata.labels.serving-runtime'
```

```output
NAME                           RUNTIME
llama-3-2-3b-instruct-engine   sglang-llama-3-2-3b
```

Since v1.3, `kubectl ome runtime explain --isvc llama-3-2-3b-instruct -n llama-demo` shows which runtimes are compatible with the model, and why; see [kubectl ome runtime](../../reference/kubectl-ome/runtime.md).

### When you name a runtime

OME uses the runtime that `spec.runtime.name` names even when it doesn't declare support for the model or list the InferenceService's accelerator class, so one runtime can serve many models. Since v1.3, the webhook admits such an InferenceService with a warning that begins `runtime "<runtime>" does not declare support for model "<model>"`. Unless the InferenceService is [pinned](runtime-revisions.md), OME also records a `RuntimeCompatibilityAdvisory` event.

The webhook still rejects an InferenceService whose runtime is disabled, or whose base model doesn't exist or is disabled. It checks that the runtime exists only when the InferenceService has a base model; without one, the InferenceService reports [`RuntimeNotFound`](#what-the-admission-webhook-checks) instead. [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md#troubleshooting) covers the webhook's messages, and [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md#the-webhook-rejects-the-inferenceservice) covers the rest.

Without `spec.model`, the runner loads the weights itself: OME skips all model work, including model volumes and `MODEL_PATH`; see [The runtime](../serving/inference-services.md#the-runtime).

!!! warning "Upgrading from v1.2.2"
    v1.2.2 ignores `kind` and looks in the InferenceService's namespace first. On v1.3, an InferenceService that names a ServingRuntime without `kind: ServingRuntime` switches to a ClusterServingRuntime of the same name, if one exists, and OME re-renders its pods unless it's [pinned](runtime-revisions.md). Before you upgrade, set `kind: ServingRuntime`, which v1.2.2 accepts.

### When a runtime changes

When you change a runtime, OME rolls the change out right away to the InferenceServices that name it, or name a runtime that inherits from it, unless they're [pinned](runtime-revisions.md). In RawDeployment that's a plain Deployment update, and in OMENative the pods roll out by the InferenceService's [update strategy](../architecture/omenative-update-strategies.md).

An InferenceService whose runtime OME chose picks up the change later, when OME chooses its runtime again. On v1.2.2, a runtime change reaches an InferenceService only when something else changes it.

## Per-accelerator configuration

An entry's `acceleratorConfig` changes how the runtime runs on each [accelerator class](accelerator-classes.md). The example also needs the `llama-3-3-70b-instruct` ClusterBaseModel from [`config/models/meta/Llama-3.3-70B-instruct.yaml`](https://github.com/ome-projects/ome/blob/main/config/models/meta/Llama-3.3-70B-instruct.yaml). Create the `nvidia-h100` and `nvidia-l40s` classes from [Accelerator classes](accelerator-classes.md) before the runtime.

This ClusterServingRuntime runs Llama 3.3 70B with `--tp-size=8` by default, and with `--tp-size=4` and `--pp-size=2` on the `nvidia-l40s` class:

```yaml title="runtime-70b.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: sglang-llama-3-3-70b
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
        version: "1.0.0"
      modelFramework:
        name: transformers
        version: "4.47.0.dev0"
      modelArchitecture: LlamaForCausalLM
      autoSelect: false
      priority: 1
      acceleratorConfig:
        nvidia-l40s:
          tensorParallelismOverride:
            tensorParallelSize: 4
            pipelineParallelSize: 2
          runtimeArgsOverride:
            - --mem-frac=0.85
  modelSizeRange:
    min: 60B
    max: 75B
  acceleratorRequirements:
    acceleratorClasses:
      - nvidia-h100
      - nvidia-l40s
  engineConfig:
    tolerations:
      - key: nvidia.com/gpu
        operator: Exists
        effect: NoSchedule
    volumes:
      - name: dshm
        emptyDir:
          medium: Memory
    runner:
      name: ome-container
      image: docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64
      command:
        - python3
        - -m
        - sglang.launch_server
      args:
        - --host=0.0.0.0
        - --port=8080
        - --model-path=$(MODEL_PATH)
        - --tp-size=8
        - --pp-size=1
        - --mem-frac=0.9
      ports:
        - containerPort: 8080
          name: http1
          protocol: TCP
      volumeMounts:
        - name: dshm
          mountPath: /dev/shm
      resources:
        requests:
          cpu: "16"
          memory: 320Gi
          nvidia.com/gpu: "8"
        limits:
          cpu: "16"
          memory: 320Gi
          nvidia.com/gpu: "8"
      readinessProbe:
        httpGet:
          path: /health_generate
          port: 8080
        periodSeconds: 60
        timeoutSeconds: 200
      startupProbe:
        httpGet:
          path: /health_generate
          port: 8080
        initialDelaySeconds: 60
        periodSeconds: 6
        timeoutSeconds: 30
        failureThreshold: 150
```

Apply the file:

```bash
kubectl apply -f runtime-70b.yaml
```

```output
clusterservingruntime.ome.io/sglang-llama-3-3-70b created
```

This InferenceService runs the model on the `nvidia-l40s` class:

```yaml title="isvc-70b.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-3-70b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-3-70b-instruct
  runtime:
    name: sglang-llama-3-3-70b
  acceleratorSelector:
    acceleratorClass: nvidia-l40s
  engine:
    minReplicas: 1
    maxReplicas: 1
```

Apply the file:

```bash
kubectl apply -f isvc-70b.yaml
```

```output
inferenceservice.ome.io/llama-3-3-70b-instruct created
```

Print the flags of the runner in the engine's Deployment:

```bash
kubectl get deployment llama-3-3-70b-instruct-engine -n llama-demo \
  -o jsonpath='{range .spec.template.spec.containers[?(@.name=="ome-container")].args[*]}{@}{"\n"}{end}'
```

```output
--host=0.0.0.0
--port=8080
--model-path=$(MODEL_PATH)
--tp-size=4
--pp-size=2
--mem-frac=0.85
```

The settings for a class can hold these fields:

| Field | What it does on that class |
| --- | --- |
| `tensorParallelismOverride` | Rewrites the value of a tensor or pipeline parallelism flag that the runner already has, in `args`, or else in `command`: `--tp-size`, `--tp` or `--tensor-parallel-size`, and `--pp-size`, `--pp` or `--pipeline-parallel-size`. It never adds a flag. |
| `runtimeArgsOverride` | Sets flags in `args`: it replaces the value of a flag that `args` has, and appends the others. A flag that's only in `command` stays there, with the override after it. |
| `environmentOverride` | Sets environment variables in the runner, over the InferenceService's values. |
| `dataParallelSize`, `minMemoryPerBillionParams` | No effect. |

OME applies these settings after it merges the runtime with the InferenceService, once it [picks a class](../../guides/deploy-models/select-accelerators.md#how-ome-picks-a-class) for the engine or the decoder, leaders and workers included. The router gets none. OME takes them from the entry that best matches the model's format and framework, counting only entries with `autoSelect: true` when OME chose the runtime.

On a class that `acceleratorConfig` leaves out, like `nvidia-h100` here, the runner keeps its own flags. Spell each key exactly as the class's name: OME ignores a misspelled key without a warning. The [AcceleratorModelConfig](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-AcceleratorModelConfig) reference lists every field.

## The runtime catalog

The `config/runtimes/` directory of the OME repository holds ready-made ClusterServingRuntimes for many models:

| Directory | Runtimes |
| --- | --- |
| `srt/` | SGLang, mostly in a subdirectory per model vendor, such as `srt/meta/` |
| `vllm/` | vLLM |
| `tokenspeed/` | TokenSpeed |

Each catalog runtime targets one model and is named after its engine and model. Since v1.3, you can instead write one runtime per engine; see [Runtime inheritance](runtime-inheritance.md). Runtimes with `-pd` in their names have an engine, a decoder and a router, and those with `-grpc` run SGLang with `--grpc-mode`.

Apply one runtime from a checkout of the repository:

```bash
kubectl apply -f config/runtimes/srt/meta/llama-3-2-3b-instruct-rt.yaml
```

```output
clusterservingruntime.ome.io/srt-llama-3-2-3b-instruct created
```

`kubectl apply -k config/runtimes` applies the subset in [`kustomization.yaml`](https://github.com/ome-projects/ome/blob/main/config/runtimes/kustomization.yaml). The webhook rejects the runtimes whose accelerator classes you haven't created, and kubectl applies the rest.

Most SGLang runtimes in the catalog set `autoSelect: false`, so OME uses them only for InferenceServices that name them, while most vLLM runtimes set `autoSelect: true`.

The `ome-serving` chart creates SGLang runtimes from its own catalog, mostly with these names. Create each runtime one way only, because Helm stops when one of the chart's runtimes already exists; see [Helm refuses to install over existing resources](../../getting-started/pre-configured-models.md#helm-refuses-to-install-over-existing-resources).

## Common errors {#what-the-admission-webhook-checks}

| What you see | Cause | Fix |
| --- | --- | --- |
| Applying a runtime fails with `unknown accelerator classes referenced in AcceleratorRequirements` | A class it lists doesn't exist. | Create the class first. |
| Applying a runtime fails with `different priorities assigned for the model format` | Two entries with `autoSelect: true` and the same `name`, or no name, set different priorities. | Give them one `priority`. |
| Applying a runtime fails with an inheritance error | Its `ome.io/inherit-from` chain is broken, or it's a profile without `disabled: true`. | See [What admission checks](runtime-inheritance.md#what-admission-checks). |
| Applying an InferenceService fails with `no supporting runtime found for model` | It leaves out `spec.runtime`, and no runtime supports its model. | See [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md). |
| Since v1.3, `RuntimeReady` is `False` with the reason `RuntimeNotFound` | The runtime it names is missing, or no runtime supports its model any more. | Create or fix the runtime. A running workload keeps serving meanwhile, and `RuntimeReady` doesn't count toward `Ready`. |
| A `RuntimeValidationError` event says the runtime is disabled | The runtime it names was disabled, and it isn't [pinned](runtime-revisions.md). | Enable the runtime, or name another. The pods keep serving, but OME stops updating them. |
| An `AcceleratorClassError` event, such as `Failed to get accelerator class for engine` | The class that OME picked doesn't exist. | Create the class; see [Select accelerators](../../guides/deploy-models/select-accelerators.md#the-inferenceservice-has-an-acceleratorclasserror-event). |
| The controller log shows `Failed to reconcile component`, and the workload doesn't change | A malformed [placeholder](#placeholders), often a key with a dash in the field form. | Use the `index` form with the key in backquotes. |

The webhook also checks `scalingPolicy`, `autoscalerPolicyRef` and each component's `autoscaler`. For a disabled runtime, it checks only the inheritance, `scalingPolicy` and `autoscalerPolicyRef`.

## Next steps

- [Runtime inheritance](runtime-inheritance.md): write one runtime per engine, build runtimes from shared profiles, and read a runtime's inheritance status.
- [Runtime revisions and pinning](runtime-revisions.md): pin an InferenceService to an immutable snapshot of its runtime.
- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md): name a runtime, and check which one an InferenceService uses.
- [Accelerator classes](accelerator-classes.md): describe the GPUs that runtimes run on.
- [Select accelerators](../../guides/deploy-models/select-accelerators.md): pick the accelerator class for each component.
- [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md): find out why OME can't choose a runtime for your model.
