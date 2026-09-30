---
title: Runtime deployment-mode matching
description: Auto-selection skips a runtime when it and your InferenceService declare different deployment modes for the same component.
---

When an [InferenceService](../../concepts/serving/inference-services.md) doesn't name a runtime, OME skips any [serving runtime](../../concepts/runtimes/serving-runtimes.md) that declares a different [deployment mode](../../concepts/architecture/deployment-modes.md) for the engine or decoder than the InferenceService asks for. Declare a mode on a runtime to keep it away from InferenceServices that ask for a different one. The [catalog runtimes](../../concepts/runtimes/serving-runtimes.md#the-runtime-catalog) declare no mode, so they always pass this check.

## The matching rule

OME compares the engine's modes, and the decoder's when the InferenceService has a `spec.decoder`. It doesn't compare the router. It rejects the runtime when both sides declare a mode for the same component and the modes differ. For the engine:

| InferenceService's engine mode | Runtime's `engineConfig` mode | Result |
|---|---|---|
| None | `MultiNode` (deprecated) | Passes |
| `OMENative` | None | Passes |
| `OMENative` | `OMENative` | Passes |
| `OMENative` | `MultiNode` | Rejected |
| `RawDeployment` | `OMENative` | Rejected |

The check is a filter: it adds nothing to a runtime's [score](runtime-selection-scoring.md). OME checks the mode before the model format, so runtimes built for other models can show a mode rejection too. [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md#step-3-read-the-exclusion-reasons) lists every check in order.

## Where each side declares a mode

Both sides use the `ome.io/deploymentMode` annotation, and an InferenceService can also set `spec.deploymentMode`:

| Where | Counts for matching |
|---|---|
| Runtime: `engineConfig.annotations` | Yes, for the engine |
| Runtime: `decoderConfig.annotations` | Yes, for the decoder |
| Runtime: an annotation inherited from a [parent runtime](../../concepts/runtimes/runtime-inheritance.md#when-the-merged-spec-applies) | Only when the InferenceService names the runtime |
| Runtime: `metadata.annotations`, `routerConfig.annotations`, or a `leader` and `worker` alone | No |
| InferenceService: `spec.engine.annotations` or `spec.decoder.annotations` | Yes, ahead of `spec.deploymentMode` |
| InferenceService: `spec.deploymentMode` (since v1.3) | Yes, for each component whose annotation sets no mode. It takes only `OMENative` or `RawDeployment`, so `MultiNode` needs the annotation. |
| InferenceService: `metadata.annotations` | No |

A mode counts when its value is `RawDeployment`, [`OMENative`](../../concepts/omenative/overview.md) (since v1.3), `MultiNode` (deprecated) or `VirtualDeployment` (legacy). Annotation values are case-sensitive, and OME silently treats a misspelled one as no mode.

Since v1.3, when the engine or the decoder asks for `OMENative`, the webhook requires both to ask for the same mode. See [For one component](../../concepts/architecture/deployment-modes.md#for-one-component).

## The runtime's mode also sets the running mode {#matching-versus-the-mode-a-component-runs}

OME merges the runtime's `engineConfig`, `decoderConfig` and `routerConfig` into the components the InferenceService declares, annotations included. So a runtime's mode becomes the component's mode, ahead of `spec.deploymentMode`, and only the component's own annotation in the InferenceService overrides it. An InferenceService that declares no mode can get a `MultiNode` runtime and run as `MultiNode`. Since v1.3, [`kubectl ome runtime effective`](../kubectl-ome/runtime.md#effective) shows each component's mode and the rule that set it. [How OME resolves the mode](../../concepts/architecture/deployment-modes.md#how-ome-resolves-the-mode) gives the full order.

A runtime whose `routerConfig` declares `MultiNode` passes matching, but a router can't run in that mode. For an InferenceService that sets `spec.router` and gets that runtime, OME can't create or update the router. The manager logs `Failed to reconcile component` with `invalid deployment mode for router`, and records no event.

## Example

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md).

This ClusterServingRuntime runs its engine in the deprecated `MultiNode` mode:

```yaml title="runtime.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-llama-3-3-70b-instruct-multinode
spec:
  supportedModelFormats:
    - modelFramework:
        name: transformers
        version: "4.47.0.dev0"
      modelFormat:
        name: safetensors
        version: "1.0.0"
      modelArchitecture: LlamaForCausalLM
      autoSelect: true
      priority: 2
  protocolVersions:
    - openAI
  modelSizeRange:
    min: 60B
    max: 75B
  engineConfig:
    annotations:
      ome.io/deploymentMode: MultiNode
    leader:
      volumes:
        - name: dshm
          emptyDir:
            medium: Memory
      runner:
        name: ome-container
        image: docker.io/lmsysorg/sglang:v0.5.5.post3-cu129-amd64
        ports:
          - containerPort: 8080
            name: http1
            protocol: TCP
        command:
          - python3
          - -m
          - sglang.launch_server
          - --host
          - "0.0.0.0"
          - --port
          - "8080"
          - --model-path
          - $(MODEL_PATH)
          - --tp-size
          - "8"
          - --nccl-init
          - $(LWS_LEADER_ADDRESS):5000
          - --nnodes
          - $(LWS_GROUP_SIZE)
          - --node-rank
          - $(LWS_WORKER_INDEX)
          - --served-model-name
          - meta-llama/Llama-3.3-70B-Instruct
        volumeMounts:
          - mountPath: /dev/shm
            name: dshm
        resources:
          requests:
            nvidia.com/gpu: 4
          limits:
            nvidia.com/gpu: 4
    worker:
      size: 1
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
          - --host
          - "0.0.0.0"
          - --port
          - "8080"
          - --model-path
          - $(MODEL_PATH)
          - --tp-size
          - "8"
          - --nccl-init
          - $(LWS_LEADER_ADDRESS):5000
          - --nnodes
          - $(LWS_GROUP_SIZE)
          - --node-rank
          - $(LWS_WORKER_INDEX)
          - --served-model-name
          - meta-llama/Llama-3.3-70B-Instruct
        volumeMounts:
          - mountPath: /dev/shm
            name: dshm
        resources:
          requests:
            nvidia.com/gpu: 4
          limits:
            nvidia.com/gpu: 4
```

This InferenceService asks for `OMENative` for all its components:

```yaml title="inference-service.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-70b
  namespace: llama-demo
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-3-70b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
```

`llama-3-3-70b-instruct` is a ClusterBaseModel, the default `kind`. Auto-selection skips the runtime above, and any other runtime whose engine declares a mode other than `OMENative`.

## Where a rejection shows up

OME applies the rule when it picks a runtime, and when it checks a runtime that the InferenceService names. A rejection gives this reason, with `engine` or `decoder` as the component and the runtime's mode first:

```text
runtime engine deployment mode MultiNode does not match requested engine deployment mode OMENative
```

| When | What you see |
|---|---|
| Another runtime passes | OME picks from the runtimes that pass, with no event. Since v1.3, [`kubectl ome runtime explain --isvc`](../kubectl-ome/runtime.md#explain) shows this one as `No`, with the reason. |
| No runtime passes | The webhook rejects the InferenceService. After admission, OME sets `RuntimeReady` to `False` and records a Warning event, both with reason `RuntimeNotFound` (since v1.3). Running pods keep serving. |
| The InferenceService names the runtime | OME uses it anyway. Since v1.3, the webhook warns `does not declare support for model`. OME records a `RuntimeCompatibilityAdvisory` Warning event, unless `spec.runtime.autoSync` is `false`. |

When the only runtimes are the example runtime and a ClusterServingRuntime with no `autoSelect: true` entry, the example InferenceService gets this message:

```text
no runtime found to support model safetensors with format safetensors in namespace llama-demo. Checked 2 runtimes (0 namespace-scoped, 2 cluster-scoped). Excluded runtimes: srt-llama-3-3-70b-instruct-multinode (runtime engine deployment mode MultiNode does not match requested engine deployment mode OMENative)
```

The message names the model by its format, and leaves out the other runtime because it passed every check.

If the example InferenceService also sets `spec.runtime.name: srt-llama-3-3-70b-instruct-multinode`, the event is:

```text
Runtime srt-llama-3-3-70b-instruct-multinode does not declare support for model llama-3-3-70b-instruct (runtime srt-llama-3-3-70b-instruct-multinode does not support model : runtime engine deployment mode MultiNode does not match requested engine deployment mode OMENative); proceeding because the runtime was named explicitly
```

## Resolve a conflict

- Use a runtime that declares the InferenceService's mode, or none. To move a `MultiNode` runtime to OMENative, [make an OMENative copy of it](../../guides/omenative/move-from-leaderworkerset.md#step-2-make-an-omenative-copy-of-the-runtime).
- Change the InferenceService's mode for that component to the runtime's, or remove it. Either way, the component runs in the runtime's mode.
- Name the runtime in `spec.runtime.name`. OME uses it with a warning, and the component runs in [the runtime's mode](#matching-versus-the-mode-a-component-runs) unless the component's own annotation sets one.
- To keep InferenceServices that declare no mode off a runtime, leave `autoSelect` unset or `false` on all its entries. Then [name the runtime](../../guides/deploy-models/reference-a-runtime-explicitly.md) where you need it.

## Related pages

- [Deployment modes and OMENative](../../concepts/architecture/deployment-modes.md): how each mode runs a component.
- [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md): move a `MultiNode` component and its runtime to OMENative.
- [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md): find out why OME picked a runtime, or none.
- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md): name the runtime instead of letting OME pick one.
- [`kubectl ome runtime`](../kubectl-ome/runtime.md): see why a runtime was skipped, and each component's mode.
- [Runtime accelerator-class matching](runtime-accelerator-class-matching.md): how accelerator classes narrow the runtimes OME can pick.
- [Model version matching](model-version-matching.md): how a model's format and framework versions match a runtime's entries.
- [OME API](../api/ome.v1beta1.md#ome-io-v1beta1-ServingRuntimeSpec): the fields of `ServingRuntimeSpec`, including `engineConfig` and `decoderConfig`.
