---
title: Runtime accelerator-class matching
description: Auto-selection rejects a runtime that does not list every AcceleratorClass your InferenceService names; an explicitly named runtime only gets a warning.
---

When an [InferenceService](../../concepts/serving/inference-services.md) names an [accelerator class](../../concepts/runtimes/accelerator-classes.md), OME auto-selects from the [serving runtimes](../../concepts/runtimes/serving-runtimes.md) that list that class, so the InferenceService runs on a runtime made for that GPU type. A runtime that you name in `spec.runtime.name` is used even when it doesn't list the class, with a warning.

## The matching rule

A runtime stays a candidate when its `spec.acceleratorRequirements.acceleratorClasses` lists every class that the InferenceService names. The check is a filter: it adds nothing to a runtime's [score](runtime-selection-scoring.md).

| The InferenceService names | The runtime's `acceleratorClasses` | Outcome |
| --- | --- | --- |
| No class | Any list, or none | Candidate |
| `nvidia-h100` | `[nvidia-h100, nvidia-h200]` | Candidate |
| `nvidia-h100` | None, or an empty list | Rejected |
| `nvidia-a100` | `[nvidia-h100, nvidia-h200]` | Rejected |
| `nvidia-h100` for the engine, `nvidia-h200` for the decoder | `[nvidia-h100, nvidia-h200]` | Candidate |
| `nvidia-h100` for the engine, `nvidia-a100` for the decoder | `[nvidia-h100, nvidia-h200]` | Rejected |

- The engine's and the decoder's classes are pooled, and a runtime has one list, so list every class it runs on.
- Most runtimes in the [OME catalog](../../concepts/runtimes/serving-runtimes.md#the-runtime-catalog) list no classes, so an InferenceService that names a class rules them out until you add the class to their list. [Runtime deployment-mode matching](runtime-deployment-mode-matching.md) works the other way: a runtime that declares no mode stays a candidate.
- Names match exactly, including case.
- Only `acceleratorClasses` counts. OME ignores the other `acceleratorRequirements` fields, such as `minMemory` and `requiredFeatures`.
- Auto-selection reads a runtime's own `acceleratorClasses`, not the ones it [inherits](../../concepts/runtimes/runtime-inheritance.md) through `ome.io/inherit-from` (since v1.3). A runtime that you name is checked with what it inherits.
- Since v1.3, an InferenceService without `spec.model` skips this check.

## Where an InferenceService names a class

An InferenceService can name classes in four places, and OME pools them:

| Where | Field |
| --- | --- |
| Annotation | `ome.io/accelerator-class` in `metadata.annotations` |
| Accelerator selector | `spec.acceleratorSelector.acceleratorClass` |
| Engine override | `spec.engine.acceleratorOverride.acceleratorClass` |
| Decoder override | `spec.decoder.acceleratorOverride.acceleratorClass` |

Only names count: a `policy` or `constraints` doesn't filter runtimes, and a policy picks from the chosen runtime's list afterwards. An empty value counts as a class that no runtime lists, so auto-selection rejects every runtime.

## Declare both sides

The example needs the [ClusterBaseModel](../../concepts/models/base-models.md) `llama-3-3-70b-instruct` and the AcceleratorClasses `nvidia-h100` and `nvidia-h200`. For a BaseModel, add `kind: BaseModel` under `spec.model`. OME doesn't ship any AcceleratorClasses, so create them first, as in [Step 1 of Select accelerators](../../guides/deploy-models/select-accelerators.md#step-1-create-the-acceleratorclasses). This ClusterServingRuntime lists both classes:

```yaml title="runtime.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: llama-3-3-70b-hopper
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
      priority: 1
  modelSizeRange:
    min: 60B
    max: 75B
  acceleratorRequirements:
    acceleratorClasses:
      - nvidia-h100
      - nvidia-h200
  engineConfig:
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
        - --enable-metrics
        - --log-requests
        - --model-path
        - $(MODEL_PATH)
        - --tp-size
        - "4"
        - --mem-frac
        - "0.9"
        - --served-model-name
        - meta-llama/Llama-3.3-70B-Instruct
      volumeMounts:
        - mountPath: /dev/shm
          name: dshm
      resources:
        requests:
          cpu: 10
          memory: 160Gi
          nvidia.com/gpu: 4
        limits:
          cpu: 10
          memory: 160Gi
          nvidia.com/gpu: 4
```

The `engineConfig` is trimmed from the catalog runtime [`srt-llama-3-3-70b-instruct`](https://github.com/ome-projects/ome/blob/main/config/runtimes/srt/meta/llama-3-3-70b-instruct-rt.yaml). If a class that the runtime lists is missing, the webhook rejects the runtime, with an error that ends like this:

```text
admission webhook "clusterservingruntime.ome-webhook-server.validator" denied the request: unknown accelerator classes referenced in AcceleratorRequirements: [nvidia-h100 nvidia-h200]
```

If you delete a class that runtimes list, the webhook rejects their later updates, from kubectl or Helm, while they're enabled and still list it. See [Change or delete a class](../../concepts/runtimes/accelerator-classes.md#change-or-delete-a-class).

On the InferenceService, name the class. `spec.acceleratorSelector` is the usual place:

```yaml title="inference-service.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-3-70b
  namespace: llama-demo
spec:
  model:
    name: llama-3-3-70b-instruct
  acceleratorSelector:
    acceleratorClass: nvidia-h100
  engine:
    minReplicas: 1
    maxReplicas: 1
```

The InferenceService names no runtime, so OME auto-selects one from the runtimes that list `nvidia-h100`. If the model is `Ready` with its [format filled in](../../concepts/models/base-models.md#what-ome-learns-from-the-model), and no other runtime that serves it lists `nvidia-h100`, the webhook names `llama-3-3-70b-hopper` when you apply the InferenceService:

```bash
kubectl apply -f inference-service.yaml
```

```output
Warning: Runtime llama-3-3-70b-hopper will be auto-selected for model llama-3-3-70b-instruct
inferenceservice.ome.io/llama-3-3-70b created
```

## Where a rejection shows up

What you see depends on whether OME picks the runtime or you name it. A runtime that fails the check gets this reason:

```text
runtime does not support the required accelerator class
```

| The runtime is | What you see |
| --- | --- |
| Auto-selected, at admission | When every runtime is ruled out, the webhook rejects the InferenceService with a `no runtime found` message. See [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md#how-a-selection-failure-shows-up). |
| Auto-selected, after admission | Since v1.3, `RuntimeReady` is `False` with reason `RuntimeNotFound`; `Ready` and serving pods are unaffected. See [Step 1 of Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md#step-1-read-the-runtimeready-condition). |
| Named in `spec.runtime.name` | Since v1.3, the webhook warns. OME uses the runtime, and records a `RuntimeCompatibilityAdvisory` Warning event unless `spec.runtime.autoSync` is `false`. See [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md#what-ome-checks-when-you-apply). |

The webhook checks only InferenceServices with an `engine` section, and leaves auto-selection until after admission when the engine sets `engine.runner.image`, or both `engine.leader.runner.image` and `engine.worker.runner.image`.

With auto-selection, the `no runtime found` message lists each excluded runtime with the first check it failed. This check runs before the format check, so runtimes made for other models can show this reason too. When the example InferenceService names `nvidia-a100` instead, and `llama-3-3-70b-hopper` is the only runtime, the message is:

```text
no runtime found to support model safetensors with format safetensors in namespace llama-demo. Checked 1 runtimes (0 namespace-scoped, 1 cluster-scoped). Excluded runtimes: llama-3-3-70b-hopper (runtime does not support the required accelerator class)
```

Once the InferenceService exists, `kubectl ome runtime explain --isvc llama-3-3-70b -n llama-demo` (since v1.3) shows this reason for each runtime that fails the check; see [`kubectl ome runtime`](../kubectl-ome/runtime.md#explain).

If the example InferenceService also sets `spec.runtime.name: srt-llama-3-3-70b-instruct`, a catalog runtime that lists no classes, kubectl prints this after `Warning:` (since v1.3):

```text
runtime "srt-llama-3-3-70b-instruct" does not declare support for model "llama-3-3-70b-instruct" (runtime srt-llama-3-3-70b-instruct does not support model : runtime does not support the required accelerator class); proceeding because the runtime was named explicitly
```

!!! warning "A named runtime without classes drops the class"
    A component gets a class only when its runtime lists at least one. If the runtime that you name lists none, OME ignores the class names and policies that the InferenceService sets, and records no event. If it lists some, the component gets the class that the InferenceService names, even one the runtime doesn't list.

To clear the rejection or the warning, add the class to the runtime's list, as in [Step 2 of Select accelerators](../../guides/deploy-models/select-accelerators.md#step-2-check-the-runtimes-candidates).

## Matching versus the class a component gets

Matching only narrows the runtimes. Once OME has the runtime, it picks a class for the engine and the decoder from `spec.acceleratorSelector` and the overrides, as [How OME picks a class](../../guides/deploy-models/select-accelerators.md#how-ome-picks-a-class) describes. The `ome.io/accelerator-class` annotation counts only for matching: it gives no component a class. When the class that a component names is missing, OME records an `AcceleratorClassError` Warning event and leaves the serving workload as it is until the class exists. See [Change or delete a class](../../concepts/runtimes/accelerator-classes.md#change-or-delete-a-class).

## Related pages

- [Select accelerators](../../guides/deploy-models/select-accelerators.md): pick the AcceleratorClass for each component by name, or let OME choose one with a policy.
- [Accelerator classes](../../concepts/runtimes/accelerator-classes.md): how an AcceleratorClass catalogs one type of GPU, and how nodes are matched to it.
- [Troubleshoot runtime selection](../../guides/deploy-models/troubleshoot-runtime-selection.md): find out why OME picked a runtime, or none.
- [Reference a runtime explicitly](../../guides/deploy-models/reference-a-runtime-explicitly.md): name the runtime instead of letting OME pick one.
- [`kubectl ome runtime`](../kubectl-ome/runtime.md): see which runtimes pass for an InferenceService, and why others fail (since v1.3).
