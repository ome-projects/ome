---
title: Runtime Deployment-Mode Matching
linkTitle: Deployment-Mode Matching
weight: 3
description: >
  When runtime auto-selection rejects a runtime because of a declared deployment mode, and how to declare modes on a runtime's engineConfig/decoderConfig and on InferenceService components.
---

When an InferenceService names no runtime (`spec.runtime` is unset), OME auto-selects one by scoring every enabled ServingRuntime and ClusterServingRuntime against the model and the service (see [Runtime Selection Logic](/ome/docs/concepts/serving_runtime#runtime-selection-logic) for the other dimensions). One of the compatibility checks compares **deployment modes** per component, so a runtime built for one dispatch style — for example a LeaderWorkerSet-backed multi-node runtime — can be kept away from services that explicitly request a different style.

## The matching rule

The engine and the decoder are compared independently, and a runtime must pass both comparisons. For each component:

> A runtime is rejected **only when both sides explicitly declare a deployment mode for that component and the two values differ.** If either side declares nothing (or declares an invalid value), the comparison is skipped and the runtime remains a candidate.

This is a hard filter, not a preference: a matching mode does not raise a runtime's score, and a silent runtime is never ranked below one that declares the requested mode.

| InferenceService engine declares | Runtime `engineConfig` declares | Outcome |
|----------------------------------|---------------------------------|---------|
| nothing                          | nothing                         | candidate |
| nothing                          | `MultiNode`                     | candidate — a silent service is never filtered |
| `MultiNode`                      | nothing                         | candidate — a silent runtime is never filtered |
| `MultiNode`                      | `MultiNode`                     | candidate |
| `RawDeployment`                  | `MultiNode`                     | **rejected** |

The same table applies to the decoder, with one extra rule: a service with no `spec.decoder` never declares a decoder mode, so a runtime's `decoderConfig` annotation never filters out services that will not deploy a decoder.

Because both sides must opt in, this dimension cannot hide a runtime from services that declare nothing. To keep a runtime out of auto-selection entirely, leave `autoSelect` off on its supported formats instead.

## Valid mode values

A declaration only counts when its value is one of:

- `RawDeployment` — Kubernetes Deployment/HPA-backed dispatch
- `MultiNode` — LeaderWorkerSet-backed multi-node serving
- `OMENative` — native lifecycle-managed pods (also covers native multi-node)
- `VirtualDeployment` — legacy

Any other value — including `PDDisaggregated` — is treated as *undeclared* and skipped. PD-disaggregated is a shape derived from a service declaring both an engine and a decoder, not a declarable mode; to scope a PD runtime, annotate its `engineConfig` and `decoderConfig` each with the mode that component actually runs in (for example `MultiNode`).

## Declaring a mode on a runtime

Put the `ome.io/deploymentMode` annotation inside `spec.engineConfig.annotations` and/or `spec.decoderConfig.annotations`:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-llama-multinode
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
        version: "1.0.0"
      autoSelect: true
      priority: 2
  engineConfig:
    annotations:
      ome.io/deploymentMode: "MultiNode"
    leader:
      # ...
    worker:
      size: 1
      # ...
```

Only the annotation counts for matching:

- A leader/worker shape alone is **not** a declaration — a runtime whose `engineConfig` defines `leader` and `worker` but carries no annotation still matches services that request any mode.
- A runtime without an `engineConfig` (or `decoderConfig`) declares no mode for that component.
- Annotations on the runtime's top-level metadata are not consulted.

## Declaring a mode on an InferenceService

For each component, the matcher reads, in order:

1. The `ome.io/deploymentMode` annotation on the component (`spec.engine.annotations` / `spec.decoder.annotations`), when it holds a valid value.
2. Otherwise, the typed `spec.deploymentMode` field, which the CRD restricts to `OMENative` or `RawDeployment` and which applies to every component. It counts for the engine comparison even when `spec.engine` is absent; for the decoder it counts only when `spec.decoder` is declared.

`MultiNode` can therefore only be requested through the per-component annotation:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-70b
spec:
  model:
    name: llama-3-3-70b-instruct
  engine:
    annotations:
      ome.io/deploymentMode: "MultiNode"
    minReplicas: 1
    maxReplicas: 1
```

This service skips runtimes whose `engineConfig` declares a *different* mode (for example `RawDeployment`), but still matches runtimes that declare nothing.

The `ome.io/deploymentMode` annotation on the InferenceService's own `metadata.annotations` is **not** used for matching — it selects the service-level deployment strategy (see [Labels and Annotations](/ome/docs/reference/labels-and-annotations)), and the matcher ignores it.

## Where a rejection surfaces

A rejected component produces the reason:

```
runtime engine deployment mode MultiNode does not match requested engine deployment mode RawDeployment
```

(or `decoder` for the decoder comparison). When no runtime survives filtering, auto-selection fails with a `no runtime found to support model ...` message that lists each excluded runtime with its first incompatibility reason; it is surfaced as a warning event and status condition on the InferenceService rather than an error requeue, and self-heals once a matching runtime is created.

An explicitly named runtime (`spec.runtime.name`) goes through the same compatibility check, but a deployment-mode mismatch there is downgraded to a `RuntimeCompatibilityAdvisory` warning event and reconciliation proceeds with the named runtime — the deliberate choice wins. (Sharded base models are the exception: validation failures on them remain hard errors.)

## Matching versus the mode a component actually runs

This check only filters candidates; it does not set anything. After a runtime is chosen, its `engineConfig`/`decoderConfig` is merged with the service's component spec (service values win) and each component's actual mode is resolved from the merged result: per-component annotation, then `spec.deploymentMode`, then leader/worker shape (which infers `OMENative`), then the `RawDeployment` default. A runtime annotation used for matching therefore usually also becomes the component's actual mode, unless the service overrides it. See [Deployment Modes](/ome/docs/concepts/inference_service#deployment-modes) for that resolution.
