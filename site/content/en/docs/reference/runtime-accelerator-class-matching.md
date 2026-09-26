---
title: Runtime Accelerator-Class Matching
linkTitle: Accelerator-Class Matching
weight: 3
description: >
  When runtime auto-selection rejects a runtime because the InferenceService names an AcceleratorClass the runtime does not list, and what happens instead when the runtime was named explicitly.
---

When an InferenceService names no runtime (`spec.runtime` is unset), OME auto-selects one by scoring every enabled ServingRuntime and ClusterServingRuntime against the model and the service (see [Runtime Selection Logic](/ome/docs/concepts/serving_runtime#runtime-selection-logic) for the other dimensions). One of the compatibility checks compares **accelerator classes**: a service that names an [AcceleratorClass](/ome/docs/tasks/run-workloads/select-accelerators/) only matches runtimes that declare support for that class, so a runtime built for one GPU generation can be kept away from services pinned to different hardware.

## Where a service names a class

The matcher collects required classes from every place the InferenceService can name one, as a single deduplicated set:

1. The `ome.io/accelerator-class` annotation on the service's `metadata.annotations`
2. `spec.acceleratorSelector.acceleratorClass`
3. `spec.engine.acceleratorOverride.acceleratorClass`
4. `spec.decoder.acceleratorOverride.acceleratorClass`

Only explicit class **names** count. A `policy` (`BestFit`, `Cheapest`, `MostCapable`, `FirstAvailable`) or a `constraints` block in `spec.acceleratorSelector` is not a named class and never filters runtimes — a policy picks from whatever classes the resolved runtime lists, after selection.

## The matching rule

> A runtime stays a candidate only when **every** class the service names appears in the runtime's `spec.acceleratorRequirements.acceleratorClasses` list. A service that names no class anywhere is never filtered on this dimension.

| Service names | Runtime `acceleratorRequirements.acceleratorClasses` | Outcome |
|---------------|------------------------------------------------------|---------|
| nothing | anything, or nothing | candidate |
| `nvidia-a100` | `[nvidia-a100, nvidia-tesla-t4]` | candidate |
| `nvidia-a100` | absent, or an empty list | **rejected** |
| `nvidia-h100` | `[nvidia-a100]` | **rejected** |
| engine override `nvidia-h100`, decoder override `nvidia-a100` | `[nvidia-h100]` | **rejected** — the one list must contain both |

Details of the comparison:

- **The classes are pooled, not matched per component.** An engine override and a decoder override naming different classes both land in the same required set, and each is checked against the runtime's single `acceleratorClasses` list. A runtime cannot declare "H100 for the engine, A100 for the decoder" — list both.
- **A silent runtime is filtered, unlike deployment-mode matching.** A runtime with no `acceleratorRequirements` (or an empty `acceleratorClasses` list) is rejected the moment the service names any class. To keep a runtime eligible for services that pin hardware, list every class it can run on.
- **It is a hard filter, not a preference.** A matching class does not raise a runtime's score, and a runtime listing the exact class is not ranked above one that merely includes it among many.
- **Matching is an exact, case-sensitive string comparison.** The matcher does not read AcceleratorClass resources at this stage, so the named class does not need to exist in the cluster for a runtime to match — existence is checked later, when the class is resolved for pod generation.
- **Only `acceleratorClasses` participates.** The other `acceleratorRequirements` fields (`minMemory`, `minComputePerformanceTFLOPS`, `minArchitectureVersion`, `requiredFeatures`, `preferredPrecisions`) do not filter runtime selection.

## Declaring the two sides

On the runtime, list the supported classes under `spec.acceleratorRequirements`:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-llama-h100
spec:
  supportedModelFormats:
    - modelFormat:
        name: safetensors
        version: "1.0.0"
      autoSelect: true
      priority: 2
  acceleratorRequirements:
    acceleratorClasses:
      - nvidia-h100
      - nvidia-h200
```

On the InferenceService, any of the four sources above works; the typed selector is the common one:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-70b
spec:
  model:
    name: llama-3-3-70b-instruct
  acceleratorSelector:
    acceleratorClass: nvidia-h100
```

This service auto-selects only among runtimes whose `acceleratorClasses` include `nvidia-h100`; the runtime above qualifies.

## Where a rejection surfaces

A rejected runtime is excluded with the reason:

```
runtime does not support the required accelerator class
```

This check runs before the deployment-mode and model-format checks, so a runtime that fails several dimensions reports this reason. When no runtime survives filtering, auto-selection fails with a `no runtime found to support model ...` message that lists each excluded runtime with its first incompatibility reason; it is surfaced as a warning event and status condition on the InferenceService rather than an error requeue, and self-heals once a matching runtime is created.

An explicitly named runtime (`spec.runtime.name`) goes through the same compatibility check, but an accelerator-class mismatch there is downgraded to a `RuntimeCompatibilityAdvisory` warning event and reconciliation proceeds with the named runtime — the deliberate choice wins. (Sharded base models are the exception: validation failures on them remain hard errors.)

## Matching versus the class a component actually gets

This check only prunes auto-selection candidates; it does not choose an accelerator or shape any pod. After a runtime is resolved, [accelerator selection](/ome/docs/tasks/run-workloads/select-accelerators/) picks each component's class following its own precedence, which leads to two asymmetries worth knowing:

- The `ome.io/accelerator-class` **annotation participates only in matching.** Post-selection resolution reads `spec.acceleratorSelector` and the per-component `acceleratorOverride` — never the annotation — so an annotation alone narrows the runtime candidates but puts no class on the generated pods.
- Auto-selection **guarantees** the resolved runtime lists every class the service names; an explicitly named runtime carries no such guarantee. Post-selection resolution does not re-check the list for an explicit class name, but it only runs at all when the resolved runtime lists at least one class — with a runtime that declares no `acceleratorRequirements`, the whole selector is silently ignored.
