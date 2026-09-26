---
title: "Reference a Serving Runtime Explicitly"
linkTitle: "Reference a Runtime Explicitly"
weight: 20
date: 2026-09-26
description: >
  Learn how to name a ServingRuntime or ClusterServingRuntime directly on an InferenceService, how the name is resolved, and what is validated.
---

This page shows you how to bypass automatic runtime selection by naming a runtime directly on an InferenceService with `spec.runtime.name`. You'll learn how the reference is resolved when a namespaced ServingRuntime and a ClusterServingRuntime share the same name, what the admission webhook checks at `kubectl apply` time, and how the controller reacts when the named runtime doesn't declare support for your model — or doesn't exist.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed
- `kubectl` configured to communicate with your cluster
- A `Ready` BaseModel or ClusterBaseModel, and the ServingRuntime or ClusterServingRuntime you intend to use

## The runtime reference

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-1b-instruct
  namespace: demo
spec:
  model:
    name: llama-3-2-1b-instruct
  runtime:
    name: srt-llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
```

| Field                 | Default                 | Description |
|-----------------------|-------------------------|-------------|
| `runtime.name`        | —                       | Name of the runtime to use. Setting it disables auto-selection entirely. |
| `runtime.kind`        | `ClusterServingRuntime` | Scope of the reference: `ClusterServingRuntime` or `ServingRuntime` (namespaced). |
| `runtime.apiGroup`    | `ome.io`                | API group of the referenced runtime. |
| `runtime.autoSync`, `runtime.revision` | `true`, unset | Control live-sync versus pinning to a runtime snapshot — see [Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision). |

An explicitly named runtime skips the weighted scoring used by auto-selection. In particular, `autoSelect: true` is **not** required on any of the runtime's `supportedModelFormats`: a runtime whose formats are all `autoSelect: false` is invisible to auto-selection but fully usable by explicit reference, and when OME derives model-format metadata for the generated pods it considers every `supportedModelFormats` entry, not only the auto-selectable ones.

## How the name is resolved

`spec.runtime.kind` scopes the lookup:

| `kind`                             | Where OME looks |
|------------------------------------|-----------------|
| omitted, or `ClusterServingRuntime` | A ClusterServingRuntime with that name first; if none exists, a ServingRuntime with that name in the InferenceService's own namespace. |
| `ServingRuntime`                   | Only a ServingRuntime in the InferenceService's namespace. Never falls back to cluster scope. |

Because the API server defaults `kind` to `ClusterServingRuntime`, a bare `name:` reference is **cluster-first**: if a namespaced ServingRuntime and a ClusterServingRuntime share the name, the ClusterServingRuntime wins. The namespaced fallback exists so a bare-name reference to a runtime that only exists in your namespace keeps resolving.

To select the namespaced runtime on a name collision, declare the kind:

```yaml
spec:
  runtime:
    name: srt-llama-3-2-1b-instruct
    kind: ServingRuntime
```

With `kind: ServingRuntime`, a missing namespaced runtime is an error even when a cluster runtime with the same name exists — OME never silently substitutes the other scope. Admission-time validation honors the declared kind too, so the webhook inspects the same runtime the controller will resolve.

## What is validated when you apply

For an InferenceService that declares an engine and a model, the validating webhook runs these checks on both create and update:

- **Model resolvable and enabled** — a missing or disabled model rejects the request.
- **Runtime exists** — a named runtime that resolves to nothing in the scopes above rejects the request, for example:

  ```
  runtime srt-typo not found in namespace demo or at cluster scope
  ```

  or, for a `kind: ServingRuntime` reference:

  ```
  ServingRuntime srt-typo not found in namespace demo
  ```

  Apply order matters here: create the runtime before the InferenceServices that name it.

- **Runtime enabled** — a runtime with `spec.disabled: true` rejects the request. Naming a runtime explicitly never overrides `disabled`.
- **Declared model support** — this one is **advisory**, not blocking. If the runtime exists but its `supportedModelFormats` don't declare a match for the model (format, framework, architecture, quantization, or size range), the request is admitted with a warning:

  ```
  Warning: runtime "srt-generic" does not declare support for model "llama-3-2-1b-instruct" (...); proceeding because the runtime was named explicitly
  ```

  A generic runtime can serve many models it never enumerates, so your deliberate choice wins over the runtime's declaration. **Exception:** models with `distribution: Sharded`. For those, a compatibility failure means no supported model-cache provider is configured, which a sharded model physically cannot load without, so it stays a hard rejection.

A runtime-only InferenceService (no `spec.model`) gets a lighter check: a disabled runtime is still rejected, but a runtime that doesn't exist *yet* is admitted — the controller parks the service until the runtime appears, as described next.

## If the runtime goes missing

A named runtime can stop resolving after admission — most commonly because someone deleted it, or because a runtime-only InferenceService was created ahead of its runtime. The controller treats this as a permanent configuration issue rather than a transient failure:

- The InferenceService gets a Warning event with reason `RuntimeNotFound` and a `RuntimeReady` status condition set to `False` with reason `RuntimeNotFound`.
- The reconcile stops **before touching any child resources**, so a currently-serving InferenceService keeps running its existing pods on the last resolved runtime spec.
- `RuntimeReady` is advisory: it is not part of the aggregate `Ready` condition, so a healthy, serving InferenceService stays `Ready=True` while the new spec is withheld.
- The controller does not requeue in a hot loop. It waits for the ServingRuntime/ClusterServingRuntime watch to re-trigger reconciliation, so the problem **self-heals** the moment a runtime with the right name is created: the condition flips to `True` with reason `RuntimeResolved` and reconciliation proceeds.

Check the condition and events:

```bash
kubectl get isvc llama-3-2-1b-instruct -n demo \
  -o jsonpath='{.status.conditions[?(@.type=="RuntimeReady")]}' | jq
kubectl describe isvc llama-3-2-1b-instruct -n demo
```

```json
{
  "type": "RuntimeReady",
  "status": "False",
  "reason": "RuntimeNotFound",
  "message": "runtime srt-llama-3-2-1b-instruct not found in namespace demo or at cluster scope"
}
```

The `RuntimeReady` condition only appears on InferenceServices that have hit a resolution problem at least once; services that always resolved cleanly don't carry it.

Auto-selected runtimes get the same treatment when no compatible runtime exists for the model, so `RuntimeReady=False` with reason `RuntimeNotFound` is the single signal to watch for "this service is waiting on a runtime".

## Declared-format mismatch at reconcile time

The same advisory posture applies while the service runs. If the explicitly named runtime doesn't declare support for the model, each reconcile emits a Warning event with reason `RuntimeCompatibilityAdvisory` and proceeds:

```
Runtime srt-generic does not declare support for model llama-3-2-1b-instruct (...); proceeding because the runtime was named explicitly
```

The event is informational — deployment, upgrades, and scaling continue normally. The sharded-model exception applies here too: for a `distribution: Sharded` model the mismatch stays a hard error (Warning event reason `RuntimeValidationError`, reconcile retried), as does any disabled runtime the controller encounters.

## Next steps

- [Serving Runtime concepts](/ome/docs/concepts/serving_runtime) — the ServingRuntime and ClusterServingRuntime resources and auto-selection scoring
- [Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision) — decouple an InferenceService from live runtime edits with `autoSync: false`
- [Deploy a Simple Inference Service](/ome/docs/tasks/run-workloads/deploy-inference-service/) — end-to-end deployment, testing, and monitoring
