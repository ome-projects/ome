---
title: "Runtime Inheritance"
linkTitle: "Runtime Inheritance"
weight: 26
description: >
  Factor shared ServingRuntime configuration into reusable parent profiles with the ome.io/inherit-from annotation.
---

Serving runtimes for related models tend to repeat the same boilerplate: the same environment variables, shared-memory volumes, node selectors, and replica defaults, copied into every runtime that differs only in image and model metadata. **Runtime inheritance** lets a [ServingRuntime or ClusterServingRuntime](/ome/docs/concepts/serving_runtime) name another runtime as its parent; OME merges the parent's spec underneath the child's, so the child only declares what is different.

A runtime opts in with a single annotation:

```yaml
metadata:
  annotations:
    ome.io/inherit-from: <parent-runtime-name>
```

Parents can themselves inherit from other runtimes, forming a chain. The merged ("effective") spec is never stored anywhere — every consumer re-walks the chain and merges at read time, which is why edits to a parent propagate to children automatically.

## Defining a profile and inheriting from it

A common pattern is a **runtime profile**: a runtime that exists only to be inherited from, never to serve traffic itself. Mark it with `ome.io/runtime-profile: "true"`; the admission webhook then requires it to set `spec.disabled: true`, which also keeps it out of runtime auto-selection.

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: sglang-base-profile
  annotations:
    ome.io/runtime-profile: "true"   # marker: this runtime is a reusable profile
spec:
  disabled: true                     # required when runtime-profile is "true"
  protocolVersions:
    - openAI
  engineConfig:
    minReplicas: 1
    maxReplicas: 3
    volumes:
      - name: dshm
        emptyDir:
          medium: Memory
    runner:
      env:
        - name: NCCL_DEBUG
          value: INFO
        - name: SGLANG_LOG_LEVEL
          value: info
```

A serving runtime inherits from the profile and declares only its own image, model metadata, and overrides:

```yaml
apiVersion: ome.io/v1beta1
kind: ClusterServingRuntime
metadata:
  name: srt-mistral-7b-instruct
  annotations:
    ome.io/inherit-from: sglang-base-profile
spec:
  disabled: false                    # must be set explicitly; see note below
  supportedModelFormats:
    - modelFormat:
        name: safetensors
      modelArchitecture: MistralForCausalLM
      autoSelect: true
      priority: 1
  modelSizeRange:
    min: 5B
    max: 9B
  engineConfig:
    runner:
      image: lmsysorg/sglang:v0.4.6.post6
      env:
        - name: NCCL_DEBUG           # overrides the profile's INFO
          value: WARN
```

The effective spec of `srt-mistral-7b-instruct` has the profile's `protocolVersions`, replica bounds, and `dshm` volume, the child's image and model formats, and a runner environment containing `SGLANG_LOG_LEVEL=info` from the profile plus the child's winning `NCCL_DEBUG=WARN`.

> **Note:** `disabled` merges like any other scalar: if the parent sets `disabled: true` and the child leaves it unset, the child's *effective* spec is disabled too, and InferenceServices referencing it are rejected as using a disabled runtime. A runtime inheriting from a disabled profile must set `spec.disabled: false` explicitly.

The `ome.io/runtime-profile` marker is optional — any runtime can be named as a parent, including an enabled runtime that serves traffic itself. The marker is for parents that should never serve directly.

## What merges

Inheritance uses a Kubernetes strategic merge of the child's spec onto the parent's: **any field the child sets wins; any field it leaves unset is filled from the parent.** Unset child fields never erase parent values. In detail:

| Field kind | Behavior | Examples |
|------------|----------|----------|
| Scalars | Child value wins if set; otherwise parent's | `disabled`, `engineConfig.minReplicas`, `runner.image` |
| Maps | Merged per key; child wins on colliding keys | `nodeSelector` |
| Lists with a merge key | Merged element-wise by key; elements with the same key are merged recursively | `containers` (by `name`), `volumes` (by `name`), `runner.env` (by `name`) |
| Plain lists | Replaced wholesale when the child sets them; inherited when it doesn't | `runner.command`, `runner.args`, `protocolVersions` |
| Nested structs | Merged recursively field by field | `engineConfig`, `routerConfig`, `decoderConfig` |

So a child can add one environment variable to an inherited container without restating the rest, but replacing `command` or `args` is all-or-nothing.

When a chain is longer than two runtimes, merging happens bottom-up from the root: each runtime is overlaid onto the result of merging everything above it, so the runtime closest to the leaf always wins a conflict.

## Chain limits

- **Maximum depth:** a chain may contain at most **5** runtimes — the runtime itself plus up to 4 ancestors. Longer chains fail resolution with reason `MaxDepthExceeded`.
- **Cycles** (including a runtime naming itself) fail resolution with reason `InheritanceCycle`.
- **Missing parents** — a name in the chain that resolves to no runtime — fail resolution with reason `ParentNotFound`.

## How parent names resolve across scopes

The `ome.io/inherit-from` value is a bare runtime name. Where it is looked up depends on the child's scope:

- A **ClusterServingRuntime** resolves its parent against cluster scope only: the parent must be another ClusterServingRuntime.
- A **ServingRuntime** resolves its parent in its **own namespace first**, then falls back to a ClusterServingRuntime of the same name. If both exist, the same-namespace runtime shadows the cluster-scoped one.
- **Cross-namespace inheritance is not possible.** A ServingRuntime in another namespace is never a resolution candidate, even if the name matches — the lookup reports `ParentNotFound`.

## Validation at admission

The validating webhook resolves the full chain on every ServingRuntime and ClusterServingRuntime create and update, and **denies** the write if:

- the chain has a missing parent, a cycle, or exceeds the maximum depth; or
- the runtime carries `ome.io/runtime-profile: "true"` without `spec.disabled: true` (checked even on otherwise-disabled runtimes).

Admission only validates the object being written, so a chain that was valid at admission can still break later:

- deleting a runtime is not validated, so removing a parent breaks every descendant's chain;
- adding a parent *above* an existing chain passes admission (the edited runtime's own chain is short) but can push a deep descendant past the depth limit.

These breakages don't reject anything at the API level — they surface through status, described next.

## Observing chain health

A controller in the OME manager re-resolves every runtime's chain whenever the runtime — or anything above it in the chain — changes spec or annotations, and records the result on the runtime's status:

| Status field | Meaning |
|--------------|---------|
| `status.inheritanceChain` | Runtime names walked, **root first**, ending with the runtime itself. A single-entry list (just the runtime's own name) when no inheritance is declared. On resolution failure the last successfully resolved chain is preserved as history. |
| `InheritanceReady` condition | `True` with reason `Resolved` when the chain resolves; `False` with reason `ParentNotFound`, `InheritanceCycle`, `MaxDepthExceeded`, or `ResolverError` when it doesn't. The message carries the resolver's error, including the chain walked so far. |

Resolution failures also emit a `Warning` event on the runtime with the same reason. To inspect:

```bash
# The resolved chain, root first (e.g. ["sglang-base-profile","srt-mistral-7b-instruct"])
kubectl get clusterservingruntime srt-mistral-7b-instruct \
  -o jsonpath='{.status.inheritanceChain}'

# Chain health
kubectl get clusterservingruntime srt-mistral-7b-instruct \
  -o jsonpath='{.status.conditions[?(@.type=="InheritanceReady")]}'

# Failure events (ParentNotFound, InheritanceCycle, MaxDepthExceeded)
kubectl describe clusterservingruntime srt-mistral-7b-instruct
```

Because a parent edit changes what every runtime below it resolves to, the controller fans status updates out to the whole subtree: editing a cluster-scoped profile re-resolves every ClusterServingRuntime and ServingRuntime that inherits from it, directly or transitively; editing a namespaced runtime re-resolves its same-namespace descendants. The `InheritanceReady` condition is informational — a broken chain does not stop the runtime object from existing, but consumers fail to resolve it (see below).

## When the merged spec applies

- **InferenceServices that name the runtime** (`spec.runtime.name`) render their workloads from the merged spec, resolved fresh on every reconcile. If the chain is broken, resolving the runtime fails and the InferenceService cannot pick up the runtime until the chain is repaired; a currently-serving InferenceService keeps running its existing pods.
- **Parent edits reach consumers automatically.** Editing a profile re-reconciles every InferenceService referencing any runtime in the subtree below it. Live-tracking services (the default, `spec.runtime.autoSync: true`) re-render and roll onto the new effective spec. Pinned services (`autoSync: false`) keep rendering their pinned snapshot and report the change through the `RuntimeDrifted` condition instead — see [Runtime Revisions and Pinning](/ome/docs/concepts/runtime-revision).
- **Runtime auto-selection does not resolve inheritance.** When an InferenceService omits `spec.runtime` and OME auto-selects a runtime for the model, each candidate is matched, scored, and rendered from its own spec as written. A runtime you rely on auto-selection for must itself carry everything it needs to match and serve — its `supportedModelFormats`, `modelSizeRange`, and workload spec — rather than inheriting them. (Profiles are excluded from auto-selection anyway, since they are disabled.)
