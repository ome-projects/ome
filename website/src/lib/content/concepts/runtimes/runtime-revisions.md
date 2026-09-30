---
title: Runtime revisions and pinning
description: Pin an InferenceService to a snapshot of its runtime, choose when runtime changes reach it, and roll back to an earlier snapshot.
---

Pinning lets you choose when a change to a shared [serving runtime](serving-runtimes.md) reaches an [InferenceService](../serving/inference-services.md). A pinned InferenceService serves from a revision, an immutable snapshot of the runtime's spec, until you roll it forward to the runtime's latest spec or back to an earlier revision. Use it to keep a production InferenceService on a known-good spec, or to roll a runtime change out one InferenceService at a time.

## Live and pinned runtimes

By default, `spec.runtime.autoSync` is `true`, and an InferenceService follows its runtime's live spec. When you change the runtime, OME rolls out pods rendered from the new spec, as the component's [deployment mode](../architecture/deployment-modes.md) does: in OMENative, with the InferenceService's [update strategy](../architecture/omenative-update-strategies.md). [When a runtime changes](serving-runtimes.md#when-a-runtime-changes) says how soon.

With `autoSync: false`, OME renders the pods from a revision instead. A revision holds only the runtime's spec: OME reads the model and the [accelerator class](accelerator-classes.md) as they are now. Only an InferenceService that names its runtime in `spec.runtime.name` can pin it.

| To | Set |
| --- | --- |
| [Pin](#how-pinning-works) an InferenceService to its runtime's current spec | `spec.runtime.autoSync: false` |
| [Roll forward](#roll-forward-to-the-latest-runtime) to the runtime's latest spec | The `ome.io/runtime-sync` annotation, to a new value |
| [Roll back](#pin-a-specific-revision), or use another InferenceService's revision | `spec.runtime.revision`, to the revision's name, with `autoSync: false` |
| [Follow the live runtime again](#stop-pinning) | `autoSync: true`, and no `spec.runtime.revision` |

## Pin an InferenceService {#how-pinning-works}

To try the examples, apply the `llama-3-2-3b-instruct` ClusterBaseModel from [`config/models/meta/Llama-3.2-3B-Instruct.yaml`](https://github.com/ome-projects/ome/blob/main/config/models/meta/Llama-3.2-3B-Instruct.yaml), with the [model agent](../../guides/operate-ome/model-agent.md) turned on and a Hugging Face token for the gated model, and wait until it's `Ready`. Then create the `llama-demo` namespace and apply `runtime.yaml`, the `sglang-llama-3-2-3b` ServingRuntime from [What's in a runtime](serving-runtimes.md#anatomy-of-a-runtime).

This InferenceService names the runtime and its kind, and sets `autoSync: false`:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-3b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-3b-instruct
  runtime:
    name: sglang-llama-3-2-3b
    kind: ServingRuntime
    autoSync: false
  engine:
    minReplicas: 1
    maxReplicas: 1
```

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-2-3b-instruct created
```

Print the revision that the InferenceService is pinned to, and its `RuntimeDrifted` condition:

```bash
kubectl get inferenceservice llama-3-2-3b-instruct -n llama-demo \
  -o 'custom-columns=NAME:.metadata.name,PINNED:.status.pinnedRevisionName,DRIFTED:.status.conditions[?(@.type=="RuntimeDrifted")].status,REASON:.status.conditions[?(@.type=="RuntimeDrifted")].reason'
```

```output
NAME                    PINNED                                      DRIFTED   REASON
llama-3-2-3b-instruct   r-llama-demo-sglang-llama-3-2-3b-590753e8   <none>    <none>
```

OME stored the runtime's live spec in a revision, and recorded its name in `status.pinnedRevisionName`. Revisions are ControllerRevisions in OME's namespace, `ome` in a default install. For a runtime that [inherits](runtime-inheritance.md) from another, the revision holds the merged spec. InferenceServices pinned to the same spec share a revision.

The revision's name depends on `spec.runtime.kind`:

| `spec.runtime.kind` | Revision name |
| --- | --- |
| `ClusterServingRuntime`, the default | `cr-<runtime>-<hash>` |
| `ServingRuntime` | `r-<namespace>-<runtime>-<hash>` |

Set `kind: ServingRuntime` when you name a ServingRuntime, or its revisions get `cr-` names; see [ServingRuntime and ClusterServingRuntime](serving-runtimes.md#servingruntime-and-clusterservingruntime).

## When the runtime changes

Change `--mem-frac=0.9` to `--mem-frac=0.85` in the `args` of `runtime.yaml`, and apply the file:

```bash
kubectl apply -f runtime.yaml
```

```output
servingruntime.ome.io/sglang-llama-3-2-3b configured
```

The live spec no longer matches the revision, so OME sets `RuntimeDrifted` to `True`, with the reason `RevisionMismatch`:

```bash
kubectl get inferenceservice llama-3-2-3b-instruct -n llama-demo \
  -o 'custom-columns=NAME:.metadata.name,PINNED:.status.pinnedRevisionName,DRIFTED:.status.conditions[?(@.type=="RuntimeDrifted")].status,REASON:.status.conditions[?(@.type=="RuntimeDrifted")].reason'
```

```output
NAME                    PINNED                                      DRIFTED   REASON
llama-3-2-3b-instruct   r-llama-demo-sglang-llama-3-2-3b-590753e8   True      RevisionMismatch
```

The condition's message says how to roll forward:

```bash
kubectl get inferenceservice llama-3-2-3b-instruct -n llama-demo \
  -o 'jsonpath={.status.conditions[?(@.type=="RuntimeDrifted")].message}'
```

```output
pinned r-llama-demo-sglang-llama-3-2-3b-590753e8, live spec differs; bump ome.io/runtime-sync annotation to advance
```

!!! warning "Drift pauses updates"
    While the InferenceService has drifted, OME leaves its serving workload as it is. The running pods keep serving. A change to the InferenceService, such as a new `minReplicas`, takes effect only after you roll forward, pin a revision or undo the change to the runtime. `RuntimeDrifted` doesn't affect `Ready`.

## Roll forward to the latest runtime

To roll forward, set the `ome.io/runtime-sync` annotation on the InferenceService to a new value:

```bash
kubectl annotate inferenceservice llama-3-2-3b-instruct -n llama-demo ome.io/runtime-sync=mem-frac-0-85 --overwrite
```

```output
inferenceservice.ome.io/llama-3-2-3b-instruct annotated
```

OME pins the InferenceService to a revision of the live spec, and removes `RuntimeDrifted`:

```bash
kubectl get inferenceservice llama-3-2-3b-instruct -n llama-demo \
  -o 'custom-columns=NAME:.metadata.name,PINNED:.status.pinnedRevisionName,DRIFTED:.status.conditions[?(@.type=="RuntimeDrifted")].status,REASON:.status.conditions[?(@.type=="RuntimeDrifted")].reason'
```

```output
NAME                    PINNED                                      DRIFTED   REASON
llama-3-2-3b-instruct   r-llama-demo-sglang-llama-3-2-3b-af9539ac   <none>    <none>
```

OME then rolls out pods rendered from the new revision.

Use a new value each time, such as a date or a ticket number: OME rolls forward once for each value, and records it in `status.lastRuntimeSyncToken`. A value that you set before you change the runtime waits, and rolls the InferenceService forward at the next change, without reporting drift first.

### kubectl ome runtime sync {since=v1.3}

!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.

`kubectl ome runtime sync llama-3-2-3b-instruct -n llama-demo` sets the annotation for you, after it checks that rolling forward is safe. For example, the InferenceService must have drifted with `RevisionMismatch`, and no rollout or earlier sync can be in progress.

To wait until OME has rolled forward, pass the request ID that the command prints to `kubectl ome wait llama-3-2-3b-instruct -n llama-demo --for=runtime-sync=acknowledged --request-id=3f0c9a52-8d1e-4b7a-9c65-2e4f1d7b8a90`. The new pods can still be rolling out then. [`kubectl ome runtime sync`](../../reference/kubectl-ome/runtime.md#sync) lists every check, flag and exit code.

## Roll back to an earlier revision {#pin-a-specific-revision}

To roll back, or to use a revision that another InferenceService created, set `spec.runtime.revision` to the revision's name. OME stores a revision when it pins an InferenceService or rolls one forward, so pin an InferenceService before you change a runtime that you might need to roll back.

List the runtime's revisions, oldest first. This needs `list` on `controllerrevisions` in OME's namespace; see [Required RBAC](../../reference/kubectl-ome/overview.md#required-rbac). If OME runs in another namespace, use it in place of `ome`:

```bash
kubectl get controllerrevisions -n ome -l ome.io/runtime-of=sglang-llama-3-2-3b --sort-by=.metadata.creationTimestamp \
  -o 'custom-columns=NAME:.metadata.name,CREATED:.metadata.creationTimestamp'
```

```output
NAME                                        CREATED
r-llama-demo-sglang-llama-3-2-3b-590753e8   2026-09-28T14:15:57Z
r-llama-demo-sglang-llama-3-2-3b-af9539ac   2026-09-28T14:17:13Z
```

The `ome.io/runtime-of` [label](../../reference/api/labels-and-annotations.md#controllerrevision-labels) holds the runtime's name, so the list can also show revisions of same-named runtimes in other namespaces or at cluster scope. Use the ones with your runtime's prefix, here `r-llama-demo-sglang-llama-3-2-3b-`.

Since v1.3, `kubectl ome runtime history llama-3-2-3b-instruct -n llama-demo -o wide` lists the same revisions, newest first, and marks the one that the InferenceService uses. Keep `-o wide`: it prints the full names that `spec.runtime.revision` needs.

To roll back to the first spec, set `revision` under `spec.runtime`:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-3b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-3b-instruct
  runtime:
    name: sglang-llama-3-2-3b
    kind: ServingRuntime
    autoSync: false
    revision: r-llama-demo-sglang-llama-3-2-3b-590753e8
  engine:
    minReplicas: 1
    maxReplicas: 1
```

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-2-3b-instruct configured
```

OME pins the InferenceService to the revision, and rolls out pods rendered from it:

```bash
kubectl get inferenceservice llama-3-2-3b-instruct -n llama-demo \
  -o 'custom-columns=NAME:.metadata.name,PINNED:.status.pinnedRevisionName,DRIFTED:.status.conditions[?(@.type=="RuntimeDrifted")].status,REASON:.status.conditions[?(@.type=="RuntimeDrifted")].reason'
```

```output
NAME                    PINNED                                      DRIFTED   REASON
llama-3-2-3b-instruct   r-llama-demo-sglang-llama-3-2-3b-590753e8   <none>    <none>
```

While `revision` is set, the InferenceService stays on that revision, and changes to the runtime's spec don't make it drift. OME ignores `ome.io/runtime-sync` until you remove `revision`.

When you set `revision`, the admission webhook checks that `autoSync` is `false`, and that the name has the [form](#how-pinning-works) for the runtime. It doesn't check that the revision exists. When it rejects a change, kubectl prints `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:` and the reason. For a ClusterServingRuntime's revision with `kind: ServingRuntime`, the reason is:

```text
spec.runtime.revision "cr-sglang-llama-3-2-3b-590753e8" does not match the expected naming convention for runtime "sglang-llama-3-2-3b" (ServingRuntime); expected the form r-llama-demo-sglang-llama-3-2-3b-<8 lowercase hex chars>
```

When you remove `revision` and keep `autoSync: false`, the InferenceService stays on that revision, and OME compares it with the live runtime again.

### Garbage collection

OME keeps every revision that an InferenceService names in `spec.runtime.revision` or `status.pinnedRevisionName`, and the ten newest revisions of each runtime name. It deletes any other revision after a 24-hour grace period. To keep more revisions, or keep them longer, see [Tune runtime-revision garbage collection](../../guides/operate-ome/configure-the-controller.md#tune-runtime-revision-garbage-collection).

## Stop pinning

To follow the live runtime again, remove `spec.runtime.revision` if it's set, and set `autoSync: true` or remove it. OME renders the pods from the live spec from then on. `status.pinnedRevisionName` and any `RuntimeDrifted` condition keep their last values. If you set `autoSync: false` again, OME resumes from that revision, not from the live spec.

## The RuntimeDrifted condition

OME sets `RuntimeDrifted` only to `True`, with one of these reasons, and removes it when the reason no longer applies:

| Reason | Meaning | What to do |
| --- | --- | --- |
| `RevisionMismatch` | The runtime's live spec differs from the pinned revision. | [Roll forward](#roll-forward-to-the-latest-runtime), [pin a revision](#pin-a-specific-revision) or undo the change to the runtime. |
| `RevisionMissing` | Someone deleted the pinned revision, or `spec.runtime.revision` names one that doesn't exist. | [Pin a revision](#pin-a-specific-revision) that exists. Rolling forward doesn't clear this reason. |
| `RuntimeMismatch` | The revision in `spec.runtime.revision` belongs to a runtime with another name. | Pin a revision of this runtime. |
| `SourceRuntimeMissing` | Since v1.3. The runtime doesn't exist, and OME keeps serving from the revision. | Recreate the runtime. |

The first three reasons pause updates, as [When the runtime changes](#when-the-runtime-changes) describes.

When OME can't pin the InferenceService at all, it records a `RuntimePinError` warning event, leaves the serving workload as it is, and retries. That happens, for example, when the runtime's [inheritance chain](runtime-inheritance.md#chain-health-in-status) doesn't resolve, or when the runtime doesn't exist the first time OME pins the InferenceService. On v1.2.2, a missing runtime causes a `RuntimePinError` even after OME has pinned the InferenceService.

Since v1.3, the admission webhook also checks the live runtime of a pinned InferenceService. While the runtime is missing or disabled, it rejects every update to an InferenceService that has `spec.model` and an `engine` section. When the example's runtime is missing, the reason is:

```text
runtime sglang-llama-3-2-3b does not support model llama-3-2-3b-instruct: ServingRuntime sglang-llama-3-2-3b not found in namespace llama-demo
```

## Next steps

- [Serving runtimes](serving-runtimes.md): the fields of a runtime, and how OME selects one.
- [kubectl ome runtime](../../reference/kubectl-ome/runtime.md): show an InferenceService's effective runtime and revision history, and roll it forward.
- [ServingRuntimeRef](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-ServingRuntimeRef) and [InferenceServiceStatus](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceServiceStatus): the pinning fields in the API reference.
