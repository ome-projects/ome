---
title: "Stuck-Deletion Recovery"
linkTitle: "Stuck-Deletion Recovery"
weight: 13
date: 2026-09-27
description: >
  Recover OMENative pods stuck Terminating on dead nodes with the opt-in
  lifecycle.forceDelete escalation, and bound how long a deleting
  InferenceReplica holds its teardown finalizer with lifecycle.teardown.
---

When an [OMENative](/ome/docs/concepts/omenative/) component scales down,
replaces an Instance during a rollout, or tears down because its
InferenceReplica is being deleted, the controller deletes its own pods and
waits for them to disappear before it declares the work done. That wait has a
failure mode Kubernetes builds in: a deleted pod object is only cleared from
the API when the kubelet on its node acknowledges the termination. If the node
is dead — powered off, partitioned, or removed entirely — no kubelet is left
to acknowledge anything, the pod stays **Terminating forever**, and everything
gated on its disappearance waits with it. A scale-down never completes; a
deleting InferenceReplica holds its finalizer indefinitely, and anything that
waits for the InferenceReplica object to disappear — a foreground cascading
delete, a terminating namespace — waits with it. (The parent InferenceService
is not among them: its controller releases its own finalizer immediately, so
under the default background cascade the InferenceService is already gone
before teardown of its InferenceReplicas even starts.)

This page covers the two operator-level controls for that situation, both in
the `lifecycle` block of the `inferenceservice-config` ConfigMap:

- **`lifecycle.forceDelete`** — an opt-in escalation that force-deletes a
  stuck-Terminating pod (grace period zero) once the node under it is provably
  unable to run its containers. This is the control that actually clears the
  wedge.
- **`lifecycle.teardown`** — a deadline on how long a deleting
  InferenceReplica may hold its teardown finalizer while draining. Past it,
  the controller warns and releases the finalizer to background garbage
  collection. This bounds the blast radius of a wedge on deletion, but does
  not remove the stuck pods themselves.

This is one specific concern: **recovery of wedged deletion**. Deadlines for
Instances that never become Ready (`instanceReadyTimeout`,
`stuckPodGracePeriod`, `unschedulableGracePeriod`) and recovery of running
pods that fail are separate mechanisms with their own configuration. Both
controls here apply only to OMENative-mode components — `RawDeployment` and
`MultiNode` components delegate pod lifecycle to the Deployment and
LeaderWorkerSet controllers.

## Where the configuration lives

Both blocks are JSON in the `lifecycle` key of the `inferenceservice-config`
ConfigMap (in the OME controller namespace, `ome` by default). With the
`ome-resources` Helm chart, set them under `ome.controller.lifecycle` and the
chart renders the key for you.

The chart's defaults differ deliberately:

- `forceDelete` ships **commented out** — the escalation is **disabled by
  default**, and there are **no in-code fallback values**. Force-deleting a
  pod is the one action here that can be wrong: if the node is merely slow
  rather than dead, removing the pod object while the containers still run
  risks two copies of the same Instance serving at once. Enabling it is a
  per-install decision.
- `teardown` ships **enabled with `deadline: 30m`**, so a chart-default
  install never wedges an InferenceReplica deletion for more than 30 minutes.
  Removing the block is the fully supported strict mode: the finalizer holds
  until cleanup is genuinely complete, with no deadline.

```yaml
# charts/ome-resources values
ome:
  controller:
    lifecycle:
      forceDelete:
        overdueSlack: 2m
        nodeUnreachableThreshold: 5m
      teardown:
        deadline: 30m
```

The rendered ConfigMap entry (the chart merges every configured `lifecycle`
field — batch sizes, `instanceReadyTimeout`, and so on — into this same JSON
document; only the two blocks of this page are shown):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: ome
data:
  lifecycle: |-
    {
      "forceDelete": {
        "overdueSlack": "2m",
        "nodeUnreachableThreshold": "5m"
      },
      "teardown": {
        "deadline": "30m"
      }
    }
```

Both blocks are resolved on every reconcile pass through a short-TTL config
cache (the manager's `--config-cache-ttl` flag, chart value
`ome.controller.configCacheTTL`, default `30s`), so a change takes effect
within the TTL **without a manager restart** — including for
InferenceReplicas already mid-teardown. That matters operationally: if a
deletion is wedged right now and nothing is configured, you can set
`lifecycle.forceDelete` (or a teardown deadline) and the very next passes pick
it up.

Invalid configuration always degrades to the safe default, never to fallback
numbers: an unparsable or non-positive `forceDelete` field disables the
escalation for that pass (logged at verbosity 1), and an invalid
`teardown.deadline` keeps the strict hold — with the parse error quoted in the
`TeardownBlocked` warning, so it cannot masquerade as "nothing configured".

## The force-delete escalation: `lifecycle.forceDelete`

Both fields are duration strings and both are **required** when the block is
present:

| Field | Description |
|-------|-------------|
| `overdueSlack` | How long past a Terminating pod's **own** `deletionTimestamp` the pod must be before it counts as wedged. The `deletionTimestamp` Kubernetes stamps already includes the pod's own `terminationGracePeriodSeconds`, so a pod with a 10-minute drain window gets its 10 minutes plus this slack, and a 30-second pod gets 30 seconds plus this slack — the escalation never races a healthy graceful shutdown. |
| `nodeUnreachableThreshold` | The minimum age of the node's unreachable evidence before the escalation may act: the `node.kubernetes.io/unreachable` taint's `timeAdded`, or the `NodeReady` `False`/`Unknown` condition's `lastTransitionTime`. Ages come from the node object itself, so the check is restart-safe — the controller persists no observation state. |

### When it fires

A Terminating pod is force-deleted only when **all** of the following hold:

1. The pod is past `deletionTimestamp + overdueSlack`.
2. The pod carries **no finalizers**. A force-delete cannot remove a
   finalizer-pinned object anyway, and OME never strips another controller's
   finalizer. Instead it emits a `PodDeleteBlockedByFinalizer` warning — once
   per pod UID, deduplicated across controller restarts — naming the
   finalizers so their owner can resolve them.
3. The node evidence, read **live from the apiserver** (never the informer
   cache), proves no kubelet can be running the containers:
   - the Node object is **gone** (deleted from the cluster), or
   - the node has carried the `node.kubernetes.io/unreachable` taint for at
     least `nodeUnreachableThreshold`, or
   - `NodeReady` has been `False` or `Unknown` for at least
     `nodeUnreachableThreshold`.

A current `NodeReady=True` **vetoes everything**, including a lingering
unreachable taint: the kubelet writes node status itself, so a dead node
cannot post `Ready=True` — and a Ready node means the kubelet is merely slow,
where force-deleting risks a double-running container. Evidence younger than
the threshold is a blip, not a death; the controller waits and re-evaluates
exactly when the threshold elapses.

### What it does

The delete is issued with **grace period zero** and a **UID precondition**.
OMENative reuses stable pod names, so the precondition guarantees a same-name
successor pod can never be hit; if the precondition misses, the pod is left
alone. After a successful delete the controller emits a `PodForceDeleted`
warning event naming the pod, node, evidence branch (`node-gone`,
`node-unreachable-taint`, or `node-not-ready`), and how far overdue the pod
was, and records the action in the component's audit ledger.

### Where it applies

The escalation runs in every OMENative path that waits on the controller's own
pod deletions:

- **scale-down** delete batches (fewer Instances desired),
- **rollout replacement**, where a new pod cannot take its stable name until
  the old one is gone,
- **migration**, where a source pod wedged Terminating on a dead node — often
  the very reason the migration was requested — would otherwise keep the
  migration from ever reaching a terminal phase,
- **teardown** of a deleting InferenceReplica (see below), which without the
  escalation waits forever on a dead node.

The same policy also lets rebuild paths reclaim a pod held in phase `Unknown`
by a dead node when nobody has requested its deletion yet. There is no
graceful-shutdown window to respect in that case, so the node evidence alone
decides; the event says "held in phase Unknown" instead of quoting an overdue
duration.

## Bounding teardown: `lifecycle.teardown`

Every InferenceReplica carries the `ome.io/ir-teardown` finalizer while it is
live. When the InferenceReplica is deleted — usually because its parent
InferenceService was deleted — the finalizer holds the object while the
controller reconciles "desired Instances = zero" through the same scale-down
pipeline as any other delete: drain, graceful pod delete, the
stuck-Terminating force-delete escalation when `lifecycle.forceDelete` is
configured, and audit. The finalizer lifts only when no owned component pods
remain and owned PodGroups are authoritatively gone.

`lifecycle.teardown` bounds that hold. The single field, `deadline`, is a
duration string and is **required** when the block is present:

- **Block absent** (the fully supported strict default): the finalizer holds
  until cleanup completes, with **no deadline**. While pods survive, the
  controller emits an aggregated `TeardownBlocked` warning on the
  InferenceReplica each pass — the message counts surviving pods and
  PodGroups, states that no deadline is configured, and includes the manual
  escape (below). Identical messages aggregate into one Event with a bumped
  count, not a stream of duplicates.
- **Block present**: the deadline is measured from the InferenceReplica's own
  `deletionTimestamp`. Once it passes with pods still surviving, the
  controller emits a `TeardownDeadlineExceeded` warning and **releases the
  finalizer**, degrading the remainder to plain background garbage
  collection. The pods keep their owner references throughout teardown, so GC
  still collects everything a live kubelet can collect.

The two controls are complementary, and the distinction matters: releasing the
finalizer unblocks the InferenceReplica (and whatever is waiting for it to
disappear), but background GC deletes pods gracefully too — a pod stuck
Terminating on a dead node **stays stuck** after the finalizer lifts. Only the
force-delete escalation (or a manual force delete) actually removes it. A
teardown deadline without `lifecycle.forceDelete` is therefore an escape
hatch for the API objects, not a cleanup of the wedged pods.

## What you will see

| Event reason | On | Meaning |
|--------------|----|---------|
| `DrainOverdue` | InferenceService (or the InferenceReplica when the parent is gone) | A deleting Instance's pods are past their drain deadline. Once per overdue episode, and diagnostic only — force-deleting stays gated on the configured policy. This is the signal you see when pods are wedged and `forceDelete` is unconfigured or not yet actionable. |
| `PodForceDeleted` | InferenceService (or InferenceReplica) | The escalation force-deleted a stuck pod; names the pod, node, evidence branch, and overdue duration. |
| `PodDeleteBlockedByFinalizer` | InferenceService (or InferenceReplica) | An overdue Terminating pod is pinned by another controller's finalizers; OME will not strip them. Once per pod UID. |
| `TeardownBlocked` | InferenceReplica | Teardown pods survive and no (valid) deadline is configured; the finalizer holds. Aggregated. |
| `TeardownDeadlineExceeded` | InferenceReplica | The configured deadline elapsed; the finalizer was released to background GC. |

Teardown warnings land on the InferenceReplica itself because the parent
InferenceService is typically already deleted by the time teardown runs; they
are mirrored to the controller log so log-aggregated views catch them without
the events stream:

```bash
kubectl get events -n <namespace> --field-selector involvedObject.kind=InferenceReplica
```

## Manual recovery

Both are standard Kubernetes escapes, and every teardown warning's message
includes the second one verbatim:

- **Force-delete a stuck pod by hand** — after confirming its node is really
  dead, not merely slow:

  ```bash
  kubectl delete pod <pod> -n <namespace> --grace-period=0 --force
  ```

- **Strip the teardown finalizer** to hand a wedged InferenceReplica deletion
  to background GC immediately:

  ```bash
  kubectl patch inferencereplica <name> -n <namespace> \
    --type=merge -p '{"metadata":{"finalizers":null}}'
  ```

  As with the automatic deadline, this unblocks the object graph but leaves
  any dead-node pods stuck Terminating until they are force-deleted or the
  node object is removed from the cluster.
