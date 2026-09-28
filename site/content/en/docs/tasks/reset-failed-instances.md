---
title: "Reset Failed Instances"
linkTitle: "Reset Failed Instances"
weight: 21
date: 2026-09-27
description: >
  Tear down and rebuild OMENative Instances parked at `Phase=Failed` with the `ome.io/reset-instances` annotation
---

When a repair attempt against an [OMENative](/ome/docs/concepts/omenative)
Instance — a **Create** or **Restart** — runs past its deadline, the
escalation parks the Instance at `Phase=Failed` and **preserves the expired
operation** in `status.instanceStatuses[].operation`. The wreckage pods are
deliberately left in place for diagnosis, and that is exactly why the
Instance stays parked: with pods still present, no Create or Restart trigger
ever fires again, and the Instance sits Failed indefinitely.

Without this verb, the only exit is a pod-template edit — which mints a new
revision and rolls **every** Instance of the component. The
`ome.io/reset-instances` annotation is the targeted alternative: it names the
Failed Instances the operator wants torn down and rebuilt, and touches
nothing else.

There is no `kubectl ome` command for this verb — you write the annotation
onto the component's InferenceReplica directly. The annotation is a
**consumed mailbox** (like
[`ome.io/release-held-revision`](/ome/docs/tasks/release-a-held-revision)):
the controller answers the request on a later reconcile, then deletes the
annotation to acknowledge it.

## Before you begin

- An InferenceService with `spec.deploymentMode: OMENative` whose component
  reports one or more Instances at `Phase=Failed`.
- RBAC: `patch` on `inferencereplicas` (group `ome.io`) in the workload
  namespace, plus `get`/`list` to inspect status.
- The write passes the InferenceReplica admission webhook only because the
  object keeps its existing `ome.io/controller-write: "true"` annotation
  (`kubectl annotate` preserves it). Do not remove that annotation.

## Step 1: Find the parked Instances

List the service's InferenceReplicas and inspect the per-Instance status of
the affected component (`engine`, `decoder`, or `router`):

```bash
kubectl get inferencereplicas -n prod -l ome.io/inferenceservice=chat
kubectl get inferencereplica chat-engine -n prod -o yaml
```

A parked Instance is `Phase=Failed` **with a preserved operation** — the
deadline-expired attempt — and usually still counts wreckage pods:

```yaml
status:
  instanceStatuses:
  - index: 3
    incarnation: 2
    phase: Failed
    podCount: 2
    operation:
      id: op-3-2
      type: Restart          # or Create — the repair attempt that expired
      step: WaitReady
      startedAt: "2026-09-27T08:00:00Z"
      lastProgressAt: "2026-09-27T08:05:00Z"
      deadline: "2026-09-27T08:20:00Z"
    lastFailure:
      podName: chat-engine-3-0
      containerName: ome-container
      reason: OOMKilled
      exitCode: 137
      time: "2026-09-27T07:59:40Z"
```

Read `lastFailure` (and the wreckage pods' logs) **before** resetting: the
reset deletes the pods, and `lastFailure` is the trace that survives.

## Step 2: Request the reset

Annotate the InferenceReplica — not the InferenceService — with the
Instances to rebuild:

```bash
# One Instance
kubectl annotate inferencereplica chat-engine -n prod \
  ome.io/reset-instances=3

# Several Instances
kubectl annotate inferencereplica chat-engine -n prod \
  ome.io/reset-instances=3,7

# Every Failed Instance of the component
kubectl annotate inferencereplica chat-engine -n prod \
  ome.io/reset-instances=all
```

The value is either the literal `all` or a comma-separated list of
non-negative Instance indices (surrounding whitespace is tolerated,
duplicates are collapsed). Any other value — including an empty string or an
empty list entry — is malformed: the controller consumes it as a no-op and
emits a Warning `InstancesResetRejected` event naming the parse error.

One request at a time: a plain `kubectl annotate` refuses to overwrite an
existing annotation, and an annotation still present on the InferenceReplica
means the controller has not answered yet (or the last attempt hit an error
and will re-drive on the next reconcile). Wait for it to disappear before
submitting another request.

The annotation is excluded from the pod-template revision hash, so adding or
removing it never mints a new revision and never rolls the other Instances —
that is the point of the verb.

## What is eligible — and what is skipped

For each requested Instance the controller deletes every live pod not
already terminating and clears the preserved operation. `Phase` stays
`Failed` and `lastFailure` is preserved; once the pods finish terminating,
the ordinary Create pass recognizes the fresh-start shape (Failed, no
operation, no pods) and rebuilds the Instance.

A requested Instance is left untouched — and reported in an
`InstancesResetSkipped` event with the reason — in these cases:

| Skip reason | Meaning |
|-------------|---------|
| `no such instance` | The index matches no entry in `status.instanceStatuses`. |
| `Phase=<phase>` | The Instance is not `Failed`. Only Failed Instances are in scope; with `all`, non-Failed Instances are never selected in the first place. |
| `owned by Update` / `owned by Migrate` | The parked operation is an Update or Migrate continuation. Those belong to the rollout and migration machinery; clearing one here would orphan its surge marker or migration record. Only a preserved **Create** or **Restart** attempt (or no operation at all) may be cleared. |
| `still serving` | At least one of the Instance's live pods is still in the serving rotation (the controller-managed `ome.io/serving` readiness gate is `True`) — for example a surge source whose replacement failed and that keeps serving on its old pods. Draining serving capacity is the rollout machinery's job; the reset never removes it. |
| `nothing to reset` | The Instance has no pod to delete and no operation to clear — it is already in the rebuild shape. This is also where a crash-redelivered request lands harmlessly. |

A single request can mix outcomes: valid targets are reset, the rest are
skipped, each with its reason.

## Step 3: Watch the outcome

The controller's answer lands as events on the parent InferenceService (or
on the InferenceReplica itself if the parent is briefly unresolvable):

```bash
kubectl get events -n prod --field-selector reason=InstancesReset
kubectl get events -n prod --field-selector reason=InstancesResetSkipped
kubectl get events -n prod --field-selector reason=InstancesResetRejected
```

| Event | Type | Meaning |
|-------|------|---------|
| `InstancesReset` | Normal | Names the indices reset: pods deleted, preserved operation cleared, the lifecycle passes rebuild them. |
| `InstancesResetSkipped` | Normal | Lists each requested index left alone with its reason, or reports that an `all` request found no Failed Instance. |
| `InstancesResetRejected` | Warning | The annotation value was malformed; nothing was reset. |

Confirm the mailbox was consumed and the Instance is rebuilding:

```bash
# Empty once the controller has answered
kubectl get inferencereplica chat-engine -n prod \
  -o jsonpath='{.metadata.annotations.ome\.io/reset-instances}'

# Leaves Failed once the rebuild starts
kubectl get inferencereplica chat-engine -n prod \
  -o jsonpath='{.status.instanceStatuses[?(@.index==3)].phase}'
```

## What a reset does — and does not do

A reset is deliberately narrow:

- **It never edits spec.** No new revision is minted; the Instance rebuilds
  toward the component's current target revision.
- **It never removes serving capacity** — a still-serving Failed Instance is
  skipped, as above.
- **It does not override a Held retry block.** The rebuild is a fresh
  attempt at the target revision, and a revision whose retry block is
  **Held** in `status.retryBlocks` denies fresh attempts. If the escalation
  that parked the Instance also recorded a Held block for the target
  revision, [release the block](/ome/docs/tasks/release-a-held-revision) as
  well, or the reset Instance stays Failed with nothing rebuilding it. A
  block still in `Backoff` delays the rebuild until the backoff expires.
- **It is not the automatic restart policy.** Ordinary pod failures are
  handled by the restart trigger without any annotation; this verb exists
  only for the parked-Failed end state that the automatic paths deliberately
  do not touch.

The write ordering makes the verb crash-safe: pod deletions and the status
write commit **before** the annotation is deleted, so a controller crash in
between re-delivers the request into the `nothing to reset` branch and the
annotation is still cleaned up. A pod-delete or status-write error leaves
the annotation in place and the request re-drives on the next reconcile;
every step is idempotent on re-delivery.
