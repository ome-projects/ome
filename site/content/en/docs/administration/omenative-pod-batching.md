---
title: "OMENative Scale Batching"
linkTitle: "Scale Batching"
weight: 13
date: 2026-09-30
description: >
  Pace bulk OMENative pod creation and deletion with the
  lifecycle.scaleUpPodBatchSize and scaleDownPodBatchSize budgets, and tune
  the scaleDownRequeueInterval polling cadence for in-flight scale-down work.
---

When an [OMENative](/ome/docs/concepts/omenative/) component gains or loses
many Instances at once — a large scale-up, a scale-down, or the teardown of a
deleting InferenceReplica — the controller does not have to issue every pod
create or delete in a single reconcile pass. Three keys in the `lifecycle`
block of the `inferenceservice-config` ConfigMap pace that work into
**per-pass waves**:

| Key | Unit | What it bounds | Chart default |
|-----|------|----------------|---------------|
| `scaleUpPodBatchSize` | Pods | Missing pods selected for creation in one pass. | `100` |
| `scaleDownPodBatchSize` | Pod-equivalents | Instances actively being drained and deleted at once. | `100` |
| `scaleDownRequeueInterval` | duration | The polling cadence while scale-down work is in flight. | `5s` |

This is one specific concern: **how fast bulk scale operations are allowed to
move**. It is distinct from rollout pacing (`maxUnavailable` in
[update strategies](/ome/docs/concepts/omenative-update-strategies/) bounds
how many Instances an update may take down), from
[readiness deadlines](/ome/docs/administration/instance-readiness-deadlines/)
(how long one attempt may run), and from
[stuck-deletion recovery](/ome/docs/administration/stuck-deletion-recovery/)
(what happens when a delete wedges). All three keys apply only to
OMENative-mode components — `RawDeployment` and `MultiNode` components
delegate pod pacing to the Deployment and LeaderWorkerSet controllers.

## Where the configuration lives

The keys sit in the same `lifecycle` JSON document as the other lifecycle
settings, in the `inferenceservice-config` ConfigMap (OME controller
namespace, `ome` by default). With the `ome-resources` Helm chart, set them
under `ome.controller.lifecycle`:

```yaml
# charts/ome-resources values
ome:
  controller:
    lifecycle:
      scaleUpPodBatchSize: 100
      scaleDownPodBatchSize: 100
      scaleDownRequeueInterval: 5s
```

The rendered ConfigMap entry (other configured `lifecycle` fields merge into
the same document; only the three keys of this page are shown):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: ome
data:
  lifecycle: |-
    {
      "scaleUpPodBatchSize": 100,
      "scaleDownPodBatchSize": 100,
      "scaleDownRequeueInterval": "5s"
    }
```

Unlike the per-pass lifecycle settings (`forceDelete`, `teardown`, the
readiness deadlines), these three values are read **once at manager startup**
and are immutable for the process lifetime — the short-TTL config cache does
not apply. Changing them requires a manager restart; the Helm chart handles
that automatically, because the manager Deployment carries a checksum of the
rendered ConfigMap and rolls every replica when it changes.

Validation is strict, at startup: a zero or negative batch size, or a
malformed, zero, or negative duration, is a **startup error** — the manager
logs `Failed to initialize lifecycle scale configuration` and exits rather
than running with a nonsensical budget.

The OME binary has **no built-in values**. Omitting a batch-size key
preserves that direction's unbounded compatibility behavior: every eligible
Instance is handled in one pass, exactly as before batching existed.
Omitting `scaleDownRequeueInterval` disables only the cadence polling —
watches and configured deadlines still drive progress (see below).

## The scale-up budget: `scaleUpPodBatchSize`

The create pass walks desired Instances in order and selects the ones whose
pods are missing. `scaleUpPodBatchSize` bounds how many missing **pods** one
pass may select:

- **Instances are indivisible.** A single-pod Instance costs 1; a leader plus
  its workers is one gang and all of its missing pods are selected together,
  or the Instance is deferred to a later wave. The budget never splits a
  gang.
- **The wave is a stable prefix.** Once an eligible Instance does not fit in
  the remaining budget, selection closes: smaller Instances later in the
  order do not jump the queue. The Instance that did not fit leads the next
  wave instead of being starved.
- **The first Instance may exceed the budget.** An indivisible gang larger
  than the whole budget is admitted alone when it is the first selection of
  the wave — a budget of 8 does not deadlock a 16-pod gang; it serializes it.
- **Intent is committed before effect.** Each selected Instance's durable
  `Creating` status entry is committed before any of its pods is created, so
  a controller restart mid-wave resumes from durable intent.
- **Only pod creation consumes the budget.** Promoting already-created
  Instances to `Ready` is free, so a wave of creations never delays readiness
  bookkeeping for the previous wave.

Between waves, progress is driven by pod watch events and the OMENative
dispatcher cadence (`lifecycle.requeue.operation`, chart default `5s`) — the
scale-down polling interval below plays no part in scale-up.

## The scale-down budget: `scaleDownPodBatchSize`

Scale-down (fewer desired Instances, and the teardown of a deleting
InferenceReplica, which reconciles through the same pipeline) is paced in
**Pod-equivalent units**: an Instance costs its observed live and Terminating
pods, with a floor of **1** for an Instance whose pods are already gone but
whose status row and per-Instance resources (such as a stale PodGroup) still
need cleanup work. Podless bookkeeping therefore consumes budget too — a
mass teardown cannot flood the apiserver with cleanup either.

Selection follows the same shape as scale-up — Instances (and gangs) are
indivisible, the wave is a stable prefix, and a first candidate larger than
the whole budget proceeds alone (an *oversized* wave) — with two additional
rules:

- **Owned work resumes first.** Instances already admitted to a delete wave
  (`Deleting`, owned by the Delete operation) are resumed — oldest operation
  first — before any newly extra Instance is admitted. Fresh candidates wait
  until in-flight destructive work completes, so shrinking the target again
  mid-wave cannot multiply concurrent drains past the budget.
- **Waves are durable and admission is effect-free.** A fresh wave is
  stamped `Deleting` in one status transaction and takes no external effect
  in that same pass; drains and pod deletes run on later passes from the
  authoritative pod snapshot. Fresh extras are selected highest index first.

Everything inside a wave keeps its ordinary semantics: serving holds land on
every member pod before any pod is deleted, drains are checked per pod, and
deletes carry UID preconditions.

## The polling cadence: `scaleDownRequeueInterval`

While destructive work remains in flight — drains pending, pods
Terminating, per-Instance cleanup outstanding — the controller wakes itself
up on this interval to re-check progress. It is a **backstop cadence**, not
the only driver: the controller also watches its own pods, the
EndpointSlices of the drain Services, owned PodGroups, and the
InferenceReplica itself, so most progress is event-driven and the poll only
catches what no event announces.

Omitting the key disables the cadence polling and nothing else. Configured
[force-delete and teardown deadlines](/ome/docs/administration/stuck-deletion-recovery/)
still schedule their **exact** wake-ups — a stuck-Terminating escalation
fires when due whether or not a poll interval is configured.

## What you will see

Every scale-down pass logs one summary line in the manager log:

```text
OMENative scale-down wave  namespace=prod isvc=chat component=engine
  podBudget=100 activePodCost=96 activeInstances=48 admittedInstances=48
  deferredInstances=152
```

`podBudget` reports `unbounded` when no budget is configured;
`deferredInstances` is the count of eligible Instances waiting behind the
budget (including fresh extras blocked behind owned work) — a persistently
high value with a saturated `activePodCost` means the budget is the
throughput limit.

The scale-down side also exports metrics:

| Metric | Type | Meaning |
|--------|------|---------|
| `ome_omenative_scale_down_batch_pods` | histogram | Pod-equivalent cost selected in one scale-down wave, by component. |
| `ome_omenative_scale_down_active_pods` | gauge | Current Pod-equivalent cost selected for scale-down, by namespace, ISVC, and component. |
| `ome_omenative_scale_down_deferred_instances` | gauge | Eligible Instances outside the active wave, by namespace, ISVC, and component. |
| `ome_omenative_scale_down_oversized_batch_total` | counter | Waves that admitted one indivisible Instance above the budget, by component. |

Scale-up has no dedicated wave metric; its progress is visible through the
per-Instance `Creating` → `Ready` transitions in the InferenceReplica status.
