---
title: OMENative overview
description: "OMENative runs each replica of an InferenceService component as an Instance, one pod or a leader and its workers, that OME creates, updates, repairs, moves and removes as one unit. Multi-node engines and decoders use it by default, and any component can opt in."
since: v1.3
---

OMENative is OME's own way of running an [InferenceService](../serving/inference-services.md) component. A replica on OMENative is an [Instance](instances.md), one pod or a leader and its workers, that OME creates, updates, repairs, moves and removes as one unit. Multi-node engines and decoders run on OMENative by default, and any component can opt in. OMENative runs inside the OME manager and is on by default, so there's nothing extra to install.

## What OMENative does

A Deployment or a StatefulSet manages each pod on its own, so it can leave a replica that spans several pods half started, half updated or short of a worker. OMENative handles the Instance as a whole:

- It creates an Instance's pods together, and tells the pods their roles and their leader's address. With the PodGroup CRD and a gang-aware scheduler, the pods also schedule all at once.
- By default, an update brings the new Instance up and serving before the old one drains, even for a multi-node gang.
- By default, when a pod of a ready multi-pod Instance fails, OME recreates the whole Instance.
- A new Instance has a readiness deadline, 30 minutes by default. One that misses it fails, and OME rebuilds it or holds its revision, depending on the cause.
- A revision that fails for a reason of its own, such as an image that can't be pulled, is held after its retries. It stays held until a new revision replaces it or you release it.
- [Prefill and decode run as two components](../../guides/omenative/serve-a-prefill-decode-model.md), the engine and the decoder, each with its own InferenceReplica. A rollout group rolls them out together, by canary, blue-green or rolling update.
- You can move a ready Instance off a node, and OME starts its replacement first.
- When you delete the InferenceService, OME drains the Instances before it deletes their pods.
- An Instance's status shows its phase, revisions, pod counts and last failure.

A component's replica count, its `minReplicas` and `maxReplicas`, and its autoscaler all count Instances, not pods.

## Turn on OMENative

OME picks a component's mode from the InferenceService and its [serving runtime](../runtimes/serving-runtimes.md):

| To run on OMENative | Set |
| --- | --- |
| A multi-node engine or decoder | Nothing. A `leader` or `worker`, in the InferenceService or the runtime, makes it OMENative. |
| Every component, the router included | `spec.deploymentMode: OMENative` |
| One component | `ome.io/deploymentMode: OMENative` in the component's annotations, such as `spec.engine.annotations` |

A component with none of these runs as a Deployment. A component's annotation, in the InferenceService or its runtime, wins over `spec.deploymentMode`. [How OME resolves the mode](../architecture/deployment-modes.md#how-ome-resolves-the-mode) gives the full order, and [Serve a model on OMENative](../../guides/omenative/serve-a-model-on-omenative.md) walks through opting in.

A single-pod component that opts in, such as the router, gets held revisions, readiness deadlines, migration and per-Instance status. [Rollout groups](../rollouts-and-traffic/rollout-groups.md) and canaries need the mode set explicitly, with `spec.deploymentMode` or the component's own annotation in the InferenceService. A `leader` or `worker` alone doesn't count, and the webhook rejects the InferenceService.

## How OMENative runs a component

For an InferenceService's OMENative component, OME writes an [InferenceReplica](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceReplica) named `{isvc}-{component}`, owned by the InferenceService. The InferenceReplica holds the component's Instances, and records the versions of their pod template as revisions. OME keeps this replica's spec in line with the InferenceService, so make your changes in the InferenceService. With the chart's controller identity configured, the admission webhook rejects direct spec changes to these controller-owned replicas from other users. Run `kubectl get irep` to list InferenceReplicas with their desired, current, ready and available Instances, and `kubectl describe irep` to see their conditions. The InferenceService shows Instance events, including failures and held revisions.

An Instance isn't an object of its own: it's a row in the InferenceReplica's status, and its pods carry its index in the `ome.io/instance-index` label. For example, take an InferenceService named `chat` with `spec.deploymentMode: OMENative`. Its router has one Instance, and its engine has two, each a leader and one worker. OME runs these objects:

```text
InferenceService chat
├── InferenceReplica chat-engine
│   ├── Instance 0: pods chat-engine-0-leader-0, chat-engine-0-worker-0
│   └── Instance 1: pods chat-engine-1-leader-0, chat-engine-1-worker-0
└── InferenceReplica chat-router
    └── Instance 0: pod chat-router-0-default-0
```

OME also creates the component's Services, and a PodDisruptionBudget, an autoscaler, a PodMonitor or PodGroups when the component and the cluster call for them. [What OME creates](../architecture/deployment-modes.md#what-ome-creates) lists every object.

### Standalone InferenceReplicas {#standalone-inferencereplicas since=v1.3}

For a complete CPU example, follow [Run a standalone InferenceReplica](../../guides/omenative/run-a-standalone-replica.md), including its user-managed Service, scaling and template update.

You can also create an InferenceReplica directly. Use this form when you want to manage one pod set and its Instance lifecycle yourself. Use an InferenceService when OME should compose the engine, decoder and router, coordinate their rollout groups, and expose the service.

A standalone replica has no `spec.parentRef` and no InferenceService owner reference. You choose its name and write its spec, subject to Kubernetes RBAC and admission checks. Set `spec.component` to `engine`, `decoder` or `router` at creation; the role cannot change afterwards. Give it a name distinct from existing InferenceServices and their component workloads in the namespace.

Choose one template source:

| Source | What to set | What OME runs |
| --- | --- | --- |
| Pod templates you supply | `spec.runners` | One `default` runner for a single-pod Instance, or `leader` and `worker` runners for a multi-pod Instance |
| A runtime, optionally with a model | `spec.runtimeRef`, optionally `spec.modelRef` | The runtime's configuration for the selected component; without a model reference, the runtime handles its own weights |
| A model with automatic runtime selection | `spec.modelRef` | A matching runtime that declares the selected component |

Do not combine `runners` with either reference. Reference-based replicas follow the live runtime; pinned runtime references are rejected. Model and runtime resources must exist and satisfy the same storage and rendering requirements as when an InferenceService uses them.

The standalone form has a smaller orchestration scope:

- Change `spec.replicas` to set the Instance count. An omitted count or `0` runs one Instance; standalone replicas do not support scale-to-zero. An external HPA or KEDA ScaledObject can target the scale subresource, but OME does not create that scaler from the replica's `autoscaler` block; `HPA` and `KEDA` classes in that block are rejected.
- Put the availability delay in `spec.minReadySeconds`, not `spec.lifecycle.minReadySeconds`. Do not assume InferenceService deployment defaults apply to a standalone replica's lifecycle policies.
- Manual Instance migration is not supported on standalone replicas. Use an InferenceService for the migration workflows in these docs.
- Read status and events from the InferenceReplica itself. The `kubectl ome` workflows that take an InferenceService name do not automatically apply to a standalone replica.

These capabilities are on the v1.3 development line and are not part of the v1.2.2 installation. Use the [source-build installation](../../getting-started/install.md#install-from-source) for the matching controller and CRDs.

## Scheduling and placement

A multi-pod Instance serves only when all its pods run, so its pods should schedule together or not at all. When the scheduler-plugins PodGroup CRD is installed, OME creates a PodGroup for each multi-pod Instance. The default Kubernetes scheduler ignores PodGroups, and OME doesn't set `schedulerName`. Set it on the component, or `spec.schedulerName` on the runtime, to a gang-aware scheduler. Without the CRD, the pods can schedule partially, and the InferenceReplica's `GangSchedulingUnavailable` condition is `True`. See [Gang scheduling](../serving/gang-scheduling.md).

- [The OME scheduler](../scheduling/ome-scheduler.md) is an alpha, gang-aware second scheduler, and runs only on Kubernetes 1.35. It places an opted-in OMENative gang in one accelerator domain, all or nothing.
- [Alfred](../scheduling/alfred.md), OME's alpha GPU cluster caretaker, recommends Instance moves when free GPUs fragment or a node turns unhealthy and, when you [let it](../../guides/scheduling/let-alfred-migrate-instances.md), asks OMENative to carry them out.
- On the engine or the decoder, `topologyKey` keeps an Instance's workers in its leader's topology domain, such as one NVLink or RDMA fabric. `topologySpread` spreads Instances across domains. See [Spread Instances across fault domains](../../guides/omenative/spread-instances-across-fault-domains.md).
- When Kueue manages pods in the namespace, label the InferenceService or the component with `kueue.x-k8s.io/queue-name`. OMENative ignores the `ome.io/dedicated-ai-cluster` annotation, so set the label yourself. Kueue holds the pods until it admits them, and the wait doesn't count against the readiness deadline. See [Set accelerator quotas](../../guides/operate-ome/accelerator-quota.md).

## What OMENative manages

All of these act on whole Instances. Per-component settings go in the component's `lifecycle` block, in the InferenceService or the runtime: `spec.engine.lifecycle` or `engineConfig.lifecycle` for the engine. The InferenceService's values win, and only OMENative components read the block. Cluster-wide settings go in the `lifecycle` entry of the `inferenceservice-config` ConfigMap, which the `ome-resources` chart writes from `ome.controller.lifecycle`.

| Feature | What OMENative does | Set or run |
| --- | --- | --- |
| [Updates](../architecture/omenative-update-strategies.md) | Moves Instances to a new revision when the pod template changes, or, by default, when the [runtime](../runtimes/runtime-revisions.md) does. `SurgeThenDrain`, the default, needs room for the extra pods. | `lifecycle.updateStrategy`, and `lifecycle.minReadySeconds` to [pace rollouts](../../guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md) |
| [Rollout groups](../rollouts-and-traffic/rollout-groups.md) | Rolls a change out across components together, by canary, blue-green or rolling update. A canary's share of requests follows its share of ready Instances. | `spec.rollout.groups`: see [Canary progression](../../reference/rollouts/canary-progression.md), [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md) and [Repin a drifted rollout plan](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md) |
| [Pause](../../guides/roll-out-changes/pause-and-resume-a-rollout.md) | Holds updates, new Instances and migrations on the InferenceService until you resume it. | [`kubectl ome rollout pause`](../../reference/kubectl-ome/rollout.md) (alpha) |
| [Restarts](instance-restart-policy.md) | By default, recreates a ready multi-pod Instance whole when one of its pods fails, and leaves a single-pod Instance's container restarts to the kubelet. | `lifecycle.restartPolicy` |
| [Readiness deadlines](../../guides/omenative/set-instance-readiness-deadlines.md) | Fails a new or recreated Instance that isn't ready in time. | `lifecycle.instanceReadyTimeout`, 30 minutes by default |
| [Held revisions](../../guides/roll-out-changes/release-a-held-revision.md) | Retries a revision that fails for a reason of its own, then holds it until you release it or replace it. Without `updateRetry`, the first failure holds it. | Cluster-wide `updateRetry`, three attempts with backoff in the chart, and `kubectl ome instance release-held` (alpha) |
| [Failed Instances](instances.md) | Retries a failed update, and a failed create when the Instance hasn't run the target revision. Otherwise, and after a failed restart, it stays `Failed` until its pods are ready. | The `ome.io/reset-instances` annotation on the InferenceReplica: see [Reset failed Instances](../../guides/omenative/reset-failed-instances.md) |
| [Migration](migration-and-transient-scale.md) | Moves a ready Instance off a node, and starts the replacement before it removes the original. Holds requests until the cluster-wide `audit` caps are set, as the chart does. | [`kubectl ome migration start`](../../reference/kubectl-ome/migration.md) (alpha): see [Request an Instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md) |
| [Transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md) | Changes the Instance count until OME or the autoscaler sets it again. | [`kubectl ome scale`](../../reference/kubectl-ome/scale.md) (alpha) |
| [Deletion](../../guides/omenative/recover-stuck-deletions.md) | Drains Instances before it deletes their pods, on scale-down and delete. It can force-delete pods stuck on an unreachable node, or let the InferenceReplica go after a deadline. | Cluster-wide `forceDelete`, off by default, and `teardown.deadline`, 30 minutes in the chart |
| [Status](../architecture/deployment-modes.md#observe-omenative-in-status) | Reports per-Instance phase, revisions, pod counts and last failure, and copies the totals and conditions to `status.components.<component>.lifecycle` on the InferenceService. | [`kubectl ome instance`](../../reference/kubectl-ome/instance.md), and the [status encoding](../../guides/omenative/change-the-status-encoding.md) for large fleets |

## OMENative and LeaderWorkerSet

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md).

OME runs a LeaderWorkerSet only for an engine or a decoder whose own `ome.io/deploymentMode` annotation is `MultiNode`. A component that moves to OMENative gains everything in [What OMENative manages](#what-omenative-manages), and a PodDisruptionBudget and an autoscaler, which a `MultiNode` component doesn't get. Once no component uses `MultiNode`, you can [uninstall LeaderWorkerSet](../../guides/omenative/move-from-leaderworkerset.md#uninstall-leaderworkerset).

When you upgrade from v1.2.2, each multi-node component without an `ome.io/deploymentMode` annotation of its own moves to OMENative, with new pods. OME leaves the old LeaderWorkerSet running on its GPUs, so the new pods can stay `Pending` until you delete it, and the component serves nothing until they're ready. A runtime that uses the `LWS_*` variables, as the catalog's `srt-deepseek-rdma`, `srt-deepseek-rdma-pd` and `srt-kimi-k2-pd` do, needs [OME's variables](../architecture/deployment-modes.md#pod-labels-and-environment) instead. To move on your own schedule, [pin your components to `MultiNode`](../../guides/omenative/move-from-leaderworkerset.md#pin-a-component-to-multinode) before you upgrade.

## Next steps

- [Serve a model on OMENative](../../guides/omenative/serve-a-model-on-omenative.md): opt a single-pod InferenceService in, and see what OME creates.
- [Serve a multi-node model](../../guides/omenative/serve-a-multi-node-model.md): run a leader and its workers as one Instance.
- [Instances](instances.md): an Instance's phases and operations, and what OME does when one fails.
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md#instances-fail-or-never-become-ready): Instances that fail or never become ready.
