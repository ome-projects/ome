---
title: Alfred
description: "Alfred, OME's alpha GPU cluster caretaker, recommends Instance moves when free GPUs fragment or a node turns unhealthy and, when you let it, asks OMENative to carry them out."
since: v1.3
---

Alfred, OME's GPU cluster caretaker, finds the [Instances](../omenative/instances.md) to move when your free GPUs fragment across nodes or a node's GPUs fail. It recommends each move and, when you let it, asks [OMENative](../omenative/overview.md) to migrate the Instance.

Alfred is alpha. You turn it on by installing its own Helm chart, `ome-alfred`, next to OME. It starts in recommend-only mode, which records recommendations and changes no workload. [Execute mode](#recommend-only-and-execute-modes), in which Alfred requests migrations, is a further opt-in. OME's releases don't publish an Alfred image, so you build one first, as [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md) shows.

## What Alfred looks for

A pod gets all of its GPUs from one node, so a cluster can have enough free GPUs in total and still leave a large replica's pods `Pending`. Alfred watches your GPU nodes, the pods on them and your [InferenceServices](../serving/inference-services.md), in every namespace, for two problems:

- **Fragmented GPUs.** The free GPUs of a GPU pool, a group of nodes with the same kind of GPU, are scattered in pieces too small for larger replicas. Alfred counts the `nvidia.com/gpu`, `nvidia.com/mig-*`, `amd.com/gpu` and `gpu.intel.com/*` resources that pods hold, whether OME created the pods or not.
- **Unhealthy nodes.** A node condition reports failed GPUs: `GpuUnhealthy` by default. OME doesn't set this condition. Your GPU health checks must set it, or you name the conditions they set in `alfredConfig.policies.nodeHealth.triggerConditions`.

Alfred also finds Instances to move off nodes that you mark for planned maintenance, with a label, taint or condition that you configure. [Alfred policies](alfred-policies.md) explains how it picks the moves.

## How Alfred decides

The leader, one of Alfred's replicas, runs a decision pass every 5 minutes by default, and early when a node's conditions or maintenance triggers change. In each pass, the policies propose moves, and Alfred records each move with an outcome that shows how far it got. [`kubectl ome admin recommendations`](../../reference/kubectl-ome/admin.md) prints them.

| Stage | What happens | Outcome |
| --- | --- | --- |
| Propose | The node-health and defragmentation policies propose moves. A move that Alfred can't make, or doesn't make in this mode, is advice with a reason. | `advisory` |
| Admit | In execute mode, the arbiter checks each move against the [safety bounds](alfred-policies.md#safety-bounds), node-health moves first. | `rejected` |
| Request | Alfred checks the admitted moves again against a fresh look at the cluster and a simulation, and requests the first one that passes. It holds back the others. | `submitted` or `withheld` |
| Follow | Alfred follows the request until OMENative reports a result. | `acknowledged`, `completed`, `failed` or `stalled` |

Alfred requests one migration at a time, and none while any migration in the cluster is waiting or in progress, including one that you start with `kubectl ome migration start`. By default, it also stops admitting moves once 10 migrations have started in the past hour, whoever requested them.

## Recommend-only and execute modes

`alfredConfig.mode` in the chart's values sets the mode: `recommend-only`, the default, or `execute`. What Alfred can do for OMENative Instances depends on what else you set. Each row adds to the one before it:

| You set | Recommendations for OMENative Instances | Migrations |
| --- | --- | --- |
| Nothing: the chart's defaults | Advice with no target nodes. Most have the reason `OMENativeUnavailable`. | None |
| Simulation workers, in `simulation.configMapName` and `alfredConfig.scheduling.profiles` | The same advice, plus where the scheduler would place the replacement pods | None |
| `migration.apiVersion: v1` | Planned moves, with target nodes and predicted placements, as advice | None |
| `alfredConfig.mode: execute` | Planned moves, which the arbiter admits or rejects | Alfred requests the admitted moves |

Until you set `migration.apiVersion`, Alfred also records an `OMENativeUnavailable` Warning event on its `alfred-config` ConfigMap, and the `alfred_omenative_unavailable` metric is 1. Both are expected.

In recommend-only mode, new moves are `advisory`. Requests left in the dispatch journal from execute mode keep their status, and a `prepared` one shows as `withheld` with `ExecutionDisabled`. The recommendations show what the policies propose, within their own [safety bounds](alfred-policies.md#safety-bounds) but before the arbiter's caps and cooldowns. [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md) installs Alfred and reads its recommendations.

Simulation replays the scheduler that your pods name against a copy of the cluster, to predict where the replacement pods would land. A predicted placement is a hint, not a reservation. Simulation and execute mode need more from your cluster:

- The PodGroup CRD, `scheduling.x-k8s.io/v1alpha1`, which the simulation reads even when no pod uses a PodGroup. No OME chart installs it: see [Gang scheduling](../serving/gang-scheduling.md#when-the-podgroup-crd-is-missing).
- Kubernetes 1.30 or newer once you set `migration.apiVersion`, for the migration guard that the chart then installs.
- The [OME scheduler](ome-scheduler.md), for an Instance of more than one pod. Alfred simulates such an Instance as a gang, so its pods must name the OME scheduler, which is alpha and needs Kubernetes 1.35.

[Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md) turns on execute mode step by step. Its [troubleshooting](../../guides/scheduling/let-alfred-migrate-instances.md#troubleshooting) shows what you see when a requirement is missing.

## Alfred and OMENative

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md).

Alfred migrates only Instances of OMENative components, because OMENative can move one Instance safely: it creates a replacement Instance before it drains the original. See [Migration and transient scale](../omenative/migration-and-transient-scale.md).

| Component | What Alfred does |
| --- | --- |
| OMENative | Recommends moves, and requests them in execute mode |
| RawDeployment | Advises, with the reason `RawDeploymentMigrationUnsupported` |
| `MultiNode` (deprecated) | Advises moving the component to OMENative, with the reason `LWSMigrationUnsupported` |

Alfred can move an OMENative Instance when:

- the component is steady: every Instance is ready and up to date, with no rollout, pause or migration in progress;
- the component's migration policy isn't `Never`;
- the model can go elsewhere: a model on a ReadWriteOnce or ReadWriteOncePod volume pins the Instance to its node;
- the replacement fits on other nodes while the original still holds its GPUs.

Otherwise Alfred only advises. [Which Instances Alfred can move](alfred-policies.md#which-instances-alfred-can-move) lists the conditions and their reasons.

To keep Alfred from moving an InferenceService's Instances, annotate it `alfred.ome.io/movable: "false"`. [Workload annotations](../../reference/scheduling/alfred-configuration.md#workload-annotations) lists the other annotations that Alfred reads.

Alfred reads each component's deployment mode from the InferenceService, not from its runtime. A component that's OMENative only through its runtime looks like a RawDeployment to Alfred. To let Alfred move it, declare OMENative in the InferenceService itself, as [Opt in to OMENative](../architecture/deployment-modes.md#opt-in-to-omenative) shows.

## What Alfred changes in your cluster

Alfred writes only these objects:

| Object | When | What it's for |
| --- | --- | --- |
| The ConfigMap `alfred-recommendations` | Either mode | The latest pass: the recommendations and their outcomes, and the nodes with a health concern or planned maintenance. [`kubectl ome admin recommendations`](../../reference/kubectl-ome/admin.md) prints it. |
| Events on InferenceServices, nodes and the `alfred-config` ConfigMap | Either mode | Recommendations, migrations, node health and configuration problems. [Alfred metrics and events](../../reference/scheduling/alfred-metrics-and-events.md) lists them. |
| The Lease `alfred.ome.io` | Either mode | Leader election. |
| The annotation `ome.io/migration-request-v1-<uuid>` on an InferenceService | Execute mode | A migration request, with the Instance, the node to leave and up to 3 preferred target nodes. `kubectl ome migration start` writes the same request: see [Request an instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md). |
| The ConfigMap `alfred-dispatch-state` | Execute mode | The dispatch journal, where Alfred follows its requests until OMENative reports a result. |

Don't delete or reset the dispatch journal: Alfred requests migrations only while it can read the journal. A request whose InferenceService or InferenceReplica is deleted or replaced before the migration ends blocks Alfred's migrations until you clear it: see [A request is stalled](../../guides/scheduling/let-alfred-migrate-instances.md#a-request-is-stalled).

When you set `migration.apiVersion`, the chart also installs the migration guard, the ValidatingAdmissionPolicy `ome-alfred-migration-writes`. It rejects any change that Alfred makes to an InferenceService beyond adding a migration request, and Alfred executes only while the guard is in place and unchanged.

Alfred never cordons or drains a node, and never evicts or deletes a pod. When OMENative migrates an Instance, its own controller creates the new pods and removes the old ones.

!!! warning "Drained doesn't mean empty"
    The `NodeDrainedForRepair` and `NodeDrainedForMaintenance` events mean only that no OME pod holds GPUs on the node. Other pods can still run there, so check the node before you take it down.

## High availability

The chart runs three Alfred replicas by default. They elect a leader through the Lease `alfred.ome.io`, and the `alfred_leader_status` metric is 1 on the leader. Every replica observes the cluster and serves metrics on port 8080. The leader alone runs decision passes and writes recommendations and migration requests. A new leader picks up the requests in the dispatch journal before it writes a new one.

Every replica reloads its configuration when the `alfred-config` ConfigMap changes. If the new configuration is invalid, Alfred records a `PolicyReloadFailed` event and keeps the last good one. See [Hot reload](../../reference/scheduling/alfred-configuration.md#hot-reload).

## Next steps

- [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md): install Alfred and read its recommendations.
- [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md): turn on execute mode.
- [Alfred policies](alfred-policies.md): how the node-health and defragmentation policies choose and score moves.
- [Alfred configuration](../../reference/scheduling/alfred-configuration.md): every chart value and policy setting.
- [Alfred metrics and events](../../reference/scheduling/alfred-metrics-and-events.md): what Alfred reports, and where.
- [Migration and transient scale](../omenative/migration-and-transient-scale.md): how OMENative carries out a migration.
