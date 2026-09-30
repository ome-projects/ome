---
title: Alfred policies
description: How Alfred's defragmentation and node-health policies find Instances to move, and how its arbiter and safety bounds decide which become recommendations or migration requests.
since: v1.3
---

[Alfred](alfred.md) runs two policies in its decision passes, 5 minutes apart by default. Defragmentation looks for moves that gather scattered free GPUs into blocks that a large Instance can use. Node health moves Instances off nodes whose GPUs have failed or that are due for planned work. Both are on by default. A move that Alfred can't make, or won't make in recommend-only mode, is advice with a reason.

Alfred is alpha, and it only moves OMENative Instances. It starts in recommend-only mode, where it changes no workload. In execute mode, a further opt-in that [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md) sets up, the [arbiter](#the-arbiter) picks which moves go ahead, within the [safety bounds](#safety-bounds). You set the policies under `alfredConfig` in the `ome-alfred` chart's values: see [Alfred configuration](../../reference/scheduling/alfred-configuration.md).

## Which Instances Alfred can move

Alfred reads a component's [deployment mode](../architecture/deployment-modes.md) from the InferenceService alone. RawDeployment Instances are always advice, with the reason `RawDeploymentMigrationUnsupported`. A component that's OMENative only through its serving runtime counts as a RawDeployment, so [declare OMENative in the InferenceService](../../guides/scheduling/run-alfred.md#an-omenative-component-shows-rawdeploymentmigrationunsupported). A `MultiNode` component gets `LWSMigrationUnsupported`.

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md).

An OMENative Instance can move when all of these hold. Otherwise its move is advice, with the reason shown:

| Condition | Reason when it fails |
| --- | --- |
| The chart sets `migration.apiVersion: v1`, which is empty by default and needs [simulation](#simulation). | `OMENativeUnavailable` |
| The InferenceService is movable, as it is by default: see [Per-workload annotations](#per-workload-annotations). | `OMENativeStateIneligible` |
| `omenativeMigrationEnabled` is `true`, the default. | `MigrationSurfaceDisabled` |
| The component's `lifecycle.migrationPolicy.mode`, its own or its runtime's, isn't `Never`. | `OMENativeStateIneligible` |
| The component is steady: no migration, pause, rollout or scaling under way, and all its Instances are ready and serving the current revision. | `OMENativeStateIneligible`, or `OMENativeObservationInvalid` |
| For defragmentation, the InferenceService is out of its [cooldown](#cooldowns). | `OMENativeStateIneligible` |
| Alfred knows where the model is, and it isn't on a ReadWriteOnce or ReadWriteOncePod volume. | `ModelUnresolved`, `VolumePinned` |
| Other nodes in the pool have room for the new pods while the old ones still run. | `NoSurgeHeadroom` |

OMENative moves an Instance by surge: it starts the new pods first, and removes the old ones once the new ones serve. See [Migration and transient scale](../omenative/migration-and-transient-scale.md).

## Defragmentation

A GPU cluster fragments as Instances come and go. Free GPUs end up a few to a node, so an Instance that needs a whole 8-GPU node stays `Pending` even when the cluster has more than 8 free. Moving a 2-GPU Instance off a half-empty node onto a fuller one can free the whole node. Defragmentation measures this damage and looks for such moves. `policies.defragmentation.enabled: false` turns it off.

Alfred scores hardware pools separately, since only nodes with the same kind of GPU can stand in for one another. Instances without GPUs stay put.

### How Alfred scores a pool

Alfred counts a pool's free GPUs on the nodes that could take a new pod. That leaves out nodes that are cordoned, quarantined by [node health](#health-states), due for maintenance or marked for scale-down by the cluster autoscaler. By default, it also leaves out [preemptible nodes](#spot-and-preemptible-nodes).

The score has two parts, and either can wake the policy alone:

- **Reclaimable fragmentation**: free GPUs that the Instance sizes the pool needs can't use now, but could once Alfred repacks the Instances it can move.
- **Pending pressure**: `Pending` GPU pods that fit nowhere now but would after that repack, weighted by how long they've waited.

`fragmentationThreshold`, 0.25 by default, decides what happens next:

- When the score is above it, Alfred plans moves, as [How Alfred picks a move](#how-alfred-picks-a-move) describes.
- When only the fragmentation that no move can fix is above it, the pool's OMENative Instances are advice with `NonExecutableObservedFragmentation`.
- Otherwise, the pool gets no recommendations.

Until you set `migration.apiVersion`, no Instance can move, so the score stays at 0 and only the second case can happen.

??? note "The score formulas"
    ```text
    slots(s)         = number of size-s Instances the pool's free GPUs can seat
    frag(s)          = 1 - slots(s) * s / free GPUs
    weight(s)        = (1 - lambda) * demand share(s) + lambda * prior(s)
    observed         = sum over the ladder of weight(s) * frag(s)
    best             = observed, recomputed after repacking the movable Instances
    reclaimable      = max(0, observed - best)
    urgency          = 1 - exp(-minutes pending / tau)
    pending pressure = 1 - product over repackable pending pods of (1 - urgency)
    score            = 1 - (1 - reclaimable) * (1 - pending pressure)
    ```

    The ladder sizes, prior, lambda and tau are the `scoring` settings in [Defragmentation policy](../../reference/scheduling/alfred-configuration.md#defragmentation-policy). The demand share counts the GPUs that running Instances use and `Pending` pods ask for.

### How Alfred picks a move

When a pool scores above the threshold, Alfred plans surge moves for the pool's OMENative Instances that can move:

1. **It picks target nodes.** Alfred tries the fullest nodes with room first. They must take new pods and have access to the model.
2. **It weighs benefit against cost.** The move must lower the pool's fragmentation by more than its cost. The cost depends on `policies.defragmentation.aggressiveness`, which changes nothing else: 0.225 for `conservative`, 0.15 for `balanced`, the default, and 0.075 for `aggressive`. A move that falls short is advice with `NonExecutableObservedFragmentation`.
3. **It boosts urgent moves.** A move scores double, and counts as an emergency, when it would seat a `Pending` pod that fits nowhere else and has waited longer than `emergencyPendingAgeMinutes`, 10 by default. The pod must be in the moved InferenceService's namespace, or belong to an InferenceService with the same `alfred.ome.io/tenant-group`. `allowCrossTenantOptimization: false` limits it to the namespace. By default, a move off a [preemptible node](#spot-and-preemptible-nodes) scores 1.25 times higher.

Apart from the emergency boost, defragmentation ignores namespaces: any Instance can make room for any other's. A move names up to 3 target nodes as hints, but the scheduler places the new pods, so execute mode [simulates](#simulation) the move first.

## Node health

The node-health policy moves Instances off nodes whose GPUs have failed, and off nodes you've marked for planned work. `policies.nodeHealth.enabled: false` turns it off. By default, a change to a node's conditions or maintenance triggers starts an extra decision pass (`earlyTickOn`).

An Instance with a pod on an `Unhealthy` node gets a move with the reason `NodeUnhealthy`, and one on a node due for maintenance gets `NodeMaintenance`. Alfred plans the same surge move as for defragmentation, but skips the threshold and the cost, and ignores [maintenance windows](#maintenance-windows). The [arbiter](#the-arbiter) considers these moves before defragmentation's.

### Health states

Alfred reads a node's health from the node conditions in `policies.nodeHealth.triggerConditions`, only `GpuUnhealthy` by default. OME doesn't set that condition: your GPU health checks must set it, or you name the conditions they do set. When conditions disagree, the worst state wins:

| State | When | What Alfred does |
| --- | --- | --- |
| `Unhealthy` | A trigger condition is `True`. | Moves the node's Instances off it, and places nothing on it. |
| `Unknown` | A trigger condition is `Unknown`, or `False` without a transition time. | Places nothing on it, and leaves its Instances where they are. |
| `Suspect` | A trigger condition turned `False` within `nodeSuspicionWindowMinutes`, 30 by default. | The same as `Unknown`, since a node that just recovered may fail again. |
| `Clear` | All trigger conditions have been `False` for longer than that, or the node has none. | Nothing. |

### Planned maintenance

`policies.nodeHealth.maintenance.triggers` marks nodes due for planned work, and is empty by default. A trigger has a unique `name` and matches nodes by one `condition`, `label` or `taint`:

```yaml
alfredConfig:
  policies:
    nodeHealth:
      maintenance:
        triggers:
          - name: os-patching
            label:
              key: maintenance.example.com/state
              value: patching
          - name: hardware-swap
            taint:
              key: maintenance.example.com/hardware-swap
              effect: NoSchedule
```

Alfred places nothing on a node that matches a trigger. Moves off such a node wait out the full [cooldowns](#cooldowns), unlike moves off `Unhealthy` nodes. [Maintenance triggers](../../reference/scheduling/alfred-configuration.md#maintenance-triggers) lists the fields.

### Signals and events

Alfred lists the nodes that aren't `Clear`, or are due for maintenance, in its recommendations, under `node.<name>` keys. It also emits events on the Node:

| Event | When |
| --- | --- |
| `NodeRepairNeeded` | The node turns `Unhealthy` or `Unknown`. It's a warning. |
| `NodeDrainedForRepair` | A node that needs repair has no OME pod with GPUs left. |
| `NodeMaintenanceRequested` | The node first matches a maintenance trigger. |
| `NodeDrainedForMaintenance` | A node due for maintenance has no OME pod with GPUs left. |

With `signalOnly: true`, the policy reports nodes and moves nothing. Use it when your own repair tooling drains nodes and acts on these events. [Alfred metrics and events](../../reference/scheduling/alfred-metrics-and-events.md#on-a-node) describes the events.

## Simulation

Alfred's plan counts GPUs, but the scheduler also weighs taints, affinity, topology spread and gangs. So Alfred asks a simulator where the new pods would land. The simulator runs the scheduler's code against a copy of the cluster. You set it up with the chart's `simulation.configMapName`, as [Set up scheduler simulation for Alfred](../../guides/scheduling/set-up-scheduler-simulation.md) shows.

Alfred simulates with the scheduler profile that the Instance's `schedulerName` names, or `default-scheduler` when it's empty. An Instance with more than one pod needs a profile that simulates gangs, like the one for the [OME scheduler](ome-scheduler.md), which is alpha and needs Kubernetes 1.35. When no profile fits, the recommendation names the problem, such as `GangUnsupported`, and execute mode withholds the move with `SourceUnsupported`.

Simulation also needs the PodGroup CRD, even when no pod uses PodGroups: see [Gang scheduling](../serving/gang-scheduling.md#when-the-podgroup-crd-is-missing).

### In recommend-only mode

Simulation works without `migration.apiVersion`. Alfred simulates up to 8 moves a pass, and records their [results](../../reference/scheduling/alfred-metrics-and-events.md#scheduling-results), `Feasible`, `Infeasible` or `Unsupported`, with the predicted pod placements. A prediction reserves no capacity.

Once you set `migration.apiVersion`, the policies plan moves and their target nodes, and Alfred makes the planned moves advice with `SimulationRecommendOnly`.

### In execute mode

In execute mode, just before Alfred submits a move, it looks at the cluster afresh, reruns the policies and the arbiter, and simulates the move, which must come out `Feasible`. It then checks that nothing the decision relied on has changed, and that the pods land on allowed, ready nodes outside their cooldown. Otherwise it withholds the request, with a reason such as `PredictedTargetUnsafe`. That's why `migration.apiVersion` needs simulation.

## The arbiter

In recommend-only mode, new moves are advice before they reach the arbiter, so their outcome is `advisory`. Requests left in Alfred's dispatch journal from execute mode keep their status, and a `prepared` one shows as `withheld` with `ExecutionDisabled`.

In execute mode, the arbiter considers the moves from both policies one at a time, in this order:

1. **By class**: moves off `Unhealthy` nodes, then off nodes due for maintenance, then defragmentation moves.
2. **By score**, highest first. A node-health move scores its `alfred.ome.io/priority` annotation, 0.5 by default. A defragmentation move scores its benefit minus cost, times any boost.
3. **By size**, smaller Instances first.

It admits a move that passes its checks: the [global limits](#global-limits) and [cooldowns](#cooldowns), one move per InferenceService and per target node in a pass, and room on a target node. The first check a move fails gives its rejection reason, and [Arbiter reasons](../../reference/scheduling/alfred-metrics-and-events.md#arbiter-reasons) lists them all.

Admission isn't submission. Alfred requests the first admitted move that passes the checks in [In execute mode](#in-execute-mode), and withholds the rest with the reasons in [Dispatch reasons](../../reference/scheduling/alfred-metrics-and-events.md#dispatch-reasons).

## Safety bounds

Maintenance windows, the spot rules and defragmentation's per-workload cooldown act inside the policies, so they shape recommendations in either mode. The global limits and the other cooldowns act in the arbiter and when Alfred submits a request, so they only hold back execute mode.

### Global limits

- **Caps**: `maxInFlightMigrations`, 3 by default, bounds the migrations in progress, and `maxMigrationsPerHour`, 10 by default, those started in the last hour. Both count everyone's migrations, not only Alfred's.
- **One at a time**: Alfred submits at most one request a pass, and none while any migration request in the cluster is waiting or in progress (`InFlightCap`).
- **Unresolved requests**: Alfred submits nothing while one of its own requests is unresolved (`UnresolvedRequest`). A request that OMENative doesn't pick up within `migration.acknowledgementTimeout`, 2 minutes by default, is marked stalled, but still counts until OMENative reports its end.
- **Failure backoff**: after one of Alfred's migrations fails or stalls, Alfred waits `migration.failureBackoff`, 5 minutes by default, before it submits another (`FailureBackoff`).
- **Circuit breaker**: when more than half of Alfred's recent migrations failed, it admits nothing for 60 minutes (`CircuitBreakerOpen`). A new leader starts the count afresh.

!!! warning "A stalled request can stop Alfred's migrations for good"
    Some stalled requests never resolve, because the result Alfred waits for can't come. That happens when the InferenceService or InferenceReplica behind the request is deleted or replaced before the migration ends (`OwnerUnavailable`, `ReplicaUnavailable` or `ReplicaIdentityChanged`). It also happens after an `AcknowledgementTimeout` when the InferenceService lacks Alfred's request annotation, or when a migration ends with a status Alfred can't read, such as `MigrationStatusInvalid`. The request stays in Alfred's dispatch journal, the ConfigMap `alfred-dispatch-state`, and Alfred withholds every later request with `UnresolvedRequest`. Upgrades, uninstalls and reinstalls keep the journal, so none of them clears it.

    To clear such a request, make sure it can't resolve, and remove its entry from the journal, as [A request is stalled](../../guides/scheduling/let-alfred-migrate-instances.md#a-request-is-stalled) shows.

### Cooldowns

| Setting | Default | What it holds back |
| --- | --- | --- |
| `perWorkloadCooldownMinutes` | 30 | Moves of an InferenceService within this long of its last migration's end, whoever requested it. `alfred.ome.io/cooldown-minutes` overrides it. |
| `recentPlacementCooldownMinutes` | 10 | Moves of an Instance whose pods started less than this long ago. |
| `perNodeCooldownMinutes` | 10 | Moves off or onto a node within this long of one of Alfred's own migrations that used it (`NodeCooldown`, `TargetCooldown`). |
| `policies.nodeHealth.healthCooldownFloorMinutes` | 5 | Replaces the first two for moves off `Unhealthy` nodes, which also skip the node cooldown on the node they leave. |

!!! warning "An invalid cooldown annotation stops all migrations"
    When it plans, Alfred ignores an `alfred.ome.io/cooldown-minutes` value that isn't a whole number of 0 or more, and uses the default. In execute mode, though, such a value on any InferenceService in the cluster withholds all migration requests, with the reason `InvalidCooldown`, until you fix or remove it.

### Maintenance windows

`maintenanceWindows` limits when defragmentation may move anything. A window lists `days`, from `Mon` to `Sun`, and a `start` and an `end` time in UTC. The start must come before the end, so a window can't cross midnight:

```yaml
alfredConfig:
  maintenanceWindows:
    - days: [Sat, Sun]
      start: "01:00"
      end: "05:00"
```

With no windows, the default, defragmentation can act at any time. Outside the windows, it keeps its advice but drops its moves, except emergencies.

### Spot and preemptible nodes

Alfred treats a node as preemptible when it has one of the labels in `spotPolicy.preemptibleLabels`, with any value other than `false`. By default, those are `node.kubernetes.io/preemptible` and `cloud.google.com/gke-preemptible`. Alfred keeps moves off such nodes and leaves their free GPUs out of the score (`spotPolicy.avoidAsTarget`), and [boosts](#how-alfred-picks-a-move) defragmentation moves off them (`spotPolicy.preferAsSource`). Both are on by default. The `alfred.ome.io/spot-policy` annotation adjusts this for one InferenceService. Its value `avoid` keeps its moves off preemptible nodes, even when `avoidAsTarget` is `false`. With `migrate`, Alfred always boosts its moves off them, and with `ignore`, never.

### Per-workload annotations

You set Alfred's annotations on an InferenceService's own `metadata.annotations`. `alfred.ome.io/movable: "false"` opts it out, and `"true"` opts it in when `defaultMovable` is `false`. The other `alfred.ome.io/` annotations, `priority`, `cooldown-minutes`, `tenant-group` and `spot-policy`, tune its moves. When it plans, Alfred ignores a value it can't parse and uses the default. [Workload annotations](../../reference/scheduling/alfred-configuration.md#workload-annotations) lists their values.

## Next steps

- [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md): install the chart, read what the policies recommend and [tune a policy](../../guides/scheduling/run-alfred.md#step-4-tune-a-policy).
- [Set up scheduler simulation for Alfred](../../guides/scheduling/set-up-scheduler-simulation.md): predict where the replacement pods of each move would land.
- [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md): turn on execute mode.
- [Alfred configuration](../../reference/scheduling/alfred-configuration.md): the settings and chart values, with their defaults.
- [Alfred metrics and events](../../reference/scheduling/alfred-metrics-and-events.md): the reasons, metrics and events the policies produce.
