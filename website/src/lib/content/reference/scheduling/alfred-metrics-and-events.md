---
title: Alfred metrics and events
description: The Prometheus metrics, Kubernetes events and recommendations ConfigMap that Alfred publishes, and what each one tells you.
since: v1.3
---

Alfred reports what it observes and recommends through Prometheus metrics, Kubernetes events and the ConfigMap `alfred-recommendations`. With `migration.apiVersion: v1`, the ConfigMap `alfred-dispatch-state` also tracks its migration requests. Alfred is alpha. It runs only when you install its opt-in chart, `ome-alfred`, and it starts in recommend-only mode, which changes no workload. Execute mode, in which Alfred migrates OMENative Instances, is a further opt-in. For what Alfred does, see [Alfred](../../concepts/scheduling/alfred.md). To install it, see [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md).

The chart runs 3 replicas. Every replica observes the cluster every 30 seconds (`observationLoopInterval`) and publishes the cluster metrics. The leader, the replica that holds the Lease `alfred.ome.io`, also runs a decision pass every 5 minutes (`decisionLoopInterval`), and early on the node changes in `earlyTickOn`. It publishes each pass as decision metrics, events and the recommendations ConfigMap. See [High availability](../../concepts/scheduling/alfred.md#high-availability).

The chart values are `configMapName` and the settings named `migration.*`, `simulation.*` and `metrics.*`. The others are keys of Alfred's `config.yaml`, which you set through the chart's `alfredConfig` values. [Alfred configuration](alfred-configuration.md) lists both, with their defaults.

In the default install, `migration.apiVersion` is empty, so Alfred has no migration executor. That's expected, and it shows in these ways:

- `alfred_omenative_unavailable` is 1 on every replica.
- Each process that becomes leader records one `OMENativeUnavailable` Warning event.
- Each steady OMENative Instance that a policy picks gets `OMENativeUnavailable` advice, with no target hints.
- The fragmentation score, reclaimable fragmentation and pending pressure stay at 0, because no Instance counts as movable.

The defragmentation policy still gives advice when a pool's demand-weighted fragmentation is above `fragmentationThreshold`, a figure no metric shows. See [How Alfred scores a pool](../../concepts/scheduling/alfred-policies.md#how-alfred-scores-a-pool). Planned moves, with target hints, need `migration.apiVersion: v1`, even in recommend-only mode. See [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md).

## Metrics

Each replica serves `/metrics` on port 8080, the container port `metrics` (the chart value `metrics.port`). The chart annotates each pod with `prometheus.io/scrape`, `prometheus.io/port` and `prometheus.io/path`. With `metrics.serviceMonitor.enabled: true`, it also creates the ServiceMonitor `ome-alfred`, which needs the Prometheus Operator CRDs.

Scrape every pod, as the ServiceMonitor and the annotations do. The Service `ome-alfred-metrics` sends each request to one replica, and only the leader publishes decision metrics. Counters start at 0 when a pod starts. The endpoint also serves the Go runtime and controller-runtime metrics. To read the leader's metrics by hand, see [Step 3: Check its metrics](../../guides/scheduling/run-alfred.md#step-3-check-its-metrics).

### Cluster metrics

Every replica republishes these gauges on each observation, and a pool, node or size that's gone drops out. A node's `pool` is its `nvidia.com/gpu.product` label, or else its instance type, shortened for known shapes, or else its GPU resource name.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `alfred_cluster_fragmentation_score` | Gauge | none | The highest pool score, 1 − (1 − reclaimable) × (1 − pending pressure), from 0 to 1. Defragmentation plans moves in pools above `fragmentationThreshold` (default 0.25). |
| `alfred_fragmentation_observed` | Gauge | `pool`, `size` | The share of the pool's free GPUs that a pod needing `size` GPUs on one node can't use. The sizes are `scoring.sizeLadder`, 1, 2, 4 and 8 by default. |
| `alfred_fragmentation_reclaimable` | Gauge | `pool` | The demand-weighted fragmentation that moving Alfred's movable Instances could remove. |
| `alfred_pending_pressure` | Gauge | `pool` | Pressure from pending pods that a repack of the pool could seat, weighted by how long they've waited. |
| `alfred_gpu_capacity` | Gauge | `node`, `status` | GPUs on each GPU node, by `status`: `total`, `allocated`, `free` and `contiguous_max`, which equals `free`. |
| `alfred_pending_pod_count` | Gauge | none | Unscheduled `Pending` pods that request GPUs, excluding pods being deleted. |
| `alfred_pending_pod_gpu_requirements` | Gauge | `size` | Those pending pods, by the GPUs each one requests. |
| `alfred_surge_headroom_gpus` | Gauge | `pool` | The most GPUs free on one node of the pool: the largest Instance that could surge there now. 0 means no move can surge. |

The fragmentation, pressure and surge headroom figures leave out nodes that can't take work. Those are nodes whose health state isn't `Clear`, and nodes that have maintenance requested, are cordoned or are marked for scale-down by the cluster autoscaler. With `spotPolicy.avoidAsTarget`, the default, the fragmentation and pressure figures also leave out spot nodes.

### Decision metrics

The leader updates these counters after each decision pass. The recommendation counters count every row, so a row that persists counts again in each pass. The signal counters count node events. `workload` is the InferenceService as `namespace/name`. `policy` is `defragmentation` or `nodehealth`, or `migration-dispatch` for a journal entry whose request Alfred can't read.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `alfred_recommendations_produced_total` | Counter | `policy`, `workload`, `component`, `reason`, `executable` | Every row. `reason` is `Fragmentation`, `NodeUnhealthy` or `NodeMaintenance`, and `executable` is `false` for advice. |
| `alfred_recommendations_accepted_total` | Counter | `policy`, `workload`, `component` | Rows that Alfred admitted and reports as `submitted` or `withheld`. |
| `alfred_recommendations_rejected_total` | Counter | `policy`, `workload`, `component`, `reason` | Rows that the arbiter rejected, by reason. Withheld rows count as accepted, despite the help text. |
| `alfred_lws_recommendations_total` | Counter | `isvc`, `action` | Rows with `LWSMigrationUnsupported` advice. `isvc` is `namespace/name`, and `action` is always `MigrateToOMENative`. |
| `alfred_nodehealth_signals_total` | Counter | `node`, `reason` | `NodeRepairNeeded` and `NodeDrainedForRepair` events. |
| `alfred_nodemaintenance_signals_total` | Counter | `node`, `reason` | `NodeMaintenanceRequested` and `NodeDrainedForMaintenance` events. |
| `alfred_cooldown_overrides_total` | Counter | `policy` | Health evacuations admitted inside the standard cooldown, each with a `CooldownOverriddenForEvacuation` event. |

To follow migration requests and their results, read the [recommendations ConfigMap](#the-recommendations-configmap) and the [events](#on-an-inferenceservice).

### Loop and state metrics

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `alfred_observation_loop_duration_seconds` | Histogram | none | How long each successful observation takes, on every replica. |
| `alfred_decision_loop_duration_seconds` | Histogram | none | How long each decision pass takes, on the leader. |
| `alfred_leader_status` | Gauge | `pod` | 1 on the replica that holds the Lease, 0 on the others. |
| `alfred_policy_reload_total` | Counter | `outcome` | Loads of `config.yaml` on each replica, including the first, by `outcome`: `success` or `failure`. |
| `alfred_circuit_breaker_state` | Gauge | none | 1 while the [circuit breaker](../../concepts/scheduling/alfred-policies.md#global-limits) is open, 0 while it's closed. The leader sets it in each pass. |
| `alfred_omenative_unavailable` | Gauge | none | 1 while Alfred has no migration executor, because `migration.apiVersion` isn't `v1`, and 0 otherwise. Every replica sets it. |

Both histograms use the default Prometheus buckets, 0.005 to 10 seconds.

## Events

Alfred records Kubernetes events with the source `alfred`. To list them in every namespace:

```bash
kubectl get events -A --field-selector source=alfred -o custom-columns=NAMESPACE:.metadata.namespace,TYPE:.type,REASON:.reason,KIND:.involvedObject.kind,NAME:.involvedObject.name
```

```output
NAMESPACE   TYPE      REASON                                KIND               NAME
ome         Warning   OMENativeUnavailable                  ConfigMap          alfred-config
prod        Normal    FragmentationRecommendationProduced   InferenceService   chat
```

Events are lossy. Repeats on one object merge into one with a count. A burst on one object can lose events, or combine them into one whose message starts with `(combined from similar events):`. The [recommendations ConfigMap](#the-recommendations-configmap) holds the full result of each pass.

### On an InferenceService

Alfred records these events on the InferenceService. `kubectl describe inferenceservice` doesn't show them, so select them by name:

```bash
kubectl get events -n prod --field-selector involvedObject.name=chat,source=alfred -o custom-columns=TYPE:.type,REASON:.reason,MESSAGE:.message
```

```output
TYPE     REASON                                MESSAGE
Normal   FragmentationRecommendationProduced   defragmentation advisory for prod/chat/engine: OMENativeUnavailable (from node-a, footprint 0 GPUs)
```

| Reason | Type | When |
| --- | --- | --- |
| `FragmentationRecommendationProduced` | Normal | A defragmentation row ends as advice. |
| `EvacuationRecommendationProduced` | Normal | A node-health row ends as advice. |
| `RawDeploymentMigrationUnsupported` | Normal | A row for a RawDeployment component ends as advice, from either policy. |
| `RecommendationRejected` | Normal | The arbiter rejects a row. |
| `RecommendationWithheld` | Normal | Alfred admits a row but doesn't write its request, or the row's journal entry is still `prepared`. |
| `MigrationSubmitted` | Normal | Alfred writes the request, or finds it on the InferenceService. |
| `MigrationAcknowledged` | Normal | The InferenceReplica reports the request in progress. |
| `MigrationCompleted` | Normal | The InferenceReplica reports the migration completed. |
| `MigrationFailed` | Warning | The InferenceReplica reports the migration failed. |
| `MigrationStalled` | Warning | The request stopped making progress. |
| `CooldownOverriddenForEvacuation` | Normal | Alfred admits a health evacuation inside the standard cooldown. |

The messages have these forms:

- Advice: `<policy> advisory for <namespace>/<name>/<component>: <advisoryReason> (from <fromNode>, footprint <GPUs> GPUs)`. The footprint is 0, except for `NoSurgeHeadroom`, where it's the Instance's GPUs.
- Rejection: `<policy> recommendation for <namespace>/<name>/<component> rejected: <rejectReason>`.
- Withheld rows and requests: `<policy> migration request uuid=<requestUUID> for <namespace>/<name>/<component> instance <instance>: <dispatchStatus> (<dispatchReason>)`. The UUID is empty until Alfred creates the request.

Alfred records these events again in every pass. Advice for several Instances of one component on one node shares one event, because the message has no Instance index.

### On Alfred's configuration ConfigMap

| Reason | Type | When |
| --- | --- | --- |
| `OMENativeUnavailable` | Warning | A process that becomes leader finds no migration executor. |
| `PolicyReloadFailed` | Warning | A replica rejects `config.yaml`, or finds the key missing, and keeps its last good configuration, or the defaults. |

Alfred records both on its configuration ConfigMap, `alfred-config` by default (the chart value `configMapName`), in its namespace. Each replica records its own `PolicyReloadFailed`, and its message gives the error. `kubectl describe configmap` shows `PolicyReloadFailed`, but not `OMENativeUnavailable`. See [Hot reload](alfred-configuration.md#hot-reload).

### On a node

Alfred never cordons, drains or evicts: these events are signals for your repair and maintenance tooling. The leader records them on the node, in the `default` namespace, and `kubectl describe node` shows them.

| Reason | Type | When |
| --- | --- | --- |
| `NodeRepairNeeded` | Warning | The node's health state turns `Unhealthy` or `Unknown`. |
| `NodeDrainedForRepair` | Normal | In a pass after `NodeRepairNeeded`, the node has no OME GPU occupants and isn't `Unknown`. |
| `NodeMaintenanceRequested` | Normal | Maintenance is requested for the node. |
| `NodeDrainedForMaintenance` | Normal | In a pass after `NodeMaintenanceRequested`, the node has no OME GPU occupants. |

OME GPU occupants are OME pods that request GPUs, and the OME workloads in the node's record. A drained node can still run pods that OME doesn't manage.

Each event fires once per episode, which Alfred tracks in the node's [record](#node-records). The repair events can fire again after the node returns to `Clear`, and the maintenance events after maintenance is no longer requested. `NodeDrainedForRepair` can also fire again after the node has been `Unknown`, and both drained events after an OME GPU occupant returns. A node that rejoins with a new UID starts over.

A new leader records the maintenance events again for nodes still under maintenance. It keeps a node's repair phase from its record when the node's state and conditions haven't changed. With `recommendationsConfigMapEnabled: false` there's no record, so it records the repair events again too. See [Signals and events](../../concepts/scheduling/alfred-policies.md#signals-and-events).

## The recommendations ConfigMap

After each decision pass, the leader writes its result to the ConfigMap `alfred-recommendations` in Alfred's namespace. `recommendationsConfigMapName` changes the name, and `recommendationsConfigMapEnabled: false` turns the record off. The chart creates the ConfigMap. If it's missing, the leader writes no record until it's back: see [Alfred logs that the recommendations ConfigMap is missing](../../guides/scheduling/run-alfred.md#alfred-logs-that-the-recommendations-configmap-is-missing).

| Key | Content |
| --- | --- |
| `last-cycle.json` | The latest pass: `timestamp`, `mode` (`recommend-only` or `execute`) and `recommendations`, one row for each candidate, or `null` when there are none. |
| `node.<node>` | The [record](#node-records) of a node whose health state isn't `Clear` or that has maintenance requested. |

To read the latest pass:

```bash
kubectl get configmap alfred-recommendations -n ome -o jsonpath='{.data.last-cycle\.json}' | jq .
```

```output
{
  "timestamp": "2026-09-28T10:15:02.418093721Z",
  "mode": "recommend-only",
  "recommendations": [
    {
      "workload": "prod/chat",
      "component": "engine",
      "instance": 3,
      "policy": "defragmentation",
      "reason": "Fragmentation",
      "outcome": "advisory",
      "advisoryReason": "OMENativeUnavailable",
      "fromNode": "node-a",
      "score": 0,
      "scheduling": {
        "schedulerName": "default-scheduler",
        "status": "Unavailable",
        "reason": "ProfileNotConfigured"
      }
    }
  ]
}
```

This row is from the default install. Instance 3 of the `engine` component of `chat` runs on `node-a` in a fragmented pool. With no executor, Alfred can only advise. The [`kubectl ome admin recommendations`](../kubectl-ome/admin.md#recommendations) command summarizes the same pass. A row leaves out fields that have no value, except `score`:

| Field | Meaning |
| --- | --- |
| `workload` | The InferenceService, as `namespace/name`. |
| `component` | The component. |
| `instance` | The Instance index, or -1 for a row about the whole component. |
| `policy` | `defragmentation`, `nodehealth` or `migration-dispatch`. |
| `reason` | Why the policy proposed the move: `Fragmentation`, `NodeUnhealthy` or `NodeMaintenance`. |
| `outcome` | What became of the row. See [Outcomes](#outcomes). |
| `advisoryReason` | Why the row is advice. See [Advisory reasons](#advisory-reasons). |
| `rejectReason` | Why the arbiter rejected the row. See [Arbiter reasons](#arbiter-reasons). |
| `fromNode` | The node the Instance runs on. |
| `target` | The target node the arbiter picked. |
| `hintTargets` | Up to 3 nodes that a planned move prefers. |
| `score` | The row's priority. |
| `emergency` | `true` when the move seats a pending pod that has waited longer than `emergencyPendingAgeMinutes` and that the current layout can't seat. |
| `cooldownOverridden` | `true` when Alfred admitted a health evacuation inside the standard cooldown. |
| `dispatchStatus` | The request's status: `withheld`, `submitted`, `acknowledged`, `completed`, `failed` or `stalled`. |
| `requestUUID` | The request's UUID. |
| `dispatchReason` | Why the dispatcher withheld the move, or the reason for the request's latest status. See [Dispatch reasons](#dispatch-reasons). |
| `scheduling` | The scheduler selection and simulation result. See [Scheduling results](#scheduling-results). |

A planned move's `score` is its benefit less a move cost that `aggressiveness` weights. See [How Alfred picks a move](../../concepts/scheduling/alfred-policies.md#how-alfred-picks-a-move). Defragmentation advice scores 0 or less. A node-health row, advice included, scores the InferenceService's `alfred.ome.io/priority` annotation, 0.5 by default.

### Outcomes

| Outcome | Meaning |
| --- | --- |
| `advisory` | Advice: Alfred can't make the move, and `advisoryReason` says why. |
| `rejected` | The arbiter rejected the move, and `rejectReason` says why. |
| `withheld` | Alfred admitted the move but didn't write its request, or its journal entry is still `prepared`. `dispatchReason` says why. |
| `submitted` | Alfred wrote the request to the InferenceService. |
| `acknowledged` | The InferenceReplica reports the request in progress. |
| `completed` | The migration completed. |
| `failed` | The migration failed. |
| `stalled` | The request stopped making progress, and `dispatchReason` says why. |

In recommend-only mode, every row is `advisory`, except the rows of requests left in the journal from execute mode. A request's row appears in every pass until the request ends, and once more in the pass that sees it end.

When maintenance windows are configured, the defragmentation policy proposes planned moves inside them, and emergencies at any time. See [Maintenance windows](../../concepts/scheduling/alfred-policies.md#maintenance-windows).

### Advisory reasons

| Reason | Meaning |
| --- | --- |
| `OMENativeUnavailable` | Alfred has no migration executor, because `migration.apiVersion` isn't `v1`. Expected in the default install. |
| `RawDeploymentMigrationUnsupported` | The component runs as a RawDeployment, which Alfred can't migrate. One row for each pod. |
| `LWSMigrationUnsupported` | The component runs in `MultiNode` mode (deprecated). Defragmentation gives one row for the component, and node health one for each Instance. `lwsRecommendationsEnabled: false` turns these rows off. |
| `NoSurgeHeadroom` | No node has room for the Instance's surge while its source still holds its GPUs. |
| `VolumePinned` | A ReadWriteOnce or ReadWriteOncePod volume pins the component to its node. One row for the component. |
| `ModelUnresolved` | Alfred can't resolve where the component's model is available. One row for the component. |
| `MigrationSurfaceDisabled` | `omenativeMigrationEnabled` is `false`. |
| `OMENativeObservationInvalid` | The component's InferenceReplica is missing, stale or invalid, or the Instance's observed state is invalid. |
| `OMENativeStateIneligible` | The Instance isn't steady, or its InferenceService isn't movable. For example, it's paused, mid-rollout, not Ready, migrating or set to `migrationPolicy: Never`. Defragmentation also gives it during the per-workload cooldown. |
| `NonExecutableObservedFragmentation` | Defragmentation only: the pool's observed fragmentation is high but its score isn't above `fragmentationThreshold`, or the planned move scores 0 or less. |
| `SimulationRecommendOnly` | A planned move in recommend-only mode whose scheduler selection is ready. `scheduling` holds its simulation. |
| A scheduling reason | A planned move whose scheduler selection isn't ready, for example `ProfileNotConfigured`. See [Scheduling results](#scheduling-results). |

A component that's OMENative only through its runtime counts as a RawDeployment. For the fix, see [An OMENative component shows RawDeploymentMigrationUnsupported](../../guides/scheduling/run-alfred.md#an-omenative-component-shows-rawdeploymentmigrationunsupported).

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../../guides/omenative/move-from-leaderworkerset.md).

### Scheduling results

Alfred adds `scheduling` to each OMENative row for one Instance. It records which scheduler would place the Instance and, when you set up simulation, where the Instance would go. In execute mode with `migration.apiVersion: v1`, rows have no `scheduling`: the dispatcher simulates each move just before it requests it.

| Field | Meaning |
| --- | --- |
| `schedulerName` | The scheduler that the Instance's pod templates name. An empty name counts as `default-scheduler`. |
| `backend`, `schedulerVersion`, `configurationID` | From the scheduling profile that matches `schedulerName`. |
| `status` | `Feasible`, `Infeasible`, `Unsupported` or `Unavailable`. |
| `reason` | Why. See the next table. |
| `snapshotID`, `snapshotTime` | The cluster snapshot that the simulation ran against. |
| `placements` | Where the simulation placed the Instance's pods. |

| Reason | Status | Meaning |
| --- | --- | --- |
| `PlacementFound` | `Feasible` | The simulation placed every replacement pod, and `placements` lists where. |
| `NoFeasiblePlacement` | `Infeasible` | The simulation found no placement. |
| `Unsupported` | `Unsupported` | The simulation worker can't model the request. |
| `GangUnsupported` | `Unsupported` | The Instance has more than one pod, and the profile's `gangScheduling` is `false`. |
| `InputUnsupported` | `Unsupported` | Alfred couldn't build a simulation request. |
| `RecommendationTooLarge` | `Unsupported` | The move needs more than 128 replacement pods. |
| `ProfileNotConfigured` | `Unavailable` | No scheduling profile names the pods' scheduler. Expected in the default install. |
| `SimulationUnavailable` | `Unavailable` | A profile matches, but no simulation worker is configured. |
| `MixedSchedulers` | `Unavailable` | The Instance's pod templates name different schedulers. |
| `TemplateBound` | `Unavailable` | A pod template sets `nodeName`. |
| `NoTemplates` | `Unavailable` | Alfred found no pod template to simulate. |
| `OMENativeObservationInvalid` | `Unavailable` | Alfred's observation of the InferenceService, its InferenceReplica or the Instance is incomplete or out of date. |
| `SimulationBudgetExceeded` | `Unavailable` | The pass already simulated 8 rows, or spent 30 seconds simulating. |
| `ObservationStale` | `Unavailable` | The latest observation is more than 30 seconds old, for example because `observationLoopInterval` is longer than `30s`. |
| `CaptureFailed` | `Unavailable` | Alfred couldn't capture the cluster snapshot, for example because the PodGroup CRD is missing. |
| `SnapshotStale` | `Unavailable` | The snapshot or the observation got older than 30 seconds before the simulation finished. |
| `SourceChanged` | `Unavailable` | The InferenceService, its InferenceReplica or the Instance's pods changed between the observation and the snapshot. |
| `WorkerFailed` | `Unavailable` | The simulation returned an error, for example because no loaded worker matches the profile. |
| `InvalidResponse` | `Unavailable` | The worker's answer failed Alfred's checks. |

Simulation needs the PodGroup CRD, which no OME chart installs. See [Gang scheduling](../../concepts/serving/gang-scheduling.md#when-the-podgroup-crd-is-missing). Simulation works without `migration.apiVersion`. With only `simulation.configMapName` set, Alfred simulates advice rows, `OMENativeUnavailable` included, and records their placements. The rows still get no `hintTargets`. To set up profiles, see [Scheduling profiles](alfred-configuration.md#scheduling-profiles).

### Arbiter reasons

In execute mode with `migration.apiVersion: v1`, the arbiter checks each executable row, health evacuations first. A rejected row shows the first check it failed, in this order:

| Reason | Meaning |
| --- | --- |
| `CircuitBreakerOpen` | The circuit breaker is open. |
| `InstanceGone` | The InferenceService or Instance no longer exists. |
| `NotMovable` | The InferenceService isn't movable. |
| `MalformedRequestPending` | The InferenceService carries a malformed migration request. |
| `MigrationStateInvalid` | The InferenceReplica's migration state is invalid. |
| `InstanceTerminating` | One of the Instance's pods is terminating. |
| `WorkloadBusy` | The InferenceService has a migration in progress, or this pass already admitted a move for it. |
| `Cooldown` | The InferenceService's last migration finished within its cooldown: `perWorkloadCooldownMinutes`, or its `alfred.ome.io/cooldown-minutes` annotation. A `NodeUnhealthy` evacuation waits `healthCooldownFloorMinutes` instead. |
| `PlacementCooldown` | One of the Instance's pods started less than `recentPlacementCooldownMinutes` ago, or `healthCooldownFloorMinutes` for a `NodeUnhealthy` evacuation. |
| `TargetUnavailable` | The target nodes can't take work. |
| `TargetUnderEvacuation` | The target is the source of a `NodeUnhealthy` or `NodeMaintenance` row in this pass. |
| `TargetNodeBusy` | This pass already admitted a move onto the target. |
| `NoCapacity` | No target node has room for the move. |
| `InFlightCap` | Migrations in progress plus moves admitted in this pass reach `maxInFlightMigrations`. |
| `HourlyCap` | Migrations started in the last hour plus moves admitted in this pass reach `maxMigrationsPerHour`. |

The targets are the simulated placements, or else the row's `hintTargets`. Both caps count every migration in the cluster, including `kubectl ome migration` requests. See [The arbiter](../../concepts/scheduling/alfred-policies.md#the-arbiter).

### Dispatch reasons

With `migration.apiVersion: v1`, the dispatcher takes the rows that the arbiter admits. It writes one migration request at a time, and none while another request is waiting or in progress in the cluster, including one from `kubectl ome migration`. A `maxInFlightMigrations` above 1 still allows only one migration at a time: it lets the dispatcher try the next admitted move when one fails its re-check.

An admitted move that Alfred doesn't request is `withheld`, with a `dispatchReason`. These checks cover the whole pass, and withhold every admitted move:

| Reason | Meaning |
| --- | --- |
| `JournalUnavailable` | Alfred can't read or write the [dispatch-state ConfigMap](#the-dispatch-state-configmap). |
| `MultipleUnresolvedRequests` | The journal holds more than one unfinished request. |
| `UnresolvedRequest` | An earlier request is still `submitted`, `acknowledged` or `stalled`. |
| `ExecutionDisabled` | Execution is off: `mode` isn't `execute`, or `omenativeMigrationEnabled` is `false`. Then only a leftover `prepared` request shows it. Otherwise, it marks an admitted row the dispatcher didn't reach. |
| `InvalidCooldown` | A cooldown setting is negative or too large, or an InferenceService's `alfred.ome.io/cooldown-minutes` isn't a whole number of 0 or more. |
| `ConfigurationChanged` | The configuration changed during the pass. |
| `DispatchDeadline` | The dispatcher used up its 30 seconds in the pass. |
| `GuardUnavailable` | The ValidatingAdmissionPolicy or binding `ome-alfred-migration-writes` is missing, differs from what Alfred expects or isn't type-checked yet. |
| `ObservationStale` | The observation is more than 30 seconds old. |
| `FailureBackoff` | A request failed or stalled less than `migration.failureBackoff` ago. |

Just before it writes a request, Alfred re-checks the move against a fresh observation. A move that fails is withheld with one of these reasons, and the dispatcher tries the next:

| Reason | Meaning |
| --- | --- |
| `ObservationUnavailable` | Alfred can't build a fresh observation. |
| `PolicyNoLongerEligible` | The policy no longer proposes the move. |
| An arbiter reason, for example `WorkloadBusy` | The arbiter no longer admits the move. See [Arbiter reasons](#arbiter-reasons). |
| `SnapshotUnavailable` | Alfred can't capture the cluster snapshot, for example because the PodGroup CRD is missing. |
| `InFlightCap` | A migration request is waiting or in progress somewhere in the cluster. A request annotation that nothing acts on counts until you remove it: see [Admitted moves are withheld](../../guides/scheduling/let-alfred-migrate-instances.md#admitted-moves-are-withheld). |
| `HourlyCap` | `maxMigrationsPerHour` migrations started in the cluster in the last hour. |
| `Cooldown` | One of Alfred's own requests moved this InferenceService within its cooldown. |
| `NodeCooldown` | Less than `perNodeCooldownMinutes` ago, one of Alfred's own requests moved an Instance off the source node or named it as a target. A `NodeUnhealthy` evacuation skips this check. |
| `MigrationStateInvalid` | An InferenceReplica somewhere in the cluster has an invalid migration record. |
| `JournalPayloadInvalid` | Alfred can't read the request stored in a journal entry. |
| `SourceUnsupported` | Alfred can't build a scheduling request for the source Instance, for example because no scheduling profile fits. |
| `SourceChanged` | The InferenceService, the InferenceReplica or the Instance's pods changed since the pass started. |
| `SimulationNotFeasible` | The simulation fails or finds no placement. |
| `SimulationStale` | The observation or snapshot was more than 30 seconds old when the simulation finished. |
| `SchedulingStateChanged` | The cluster's scheduling state changed during the simulation. |
| `SafetyStateChanged` | After the simulation, the arbiter no longer admits the move. |
| `PredictedTargetUnsafe` | A simulated placement is outside the allowed targets, or on a node that can't take work or isn't Ready. |
| `TargetCooldown` | Less than `perNodeCooldownMinutes` ago, one of Alfred's own requests used a simulated target node. |
| `InvalidRequest` | Alfred can't build a journal entry for the move. |

Alfred records the move in the journal before it writes the request. These reasons describe the write:

| Reason | Reported as | Meaning |
| --- | --- | --- |
| `OwnerChanged` | `withheld` | The InferenceService changed after the re-check, so Alfred didn't write the request. |
| `SubmissionPrepared` | `withheld` | Alfred recorded its attempt and is writing the request. |
| `RequestSubmitted` | `submitted` | Alfred wrote the request as the annotation `ome.io/migration-request-v1-<uuid>` on the InferenceService. |
| `SubmissionUncertain` | `withheld` | Alfred can't tell whether the write succeeded, and the entry stays `prepared`. |
| `SubmissionJournalUncertain` | `submitted` or `withheld` | Alfred can't record the write's result in the journal. The next pass reads the InferenceService to find out. |
| `SerialDispatchLimit` | `withheld` | Alfred already tried a move in this pass. |

After Alfred tries a new move, every other admitted row still `withheld` shows `SerialDispatchLimit`, even a row that failed a re-check earlier in the pass. So does the tried row while its journal entry is `prepared`, though the entry keeps its own reason. When no move passes its re-check, each row keeps the reason it failed.

A `prepared` request left from an earlier pass is retried before any new move, but only within `migration.acknowledgementTimeout` of its `createdAt`. With the defaults, the next pass comes later than that, so the request usually stalls with `AcknowledgementTimeout` instead. If a retried request fails its re-check, every admitted move is withheld with that reason.

### Node records

A `node.<node>` key holds a JSON record with these fields:

| Field | Meaning |
| --- | --- |
| `nodeUID` | The node's UID. |
| `state` | The node's health state: `Clear`, `Suspect`, `Unhealthy` or `Unknown`. |
| `conditions` | The node's trigger conditions (`triggerConditions`), each with `type`, `status` and `lastTransitionTime`. |
| `suspectUntil` | When a `Suspect` node turns `Clear` if nothing changes. |
| `maintenance` | `requested` and the `triggers` that requested maintenance. |
| `workloads` | The OME workloads on the node, as `namespace/name`. |
| `omeGpuOccupantsPresent` | Whether OME GPU occupants remain on the node. |
| `observedAt` | The time of the observation the record comes from. |
| `signaledAt`, `drainedAt` | When Alfred recorded `NodeRepairNeeded` and `NodeDrainedForRepair`. |
| `maintenanceRequestedAt`, `maintenanceDrainedAt` | When Alfred recorded `NodeMaintenanceRequested` and `NodeDrainedForMaintenance`. |

Alfred deletes a node's record when the node is `Clear` with no maintenance requested, or leaves the cluster. See [Health states](../../concepts/scheduling/alfred-policies.md#health-states).

## The dispatch-state ConfigMap

With `migration.apiVersion: v1`, the chart creates the ConfigMap `alfred-dispatch-state` in Alfred's namespace. The name is fixed. It's Alfred's journal: Alfred records each request there before it writes the request to the InferenceService. In every decision pass, in either mode, the leader updates each entry from what it observes. In recommend-only mode, it writes no requests.

Its one key, `state.json`, holds `version` (`v1`), `entries` and, after a failure, `backoffUntil`, when the failure backoff ends. A document Alfred can't read, for example one with an unknown field, gives `JournalUnavailable`, and the log doesn't say why.

To read the journal:

```bash
kubectl get configmap alfred-dispatch-state -n ome -o jsonpath='{.data.state\.json}'
```

```output
{"version":"v1","entries":[]}
```

This journal is empty. Each entry has these fields:

| Field | Meaning |
| --- | --- |
| `uuid` | The request's UUID. It names the annotation `ome.io/migration-request-v1-<uuid>` and the migration in the InferenceReplica's status. |
| `workload`, `workloadUID` | The InferenceService, as an object with `Namespace` and `Name`, and its UID. |
| `irName`, `irUID` | The InferenceReplica's name and UID. |
| `component`, `instance`, `fromNode` | The component, the Instance index and the node the Instance runs on. |
| `targets` | The simulated target nodes. |
| `payload` | The request, as Alfred writes it to the annotation. |
| `sourceFingerprint` | A fingerprint of the source Instance, for detecting changes. |
| `createdAt`, `lastAttempt` | When Alfred created the entry, and when it last tried to write the request. |
| `acknowledgedAt`, `completedAt` | When the InferenceReplica first reported the request, and when the request completed or failed. |
| `phase`, `reason` | The entry's state and its reason. See [Journal states](#journal-states). |

### Journal states

| Phase | Meaning | Reasons |
| --- | --- | --- |
| `prepared` | Recorded, but the request isn't known to be written. Reported as `withheld`. | `SubmissionPrepared`, `SubmissionUncertain` or none |
| `submitted` | The annotation is on the InferenceService. | `RequestSubmitted`, `RequestAnnotationObserved` |
| `acknowledged` | The InferenceReplica reports the request as `Accepted`, `SurgePending`, `SurgeReady` or `Draining`. | `UUIDStatusObserved` |
| `completed` | The InferenceReplica reports the migration `Completed`. The entry has ended. | `TerminalStatusObserved` |
| `failed` | The InferenceReplica reports the migration `Failed`. The entry has ended. | `TerminalStatusObserved` |
| `stalled` | The request stopped making progress. | See the next table. |

| Stall reason | When |
| --- | --- |
| `AcknowledgementTimeout` | The InferenceReplica hasn't reported the request within `migration.acknowledgementTimeout` (default 2 minutes) of `createdAt`. This applies to `prepared` entries too. |
| `ConsumerDeadlineExceeded` | The migration is still in progress past its own deadline. |
| `AcknowledgedStatusMissing` | The InferenceReplica stopped reporting a request it had acknowledged. |
| `RequestPayloadChanged` | The annotation on the InferenceService no longer matches the journal. |
| `OwnerUnavailable`, `ReplicaUnavailable`, `ReplicaIdentityChanged` | Past the acknowledgement timeout, the InferenceService or InferenceReplica is missing or has been replaced. |
| `MigrationStatusInvalid` | Past the acknowledgement timeout, the InferenceReplica reports the UUID with a status that doesn't match the request. |

The manager's InferenceReplica controller acknowledges Alfred's requests. With `--enable-inferencereplica-controller=false`, the first request stalls with `AcknowledgementTimeout` and blocks every later one.

A `failed` or `stalled` entry sets `backoffUntil` to `migration.failureBackoff` (default 5 minutes) later, and Alfred withholds moves with `FailureBackoff` until then. Only `completed` and `failed` end an entry. Until then, a `stalled` entry withholds every new move with `UnresolvedRequest`, and some stalled entries never end: see [Handling the journal](#handling-the-journal).

### Handling the journal

!!! warning "Keep the journal"
    Don't delete or reset `alfred-dispatch-state`. While Alfred can't read it, it withholds every move with `JournalUnavailable`. Helm keeps the journal when you uninstall the chart, and the chart keeps an existing journal's data as it is. The `helm template` command can't read the live journal and renders an empty one, so never apply its output to a cluster that has a journal.

With Kustomize, `config/alfred/dispatch-state.yaml` creates the journal. Create it once, on a cluster with no dispatch history:

```bash
kubectl create -f config/alfred/dispatch-state.yaml
```

```output
configmap/alfred-dispatch-state created
```

Never `kubectl apply` or `kubectl replace` the file, and keep it out of Kustomize and GitOps syncs. If `kubectl create` fails with `AlreadyExists`, stop and inspect the existing journal.

Some stalled entries never end: a request whose write never reached the InferenceService, or whose InferenceService or InferenceReplica was deleted or replaced. Alfred never cancels a request, so such an entry blocks new moves until you remove it from `entries` in `state.json`. [A request is stalled](../../guides/scheduling/let-alfred-migrate-instances.md#a-request-is-stalled) shows how to tell such an entry from a request that can still end, and how to remove it. When you turn migration back on, Alfred adopts the journal it finds.

## Related pages

- [Alfred](../../concepts/scheduling/alfred.md): what Alfred does, its two modes and how its replicas share the work.
- [Alfred policies](../../concepts/scheduling/alfred-policies.md): how the policies pick moves, and how the arbiter and the dispatcher check them.
- [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md): install Alfred and read its output.
- [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md): turn on planned moves and execute mode.
- [Alfred configuration](alfred-configuration.md): every `config.yaml` key, flag and chart value, with its default.
- [kubectl ome admin](../kubectl-ome/admin.md): read Alfred's latest recommendations from the command line.
- [kubectl ome migration](../kubectl-ome/migration.md): show an InferenceService's migrations and request one.
- [Migration and transient scale](../../concepts/omenative/migration-and-transient-scale.md): how the manager carries out a migration request.
