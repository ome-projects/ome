---
title: Migration and transient scale
description: OMENative moves an Instance to another node by starting its replacement first, and can run more or fewer Instances for a while without an edit to the InferenceService.
since: v1.3
---

[OMENative](overview.md) can move an [Instance](instances.md) to another node with no dip in serving capacity. It starts a replacement elsewhere and drains the old Instance once the replacement serves. Use a migration to empty a node before maintenance. A plain `kubectl drain` evicts the pods first, so the Instance stops serving until its pods run again elsewhere. A transient scale changes a component's Instance count without an edit to the [InferenceService](../serving/inference-services.md), until OME or the autoscaler sets it again.

Both act on the component's [InferenceReplica](../architecture/deployment-modes.md#what-ome-creates), which OME creates to hold its Instances, so both need a component on OMENative. [Request an Instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md) and [Request a transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md) give the steps.

The examples use the InferenceService `qwen3-0-6b` in the namespace `qwen3-0-6b`, from [Serve your first model](../../getting-started/serve-your-first-model.md), with its engine [switched to OMENative](../architecture/deployment-modes.md#opt-in-to-omenative) and running two Instances, 0 and 1. The engine's InferenceReplica is `qwen3-0-6b-engine`.

## How a migration works {#surge-migration}

OMENative moves an Instance by surge: it starts a replacement Instance, the surge, before it removes the old one, the source. Moving Instance 0 of `qwen3-0-6b-engine` off `gpu-node-a`, with `gpu-node-b` as a hint, goes like this:

1. OME gives the surge the lowest unused index, 2. The migration record moves to `SurgePending`, and OME records a `MigrationRequestAccepted` event. [`kubectl ome instance list`](../../reference/kubectl-ome/instance.md) shows Instance 0 as `Migrating` and Instance 2 as `Creating`.
2. OME creates the surge's pods from Instance 0's revision, with the same pod layout. They can't schedule on `gpu-node-a`, and the scheduler prefers `gpu-node-b`.
3. When the surge is ready and serving, and has stayed ready for the component's [`lifecycle.minReadySeconds`](../../guides/roll-out-changes/pace-rollouts-with-min-ready-seconds.md#ready-versus-available), the record moves to `SurgeReady`.
4. OME drains Instance 0, with the record in `Draining`, and then deletes it.
5. The record ends in `Completed`, with the message `migrated to instance=2`, and OME records a `MigrationCompleted` event.

While the move runs, the InferenceReplica's `CURRENT` column counts the surge, one Instance over `DESIRED`:

```bash
kubectl get inferencereplica qwen3-0-6b-engine -n qwen3-0-6b
```

```output
NAME                COMPONENT   DESIRED   CURRENT   READY   AVAILABLE   AGE
qwen3-0-6b-engine   engine      2         3         2       2           3d
```

So a migration needs room for one more whole Instance, even when only one pod of a multi-pod Instance is on the node you empty. If no node has room for the surge, the request fails at its [deadline](#deadline).

The surge keeps its [index](../architecture/deployment-modes.md#pod-labels-and-environment), so after the move the engine's Instances are 1 and 2, and a later scale-up takes index 0.

## Migration requests

A migration request asks OMENative to move one Instance off its node. It's an annotation on the InferenceService, not on the InferenceReplica, and three kinds of sender write it:

- [`kubectl ome migration start`](../../reference/kubectl-ome/migration.md), which is alpha, writes the request and fills in `from_node`. For an Instance whose pods span nodes, `--from-node` names the node to leave.
- [Alfred](../scheduling/alfred.md), OME's alpha GPU cluster caretaker, recommends Instance moves when free GPUs fragment or a node turns unhealthy and, when you [let it](../../guides/scheduling/let-alfred-migrate-instances.md), asks OMENative to carry them out.
- You, or your own tooling. OME treats a hand-written annotation like any other.

The key is `ome.io/migration-request-v1-` followed by a request UUID, and the value is a JSON object:

```json
{
  "schemaVersion": "v1",
  "component": "engine",
  "instance": 0,
  "from_node": "gpu-node-a",
  "hint_target_nodes": ["gpu-node-b"],
  "reason": "drain gpu-node-a for maintenance",
  "requested_at": "2026-09-28T08:00:00Z",
  "requested_by": "kubectl-ome"
}
```

| Field | Meaning |
| --- | --- |
| `schemaVersion` | Always `v1`. |
| `component` | `engine`, `decoder` or `router`. |
| `instance` | The index of the Instance to move. |
| `from_node` | The node to leave. One of the Instance's pods must run there. |
| `hint_target_nodes` | Optional. Nodes the scheduler should prefer for the surge. |
| `reason` | Optional. Free text that OME keeps in the migration record. |
| `requested_at` | Optional. When the request was made. |
| `requested_by` | Optional. Who asked, such as `kubectl-ome` or `alfred`. |

OME's webhook rejects a malformed request, or one for a component the InferenceService doesn't have, with a message that starts with `migration request`.

OME then records the request in the InferenceReplica's `status.migrations` as `Accepted`, adds a row to the audit ConfigMap `qwen3-0-6b-ome-migration-audit`, and removes the annotation. So a missing annotation means OME has taken the request, not that the Instance has moved. The `MigrationRequestAccepted` event comes later, when the move starts. A request OME can't act on, such as one for a removed component, gets a `MigrationRequestRejected` event instead of a record.

## Migration policy

`spec.<component>.lifecycle.migrationPolicy.mode` sets whether a component's Instances can move:

| Mode | Effect |
| --- | --- |
| `Auto` | The default. OME carries out migration requests, and [relocates](#automatic-relocation) an Instance that stalls on its node. |
| `Surge` | Same as `Auto` in this release. |
| `Never` | OME fails each migration request at once, with `migrations disabled by MigrationPolicy Mode=Never`, and turns relocation off. |

The runtime's `engineConfig`, `decoderConfig` or `routerConfig` can set the mode too, and the InferenceService's setting wins (see [The engine, decoder and router](../runtimes/serving-runtimes.md#the-engine-decoder-and-router) and [MigrationPolicy](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-MigrationPolicy)). This is the examples' InferenceService, with migration turned off for its engine:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: qwen3-0-6b
spec:
  deploymentMode: OMENative
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b
  engine:
    minReplicas: 2
    maxReplicas: 4
    lifecycle:
      migrationPolicy:
        mode: Never
```

## Deadline and caps {#deadline}

Each request has a deadline: the time OME accepts it, plus the component's `lifecycle.instanceReadyTimeout`, or OME's cluster-wide `instanceReadyTimeout` when the component sets none. The clock runs while a request waits. A paused rollout holds every migration until you resume it, and a request whose deadline passes during the pause fails when you do.

A request that misses its deadline fails with a `MigrationExpired` event. OME removes the surge, if there is one, and the source goes back to `Ready`, or to `Failed` if its pods aren't ready.

A known bug: if neither timeout is set, or the controller's `lifecycle` settings are invalid, every request fails at once with `deadline exceeded in phase Accepted: surge never allocated`. Set a timeout first, as [Set Instance readiness deadlines](../../guides/omenative/set-instance-readiness-deadlines.md) shows.

Migrations also use these controller settings, in the `lifecycle` entry of the `inferenceservice-config` ConfigMap or the chart's `ome.controller.lifecycle`:

| Setting | Chart | Kustomize manifests | What it sets |
| --- | --- | --- | --- |
| `instanceReadyTimeout` | `30m` | `30m` | The deadline, when the component sets none. |
| `stuckPodGracePeriod` | `60s` | Not set | How old a surge pod can get while stuck in a state such as `CrashLoopBackOff` before OME fails the request. Unset leaves it to the deadline. |
| `audit.maxMigrationsPerWindow` | `10` | Not set | How many of a component's migrations can start within `window`. |
| `audit.window` | `1h` | Not set | The window for that cap, and how long finished records stay in `status.migrations`. |
| `audit.maxInFlightMigrations` | `3` | Not set | Migrations running at once. It has no effect, since OME moves one Instance of a component at a time. |
| `autoMigrate.maxAttempts` | `3` | Not set | How many times OME [relocates](#automatic-relocation) an Instance. Unset turns relocation off. |

A request that would go over `maxMigrationsPerWindow` fails, with a `RateLimited` event and a message such as `migration rate cap reached (10/10 in the last 1h0m0s)`. Without all three `audit` settings, as on a kustomize install, a request waits in `Accepted` until you add them or its deadline passes. OME records a `MigrationPolicyUnconfigured` event, and sets a [condition](../architecture/deployment-modes.md#conditions) of the same name on the InferenceReplica. [The request stays Accepted](../../guides/scale-and-migrate/request-an-instance-migration.md#the-request-stays-accepted) shows how to add them.

## Automatic relocation

OMENative also moves Instances on its own. When a new Instance, or an update of a single-pod Instance, runs out of time or gets stuck, OME can mark it `Failed` and rebuild it on another node, if:

- all the attempt's pods ran on one node, and none failed for a cause in the workload itself, such as an image that can't be pulled;
- the mode is `Auto` or `Surge`;
- the Instance has relocations left in `autoMigrate.maxAttempts`.

The rebuilt Instance also avoids nodes that earlier relocations left. Each relocation adds a `Relocated` record to `status.migrations` and records an `AutoMigrationTriggered` event. After the last one allowed, which also records `AutoMigrationCapReached`, OME stops relocating the Instance until it's next `Ready`. [What OME does with a failed Instance](instances.md#what-ome-does-with-a-failed-instance) has the full rules.

## When a request waits or fails {#what-refuses-a-migration-request}

Once OME has taken a request, the request waits in `Accepted` while:

- another migration of the same component runs: OME moves one Instance of a component at a time, oldest request first;
- an update's surge Instance is in flight on the component;
- the source Instance has another operation in progress, has an unscheduled pod, or isn't `Ready`;
- the `audit` settings aren't set (see [Deadline and caps](#deadline)).

OME fails a request when `from_node` doesn't host the source, when `maxMigrationsPerWindow` is reached or when the deadline passes. The record ends in `Failed`, with a Warning event that says why: see [The migration fails](../../guides/scale-and-migrate/request-an-instance-migration.md#the-migration-fails).

When a move can't start now, `kubectl ome migration start` exits `3` with `migration precondition conflicts or is stale; inspect migration status and retry explicitly`. The usual causes are a paused rollout, an Instance that isn't `Ready` or already has a request, and the mode `Never`. An `ome.io/rollout-rollback` annotation left from a [rollback](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#roll-back-the-canary) blocks it too. [Refusals](../../reference/kubectl-ome/migration.md#start-refusals) lists the others.

A request for a component in RawDeployment or `MultiNode` (deprecated) mode stays on the InferenceService, since those modes have no InferenceReplica to act on it. Remove it yourself:

```bash
kubectl annotate inferenceservice qwen3-0-6b -n qwen3-0-6b ome.io/migration-request-v1-3f9c2a1e-7b4d-4c8e-9a6f-2d1b5e8c4a70-
```

```output
inferenceservice.ome.io/qwen3-0-6b annotated
```

## Watch migrations

`kubectl ome migration status` shows each migration record and its phase. For older moves, `kubectl ome migration history` also reads the audit ConfigMap, which keeps the last 200 finished migrations until you delete the InferenceService. [Step 3 of Request an Instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md#step-3-follow-the-request) shows both commands, [`kubectl ome migration`](../../reference/kubectl-ome/migration.md) explains their columns and [MigrationStatus](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-MigrationStatus) lists a record's fields.

OME records migration events on the InferenceService. This command lists completed migrations:

```bash
kubectl get events -n qwen3-0-6b --field-selector reason=MigrationCompleted -o custom-columns=KIND:.involvedObject.kind,NAME:.involvedObject.name,MESSAGE:.message
```

```output
KIND               NAME         MESSAGE
InferenceService   qwen3-0-6b   OMENative migration uuid=3f9c2a1e-7b4d-4c8e-9a6f-2d1b5e8c4a70 complete: component=engine instance=0 -> instance=2 (revision=qwen3-0-6b-engine-7b4c2e19)
```

| Reason | Type | When |
| --- | --- | --- |
| `MigrationRequestAccepted` | Normal | A move starts. |
| `MigrationCompleted` | Normal | The surge has replaced the source. |
| `MigrationRequestRejected` | Warning | OME rejected or failed a request, for any reason but its deadline or a stuck surge pod. |
| `MigrationFromNodeMismatch` | Warning | `from_node` doesn't host the source's pods. |
| `MigrationNodeAffinityConflict` | Warning | The source's node affinity requires `from_node`. |
| `RateLimited` | Warning | A cap is reached. |
| `MigrationPolicyUnconfigured` | Warning | The `audit` settings aren't set, so the request waits. |
| `MigrationSurgeCreateBlocked` | Warning | The API server refused a surge pod. OME retries until the deadline. |
| `MigrationSurgeWedged` | Warning | A surge pod is stuck, so OME failed the request. |
| `MigrationExpired` | Warning | A request passed its deadline. |
| `AutoMigrationTriggered` | Normal | OME relocated an Instance. |
| `AutoMigrationCapReached` | Warning | An Instance used its last relocation. |

## Transient scale

A transient scale sets `spec.replicas` on a component's InferenceReplica through the Kubernetes scale subresource. The InferenceService's `status.components.<component>.scaleTargetRef` names the InferenceReplica. Use `kubectl ome scale`, which checks first, or plain `kubectl scale`, which skips those checks.

The count lasts until whatever owns it writes it again. To change it for good, change `minReplicas`, `maxReplicas` or the autoscaler in the InferenceService.

### Who owns the count

The component's autoscaler class decides what writes the count next. With no autoscaling settings, the class is `HPA`, at 80% average CPU utilization. [Component autoscaling](../serving/component-autoscaling.md) explains the classes.

| Class | What replaces your count |
| --- | --- |
| `HPA` | The HorizontalPodAutoscaler that OME creates. |
| `KEDA` | KEDA, from the [ScaledObject](../serving/component-autoscaling.md#keda) that OME creates. |
| `External` | The autoscaler you run. |
| `None` | OME, which sets it back to `minReplicas` within moments. |

### `kubectl ome scale`

!!! note "Alpha"
    This command is alpha. Its flags and behavior can change between releases.

[`kubectl ome scale`](../../reference/kubectl-ome/scale.md) needs `--override-autoscaler --yes`, whatever the autoscaler class. It reports that the API server stored the count, not that the new Instances are up.

The command refuses while a rollout, a migration or other Instance work is in progress on the component, and when the count is outside the component's bounds. It reads the bounds from the InferenceService and its runtime, not from the controller's [replica defaults](../../guides/operate-ome/set-replica-defaults.md). So when neither sets `maxReplicas`, it accepts only `minReplicas`, or 1 when that's unset. [Refusals](../../reference/kubectl-ome/scale.md#refusals) lists every cause.

### When the count changes

- When `spec.replicas` goes up, OME creates Instances at the lowest unused indices.
- When it goes down, OME keeps `Ready` Instances first and lower indices next, and drains and deletes the rest. It keeps an Instance that's migrating, and its surge.
- A count of 0 runs one Instance until OME sets the count back to `minReplicas`. `kubectl ome scale` refuses 0.
- A paused rollout holds scale-up, but OME still scales down.

`kubectl get inferencereplica` shows the new count under `DESIRED`.

## Next steps

- [Request an Instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md): move an Instance off a node, step by step.
- [Request a transient scale](../../guides/scale-and-migrate/request-a-transient-scale.md): change a component's Instance count for a while.
- [Alfred](../scheduling/alfred.md): how Alfred finds Instances to move.
- [Configure pod disruption budgets](../../guides/deploy-models/configure-pod-disruption-budgets.md): limit how many Instances a node drain takes down at once.
