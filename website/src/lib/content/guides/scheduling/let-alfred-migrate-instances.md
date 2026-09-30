---
title: Let Alfred migrate Instances
description: Turn on Alfred's execute mode, so that it asks OMENative to migrate Instances off fragmented or unhealthy nodes within the limits you set.
since: v1.3
---

In execute mode, [Alfred](../../concepts/scheduling/alfred.md) stops only recommending moves and starts making them. For each move it makes, it writes the same migration request that `kubectl ome migration start` writes, and OMENative moves the Instance as the component's migration policy says. Alfred is alpha.

Alfred can then move any OMENative Instance, in any namespace, that its [defragmentation](../../concepts/scheduling/alfred-policies.md#defragmentation) or [node health](../../concepts/scheduling/alfred-policies.md#node-health) policy picks and its [arbiter](../../concepts/scheduling/alfred-policies.md#the-arbiter) admits, unless you [opt the workload out](../../concepts/scheduling/alfred-policies.md#per-workload-annotations). These guardrails stay in place:

- Alfred's only change to a workload is one request annotation on its InferenceService, and a migration guard that the chart installs rejects any other. Alfred never cordons or drains a node, and never evicts or deletes a pod: see [What Alfred changes in your cluster](../../concepts/scheduling/alfred.md#what-alfred-changes-in-your-cluster).
- Alfred sends one request at a time, and none while any migration request in the cluster is waiting or in progress, yours included.
- Just before it sends a request, Alfred checks the move again, and sends it only when a simulation places every replacement pod on a ready node that the plan allowed.
- Alfred follows its requests in a journal until OMENative reports their results, and never cancels or replaces one.
- Caps, cooldowns, maintenance windows and a failure backoff bound what Alfred moves: see [Safety bounds](../../concepts/scheduling/alfred-policies.md#safety-bounds).

<div class="prerequisites" markdown>

- Alfred installed with the `ome-alfred` chart in the `ome` namespace, in recommend-only mode, and its recommendations reviewed: see [Run Alfred in recommend-only mode](run-alfred.md). Keep every value in the `values.yaml` you installed it with: `helm upgrade -f` resets any value that the file leaves out to the chart's default.
- Kubernetes 1.30 or newer, for the migration guard.
- OME v1.3 or newer, with the [InferenceReplica controller](../../reference/operate-ome/controller-manager-flags.md#controllers) running and its migration caps set, as the ome-resources chart does by default. The manifests in `config/` leave out the caps, and without them the controller holds every request: see [The request stays Accepted](../scale-and-migrate/request-an-instance-migration.md#the-request-stays-accepted).
- InferenceServices that declare OMENative themselves, not only through their runtime: with `spec.deploymentMode: OMENative`, a `leader` or `worker` block, or the `ome.io/deploymentMode` annotation on the component. See [Opt in to OMENative](../../concepts/architecture/deployment-modes.md#opt-in-to-omenative). Alfred only advises on RawDeployment and `MultiNode` (deprecated) components: to let it move a `MultiNode` component, first [move it to OMENative](../omenative/move-from-leaderworkerset.md).
- For an Instance of more than one pod, the [OME scheduler](../../concepts/scheduling/ome-scheduler.md), which is alpha and runs only on Kubernetes 1.35. Alfred can simulate such an Instance only when its pods use the OME scheduler: see [Opt in OMENative components](../operate-ome/ome-scheduler.md#opt-in-omenative-components).
- Helm, kubectl, jq and [kubectl-ome](../../reference/kubectl-ome/overview.md).
- Permission to upgrade the `ome-alfred` release and edit ConfigMaps in `ome`, to read ValidatingAdmissionPolicies, their bindings and InferenceServices cluster-wide, and to read InferenceReplicas, events and ConfigMaps in your workloads' namespaces.

The examples use an InferenceService `chat` in the namespace `prod`, whose engine is OMENative.

</div>

## Step 1: Configure the simulation workers

Alfred sends a request only when a simulation of your scheduler places every replacement pod, so set up the simulation first, as [Set up scheduler simulation for Alfred](set-up-scheduler-simulation.md) shows. Once its predictions show in the recommendation record, go on to Step 2 with the same `values.yaml`.

## Step 2: Turn on the migration API

`migration.apiVersion: v1` tells Alfred that your OME manager accepts v1 migration requests, which Alfred doesn't check. With it, the chart installs the migration guard and the dispatch journal, and Alfred plans each move with target nodes. It still sends nothing while `mode` is `recommend-only`.

Add the block to your `values.yaml`:

```yaml title="values.yaml"
migration:
  apiVersion: v1
```

Upgrade the release:

```bash
helm upgrade ome-alfred oci://ghcr.io/moirai-internal/charts/ome-alfred \
  --version 1.3.0 --namespace ome -f values.yaml
```

Helm upgrades the release and prints its status. Wait for the new pods:

```bash
kubectl rollout status deployment/ome-alfred -n ome
```

```output
deployment "ome-alfred" successfully rolled out
```

When the rollout doesn't finish, see [Alfred exits at startup](set-up-scheduler-simulation.md#alfred-exits-at-startup).

From the next decision pass, Alfred records the moves that it would send as advice with the reason `SimulationRecommendOnly`, and their preferred target nodes as `hintTargets`:

```bash
kubectl get configmap alfred-recommendations -n ome -o jsonpath='{.data.last-cycle\.json}' \
  | jq '.recommendations[] | select(.advisoryReason == "SimulationRecommendOnly") | {workload, component, instance, reason, fromNode, hintTargets}'
```

```output
{
  "workload": "prod/chat",
  "component": "engine",
  "instance": 3,
  "reason": "Fragmentation",
  "fromNode": "node-a",
  "hintTargets": [
    "node-c",
    "node-d"
  ]
}
```

Review these moves: in execute mode, Alfred sends the ones that pass its checks, one at a time. In this release, `kubectl ome admin recommendations` wrongly shows this record as `Partial`, so read it with jq.

Check that the guard is in place:

```bash
kubectl get validatingadmissionpolicy,validatingadmissionpolicybinding ome-alfred-migration-writes -o name
```

```output
validatingadmissionpolicy.admissionregistration.k8s.io/ome-alfred-migration-writes
validatingadmissionpolicybinding.admissionregistration.k8s.io/ome-alfred-migration-writes
```

Then check the journal:

```bash
kubectl get configmap alfred-dispatch-state -n ome -o jsonpath='{.data.state\.json}'
```

```output
{"version":"v1","entries":[]}
```

The chart keeps the journal across upgrades and uninstalls, so a journal from an earlier install can already have entries.

!!! warning "Don't overwrite the journal"
    The journal is Alfred's record of its requests. Don't delete or reset it, and don't `helm rollback` the release, which can put back an old journal. Don't apply manifests that `helm template` renders either, yourself or with a tool such as Argo CD: they carry an empty journal. To undo execute mode, change your `values.yaml` and upgrade, as in [Clean up](#clean-up).

## Step 3: Switch Alfred to execute mode

Before you switch, decide how much Alfred may move. By default, it starts no migration once 10 have started in the cluster in the last hour, whoever started them: to go slower, lower `alfredConfig.maxMigrationsPerHour`. To let Alfred move only the InferenceServices you pick, set `alfredConfig.defaultMovable` to `false`, and annotate them `alfred.ome.io/movable: "true"`: see [Workload annotations](../../reference/scheduling/alfred-configuration.md#workload-annotations). You can also limit defragmentation to [maintenance windows](../../concepts/scheduling/alfred-policies.md#maintenance-windows), which node-health moves ignore.

Set `alfredConfig.mode` to `execute`. With the earlier steps, your `values.yaml` now reads as follows, next to any values you set before:

```yaml title="values.yaml"
global:
  hub: registry.example.com/ome
image:
  tag: v1.3.0@sha256:<digest>
simulation:
  configMapName: alfred-simulation-1
migration:
  apiVersion: v1
alfredConfig:
  mode: execute
  scheduling:
    profiles:
      default-scheduler:
        backend: kube-v135
        schedulerVersion: v1.35.4
        configurationID: sha256:<default-id>
      ome-scheduler:
        backend: ome-v135
        schedulerVersion: v1.35.4
        configurationID: sha256:<ome-id>
        gangScheduling: true
```

Upgrade the release, and wait for the new pods:

```bash
helm upgrade ome-alfred oci://ghcr.io/moirai-internal/charts/ome-alfred \
  --version 1.3.0 --namespace ome -f values.yaml
```

```bash
kubectl rollout status deployment/ome-alfred -n ome
```

```output
deployment "ome-alfred" successfully rolled out
```

After the next decision pass, the record shows the new mode:

```bash
kubectl get configmap alfred-recommendations -n ome -o jsonpath='{.data.last-cycle\.json}' | jq -r .mode
```

```output
execute
```

From now on, each admitted move's `outcome` is its request's status, such as `submitted`, or `withheld` with a `dispatchReason`. A move that the arbiter turns down is `rejected`, with a `rejectReason`: see [The arbiter](../../concepts/scheduling/alfred-policies.md#the-arbiter).

## Step 4: Watch a migration

Alfred sends its first request once a policy picks a move that the arbiter admits and that passes the checks. It records an event on the InferenceService at each step of a request:

```bash
kubectl get events -n prod --field-selector involvedObject.name=chat,source=alfred -o custom-columns=TYPE:.type,REASON:.reason,MESSAGE:.message
```

```output
TYPE     REASON               MESSAGE
Normal   MigrationSubmitted   defragmentation migration request uuid=7c9e6679-7425-40de-944b-e07fc1f90ae7 for prod/chat/engine instance 3: submitted (RequestSubmitted)
```

`MigrationAcknowledged` and `MigrationCompleted` follow, or the Warnings `MigrationFailed` or `MigrationStalled`: see [On an InferenceService](../../reference/scheduling/alfred-metrics-and-events.md#on-an-inferenceservice).

The recommendation record shows each admitted move, with its request's status and ID:

```bash
kubectl get configmap alfred-recommendations -n ome -o jsonpath='{.data.last-cycle\.json}' \
  | jq '.recommendations[] | select(.dispatchStatus) | {workload, component, instance, dispatchStatus, dispatchReason, requestUUID}'
```

```output
{
  "workload": "prod/chat",
  "component": "engine",
  "instance": 3,
  "dispatchStatus": "submitted",
  "dispatchReason": "RequestSubmitted",
  "requestUUID": "7c9e6679-7425-40de-944b-e07fc1f90ae7"
}
```

To block until the migration ends, pass its `requestUUID` to `kubectl ome wait`:

```bash
kubectl ome wait chat -n prod --for=migration=terminal --request-id=7c9e6679-7425-40de-944b-e07fc1f90ae7 --timeout=30m
```

The command exits `0` once the migration ends, even when it failed, so check the outcome:

```bash
kubectl ome migration status chat -n prod --component engine
```

```output
SUBJECT/COMP      STATUS                       DETAIL
7c9e6679/engine   Completed/Terminal/Current   MSG: migrated to instan...
```

[Step 3: Follow the request](../scale-and-migrate/request-an-instance-migration.md#step-3-follow-the-request) explains these columns. After a request fails or stalls, Alfred waits `migration.failureBackoff`, 5 minutes by default, before it sends another.

## Troubleshooting

### The chart fails to render

`helm upgrade` stops with an error that ends in one of these messages:

- `migration.apiVersion must be v1 when enabled`: set `migration.apiVersion` to `v1`, or leave it empty.
- `migration.apiVersion requires simulation.configMapName`: set up the simulation first, as in [Step 1](#step-1-configure-the-simulation-workers).
- `existing alfred-dispatch-state is missing state.json; restore its journal before enabling migration`: put the journal's `state.json` back before you set `migration.apiVersion`.

### Moves stay advisory

Check each move's `advisoryReason` in the record:

- `OMENativeUnavailable`: `migration.apiVersion` isn't set. See [Step 2](#step-2-turn-on-the-migration-api).
- `SimulationRecommendOnly`: `mode` is still `recommend-only`, or the record is older than your upgrade. See [Step 3](#step-3-switch-alfred-to-execute-mode).
- A scheduling reason, such as `ProfileNotConfigured` or `GangUnsupported`: the simulation has no prediction for the move. See [Check the predictions](set-up-scheduler-simulation.md#step-6-check-the-predictions).
- `RawDeploymentMigrationUnsupported` on an OMENative component: the component is OMENative only through its runtime. See [Before you begin](#before-you-begin).
- `MigrationSurfaceDisabled`: `alfredConfig.omenativeMigrationEnabled` is `false`.
- `OMENativeStateIneligible`: a migration of the InferenceService ended less than 30 minutes ago, the default `perWorkloadCooldownMinutes`. Or the InferenceService isn't movable, the component's migration policy is `Never`, or the component isn't steady, for example during a rollout.

[Which Instances Alfred can move](../../concepts/scheduling/alfred-policies.md#which-instances-alfred-can-move) gives the conditions.

### Admitted moves are withheld

Check each withheld move's `dispatchReason` in the record:

| Reason | What to do |
| --- | --- |
| `GuardUnavailable` | The guard is missing, changed, or not type-checked yet. Check it as in [Step 2](#step-2-turn-on-the-migration-api), then compare its generations, as below. |
| `JournalUnavailable` | Alfred can't read or write `alfred-dispatch-state`, or its `state.json` is invalid. |
| `SnapshotUnavailable` | Alfred can't copy the cluster, for example because the PodGroup CRD is missing. |
| `SourceUnsupported` | No profile fits the Instance's pods, their node affinity still differs from the InferenceReplica's template after an earlier migration, or the node's `kubernetes.io/hostname` label differs from its name. |
| `SimulationNotFeasible`, `PredictedTargetUnsafe` | The last simulation didn't place every replacement pod on a ready node that the plan allowed. |
| `UnresolvedRequest`, `MultipleUnresolvedRequests` | One of Alfred's requests is still under way, as expected during a migration. If it has stalled, see [A request is stalled](#a-request-is-stalled). |
| `FailureBackoff` | One of Alfred's requests failed or stalled less than `migration.failureBackoff` ago. |
| `InFlightCap` | A migration request is waiting or in progress somewhere in the cluster. List the waiting requests, as below. |
| `InvalidCooldown` | An InferenceService has an invalid `alfred.ome.io/cooldown-minutes` annotation. Find it, as below. |
| `SerialDispatchLimit` | Alfred tried another move in this pass. When the tried move shows it too, its request didn't reach the InferenceService: see [A request is stalled](#a-request-is-stalled). |
| Another reason | See [Dispatch reasons](../../reference/scheduling/alfred-metrics-and-events.md#dispatch-reasons). |

When moves show `GuardUnavailable` while the guard is in place, compare the policy's generations:

```bash
kubectl get validatingadmissionpolicy ome-alfred-migration-writes \
  -o jsonpath='{.metadata.generation} {.status.observedGeneration}{"\n"}'
```

```output
1 1
```

Alfred uses the guard only when the two numbers are equal, and the policy's `status.typeChecking` lists no `expressionWarnings`.

To find the requests behind `InFlightCap`, list the waiting request annotations:

```bash
kubectl get inferenceservices -A -o json \
  | jq -r '.items[] | .metadata as $m | ($m.annotations // {}) | keys[] | select(startswith("ome.io/migration-request-v1-")) | "\($m.namespace)/\($m.name) \(.)"'
```

```output
prod/chat ome.io/migration-request-v1-7c9e6679-7425-40de-944b-e07fc1f90ae7
```

The command prints nothing when no request is waiting. Remove an annotation that nothing acts on, such as one that names a component that isn't OMENative. Migrations already under way show in `kubectl ome migration status` instead.

To find an invalid cooldown annotation, list them all, with their values:

```bash
kubectl get inferenceservices -A -o json \
  | jq -r '.items[] | select(.metadata.annotations["alfred.ome.io/cooldown-minutes"] != null) | "\(.metadata.namespace)/\(.metadata.name) \(.metadata.annotations["alfred.ome.io/cooldown-minutes"])"'
```

```output
prod/chat 30m
```

A valid value is a whole number of minutes, such as `30`. Fix or remove any other.

### A request is stalled

A stalled request stays unresolved, so Alfred withholds every later move with `UnresolvedRequest`. Alfred resolves it once the InferenceReplica's status reports that the migration completed or failed. Find the stalled entry in the journal:

```bash
kubectl get configmap alfred-dispatch-state -n ome -o jsonpath='{.data.state\.json}' \
  | jq '.entries[] | select(.phase == "stalled") | {uuid, workload, component, instance, reason, workloadUID, irUID}'
```

```output
{
  "uuid": "9b2d4e71-5c3a-4f8e-a1d2-6e7f8a9b0c1d",
  "workload": {
    "Namespace": "prod",
    "Name": "chat"
  },
  "component": "engine",
  "instance": 3,
  "reason": "AcknowledgementTimeout",
  "workloadUID": "<isvc-uid>",
  "irUID": "<ir-uid>"
}
```

Then check what OME recorded for the InferenceService's migrations:

```bash
kubectl ome migration history chat -n prod
```

```output
EVID    REQUEST/COMP   PHASE/STATE   WHEN           DETAIL
AUTH    7c9e6679/E     Completed/T   09-28T10:31Z   migrated to insta...
AUDIT   7c9e6679/E     Completed/T   09-28T10:31Z   migrated
AUDIT   3f2a9c1e/E     Failed/T      09-28T09:20Z   deadline exceeded...
```

Here, the history has no row for the stalled request, `9b2d4e71`. What to do depends on the entry's `reason`:

| Reason | What to do |
| --- | --- |
| `ConsumerDeadlineExceeded` | Wait: the migration is still under way after its deadline, and the request resolves when it ends. |
| `AcknowledgementTimeout` | Look for the request's annotation, as in [Admitted moves are withheld](#admitted-moves-are-withheld). While it's there, the request goes ahead once the InferenceReplica controller runs. With neither the annotation nor a history row, clear the entry. |
| `OwnerUnavailable`, `ReplicaUnavailable`, `ReplicaIdentityChanged` | Clear the entry once the InferenceService or InferenceReplica is gone, or its `metadata.uid` differs from the entry's `workloadUID` or `irUID`. Check first: a failed read gives the first two reasons too. |
| Another reason | Clear the entry once the history shows that the request ended. |

To clear an entry, edit the journal. In its `state.json` key, remove the stalled entry from `entries`, and leave the rest as it is, including `"entries": []` when no entry is left:

```bash
kubectl edit configmap alfred-dispatch-state -n ome
```

```output
configmap/alfred-dispatch-state edited
```

Alfred reads the journal again at the start of each decision pass. Keep `state.json` valid: while Alfred can't read it, it withholds every move with `JournalUnavailable`. [Journal states](../../reference/scheduling/alfred-metrics-and-events.md#journal-states) explains the reasons.

### Alfred's migrations fail

The journal shows the request as `failed`, and `kubectl ome migration status` shows OMENative's message. When every migration fails at once with `deadline exceeded in phase Accepted: surge never allocated`, no instance-ready timeout is set: see [Deadline](../../concepts/omenative/migration-and-transient-scale.md#deadline). For other messages, see [The migration fails](../scale-and-migrate/request-an-instance-migration.md#the-migration-fails).

Alfred's target nodes are preferences, not reservations: another pod can take one first, and a migration that can't finish by its deadline fails. When too many of Alfred's recent migrations fail, the arbiter rejects every candidate with `CircuitBreakerOpen` for a while: see [Global limits](../../concepts/scheduling/alfred-policies.md#global-limits).

## Clean up

To stop Alfred from sending requests, set `alfredConfig.mode` back to `recommend-only` in your `values.yaml`, and upgrade the release:

```bash
helm upgrade ome-alfred oci://ghcr.io/moirai-internal/charts/ome-alfred \
  --version 1.3.0 --namespace ome -f values.yaml
```

```bash
kubectl rollout status deployment/ome-alfred -n ome
```

```output
deployment "ome-alfred" successfully rolled out
```

Alfred then sends no request, but still follows the requests in its journal.

To turn migration off as well, first wait until this command, which lists the unresolved requests in the journal, prints nothing:

```bash
kubectl get configmap alfred-dispatch-state -n ome -o jsonpath='{.data.state\.json}' \
  | jq -r '.entries[] | select(.phase != "completed" and .phase != "failed") | .uuid'
```

A stalled request can stay unresolved for good: see [A request is stalled](#a-request-is-stalled). Then remove the `migration` block from your `values.yaml`, and upgrade again. The chart removes the guard, and keeps the journal.

Keep the journal, even after you uninstall Alfred: when you turn migration on again, its unresolved requests still hold back new ones. To remove the simulation too, see [Set up scheduler simulation for Alfred](set-up-scheduler-simulation.md#clean-up).

## Next steps

- [Alfred policies](../../concepts/scheduling/alfred-policies.md): the policies, the arbiter and the safety bounds.
- [Alfred configuration](../../reference/scheduling/alfred-configuration.md): every chart value and `alfredConfig` setting.
- [Alfred metrics and events](../../reference/scheduling/alfred-metrics-and-events.md): what Alfred reports.
- [Request an instance migration](../scale-and-migrate/request-an-instance-migration.md): write a request yourself, and follow it.
