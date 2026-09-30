---
title: Alfred configuration
description: Alfred's config.yaml keys, ome-alfred Helm chart values and workload annotations, with their defaults and limits.
since: v1.3
---

You configure Alfred through its Helm chart, `ome-alfred`, which you install separately from OME. Its policies and safety limits go under the chart's `alfredConfig` value, and `alfred.ome.io/*` annotations override some of them for one InferenceService.

Alfred is alpha. It starts in recommend-only mode, which reports the moves its policies would make and changes no workload. Execute mode, in which it also migrates [OMENative Instances](../../concepts/omenative/instances.md), is a further opt-in. For what the settings do, see [Alfred](../../concepts/scheduling/alfred.md) and [Alfred policies](../../concepts/scheduling/alfred-policies.md).

## config.yaml keys

Set these keys under `alfredConfig` in your values file, and apply them with `helm upgrade`, as [Tune a policy](../../guides/scheduling/run-alfred.md#step-4-tune-a-policy) shows. The chart renders them into the ConfigMap `alfred-config` at each upgrade, so change your values, not the ConfigMap. Helm merges your maps into the chart's defaults key by key, and replaces lists whole. Because the chart has no values schema, Helm silently accepts a key outside `alfredConfig`, and Alfred ignores it.

Alfred parses the document strictly: an unknown key, a duplicate key or a value of the wrong type fails the load with `parse config.yaml: ...`. `schemaVersion` is required, and a missing key takes its default. For an integer or duration key, `0` also means the default, so it can't turn a limit off, and a negative value fails. For a number or boolean key, `0` or `false` stays as set. Durations use Go syntax, such as `90s` or `1h30m`. [Hot reload](#hot-reload) says what happens to a document that fails.

The simulation profiles, `scheduling.profiles`, are under [Scheduling profiles](#scheduling-profiles).

### Mode and loops

| Key | Type | Default | Limits | What it does |
| --- | --- | --- | --- | --- |
| `schemaVersion` | integer | none; required | `1` | The version of the document's format. |
| `mode` | string | `recommend-only` | `recommend-only` or `execute` | `recommend-only` records every outcome as `advisory`. `execute` lets Alfred request migrations, and needs the chart's [`migration.apiVersion: v1`](#migration). See [Recommend-only and execute modes](../../concepts/scheduling/alfred.md#recommend-only-and-execute-modes). |
| `decisionLoopInterval` | duration | `5m` | at least `1s` | How often the leader runs a decision pass. |
| `observationLoopInterval` | duration | `30s` | at least `1s` | How often every replica observes the cluster. Above `30s`, simulations can fail with `ObservationStale`. |
| `earlyTickOn` | list of strings | `[NodeConditionChange, NodeMaintenanceChange]` | `NodeConditionChange` or `NodeMaintenanceChange` | The node changes that make the leader run an extra decision pass. `[]` turns extra passes off. |

`NodeConditionChange` fires when any node condition appears, disappears, or changes its status or last transition time. `NodeMaintenanceChange` fires when a node starts or stops matching a [maintenance trigger](#maintenance-triggers).

### Defragmentation policy

Keys under `policies.defragmentation`:

| Key | Type | Default | Limits | What it does |
| --- | --- | --- | --- | --- |
| `enabled` | boolean | `true` | | Turns the defragmentation policy on or off. |
| `fragmentationThreshold` | number | `0.25` | 0 to 1 | Alfred plans moves in a GPU pool only when its score is above this. The score combines the fragmentation a repack could remove with pending pods' pressure. |
| `aggressiveness` | string | `balanced` | `conservative`, `balanced` or `aggressive` | How heavily a move's cost counts against its gain: most with `conservative`, least with `aggressive`. It changes only how moves rank. |
| `scoring.sizeLadder` | list of integers | `[1, 2, 4, 8]` | positive, ascending, no duplicates | The per-node GPU counts that Alfred measures fragmentation against. Empty means the default. |
| `scoring.demandBlendLambda` | number | `0.3` | 0 to 1 | How Alfred blends the demand it observes with `sizePrior`: `0` uses only observed demand, `1` only the prior. |
| `scoring.sizePrior` | map of size to weight | `"1": 0.1`, `"2": 0.1`, `"4": 0.2`, `"8": 0.6` | keys from `sizeLadder`; weights 0 or more, summing to 1 | The share of demand Alfred expects for each size. Empty means the default. |
| `scoring.pendingUrgencyTauMinutes` | integer | `30` | positive | How fast a pending pod adds pressure: the higher the value, the slower. |

Helm merges your `sizePrior` into the chart's, so a size you leave out keeps the chart's weight. When you shorten `sizeLadder`, set each size you drop to `null`, or Alfred rejects the document:

```yaml
alfredConfig:
  policies:
    defragmentation:
      scoring:
        sizeLadder: [1, 2, 4]
        sizePrior:
          "1": 0.2
          "2": 0.3
          "4": 0.5
          "8": null
```

### Node-health policy

Keys under `policies.nodeHealth`. Alfred reads node conditions that another agent sets, such as a GPU health checker.

| Key | Type | Default | Limits | What it does |
| --- | --- | --- | --- | --- |
| `enabled` | boolean | `true` | | Turns the node-health policy on or off. With `false`, Alfred records no node signals and plans no evacuations. |
| `aggressiveness` | string | `balanced` | `conservative`, `balanced` or `aggressive` | Checked, but has no effect. |
| `triggerConditions` | list of strings | `[GpuUnhealthy]` | valid condition types, no duplicates | The node conditions that decide a node's health. Empty means the default: to stop evacuations, set `enabled: false` or `signalOnly: true`. |
| `signalOnly` | boolean | `false` | | With `true`, Alfred still records each node's `RemediationSignal` but plans no evacuations. |
| `healthCooldownFloorMinutes` | integer | `5` | positive | In execute mode, the cooldown for moves off an unhealthy node: this many minutes after both the InferenceService's last migration and its pods' start. It replaces `perWorkloadCooldownMinutes`, `recentPlacementCooldownMinutes` and `alfred.ome.io/cooldown-minutes` for these moves. |
| `nodeSuspicionWindowMinutes` | integer | `30` | positive | How long a node stays suspect after a trigger condition turns `False`. |
| `maintenance.triggers` | list | `[]` | see [Maintenance triggers](#maintenance-triggers) | The signals that mark a node for planned maintenance. |

A node is unhealthy while one of its trigger conditions is `True`, and Alfred plans to move its Instances off it. Suspect and unknown nodes aren't move targets. [Health states](../../concepts/scheduling/alfred-policies.md#health-states) defines the states.

### Maintenance triggers

`policies.nodeHealth.maintenance.triggers` lists the node conditions, labels and taints that mark a node for planned maintenance. The list is empty by default, so add a trigger for each signal that your maintenance tooling sets. Each trigger has a `name` and exactly one of `condition`, `label` or `taint`:

| Field | Required | What it does |
| --- | --- | --- |
| `name` | yes | A unique name for the trigger, shown in the node's `RemediationSignal`. |
| `condition.type` | with `condition` | Matches a node condition of this type. |
| `condition.status` | with `condition` | The condition's status: `"True"`, `"False"` or `"Unknown"`. Quote it: unquoted, YAML reads `True` and `False` as booleans, and Alfred rejects the document. |
| `label.key` | with `label` | Matches a node label with this key. |
| `label.value` | no | The label's value. Without it, any value matches; `""` matches only an empty value. |
| `taint.key` | with `taint` | Matches a node taint with this key. |
| `taint.value` | no | The taint's value, matched like `label.value`. |
| `taint.effect` | no | `NoSchedule`, `PreferNoSchedule` or `NoExecute`. Without it, any effect matches. |

A node is due for maintenance when it matches any trigger. Maintenance evacuations use the cooldowns under [Workload defaults](#workload-defaults), not `healthCooldownFloorMinutes`.

```yaml
alfredConfig:
  policies:
    nodeHealth:
      maintenance:
        triggers:
          - name: maintenance-pending
            condition:
              type: example.com/MaintenancePending
              status: "True"
          - name: patching-label
            label:
              key: maintenance.example.com/state
              value: patching
          - name: patching-taint
            taint:
              key: maintenance.example.com/patching
              effect: NoSchedule
```

### Workload defaults

These keys apply to every InferenceService, and the [workload annotations](#workload-annotations) override some of them for one. Only `defaultMovable` and the defragmentation policy's use of `perWorkloadCooldownMinutes` apply in recommend-only mode. [Cooldowns](../../concepts/scheduling/alfred-policies.md#cooldowns) explains how the cooldowns combine.

| Key | Type | Default | Limits | What it does |
| --- | --- | --- | --- | --- |
| `defaultMovable` | boolean | `true` | | Whether Alfred may plan moves for an InferenceService without an `alfred.ome.io/movable` annotation. |
| `recentPlacementCooldownMinutes` | integer | `10` | positive | How long after an Instance's pods start, whoever placed them, before Alfred may move it. |
| `perWorkloadCooldownMinutes` | integer | `30` | positive | How long after an InferenceService's last migration finished, whoever requested it, before Alfred plans another move for it. Inside the window, the defragmentation policy gives its Instances `OMENativeStateIneligible` advice, in both modes. |
| `perNodeCooldownMinutes` | integer | `10` | positive | How long after one of Alfred's own requests used a node, as its source or a target, before Alfred requests another move off or onto it. An unhealthy-node evacuation may still leave the node. |

### Safety bounds

| Key | Type | Default | Limits | What it does |
| --- | --- | --- | --- | --- |
| `maxInFlightMigrations` | integer | `3` | positive | In execute mode, how many moves the arbiter may admit, counting the migrations already in progress in the cluster. Alfred still sends at most one request a pass, and none while any migration is in progress. A higher value lets it try the next admitted move when one fails its final checks. |
| `maxMigrationsPerHour` | integer | `10` | positive | In execute mode, the most migrations that may start in the cluster in any rolling hour, whoever requested them. |
| `emergencyPendingAgeMinutes` | integer | `10` | positive | How long a pod that can't be seated must be pending before it's an emergency. A defragmentation move that would seat it scores double and ignores maintenance windows. [`allowCrossTenantOptimization`](#spot-nodes-and-tenants) decides which pods count. |

### Maintenance windows

`maintenanceWindows` limits when Alfred plans defragmentation moves. Don't confuse it with [maintenance triggers](#maintenance-triggers), which mark nodes for evacuation. Each window has these fields:

| Field | Type | Limits | What it does |
| --- | --- | --- | --- |
| `days` | list of strings | not empty; `Mon`, `Tue`, `Wed`, `Thu`, `Fri`, `Sat` or `Sun` | The days the window is open, in UTC. |
| `start` | string | `HH:MM`, before `end` | When the window opens, in UTC. This minute is inside the window. |
| `end` | string | `HH:MM` | When the window closes, in UTC. This minute is outside the window. |

By default there are no windows, and Alfred may plan defragmentation at any time. Outside your windows, in both modes, it drops its planned defragmentation moves, except emergencies, and keeps its advice. Node-health and maintenance evacuations ignore windows. A window can't cross midnight, so split one that does into two.

```yaml
alfredConfig:
  maintenanceWindows:
    - days: [Sat, Sun]
      start: "02:00"
      end: "06:00"
```

### Spot nodes and tenants

| Key | Type | Default | What it does |
| --- | --- | --- | --- |
| `spotPolicy.avoidAsTarget` | boolean | `true` | Keeps Alfred's moves off spot nodes, and leaves their free GPUs out of the fragmentation score. |
| `spotPolicy.preferAsSource` | boolean | `true` | Multiplies the score of a defragmentation move off a spot node by 1.25. |
| `spotPolicy.preemptibleLabels` | list of strings | `[node.kubernetes.io/preemptible, cloud.google.com/gke-preemptible]` | The node labels that mark a spot node, when set to any value but `false`. `[]` marks none. |
| `allowCrossTenantOptimization` | boolean | `true` | With `true`, a pending pod in another namespace can make a move an emergency when its InferenceService shares the moved one's non-empty `alfred.ome.io/tenant-group`. Pods in the moved InferenceService's namespace always count. |

### Deployment modes

Alfred migrates only OMENative Instances. For RawDeployment and LeaderWorkerSet (deprecated) workloads, it records advice at most.

| Key | Type | Default | What it does |
| --- | --- | --- | --- |
| `omenativeMigrationEnabled` | boolean | `true` | Whether Alfred plans moves for OMENative Instances. With `false`, they get `MigrationSurfaceDisabled` advice. |
| `rawDeploymentMigrationEnabled` | boolean | `false` | Reserved, with no effect: RawDeployment workloads always get `RawDeploymentMigrationUnsupported` advice. |
| `lwsRecommendationsEnabled` | boolean | `true` | Whether Alfred records `LWSMigrationUnsupported` advice for LeaderWorkerSet workloads. With `false`, it leaves them out of its record. |

Alfred reads each component's mode from the InferenceService, never from its runtime. So a component that's OMENative only through its runtime gets `RawDeploymentMigrationUnsupported` advice. To let Alfred move it, declare the mode in the InferenceService, as [An OMENative component shows RawDeploymentMigrationUnsupported](../../guides/scheduling/run-alfred.md#an-omenative-component-shows-rawdeploymentmigrationunsupported) shows.

### Output and logging

| Key | Type | Default | What it does |
| --- | --- | --- | --- |
| `recommendationsConfigMapEnabled` | boolean | `true` | Whether Alfred writes its latest recommendations to the recommendations ConfigMap. |
| `recommendationsConfigMapName` | string | `alfred-recommendations` | The recommendations ConfigMap, in Alfred's namespace. The chart creates it, and Alfred can't, so change the name only in your values. [`kubectl ome admin recommendations`](../kubectl-ome/admin.md) reads the name from Alfred's configuration. |
| `logLevel` | string | `info` | Has no effect. |
| `structuredLogging` | boolean | `true` | Has no effect. |

Alfred logs JSON at `info` level. The chart has no value that changes its logging.

## Hot reload

Every Alfred replica watches the ConfigMap and reloads `config.yaml` when it changes, without a restart. A new document applies from the next decision pass, and each replica logs `config reloaded`.

When a document fails to parse or validate, or the ConfigMap has no `config.yaml` key, each replica keeps its last good configuration. It logs `config reload failed; keeping last-known-good` and counts the failure in `alfred_policy_reload_total{outcome="failure"}`. It also records its own `PolicyReloadFailed` Warning event on the ConfigMap, which gives the first problem Alfred found. [A configuration change doesn't take effect](../../guides/scheduling/run-alfred.md#a-configuration-change-doesnt-take-effect) shows how to find these events. A replica also keeps its configuration if you delete the ConfigMap.

A replica that starts with a rejected document has no last good configuration, so it runs Alfred's built-in defaults, which are recommend-only. A `helm upgrade` that changes `alfredConfig` replaces every pod. So after an upgrade with a rejected `alfredConfig`, every replica runs the defaults, even if Alfred ran in execute mode before.

## Workload annotations

Alfred reads these annotations from an InferenceService's `metadata.annotations`, and they override the defaults above for that InferenceService. The policies ignore an invalid value and use the default.

| Annotation | Values | Default | What it does |
| --- | --- | --- | --- |
| `alfred.ome.io/movable` | `true` or `false` | `defaultMovable` | With `false`, Alfred plans no move for the InferenceService: its OMENative Instances get `OMENativeStateIneligible` advice, and its other workloads get only node-health advice. |
| `alfred.ome.io/priority` | a number from 0 to 1 | `0.5` | The score of the InferenceService's node-health and maintenance evacuations. In execute mode, higher scores go first among evacuations of the same kind. Defragmentation ignores it. |
| `alfred.ome.io/cooldown-minutes` | a whole number, 0 or more | `perWorkloadCooldownMinutes` | Replaces `perWorkloadCooldownMinutes` for this InferenceService. `0` means no cooldown. |
| `alfred.ome.io/spot-policy` | `avoid`, `migrate` or `ignore` | follows `spotPolicy` | `avoid` keeps the InferenceService off spot nodes, even with `spotPolicy.avoidAsTarget: false`. `migrate` always gives its defragmentation moves off spot nodes the 1.25 boost, and `ignore` never does. With `ignore`, `avoidAsTarget` still applies. |
| `alfred.ome.io/tenant-group` | any string | none | Groups InferenceServices across namespaces for [`allowCrossTenantOptimization`](#spot-nodes-and-tenants). |
| `alfred.ome.io/opt-out-reason` | any string | none | A note on why you set `alfred.ome.io/movable` to `false`. Alfred ignores it. |

!!! warning "A malformed cooldown stops every migration"
    In execute mode, if any InferenceService's `alfred.ome.io/cooldown-minutes` isn't a whole number of 0 or more, Alfred requests no migration and withholds its admitted moves with `InvalidCooldown`. Recommendations still appear.

To stop Alfred from planning moves for an InferenceService:

```bash
kubectl annotate inferenceservice chat -n prod alfred.ome.io/movable=false
```

```output
inferenceservice.ome.io/chat annotated
```

Alfred sees the change at its next observation and applies it from the next decision pass.

## Helm chart values

!!! note "No release publishes the Alfred image"
    No OME release publishes the chart's default image, `ghcr.io/moirai-internal/alfred:latest`. From a checkout of your release's tag, `make push-alfred-image REGISTRY=<registry> TAG=<tag>` builds the image and pushes it as `<registry>/alfred:<tag>`. Set `global.hub` to `<registry>` and `image.tag` to `<tag>`, as [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md) shows.

| Value | Default | What it sets |
| --- | --- | --- |
| `enabled` | `true` | Whether the chart renders anything. |
| `global.hub` | `ghcr.io/moirai-internal` | The image's registry. The image is `<hub>/<repository>:<tag>`, or `<repository>:<tag>` when `hub` is empty or `repository` contains a `/`. |
| `global.imagePullSecrets` | `[]` | The pods' `imagePullSecrets`, as a list of `name:` entries. |
| `replicaCount` | `3` | The replicas of the Deployment `ome-alfred`. |
| `image.repository` | `alfred` | The image's repository. |
| `image.tag` | `latest` | The image's tag. |
| `image.pullPolicy` | `Always` | The container's `imagePullPolicy`. |
| `resources` | requests `100m` CPU and `256Mi` memory; limits `1` CPU and `1Gi` memory | The container's resources. |
| `nodeSelector` | `{}` | The pods' `nodeSelector`. |
| `tolerations` | `[]` | The pods' `tolerations`. |
| `affinity` | `{}` | The pods' `affinity`. |
| `topologySpreadConstraints` | spread by `kubernetes.io/hostname`, with `maxSkew: 1` and `ScheduleAnyway` | The pods' `topologySpreadConstraints`. `[]` removes them. |
| `metrics.port` | `8080` | The metrics port, which the Service `ome-alfred-metrics` and the `prometheus.io/port` annotation also use. Sets `--metrics-bind-address`. |
| `metrics.serviceMonitor.enabled` | `false` | Renders the ServiceMonitor `ome-alfred`, which needs the Prometheus Operator's CRDs. |
| `metrics.serviceMonitor.additionalLabels` | `{}` | Extra labels on the ServiceMonitor. |
| `metrics.serviceMonitor.interval` | `30s` | The ServiceMonitor's scrape interval. |
| `metrics.serviceMonitor.scrapeTimeout` | `10s` | The ServiceMonitor's scrape timeout. |
| `healthPort` | `8081` | The port of the liveness probe (`/healthz`) and the readiness probe (`/readyz`). Sets `--health-probe-bind-address`. |
| `configMapName` | `alfred-config` | The ConfigMap that the chart renders `alfredConfig` into, and that Alfred watches. Sets `--config-name`. |
| `alfredConfig` | the defaults under [config.yaml keys](#configyaml-keys) | The `config.yaml` document in that ConfigMap. |
| `simulation.configMapName` | `""` | Your [simulation](#simulation) ConfigMap. Empty turns simulation off. Sets `--simulation-workers`. |
| `simulation.timeout` | `10s` | The time limit for each simulation. Positive, at most `1m`. Sets `--simulation-timeout`. |
| `migration.apiVersion` | `""` | `v1` lets Alfred plan moves for OMENative Instances: see [Migration](#migration). Sets `--migration-api-version` and `--migration-service-account=ome-alfred`. |
| `migration.acknowledgementTimeout` | `2m` | How long a migration request may go unacknowledged before Alfred marks it `stalled`. Positive, at most `1h`. Sets `--migration-ack-timeout`. |
| `migration.failureBackoff` | `5m` | How long Alfred waits after a request fails or stalls before it sends another. Positive, at most `1h`. Sets `--migration-failure-backoff`. |

The chart runs Alfred with leader election, using the Lease `alfred.ome.io`, and keeps Alfred's ConfigMaps and Lease in the release namespace. Alfred exits at startup when a duration is out of range or a simulation worker fails to load. It logs `invalid startup configuration` or `unable to configure recommendation simulation` with the error.

### Migration

`migration.apiVersion: v1` lets Alfred plan moves for OMENative Instances, in either mode:

- By default the value is empty, and OMENative Instances get advice that names no target nodes, usually with the reason `OMENativeUnavailable`. This is expected, and [Alfred metrics and events](alfred-metrics-and-events.md) lists the metric and event that also show it.
- With it, each planned move names up to 3 target nodes in `hintTargets`. In recommend-only mode, the move stays advice, with the reason `SimulationRecommendOnly`, or a [scheduling reason](alfred-metrics-and-events.md#scheduling-results) when no profile fits. In execute mode, Alfred may request it.

The value needs `simulation.configMapName`, or the chart fails with `migration.apiVersion requires simulation.configMapName`. Any value but `v1` fails with `migration.apiVersion must be v1 when enabled`. It also renders:

- The migration guard, the ValidatingAdmissionPolicy and binding `ome-alfred-migration-writes`. It fails closed, and lets Alfred's ServiceAccount add only one bounded `ome.io/migration-request-v1-<uuid>` annotation, the same request as [Request an instance migration](../../guides/scale-and-migrate/request-an-instance-migration.md). Its denials start with `Alfred may`. It needs Kubernetes 1.30 or later: on an older cluster, the install or upgrade fails.
- The journal, the ConfigMap `alfred-dispatch-state`, which Helm keeps when you uninstall the chart. Never delete or reset it. If it exists without `state.json`, the chart fails with `existing alfred-dispatch-state is missing state.json; restore its journal before enabling migration`.

A request that isn't acknowledged within `migration.acknowledgementTimeout` becomes `stalled`. Alfred never cancels it, and it blocks every later request. Alfred's first request stalls this way when the OME manager's InferenceReplica controller is off, for example with `--enable-inferencereplica-controller=false`. For the whole setup, see [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md).

## Simulation

Simulation predicts whether your scheduler would place a move's replacement pods. In recommend-only mode, Alfred records the prediction. In execute mode, it requests only moves that the simulation places.

`simulation.configMapName` names a ConfigMap that you create, with `workers.json` and a scheduler configuration file for each worker. The chart mounts it at `/etc/alfred-simulation`. [Set up scheduler simulation for Alfred](../../guides/scheduling/set-up-scheduler-simulation.md#step-4-create-the-simulation-configmap) builds one.

Make the ConfigMap immutable: Alfred loads it once, at startup. To change it, create a new ConfigMap and set `simulation.configMapName` to its name, which rolls the pods. A worker's identity includes a hash of Alfred's build, so a new Alfred image needs a new ConfigMap and new profiles.

Alfred exits at startup when a worker reports an identity other than the one in `workers.json`, or when a worker's scheduler configuration file breaks one of these rules:

- It's a `kubescheduler.config.k8s.io/v1` KubeSchedulerConfiguration that passes Kubernetes' validation.
- It has exactly one profile.
- It sets no `clientConnection.kubeconfig` and no extenders.
- Its enabled plugins and its `pluginConfig` name only Kubernetes' in-tree plugins and OMEGangPack. The upstream `GangScheduling` plugin isn't supported.
- If OMEGangPack runs, it runs at all of PreFilter, Filter, PostFilter, Reserve, Permit and PostBind, as enabling it under `multiPoint` does. It also needs `pluginConfig` args with a positive `gcIntervalSeconds`.

The [OME scheduler](../../concepts/scheduling/ome-scheduler.md)'s configuration, as its chart renders it, meets these rules. The simulator runs the scheduler of Kubernetes v1.35.4, whatever version your cluster runs.

Simulation needs the PodGroup CRD (`scheduling.x-k8s.io/v1alpha1`), which no chart installs. Without it, simulations fail with `CaptureFailed`, and execute mode withholds moves with `SnapshotUnavailable`. See [Gang scheduling](../../concepts/serving/gang-scheduling.md#when-the-podgroup-crd-is-missing).

### Scheduling profiles

The `config.yaml` key `scheduling.profiles` maps each scheduler name that replacement pods use to the simulation worker that models that scheduler. A pod without a `schedulerName` uses `default-scheduler`. Alfred selects only the profile with the pods' scheduler name, with no fallback.

| Key | Type | Default | Limits | What it does |
| --- | --- | --- | --- | --- |
| `scheduling.profiles` | map | `{}` | keys are DNS-1123 subdomain names | One entry for each scheduler name. |
| `scheduling.profiles.<name>.backend` | string | none | required | The worker's backend name in `workers.json`, matched exactly. |
| `scheduling.profiles.<name>.schedulerVersion` | string | none | required | The scheduler version the worker models. |
| `scheduling.profiles.<name>.configurationID` | string | none | required | The digest of the worker's scheduler configuration and Alfred's build. |
| `scheduling.profiles.<name>.gangScheduling` | boolean | `false` | | Whether the worker models gang scheduling. An Instance with more than one pod needs `true`. |

Copy the values from what the worker prints with `--print-profile`, as [Set up scheduler simulation for Alfred](../../guides/scheduling/set-up-scheduler-simulation.md#print-each-profile) shows. A profile that matches no loaded worker makes each of its simulations fail. In recommend-only mode, the recommendation's `scheduling.reason` is `WorkerFailed`. In execute mode, Alfred withholds the move with `SimulationNotFeasible`.

This example has profiles for the default scheduler and for the OME scheduler, which models gangs:

```yaml
alfredConfig:
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

Alfred adds a `scheduling` object to each OMENative recommendation that names an Instance, with the profile it selected, if any, and a `status` and `reason`. [Scheduling results](alfred-metrics-and-events.md#scheduling-results) lists the reasons. In execute mode with `migration.apiVersion: v1`, recommendations have no `scheduling` object: Alfred simulates each move just before it requests it. It withholds the move with `SourceUnsupported` when no profile fits, or with `SimulationNotFeasible` when the simulation fails or returns a status other than `Feasible`.

## Related pages

- [Alfred](../../concepts/scheduling/alfred.md): what Alfred is and how its loops, modes and outputs fit together.
- [Alfred policies](../../concepts/scheduling/alfred-policies.md): how the defragmentation and node-health policies choose moves.
- [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md): install the chart and read its recommendations.
- [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md): turn on execute mode.
- [Alfred metrics and events](alfred-metrics-and-events.md): what Alfred exports and records.
