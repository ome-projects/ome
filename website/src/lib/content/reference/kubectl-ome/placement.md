---
title: kubectl ome placement
description: "Inspect the alpha multi-cluster placement of an InferenceService: its reported status, the workload clusters its selectors match and its routing table."
status: preview
since: v1.3
---

`kubectl ome placement` shows where an [InferenceService](../../concepts/serving/inference-services.md) runs across workload clusters. It shows the placement that the control plane reports, the [WorkloadClusters](../api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster) that the InferenceService's selectors match and the routing table in its [TrafficMap](../../concepts/rollouts-and-traffic/traffic-map.md). The command reads multi-cluster placement and routing, which are alpha, still in development and off by default, so what it reports can change between releases. [Turn on routing](../../concepts/rollouts-and-traffic/traffic-map.md#turn-on-routing) shows the Helm values for the control-plane cluster. Run the command with the control plane as your current context. It reads what the control plane recorded and changes nothing. [`kubectl ome cluster`](cluster.md) shows the workload clusters themselves.

```text
kubectl ome placement SUBCOMMAND INFERENCESERVICE [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`status`](#status) | Shows the placement phase and the workload clusters chosen for the InferenceService, with their endpoints. |
| [`explain`](#explain) | Shows which workload clusters its placement selectors match, and sums up its routing settings. |
| [`endpoint`](#endpoint) | Shows its routing table: each cluster's endpoint, weight, drains and recorded probe. |

Each subcommand prints a FIELD and VALUE table, one row per fact. The table cuts long field names and values and ends them with `...`, so `Selector compat...` is the Selector compatible row. The `-o wide` output adds rows, and `-o json` and `-o yaml` print the whole report, uncut, as [Output formats](overview.md#output-formats) describes. The problems that the CLI finds are in the Issue rows and in `content.issues`, and `warnings` is always empty. [Required RBAC](overview.md#required-rbac) lists the permissions that each subcommand needs.

The CLI calls a workload cluster in the InferenceService's `status.placement.candidates` a home, and an entry of its TrafficMap a route home. These rules hold in all three tables:

- An address shows only its origin: its scheme, host and port. It reads `NotRecorded` when it's missing, and `InvalidEndpoint` when the URL is malformed or uses a scheme other than `http` or `https`. In JSON, `pathPresent` says whether the URL had a path.
- An unrecognized mode or phase reads `Unknown`, and an unrecognized reason reads `OtherReportedReason`.
- Homes, clusters and route homes are sorted by name. With more than four, the table shows a preview row such as Home preview `4/6; use -o wide or json`, then the first four. `-o wide` shows the rest.
- The tables end with Hint rows and a View row, `Bounded cells; use -o json for complete identities`.

Freshness values say whether a row describes the current version of its object. The CLI compares the generation that the controller recorded with the object's `metadata.generation`:

| Freshness | Meaning |
| --- | --- |
| `Current` | The controller recorded the object's current generation. |
| `Stale` | The controller recorded an older generation: it hasn't caught up with the latest change. |
| `Unverifiable` | Nothing records a generation, so the CLI can't tell. |
| `Invalid` | The record is malformed or contradicts itself, as when a generation is newer than the object's. |
| `Unavailable` | The CLI couldn't read the object. |

Rows named inspection or inspect show how the CLI read a list, as `<state> <kept>/<total>; truncated=<bool>`, where the report keeps `<kept>` of the `<total>` items:

| State | Meaning |
| --- | --- |
| `Validated` | The CLI read the list. |
| `NotRecorded` | The object records no list. |
| `MalformedPayload`, `ConflictingDuplicates`, `BudgetExceeded` or `Unavailable` | The list is malformed, has entries that disagree, is too long or couldn't be read. |

## `status`

```text
kubectl ome placement status INFERENCESERVICE [flags]
```

`status` shows the placement that the controller reports in the InferenceService's `status.placement`: its phase, and each home with its endpoint. It also shows the placement inputs that the InferenceService declares. OME places an InferenceService that sets `spec.placement.requirements` or `spec.placement.clusterSelector`. Without `spec.placement`, the legacy `ome.io/accelerator-requirements` or `ome.io/cluster-selector` annotation does the same. Placement reads `NotRecorded` for an InferenceService that OME hasn't tried to place, and on a cluster that isn't a control plane. When Placement reads `Pending`, [`explain`](#explain) shows which clusters match and whether they're `Ready`.

### Flags {#status-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#status-output-fields}

All three subcommands start with these rows:

| Row | What it shows |
| --- | --- |
| Mode | The placement mode, `Single`, `All` or `Split`, then `(Declared)` when `spec.placement.mode` sets it, or `(Defaulted)` for the default, `Single`. |
| Placement | The phase that the controller reports, then `(reported)`: `Pending` until a matching workload cluster is connected, `Admitting` until one admits the InferenceService, `Placed` once at least one has, and `Failed` when placement failed for good. `NotRecorded` when the status has no placement. |
| Freshness | Always `Unverifiable`: `status.placement` records no generation. |
| Input source | `Structured` when the InferenceService sets `spec.placement`, and `LegacyAnnotations` otherwise, even when it sets neither annotation. |
| Selectors | Whether the requirements and the cluster selector, from `spec.placement` or the annotations, parse as label selectors: `Valid`, `InvalidSelector`, or `NoRequirements` when neither is set. `BudgetExceeded` when one is too long. |
| Reported cluster | The cluster the controller chose, from `status.placement.cluster`. Only mode `Single` chooses one, so the row is empty in modes `All` and `Split`. |
| Endpoint origin | The origin of `status.placement.endpoint`, which only mode `Single` records. |
| Service origin | The origin of the InferenceService's `status.url`. On a control plane, it's the chosen cluster's endpoint in mode `Single`, and empty in the other modes. |
| Home inspection | How the CLI read `status.placement.candidates`. |
| Provenance inspect | How the CLI read the AutoscalerPolicies and rollout groups recorded for each home. `Unavailable` when a home's record is malformed or too large. |
| Issue | A problem that the CLI found in what it read, as `<group>: <code> (<count>)`, for example `PlacementPhase: UnknownValue (1)`. One row per problem. |

Then each home gets these rows:

| Row | What it shows |
| --- | --- |
| Reported home | The cluster, then its phase: `Admitting` until the cluster admits the InferenceService, then `Admitted`. `NotRecorded` when there's no phase. |
| Home origin | The origin of the home's endpoint. |

`-o wide` adds these rows to each home:

| Row | What it shows |
| --- | --- |
| Admitted replicas | The replicas the home's cluster admitted, as in `2 (Reported)`, or `Unknown` when the count is 0 or missing. |
| Ready replicas | The home's ready replicas, in the same form. |
| Provenance | Whether the controller recorded the AutoscalerPolicies or rollout groups for the home, `Reported` or `NotRecorded`, then `freshness unverifiable`. |
| Policy inspection | How the CLI read the home's AutoscalerPolicies. |
| Group inspection | How the CLI read the home's active rollout groups. |

The table ends with the hint `Placed does not prove replica floor met`: a cluster has admitted the InferenceService, but it may not have the replicas it needs yet.

### Examples {#status-examples}

On a control plane, show the placement of `chat` in `prod`. It sets `spec.placement.mode: All` and `clusterSelector: pool=gpu`, and OME placed it on the two workload clusters labeled `pool=gpu`:

```bash
kubectl ome placement status chat -n prod
```

```output
FIELD                VALUE
Mode                 All (Declared)
Placement            Placed (reported)
Freshness            Unverifiable
Input source         Structured
Selectors            Valid
Reported cluster
Endpoint origin      NotRecorded
Service origin       NotRecorded
Home inspection      Validated 2/2; truncated=false
Provenance inspect   Validated 2/2; truncated=false
Reported home        worker-a (Admitted)
Home origin          https://chat.worker-a.example.com
Reported home        worker-b (Admitted)
Home origin          https://chat.worker-b.example.com
Hint                 Placed does not prove replica floor met
View                 Bounded cells; use -o json for complete identities
```

With `-o wide`, each home also gets its replica counts and provenance. For `worker-a`, these rows follow its Home origin row:

```bash
kubectl ome placement status chat -n prod -o wide
```

```output
Admitted replicas    2 (Reported)
Ready replicas       2 (Reported)
Provenance           NotRecorded; freshness unverifiable
Policy inspection    NotRecorded 0/0; truncated=false
Group inspection     NotRecorded 0/0; truncated=false
```

Provenance reads `NotRecorded` because `chat` references no AutoscalerPolicy or RolloutPolicy.

On a single cluster, for a `chat` that omits `spec.placement`, the same command shows that nothing placed it:

```bash
kubectl ome placement status chat -n prod
```

```output
FIELD                VALUE
Mode                 Single (Defaulted)
Placement            NotRecorded (reported)
Freshness            Unverifiable
Input source         LegacyAnnotations
Selectors            NoRequirements
Reported cluster
Endpoint origin      NotRecorded
Service origin       http://chat.prod.svc.cluster.local:8080
Home inspection      NotRecorded 0/0; truncated=false
Provenance inspect   NotRecorded 0/0; truncated=false
Hint                 Placed does not prove replica floor met
View                 Bounded cells; use -o json for complete identities
```

The only address is the origin of its `status.url`.

## `explain`

```text
kubectl ome placement explain INFERENCESERVICE [flags]
```

`explain` checks the InferenceService's placement selectors against the labels of each registered WorkloadCluster, and sums up the routing settings in its `spec.routing`. A match doesn't make a cluster eligible: `explain` doesn't say where the controller can place the InferenceService.

### Flags {#explain-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#explain-output-fields}

The table starts with the same rows as [`status`](#status-output-fields), without the homes. Then come the routing rows:

| Row | What it shows |
| --- | --- |
| Routing intent | `Declared` when the InferenceService sets `spec.routing` or the deprecated `spec.placement.capacityFactors`, and `Absent` otherwise. `Invalid` when a setting fails the routing checks, for example a capacity factor that isn't positive, and `BudgetExceeded` when there are too many entries. |
| Routing enablement | `spec.routing.enabled`: `OptIn` for `true`, `OptOut` for `false` and `Inherited` when unset. |
| Install routing | Always `Gate/defaults unobserved`: the CLI doesn't read whether routing is on for the installation or what its defaults are. |

When Routing intent isn't `Absent`, four rows follow:

| Row | What it shows |
| --- | --- |
| Capacity factors | Their source, then their count, as in `Routing (1)`: `Routing` for `spec.routing.capacityFactors`, `LegacyPlacement` for the deprecated `spec.placement.capacityFactors`, `Conflict` for both and `Inherited` for neither. |
| Routing probe | `spec.routing.probe`: `Inherited` when unset, `Disabled` when it sets `disabled: true` and `Configured` otherwise. |
| Capacity poll | `spec.routing.capacity`, with the same values. |
| Publisher options | `spec.routing.publisher`: `Configured` or `Inherited`, then the number of options it sets. |

With `-o wide`, a `Configured` probe adds rows that sum up its settings: Probe method, Probe statuses, Probe thresholds, Probe timing and All-failed policy. A `Configured` capacity poll adds Capacity method, Capacity window and Capacity timing. A setting that isn't set reads `-`.

The Fleet rows say how the CLI listed the WorkloadClusters:

| Row | What it shows |
| --- | --- |
| Fleet state | `Observed` when the CLI read every WorkloadCluster, `Partial` when it read only some and `Unavailable` when it read none. |
| Fleet reason | Why the state isn't `Observed`, such as `Forbidden`, `Timeout`, `UnsupportedAPI` when the WorkloadCluster API isn't installed, or `PageBudgetExceeded` above 64 WorkloadClusters. |

Fleet Returned, Fleet Admitted, Fleet Pages, Fleet Complete and Fleet Truncated describe what the CLI read.

Then each cluster gets these rows:

| Row | What it shows |
| --- | --- |
| Cluster | The WorkloadCluster's name. |
| `Selector compat...` | Selector compatible: `True` when the cluster's labels match all the InferenceService's selectors, and `False` otherwise. A selector can also match the cluster's name, as `metadata.name`. `NotApplicable` without a selector, and `Unknown` when a selector is invalid or the cluster's labels are malformed. |
| Reported WLC Ready | The status of the cluster's `Ready` condition, then its freshness, as in `True (Current)`. `Ready` means that the control plane can reach the cluster, not that it has capacity. `Unknown (Unverifiable)` without a `Ready` condition, and `Unknown (Invalid)` when it's malformed or its copies disagree. |
| Condition inspect | How the CLI read the cluster's conditions: `<kept>` counts the `Ready` condition, the only one it reads, and `<total>` all of them. |

`-o wide` adds these rows to each cluster:

| Row | What it shows |
| --- | --- |
| Reported home | Whether the cluster is one of the InferenceService's homes: `True` or `False`. `Unknown` when there's no placement or the CLI couldn't read the homes. |
| Connection source | How the control plane connects to the cluster: `KubeConfigReferenceNotResolved` for a kubeconfig Secret, `ClusterProfileReferenceNotResolved` for a ClusterProfile and `Unknown` otherwise. The values end in `NotResolved` because the CLI doesn't read the Secret or the ClusterProfile. |
| Ready reason | The `Ready` condition's reason when it's `ProbeFailedRetrying`, `ConnectionFailed` or `ClusterProfileUnsupported`, and otherwise `OtherReportedReason`, even for a reachable cluster. `NotRecorded` without a `Ready` condition. |

The table ends with two hints, `WLC Ready is reachability, not capacity` and `Partial fleet: no global eligibility verdict`. The second prints even when the CLI read the whole fleet.

### Examples {#explain-examples}

On the control plane from the [`status` examples](#status-examples), with a third workload cluster, `worker-c`, labeled `pool=cpu`:

```bash
kubectl ome placement explain chat -n prod
```

```output
FIELD                VALUE
Mode                 All (Declared)
Placement            Placed (reported)
Freshness            Unverifiable
Input source         Structured
Selectors            Valid
Reported cluster
Endpoint origin      NotRecorded
Service origin       NotRecorded
Home inspection      Validated 2/2; truncated=false
Provenance inspect   Validated 2/2; truncated=false
Routing intent       Absent
Routing enablement   Inherited
Install routing      Gate/defaults unobserved
Fleet state          Observed
Fleet reason
Fleet Returned       3
Fleet Admitted       3
Fleet Pages          1
Fleet Complete       true
Fleet Truncated      false
Cluster              worker-a
Selector compat...   True
Reported WLC Ready   True (Current)
Condition inspect    Validated 1/1; truncated=false
Cluster              worker-b
Selector compat...   True
Reported WLC Ready   True (Current)
Condition inspect    Validated 1/1; truncated=false
Cluster              worker-c
Selector compat...   False
Reported WLC Ready   True (Current)
Condition inspect    Validated 1/1; truncated=false
Hint                 WLC Ready is reachability, not capacity
Hint                 Partial fleet: no global eligibility verdict
View                 Bounded cells; use -o json for complete identities
```

`pool=gpu` matches `worker-a` and `worker-b`. `chat` sets no `spec.routing`, so Routing intent is `Absent` and the table leaves out the four routing detail rows.

With `-o wide`, these rows follow the Condition inspect row of `worker-a`:

```bash
kubectl ome placement explain chat -n prod -o wide
```

```output
Reported home        True
Connection source    KubeConfigReferenceNotResolved
Ready reason         OtherReportedReason
```

`worker-a` connects through a kubeconfig Secret, and its `Ready` reason, `Reachable`, shows as `OtherReportedReason`. `worker-c` shows `False` for Reported home.

## `endpoint`

```text
kubectl ome placement endpoint INFERENCESERVICE [flags]
```

`endpoint` shows the routing table that the controller computed for the InferenceService, from the TrafficMap with the same name and namespace. It shows the TrafficMap's conditions, whether the publisher acknowledged it, and each route home with its endpoint, weight and recorded probe. These are recorded values: the CLI doesn't probe the endpoints or read the gateway. OME writes the TrafficMap only when routing is on, as [Turn on routing](../../concepts/rollouts-and-traffic/traffic-map.md#turn-on-routing) shows. [Traffic map](../../concepts/rollouts-and-traffic/traffic-map.md) describes the fields.

### Flags {#endpoint-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#endpoint-output-fields}

The table starts with the same rows as [`status`](#status-output-fields), without the homes. The TrafficMap rows follow:

| Row | What it shows |
| --- | --- |
| TrafficMap state | `Observed` when the CLI read a TrafficMap that belongs to this InferenceService, and otherwise `Unavailable`. |
| TrafficMap reason | Why the state is `Unavailable`: `NotFound` when there's no TrafficMap or TrafficMap API, `Forbidden`, `Timeout`, `Cancelled`, `Unreadable`, or `OwnershipUnbound` when the TrafficMap doesn't belong to this InferenceService or one of them is being deleted. |

TrafficMap Returned, Admitted, Pages, Complete and Truncated then describe the read: `1`, `1`, `1`, `true` and `false` when the report uses the TrafficMap. Then come the rows about its contents:

| Row | What it shows |
| --- | --- |
| Entry inspection | How the CLI read `spec.entries`. |
| Probe inspection | How the CLI read the entries' recorded probes. `Unavailable` when a probe is malformed or dated in the future. |
| Condition inspect | How the CLI read the TrafficMap's conditions: `<kept>` counts the ones that the rows below show, and `<total>` all of them. |
| Routing freshness | Whether the routing table describes the current InferenceService: the TrafficMap's `spec.observedISVCGeneration` compared with the InferenceService's `metadata.generation`. [Staleness checks](../../concepts/rollouts-and-traffic/traffic-map.md#staleness-checks) explains both generations. |
| Traffic override | The `OverrideActive` condition, as `<status>: <reason> (<freshness>)`, for example `True: OverridesApplied (Current)`. |
| Capacity fallback | The `CapacityFallback` condition, in the same form. |
| Routable | The `Routable` condition, as `<status>: <reason>`. |
| Routable freshness | The `Routable` condition's freshness and the reason for that freshness, as in `Stale: StaleGeneration`. A current condition reads `Current:`. |
| Published | The `Published` condition, as `<status>: <reason> (<freshness>)`. |
| Publisher | Whether the publisher acknowledged the TrafficMap, from the `Published` condition or `status.published`: `ReportedTrue` or `ReportedFalse`, `NoAcknowledgement` before it records anything, `Unknown` when it records only a generation, and `Invalid` when the record is malformed, contradicts itself or has an unrecognized reason. |
| Publisher fresh | Whether the acknowledgement is for the TrafficMap's current generation, as a Freshness value. `Unverifiable` before the publisher records a generation, and `Invalid` when Publisher is `Invalid`. |

A condition that the TrafficMap doesn't have reads `Unknown: NotRecorded (Unverifiable)`. [Conditions](../../concepts/rollouts-and-traffic/traffic-map.md#conditions) lists each condition's reasons.

While a publish runs, Published reads `False: OtherReportedReason` and Publisher reads `Invalid`. Run the command again when the publish finishes. This is a known bug.

Then each route home gets these rows:

| Row | What it shows |
| --- | --- |
| Route home | The entry's cluster. |
| Endpoint origin | The origin of the entry's endpoint. |
| Final weight | The entry's weight, relative to the other entries, then `healthy=true` or `healthy=false`. [Why a cluster's weight is zero](../../concepts/rollouts-and-traffic/traffic-map.md#why-a-clusters-weight-is-zero) lists why it can be 0. |
| Recorded probe | The result of the entry's last probe: `Passing`, `Failing` or `Unknown`. `NotRecorded` without a probe, and `Invalid` when the probe is malformed or dated in the future. |
| Route evidence | `drains=<count>; gate=<gate>; fallback=<code>`: how many drains hold the home at weight 0, whether its probe gates it, and its capacity fallback code. |
| Probe policy | The hash of the probe settings behind the entry's probe, or `NotRecorded` without a probe. |

`-o wide` adds these rows to each route home. The first three appear only when the entry records its capacity:

| Row | What it shows |
| --- | --- |
| `Capacity proven...` | Capacity provenance, the source of the allocated count: `ControlPlane`, `Endpoint` when the capacity polled from the home was lower, or `UnknownOrDefaultedControlPlane` when the entry names no source. |
| Allocated count | The replicas allocated to the home, as in `2 (Reported)`, or `Unknown` when it's 0. |
| Ready count | The home's ready replicas, in the same form. |
| Probe freshness | `Unverifiable`, since probe results record no generation, or `Invalid` when the probe is malformed or dated in the future. |
| Drain refs | The IDs of the drains that hold the home, separated by commas. `Validated` when the entry lists none. |
| Probe gate | Whether the probe gates the home: `True (Reported)` or `False (Reported)`. `NotRecorded` without a probe. |
| Home cap fallback | A code for why the home falls back to its admitted replicas, such as `Unreachable` or `StaleReport`, or `NotRecorded` when there's none. The entry's `capacity.fallbackReason` has the full text, and [A home falls back to its admitted replicas](../../guides/multi-cluster/tune-routing-capacity-polling.md#a-home-falls-back-to-its-admitted-replicas) explains the common reasons. |

The table ends with two hints, `Routing generation does not date placement` and `Recorded probes only; no CLI network probe`. The first means that Routing freshness says nothing about how current the placement status is.

### Examples {#endpoint-examples}

On the control plane from the [`status` examples](#status-examples), with routing on, while `worker-a` is drained for maintenance:

```bash
kubectl ome placement endpoint chat -n prod
```

```output
FIELD                VALUE
Mode                 All (Declared)
Placement            Placed (reported)
Freshness            Unverifiable
Input source         Structured
Selectors            Valid
Reported cluster
Endpoint origin      NotRecorded
Service origin       NotRecorded
Home inspection      Validated 2/2; truncated=false
Provenance inspect   Validated 2/2; truncated=false
TrafficMap state     Observed
TrafficMap reason
TrafficMap Retu...   1
TrafficMap Admi...   1
TrafficMap Pages     1
TrafficMap Comp...   true
TrafficMap Trun...   false
Entry inspection     Validated 2/2; truncated=false
Probe inspection     Validated 2/2; truncated=false
Condition inspect    Validated 4/4; truncated=false
Routing freshness    Current
Traffic override     True: OverridesApplied (Current)
Capacity fallback    False: CapacityPollingDisabled (Current)
Routable             True: Routable
Routable freshness   Current:
Published            True: Published (Current)
Publisher            ReportedTrue
Publisher fresh      Current
Route home           worker-a
Endpoint origin      https://chat.worker-a.example.com
Final weight         0; healthy=true
Recorded probe       NotRecorded
Route evidence       drains=1; gate=NotRecorded; fallback=NotRecorded
Probe policy         NotRecorded
Route home           worker-b
Endpoint origin      https://chat.worker-b.example.com
Final weight         1; healthy=true
Recorded probe       NotRecorded
Route evidence       drains=0; gate=NotRecorded; fallback=NotRecorded
Probe policy         NotRecorded
Hint                 Routing generation does not date placement
Hint                 Recorded probes only; no CLI network probe
View                 Bounded cells; use -o json for complete identities
```

One drain holds `worker-a` at weight 0, so `worker-b` gets all the traffic and `OverrideActive` is `True`. The TrafficMap reflects the current InferenceService, and the publisher has acknowledged its current generation. The entries record no probe results, so Recorded probe and Probe policy read `NotRecorded`.

With `-o wide`, these rows follow the Probe policy row of `worker-a`:

```bash
kubectl ome placement endpoint chat -n prod -o wide
```

```output
Capacity proven...   ControlPlane
Allocated count      2 (Reported)
Ready count          2 (Reported)
Probe freshness      Unverifiable
Drain refs           maintenance-a
Probe gate           NotRecorded
Home cap fallback    NotRecorded
```

`maintenance-a` is the ID of the drain, added with the alpha [`kubectl ome traffic drain`](traffic.md#drain-and-undrain) command. [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md) shows how to add and withdraw one.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success. The report printed, even when the WorkloadClusters or the TrafficMap were missing or unreadable. | All subcommands |
| `1` | General error: an invalid name, namespace, flag or output format, or the CLI couldn't read the InferenceService, as in `InferenceService read unavailable: NotFound`. | All subcommands |

Errors print to stderr as `error: <message>`.

## Related guides

- [Configure routing health probes](../../guides/multi-cluster/routing-health-probes.md)
- [Weight traffic for heterogeneous clusters](../../guides/multi-cluster/weight-traffic-for-heterogeneous-clusters.md)
- [Tune routing capacity polling](../../guides/multi-cluster/tune-routing-capacity-polling.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
