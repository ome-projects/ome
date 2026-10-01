---
title: kubectl ome admin
description: "Check that a cluster serves the APIs OME needs with admin doctor, and read the moves that Alfred, which is alpha, recommends."
since: v1.3
---

`kubectl ome admin` checks an OME installation. `doctor` confirms that the cluster serves the APIs OME uses, and compares your plugin's version with the image of the controller manager. `recommendations` shows the moves that [Alfred](../../concepts/scheduling/alfred.md), OME's alpha GPU cluster caretaker, recommended in its latest pass.

```text
kubectl ome admin SUBCOMMAND [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`doctor`](#doctor) | Checks that the cluster serves the APIs OME needs. |
| [`recommendations`](#recommendations) | Shows the moves from Alfred's latest decision pass. |

Both subcommands only read, and take no arguments. [Required RBAC](overview.md#required-rbac) lists the permissions they need. Each prints a table with the columns SUBJECT, EVIDENCE and DETAIL, one row per fact. The table cuts long values to fit 80 columns and ends them with `...`, so `NotDiscoverableAtVersion` shows as `NotDiscoverableA...`. `-o json` and `-o yaml` print the whole report, as [Output formats](overview.md#output-formats) describes.

## `doctor`

```text
kubectl ome admin doctor [flags]
```

`doctor` checks that the cluster serves the APIs OME uses, with the kind and scope OME expects. It also reads the `ome-controller-manager` Deployment in the OME namespace, `ome` unless you set `--ome-namespace`. With `--isvc`, it also shows which parts of status one InferenceService carries. Run it after you install or upgrade OME. It doesn't check that the controller is running: [Install OME](../../getting-started/install.md#step-4-verify-the-installation) shows how.

A healthy installation shows `Incomplete` on the `Doctor` row, and `doctor` exits `0`. `Incomplete` is the best state it reports, because no source tells it which version of OME runs. `Violations` means that a required API is missing: `doctor` prints the report, then exits `2`. An unreadable source shows as `Unavailable` with a reason, and doesn't change the exit code. The exception is `--isvc`: when `doctor` can't read that InferenceService, it prints `error: doctor collection failed` instead of a report, and exits `1`.

### Flags {#doctor-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--isvc` | None | Inspect only this exact named InferenceService in the workload namespace |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

### Output fields {#doctor-output-fields}

The table has these rows, in this order:

| SUBJECT | EVIDENCE | DETAIL |
| --- | --- | --- |
| `Doctor` | The summary state: `Incomplete` or `Violations`. | `GET-only; not authorization` |
| `Context` | The kubeconfig context in use. | `Current kubeconfig context` |
| `Workload namespace` | The namespace `doctor` reads the `--isvc` InferenceService in. | `Only selected named GET` |
| `OME namespace` | The namespace `doctor` reads the manager Deployment in. | `Named manager Deployment GET` |
| One row per API, named by its resource | The API's availability. | `Required` or `Optional`, and the reason, if any. |
| `ManagerDeployment` | The outcome of the Deployment read: `Available` or `Unavailable`. | `<namespace>/ome-controller-manager`, and the reason, if any. |
| `SelectedInferenceService` | The outcome of the `--isvc` read: `Available`, or `NotRequested` without `--isvc`. | `<namespace>/<name>`, or `<namespace>/` without `--isvc`. |
| `CLI version` | The version of the plugin. | `CanonicalCandidate` or `Unknown`. |
| `Manager image candidate` | The version in the tag of the manager image. | The image state. |
| `Operator compatibility` | Always `Unverifiable`. | `No canonical running version` |
| `Image-tag comparison` | How the manager image candidate compares with the CLI version. | `Computed; not compatibility` |
| One row per feature | The feature's availability. | `Generation unverifiable` |

`doctor` checks these APIs. A required API that's missing is a violation. An optional API never is.

| Group version | Required | Optional |
| --- | --- | --- |
| `v1` | `pods`, `events`, `configmaps` | None |
| `apps/v1` | `deployments` | `controllerrevisions` |
| `ome.io/v1beta1` | `inferenceservices`, `basemodels`, `servingruntimes`, `clusterbasemodels`, `clusterservingruntimes` | `inferencereplicas`, `autoscalerpolicies`, `trafficmaps`, `benchmarkjobs`, `finetunedweights`, `acceleratorclasses`, `acceleratorquotas`, `workloadclusters` |
| `autoscaling/v2` | None | `horizontalpodautoscalers` |
| `keda.sh/v1alpha1` | None | `scaledobjects` |
| `gateway.networking.k8s.io/v1` | None | `httproutes`, `gateways`, `gatewayclasses` |
| `kueue.x-k8s.io/v1beta1` | None | `workloads`, `localqueues`, `clusterqueues` |

A missing optional API matters only when you use the feature that needs it, such as KEDA for [scale to zero](../../guides/scale-and-migrate/scale-to-zero-with-keda.md) or the Gateway API for [ingress through HTTPRoutes](../../guides/networking/configure-ingress.md).

The output shows only the resource, so `workloads` is Kueue's Workload API. InferenceReplica, AutoscalerPolicy, TrafficMap, AcceleratorQuota and WorkloadCluster are new in v1.3, so a v1.2.2 installation shows them as `NotDiscoverableAtVersion`. AutoscalerPolicy, TrafficMap and WorkloadCluster are alpha, and TrafficMap and WorkloadCluster belong to multi-cluster routing, which is still in development. The `ome-crd` chart installs AutoscalerPolicy only when you set `ome.autoscalerPolicy.enabled`.

An API's availability is one of these:

| Availability | Meaning |
| --- | --- |
| `Available` | API discovery lists the resource, with the kind and scope OME expects. |
| `NotDiscoverableAtVersion` | API discovery doesn't list the resource at this version. The reason is `NotFound` when the whole group version is missing. |
| `Unavailable` | `doctor` couldn't confirm the API, for one of the reasons below. This is never a violation. |

An `Unavailable` API has one of these reasons:

| Reason | Cause |
| --- | --- |
| `Forbidden` | The API server refused the request. Check [Required RBAC](overview.md#required-rbac). |
| `Unauthorized` | The API server rejected your credentials. |
| `Timeout` | The request timed out. |
| `Throttled` | The API server answered 429 Too Many Requests. |
| `ServerError` | The API server answered 500 or 503. |
| `Unreadable` | The request failed in another way, such as a network error. |
| `MalformedPayload` | The document couldn't be decoded, or lists the resource twice or with another kind or scope. |

On the `ManagerDeployment` row, the reason `NotFound` means that the OME namespace has no `ome-controller-manager` Deployment: set `--ome-namespace` to the namespace you installed OME in. The table cuts the reason off, so read it with `-o json`.

The CLI version shows only for a release build of the plugin, such as `v1.3.0`. Other builds show `Unknown`, including release candidates and `-dirty` builds, as [`kubectl ome version`](version.md) explains.

The manager image candidate is the tag of the `manager` container's image in the Deployment's pod template: the version the Deployment declares, not the one that runs. The row shows a version only in the state `SelectedStableTag`:

| Image state | Meaning |
| --- | --- |
| `SelectedStableTag` | The tag is `vX.Y.Z` or `X.Y.Z`, shown as `vX.Y.Z`. |
| `NonCanonicalTag` | Any other tag, such as `latest` or `v1.3.0-rc.1`. |
| `DigestOnly` | The image reference has a digest, with or without a tag. |
| `UnversionedImage` | The image reference has no tag. |
| `Malformed` | The image reference can't be parsed, such as `ome-manager:`. |
| `NoManagerContainer` | The pod template has no container named `manager`. |
| `AmbiguousManagerContainer` | The pod template has more than one container named `manager`. |
| `Unavailable` | `doctor` couldn't read the Deployment or its image. |

The image-tag comparison compares the CLI version with the manager image candidate:

| Comparison | Meaning |
| --- | --- |
| `WithinOneMinor` | The same major version, and minor versions at most one apart. |
| `OutsideOneMinor` | The same major version, and minor versions more than one apart. |
| `MajorMismatch` | Different major versions. |
| `Unknown` | The CLI version or the manager image candidate is empty. |

The feature rows show which status fields the `--isvc` InferenceService has: `Present` or `AbsentOnSelectedObject`, or `NotSelected` without `--isvc`. `Present` only means that the field is there. It may be from an earlier generation, which is what `Generation unverifiable` says.

| Feature | `Present` when the status has |
| --- | --- |
| `Autoscaling` | `autoscaler` in the status of any component. |
| `Canary` | `canary`. |
| `Components` | The status of any component: `engine`, `decoder` or `router`. |
| `Lifecycle` | `lifecycle` in the status of any component. |
| `MigrationHistory` | At least one entry in `migrationHistory`. |
| `Placement` | `placement`, which only alpha multi-cluster placement sets, on the control-plane cluster. |
| `Rollout` | `rollout`. |
| `RolloutCoordination` | `rolloutCoordination`. |
| `RuntimePin` | `pinnedRevisionName` or `lastRuntimeSyncToken`. |
| `ScaleTarget` | `scaleTargetRef` in the status of any component. |
| `Traffic` | `traffic`. |

To see the state behind a feature, use [`rollout`](rollout.md), [`autoscale`](autoscale.md), [`traffic`](traffic.md), [`instance`](instance.md) or [`migration`](migration.md).

`-o wide` shows availability values such as `NotDiscoverableAtVersion` in full, and adds each API's group version, kind and scope, though long cells are still cut. To see every value in full, use `-o json` or `-o yaml`.

`-o json` and `-o yaml` also list a `SourceUnavailable` warning for each source that isn't fully available. Both also give each API, read and feature an evidence level: `Observed` for an `Available` API or read, `Reported` for a feature of the `--isvc` InferenceService, and `Unavailable` otherwise. The image-tag comparison is `Computed`.

### Examples {#doctor-examples}

Check the installation from the context `prod-east`. The cluster has OME v1.3.0, installed with the charts' defaults, and no KEDA, Gateway API or Kueue:

```bash
kubectl ome admin doctor --context prod-east -n team-a
```

```output
SUBJECT                    EVIDENCE              DETAIL
Doctor                     Incomplete            GET-only; not authorization
Context                    prod-east             Current kubeconfig context
Workload namespace         team-a                Only selected named GET
OME namespace              ome                   Named manager Deployment GET
controllerrevisions        Available             Optional
deployments                Available             Required
horizontalpodautoscalers   Available             Optional
gatewayclasses             NotDiscoverableA...   Optional NotFound
gateways                   NotDiscoverableA...   Optional NotFound
httproutes                 NotDiscoverableA...   Optional NotFound
scaledobjects              NotDiscoverableA...   Optional NotFound
clusterqueues              NotDiscoverableA...   Optional NotFound
localqueues                NotDiscoverableA...   Optional NotFound
workloads                  NotDiscoverableA...   Optional NotFound
acceleratorclasses         Available             Optional
acceleratorquotas          Available             Optional
autoscalerpolicies         NotDiscoverableA...   Optional
basemodels                 Available             Required
benchmarkjobs              Available             Optional
clusterbasemodels          Available             Required
clusterservingruntimes     Available             Required
finetunedweights           Available             Optional
inferencereplicas          Available             Optional
inferenceservices          Available             Required
servingruntimes            Available             Required
trafficmaps                Available             Optional
workloadclusters           Available             Optional
configmaps                 Available             Required
events                     Available             Required
pods                       Available             Required
ManagerDeployment          Available             ome/ome-controller-manager
SelectedInferenceService   NotRequested          team-a/
CLI version                v1.3.0                CanonicalCandidate
Manager image candidate    v1.3.0                SelectedStableTag
Operator compatibility     Unverifiable          No canonical running version
Image-tag comparison       WithinOneMinor        Computed; not compatibility
Autoscaling                NotSelected           Generation unverifiable
Canary                     NotSelected           Generation unverifiable
Components                 NotSelected           Generation unverifiable
Lifecycle                  NotSelected           Generation unverifiable
MigrationHistory           NotSelected           Generation unverifiable
Placement                  NotSelected           Generation unverifiable
Rollout                    NotSelected           Generation unverifiable
RolloutCoordination        NotSelected           Generation unverifiable
RuntimePin                 NotSelected           Generation unverifiable
ScaleTarget                NotSelected           Generation unverifiable
Traffic                    NotSelected           Generation unverifiable
```

Every required API is `Available`, so the command exits `0`. KEDA, Gateway API and Kueue aren't installed, so their APIs are `NotDiscoverableAtVersion` with the reason `NotFound`. The `ome-crd` chart doesn't install AutoscalerPolicy by default, so `autoscalerpolicies` is missing too, with no reason.

Print only the summary of the report:

```bash
kubectl ome admin doctor --context prod-east -n team-a -o json | jq .content.summary
```

```output
{
  "state": "Incomplete",
  "requiredAPIViolations": 0,
  "unavailableEvidence": 21
}
```

`requiredAPIViolations` counts the missing required APIs. `unavailableEvidence` counts every API, read and feature that isn't `Available` or `Present`, plus the operator compatibility, so it's never 0. Script against the exit code or `requiredAPIViolations`.

Check the context `my-context`, whose cluster has no OME, KEDA, Gateway API or Kueue:

```bash
kubectl ome admin doctor --context my-context
```

```output
SUBJECT                    EVIDENCE              DETAIL
Doctor                     Violations            GET-only; not authorization
Context                    my-context            Current kubeconfig context
Workload namespace         default               Only selected named GET
OME namespace              ome                   Named manager Deployment GET
controllerrevisions        Available             Optional
deployments                Available             Required
horizontalpodautoscalers   Available             Optional
gatewayclasses             NotDiscoverableA...   Optional NotFound
gateways                   NotDiscoverableA...   Optional NotFound
httproutes                 NotDiscoverableA...   Optional NotFound
scaledobjects              NotDiscoverableA...   Optional NotFound
clusterqueues              NotDiscoverableA...   Optional NotFound
localqueues                NotDiscoverableA...   Optional NotFound
workloads                  NotDiscoverableA...   Optional NotFound
acceleratorclasses         NotDiscoverableA...   Optional NotFound
acceleratorquotas          NotDiscoverableA...   Optional NotFound
autoscalerpolicies         NotDiscoverableA...   Optional NotFound
basemodels                 NotDiscoverableA...   Required NotFound
benchmarkjobs              NotDiscoverableA...   Optional NotFound
clusterbasemodels          NotDiscoverableA...   Required NotFound
clusterservingruntimes     NotDiscoverableA...   Required NotFound
finetunedweights           NotDiscoverableA...   Optional NotFound
inferencereplicas          NotDiscoverableA...   Optional NotFound
inferenceservices          NotDiscoverableA...   Required NotFound
servingruntimes            NotDiscoverableA...   Required NotFound
trafficmaps                NotDiscoverableA...   Optional NotFound
workloadclusters           NotDiscoverableA...   Optional NotFound
configmaps                 Available             Required
events                     Available             Required
pods                       Available             Required
ManagerDeployment          Unavailable           ome/ome-controller-manager...
SelectedInferenceService   NotRequested          default/
CLI version                v1.3.0                CanonicalCandidate
Manager image candidate                          Unavailable
Operator compatibility     Unverifiable          No canonical running version
Image-tag comparison       Unknown               Computed; not compatibility
Autoscaling                NotSelected           Generation unverifiable
Canary                     NotSelected           Generation unverifiable
Components                 NotSelected           Generation unverifiable
Lifecycle                  NotSelected           Generation unverifiable
MigrationHistory           NotSelected           Generation unverifiable
Placement                  NotSelected           Generation unverifiable
Rollout                    NotSelected           Generation unverifiable
RolloutCoordination        NotSelected           Generation unverifiable
RuntimePin                 NotSelected           Generation unverifiable
ScaleTarget                NotSelected           Generation unverifiable
Traffic                    NotSelected           Generation unverifiable
error: doctor found required API violations
```

The five required `ome.io` APIs are missing, so the command exits `2`. Check that the context is the one you meant, or [install OME](../../getting-started/install.md). With no `ome-controller-manager` Deployment to read, the image state is `Unavailable` and the comparison is `Unknown`.

## `recommendations`

```text
kubectl ome admin recommendations [flags]
```

`recommendations` shows the moves that Alfred recommended in its latest decision pass, and how far each got. [Alfred](../../concepts/scheduling/alfred.md), OME's alpha GPU cluster caretaker, recommends Instance moves when free GPUs fragment or a node turns unhealthy and, when you let it, asks OMENative to carry them out. It isn't part of a default installation: [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md) installs it with the `ome-alfred` chart. Without Alfred, the command prints a report with the state `Unavailable`, and exits `0`.

The command reads Alfred's configuration, the key `config.yaml` in the ConfigMap `alfred-config`. When the configuration turns it on, the command also reads the record of the latest pass: the key `last-cycle.json` in the recommendations ConfigMap, `alfred-recommendations` by default. Both ConfigMaps are in Alfred's namespace, which is the OME namespace unless you set `--alfred-namespace`. The command reads the configuration as stored. That can differ from the one Alfred runs, since Alfred keeps its last good configuration when a new one fails to load; see [Hot reload](../scheduling/alfred-configuration.md#hot-reload). [Alfred configuration](../scheduling/alfred-configuration.md) describes its keys.

This inspection performs at most two named ConfigMap GETs, each with a ten-second timeout. It does not list resources, read Secrets or change anything. The record name comes from the selected configuration, not a separate command-line override. A namespace-scoped Role granting `get` on those ConfigMaps is enough; no cluster-wide ConfigMap access is needed.

### Flags {#recommendations-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `--alfred-config-key` | `config.yaml` | Key inside Alfred's configuration ConfigMap |
| `--alfred-config-name` | `alfred-config` | Name of Alfred's configuration ConfigMap |
| `--alfred-namespace` | None | Namespace where Alfred is installed (defaults to --ome-namespace) |
| `--ome-namespace` | `ome` | Namespace where the OME control plane is installed |
| `-o`, `--output` | `table` | Output format: table, wide, json, or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

`-n` has no effect, since the command reads only in Alfred's namespace. An empty or invalid value in a namespace or config flag fails with `invalid Alfred namespace or config selection`.

### Output fields {#recommendations-output-fields}

The table has these rows, in this order:

| SUBJECT | EVIDENCE | DETAIL |
| --- | --- | --- |
| `Alfred recommendations` | The report state. | The freshness of the latest pass. |
| `Source namespace` | Alfred's namespace. | `Config and cycle sources` |
| `Config source` | The name of the configuration ConfigMap. | The configuration key. |
| `Record source` | `last-cycle.json` | The name of the recommendations ConfigMap. Shown only when the configuration is `Available`. |
| `ConfigMap config` | The state of the configuration. | The mode in the configuration: `recommend-only` or `execute`. |
| `Latest cycle` | The state of the cycle record. | The mode Alfred recorded for the pass. |
| `Rows` | `<shown> shown/<scanned> scanned` | `<invalid> invalid; <omitted> omitted` |
| One row per recommendation, as `<namespace>/<name>/<component>#<instance>` | The outcome. | `<policy>/<reason>` |
| `Issue`, one row per issue | The issue. | Empty. |
| `Advisory evidence` | `Not authorization` | `Convergence unverified` |
| `Executability` | `Unverifiable` | `Not persisted in cycle` |
| `Scope` | `Latest cycle only` | `Node/scheduling data omitted` |

The last three rows are always the same: they mark the report as advice from the latest pass only.

The `Alfred recommendations` row shows the report state:

| State | Meaning |
| --- | --- |
| `Reported` | The table shows every recommendation in the latest pass. |
| `Partial` | The pass has invalid or duplicated recommendations, or more than 200 valid ones, so the table leaves some out. |
| `Empty` | The pass recorded no recommendations. |
| `Disabled` | The configuration sets `recommendationsConfigMapEnabled: false`, so Alfred records no pass. |
| `Unavailable` | The CLI has no valid configuration or cycle record, or the pass has more than 800 recommendations. The `ConfigMap config` and `Latest cycle` rows show why. |

Its DETAIL is the freshness of the pass, measured against twice the `decisionLoopInterval` in the configuration, which makes 10 minutes by default:

| Freshness | Meaning |
| --- | --- |
| `Recent` | The pass is no older than twice the interval. |
| `Stale` | The pass is older: Alfred hasn't recorded one since. [The command reports Unavailable or Stale](../../guides/scheduling/run-alfred.md#the-command-reports-unavailable-or-stale) lists the causes. |
| `Future` | The pass's timestamp is ahead of your clock, such as from clock skew. |
| `Unavailable` | The CLI has no valid cycle record. |

The `ConfigMap config` and `Latest cycle` rows show the state of each source:

| State | Meaning |
| --- | --- |
| `Available` | The CLI read the source, and it's valid. |
| `NotRead` | `Latest cycle` only: the configuration isn't `Available`, so the CLI didn't read the record. |
| `Disabled` | `Latest cycle` only: the configuration turns the recommendations ConfigMap off. |
| `NotFound` | Alfred's namespace has no such ConfigMap. For the configuration, set `--alfred-namespace` to Alfred's namespace. |
| `KeyAbsent` | The ConfigMap has no such key. On `Latest cycle`, you see it until Alfred records its first pass. |
| `Forbidden` | You may not get the ConfigMap. |
| `Unreadable` | The GET failed in another way, such as by timing out. |
| `IdentityMismatch` | The API server returned a different ConfigMap. |
| `Oversized` | The configuration is larger than 64 KiB, or the record larger than 256 KiB. |
| `UnsupportedSchema` | `ConfigMap config` only: the configuration's `schemaVersion` isn't 1. |
| `Malformed` | The configuration or the record is invalid. |
| `ScanLimitExceeded` | `Latest cycle` only: the pass has more than 800 recommendations, so the table shows none. |

Each recommendation's outcome is one of these; [How Alfred decides](../../concepts/scheduling/alfred.md#how-alfred-decides) explains the stages behind them:

| Outcome | Meaning |
| --- | --- |
| `advisory` | The move is advice that the arbiter didn't evaluate; `-o wide` shows its reason. In recommend-only mode, new moves are `advisory`, and requests left from execute mode keep their status. |
| `rejected` | The arbiter rejected the move against its safety bounds. |
| `withheld` | The arbiter admitted the move, but Alfred didn't request the migration. |
| `admitted` | Reserved reporter value; classified `Unverifiable`, with no execution inferred. |
| `submitted`, `acknowledged`, `completed`, `failed` or `stalled` | The status of the migration request Alfred wrote for the move. |

The CLI leaves out recommendations it doesn't recognize, and every copy of a duplicated one, and counts them as invalid. The `Rows` row counts them, and an `Issue` row names the cause.

A reported `completed` dispatch is Alfred's account, not independent proof of convergence. Verify the workload with [migration status and history](migration.md). Neither a recent timestamp nor a recommendation authorizes a migration or proves Alfred's current health.

The CLI also counts some valid recommendations as invalid, so the report shows `Partial`. These are whole-component ones, with `instance` -1, and, once you set `migration.apiVersion`, OMENative ones with a scheduling advisory reason, such as `SimulationUnavailable`. To see them, read `last-cycle.json` in the recommendations ConfigMap, as [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md#step-2-read-its-recommendations) shows. This is a known bug.

An `Issue` row appears for each of these issues:

| Issue | Meaning |
| --- | --- |
| `ConfigRecordModeMismatch` | The mode in the configuration differs from the mode Alfred recorded for the pass. |
| `DuplicateCandidates` | The CLI left out duplicated recommendations. |
| `InvalidRows` | The CLI left out recommendations that it counts as invalid. |

`-o wide` cuts no cell. Under each recommendation, it adds a `Reason code` row for each reason code the recommendation has. It ends with a few more rows, such as the freshness window and the pass's timestamp, as the second example shows.

Only `-o json` and `-o yaml` show warnings. They carry a code and no message:

| Warning | When |
| --- | --- |
| `SourceUnavailable` | The state is `Unavailable`. |
| `PartialData` | The state is `Partial`. |
| `StaleEvidence` | The freshness is `Stale` or `Future`. |
| `Truncated` | The pass has more than 200 valid recommendations or more than 800 in all, or the configuration or the record is oversized. |

### Examples {#recommendations-examples}

Show Alfred's latest pass:

```bash
kubectl ome admin recommendations
```

```output
SUBJECT                    EVIDENCE            DETAIL
Alfred recommendations     Reported            Recent
Source namespace           ome                 Config and cycle sources
Config source              alfred-config       config.yaml
Record source              last-cycle.json     alfred-recommendations
ConfigMap config           Available           recommend-only
Latest cycle               Available           recommend-only
Rows                       3 shown/3 scanned   0 invalid; 0 omitted
team-a/chat/engine#0       advisory            defragmentation/Fragmentation
team-a/chat/engine#1       advisory            defragmentation/Fragmentation
team-b/summarize/engi...   advisory            nodehealth/NodeMaintenance
Advisory evidence          Not authorization   Convergence unverified
Executability              Unverifiable        Not persisted in cycle
Scope                      Latest cycle only   Node/scheduling data omitted
```

Alfred runs in recommend-only mode, so the moves are `advisory`. The third row is `team-b/summarize/engine#0`, cut short.

If Alfred runs in another namespace, such as `caretaker`, name it with `--alfred-namespace`. Add `-o wide` to see each recommendation's reason code:

```bash
kubectl ome admin recommendations --alfred-namespace caretaker -o wide
```

```output
SUBJECT                     EVIDENCE               DETAIL
Alfred recommendations      Reported               Recent
Source namespace            caretaker              Config and cycle sources
Config source               alfred-config          config.yaml
Record source               last-cycle.json        alfred-recommendations
ConfigMap config            Available              recommend-only
Latest cycle                Available              recommend-only
Rows                        3 shown/3 scanned      0 invalid; 0 omitted
team-a/chat/engine#0        advisory               defragmentation/Fragmentation
team-a/chat/engine#0        Reason code            OMENativeUnavailable
team-a/chat/engine#1        advisory               defragmentation/Fragmentation
team-a/chat/engine#1        Reason code            OMENativeUnavailable
team-b/summarize/engine#0   advisory               nodehealth/NodeMaintenance
team-b/summarize/engine#0   Reason code            OMENativeUnavailable
Advisory evidence           Not authorization      Convergence unverified
Executability               Unverifiable           Not persisted in cycle
Scope                       Latest cycle only      Node/scheduling data omitted
Config authority            SelectedConfigMap      Alfred may retain last-known-good config
Cycle authority             AlfredReportedCycle    Does not prove policy loop health
Freshness window            600s                   2 x declared decision loop interval
Cycle timestamp             2026-10-01T09:27:41Z
```

Each recommendation has the advisory reason `OMENativeUnavailable`: Alfred plans moves for OMENative Instances only once you set `migration.apiVersion`, as [Recommend-only and execute modes](../../concepts/scheduling/alfred.md#recommend-only-and-execute-modes) explains.

Print the first recommendation as JSON:

```bash
kubectl ome admin recommendations -o json | jq '.content.recommendations[0]'
```

```output
{
  "workload": "team-a/chat",
  "component": "engine",
  "instance": 0,
  "policy": "defragmentation",
  "reason": "Fragmentation",
  "outcome": "advisory",
  "classification": "Advisory",
  "executability": "Unverifiable",
  "advisoryReason": "OMENativeUnavailable"
}
```

On a cluster without Alfred:

```bash
kubectl ome admin recommendations
```

```output
SUBJECT                  EVIDENCE            DETAIL
Alfred recommendations   Unavailable         Unavailable
Source namespace         ome                 Config and cycle sources
Config source            alfred-config       config.yaml
ConfigMap config         NotFound
Latest cycle             NotRead
Rows                     0 shown/0 scanned   0 invalid; 0 omitted
Advisory evidence        Not authorization   Convergence unverified
Executability            Unverifiable        Not persisted in cycle
Scope                    Latest cycle only   Node/scheduling data omitted
```

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | `doctor`, when no required API is missing. `recommendations`, whenever it prints a report. |
| `1` | General error | Both subcommands, for an argument, an invalid flag value, a kubeconfig the CLI can't use, or Ctrl-C. `doctor` also returns it when it can't read the `--isvc` InferenceService or runs longer than 30 seconds. The command then prints no report. |
| `2` | Assertion unmet | `doctor`, after it prints the report, when the summary state is `Violations`. It then prints `error: doctor found required API violations`. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Run Alfred in recommend-only mode](../../guides/scheduling/run-alfred.md)
- [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md)
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
