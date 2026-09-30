---
title: kubectl ome cluster
description: Show whether OME reports each alpha WorkloadCluster as connected, from the Ready condition that its controller writes.
status: preview
since: v1.3
---

`kubectl ome cluster` shows whether OME reports each of your workload clusters as connected. It reads the [WorkloadClusters](../api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster) in the cluster of your current kubeconfig context, usually the control-plane cluster. A WorkloadCluster registers a cluster that OME can place the replicas of an [InferenceService](../../concepts/serving/inference-services.md) onto, for multi-cluster serving.

The `status` subcommand, the WorkloadCluster API and multi-cluster serving are alpha and still in development, and multi-cluster is off by default. To run the WorkloadCluster controller, set `ome.multicluster.enabled: true` in the `ome-resources` chart, as [Controller manager flags](../operate-ome/controller-manager-flags.md#multi-cluster) shows. Without the controller, nothing reports a WorkloadCluster's status.

```text
kubectl ome cluster SUBCOMMAND [WORKLOADCLUSTER] [flags]
```

| Subcommand | What it does |
| --- | --- |
| [`status`](#status) | Alpha. Shows the WorkloadCluster status that the controller reports. |

## `status`

```text
kubectl ome cluster status [WORKLOADCLUSTER] [flags]
```

`status` shows how each WorkloadCluster declares its connection, and the Ready condition that the controller last wrote in its status. Name a WorkloadCluster to see only that one. Without a name, `status` lists up to 64, sorted by name. You need `get` on `workloadclusters` in the `ome.io` API group to read one by name, and `list` to list them. The reader ClusterRole in [Required RBAC](overview.md#required-rbac) grants both.

`kubectl get wlc` shows the same Ready status in its REACHABLE column. [`kubectl ome get workloadclusters -o wide`](get.md#workloadclusters-columns) adds the Ready condition's reason, and `kubectl describe workloadcluster` with the WorkloadCluster's name shows its message.

!!! warning "READY is the controller's last report"
    READY shows what the WorkloadCluster controller last wrote, not a live check, and covers the connection only. `status` never contacts a workload cluster or reads its kubeconfig Secret. `True` means that the controller's last check succeeded, or that checks have been failing for less than the grace period, 2 minutes by default, with the reason `ProbeFailedRetrying`. If the controller stops, its last report stays `Current` until the WorkloadCluster's spec changes.

[Routing health probes](../../guides/multi-cluster/routing-health-probes.md) check that each workload cluster answers for a service, and take a failing one out of the service's traffic. [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md) takes one out for maintenance, and [`kubectl ome placement`](placement.md) shows where an InferenceService is placed.

### Flags {#status-flags}

| Flag | Default | Description |
| --- | --- | --- |
| `-o`, `--output` | `table` | Output format: table, wide, json or yaml |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

WorkloadClusters are cluster-scoped, so `status` ignores `-n`. To read the WorkloadClusters of another cluster, use `--context`.

### Output fields {#status-output-fields}

The default table fits in 80 columns, and cuts long values with `...`. `-o wide` prints every field in full, one per row, in FIELD and EVIDENCE columns. `-o json` and `-o yaml` print a `ClusterStatusReport`, as [Output formats](overview.md#output-formats) describes.

Each WorkloadCluster gets a row, after four rows marked `@` that describe the read. Fields marked (wide) appear only in `-o wide`, JSON and YAML:

| Column | Wide row, field | What it shows |
| --- | --- | --- |
| CLUSTER | Cluster, `name` | The WorkloadCluster's name. |
| DECLARED | Declared connection, `declaredConnectionKind` | How the spec says to connect to the cluster. |
| READY | Reported Ready, `reportedReady` | The Ready condition's status: `True`, `False` or `Unknown`. It's also `Unknown` when there's no Ready condition that the CLI can use. |
| FRESHNESS | Ready freshness, `freshness` | Whether the controller wrote the Ready condition for the current spec. |
| CONDITIONS | Ready condition state, `conditionState` | Whether the conditions hold one well-formed Ready condition. |
| (wide) | Generation, `generation` | The WorkloadCluster's `metadata.generation`, which goes up each time its spec changes. |
| (wide) | Source state, `sourceState` | `Reported` when DECLARED is valid, and `Malformed` when it's `Invalid`. |
| (wide) | Declared profile, `declaredProfile` | The ClusterProfile's name, for a `ClusterProfile` declaration. |
| (wide) | Profile resolution, `profileResolution` | Always `NotAttempted`. |
| (wide) | Evidence, `evidence` | Always `Reported`. |
| (wide) | Connection state, `connectionState` | `ReportedReady` or `ReportedNotReady` when READY is `True` or `False`, FRESHNESS is `Current`, DECLARED is `KubeConfigSecret` or `ClusterProfile`, and Transition evidence is `Reported`. `Unknown` otherwise. |
| (wide) | Conditions total/scanned/kept, `totalConditions`, `scannedConditions`, `retainedConditions` | How many conditions the WorkloadCluster has, how many the CLI checked and how many it lists. |

DECLARED is one of:

| Value | Meaning |
| --- | --- |
| `KubeConfigSecret` | `spec.clusterSource.kubeConfig` names a Secret by a valid name and namespace, and a valid key. |
| `ClusterProfile` | `spec.clusterSource.clusterProfileRef` names a SIG-Multicluster ClusterProfile by a valid name. The controller doesn't support this source yet, and sets Ready to `False`. |
| `Invalid` | The declaration is invalid, for example a `secretRef` with no `namespace`. |

FRESHNESS is one of:

| Value | Meaning |
| --- | --- |
| `Current` | The Ready condition's `observedGeneration` matches the WorkloadCluster's `metadata.generation`. |
| `Stale` | The spec changed after the controller last wrote the Ready condition. |
| `Unobserved` | The Ready condition is missing or has no `observedGeneration`. |
| `Invalid` | The conditions are malformed, or the Ready condition claims a newer generation than the WorkloadCluster's. |

CONDITIONS is one of:

| Value | Meaning |
| --- | --- |
| `Reported` | There's one Ready condition, and all conditions are well formed. |
| `Missing` | There's no Ready condition yet, for example while multi-cluster is off. |
| `Malformed` | The conditions aren't well formed. READY is `Unknown`. OME's CRDs prevent this. |

A `*` after a DECLARED or CONDITIONS value means that the CLI left some detail out. The table then ends with a row, `@ * = truncated`.

`-o wide`, JSON and YAML then show the conditions (`conditions`), Ready first:

| Wide row | Field | What it shows |
| --- | --- | --- |
| Condition type/status | `type`, `status` | `Ready`, or `Other` for any other type, then `True`, `False` or `Unknown`. |
| Observed generation/freshness | `observedGeneration`, `freshness` | The generation that the condition was written for, and its freshness, as in FRESHNESS. |
| Reason classification | `reason` | `ConnectionFailed`, or `Other` for any other reason. |
| Transition evidence | `transitionState` | `Reported`, or `Malformed` when `lastTransitionTime` is later than your machine's clock. |
| Transition time | `transitionTime` | The condition's `lastTransitionTime`, in UTC. |

The Ready condition's reason says why a WorkloadCluster is Ready or not. `status` shows `ConnectionFailed` as written, but every other reason as `Other`, `Reachable` included. Read the reason from the REASON column of [`kubectl ome get workloadclusters -o wide`](get.md#workloadclusters-columns) instead. This is a known bug. The controller writes these reasons:

| Reason | Ready | Meaning and fix |
| --- | --- | --- |
| `Reachable` | `True` | The controller read the kubeconfig and reached the cluster. |
| `ProbeFailedRetrying` | `True` | Checks of a cluster that was reachable have been failing for less than the grace period, which the `ome-resources` value `ome.multicluster.config.workloadCluster.connectionGrace` sets. |
| `ConnectionFailed` | `False` | The controller can't reach the cluster, read the Secret or set up its client, and no grace period applies. Check the kubeconfig's server and credentials, and the network path. |
| `SecretNotFound` | `False` | The Secret that `secretRef` names doesn't exist. Create it, or fix the name and namespace. |
| `BadKubeConfig` | `False` | The Secret's key is missing or empty, or holds a kubeconfig that the controller can't parse or rejects. An exec credential plugin needs `ome.multicluster.execCredentials.enabled`, as [Controller manager flags](../operate-ome/controller-manager-flags.md#multi-cluster) shows. |
| `ClusterProfileUnsupported` | `False` | The WorkloadCluster uses `clusterProfileRef`, which the controller doesn't support yet. Use `kubeConfig`. |

The four `@` rows are:

| Row | What it shows | Report fields |
| --- | --- | --- |
| `@ observation` | How the read went. | `observation` |
| `@ availability` | Why the read failed, or `-` when it didn't. | `unavailableReason` |
| `@ sources` | WorkloadClusters returned (`returned=`) and kept (`kept=`), the most the CLI keeps (`max=`), and whether it stopped before the end of the list (`cut=`). | `returnedSources`, `admittedSources`, `sourceLimit`, `sourcesTruncated` |
| `@ pages` | Pages read, out of the page limit (`seen=`), requests made (`calls=`) and allowed (`cap=`), and the page size (`size=`). | `observedPages`, `pageLimit`, `requestedPages`, `requestLimit`, `pageSize` |

`-o wide` starts with the `@` rows spelled out, then Condition scan/output limits, always `128/32`, and Collected at (`collectedAt`), when the CLI built the report, in UTC.

`@ observation` is one of:

| Observation | Meaning |
| --- | --- |
| `Complete` | The CLI read every WorkloadCluster, or the one you named. |
| `Empty` | The list worked, and there are no WorkloadClusters. |
| `Partial` | The report holds only some WorkloadClusters: there are more than 64, or a request failed partway. |
| `Unavailable` | The read failed, and the report holds no WorkloadClusters. |

`@ availability` is one of:

| Reason | Meaning |
| --- | --- |
| `Forbidden` | The API server rejected your credentials, or RBAC denies you access to WorkloadClusters. See [Required RBAC](overview.md#required-rbac). |
| `NotFound` | The WorkloadCluster you named doesn't exist, or the cluster lacks the WorkloadCluster CRD. |
| `MalformedPayload` | For a named read, the API server returned an object with another name, or with a namespace. |
| `Unreadable` | Any other failure, such as a timeout or an API server that the CLI can't reach. |

A failed read still prints the report, and `status` exits with `0`. In a script, check `observation` in the `-o json` output, not the exit code.

### Examples {#status-examples}

The examples run on the control-plane cluster, through its context `hub`. It holds two WorkloadClusters, `worker-a` and `worker-b`.

List the WorkloadClusters:

```bash
kubectl ome cluster status
```

```output
CLUSTER          DECLARED           READY     FRESHNESS   CONDITIONS
@ observation    Complete           -         -           -
@ availability   -                  -         -           -
@ sources        returned=2         kept=2    max=64      cut=false
@ pages          seen=1/2           calls=1   cap=4       size=32
worker-a         KubeConfigSecret   True      Current     Reported
worker-b         KubeConfigSecret   False     Stale       Reported
```

The controller reports `worker-a` Ready for its current spec. `worker-b` is at generation 3, but its Ready condition is for generation 2, so FRESHNESS is `Stale` and READY `False` describes the old spec. A report that stays `Stale` suggests that the controller isn't running.

Print the report for `worker-a` as JSON:

```bash
kubectl ome cluster status worker-a -o json
```

```output
{
  "apiVersion": "cli.ome.io/v1alpha1",
  "kind": "ClusterStatusReport",
  "collectedAt": "2026-01-01T00:00:00Z",
  "observation": "Complete",
  "requestedPages": 1,
  "observedPages": 1,
  "returnedSources": 1,
  "admittedSources": 1,
  "sourceLimit": 1,
  "pageLimit": 1,
  "requestLimit": 1,
  "pageSize": 1,
  "conditionScanLimit": 128,
  "conditionOutputLimit": 32,
  "sourcesTruncated": false,
  "clusters": [
    {
      "name": "worker-a",
      "generation": 1,
      "declaredConnectionKind": "KubeConfigSecret",
      "sourceState": "Reported",
      "profileResolution": "NotAttempted",
      "evidence": "Reported",
      "reportedReady": "True",
      "conditionState": "Reported",
      "freshness": "Current",
      "connectionState": "ReportedReady",
      "totalConditions": 1,
      "scannedConditions": 1,
      "retainedConditions": 1,
      "conditionsTruncated": false,
      "conditions": [
        {
          "type": "Ready",
          "status": "True",
          "observedGeneration": 1,
          "freshness": "Current",
          "reason": "Other",
          "transitionTime": "2025-12-31T23:00:00Z",
          "transitionState": "Reported"
        }
      ]
    }
  ]
}
```

The controller wrote the reason `Reachable`, which `status` shows as `Other`.

Name a WorkloadCluster that doesn't exist:

```bash
kubectl ome cluster status worker-c
```

```output
CLUSTER          DECLARED      READY     FRESHNESS    CONDITIONS
@ observation    Unavailable   -         -            -
@ availability   NotFound      -         -            -
@ sources        returned=0    kept=0    max=1        cut=false
@ pages          seen=0/1      calls=1   cap=1        size=1
-                -             Unknown   Unobserved   Unavail...
```

The report holds no WorkloadClusters, so the last row shows the observation, cut to fit its column.

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | `status`, whenever it prints the report, even when the observation is `Partial` or `Unavailable`. |
| `1` | General error | `status`, for an invalid name (`invalid WorkloadCluster name`), an extra argument, an unknown flag or format, an unusable kubeconfig (`cannot create current-context OME client`), Ctrl-C, or a failed write. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
