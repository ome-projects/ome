---
title: kubectl ome get
description: List OME resources with model-centric columns, including merged views of BaseModels with ClusterBaseModels and ServingRuntimes with ClusterServingRuntimes.
since: v1.3
---

`kubectl ome get` lists OME resources, or shows one by name, with columns picked for each kind. An [InferenceService](../../concepts/serving/inference-services.md) row shows its model, runtime and readiness, and an [InferenceReplica](../../concepts/omenative/overview.md) row shows its Instance counts. The `models` and `runtimes` views list [base models](../../concepts/models/base-models.md) and [serving runtimes](../../concepts/runtimes/serving-runtimes.md) of both scopes in one table.

```text
kubectl ome get RESOURCE [NAME] [flags]
```

Give one RESOURCE from [Resources](#resources), in any letter case, and at most one NAME. Unlike `kubectl get`, `get` has no `TYPE/NAME` form and takes no `.ome.io` suffix. [Required RBAC](overview.md#required-rbac) lists the permissions it needs.

## Resources

| Resource | Also accepted | Scope | What it lists |
| --- | --- | --- | --- |
| `inferenceservices` | `inferenceservice`, `isvc`, `isvcs` | Namespaced | InferenceServices |
| `inferencereplicas` | `inferencereplica`, `ir` | Namespaced | The InferenceReplicas that OME creates, one for each OMENative component |
| `models` | `model` | Merged | BaseModels, then ClusterBaseModels |
| `basemodels` | `basemodel`, `bm` | Namespaced | BaseModels |
| `clusterbasemodels` | `clusterbasemodel`, `cbm` | Cluster | ClusterBaseModels |
| `runtimes` | `runtime` | Merged | ServingRuntimes, then ClusterServingRuntimes |
| `servingruntimes` | `servingruntime`, `srt` | Namespaced | ServingRuntimes |
| `clusterservingruntimes` | `clusterservingruntime`, `csrt` | Cluster | ClusterServingRuntimes |
| `finetunedweights` | `finetunedweight`, `ftw` | Cluster | [FineTunedWeights](../../concepts/models/fine-tuned-weights.md) |
| `acceleratorclasses` | `acceleratorclass`, `ac` | Cluster | [AcceleratorClasses](../../concepts/runtimes/accelerator-classes.md) |
| `acceleratorquotas` | `acceleratorquota`, `aq` | Cluster | The budgets of each [AcceleratorQuota](../../guides/operate-ome/accelerator-quota.md), new in v1.3 |
| `benchmarkjobs` | `benchmarkjob`, `bj` | Namespaced | [BenchmarkJobs](../../concepts/serving/benchmarks.md) |
| `rolloutpolicies` | `rolloutpolicy`, `rp` | Namespaced | Alpha. [RolloutPolicies](../../concepts/rollouts-and-traffic/rollout-policy.md) |
| `autoscalerpolicies` | `autoscalerpolicy`, `ap` | Namespaced | Alpha. [AutoscalerPolicies](../../concepts/serving/autoscaler-policy.md) |
| `workloadclusters` | `workloadcluster`, `wc` | Cluster | Alpha. The [WorkloadClusters](../api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster) of multi-cluster serving |
| `trafficmaps` | `trafficmap`, `tm`, `tmap` | Namespaced | Alpha. The [TrafficMaps](../../concepts/rollouts-and-traffic/traffic-map.md) of multi-cluster routing |

`kubectl get` knows the short names `irep` and `wlc`, but `kubectl ome get` doesn't: use `ir` and `wc`, or the full names.

!!! note "Alpha resources"
    RolloutPolicies, AutoscalerPolicies, WorkloadClusters and TrafficMaps are alpha. Their fields and behavior can change between releases.

    RolloutPolicies and AutoscalerPolicies are off by default: see Turn on the feature for [rollout policies](../../concepts/rollouts-and-traffic/rollout-policy.md#turn-on-the-feature) and [autoscaler policies](../../concepts/serving/autoscaler-policy.md#turn-on-the-feature). Until then, `get` fails with the errors in [Errors](#errors).

    WorkloadClusters and TrafficMaps belong to multi-cluster serving, which is still in development. Its routing is off by default and runs only on the control-plane cluster, so other clusters have no TrafficMaps.

### Merged views {#merged-views}

`models` and `runtimes` list the namespaced kind first, then every object of the cluster-scoped kind. The SCOPE column says which kind each row is, and `-l` filters both. When either list fails, `get` prints only the error. If you can't list the cluster-scoped kind, use `basemodels` or `servingruntimes`.

With a NAME, `get` shows the namespaced object, or the cluster-scoped one when there's none. When a BaseModel and a ClusterBaseModel [share a name](../../concepts/models/base-models.md), an InferenceService in the BaseModel's namespace gets the BaseModel too, even when it asks for the ClusterBaseModel. For an InferenceService's runtime, OME looks for the ClusterServingRuntime first, unless the InferenceService sets `spec.runtime.kind: ServingRuntime`. So `get runtimes NAME` can show a different runtime from the one an InferenceService runs, which [`kubectl ome status`](status.md) `-o wide` names.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-A`, `--all-namespaces` | `false` | List across all namespaces |
| `-o`, `--output` | `table` | Output format: table (default), wide, json or yaml |
| `-l`, `--selector` | None | Label selector to filter on |

Plus the standard kubeconfig flags, such as `-n` and `--context`.

A NAME can't be combined with `-A` or `-l`. For a cluster-scoped resource, `get` ignores `-n`, and ignores `-A` with a warning on stderr. `-o wide` adds the Wide columns in [Output fields](#output-fields), where a resource has any. `-o json` and `-o yaml` print Kubernetes objects, not a report. With a NAME, that's the object; otherwise it's a `List` with `apiVersion: v1`, whose items carry their own `apiVersion` and `kind`.

## Output fields

Every table starts with NAME and, by default, ends with AGE, the time since the object was created, such as `3h`. With `-A`, namespaced resources get a NAMESPACE column before NAME, which shows `-` for the cluster-scoped rows of a merged view. A text cell shows `-` when its field isn't set, and rows marked Wide are the columns that `-o wide` adds.

Some columns recur:

| Column | What it shows |
| --- | --- |
| READY, DEGRADED, IN-USE, ROUTABLE, PUBLISHED, OVERRIDE | The condition's status, or `Unknown` when there's none. On InferenceReplicas, READY is a count. |
| STATUS-FRESHNESS, LIFECYCLE-FRESHNESS, PUBLISHER-FRESHNESS | Whether the status describes the current spec: `Current`, `Stale` when it's older, or `Unobserved` before it's first written. |

The RolloutPolicy, AutoscalerPolicy and TrafficMap tables hide stale values: a condition from an older generation shows as `Unknown`, and its reason as `-`. The policy tables also show `-` for their digest and counts until the status is current. The other tables show the last status as it is.

### `inferenceservices` {#inferenceservices-columns}

[`kubectl ome status`](status.md) shows the full readiness of one InferenceService.

| Column | What it shows |
| --- | --- |
| MODEL | `spec.model.name`, or `-` when the InferenceService names only a runtime. |
| RUNTIME | `spec.runtime.name`, or `-` when OME picks the runtime, which [`kubectl ome runtime effective`](runtime.md#effective) shows. |
| READY | The `Ready` condition. |
| URL | `status.url`. |

### `inferencereplicas` {#inferencereplicas-columns}

OME writes an InferenceReplica for each OMENative component of an InferenceService. It holds the component's replicas, each one an [Instance](../../concepts/omenative/instances.md): one pod, or a leader and its workers. [`kubectl ome instance`](instance.md) lists the Instances themselves.

| Column | What it shows |
| --- | --- |
| COMPONENT | `spec.component`. |
| PARENT | `spec.parentRef.name`, the InferenceService. |
| DESIRED | `spec.replicas`, the number of Instances the component should run. |
| CURRENT | `status.replicas`, the number of Instances, in any phase. |
| READY | `status.readyReplicas`, as [Readiness and availability](../../concepts/omenative/instances.md#readiness-and-availability) defines it. |
| AVAILABLE | Wide. `status.availableReplicas`. |
| LIFECYCLE | The main condition, as `Type=Status`: `RolloutStalled=True` while a rollout is stalled, and otherwise `Ready`, or `RolloutStalled` when there's no `Ready`. `Unavailable` when there's neither. |
| REASON | Wide. The reason of the LIFECYCLE condition. |
| SERVING | Wide. `status.servingReplicas`. |
| UPDATED | Wide. `status.updatedReplicas`, the number of Instances on the update revision. |
| ENCODING | Wide. How the status stores its Instance rows: `ColumnarV2` or `DenseV1`. See [Change the status encoding](../../guides/omenative/change-the-status-encoding.md). |
| CURRENT-REVISION, UPDATE-REVISION | Wide. `status.currentRevision`, which serves traffic, and `status.updateRevision`, which is rolling out. |
| MIGRATIONS | Wide. The number of entries in `status.migrations`. See [Migration and transient scale](../../concepts/omenative/migration-and-transient-scale.md). |
| PAUSED | Wide. `spec.paused`: `true` or `false`. |
| COORDINATION | Wide. `status.coordinationGroupRef`. Always `-` in this release. |
| LIFECYCLE-FRESHNESS | Wide. Whether the LIFECYCLE condition is current, or `Unavailable` when there's none. |

### `models`, `basemodels` and `clusterbasemodels` {#models-columns}

| Column | What it shows |
| --- | --- |
| SCOPE | `models` only. `Namespaced` for a BaseModel and `Cluster` for a ClusterBaseModel. |
| ARCH | `spec.modelArchitecture`. |
| PARAMS | `spec.modelParameterSize`, such as `3.21B`. |
| FORMAT | `spec.modelFormat.name`, such as `safetensors`. |
| STATE | `status.state`, the model's state across the cluster, as [Model lifecycle](../../concepts/models/base-models.md#model-lifecycle) describes. |

ARCH, PARAMS and FORMAT show `-` until the spec sets them or OME reads them from the model's files, as [What OME learns from the model](../../concepts/models/base-models.md#what-ome-learns-from-the-model) explains.

### `runtimes`, `servingruntimes` and `clusterservingruntimes` {#runtimes-columns}

| Column | What it shows |
| --- | --- |
| SCOPE | `runtimes` only. `Namespaced` for a ServingRuntime and `Cluster` for a ClusterServingRuntime. |
| DISABLED | `true` when `spec.disabled` is `true`, otherwise `false`. |
| FORMATS | The formats in `spec.supportedModelFormats`, joined with commas. Past three, the rest show as a count, such as `safetensors,pytorch,onnx,+2`. |

### `finetunedweights` {#finetunedweights-columns}

| Column | What it shows |
| --- | --- |
| TYPE | `spec.modelType`, such as `LoRA`. |
| BASEMODEL | `spec.baseModelRef.name`, the base model the weights apply to. |

### `acceleratorclasses` {#acceleratorclasses-columns}

| Column | What it shows |
| --- | --- |
| VENDOR | `spec.vendor`. |
| FAMILY | `spec.family`. |

### `acceleratorquotas` {#acceleratorquotas-columns}

The table has a row for each budget. [`kubectl ome quota`](quota.md) shows the whole quota tree.

| Column | What it shows |
| --- | --- |
| ROLE | `spec.role`: `Cohort` or `ClusterQueue`. |
| PARENT | `spec.parentRef.name`, the quota's parent in the quota tree. |
| RESOURCE, FLAVOR | The budget's resource and flavor. |
| NOMINAL | The budget's share. |
| ADMITTED | The quantity admitted against the quota's queues, including anything borrowed. |
| SOURCE | `Reported` for a budget from the status, `Declared` for one from `spec.budgets`, and `Unavailable` when the quota has neither. |
| STATUS-FRESHNESS | Freshness of the status. |
| READY, DEGRADED | The status of the `Ready` and `Degraded` [conditions](../../guides/operate-ome/accelerator-quota.md#conditions). Read them with STATUS-FRESHNESS: they can describe an older spec. |
| BORROWED | Wide. The admitted quantity above the nominal share. |
| RESERVED | Wide. The quantity held by workloads with a quota reservation, admitted or not. |

When the status isn't current or has no budgets, the table adds a Declared row for each budget in the spec, with `-` for ADMITTED, BORROWED and RESERVED. That lets you compare what you declared with what the quota manager last reported.

### `benchmarkjobs` {#benchmarkjobs-columns}

The columns are NAME, STATE and AGE. STATE is `status.state`: `Pending`, `Running`, `Completed` or `Failed`, as [Status](../../concepts/serving/benchmarks.md#status) describes.

### `rolloutpolicies` {#rolloutpolicies-columns}

Alpha. [Conditions and status](../../concepts/rollouts-and-traffic/rollout-policy.md#conditions-and-status) lists the condition reasons.

| Column | What it shows |
| --- | --- |
| PROGRESSION | The progression the policy defines: `canary`, `blueGreen` or `rollingUpdate`. |
| READY | The `Ready` condition. |
| DIGEST | `status.portableDigest`, a digest of the defaulted spec. |
| REFS | `status.attachedGroups`, the number of rollout groups in the namespace that reference the policy. |
| REASON | Wide. The reason of the `Ready` condition. |
| IN-USE | Wide. The `InUse` condition. |
| STATUS-FRESHNESS | Wide. Freshness of the status. |

### `autoscalerpolicies` {#autoscalerpolicies-columns}

Alpha. [Conditions and status](../../concepts/serving/autoscaler-policy.md#conditions-and-status) lists the condition reasons.

| Column | What it shows |
| --- | --- |
| CLASS | `spec.class`: `HPA` or `KEDA`. |
| READY | The `Ready` condition. |
| ATTACHED | `status.attachedComponents`, the number of components in the namespace that reference the policy. |
| DIGEST | Wide. `status.portableDigest`, a digest of the defaulted spec. |
| REASON | Wide. The reason of the `Ready` condition. |
| IN-USE | Wide. The `InUse` condition. |
| STATUS-FRESHNESS | Wide. Freshness of the status. |

### `workloadclusters` {#workloadclusters-columns}

Alpha. A WorkloadCluster registers a cluster for multi-cluster serving, which is still in development. [`kubectl ome cluster`](cluster.md) shows more of a WorkloadCluster's status.

| Column | What it shows |
| --- | --- |
| CONNECTION | `KubeConfig` for a kubeconfig Secret in `spec.clusterSource.kubeConfig`, or `ClusterProfile` for `spec.clusterSource.clusterProfileRef`. |
| REFERENCE | The Secret, as `namespace/name` or `name`, or the ClusterProfile's name. |
| KEY | The Secret's kubeconfig key, `kubeconfig` by default. `-` for a ClusterProfile. |
| READY | The status of the `Ready` condition. Compare GENERATION with OBSERVED-GENERATION to see whether it's current. |
| GENERATION | `metadata.generation`. |
| OBSERVED-GENERATION | The `observedGeneration` of the `Ready` condition. |
| REASON | Wide. The reason of the `Ready` condition. |

### `trafficmaps` {#trafficmaps-columns}

Alpha. A TrafficMap is the routing table of a multi-cluster InferenceService, and multi-cluster routing is still in development. [Conditions](../../concepts/rollouts-and-traffic/traffic-map.md#conditions) lists the condition reasons.

| Column | What it shows |
| --- | --- |
| MODE | `spec.mode`, the InferenceService's placement mode: `Single`, `All` or `Split`. |
| TARGETS | The number of entries, including those with weight 0. |
| ROUTABLE, PUBLISHED | The [Routable](../../concepts/rollouts-and-traffic/traffic-map.md#routable) and [Published](../../concepts/rollouts-and-traffic/traffic-map.md#published) conditions. |
| SERVICE | Wide. `spec.service`, the InferenceService. |
| ACTIVE | Wide. The number of entries with a weight above 0. |
| HEALTHY | Wide. The number of entries with `healthy: true`. When it's higher than ACTIVE, healthy clusters get no requests, as [Why a cluster's weight is zero](../../concepts/rollouts-and-traffic/traffic-map.md#why-a-clusters-weight-is-zero) explains. |
| OVERRIDE, OVERRIDE-REASON | Wide. The status and reason of the [OverrideActive](../../concepts/rollouts-and-traffic/traffic-map.md#overrideactive) condition, such as `True` and `OverridesApplied` while a drain holds an entry at weight 0. |
| REASON | Wide. The reason of the Routable condition, such as `NoRoutableCapacity`. |
| GATEWAY | Wide. `status.gatewayRef`, as `group/kind:namespace/name`. |
| PUBLISHER-FRESHNESS | Wide. Whether the publisher has published the map's current generation. See [Staleness checks](../../concepts/rollouts-and-traffic/traffic-map.md#staleness-checks). |

## Examples

The objects in these examples have no creation time, so AGE shows `-`.

List the InferenceServices in `team-a`:

```bash
kubectl ome get isvc -n team-a
```

```output
NAME     MODEL           RUNTIME     READY     URL   AGE
a-isvc   llama-3-3-70b   srt-llama   Unknown   -     -
b-isvc   llama-3-3-70b   srt-llama   Unknown   -     -
```

Neither InferenceService has a `Ready` condition or a URL yet.

List the BaseModels in `team-a` and every ClusterBaseModel:

```bash
kubectl ome get models -n team-a
```

```output
NAME            SCOPE        ARCH               PARAMS   FORMAT   STATE   AGE
ns-model        Namespaced   LlamaForCausalLM   -        -        -       -
cluster-model   Cluster      LlamaForCausalLM   -        -        -       -
```

These models set only an architecture, so the other columns show `-`.

List the InferenceReplicas in `demo`:

```bash
kubectl ome get ir -n demo
```

```output
NAME          COMPONENT   PARENT   DESIRED   CURRENT   READY   LIFECYCLE    AGE
chat-engine   engine      chat     4         3         2       Ready=True   -
```

The engine of `chat` should run 4 Instances. It has 3, of which 2 are ready, and its `Ready` condition is `True`.

List the accelerator quotas:

```bash
kubectl ome get aq
```

```output
NAME     ROLE           PARENT   RESOURCE         FLAVOR   NOMINAL   ADMITTED   SOURCE     STATUS-FRESHNESS   READY   DEGRADED   AGE
team-a   ClusterQueue   root     nvidia.com/gpu   h100     8         3          Reported   Current            True    False      -
```

`team-a` has a share of 8 `h100` GPUs, and 3 are admitted. The status describes the current spec, so the row shows the reported budget.

List the traffic maps in `team-a`, with the wide columns:

```bash
kubectl ome get tm -n team-a -o wide
```

```output
NAME       MODE    TARGETS   ROUTABLE   PUBLISHED   AGE   SERVICE    ACTIVE   HEALTHY   OVERRIDE   OVERRIDE-REASON   REASON     GATEWAY                                               PUBLISHER-FRESHNESS
checkout   Split   2         True       True        -     checkout   1        1         False      NoOverrides       Routable   gateway.networking.k8s.io/HTTPRoute:team-a/checkout   Current
```

Of the map's two entries, one has a weight above 0 and one is healthy. The InferenceService has no drains, and the publisher has published this generation of the map.

When nothing matches, `get` prints a message to stderr and exits `0`:

```bash
kubectl ome get workloadclusters
```

```output
No workloadclusters found.
```

With `-o json` or `-o yaml`, `get` prints an empty list instead:

```bash
kubectl ome get isvc -n team-a -o yaml
```

```output
apiVersion: v1
items: []
kind: List
metadata: {}
```

## Errors {#errors}

An API error names the operation, the resource and the object or namespace, then the reason.

| Error | Meaning |
| --- | --- |
| `unknown resource "irep" (valid resources: ...)` | The name isn't in [Resources](#resources). |
| `get inferenceservices team-a/missing: NotFound` | The namespace has no object of that name. |
| `list clusterbasemodels: Forbidden` | You can't list the resource. See [Required RBAC](overview.md#required-rbac). |
| `list inferenceservices team-a: Unauthorized` | The API server didn't accept your credentials. |
| `list autoscalerpolicies team-a: OMEAPIMissing (install OME first)` | OME isn't installed or, for AutoscalerPolicies, the feature is off. |
| `list rolloutpolicies team-a: Unavailable` | The CLI can't reach the API server or, for RolloutPolicies, the feature is off. |

## Exit codes

| Code | Meaning | Returned by |
| --- | --- | --- |
| `0` | Success | Every run that finishes, including one that finds nothing. |
| `1` | General error | Every run that fails, such as an unknown resource, a NAME with `-A` or `-l`, a failed API request or Ctrl-C. |

Errors print to stderr as `error: <message>`.

## Related guides

- [Drain a workload cluster](../../guides/multi-cluster/drain-a-workload-cluster.md), which uses the alpha multi-cluster routing
- [Troubleshoot an InferenceService](../../guides/troubleshoot/troubleshoot-an-inferenceservice.md)
