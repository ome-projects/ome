---
title: Run Alfred in recommend-only mode
description: Install Alfred and read the Instance moves it recommends when free GPUs fragment or a node turns unhealthy, then tune its policies.
since: v1.3
---

[Alfred](../../concepts/scheduling/alfred.md), OME's GPU cluster caretaker, recommends migrations that reclaim fragmented GPUs or move [Instances](../../concepts/omenative/instances.md) off unhealthy nodes. It starts in recommend-only mode: it reports these moves and changes no workload, so you can review its advice before you [let it act](let-alfred-migrate-instances.md). Alfred is alpha. You turn it on by installing its own chart, `ome-alfred`, next to OME. Then you read its recommendations, events and metrics, and tune a policy.

<div class="prerequisites" markdown>

- OME, installed as [Install OME](../../getting-started/install.md) shows.
- Helm, and permission to create a ClusterRole and a ClusterRoleBinding: Alfred reads nodes, pods and OME's resources across the cluster.
- An Alfred image that your nodes can pull. OME's releases don't publish one, so build it yourself with Go and Docker or nerdctl. From a checkout of the OME repository at your release's tag, `make push-alfred-image REGISTRY=registry.example.com/ome TAG=v1.3.0` builds `registry.example.com/ome/alfred:v1.3.0` for `linux/amd64` and pushes it. Add `ARCH=linux/arm64` for Arm nodes, and `DOCKER_BUILD_CMD=docker` to build with Docker when nerdctl is also installed.
- The [`kubectl ome`](../../reference/kubectl-ome/overview.md) plugin, `jq` and `curl`.

</div>

## What Alfred recommends

Alfred runs two policies:

- The defragmentation policy looks for GPU pools, groups of nodes with the same kind of GPU, whose free GPUs are scattered in pieces too small for larger workloads.
- The node-health policy looks for Instances on unhealthy nodes, by default nodes whose `GpuUnhealthy` condition is `True`. OME doesn't set this condition, so your GPU health checks must. Once you configure [maintenance triggers](../../reference/scheduling/alfred-configuration.md#maintenance-triggers), the policy also looks for nodes marked for planned maintenance.

[Alfred policies](../../concepts/scheduling/alfred-policies.md) explains how they choose.

## Step 1: Install Alfred

Save the image settings in a values file:

```yaml title="values.yaml"
global:
  hub: registry.example.com/ome
image:
  tag: v1.3.0
```

The chart then runs `registry.example.com/ome/alfred:v1.3.0` instead of its unpublished default, `ghcr.io/moirai-internal/alfred:latest`. If your registry needs credentials, list your pull secrets under `global.imagePullSecrets`, as `name` entries.

Install the chart into OME's namespace, `ome`, where `kubectl ome` looks for Alfred by default:

```bash
helm install ome-alfred oci://ghcr.io/moirai-internal/charts/ome-alfred \
  --version 1.3.0 --namespace ome -f values.yaml
```

Helm installs the release and prints its status. The chart creates the Deployment `ome-alfred`, with three replicas, and two ConfigMaps: `alfred-config`, which holds Alfred's configuration, and `alfred-recommendations`, where Alfred writes its recommendations. The chart's notes suggest looking for `RecommendationWithheld` events, but Alfred records those only in execute mode.

Install one Alfred per cluster. Its ClusterRole and ClusterRoleBinding have fixed names, so a second release in another namespace fails to install.

Wait for Alfred's pods:

```bash
kubectl rollout status deployment/ome-alfred -n ome
```

```output
deployment "ome-alfred" successfully rolled out
```

If the rollout stalls, see [The pods don't start](#the-pods-dont-start).

One replica, the leader, runs the policies and writes the recommendations. If it goes away, another replica takes over. See which pod leads:

```bash
kubectl get lease alfred.ome.io -n ome -o jsonpath='{.spec.holderIdentity}'
```

```output
ome-alfred-6d8f7c9b54-2xkqp_0b6c5f2e-4c1d-4f7e-9a53-2d8e6b1f0c47
```

The part before the underscore is the leader's pod name.

## Step 2: Read its recommendations

The leader writes its first recommendations within one decision interval, 5 minutes by default. Read them with `kubectl ome`:

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
Rows                       2 shown/2 scanned   0 invalid; 0 omitted
team-a/llama-8b/engine#0   advisory            defragmentation/Fragmentation
team-b/qwen-32b/engine#0   advisory            defragmentation/Fragmentation
Advisory evidence          Not authorization   Convergence unverified
Executability              Unverifiable        Not persisted in cycle
Scope                      Latest cycle only   Node/scheduling data omitted
```

The first rows show where the command read the recommendations, the mode, and whether they're `Recent`: written within the last two decision intervals. Then come the recommendations: the InferenceService, component and Instance, the outcome, and the policy with its reason. Add `-o wide` to see their advisory reasons. [`kubectl ome admin`](../../reference/kubectl-ome/admin.md#recommendations-output-fields) explains the output.

In recommend-only mode, the outcome is always `advisory`. With the chart's defaults, the rows also name no target node, and their advisory reason explains what blocks the move. Alfred moves only [OMENative](../../concepts/omenative/overview.md) Instances, so the reason depends on the component's deployment mode:

| Deployment mode | Advisory reason |
| --- | --- |
| RawDeployment | `RawDeploymentMigrationUnsupported`, with one row per flagged pod, numbered from 0 in the order of the pods' names |
| `MultiNode` (deprecated) | `LWSMigrationUnsupported` |
| OMENative | `OMENativeUnavailable`, until you set the chart's `migration.apiVersion`, as [Let Alfred migrate Instances](let-alfred-migrate-instances.md) does |
| OMENative, but not steady, such as during a rollout | `OMENativeStateIneligible` or `OMENativeObservationInvalid` |

A component that's OMENative only through its runtime counts as a RawDeployment: see [An OMENative component shows RawDeploymentMigrationUnsupported](#an-omenative-component-shows-rawdeploymentmigrationunsupported). [Advisory reasons](../../reference/scheduling/alfred-metrics-and-events.md#advisory-reasons) lists the others. The rows show what the policies propose. In execute mode, the arbiter's caps and cooldowns also hold moves back, as [Safety bounds](../../concepts/scheduling/alfred-policies.md#safety-bounds) explains.

!!! warning "LeaderWorkerSet is deprecated"
    The LeaderWorkerSet-backed `MultiNode` mode is deprecated. Use OMENative for multi-node serving instead: see [Move from LeaderWorkerSet to OMENative](../omenative/move-from-leaderworkerset.md).

To see all the fields, read the recommendations ConfigMap:

```bash
kubectl get configmap alfred-recommendations -n ome -o jsonpath='{.data.last-cycle\.json}' | jq .
```

```output
{
  "timestamp": "2026-09-28T10:15:02.418377251Z",
  "mode": "recommend-only",
  "recommendations": [
    {
      "workload": "team-a/llama-8b",
      "component": "engine",
      "instance": 0,
      "policy": "defragmentation",
      "reason": "Fragmentation",
      "outcome": "advisory",
      "advisoryReason": "RawDeploymentMigrationUnsupported",
      "fromNode": "gpu-node-3",
      "score": 0
    },
    {
      "workload": "team-b/qwen-32b",
      "component": "engine",
      "instance": 0,
      "policy": "defragmentation",
      "reason": "Fragmentation",
      "outcome": "advisory",
      "advisoryReason": "OMENativeUnavailable",
      "fromNode": "gpu-node-5",
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

On OMENative rows, `scheduling` is `Unavailable` with the reason `ProfileNotConfigured` until you set up simulation, as [Set up scheduler simulation for Alfred](set-up-scheduler-simulation.md) shows. [The recommendations ConfigMap](../../reference/scheduling/alfred-metrics-and-events.md#the-recommendations-configmap) describes the fields.

Alfred also records the rows as events:

```bash
kubectl get events -A --field-selector source=alfred -o custom-columns=NAMESPACE:.metadata.namespace,TYPE:.type,REASON:.reason,KIND:.involvedObject.kind,NAME:.involvedObject.name
```

```output
NAMESPACE   TYPE      REASON                                KIND               NAME
ome         Warning   OMENativeUnavailable                  ConfigMap          alfred-config
team-a      Normal    RawDeploymentMigrationUnsupported     InferenceService   llama-8b
team-b      Normal    FragmentationRecommendationProduced   InferenceService   qwen-32b
```

The Warning `OMENativeUnavailable` is expected until you set `migration.apiVersion`. `kubectl describe` doesn't list these events, so use `kubectl get events`. Alfred merges and rate-limits repeated events, so the ConfigMap is the complete record. [Events](../../reference/scheduling/alfred-metrics-and-events.md#events) lists the reasons.

## Step 3: Check its metrics

All replicas serve Prometheus metrics on port 8080, at `/metrics`. Only the leader publishes the decision metrics, such as the recommendation counts, so forward a local port to the leader's pod:

```bash
LEADER=$(kubectl get lease alfred.ome.io -n ome -o jsonpath='{.spec.holderIdentity}' | cut -d_ -f1)
kubectl port-forward -n ome "pod/$LEADER" 8080:8080
```

```output
Forwarding from 127.0.0.1:8080 -> 8080
Forwarding from [::1]:8080 -> 8080
```

Leave it running, and read a few of Alfred's metrics from another terminal:

```bash
curl -s localhost:8080/metrics | grep -E '^alfred_(cluster_fragmentation_score|fragmentation_observed|leader_status|omenative_unavailable|policy_reload_total|recommendations_produced_total)'
```

```output
alfred_cluster_fragmentation_score 0
alfred_fragmentation_observed{pool="NVIDIA-H100-80GB-HBM3",size="1"} 0
alfred_fragmentation_observed{pool="NVIDIA-H100-80GB-HBM3",size="2"} 0.25
alfred_fragmentation_observed{pool="NVIDIA-H100-80GB-HBM3",size="4"} 1
alfred_fragmentation_observed{pool="NVIDIA-H100-80GB-HBM3",size="8"} 1
alfred_leader_status{pod="ome-alfred-6d8f7c9b54-2xkqp"} 1
alfred_omenative_unavailable 1
alfred_policy_reload_total{outcome="success"} 1
alfred_recommendations_produced_total{component="engine",executable="false",policy="defragmentation",reason="Fragmentation",workload="team-a/llama-8b"} 4
alfred_recommendations_produced_total{component="engine",executable="false",policy="defragmentation",reason="Fragmentation",workload="team-b/qwen-32b"} 4
```

| Metric | What it shows |
| --- | --- |
| `alfred_cluster_fragmentation_score` | The highest pool score, from 0 to 1. It counts only the fragmentation that moving Instances could fix, so it stays 0 until you set `migration.apiVersion`. |
| `alfred_fragmentation_observed` | By GPU pool and Instance size, the share of the pool's free GPUs that Instances of that size can't use. |
| `alfred_leader_status` | 1 on the leader, 0 on the other replicas. |
| `alfred_omenative_unavailable` | 1 while OMENative migration is unavailable, as with the chart's defaults. |
| `alfred_policy_reload_total` | The configuration loads, by `outcome`: `success` or `failure`. |
| `alfred_recommendations_produced_total` | The rows that the policies produced, pass after pass. `executable="false"` marks advice. |

The defragmentation policy still reports rows while the score is 0. With the chart's defaults, it flags a pool whose demand-weighted fragmentation is above its `fragmentationThreshold`, 0.25 by default. [How Alfred scores a pool](../../concepts/scheduling/alfred-policies.md#how-alfred-scores-a-pool) explains both figures.

To scrape Alfred with the Prometheus Operator, set `metrics.serviceMonitor.enabled: true` in `values.yaml`. The chart then creates a ServiceMonitor that scrapes all replicas through the Service `ome-alfred-metrics`. The pods also carry `prometheus.io/scrape` annotations. [Alfred metrics and events](../../reference/scheduling/alfred-metrics-and-events.md) lists all of Alfred's metrics.

## Step 4: Tune a policy

You configure Alfred through the chart's `alfredConfig` value, which the chart writes to the ConfigMap `alfred-config`. [Alfred configuration](../../reference/scheduling/alfred-configuration.md) lists the settings.

For example, to have the defragmentation policy flag only more fragmented pools, raise its `fragmentationThreshold` from 0.25 to 0.4 in `values.yaml`:

```yaml title="values.yaml"
global:
  hub: registry.example.com/ome
image:
  tag: v1.3.0
alfredConfig:
  policies:
    defragmentation:
      fragmentationThreshold: 0.4
```

Keep the image settings in the file: with `-f`, `helm upgrade` starts again from the chart's defaults. Under `alfredConfig`, set only what you change. Helm merges maps into the chart's defaults key by key, but a list, such as `earlyTickOn`, replaces the chart's list whole.

Upgrade the release:

```bash
helm upgrade ome-alfred oci://ghcr.io/moirai-internal/charts/ome-alfred \
  --version 1.3.0 --namespace ome -f values.yaml
```

Helm updates `alfred-config` and replaces Alfred's pods, which start with the new configuration. Check that the replicas loaded it:

```bash
kubectl logs -n ome -l control-plane=ome-alfred --tail=-1 --prefix | grep 'config reload'
```

kubectl prints the matching lines, prefixed with the pod's name. A replica that applied the configuration logs `config reloaded`, with the `mode` it runs in. If one logs `config reload failed; keeping last-known-good`, see [A configuration change doesn't take effect](#a-configuration-change-doesnt-take-effect). After the next decision pass, `kubectl ome admin recommendations` shows fewer defragmentation rows, or none.

To try a change without an upgrade, edit the ConfigMap with `kubectl edit configmap alfred-config -n ome`. Alfred applies the edit within seconds, without a restart. With Helm 3, the next `helm upgrade` writes the chart's configuration back over it, so keep your settings in `values.yaml`.

## Troubleshooting

### The pods don't start

List Alfred's pods:

```bash
kubectl get pods -n ome -l control-plane=ome-alfred
```

kubectl lists the pods with their status. `ErrImagePull` or `ImagePullBackOff` means that the nodes can't pull the image. Check `global.hub` and `image.tag` against the image you pushed, and add a pull secret to `global.imagePullSecrets` if your registry needs one. `Pending` means that no node has room for a pod, which requests 100m of CPU and 256Mi of memory: `kubectl describe pod` shows why.

### The command reports Unavailable or Stale

The first rows of `kubectl ome admin recommendations` show the cause:

| What you see | Cause and fix |
| --- | --- |
| `KeyAbsent` on the `Latest cycle` row | The leader hasn't finished its first pass. Wait one decision interval, 5 minutes by default. |
| `Stale` on the first row | No leader has finished a pass for two decision intervals. Check the Lease and the logs, as below. |
| `NotFound` on the `ConfigMap config` row | The command looked for Alfred in the wrong namespace. Add `--alfred-namespace` with the namespace of Alfred's release. |
| `NotFound` on the `Latest cycle` row | `alfred-recommendations` is missing. See [Alfred logs that the recommendations ConfigMap is missing](#alfred-logs-that-the-recommendations-configmap-is-missing). |

If `KeyAbsent` or `Stale` persists, check that the Lease has a holder, as Step 1 shows, and read the replicas' logs:

```bash
kubectl logs -n ome -l control-plane=ome-alfred --tail=-1 --prefix
```

kubectl prints the replicas' logs. A repeated `observation pass failed` error means that the replica can't read the cluster, and the rest of the line says why. A single `initial observation pass failed` error while a pod starts is harmless on its own.

### Alfred recommends nothing

The command reports `Empty` when the leader's pass found nothing to report, which is normal on a healthy cluster. The defragmentation policy flags a pool when its demand-weighted fragmentation is above `fragmentationThreshold`, and `alfred_fragmentation_observed` shows how fragmented the pools are. The node-health policy flags Instances on nodes whose `GpuUnhealthy` condition is `True`, or on nodes that match a maintenance trigger you configure. [Alfred configuration](../../reference/scheduling/alfred-configuration.md) shows how to change the threshold, conditions and triggers.

### An OMENative component shows RawDeploymentMigrationUnsupported

Alfred reads a component's deployment mode from the InferenceService alone. A component that's OMENative only through its runtime, by the runtime's `leader` or `worker` or its `ome.io/deploymentMode` annotation, is a RawDeployment to Alfred, which never moves it. Set `spec.deploymentMode: OMENative` in the InferenceService, as [For the whole InferenceService](../../concepts/architecture/deployment-modes.md#for-the-whole-inferenceservice) shows. The field also makes a router OMENative, unless the router sets its own mode.

If the runtime sets the mode with the `ome.io/deploymentMode` annotation, set that annotation on the component in the InferenceService instead, as [For one component](../../concepts/architecture/deployment-modes.md#for-one-component) shows.

### A configuration change doesn't take effect

Look for `PolicyReloadFailed` events. Alfred records one when it rejects a configuration, such as a `fragmentationThreshold` of 1.5:

```bash
kubectl get events -n ome --field-selector reason=PolicyReloadFailed
```

```output
LAST SEEN   TYPE      REASON               OBJECT                    MESSAGE
2m10s       Warning   PolicyReloadFailed   configmap/alfred-config   alfred-config rejected, keeping last-known-good: policies.defragmentation.fragmentationThreshold must be in [0, 1], got 1.5
2m10s       Warning   PolicyReloadFailed   configmap/alfred-config   alfred-config rejected, keeping last-known-good: policies.defragmentation.fragmentationThreshold must be in [0, 1], got 1.5
2m9s        Warning   PolicyReloadFailed   configmap/alfred-config   alfred-config rejected, keeping last-known-good: policies.defragmentation.fragmentationThreshold must be in [0, 1], got 1.5
2m4s        Warning   PolicyReloadFailed   configmap/alfred-config   alfred-config rejected, keeping last-known-good: policies.defragmentation.fragmentationThreshold must be in [0, 1], got 1.5
104s        Warning   PolicyReloadFailed   configmap/alfred-config   alfred-config rejected, keeping last-known-good: policies.defragmentation.fragmentationThreshold must be in [0, 1], got 1.5
83s         Warning   PolicyReloadFailed   configmap/alfred-config   alfred-config rejected, keeping last-known-good: policies.defragmentation.fragmentationThreshold must be in [0, 1], got 1.5
```

The running replicas and the new pods that the upgrade starts all record the event. The running replicas keep their last good configuration, but a new pod has none, so it runs Alfred's built-in defaults: recommend-only mode, with both policies on. Once the upgrade finishes, all replicas run these defaults until you fix the value and upgrade again.

If there are no such events, check that your setting reached `alfred-config`. Helm accepts values that the chart ignores, such as `policies` at the top level instead of under `alfredConfig`. Print what Alfred reads:

```bash
kubectl get configmap alfred-config -n ome -o jsonpath='{.data.config\.yaml}'
```

Check that your setting is in the `config.yaml` document it prints.

### The command reports Partial

The command shows at most 200 rows, and the `Rows` row counts the ones it left out. It also leaves out rows about a whole component, which have `instance` -1 in the ConfigMap, and counts them as invalid, with an `InvalidRows` issue. This is a known bug: read the ConfigMap, as Step 2 shows, to see them all.

### Alfred logs that the recommendations ConfigMap is missing

The leader logs `recommendations ConfigMap missing; record disabled (the chart pre-creates it)` when `alfred-recommendations` is gone. Alfred can't recreate it, so run your `helm upgrade` again. Alfred writes its recommendations again at its next pass.

## Clean up

Uninstall the chart:

```bash
helm uninstall ome-alfred --namespace ome
```

```output
release "ome-alfred" uninstalled
```

Helm deletes everything that the chart created, including Alfred's recommendations. The leader-election Lease `alfred.ome.io` stays, because Alfred created it. Delete it:

```bash
kubectl delete lease alfred.ome.io -n ome
```

```output
lease.coordination.k8s.io "alfred.ome.io" deleted
```

Alfred's events expire after an hour by default. Recommend-only mode left your workloads as they were.

## Next steps

- [Set up scheduler simulation for Alfred](set-up-scheduler-simulation.md): predict where the replacement pods of each move would land.
- [Let Alfred migrate Instances](let-alfred-migrate-instances.md): turn on migration, and let Alfred carry out its moves.
- [Alfred](../../concepts/scheduling/alfred.md): how Alfred observes, decides and migrates.
- [Alfred policies](../../concepts/scheduling/alfred-policies.md): how the defragmentation and node-health policies choose what to move.
- [Alfred configuration](../../reference/scheduling/alfred-configuration.md): the settings in `alfredConfig`.
- [Alfred metrics and events](../../reference/scheduling/alfred-metrics-and-events.md): the metrics and events that Alfred records.
- [kubectl ome admin](../../reference/kubectl-ome/admin.md): the flags and output of `kubectl ome admin recommendations`.
