---
title: OME scheduler configuration
description: The ome-scheduler Helm chart's values, the PodGroup fields that the scheduler reads, and its events and metrics.
since: v1.3
---

The `ome-scheduler` Helm chart installs the [OME scheduler](../../concepts/scheduling/ome-scheduler.md) as a second scheduler next to your default one, and its values configure it. Pods opt in by setting `spec.schedulerName` to the scheduler's name. The OME scheduler is alpha, and it runs only on Kubernetes 1.35. [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md) installs the chart and opts a workload in.

## Chart values

Set these values in the values file that you pass to `helm install` or `helm upgrade`, as [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md#step-1-install-the-chart) shows. Helm rejects values outside the limits in the Type column, and the combinations in [Invalid values](#invalid-values). It ignores unknown keys without a warning, so a misspelled key has no effect. `{name}` stands for the value of `scheduler.name`.

### Name and image

| Value | Type | Default | What it sets |
| --- | --- | --- | --- |
| `scheduler.name` | string: a DNS label | `ome-scheduler` | The name that pods set in `spec.schedulerName`. It also names the Deployment, the leader-election Lease and the chart's other objects. |
| `global.hub` | string | `""` | A registry that the chart puts in front of `scheduler.image.repository` when the repository has no `/`. |
| `scheduler.image.repository` | string | `ome-scheduler` | The scheduler's image. OME's releases don't publish it, so build and push it yourself, as [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md#before-you-begin) shows. |
| `scheduler.image.tag` | string | `latest` | The image's tag. |
| `scheduler.image.pullPolicy` | string | `IfNotPresent` | The container's `imagePullPolicy`. |
| `scheduler.imagePullSecrets` | list | `[]` | The pods' image pull secrets, each with a `name`. When it's empty, `global.imagePullSecrets` applies. |
| `global.imagePullSecrets` | list | `[]` | The fallback for `scheduler.imagePullSecrets`. |

### Replicas and availability

| Value | Type | Default | What it sets |
| --- | --- | --- | --- |
| `scheduler.replicaCount` | integer, at least 1 | `2` | The number of replicas. Only the leader schedules, and the others wait to take over. More than one replica needs `scheduler.leaderElect: true`. |
| `scheduler.leaderElect` | boolean | `true` | Leader election between the replicas, through the Lease `{name}` in the release's namespace. |
| `scheduler.deploymentStrategy.type` | string: `RollingUpdate` | `RollingUpdate` | The Deployment's update strategy. |
| `scheduler.deploymentStrategy.rollingUpdate.maxUnavailable` | integer of at least 0, or string | `1` | How many replicas an update can stop at once. |
| `scheduler.deploymentStrategy.rollingUpdate.maxSurge` | integer of at least 0, or string | `0` | How many extra replicas an update can start. With `0`, an update stops a replica before it starts the replacement, so it needs no spare node. |
| `scheduler.podDisruptionBudget.enabled` | boolean | `true` | Creates the PodDisruptionBudget `{name}` for the scheduler's pods. |
| `scheduler.podDisruptionBudget.minAvailable` | integer, at least 0 | `1` | How many replicas a voluntary disruption, like a node drain, must leave running. It must be below `scheduler.replicaCount`. |

### Gangs

Each `scheduler.plugin.<name>` value sets the OMEGangPack plugin's `<name>` argument, and startup errors name the argument. The plugin pins each gang to the domain that it picks, as [How it places a gang](../../concepts/scheduling/ome-scheduler.md#how-it-places-a-gang) explains.

| Value | Type | Default | What it sets |
| --- | --- | --- | --- |
| `scheduler.plugin.topologyKey` | string | `""` (none) | The node label whose values define accelerator domains, for gangs whose PodGroup has no `ome.io/topology-key` annotation, and for standalone packing. |
| `scheduler.plugin.defaultPermitTimeoutSeconds` | integer, at least 1 | `600` | How long a gang's members wait at the scheduler's Permit stage for the rest of the gang, when the PodGroup sets no `scheduleTimeoutSeconds`. |
| `scheduler.omeControllerIntegration.enabled` | boolean | `true` | Locks `scheduler.plugin.podGroupTopologyKeyAnnotation` to `ome.io/topology-key`, the annotation that OME writes. Turn it off only when another tool creates your PodGroups with a different annotation. |
| `scheduler.plugin.podGroupTopologyKeyAnnotation` | string, not empty | `ome.io/topology-key` | The PodGroup annotation that names a gang's topology label. |
| `scheduler.plugin.unsupportedPlacementGroupLabel` | string | `ome.io/placement-group` | The name of the label that keeps a gang `Pending`, as [PodGroups and labels](#labels-and-annotations) describes. An empty value turns this check off. |
| `scheduler.plugin.podGroupSyncTimeoutSeconds` | integer, at least 1 | `30` | How long the scheduler waits at startup to load PodGroups. Without the PodGroup CRD, it starts after the wait, and picks up the CRD when you install it. |
| `scheduler.plugin.gcIntervalSeconds` | integer, at least 1 | `60` | How often the plugin releases the pins of gangs whose pods all finished or were deleted, or whose PodGroup was deleted or re-created. |

### Packing and preemption

| Value | Type | Default | What it sets |
| --- | --- | --- | --- |
| `scheduler.plugin.standaloneDomainPacking` | boolean | `true` | Steers standalone pods, those without a gang, toward partly used domains, so whole domains stay free for gangs. It needs `scheduler.plugin.topologyKey`, and assumes that all domains have the same number of nodes. |
| `scheduler.percentageOfNodesToScore` | integer, 1 to 100 | Unset | The percentage of nodes that must pass filtering before the scheduler stops looking for more. Unset keeps kube-scheduler's default: all nodes in a cluster of fewer than 100, and a smaller share in larger ones. For standalone packing, set `100` so that its score compares every domain. |
| `scheduler.omeGangPack.scoreWeight` | integer, at least 1 | `10` | The weight of OMEGangPack's score, which does the standalone packing. Keep it at or above `scheduler.nodeResourcesFit.scoreWeight`. |
| `scheduler.nodeResourcesFit.scoreWeight` | integer, at least 1 | `10` | The weight of the `NodeResourcesFit` score, which favors busy nodes. |
| `scheduler.nodeResourcesFit.resources` | list | `cpu` and `memory`, weight `1` each | The resources that the `NodeResourcesFit` score weighs, each with a `name` and a `weight` of 1 to 100. A larger weight stops the scheduler from starting. [Pack by accelerator](../../guides/operate-ome/ome-scheduler.md#pack-by-accelerator) adds a GPU resource. |
| `scheduler.disablePodTopologySpreadScore` | boolean | `true` | Turns off the `PodTopologySpread` score, whose spreading works against packing. Topology spread constraints with `whenUnsatisfiable: DoNotSchedule` still apply. |
| `scheduler.disablePreemption` | boolean | `true` | Turns off kube-scheduler's `DefaultPreemption` plugin for this scheduler, so a gang that doesn't fit waits for capacity instead of evicting pods. The default scheduler still preempts for its own pods. |

### Metrics endpoint

| Value | Type | Default | What it sets |
| --- | --- | --- | --- |
| `scheduler.metrics.bindAddress` | string, not empty | `0.0.0.0` | The address of the scheduler's HTTPS server, passed as `--bind-address`. |
| `scheduler.metrics.securePort` | integer, 1 to 65535 | `10259` | The HTTPS port for `/metrics` and the health checks, passed as `--secure-port`. The container names it `https-metrics`. |
| `scheduler.metrics.authDelegation.enabled` | boolean | `true` | Binds the scheduler's ServiceAccount to `system:auth-delegator`, which lets it check the callers of `/metrics`. Its `system:kube-scheduler` binding grants the same access, so `/metrics` works with this off too. |
| `scheduler.metrics.reader.create` | boolean | `true` | Creates the ClusterRole `{name}-metrics-reader`, which allows `get` on `/metrics`. |
| `scheduler.metrics.reader.serviceAccounts` | list | `[]` | The ServiceAccounts that may read `/metrics`, each with a `name` and a `namespace`. Needs `scheduler.metrics.reader.create`. |
| `scheduler.metrics.service.enabled` | boolean | `true` | Creates the Service `{name}-metrics` for the metrics port. |
| `scheduler.metrics.service.type` | string: `ClusterIP`, `NodePort` or `LoadBalancer` | `ClusterIP` | The Service's type. |
| `scheduler.metrics.service.port` | integer, 1 to 65535 | `10259` | The Service's port, named `https-metrics`. |
| `scheduler.metrics.service.annotations` | map | `{}` | Annotations on the Service. |
| `scheduler.metrics.serviceMonitor.enabled` | boolean | `false` | Creates the ServiceMonitor `{name}`, which scrapes `/metrics` through the Service. Prometheus scrapes with its own ServiceAccount's token, so list that account in `scheduler.metrics.reader.serviceAccounts`. Needs the Prometheus Operator's ServiceMonitor CRD and `scheduler.metrics.service.enabled`. |
| `scheduler.metrics.serviceMonitor.interval` | string | `30s` | How often Prometheus scrapes the scheduler. |
| `scheduler.metrics.serviceMonitor.scrapeTimeout` | string | `10s` | The scrape's timeout. |
| `scheduler.metrics.serviceMonitor.additionalLabels` | map | `{}` | Extra labels on the ServiceMonitor, for example to match your Prometheus's `serviceMonitorSelector`. |
| `scheduler.metrics.serviceMonitor.tlsConfig` | map | `{}` | The scrape's TLS settings. When it's empty, the ServiceMonitor skips certificate verification, since the scheduler's certificate is self-signed. |
| `scheduler.metrics.serviceMonitor.relabelings` | list | `[]` | The endpoint's `relabelings`. |
| `scheduler.metrics.serviceMonitor.metricRelabelings` | list | `[]` | The endpoint's `metricRelabelings`. |

### Scheduler pods

These values shape the scheduler's own pods, which the default scheduler places.

| Value | Type | Default | What it sets |
| --- | --- | --- | --- |
| `scheduler.verbosity` | integer | `2` | The log level, passed as `--v`. At `4`, the plugin logs its placement decisions, as [Read the scheduler's logs](../../guides/operate-ome/ome-scheduler.md#logs) shows. |
| `scheduler.probes.liveness` | map: `path`, `initialDelaySeconds`, `periodSeconds` | `/healthz`, `15`, `20` | The liveness probe, over HTTPS on the metrics port. |
| `scheduler.probes.readiness` | map: `path`, `initialDelaySeconds` | `/readyz`, `5` | The readiness probe, over HTTPS on the metrics port. |
| `scheduler.resources` | map | Requests of `200m` CPU and `4Gi` memory, limits of `2` CPUs and `12Gi` memory | The container's resources. The scheduler's memory use grows with the number of pods, nodes and PodGroups in the cluster. |
| `scheduler.nodeSelector` | map | `{}` | The pods' node selector. |
| `scheduler.affinity` | map | A required pod anti-affinity between the replicas on `kubernetes.io/hostname` | The pods' affinity. The default needs a separate node for each replica. |
| `scheduler.tolerations` | list | `[]` | The pods' tolerations. |
| `scheduler.topologySpreadConstraints` | list | A spread of the replicas across nodes, with `maxSkew: 1` and `whenUnsatisfiable: ScheduleAnyway` | The pods' topology spread constraints. |
| `scheduler.podAnnotations` | map | `{}` | Annotations on the pods. For annotation-based Prometheus discovery, set the scheme to `https`. |

### Invalid values

Helm refuses to install or upgrade the release, and prints the message, when the values break one of these rules:

| Rule | Message |
| --- | --- |
| More than one replica needs leader election. | `scheduler.leaderElect must be true when scheduler.replicaCount is greater than 1` |
| While `scheduler.omeControllerIntegration.enabled` is on, the topology annotation is `ome.io/topology-key`. | `scheduler.plugin.podGroupTopologyKeyAnnotation must be ome.io/topology-key when scheduler.omeControllerIntegration.enabled is true; disable OME controller integration for generic producers` |
| With the PodDisruptionBudget on, `minAvailable` is below `scheduler.replicaCount`. | `scheduler.podDisruptionBudget.minAvailable must be less than scheduler.replicaCount so voluntary maintenance can make progress` |
| A ServiceMonitor needs the metrics Service. | `scheduler.metrics.serviceMonitor.enabled requires scheduler.metrics.service.enabled — the ServiceMonitor selects the metrics Service` |

The scheduler checks the label keys in `scheduler.plugin.topologyKey`, `scheduler.plugin.podGroupTopologyKeyAnnotation` and `scheduler.plugin.unsupportedPlacementGroupLabel` when it starts. Helm installs a release with a malformed key. The scheduler then exits with an error that names the argument, like `topologyKey "gpu clique" is not a valid node label key`. Its pods restart until you fix the value.

## PodGroups and labels {#labels-and-annotations}

The plugin reads each gang from its pods and its PodGroup, a scheduler-plugins `scheduling.x-k8s.io/v1alpha1` PodGroup. Install the PodGroup CRD from scheduler-plugins `v0.35.4-devel`, as [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md#before-you-begin) shows.

| Key | On | What the plugin does with it |
| --- | --- | --- |
| `scheduling.x-k8s.io/pod-group` label | Pod | Names the pod's PodGroup, in the pod's namespace. Pods with the same value form one gang, and a pod without the label is standalone. |
| `spec.minMember` | PodGroup | The gang's size. The plugin picks a domain once this many members exist, and binds them once this many reach Permit. It must be positive. |
| `spec.scheduleTimeoutSeconds` | PodGroup | How long the gang's members wait at Permit. When it's unset or not positive, `scheduler.plugin.defaultPermitTimeoutSeconds` applies. |
| `ome.io/topology-key` annotation | PodGroup | The node label whose values define the gang's accelerator domains. It wins over `scheduler.plugin.topologyKey`. `scheduler.plugin.podGroupTopologyKeyAnnotation` sets the annotation's name. |
| `ome.io/placement-group` label | Pod or PodGroup | Asks to place the gang with other gangs, which the plugin doesn't support, so it keeps the gang's members `Pending`. OME never sets it. `scheduler.plugin.unsupportedPlacementGroupLabel` sets the label's name. |

The plugin writes nothing to PodGroups, including their status, and ignores their other fields, like `spec.minResources`.

When the PodGroup CRD is installed, OME creates a PodGroup for each multi-pod [OMENative](../../concepts/omenative/overview.md) Instance, and copies the component's `topologyKey` into its `ome.io/topology-key` annotation. OME labels the Instance's pods even without the CRD, and the OME scheduler then keeps them `Pending`, as [Scheduling events](#scheduling-events) shows. [How OME workloads use it](../../concepts/scheduling/ome-scheduler.md#how-ome-workloads-use-it) has the details.

## Scheduling events

When the scheduler places a pod, the pod gets a `Scheduled` event from `ome-scheduler`, or the name that you set in `scheduler.name`. When it can't, the pod gets `FailedScheduling` events whose message gives the reason. Next to kube-scheduler's own reasons, OMEGangPack gives these:

| Reason | What it means |
| --- | --- |
| `no domain has room for gang {namespace}/{name}` | No domain has room for the whole gang. The gang waits until one does. |
| `PodGroup {namespace}/{name} not resolvable yet` | No PodGroup of that name exists in the pod's namespace. Create it, or fix the label. For OMENative pods, install the CRD and restart OME's manager, as [Opt in OMENative components](../../guides/operate-ome/ome-scheduler.md#opt-in-omenative-components) shows. |
| `PodGroup {namespace}/{name} must declare ome.io/topology-key or the scheduler must configure topologyKey` | The gang has no topology label. Set the component's `topologyKey`, or `scheduler.plugin.topologyKey`. |
| `waiting for all PodGroup member templates for {namespace}/{name}` | Fewer than `minMember` member pods exist. |
| `waiting for gang sibling {namespace}/{pod} to be placed` | The pod has a required pod affinity to a sibling, like its leader, and waits until the sibling is placed. |
| `pinned domain {domain} cannot fit all remaining gang members {namespace}/{name}` | Some members have nodes, and their domain has no room for the rest. The gang stays in that domain while any member runs there. |
| `adopted domain {domain} cannot fit remaining gang members {namespace}/{name}` | The same, after a scheduler restart or failover. |
| `pinned domain {domain} has no node feasible for gang member {namespace}/{name}` | No node in the gang's domain can run this member and leave room for the rest. |
| `gang {namespace}/{name} already has members in multiple topology domains` | The gang's placed members span domains or unlabeled nodes, for example after node labels changed. Its other pods wait until the split ends. |
| `node is reserved for a forming gang` | The node is in the domain of a forming gang. Standalone pods can use it once that gang's last member has a node, or the gang is released. |
| `gang reservation released after all candidate nodes were filtered` | Another filter rejected all the nodes the gang could use in its domain. The scheduler tries its other domains first. |
| `gang {namespace}/{name} unwound after a member failed to schedule` | Another member of the pod's gang failed, so the scheduler tries the gang again. |
| `PodGroup {namespace}/{name} must declare a positive minMember` | Set a positive `minMember` on the PodGroup. |
| `partner placement groups are not supported` | Remove the `ome.io/placement-group` label from the pod and its PodGroup. |

Nodes that the message counts as `didn't satisfy plugin(s) [OMEGangPack]` are outside the gang's domain, or can't run the member and leave room for the rest.

## Metrics

Each replica serves kube-scheduler's standard metrics and OMEGangPack's metrics at `/metrics`. The [Metrics endpoint](#metrics-endpoint) values set who may read them, and [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md#metrics) sets up a Prometheus scrape. Only the leader schedules, so only its OMEGangPack metrics change. OMEGangPack's metrics are alpha: their names and labels can change.

| Metric | Type | Labels | What it counts |
| --- | --- | --- | --- |
| `ome_scheduler_gang_pin_total` | Counter | `result` | Domain decisions for gangs, by `result`, as the next table lists. A gang that fits in no domain adds a `no_fit` each time the scheduler tries one of its pods. |
| `ome_scheduler_gang_gate_total` | Counter | `result` | Permit checks of gang members, by `result`: `wait` while the rest of the gang still needs nodes, and `admit` when a member completes the gang and lets it bind. |
| `ome_scheduler_gang_activation_total` | Counter | `trigger` | Times the plugin sent a gang's pods back to the scheduling queue, by `trigger`: `permit` when a member reached Permit, and `templates_complete` when a waiting gang reached `minMember` pods. |
| `ome_scheduler_gang_unwind_total` | Counter | None | Times the plugin released a gang's pin and rejected its members waiting at Permit, for example after a member failed to schedule. It also grows when a gang ends, its PodGroup is deleted or re-created, or its topology label changes, so it isn't a failure count on its own. |
| `ome_scheduler_pinned_groups` | Gauge | None | Gangs pinned to a domain now, including gangs whose members all have nodes. |

The `result` values of `ome_scheduler_gang_pin_total`:

| `result` | When the plugin counts it |
| --- | --- |
| `pinned` | It picked a domain with room for the whole gang, and pinned the gang to it. |
| `no_fit` | No domain has room for the gang. |
| `adopted` | It pinned a gang to the domain where its members already have nodes, for example after a restart or a failover. |
| `stale_replan` | It released a gang's pin to pick again, because the domain lost its nodes, or lost room for the gang while no member had a node there. |
| `topology_replan` | It released a gang's pin to pick again, because the PodGroup's topology label changed. |

## Scheduler profile

The chart writes the scheduler's KubeSchedulerConfiguration to the `config.yaml` key of the ConfigMap `{name}-config`. With the chart's defaults, for a release in the `ome` namespace, it renders as follows, without its comments:

```yaml title="config.yaml" check=skip
apiVersion: kubescheduler.config.k8s.io/v1
kind: KubeSchedulerConfiguration
leaderElection:
  leaderElect: true
  resourceName: ome-scheduler
  resourceNamespace: ome
profiles:
  - schedulerName: ome-scheduler
    plugins:
      multiPoint:
        enabled:
          - name: OMEGangPack
        disabled:
          - name: DefaultPreemption
      score:
        enabled:
          - name: NodeResourcesFit
            weight: 10
          - name: OMEGangPack
            weight: 10
        disabled:
          - name: PodTopologySpread
    pluginConfig:
      - name: OMEGangPack
        args:
          podGroupTopologyKeyAnnotation: "ome.io/topology-key"
          unsupportedPlacementGroupLabel: "ome.io/placement-group"
          defaultPermitTimeoutSeconds: 600
          podGroupSyncTimeoutSeconds: 30
          gcIntervalSeconds: 60
          standaloneDomainPacking: true
      - name: NodeResourcesFit
        args:
          scoringStrategy:
            type: MostAllocated
            resources:
              - name: cpu
                weight: 1
              - name: memory
                weight: 1
```

`NodeResourcesFit` always uses the `MostAllocated` strategy, which favors busy nodes, where kube-scheduler's default, `LeastAllocated`, favors empty ones. kube-scheduler's other default plugins stay on, with their default weights. When a Helm upgrade changes this configuration, the Deployment replaces the scheduler's pods.

[Alfred](../../concepts/scheduling/alfred.md) simulates the OME scheduler from a copy of this configuration, as [Write the scheduler configurations](../../guides/scheduling/set-up-scheduler-simulation.md#step-2-write-the-scheduler-configurations) shows.

## Related pages

- [The OME scheduler](../../concepts/scheduling/ome-scheduler.md): why OME has a second scheduler, and how OMEGangPack places a gang.
- [Use the OME scheduler](../../guides/operate-ome/ome-scheduler.md): install the chart, opt a workload in, tune packing and troubleshoot.
- [Gang scheduling](../../concepts/serving/gang-scheduling.md): the PodGroups that OME creates for multi-pod Instances.
- [Spread Instances across fault domains](../../guides/omenative/spread-instances-across-fault-domains.md): the hard spread constraints that the OME scheduler enforces.
- [Let Alfred migrate Instances](../../guides/scheduling/let-alfred-migrate-instances.md): to migrate a multi-pod Instance, Alfred needs its pods on the OME scheduler.
