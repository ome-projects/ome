# ome-scheduler Helm chart

Installs [ome-scheduler](../../scheduler) — the upstream kube-scheduler built as a
library with the OME placement plugin (`OMEGangPack`) — as a **second
scheduler**. It does not replace the cluster's default scheduler; workloads opt
in per-pod via `spec.schedulerName`.

> **Alpha version pin:** the scheduler module and image are built against
> Kubernetes **1.35.4**, and the chart accepts only Kubernetes 1.35. The matching
> scheduler-plugins dependency is currently published only as the
> `v0.35.4-devel` tag. Keep this chart opt-in until a stable v0.35 tag replaces
> that development dependency.

## Why a separate chart

ome-scheduler is optional, opt-in, and pinned to the fleet's Kubernetes version
on its own cadence (it's a separate Go module). Keeping it out of the operator
chart (`ome-resources`) means its broad, sensitive scheduler RBAC isn't forced
onto every OME install, and it can be upgraded/rolled back independently.

## Prerequisite: the PodGroup CRD

The plugin reads the scheduler-plugins `PodGroup` CR
(`scheduling.x-k8s.io/v1alpha1`) for gang size and the topology annotation. That
CRD must be installed in the cluster first (it is **not** bundled here — it's an
external, shared dependency):

```sh
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/scheduler-plugins/v0.35.4-devel/config/crd/bases/scheduling.x-k8s.io_podgroups.yaml
```

## Install

```sh
helm install ome-scheduler charts/ome-scheduler -n ome --create-namespace
```

## Using it

A workload opts in by setting the scheduler name and declaring its gang via a
PodGroup:

```yaml
# pod template
spec:
  schedulerName: ome-scheduler
  # + the standard gang label:
  #   metadata.labels["scheduling.x-k8s.io/pod-group"]: my-gang
```

```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: PodGroup
metadata:
  name: my-gang
  annotations:
    ome.io/topology-key: <node-label-that-defines-a-domain>   # e.g. an NVLink clique / TPU slice / rack label
spec:
  minMember: <gang size>
  scheduleTimeoutSeconds: 600
```

The scheduler plugin bakes in no OME metadata names: this chart configures
`scheduler.plugin.podGroupTopologyKeyAnnotation` as `ome.io/topology-key`.
Gang size comes from `minMember`, the annotation value names the domain label,
and node free-ness comes from each pod's own resource requests.

The chart validates that annotation name while
`scheduler.omeControllerIntegration.enabled` is true, because the OME
controller publishes the fixed `ome.io/topology-key` contract. Generic
PodGroup producers may disable the integration guard and configure another
annotation name.

Add the fleet's accelerator resource to
`scheduler.nodeResourcesFit.resources` if the built-in `MostAllocated` score
should pack at node level. Choosing production weights requires fleet-specific
fragmentation measurements and is outside this chart's contract:

```yaml
scheduler:
  nodeResourcesFit:
    resources:
      - name: example.com/accelerator
        weight: 10
      - name: cpu
        weight: 1
      - name: memory
        weight: 1
```

The chart disables only built-in `PodTopologySpread` scoring because soft
spreading contradicts packing. Explicit `DoNotSchedule` topology constraints
remain enforced by its filter path.

## Availability and metrics

The default deployment runs two replicas with leader election and creates a
PodDisruptionBudget. Rendering fails when `replicaCount > 1` and leader election
is disabled. Required hostname anti-affinity keeps the leader and standby off the
same node. Updates replace one standby at a time with no surge, so a two-node
control pool can roll while one scheduler remains available. The PDB protects
the same floor during voluntary disruptions, and rendering rejects a
`minAvailable` value greater than or equal to `replicaCount`, which would block
maintenance. On a one-node development cluster, set `replicaCount: 1` and
either disable the PDB or set `minAvailable: 0`. The optional
`*-metrics` Service exposes the scheduler's
authenticated HTTPS `/metrics` endpoint; configure your monitor with Kubernetes
service-account authentication and any required RBAC.

## Permissions and version alignment

The chart binds the scheduler ServiceAccount to the API server's auto-reconciled
`system:kube-scheduler` and `system:volume-scheduler` ClusterRoles. This avoids a
copied permission list drifting from the Kubernetes minor in the cluster. The
chart-owned `*-extras` role adds only the scheduler-specific leader Lease and
read-only PodGroup access.

This role arrangement also means Kubernetes supplies the DRA permissions that
belong to that release when DRA is enabled. It does **not** make a scheduler
binary portable across Kubernetes minors; the chart's `kubeVersion` guard must
move together with the binary, dependency set, and envtest pin.

## Test boundary

`make helm-lint` renders this chart and checks the scheduler configuration, HA
guard, RBAC bindings, probes, metrics Service, PDB, and replica separation. The
scheduler module's envtest suite exercises the plugin inside kube-scheduler.
`make test-kind-scheduler` builds the scheduler locally and runs the focused
OME-controller-to-scheduler contract on its own Kubernetes 1.35 KIND cluster:
the generated scheduler name and PodGroup annotation must result in the whole
gang landing in one synthetic topology domain. The broader controller KIND
suite still uses the default scheduler and does not duplicate that coverage.

## Key values

| Value | Default | Meaning |
|---|---|---|
| `scheduler.name` | `ome-scheduler` | Profile / opt-in name (also the leader-election lock). |
| `scheduler.image.repository` / `.tag` | `ome-scheduler` / `latest` | Image (prefixed by `global.hub` if set). |
| `scheduler.replicaCount` | `2` | Leader plus warm standby for HA. |
| `scheduler.leaderElect` | `true` | Only the leader schedules. |
| `scheduler.omeControllerIntegration.enabled` | `true` | Enforce OME's fixed PodGroup topology annotation contract. |
| `scheduler.deploymentStrategy` | one unavailable, no surge | Roll on two eligible nodes without violating required anti-affinity. |
| `scheduler.verbosity` | `2` | klog `-v` level. |
| `scheduler.plugin.topologyKey` | `""` | Fallback domain label for unannotated PodGroups. |
| `scheduler.plugin.podGroupTopologyKeyAnnotation` | `ome.io/topology-key` | PodGroup annotation whose value names the per-gang domain label. |
| `scheduler.plugin.unsupportedPlacementGroupLabel` | `ome.io/placement-group` | Label that fails closed until partner-gang reservations are implemented. |
| `scheduler.plugin.defaultPermitTimeoutSeconds` | `600` | Configured fallback for PodGroups without a timeout. |
| `scheduler.plugin.podGroupSyncTimeoutSeconds` | `30` | Maximum initial PodGroup cache-sync wait. |
| `scheduler.plugin.gcIntervalSeconds` | `60` | Pin and reservation reconciliation cadence. |
| `scheduler.plugin.standaloneDomainPacking` | `true` | Pack standalone whole-node pods into partly-filled domains; inert until `topologyKey` is set. |
| `scheduler.percentageOfNodesToScore` | unset | Profile-scoped feasibility-scan breadth. Unset keeps upstream's adaptive 5-50% sample; set `100` wherever `standaloneDomainPacking` is active so the domain score sees every free node. |
| `scheduler.omeGangPack.scoreWeight` | `10` | Weight of the domain-packing score; keep at or above `nodeResourcesFit.scoreWeight`. |
| `scheduler.nodeResourcesFit.*` | see values | `MostAllocated` score weight and resource weights. |
| `scheduler.metrics.service.enabled` | `true` | Create the authenticated HTTPS metrics Service. |
| `scheduler.podDisruptionBudget.*` | enabled / `1` | Keep one scheduler available; `minAvailable` must be lower than replicas. |
| `scheduler.resources.requests.memory` / `.limits.memory` | `4Gi` / `12Gi` | Accommodate cluster-wide informer caches and startup LIST peaks; override for constrained clusters. |
| `scheduler.affinity` | required hostname anti-affinity | Keep the active scheduler and standby on different nodes. |

See [`values.yaml`](values.yaml) for the full set.
