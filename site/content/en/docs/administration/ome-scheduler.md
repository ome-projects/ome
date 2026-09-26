---
title: "OME Scheduler"
linkTitle: "OME Scheduler"
weight: 15
description: >
  Install the optional ome-scheduler second scheduler and opt workloads into topology-packed gang placement.
---

`ome-scheduler` is the upstream kube-scheduler built as a library (the
`scheduler/` Go module in this repository) with one extra plugin registered:
**OMEGangPack**. Given a gang — a set of pods that must run together — it picks
a single topology *domain* that fits the whole gang and keeps every member
there: one NVLink clique, one TPU slice, one rack, whatever node label your
fleet uses to mark the boundary. `PreFilter` best-fits and pins the domain,
`Filter` rejects nodes outside it, `Reserve` claims the domain's capacity so
two gangs cannot race into the same one, `Permit` holds each member at the
gate until all of them can land, and a half-formed gang is unwound rather than
left partially bound.

The chart at `charts/ome-scheduler` installs it as a **second scheduler**. It
does not replace or reconfigure the cluster's default scheduler; nothing
changes for any workload until a pod opts in with
`spec.schedulerName: ome-scheduler`.

> **Alpha, pinned to Kubernetes 1.35.** The scheduler binary embeds
> `k8s.io/kubernetes v1.35.4` and the chart refuses to install on anything but
> Kubernetes 1.35 (`kubeVersion: ">=1.35.0-0 <1.36.0-0"`). Its
> scheduler-plugins dependency is currently the pre-release `v0.35.4-devel`
> tag — no stable v0.35 tag exists yet. Treat the whole component as opt-in
> until that stabilizes.

It ships as its own chart, not inside `ome-resources`, on purpose: a scheduler
needs broad cluster RBAC (it binds pods), and keeping it separate means that
grant is never forced onto an OME install that doesn't want gang placement,
and the scheduler can be upgraded or rolled back on its own cadence.

## Prerequisite: the PodGroup CRD

The plugin reads gang metadata from the scheduler-plugins `PodGroup` CR
(`scheduling.x-k8s.io/v1alpha1`). That CRD is an external, shared dependency —
the chart does not bundle it — and must exist before gangs can be placed:

```bash
kubectl apply -f https://raw.githubusercontent.com/kubernetes-sigs/scheduler-plugins/v0.35.4-devel/config/crd/bases/scheduling.x-k8s.io_podgroups.yaml
```

The scheduler itself boots fine without the CRD and schedules non-gang pods
normally; gang behavior activates once the CRD (and PodGroups) appear.

## Install

```bash
helm install ome-scheduler charts/ome-scheduler -n ome --create-namespace
```

The default image reference is the unqualified `ome-scheduler:latest`; there
is no published image yet. Build it from source with `make
ome-scheduler-image` (which uses `dockerfiles/ome-scheduler.Dockerfile` and
tags `$(REGISTRY)/ome-scheduler:$(TAG)`), push it to your registry, and set
`global.hub` — the chart prefixes it onto any repository that doesn't already
contain a `/`.

Then verify the rollout and that a leader was elected:

```bash
kubectl -n ome rollout status deployment/ome-scheduler
kubectl -n ome get lease ome-scheduler
```

RBAC-wise the chart binds the scheduler's ServiceAccount to the API server's
auto-reconciled `system:kube-scheduler` and `system:volume-scheduler`
ClusterRoles — so core, volume-binding, and DRA permissions always match the
cluster's Kubernetes minor instead of drifting in a copied list. The only
chart-owned role adds the scheduler's leader-election Lease and **read-only**
PodGroup access.

## Opt a workload in

Three pieces, all declared by the workload — the scheduler binary bakes in no
defaults for any of them:

1. **The scheduler name.** The pod sets `spec.schedulerName: ome-scheduler`
   (the chart's `scheduler.name`). This is the entire opt-in surface.
2. **Gang membership.** Each member pod carries the scheduler-plugins standard
   label `scheduling.x-k8s.io/pod-group: <podgroup-name>`.
3. **A PodGroup** in the same namespace declaring the gang size and the
   packing domain:

```yaml
apiVersion: scheduling.x-k8s.io/v1alpha1
kind: PodGroup
metadata:
  name: my-gang
  annotations:
    # The VALUE is a node label KEY. Nodes sharing a value of that label form
    # one domain, e.g. an NVLink clique label, a TPU slice/partition label
    # (such as GKE's cloud.google.com/gke-tpu-partition-2x2x2-id), or
    # topology.kubernetes.io/rack.
    ome.io/topology-key: nvidia.com/gpu.clique
spec:
  minMember: 4              # gang size: all 4 must land in one domain
  scheduleTimeoutSeconds: 600
```

Gang size comes from `spec.minMember`, the domain label from the
`ome.io/topology-key` annotation, and per-node free-ness from each pod's own
resource requests. `spec.scheduleTimeoutSeconds` bounds how long an arrived
member waits at the Permit gate for its siblings; when unset, the chart's
`scheduler.plugin.defaultPermitTimeoutSeconds` (default `600`) applies. On
timeout the gang is unwound and retried rather than holding nodes forever.

Two fallbacks and one guard are worth knowing:

- A PodGroup **without** the annotation falls back to the chart-wide
  `scheduler.plugin.topologyKey`. If neither is set, gang pods stay
  unschedulable with an explicit message naming the missing configuration —
  the plugin never silently places a gang unpinned.
- The annotation *name* is chart configuration
  (`scheduler.plugin.podGroupTopologyKeyAnnotation`), not compiled into the
  plugin. While `scheduler.omeControllerIntegration.enabled` is true (the
  default), rendering fails if it is changed away from `ome.io/topology-key`,
  because that is the fixed name the OME controller publishes. Generic,
  non-OME PodGroup producers may disable the guard and pick their own name.
- A PodGroup labeled `ome.io/placement-group` **fails closed**: partner-gang
  (multi-gang co-placement) reservations are not implemented yet, and the
  plugin refuses to schedule such gangs rather than silently placing partners
  apart.

### OME-managed workloads

For OMENative multi-pod instances (leader + workers), the OME controller
already creates the PodGroup — `minMember` equal to the instance's total pod
count — and stamps the `scheduling.x-k8s.io/pod-group` label on the member
pods, whenever the PodGroup CRD is installed. What it deliberately does *not*
do is set the scheduler name: set `spec.schedulerName: ome-scheduler` on the
`ServingRuntime` (or on the InferenceService component's pod spec) yourself.
If PodGroups are being created while the effective scheduler name is empty or
`default-scheduler`, the controller emits a one-shot
`MaybeNoGangScheduler` warning event on the workload, because the stock
kube-scheduler ignores PodGroups entirely.

## Packing tuning

**Node-level packing.** The profile scores nodes with `NodeResourcesFit` in
`MostAllocated` mode. Add your fleet's accelerator resource so the score packs
what you actually care about — the defaults only weight `cpu` and `memory`:

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

Production weights need fleet-specific fragmentation measurements; the chart
deliberately doesn't guess them.

**Domain-level packing for standalone pods.**
`scheduler.plugin.standaloneDomainPacking` (default `true`) steers standalone
— non-gang — whole-node pods toward already-busy domains, so partly-filled
slices fill up before empty ones are opened and whole free slices stay
available for multi-host gangs. It is inert until the chart-wide
`scheduler.plugin.topologyKey` is set, since it needs to know what a domain
is. It comes with a trade-off: the domain score only sees the nodes the
feasibility scan sampled, and upstream's adaptive default samples 5–50% of the
cluster — a partial view can rank an empty domain above a partly-filled one.
Set `scheduler.percentageOfNodesToScore: 100` wherever standalone packing is
active. The setting is profile-scoped, so only workloads that opted into
`ome-scheduler` pay for the exhaustive scan; gang pods never depend on it
because `PreFilter` already narrows them to one pinned domain.

**Score interplay.** Keep `scheduler.omeGangPack.scoreWeight` (default `10`)
at or above `scheduler.nodeResourcesFit.scoreWeight` (default `10`): when all
whole-node candidates are equally empty, node-level `MostAllocated` ties, and
only the domain-level signal can break the tie.

**What is disabled, and why.** The profile disables `PodTopologySpread`'s
*scoring* only — soft spreading is the exact opposite of packing — while
explicit hard `DoNotSchedule` constraints are still enforced by its filter
path (`scheduler.disablePodTopologySpreadScore`). It also disables
`DefaultPreemption` (`scheduler.disablePreemption`, default `true`): default
preemption is domain-blind for gangs and can evict victims scattered across
domains that never assemble one whole free domain. ome-scheduler waits for
capacity instead; the default scheduler still preempts for everything *it*
schedules.

## Availability

The chart defaults to two replicas with leader election — one active
scheduler, one warm standby. A scheduler is a single-writer decision loop, so
rendering fails if `replicaCount > 1` with `leaderElect: false` (two active
instances would double-bind pods). Required hostname anti-affinity keeps the
two replicas on different nodes, updates replace one standby at a time with no
surge, and a PodDisruptionBudget (`minAvailable: 1`) holds the same floor
during drains; rendering rejects `minAvailable >= replicaCount`, which would
block maintenance. On a one-node development cluster set
`scheduler.replicaCount: 1` and either disable the PDB or set
`minAvailable: 0`.

Memory requests default to `4Gi` (limit `12Gi`) because kube-scheduler's
informers cache cluster-wide Pods, Nodes, and PodGroups and startup LIST
processing peaks above steady state; constrained clusters can override.

## Metrics and debugging

The scheduler serves `/metrics` on a secure HTTPS port (default `10259`) with
delegated authentication and authorization — the same contract as
kube-scheduler, so a plain-HTTP anonymous scrape fails. Scraping needs three
things: `scheduler.metrics.authDelegation.enabled` (default `true`, lets the
scheduler verify callers via TokenReview/SubjectAccessReview), the scraper
bound to the generated `*-metrics-reader` ClusterRole (list its
ServiceAccounts under `scheduler.metrics.reader.serviceAccounts` — the chart
does not guess who scrapes), and a scrape target — either the created
`*-metrics` Service with `scheduler.metrics.serviceMonitor.enabled: true` on a
Prometheus-Operator stack, or `prometheus.io/*` pod annotations with the
scheme set to `https`.

Alongside the built-in scheduler framework metrics, the plugin exports
`ome_scheduler_gang_pin_total` (placement decisions by result),
`ome_scheduler_gang_gate_total` (Permit waits vs. admissions),
`ome_scheduler_gang_activation_total`, `ome_scheduler_gang_unwind_total`
(gangs torn down after a member failed), and `ome_scheduler_pinned_groups`
(gangs currently holding a domain reservation). To debug a placement — which
domain a gang pinned, per-domain free counts, why Filter rejected a node —
raise `scheduler.verbosity` to `4`; the decision trace logs at that level.

## Key values

| Value | Default | Meaning |
| --- | --- | --- |
| `scheduler.name` | `ome-scheduler` | The `spec.schedulerName` workloads opt in with; also the leader-election lock name. |
| `scheduler.image.repository` / `.tag` | `ome-scheduler` / `latest` | Image, prefixed by `global.hub` when set. |
| `scheduler.replicaCount` | `2` | Leader plus warm standby; requires `leaderElect: true` when above 1. |
| `scheduler.omeControllerIntegration.enabled` | `true` | Guard the annotation name below against drifting from OME's fixed contract. |
| `scheduler.plugin.podGroupTopologyKeyAnnotation` | `ome.io/topology-key` | PodGroup annotation whose value names the per-gang domain label. |
| `scheduler.plugin.topologyKey` | `""` | Fallback domain label for PodGroups without the annotation; also what standalone packing packs by. |
| `scheduler.plugin.defaultPermitTimeoutSeconds` | `600` | Gate timeout for PodGroups without `scheduleTimeoutSeconds`. |
| `scheduler.plugin.standaloneDomainPacking` | `true` | Pack standalone whole-node pods into partly-filled domains; inert until `topologyKey` is set. |
| `scheduler.percentageOfNodesToScore` | unset | Set `100` wherever standalone packing is active so the domain score sees every free node. |
| `scheduler.omeGangPack.scoreWeight` | `10` | Domain-packing score weight; keep at or above `nodeResourcesFit.scoreWeight`. |
| `scheduler.nodeResourcesFit.resources` | `cpu`, `memory` | `MostAllocated` resource weights; add your accelerator resource. |
| `scheduler.disablePreemption` | `true` | Skip domain-blind default preemption; wait for capacity instead. |
| `scheduler.podDisruptionBudget.*` | enabled, `minAvailable: 1` | Must stay below `replicaCount`. |
| `scheduler.verbosity` | `2` | klog level; `4` traces placement decisions. |

See `charts/ome-scheduler/values.yaml` for the full commentary.
