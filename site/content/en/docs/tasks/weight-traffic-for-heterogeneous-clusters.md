---
title: "Weight Traffic for Heterogeneous Clusters"
linkTitle: "Weight Heterogeneous Clusters"
weight: 25
date: 2026-09-27
description: >
  Declare per-replica capacity factors with spec.routing.capacityFactors so TrafficMap weights account for workload clusters whose replicas each serve more (or less) traffic than others.
---

This page shows you how to tell the routing controller that one workload
cluster's replicas each serve **more (or less) traffic** than another's, using
`spec.routing.capacityFactors` on a multi-cluster InferenceService.

By default, a [TrafficMap](/ome/docs/concepts/traffic_map/) weight treats
every ready replica as equal: each serving cluster's share is proportional to
`min(allocated, ready)`. That is correct for a homogeneous fleet and wrong for
a heterogeneous one — a replica on a newer accelerator may sustain several
times the throughput of a replica on an older one, yet raw replica counting
would send both the same share. A **capacity factor** is the per-replica
multiplier that corrects this: the weight becomes proportional to
`min(allocated, ready) × factor`.

TrafficMap routing is part of OME's multi-cluster support, which is still
under active development. The `spec.routing` API is **alpha** and may change
without notice; do not build production automation on it yet.

## Before you begin

- Multi-cluster TrafficMap routing must be enabled for the installation
  (`routing.enabled` in the operator's `multicluster` configuration); see
  [Routing Health Probes](/ome/docs/administration/routing-health-probes/)
  for the enable gate. A service may opt out with
  `spec.routing.enabled: false`, but cannot opt in where the installation
  gate is off.
- Set the field on the **control-plane** InferenceService (the one you
  authored). Capacity factors are a control-plane routing directive: the
  placement flow strips `spec.routing` from the derived copies it creates on
  workload clusters.
- Factors only change a traffic *ratio*, so they matter when several clusters
  serve at once — `All` or `Split` placement. With a single serving home the
  one arm gets all traffic regardless of its factor.

## Declare capacity factors

`spec.routing.capacityFactors` maps **WorkloadCluster names** (the same names
`kubectl get workloadclusters` lists and TrafficMap entries carry in
`cluster`) to per-replica relative capacity, as Kubernetes quantities:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
  namespace: prod
spec:
  placement:
    mode: All
    requirements: "accelerator in (gb300, h100)"
  routing:
    capacityFactors:
      worker-b: "3"      # each worker-b replica carries 3x a baseline replica
      worker-c: "500m"   # each worker-c replica carries half a baseline replica
```

The baseline is `1`: a cluster absent from the map — or the whole field unset
— uses the identity factor 1. `"2"` means each replica on that cluster
carries twice the share of a factor-1 replica; `"500m"` means half. You only
need entries for the clusters that deviate from the baseline.

The factor is **per replica**, not per cluster. You are not pinning a traffic
percentage: the cluster's share still moves with its admitted and ready
replica counts, so scaling events and partial readiness shift traffic
automatically while the per-replica ratio you declared stays constant.

Choose the values yourself, from your own measurements of relative sustained
throughput per replica. The factor is operator-supplied configuration; OME
never derives it from hardware specifications.

## How the factor enters the weight

Each serving cluster's unnormalized weight is
**`min(allocated, ready) × factor`** — where `allocated` is the effective
allocation, possibly already lowered by an endpoint capacity report — gated
to zero by a failing health probe, then forced to zero by any manual drain
override. The results are reduced to their smallest whole-number ratio before
they are written to the TrafficMap.

Worked example: `worker-a` has 4 ready replicas and no declared factor
(identity 1); `worker-b` has 2 ready replicas and factor `"3"`:

| Cluster | min(allocated, ready) | Factor | Raw | Written weight |
|---------|-----------------------|--------|-----|----------------|
| worker-a | 4 | 1 (unset) | 4 | 2 |
| worker-b | 2 | 3 | 6 | 3 |

`worker-b` receives 60% of the traffic with half the replicas. Factors are
honored to milli precision (thousandths, the finest quantity granularity the
weight math uses); a value finer than `1m` is rounded up to the next
thousandth.

The declared factor is recorded on the matching TrafficMap entry as
`capacity.factor`, so the written table explains the weight it carries; the
field is omitted on entries whose cluster has no declared factor. See
[How a weight is computed](/ome/docs/concepts/traffic_map/#how-a-weight-is-computed)
for the full provenance of every input.

## What capacity factors are not

- **Not placement.** The factor weights traffic only. It never influences
  which clusters are selected, how many replicas a cluster admits, or Split
  apportionment.
- **Not live capacity.** The factor is a static declaration of relative
  per-replica throughput. The separate endpoint capacity poll
  (`spec.routing.capacity`) measures what a cluster can *currently* serve and
  can only lower an allocation. The two are independent inputs to the same
  weight: configuring one never enables the other.
- **Not a fixed traffic split.** Because the factor multiplies a live replica
  count, the resulting share changes as readiness changes. To hold an arm at
  zero regardless of capacity, use a
  [traffic-drain override](/ome/docs/tasks/drain-traffic-from-a-workload-cluster/)
  instead.

## Validation and the deprecated alias

Every quantity in `spec.routing.capacityFactors` must be **greater than
zero**; admission rejects the object otherwise
(`spec.routing.capacityFactors["worker-b"] must be positive, got "0"`). Map
keys are *not* checked against existing WorkloadClusters — a misspelled name
is silently ignored and the real cluster keeps the identity factor 1, so
verify the keys against `kubectl get workloadclusters`.

The field replaces the deprecated alias `spec.placement.capacityFactors`,
which has the same shape and semantics and remains readable during its
compatibility window: the routing controller consults it only when the
routing map is absent. Setting **both** is rejected at admission (by CRD
validation and the webhook):

```
spec.routing.capacityFactors and deprecated spec.placement.capacityFactors must not both be set
```

Prefer `spec.routing.capacityFactors` for new manifests. Beyond deprecation,
the routing field is the validated one: a zero or negative quantity on the
deprecated placement alias is not rejected at admission and is silently
treated as the identity factor 1 by the weight computation, hiding the
mistake.

## Verify

`kubectl ome placement explain` summarizes the declared routing intent,
including where the factors come from and how many are declared:

```bash
kubectl ome placement explain chat -n prod
```

The `Capacity factors` row reports the source — `Routing`
(`spec.routing.capacityFactors`), `LegacyPlacement` (the deprecated alias),
`Conflict` (both persisted, seen only on objects that predate the
validation), or `Inherited` (a `spec.routing` block is declared but carries
no factor map; every cluster at identity 1) — and the entry count.

Then read the effect on the generated table:

```bash
kubectl get trafficmap chat -n prod -o yaml
```

```yaml
spec:
  entries:
  - cluster: worker-a
    weight: 2
    capacity:                # no factor field — identity 1
      allocated: 4
      ready: 4
  - cluster: worker-b
    weight: 3
    capacity:
      allocated: 2
      ready: 2
      factor: "3"
```

The TrafficMap is controller-reported state: a published table records what
the routing controller computed, not observed data-plane behavior.
