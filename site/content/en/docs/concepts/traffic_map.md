---
title: "Traffic Map"
linkTitle: "Traffic Map"
weight: 34
description: >
  Read the TrafficMap of a multi-cluster InferenceService — per-cluster routing entries, weight provenance, and the Routable, Published, OverrideActive, and CapacityFallback conditions.
---

A **TrafficMap** is the capacity-aware routing table of one multi-cluster InferenceService: one entry per serving cluster, each carrying the endpoint a gateway should send to, the final traffic **weight**, and the evidence that produced that weight. Placement decides *where* replicas run and quota decides *how much* each cluster may run; the TrafficMap records the resulting *traffic split*.

> **Note:** multi-cluster routing is **alpha** and under active development. The TrafficMap is strictly **controller-reported state** — it records what the routing controller computed and what the publisher claims to have written, not observed data-plane behavior. A published map is not proof that gateways are actually splitting traffic that way.

This page is about **reading** a TrafficMap you already have. Configuring the inputs that feed it (enabling routing, endpoint health probes, capacity polling) is out of scope here.

## Where it comes from

The TrafficMap is **machine-written**. A control-plane routing controller generates it from the InferenceService's `status.placement` plus quota allocation, and rewrites `spec` on every pass — treat the resource as read-only; hand edits are overwritten, never honored.

It is namespaced, **named after the InferenceService**, and owner-referenced to it, so it is garbage-collected with the service (a publisher finalizer may briefly hold it while external route objects are cleaned up). It exists for as long as the service is routed — **including while nothing is routable**. An empty or all-zero table is a state of the map, not its absence, and the `Routable` condition always says why.

```bash
kubectl get trafficmap chat -n prod   # short names: tm, tmap
```

```
NAME   MODE    PUBLISHED   ROUTABLE   OVERRIDE   REASON     AGE
chat   Split   True        True       False      Routable   4d
```

`MODE` mirrors the service's placement mode (`Single`, `All`, or `Split`); the other columns surface the conditions described below, with `REASON` taken from `Routable`.

## Reading the routing table

`spec.entries` holds one entry per serving cluster. This example shows a healthy two-cluster map where one cluster nevertheless gets no traffic:

```yaml
apiVersion: ome.io/v1beta1
kind: TrafficMap
metadata:
  name: chat
  namespace: prod
spec:
  service: chat                    # the routed InferenceService
  mode: Split
  observedISVCGeneration: 7        # ISVC generation this table was computed from
  entries:
    - cluster: worker-a
      endpoint: https://chat.worker-a.example.com
      weight: 1
      healthy: true
      capacity:
        allocated: 6               # effective allocation ceiling
        ready: 6                   # live ready replicas
        source: ControlPlane
      probe:
        result: Passing
        gated: false
        lastProbeTime: "2026-09-26T10:00:00Z"
    - cluster: worker-b
      endpoint: https://chat.worker-b.example.com
      weight: 0
      healthy: true                # ready and reachable — yet weight 0
      capacity:
        allocated: 0               # the home reported it can serve nothing
        ready: 4
        source: Endpoint
        reported: 0
      probe:
        result: Passing
        gated: false
        lastProbeTime: "2026-09-26T10:00:00Z"
status:
  sourceUID: 4f6b1c2a-…            # UID of the generating InferenceService
  published: true
  observedTrafficMapGeneration: 12 # spec generation the publisher last realized
  gatewayRef:
    group: gateway.networking.k8s.io
    kind: HTTPRoute
    name: chat
  conditions:
    - type: Routable
      status: "True"
      reason: Routable
      message: 2 home(s) serving
    - type: Published
      status: "True"
      reason: Published
      message: TrafficMap is published by publisher "gatewayapi"
    - type: OverrideActive
      status: "False"
      reason: NoOverrides
      message: no traffic-drain annotation override is configured
    - type: CapacityFallback
      status: "False"
      reason: EndpointCapacityAvailable
      message: endpoint capacity is available for all 2 home(s)
```

### How a weight is computed

Each entry's `weight` is proportional to **`min(capacity.allocated, capacity.ready) × capacity.factor`**, gated to zero by a failing probe, then forced to zero by any manual drain override. The resulting values are reduced to their smallest whole-number ratio — which is why the example shows `1`, not `6` — as relative integers from 0 through 1,000,000 (the Gateway API `backendRef` weight limit). A consumer applies them verbatim, or divides by their sum for a percentage. All gating and normalization is already done; nothing is left for the gateway to redo.

The per-field provenance:

| Field | Meaning |
|-------|---------|
| `capacity.allocated` | The effective allocation ceiling: the control-plane-admitted replica count, optionally *lowered* by the cluster's own capacity report. |
| `capacity.ready` | The cluster's live ready-replica count. It caps `allocated` in the weight; zero forces weight 0. |
| `capacity.factor` | Operator-supplied per-replica relative capacity for heterogeneous hardware (baseline `1`, unset means `1`). |
| `capacity.source` | `ControlPlane` (default) or `Endpoint` — the latter **only** when the cluster's report actually lowered the plan. |
| `capacity.reported` | The cluster's own servable-capacity figure, recorded whenever the endpoint source answered — even when it was at or above the plan and changed nothing. A report can only lower a weight, never raise it. |
| `capacity.fallbackReason` | Why a *configured* endpoint capacity source produced no usable ceiling and the control-plane number was retained. Empty when polling is off or the latest result was usable. |
| `healthy` | Readiness plus probe evidence: ready replicas exist and no probe verdict says unreachable. Independent of capacity — a healthy cluster can still carry weight 0. |
| `probe` | Present only when end-to-end probing is configured; see below. |
| `drainRefs` | The override IDs from the service's `ome.io/traffic-drain` annotation that force this entry to zero. Empty means no manual override contributed. |

### Probe evidence

`probe` records what the active end-to-end probe observed against the entry's `endpoint` — the URL a client uses — so it covers the whole serving path (ingress, DNS, certificate, route), not just pod readiness:

- **`result`** — `Passing`, `Failing`, or `Unknown`. `Unknown` means no verdict was reached (probe not run yet, missing credentials, blocked egress) and **never gates a weight**: that state is uniform across clusters, so treating it as failure would zero the whole fleet at once.
- **`consecutiveFailures`** — unbroken `Failing` verdicts. A weight is gated only once this reaches the configured threshold; a single dropped packet must not move a large traffic share.
- **`gated`** — the current hysteresis gate. It stays `true` through `Unknown` attempts until enough consecutive passes reopen the cluster, so `result: Passing` with `gated: true` means recovery is in progress but not yet trusted.
- **`lastProbeTime`** — when the latest attempt completed. A stale timestamp exposes a stalled prober rather than reading as a steady pass.
- **`message`** — the observed status code, transport error, or why the probe could not run.

## Why is this cluster's weight zero?

Read the zero-weight entry and match it against the first row that fits:

| Observation on the entry | Cause |
|--------------------------|-------|
| `drainRefs` is non-empty | A manual [traffic-drain override](/ome/docs/tasks/drain-traffic-from-a-workload-cluster/) holds this arm at zero. The listed IDs are the keys in the service's `ome.io/traffic-drain` annotation; `undrain` them to release. |
| `capacity.ready: 0` (and `healthy: false`) | No ready replicas in that cluster. Fix the workload there; traffic returns as readiness does. |
| `capacity.allocated: 0` with `source: Endpoint`, `reported: 0` | The cluster itself reported it can serve nothing (for example, no usable prefill/decode pairing yet), which lowered its ceiling to zero even though replicas are ready. |
| `capacity.allocated: 0` with `source: ControlPlane` | The control plane admitted no replicas to this cluster — a placement/quota outcome, not a health one. |
| `probe.gated: true` | The end-to-end probe conclusively failed often enough to gate the arm. Check `probe.message` for the status code or transport error; the endpoint path (ingress, DNS, certificate) is the usual suspect when pods are ready. |

If **every** entry is zero (or the table is empty), stop reading entries and read the `Routable` condition instead — its reason names the fleet-wide cause.

## Conditions

Four conditions with two writers: the **routing controller** owns `Routable`, `CapacityFallback`, and `OverrideActive`; the **publisher** owns `Published`.

### Routable

Whether the map carries any target a gateway can send traffic to. Written on every pass, so an empty or all-zero table always explains itself:

| Status | Reason | Meaning |
|--------|--------|---------|
| `True` | `Routable` | At least one arm has positive weight. |
| `True` | `AllHomesProbeFailed` | Every probe conclusively failed, but the all-failed policy is *PreserveTraffic*: capacity-derived weights are retained for ready clusters rather than blackholing all traffic on a signal that failed everywhere at once. |
| `False` | `NotPlaced` | The service is not Placed yet — the table is **empty**; there are no homes to route to. |
| `False` | `NoAddressableHome` | Placed, but no admitted cluster reports an addressable endpoint — table **empty**. |
| `False` | `AllHomesUnready` | Clusters are addressable but none has ready replicas — table present, **all-zero**. |
| `False` | `NoRoutableCapacity` | Ready clusters exist, but none has positive routable capacity (`min(allocated, ready)` is zero everywhere) — all-zero. |
| `False` | `AllHomesProbeFailed` | Every probe conclusively failed and the all-failed policy is *Drain*: an authoritative all-zero table. |
| `False` | `TrafficDrain` | Manual drain overrides turned an otherwise-positive table all-zero. Inspect `spec.entries[].drainRefs`. |

### OverrideActive

Whether manual traffic-drain annotation overrides currently hold any arm at zero:

| Status | Reason | Meaning |
|--------|--------|---------|
| `True` | `OverridesApplied` | At least one override matches a route arm; the message counts applied overrides and held arms. |
| `False` | `OverridesPending` | Overrides exist on the service but **no routable arm matches** the named cluster — commonly a misspelled `--workload-cluster`. The override sits pending; it drains nothing. |
| `False` | `NoOverrides` | No traffic-drain override is configured. |

### CapacityFallback

Whether configured endpoint-capacity polling has *fallen open* to the control-plane allocation. It has **abnormal-true polarity** — `True` is the degraded state:

| Status | Reason | Meaning |
|--------|--------|---------|
| `True` | `EndpointCapacityUnavailable` | One or more clusters could not supply a usable capacity report and are using the control-plane number. Inspect `spec.entries[].capacity.fallbackReason`. |
| `False` | `EndpointCapacityAvailable` | Polling is on and every cluster's report was usable. |
| `False` | `CapacityPollingDisabled` | Endpoint capacity polling is not enabled; the plan always stands. |
| `False` | `NoCapacityTargets` | No addressable clusters to poll. |

### Published

Whether the active publisher has realized this map onto a concrete data plane — for the built-in Gateway API publisher (`gatewayapi`), an HTTPRoute identified by `status.gatewayRef`. `True`/`Published` means the publisher's write succeeded; `False`/`Withdrawn` means it deliberately removed or withheld publication (publishing disabled, or the service resolves no global hostname) rather than failing. Failure reasons (`ApplyFailed`, `ClaimRejected`, `InvalidPlan`, `InvalidOptions`, `InvalidOwner`, `LegacyLifecycleActive`, `PublisherChanged`, `UnpublishFailed`) mark rejected or retried publication, and `Unpublished` reports that publisher state has been fully removed.

Alongside the condition, the publisher owns `status.published`, `status.gatewayRef`, `status.observedTrafficMapGeneration` — compare it with `metadata.generation` to see whether the latest table has been realized yet — and `status.publisher`, a durable journal of the data-plane objects it claimed (`claimedTargets`), kept so cleanup survives restarts.

Even `Published: True` is the publisher's claim that it wrote the route object, not evidence of live traffic distribution.

## Staleness checks

Two generation fields bracket the pipeline: `spec.observedISVCGeneration` says which InferenceService generation the table was computed from (lagging the service's current generation means the routing controller hasn't caught up), and `status.observedTrafficMapGeneration` says which table generation the publisher last realized (lagging `metadata.generation` means the data plane hasn't caught up). `status.sourceUID` pins the map to the exact InferenceService instance that generated it, so a deleted-and-recreated service is distinguishable from its predecessor's map.
