---
title: "Routing Capacity Polling"
linkTitle: "Routing Capacity Polling"
weight: 13
date: 2026-09-27
description: >
  Configure the endpoint-reported capacity poll that can lower a workload cluster's TrafficMap weight — the Report format, the samples/quorum smoothing window, and the maxAge fall-open.
---

When multi-cluster TrafficMap routing is enabled, the control plane can
periodically ask each serving home's externally addressable endpoint what it
can currently serve. The answer is applied as a **ceiling** on that home's
control-plane allocation — `allocated` becomes `min(planned, reported)` — and
therefore can only **lower** the home's traffic weight, never raise it.

TrafficMap routing is part of OME's multi-cluster support, which is still
under active development. The `spec.routing` API is **alpha** and may change
without notice; do not build production automation on it yet.

Why a ceiling and not a two-way signal: only the home knows its realized
prefill/decode pairing. The control plane computes
`min(ready_prefill, ready_decode)`, which is an *upper bound* on usable pairs
rather than a count of them — and during a rollout the two components recover
at different rates, so the skew is widest exactly when the number matters
most. Letting a report raise a share would reintroduce the reactive drift
capacity planning exists to remove, and would let a hot, buggy, or stale home
claim more traffic than the plan ever granted it. Constrained to a ceiling, a
home can say only "I cannot yet serve what you planned for me."

The capacity poll is one of several **independent** inputs to a TrafficMap
weight. A weight is proportional to `min(allocated, ready) × factor`, gated by
the separate [end-to-end health
probe](/ome/docs/administration/routing-health-probes/) (`routing.probe`), then
forced to zero by any [manual drain
override](/ome/docs/tasks/drain-traffic-from-a-workload-cluster/). Probe and
capacity are explicitly independent: they fix different failure modes
(reachability versus capacity overstatement), they cost differently — a deep
probe consumes serving capacity, a capacity poll does not — and enabling one
never enables the other.

## How a poll becomes a ceiling

Every `period`, the control plane sends the configured `method` to each home's
endpoint plus `path`. A reading is **accepted** only when all of the following
hold:

1. The response status is exactly **200**; anything else falls open.
2. The body (read up to `routing.observer.maxResponseBytes`) decodes under the
   configured `format`.
3. The decoded `servable` count is non-negative. A negative report is
   malformed, not an instruction to serve nothing — a parse or arithmetic bug
   on the home must not be able to black-hole it.
4. The report's `observedAt` stamp is not in the future and is younger than
   `maxAge`.

Accepted readings enter a per-home window of the `samples` most recent, and
the applied ceiling is the **`quorum`-th smallest** reading in that window.
Until the window holds at least `quorum` readings, no ceiling applies and the
control-plane plan stands.

**Every failure mode falls open to the plan.** An unreachable endpoint, a
non-200 status, an undecodable body, a stale or future-stamped report — each
releases the ceiling rather than gating the home, and also clears the sample
window (retaining readings would keep constraining a home that can no longer
be observed). Fail-closed would turn a control-plane-to-data-plane network
blip into a fleet-wide traffic cutoff, which is far worse than briefly routing
to an over-weighted home. The fallback is never silent: the reason lands in
the TrafficMap entry's `capacity.fallbackReason` and in the `CapacityFallback`
condition, because "the home agreed with the plan" and "the poll failed" look
identical in the weight but have opposite remediations.

## The `Report` format

`Report` is the built-in response shape and the only one this repository
guarantees. The endpoint answers with a JSON object:

```json
{"servable": 4, "observedAt": "2026-09-27T10:00:00Z"}
```

- **`servable`** is an **absolute count in servable units** — usable
  prefill/decode pairs for a disaggregated home, or servable replicas
  otherwise. Deliberately not a utilization ratio (a saturated home and an
  idle one report the same fraction, which says nothing about how much either
  can serve), and deliberately not a flat sum of per-worker request slots
  (that counts prefill slots with no decode partner and *overstates* capacity
  during exactly the rollout skew this input exists to correct). A reported
  `0` is a real signal — the home can currently serve nothing — and drops the
  home's weight to zero.
- **`observedAt`** is when the *home* computed the figure. It is required:
  without it a frozen reporter is indistinguishable from a fresh one, and the
  `maxAge` staleness guard could never fire.

A report missing either field is rejected and falls open. The `Report` format
accepts no `options` — the report already states servable units, so a
conversion factor would silently scale a number that needs no scaling — and
configuring any is a startup error.

Other formats can be compiled into the manager binary by optional packages.
The manager logs the available set at startup
(`capacityFormats` on the "Setting up multi-cluster TrafficMap routing
controller" line), and naming a format this build does not carry is a
**startup error**, not a silent fallback.

One caution: `method` accepts `GET`, `HEAD`, or `POST` (the list is shared
with the probe), but a `HEAD` response carries no body, so the `Report`
decoder has nothing to read and every poll falls open. Use `GET` or `POST`.

## Smoothing: `samples` and `quorum`

The control plane polls **one address per home** — the external endpoint — so
behind a load balancer, successive polls may sample *different* reporters.
Keeping only the latest reading would be neither min nor max but
any-responder: a lucky high reading would raise a ceiling that an earlier
reading correctly lowered. Hence the window and the quorum:

- **`samples`** is how many recent readings are retained. The window
  *smooths*; it does not protect. A longer window delays recovery, because a
  low reading has to age out (or be pushed out by newer readings) before the
  ceiling can rise again.
- **`quorum`** is how many retained readings must corroborate a lower value
  before it is applied: the ceiling is the quorum-th *smallest* reading, not
  the smallest. This tolerates `quorum − 1` misbehaving reporters — the
  difference between believing a pessimist and believing a bug. One router
  that has lost its watch reports a near-zero figure; a strict minimum would
  take it at face value and hold the home there for the whole window. A
  reporter can still always lower the ceiling; it just cannot do so alone.
- Quorum is also the **lag before a genuine drop is believed**: `quorum`
  polls.
- Until the window holds `quorum` accepted readings — at startup, after a
  failed poll cleared it, or after stale samples expired — no ceiling applies
  and the plan stands (`fallbackReason: capacity report awaiting quorum
  (1/2 accepted samples)`).

The window is in-memory state of the manager, keyed by the home, its
endpoint URL, and a digest of every capacity setting. A manager restart, a
change to any capacity setting, or a changed endpoint starts an **empty**
window: the plan stands until `quorum` fresh readings accumulate again.

## Staleness: the `maxAge` fall-open

`maxAge` is the exclusive upper bound on a report's age, measured from its
`observedAt` stamp. A report at or beyond it is rejected at receipt, and an
accepted sample expires out of the window when its stamp reaches that age. If
expiry drops the window below quorum, the ceiling is released and the plan
stands — so a home whose reporter froze **releases** its ceiling instead of
holding a stale one forever.

Because the guard compares producer time against the control plane's clock,
significant clock skew between them surfaces as either
`capacity report timestamp is in the future` or premature staleness. The
built-in `Report` format stamps its own readings; a hypothetical compiled-in
format that does not is stamped at receipt, which makes `maxAge` inert for it
(a wedged home that still answers holds its ceiling).

## Operator-level configuration

The operator-level poll lives in the `routing.capacity` block of the
`multicluster` key in the `inferenceservice-config` ConfigMap (in the OME
controller namespace, `ome` by default). The block is read **once at manager
startup**; changing it requires a manager restart.

`path` is the enable switch: an empty path means no polling and the plan
always stands. There are **no in-code defaults** — once `path` is set, every
field below is required, and a partial or inconsistent configuration is a
**startup error** (`invalid multi-cluster configuration: ...`), not a silent
fallback.

| Field | Description |
|-------|-------------|
| `path` | Capacity endpoint appended to each home's endpoint. Must begin with `/`. Empty disables polling. |
| `method` | `GET`, `HEAD`, or `POST` — but see the `HEAD` caution above. |
| `format` | Response shape, e.g. `Report`. Must be registered in this build; the startup error lists what is available. |
| `options` | Format-specific settings, opaque to the control plane. The built-in `Report` format rejects any. |
| `period` | How often each home is polled, as a duration string. Must be at least `routing.observer.minPeriod`. |
| `timeout` | Bound on one capacity request. Must be shorter than `period`, otherwise a slow home's polls overlap and the effective rate stops matching the configured period. |
| `samples` | Window length. Must be positive and must not exceed `routing.observer.maxSamples`. |
| `quorum` | Corroboration count. Must be positive and must not exceed `samples` — an unreachable quorum would never lower a ceiling, so the input would silently do nothing. |
| `maxAge` | Staleness bound on a report's `observedAt` stamp. Must be at least `period` (otherwise every report ages out before the next poll and the ceiling flaps every cycle), and when `quorum` > 1, must span at least `quorum` whole periods so a quorum can exist among fresh samples. |

Polling requires routing to be enabled, which in turn requires the
`routing.observer` limits and `routing.publisher.resyncInterval`:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: ome
data:
  multicluster: |
    {
      "routing": {
        "enabled": true,
        "observer": {
          "maxConcurrentReconciles": 2,
          "maxConcurrentRequests": 16,
          "maxResponseBytes": 65536,
          "minPeriod": "10s",
          "maxSamples": 10
        },
        "publisher": {
          "resyncInterval": "5m"
        },
        "capacity": {
          "path": "/v1/capacity",
          "method": "GET",
          "format": "Report",
          "period": "15s",
          "timeout": "5s",
          "samples": 5,
          "quorum": 2,
          "maxAge": "60s"
        }
      }
    }
```

Malformed durations (`"30"`, `"1 m"`) are also startup errors — they would
otherwise be indistinguishable from "not set" and silently discard your
intended value for the life of the process.

The polls are sent by the control-plane `ome-manager` pod, so its egress must
reach each home's external endpoint. Capacity polls share the HTTP client and
the `routing.observer.maxConcurrentRequests` budget with the health probes:
both talk to the same homes with the same credentials.

## Per-service override: `spec.routing.capacity`

A service can replace the operator-level poll with `spec.routing.capacity` on
the **control-plane** InferenceService (the one you authored — the field is
ignored on derived copies and in single-cluster deployments):

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
  namespace: prod
spec:
  routing:
    capacity:
      path: /v1/capacity
      method: GET
      format: Report
      period: 15s
      timeout: 5s
      samples: 5
      quorum: 2
      maxAge: 60s
```

The block is a **complete replacement, not a merge**. You cannot override just
the path and inherit the rest: CRD validation (CEL) and the admission webhook
reject an enabled block that does not specify all eight fields — `path`,
`method`, `format`, `period`, `timeout`, `samples`, `quorum`, and `maxAge`
(`options` stays optional). Admission also enforces the per-field rules:
positive `period` and `timeout`, `timeout` shorter than `period`, `maxAge` at
least `period` and spanning at least `quorum` periods, `quorum` not exceeding
`samples`.

To explicitly turn polling off for one service while the operator-level poll
stays on, set `disabled` — and nothing else, or admission rejects the block:

```yaml
spec:
  routing:
    capacity:
      disabled: true
```

Allocation for that service then comes from the control-plane plan alone.

Three constraints cannot be checked at admission because they depend on the
operator configuration or the manager build, and surface at reconcile time as
manager-log errors while the service's TrafficMap keeps its last state until
the spec is fixed:

- `period` must still be at least `routing.observer.minPeriod`
  (`spec.routing.capacity.period (…) must be at least routing.observer.minPeriod (…)`).
- `samples` must still not exceed `routing.observer.maxSamples`
  (`spec.routing.capacity.samples (…) must not exceed routing.observer.maxSamples (…)`).
- `format` must be registered in the running manager binary
  (`spec.routing.capacity.format "…" is not registered in this build (available: […])`).

Changing any capacity setting changes the policy digest and resets the
per-home sample windows for that service; ceilings reapply once `quorum` fresh
readings accumulate under the new policy.

## Observing capacity state

Each TrafficMap entry records the poll's contribution in its `capacity` block
(see [Traffic Map](/ome/docs/concepts/traffic_map/) for the full field
reference):

```bash
kubectl get trafficmap chat -n prod -o yaml
```

```yaml
spec:
  entries:
  - cluster: worker-a
    endpoint: https://chat.worker-a.example.com
    weight: 2
    healthy: true
    capacity:
      allocated: 2          # plan of 6, lowered by the report
      ready: 6
      source: Endpoint      # the report actually lowered the plan
      reported: 2           # the accepted (quorum-smoothed) figure
```

`source: Endpoint` appears **only** when the report actually lowered the
plan. `reported` is recorded whenever the endpoint answered usably — including
when it was at or above the plan and changed nothing — which is what makes the
ceiling auditable: `allocated` alone cannot show that a report was received
and found non-binding.

When a configured poll produces no usable ceiling, `capacity.fallbackReason`
says why the control-plane number was retained:

| `fallbackReason` | Meaning |
|------------------|---------|
| `no capacity report yet` | The first poll has not completed — including right after a manager restart or a policy change. |
| `capacity report awaiting quorum (n/q accepted samples)` | Polls succeed but the window has not yet accumulated `quorum` readings. |
| `capacity endpoint unreachable: …` | Transport error or timeout; check manager egress and the home's ingress. |
| `capacity endpoint returned HTTP nnn` | Any non-200 status falls open. |
| `capacity response is not a valid report: …`, `… has no servable count`, `… has no observedAt stamp` | The body does not decode under the configured format. |
| `capacity report is negative (…)` | Malformed report; fail open rather than black-holing the home. |
| `capacity report timestamp is in the future` | Producer clock is ahead of the control plane's. |
| `capacity report is stale (at least configured maximum age …)` | `observedAt` is too old — a frozen reporter, or clock skew. |
| `capacity report awaiting quorum after stale samples expired (n/q accepted samples)` | Expiry decayed the window below quorum. |

At the fleet level, the routing controller writes the `CapacityFallback`
condition on the TrafficMap. It has abnormal-true polarity: `True` with reason
`EndpointCapacityUnavailable` means one or more homes fell open (the message
counts them); `False` covers reasons `EndpointCapacityAvailable`,
`CapacityPollingDisabled`, and `NoCapacityTargets`.

The same signal is exported as a Prometheus gauge for alerting:

```text
ome_trafficmap_capacity_fallback_homes{namespace="prod", trafficmap="chat"}
```

It counts the homes currently using the control-plane allocation because
configured endpoint capacity did not produce a usable ceiling — one series per
capacity-enabled TrafficMap, removed when polling is disabled. A sustained
non-zero value means the ceiling you configured is not actually in force for
those homes.
