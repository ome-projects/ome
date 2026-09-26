---
title: "Routing Health Probes"
linkTitle: "Routing Health Probes"
weight: 12
date: 2026-09-26
description: >
  Configure the active end-to-end health probe that gates a workload cluster's TrafficMap weight to zero, at the operator level or per InferenceService.
---

When multi-cluster TrafficMap routing is enabled, the control plane can run an
**active end-to-end health probe**: it periodically sends an HTTP request to
each serving home's externally addressable endpoint — the same URL a client
uses — and gates a home's route weight to **zero** after enough consecutive
failures. Because the probe travels the whole serving path, it catches faults
that pod readiness inside the home cannot: broken ingress, DNS, certificates,
or routes.

TrafficMap routing is part of OME's multi-cluster support, which is still
under active development. The `spec.routing` API is **alpha** and may change
without notice; do not build production automation on it yet.

The probe is one of several **independent** inputs to a TrafficMap weight:

- **The enable gate.** Routing itself is off by default (`routing.enabled` in
  the operator configuration); a service may opt out with
  `spec.routing.enabled: false`, but cannot opt in where the installation gate
  is off.
- **Readiness and capacity.** Without a probe, the health gate is simply
  `readyReplicas > 0`. A weight is proportional to
  `min(allocated, ready) × factor`, optionally lowered by the separate
  endpoint-capacity poll (`routing.capacity`). Probe and capacity are
  explicitly independent: enabling one never enables the other.
- **Manual overrides.** [Traffic-drain
  overrides](/ome/docs/tasks/drain-traffic-from-a-workload-cluster/) force
  arms to zero after all automatic inputs.

The probe adds a **reachability gate** on top: a home that is ready and has
capacity still drops to weight zero when its endpoint conclusively fails
probing, and its former traffic share is renormalized across the passing
homes.

## How a probe attempt is judged

Each attempt sends the configured `method` to the home's endpoint plus `path`
and classifies the outcome:

- **Passing** — the response status is in `acceptStatuses`.
- **Failing** — the response status is in `gateStatuses`, or the request had a
  transport error, or it timed out.
- **Unknown (inconclusive)** — the response status is in **neither** list, or
  the probe could not be constructed at all. An inconclusive attempt moves
  nothing: it does not advance the failure count, reset the pass count, or
  change the current gate.

Redirects are not followed; the 3xx status itself is classified against the
two lists. The response body is read (up to `routing.observer.maxResponseBytes`)
and discarded — only the status code and transport outcome matter.

Three statuses are refused in `gateStatuses` outright, because the interesting
codes are the ones that belong to neither list:

- **429** means the home is alive and overloaded. Gating it would remove
  capacity exactly when the fleet is hot.
- **401 / 403** mean the *prober's own* credentials are wrong. Failing the
  home would hide a control-plane defect and take traffic down with it.

`Unknown` never gates for a structural reason: the conditions that produce it
(the probe has not run yet, or the prober itself is broken) are uniform across
homes, so treating them as failure would zero the whole fleet at once.

## Thresholds and hysteresis

A single verdict never moves traffic:

- A home is **gated to weight zero** only after `failureThreshold`
  *consecutive* Failing verdicts. A single dropped packet must not move a
  large traffic share, and the reprogramming churn from flapping would itself
  be the outage.
- A gated home is **restored** only after `successThreshold` consecutive
  Passing verdicts.
- An Unknown attempt leaves both counters and the gate untouched — a gated
  home stays gated through Unknown results until enough passes accumulate.

The per-home gate, failure count, last result, and timestamp are persisted on
the TrafficMap (`spec.entries[].probe`), so hysteresis state survives a
manager restart — but only while the entry identity and the
`probe.policyDigest` still match the effective policy. Changing any probe
setting changes the digest and resets probe state for that service.

## Operator-level configuration

The operator-level probe lives in the `routing.probe` block of the
`multicluster` key in the `inferenceservice-config` ConfigMap (in the OME
controller namespace, `ome` by default). The block is read **once at manager
startup**; changing it requires a manager restart.

`path` is the enable switch, not just an unset default: an empty path means no
probing, and there are **no in-code defaults** for any field. A guessed probe
path baked into the binary would look like it works — a shallow probe against
a wrong-but-live path returns 200 forever while catching nothing — so once
`path` is set, *every* field below is required and a partial configuration is
a **startup error**, not a silent fallback.

| Field | Description |
|-------|-------------|
| `path` | Request path appended to each home's endpoint. Must begin with `/`. Empty disables probing. |
| `method` | `GET`, `HEAD`, or `POST`. Required explicitly because probe cost is a real choice: GET and POST against an inference path are very different loads. |
| `acceptStatuses` | Status codes counted as a pass. Required — "not 200 means down" is too blunt a policy to infer. |
| `gateStatuses` | Status codes counted as a failure. Must not contain `401`, `403`, or `429`, and must not overlap `acceptStatuses`. |
| `period` | How often each home is probed, as a duration string. Must be at least `routing.observer.minPeriod`. |
| `timeout` | Bound on one probe request. Must be shorter than `period`, otherwise a slow home's probes overlap and the effective rate stops matching the configured period. A timeout counts as a failure. |
| `failureThreshold` | Consecutive failures before a home is gated to weight zero. Must be positive. |
| `successThreshold` | Consecutive passes before a gated home is restored. Must be positive. |
| `allFailedPolicy` | `PreserveTraffic` or `Drain` — what happens when *every* home has crossed the failure threshold. See below. |

Probing requires routing to be enabled, which in turn requires the
`routing.observer` limits and `routing.publisher.resyncInterval` — they bound
the shared prober/poller process resources and are required explicitly
whenever routing is on:

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
        "probe": {
          "path": "/health",
          "method": "GET",
          "acceptStatuses": [200],
          "gateStatuses": [404, 500, 502, 503, 504],
          "period": "30s",
          "timeout": "5s",
          "failureThreshold": 3,
          "successThreshold": 2,
          "allFailedPolicy": "PreserveTraffic"
        }
      }
    }
```

The manager validates this block before any controller is wired and **refuses
to start** on an unusable value (`invalid multi-cluster configuration: ...`).
That includes malformed durations: a typo like `"30"` or `"1 m"` would
otherwise be indistinguishable from "not set" and silently discard your
intended value for the life of the process.

**Choose the path deliberately.** Depth is the whole decision: a liveness path
proves a process is up, while an inference path proves the home can actually
serve — and only the second catches the failure this feature exists for. The
trade is cost, because a synthetic inference request occupies real serving
capacity. That choice is yours, per install.

The probes are sent by the control-plane `ome-manager` pod, so its egress must
reach each home's external endpoint. In-flight probe and capacity requests
share the `routing.observer.maxConcurrentRequests` budget.

## Per-service override: `spec.routing.probe`

A service can replace the operator-level probe with `spec.routing.probe` on
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
    probe:
      path: /v1/models
      method: GET
      acceptStatuses: [200]
      gateStatuses: [404, 500, 502, 503, 504]
      period: 30s
      timeout: 5s
      failureThreshold: 3
      successThreshold: 2
      allFailedPolicy: Drain
```

The block is a **complete replacement, not a merge**. You cannot override just
the path and inherit the rest: CRD validation (CEL) and the admission webhook
reject an enabled probe that does not specify all nine fields —
`path`, `method`, `acceptStatuses`, `gateStatuses`, `period`, `timeout`,
`failureThreshold`, `successThreshold`, and `allFailedPolicy`. The same
per-field rules apply as at the operator level: positive `period` and
`timeout`, `timeout` shorter than `period`, no `401`/`403`/`429` in
`gateStatuses`, no overlap between the two lists.

To explicitly turn probing off for one service while the operator-level probe
stays on, set `disabled` — and nothing else, or admission rejects the block:

```yaml
spec:
  routing:
    probe:
      disabled: true
```

The health gate for that service then falls back to `readyReplicas > 0`.

One constraint cannot be checked at admission because it depends on operator
configuration: the override's `period` must still be at least
`routing.observer.minPeriod`. A violation surfaces at reconcile time as a
manager-log error
(`spec.routing.probe.period (…) must be at least routing.observer.minPeriod (…)`)
and the service's TrafficMap keeps its last state until the spec is fixed.

## `allFailedPolicy`: PreserveTraffic vs Drain

The policy only matters in one situation: **every** home has independently
crossed its failure threshold with a *conclusive* Failing verdict. While any
home is passing — or has no conclusive verdict yet — the policy has no effect,
and Unknown never authorizes it.

- **`PreserveTraffic`** ignores *only the probe gates* and recomputes the
  weights from the remaining inputs. Admitted, ready, and reported capacity
  still apply: an unready, unadmitted, or zero-reported-capacity home stays at
  zero. The rationale: probes are measured from the control plane's single
  vantage point, so a fleet-wide conclusive failure can be the manager's own
  egress path rather than every home at once — and keeping traffic flowing to
  homes with ready capacity beats black-holing all of it on that one signal.
- **`Drain`** preserves the all-zero table, so the publisher removes every
  route arm. Choose this when serving into a genuinely dead fleet is worse
  than serving nothing — for example when a failed probe means responses would
  be wrong, not just slow.

The TrafficMap's `Routable` condition reports which happened: reason
`AllHomesProbeFailed` with status `True` means the `PreserveTraffic` fallback
is active ("preserving traffic among homes with ready capacity"); with status
`False` it means all route weights are zero.

## Observing probe state

Each TrafficMap entry records the probe's contribution separately from
readiness, because those are different faults with different owners —
readiness belongs to the workload, reachability to the path:

```bash
kubectl get trafficmap chat -n prod -o yaml
```

```yaml
spec:
  entries:
  - cluster: worker-a
    endpoint: https://chat.worker-a.example.com
    weight: 0
    healthy: false
    probe:
      policyDigest: sha256:…
      result: Failing          # Passing | Failing | Unknown
      gated: true              # the current hysteresis gate
      consecutiveFailures: 3
      lastProbeTime: "2026-09-26T10:00:00Z"
      message: "HTTP 503"
```

`result` is the most recent attempt's verdict; `gated` is the standing gate
(it can be `true` while `result` is already `Passing`, until
`successThreshold` passes accumulate). `message` explains the verdict — the
observed status code, the transport error, or `HTTP <code> (not in accept or
gate list)` for an inconclusive status. `lastProbeTime` makes a stalled prober
visible rather than reading as a steady pass. An entry's `healthy` is
readiness **and** probe reachability combined; the `probe` block is present
only when probing is configured.

The `kubectl get tm` printer columns include `Routable` and its reason, so an
all-homes probe failure is visible at a glance.
