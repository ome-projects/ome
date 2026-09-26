---
title: Canary Metric Analysis
linkTitle: Canary Metric Analysis
weight: 3
description: >
  Reference for metric-gated canary promotion: where analysis metrics come from, how
  interval, initialDelay, and failureLimit shape the verdict, which template variables a
  query may use, and what Hold, Rollback, and RollbackOnStall do when Prometheus cannot
  be read.
---

A canary step opts into metric-gated promotion by setting
`spec.rollout.groups[].canary.steps[].analysis`. The analysis block carries the **checks**
(metrics) and the **policy** (`interval`, `initialDelay`, `failureLimit`,
`onInconclusive`) for that one step; the **metrics source** is declared once per canary as
`spec.rollout.groups[].canary.prometheus` and shared by every analysis step. This page is
the reference for that evaluation contract. It applies identically whether the canary body
is inline on the InferenceService or supplied by a
[RolloutPolicy](/ome/docs/concepts/rollout_policy/) reference.

```yaml
spec:
  rollout:
    groups:
      - components: [engine]
        canary:
          prometheus:
            serverAddress: http://prometheus.monitoring.svc:9090
          steps:
            - capacity: "25%"
              traffic: 10
              pause:
                duration: 10m        # minimum bake before a passing sample advances
              analysis:
                interval: 60s        # sample at most once per minute
                initialDelay: 2m     # skip cold-start noise after the step starts serving
                failureLimit: 2      # second failing sample in this step rolls back
                onInconclusive: RollbackOnStall
                metrics:
                  - name: error-rate
                    query: 'sum(rate(request_errors_total{service="{{.CanaryService}}"}[2m])) / sum(rate(request_total{service="{{.CanaryService}}"}[2m]))'
                    operator: LTE
                    threshold: "0.05"
            - capacity: "100%"
              traffic: 100
```

For how analysis relates to the other step gates (immediate, timed, manual) and how to
promote or abort at a gate, see
[Promote or Roll Back a Canary](/ome/docs/tasks/promote-or-rollback-a-canary/).

## Where the metrics come from

`canary.prometheus` locates and authenticates to the Prometheus HTTP API. At most one of
`providerRef` and `serverAddress` may be set (admission rejects both). The effective
source resolves in this order:

1. **`providerRef`** — names a logical metric provider bound in the operator's
   [`metricProviders` configuration](/ome/docs/concepts/autoscaler_policy/#metric-providers)
   (the same bindings AutoscalerPolicy triggers use). The binding supplies the server
   address, auth Secret, and default headers; headers declared on the canary are merged
   over the binding's (the canary's key wins), and an inline `authRef` overrides the
   binding's secret. A `providerRef` that is not bound on this cluster parks the rollout
   at run open; a binding that disappears mid-run yields an empty source, so samples read
   inconclusive — the step is never silently un-gated.
2. **`serverAddress`** — a raw base URL (e.g. `http://prometheus.monitoring.svc:9090`),
   used directly. Allowed inline; rejected inside a RolloutPolicy body, which must use
   `providerRef` ([portability restrictions](/ome/docs/concepts/rollout_policy/#what-admission-rejects)).
3. **Neither** — the operator's `canaryAnalysis.defaultProvider` binding when one is
   named, else `canaryAnalysis.bundledPrometheusAddress`. The Helm chart renders the
   bundled address to the [bundled Prometheus](/ome/docs/administration/metrics/) Service
   when the value is left empty.
4. **No source at all** (no per-canary source, no default provider, empty bundled
   address) — the analysis has no source and **every sample reads inconclusive**.

Authentication is a bearer token read from a Secret key in the InferenceService's own
namespace (`authRef`), sent as `Authorization: Bearer <token>`; omit it for an
unauthenticated Prometheus. `headers` are extra plaintext HTTP headers sent on every
query — e.g. `X-Scope-OrgID` for a multi-tenant Cortex/Thanos/Mimir front-end; without
the tenant header such a backend returns no data, which the gate reads as a permanent
stall. TLS/mTLS is not supported.

Admission never checks the source is reachable: an unreachable or missing source surfaces
controller-side as inconclusive samples, not as a rejection.

## When and how often a query runs

Sampling for a step proceeds in three phases:

- **Warm-up (`initialDelay`)** — no sampling until `initialDelay` has elapsed after the
  step starts serving (new pods Ready, traffic applied). New pods need a moment before
  their metrics mean anything; sampling immediately would read cold-start noise. Optional;
  omitted means sampling may begin at once.
- **Throttle (`interval`)** — the controller evaluates the metrics at most once per
  `interval`, and re-checks a held step on the same cadence. The throttle also bounds
  failure accrual: one count per interval, so a burst of unrelated reconciles cannot
  spuriously burn the failure budget. Required, must be `> 0`.
- **Background query** — the actual Prometheus call never runs on the reconcile
  goroutine. A due sample kicks a bounded background query and holds; the completed
  result is consumed on a later reconcile, and each result is consumed exactly once.

The sampler itself is tuned fleet-wide in the operator's `canaryAnalysis` config block
(the `inferenceservice-config` ConfigMap, set via the `ome-resources` chart):

| Key | Default | Meaning |
|-----|---------|---------|
| `queryTimeout` | `5s` | Bounds one sampling pass — all of a step's metric queries together. |
| `maxConcurrency` | `8` | Cap on analysis queries in flight at once, across **all** canaries. Queries queue behind it, so with many concurrent canaries the effective sample interval degrades; scale with fleet size. |
| `cacheTTL` | `2m` | How long an unconsumed sampled result stays usable before eviction. |

## Query template variables

Each metric's `query` is a PromQL expression, templated with the rollout's revision
context before execution:

| Variable | Value |
|----------|-------|
| `{{.Namespace}}` | The InferenceService's namespace. |
| `{{.ISVCName}}` | The InferenceService's name. |
| `{{.Component}}` | The component being canaried (`engine`, `decoder`, `router`). |
| `{{.CanaryService}}` | The canary revision's per-revision Service name (`<isvc>-<component>-rev-<hash>`) — the same Service the executor programs traffic onto, so a query can target exactly the canary's pods. |
| `{{.StableService}}` | The stable revision's per-revision Service name. |
| `{{.CanaryRevision}}` | The canary revision hash (matches the `revision_hash` label the [bundled Prometheus](/ome/docs/administration/metrics/) attaches to scraped pods). |
| `{{.StableRevision}}` | The stable revision hash. |

A reference to a variable that does not exist is an execution error, not a silent empty
string: a typo'd `{{.CnaaryService}}` fails loudly as an inconclusive sample (with the
template error in status) instead of querying a malformed selector that would read as "no
data" forever.

## How one sample is judged

A **sample** is one evaluation of all the step's metrics. Per metric, the rendered query
runs as an instant query at the current time:

- A **scalar** result yields one value; a **vector** yields one value per series. NaN and
  ±Inf values are skipped: a `0/0` error-rate ratio before any traffic means "no signal,"
  and a divide-by-zero `+Inf` must not be read as "infinitely bad."
- A result with no usable value — empty, or all NaN/Inf — reads as **no data**
  (inconclusive for that metric), never as a breach.
- A multi-series result is reduced to the **worst series** for the operator before
  comparison: the maximum for `LT`/`LTE` (the pod with the highest error rate), the
  minimum for `GT`/`GTE` (the pod with the lowest success rate). The metric passes only
  when every series passes.
- The metric passes when `result <operator> threshold` holds, with `threshold` parsed as
  a float.

The per-metric results combine with **AND** semantics and `Fail > Inconclusive > Pass`
precedence into one verdict:

| Verdict | Condition |
|---------|-----------|
| **Pass** | Every metric was evaluated and satisfied its condition. |
| **Fail** | At least one metric was evaluated and breached its condition — a known breach is definitive, even if another metric could not be read. |
| **Inconclusive** | Nothing breached, but at least one metric could not be evaluated: Prometheus unreachable, query or template error, no data, or no source configured. A source wiring failure before any query (e.g. an unreadable `authRef` Secret) also reads inconclusive. |

## What each verdict does

- **Pass** advances the step once its bake window has elapsed — `pause.duration`,
  measured from step entry, is the minimum bake before a passing sample may advance. A
  step with no `pause.duration` advances on the first passing sample.
- **Fail** increments the per-step failure count (`analysisFailedChecks` in status).
  Reaching `failureLimit` triggers **automatic rollback** to the stable revision;
  `failureLimit: 1` rolls back on the first bad sample. The count resets on every step
  advance, so each traffic level gets its own budget. Failing samples below the limit
  hold the step and keep sampling.
- **Inconclusive** holds, rolls back, or escalates per `onInconclusive` (next section).
  Except under `Rollback`, inconclusive samples never count toward the failure budget.

A matching `ome.io/rollout-promote` annotation force-advances the step mid-bake,
overriding the gate — the CLI spelling is
[`kubectl ome rollout promote --override-analysis --yes`](/ome/docs/tasks/promote-or-rollback-a-canary/#override-an-analysis-gate).

## onInconclusive: when Prometheus cannot be read

`onInconclusive` selects what happens when samples are inconclusive — distinct from a
metric breach. Two of the three behaviors key off the **stall timeout**, measured from the
last *conclusive* sample (pass or fail — i.e. Prometheus answered), or from step entry if
there has been none:

| Value | Behavior |
|-------|----------|
| `Hold` (default) | Keep traffic at the current step and keep sampling; a transient gap costs nothing. Once the stall timeout expires, escalate to the `Failed` phase for an operator decision — traffic still does not move, stable keeps serving. A monitoring outage is not evidence the canary is bad. |
| `Rollback` | Fail-safe: treat "can't tell" as "assume bad" and roll back to stable on the **first** inconclusive sample, without waiting out the stall timeout. A scrape blip (or an unreadable auth Secret) reverts the canary. |
| `RollbackOnStall` | Ride out transient gaps like `Hold`, but once the stall timeout expires, roll back instead of parking — a gate that never recovers does not leave the fleet split. |

The stall timeout is the canary's **ready timeout**, resolved in order: the
`ome.io/rollout-ready-timeout` annotation, else `canary.readyTimeout`, else the
operator-configured `rollout.defaultReadyTimeout` (the `ome-resources` chart sets `15m`).
If none of the three is configured, the stall timeout does not exist: `Hold` holds
indefinitely and `RollbackOnStall` never fires — only `Rollback` acts without it.

A rollout parked `Failed` by a stall is resolved by the operator: abort it with
[`kubectl ome rollout rollback`](/ome/docs/tasks/promote-or-rollback-a-canary/#roll-back-the-canary)
(accepted in the `Failed` phase), or fix the metrics source — an unbound provider,
unreachable address, or missing tenant header shows up in the per-metric `message` in
status.

## Observing analysis in status

The executor records analysis state on the component's canary status
(`status.components.<component>.canary`), all maintained only while a step gates on
analysis:

| Field | Meaning |
|-------|---------|
| `analysisFailedChecks` | Failing samples in the **current** step; reset to zero on each advance. Rollback fires when it reaches `failureLimit`. |
| `lastEvaluationTime` | When metrics were last sampled; the interval throttle measures from here. |
| `lastConclusiveEvaluationTime` | When the last conclusive sample (pass or fail) was recorded; the stall timeout measures from here. |
| `metricResults[]` | The most recent per-metric evaluation: `name`, `value` (empty when the metric could not be read), `threshold`, `operator`, `passed`, `message` (the reason when not passed — "no data", a query or template error), and `time`. |

`metricResults` is why a step held or rolled back, without leaving `kubectl`:

```bash
kubectl get isvc my-isvc -o jsonpath='{.status.components.engine.canary.metricResults}'
```

## What admission validates

Analysis blocks are validated at InferenceService admission (and again for the composed
plan at run open; RolloutPolicy bodies face the same rules plus
[policy-only restrictions](/ome/docs/concepts/rollout_policy/#what-admission-rejects)):

- `interval` must be `> 0`; `failureLimit` must be `>= 1`.
- At least one metric (at most 10), each with a unique non-empty `name`, a non-empty
  `query`, a numeric `threshold`, and an `operator` of `LT`/`LTE`/`GT`/`GTE`.
- At most one of `prometheus.providerRef` and `prometheus.serverAddress`.

Source reachability, query correctness against the live Prometheus, and template
rendering are **not** checked at admission — they surface at runtime as inconclusive
samples, handled per `onInconclusive`.

## Related pages

- [Promote or Roll Back a Canary](/ome/docs/tasks/promote-or-rollback-a-canary/) — the
  operator actions at a gate, including the analysis override.
- [Rollout Policy](/ome/docs/concepts/rollout_policy/) — reusable canary progressions and
  the `providerRef`-only restriction on policy bodies.
- [Metrics and the bundled Prometheus](/ome/docs/administration/metrics/) — what the
  bundled instance scrapes, the `revision_hash` label, and rebinding
  `canaryAnalysis.bundledPrometheusAddress` to your own Prometheus.
