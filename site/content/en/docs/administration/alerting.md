---
title: "Alerting"
linkTitle: "Alerting"
weight: 20
description: >
  Enable the chart's P0 alert pack — five page-immediately PrometheusRule alerts — and tune their thresholds and selectors.
---

The `ome-resources` chart ships a P0 alert pack: five alert rules covering the
failures where OME as a whole has stopped working — the tier that should page
immediately. Every rule alerts on a symptom a user can feel (nothing is
reconciling, no InferenceService write is admitted, a service has no healthy
backend, rollouts are failing en masse, the controller is wedged). Causes and
single-workload problems belong in lower, non-paging tiers that you build
yourself; the pack deliberately does not include them.

The pack renders as a single `monitoring.coreos.com/v1` `PrometheusRule` named
`ome-p0-alerts` in the release namespace, containing one rule group, `ome.p0`.
It is **disabled by default** (`prometheusRule.enabled: false`): an alert rule
that fires into a routing tree nobody has configured is worse than no rule at
all. Enable it once you have an Alertmanager route for `severity: critical`
pages.

## Prerequisites

The pack is a prometheus-operator object, and enabling it does nothing by
itself. Three things must already be true in the cluster:

1. **The `monitoring.coreos.com/v1` CRDs are installed** — that is,
   [prometheus-operator](https://github.com/prometheus-operator/prometheus-operator)
   or the `kube-prometheus-stack` chart from the
   [installation guide](/ome/docs/installation/). Installing `ome-resources`
   with `prometheusRule.enabled: true` on a cluster without the CRD fails at
   install time with an unknown-kind error.
2. **An operator-managed Prometheus selects the rule.** The Prometheus custom
   resource's `ruleSelector` must match the `PrometheusRule`'s labels (use
   `prometheusRule.additionalLabels` to add whatever it matches on), and its
   `ruleNamespaceSelector` must include the OME release namespace.
3. **That same Prometheus scrapes OME's metrics.** Rules only evaluate series
   their Prometheus has. Controller-manager metrics are served on port `8080`
   at `/metrics`; the chart can create a ServiceMonitor for them
   (`serviceMonitor.enabled: true`), or vanilla `prometheus.io/scrape`
   annotation discovery keeps working. The `router_pool_healthy_count` /
   `router_pool_total_count` series come from OME's router pods and must be
   scraped as well, or `OMEServiceUnavailable` can never fire.

**The Prometheus bundled by this chart does not read alert rules.** The
`prometheus.enabled: true` instance the chart deploys exists for KEDA
autoscaling input and canary metric analysis: it is a plain Deployment reading
its scrape config from a ConfigMap, and it does **not** consume
`PrometheusRule` objects. Enabling the pack in a cluster whose only Prometheus
is the bundled one silently does nothing — the object is created, nothing
evaluates it, and no error surfaces anywhere. Point the pack at the
operator-managed Prometheus that backs your alerting instead.

## Enabling the pack

With a `kube-prometheus-stack` install, whose default `ruleSelector` matches
rules labeled with the stack's Helm release name:

```yaml
# alerting-values.yaml
prometheusRule:
  enabled: true
  # Merged onto the PrometheusRule so the operator's ruleSelector matches it.
  additionalLabels:
    release: kube-prometheus-stack
  # Each alert links to "<base>#<alert name>" in its runbook_url annotation.
  runbookUrlBase: "https://wiki.example.com/runbooks/ome"

# Scrape the controller-manager with the same Prometheus.
serviceMonitor:
  enabled: true
  additionalLabels:
    release: kube-prometheus-stack
```

```bash
helm upgrade --install ome charts/ome-resources -f alerting-values.yaml
```

Then verify the chain end to end:

```bash
# 1. The object exists:
kubectl get prometheusrule ome-p0-alerts -n ome

# 2. Prometheus picked it up: the "ome.p0" group appears under
#    Status -> Rules in the Prometheus UI.
```

If step 1 succeeds but the group never appears in step 2, the Prometheus is
not selecting the rule: its `ruleSelector` does not match the labels on
`ome-p0-alerts`, or its `ruleNamespaceSelector` does not cover the OME
namespace. Nothing in the cluster reports this mismatch — checking the
Prometheus UI (or the prometheus-operator logs) is the only verification.

## The five alerts

Every rule carries three routing labels: `severity` (from
`prometheusRule.severity`, default `critical`), `tier: p0`, and a `component`
label identifying the failing part. All `for:` durations and thresholds below
are the values defaults and are tunable per rule; the rate lookback windows
(5m, 15m, 10m) are fixed in the template. Each rule can be disabled
individually via `prometheusRule.rules.<rule>.enabled`.

| Alert | Component | Fires after | Condition |
|-------|-----------|-------------|-----------|
| `OMEControllerDown` | `control-plane` | 5m | No controller replica is both up and holding the leader lease |
| `OMEWebhookFailing` | `webhook` | 5m | More than 10% of admission requests return 5xx |
| `OMEServiceUnavailable` | `router` | 5m | An InferenceService has registered backends but no router sees a healthy one |
| `OMEMassRolloutFailure` | `coordination` | 15m | Over half of terminal rollout transitions land in `Failed`, across 3+ InferenceServices |
| `OMEReconcileStalled` | `control-plane` | 10m | The work queue is growing while the reconcile rate is effectively zero |

### OMEControllerDown

Nothing in the cluster is being reconciled: no InferenceService will converge,
scale, or roll out until this recovers. One alert deliberately covers three
distinct failure shapes, because the response is the same and the on-call does
not benefit from three pages for one outage:

- every controller pod is gone (`absent(up{...})` — needed because when all
  pods disappear, `up` stops being reported and a plain `up == 0` matches
  nothing, so the most severe case would otherwise never page),
- every replica is failing its scrape (`max(up) == 0`), or
- replicas are up but none holds the leader lease
  (`max(leader_election_master_status) == 0`, published by the
  controller-manager's leader-election integration; the chart runs the manager
  with `--leader-elect` by default).

Tunables: `rules.controllerDown.enabled`, `duration` (default `5m`).

### OMEWebhookFailing

More than `ratioThreshold` of admission webhook requests are failing with
5xx over a 5-minute window. OME serves its webhooks from the
controller-manager, so this blocks every InferenceService create and update in
the cluster — including the ones that would fix the problem.

The rule counts server errors only, from
`controller_runtime_webhook_requests_total`. An admission *rejection* is an
HTTP 200 carrying `allowed=false`, so a spike in rejected specs does not fire
this alert — that is the webhook doing its job.

Tunables: `rules.webhookFailing.enabled`, `duration` (default `5m`),
`ratioThreshold` (default `0.1`).

### OMEServiceUnavailable

Per `(namespace, inferenceservice)`: backends are registered
(`router_pool_total_count > 0`) but no router considers any of them healthy
(`max` of `router_pool_healthy_count` across routers is `0`), so requests to
that InferenceService are failing. The description on the alert says it
first: check the engine pods before the router — an ejected backend is
usually an engine that stopped passing its health check.

Two deliberate choices in the expression:

- `max()` across router pods, not `min()`: the service is only down when *no*
  router can see a healthy backend. One router with an empty pool while its
  peers are serving is a router problem, not an outage, and belongs in a
  lower tier.
- `router_pool_total_count > 0` stands in for "desired replicas > 0", because
  OME exports no desired-replica series today. The rule therefore does not
  fire for a workload intentionally scaled to zero.

Tunables: `rules.serviceUnavailable.enabled`, `duration` (default `5m`).

### OMEMassRolloutFailure

Rollouts are failing broadly enough to implicate the control plane or a shared
dependency rather than any one workload. The rule is built on
`ome_omenative_rollout_group_transition_total`, the phase-transition counter
emitted by the OMENative rollout coordination layer, and requires **both** a
rate and a spread:

- more than `ratioThreshold` of coordination groups reaching a terminal phase
  (`Idle`, `Staged`, or `Failed`) over 15 minutes are landing in `Failed`, and
- at least `minInferenceServices` distinct InferenceServices saw a failed
  transition in that window.

A ratio alone would fire when one busy workload fails repeatedly; the
InferenceService count is what makes this "the control plane broke rollouts"
rather than "a workload is broken", which is a lower tier. The rule
deliberately does not use `ome_omenative_rollout_group_failure_total`: that
counter increments on every reconcile while a group sits in `Failed`, so a
single stuck workload would drive any ratio built on it without bound. The
transition counter only moves on an actual phase change.

Tunables: `rules.massRolloutFailure.enabled`, `duration` (default `15m`),
`ratioThreshold` (default `0.5`), `minInferenceServices` (default `3`).

### OMEReconcileStalled

Wedged, not down: the controller-manager is being scraped and holds the
lease, but its work queue is growing while the reconcile rate is effectively
zero. Every liveness signal looks healthy, which is exactly why this needs its
own alert. Check for a blocked apiserver call or a deadlocked reconcile before
restarting.

All three clauses of the expression are needed: queue depth alone
(`workqueue_depth` sum above `minQueueDepth`) is just a busy controller; a
near-zero reconcile rate alone (`controller_runtime_reconcile_total` rate at
or below `maxReconcileRate`) is just an idle one; only a backlog that is
*growing* (a positive `deriv()` of queue depth over 10 minutes) while nothing
drains it is a stall.

Tunables: `rules.reconcileStalled.enabled`, `duration` (default `10m`),
`minQueueDepth` (default `1` — depth below this is normal churn),
`maxReconcileRate` (default `0.01` reconciles/sec — at or below this counts
as no progress).

## Pack-wide settings

| Value | Default | Purpose |
|-------|---------|---------|
| `prometheusRule.enabled` | `false` | Render the `PrometheusRule` at all. |
| `prometheusRule.additionalLabels` | `{}` | Extra labels merged onto the object, to match the operator's `ruleSelector`. |
| `prometheusRule.interval` | `30s` | How often Prometheus evaluates the `ome.p0` group. |
| `prometheusRule.severity` | `critical` | Routing label stamped on every rule. |
| `prometheusRule.runbookUrlBase` | `""` | Base URL for `runbook_url` annotations. |
| `prometheusRule.selectors.controlPlane` | `pod=~"ome-controller-manager.*"` | Label matchers selecting control-plane series. |
| `prometheusRule.selectors.router` | `""` | Label matchers selecting router series; empty matches all routers. |

### Series selectors

The two selectors are PromQL label matchers **without** the enclosing braces,
substituted verbatim into every expression. Control-plane series are selected
by pod name rather than by `job`, because the `job` label depends on which
scrape path is in use (ServiceMonitor vs the `prometheus.io/scrape`
annotation) and so is not portable across installs. Override the selectors if
your metrics store relabels differently, or to scope the alerts to one
cluster in a shared store:

```yaml
prometheusRule:
  selectors:
    controlPlane: pod=~"ome-controller-manager.*",cluster="prod-us-1"
    router: cluster="prod-us-1"
```

The router series come from the router image, which is built outside this
repository; the default empty selector matches every router series the store
knows about.

### Runbook links

Each alert's `runbook_url` annotation is `runbookUrlBase` plus its own anchor
(`#omecontrollerdown`, `#omewebhookfailing`, `#omeserviceunavailable`,
`#omemassrolloutfailure`, `#omereconcilestalled`), so a reader lands on the
section for the alert that woke them. Point it at a single runbook page with
one section per alert. Left empty, the annotation is just the bare anchor:
every alert still names its section, it just is not clickable.

## Default values

```yaml
prometheusRule:
  enabled: false
  additionalLabels: {}
  interval: 30s
  severity: critical
  runbookUrlBase: ""
  selectors:
    controlPlane: pod=~"ome-controller-manager.*"
    router: ""
  rules:
    controllerDown:
      enabled: true
      duration: 5m
    webhookFailing:
      enabled: true
      duration: 5m
      ratioThreshold: 0.1
    serviceUnavailable:
      enabled: true
      duration: 5m
    massRolloutFailure:
      enabled: true
      duration: 15m
      ratioThreshold: 0.5
      minInferenceServices: 3
    reconcileStalled:
      enabled: true
      duration: 10m
      minQueueDepth: 1
      maxReconcileRate: 0.01
```
