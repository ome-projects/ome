---
title: Canary metric analysis
description: "Metric-gated canary steps sample Prometheus each interval, roll back after failureLimit failing samples, and hold or roll back when metrics can't be read."
since: v1.3
---

Add `analysis` to a canary step, and the step moves on when Prometheus says the new revision is healthy. Every `interval`, OME runs the step's PromQL queries and compares each result with a threshold. The step advances on a sample in which every metric passes, and the canary rolls back when `failureLimit` samples fail. A sample that can't be judged is inconclusive, and `onInconclusive` decides what it does. [Canary progression](canary-progression.md) covers the steps and their other gates.

Canary works only on components that use the OMENative deployment mode: [Deployment modes and OMENative](../../concepts/architecture/deployment-modes.md) shows how to opt in. A [RolloutPolicy](../../concepts/rollouts-and-traffic/rollout-policy.md), which is alpha, carries the same fields at `spec.canary`.

## Example

This canary moves a quarter of the engine's instances to the new revision and checks their error rate before it moves the rest:

```yaml title="chat.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: chat
  namespace: prod
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 4
    maxReplicas: 4
  rollout:
    groups:
      - components:
          - engine
        canary:
          steps:
            - capacity: "25%"
              traffic: 10
              pause:
                duration: 10m
              analysis:
                interval: 1m
                initialDelay: 2m
                failureLimit: 3
                metrics:
                  - name: error-rate
                    operator: LT
                    threshold: "0.01"
                    query: |
                      sum(rate(request_errors_total{namespace="{{.Namespace}}", inferenceservice="{{.ISVCName}}", revision_hash="{{.CanaryRevision}}"}[5m]))
                      / sum(rate(requests_total{namespace="{{.Namespace}}", inferenceservice="{{.ISVCName}}", revision_hash="{{.CanaryRevision}}"}[5m]))
            - capacity: "100%"
              traffic: 100
```

- Step 1 samples about once a minute, from 2 minutes after its new instances are ready.
- It advances on the first passing sample after its 10-minute pause, and rolls the canary back at the third failing sample.
- `request_errors_total` and `requests_total` stand for your runtime's error and request counters.
- Step 2 has no gate, so the canary completes once the new revision is ready on all the engine's instances.

!!! warning "The traffic weight doesn't route requests"
    OME records the step's `traffic` weight in status but doesn't route by it. The new revision gets requests in proportion to its share of ready instances, which `capacity` sets: about a quarter at step 1, not 10%. See [Traffic split](canary-progression.md#traffic-split).

## Fields

Each step sets its own checks, at `spec.rollout.groups[].canary.steps[].analysis`:

| Field | Required | What it sets |
| --- | --- | --- |
| `interval` | Yes | Time between samples, such as `1m`. |
| `initialDelay` | No | A warm-up before the first sample, from when the step's new instances are ready. |
| `failureLimit` | Yes | How many failing samples at this step roll the canary back. At least 1. |
| `onInconclusive` | No | What an inconclusive sample does: `Hold` (the default), `Rollback` or `RollbackOnStall`. |
| `metrics` | Yes | 1 to 10 metrics, with different names. |

Each metric has these fields, all required:

| Field | What it sets |
| --- | --- |
| `name` | The metric's name in status and in controller metrics. |
| `query` | A PromQL query, which can use the [template variables](#query-template-variables). |
| `operator` | `LT` (`<`), `LTE` (`<=`), `GT` (`>`) or `GTE` (`>=`): how the value must compare with `threshold` to pass. |
| `threshold` | A number, as a quoted string such as `"0.01"`. |

A `pause.duration` on the step sets a minimum bake: the step advances on the first passing sample after it. An analysis step ignores a `pause` without a `duration`.

The canary sets one source for all its analysis steps, at `spec.rollout.groups[].canary.prometheus`. Without one, it queries the [default source](#where-the-metrics-come-from), normally the chart's bundled Prometheus.

| Field | What it sets |
| --- | --- |
| `serverAddress` | The base URL of a Prometheus HTTP API, such as `http://prometheus.monitoring.svc:9090`. |
| `providerRef.name` | A [metric provider binding](#metric-provider-bindings), instead of `serverAddress`. |
| `authRef` | A Secret in the InferenceService's namespace, by `name` and `key`, that holds a bearer token. It replaces a binding's token. |
| `headers` | Plain-text HTTP headers for every query, such as `X-Scope-OrgID`. They override a binding's headers key by key. |

This canary queries its own Prometheus, with a token and a tenant header:

```yaml
spec:
  rollout:
    groups:
      - components:
          - engine
        canary:
          prometheus:
            serverAddress: "http://prometheus.monitoring.svc:9090"
            authRef:
              name: prometheus-bearer
              key: token
            headers:
              X-Scope-OrgID: tenant-a
```

There are no fields for a CA bundle or a client certificate, so mutual TLS isn't supported.

A running canary keeps the `analysis` and `prometheus` blocks it started with, but reads the token Secret and the binding at every sample. An edit to the blocks applies from the next rollout, or when you repin the canary with the alpha [`kubectl ome rollout repin`](../kubectl-ome/rollout.md#repin).

## Query template variables

A `query` is a Go template with these variables:

| Variable | Value | Bundled Prometheus label |
| --- | --- | --- |
| `{{.Namespace}}` | The InferenceService's namespace. | `namespace` |
| `{{.ISVCName}}` | The InferenceService's name. | `inferenceservice` |
| `{{.Component}}` | The canary's component: the group's `router`, or else its `engine`. | `component` |
| `{{.CanaryRevision}}` | The new revision's hash. | `revision_hash` |
| `{{.StableRevision}}` | The stable revision's hash. | `revision_hash` |
| `{{.CanaryService}}` | The new revision's Service, `<isvc>-<component>-rev-<hash>`. | None |
| `{{.StableService}}` | The stable revision's Service, named the same way. | None |

The bundled Prometheus puts these labels, and `pod`, on the series of [InferenceService pods](../../guides/operate-ome/metrics.md#inferenceservice-pods). Another Prometheus needs a scrape configuration that adds them.

Before you gate a step on a query, run it in Prometheus with real label values, and check that it returns a number. [Check the targets](../../guides/operate-ome/metrics.md#step-1-check-the-targets) shows how to reach the bundled Prometheus.

## When queries run

An analysis starts once the step's new instances are ready. It waits `initialDelay`, then samples every `interval`. If the step returns to `Pending` because it lost instances, the warm-up and the bake start over.

Each query is an instant query at the time of the sample, so `[5m]` covers the 5 minutes before it. Until the new pods have served requests in that range, a query can return no data, and `initialDelay` gives them that time. The bundled Prometheus can drop data older than 30 minutes by default (`prometheus.retention`), so keep ranges within 30 minutes.

## How a sample is judged

For each metric, OME compares the worst number in the query's result with `threshold`: the highest for `LT` and `LTE`, and the lowest for `GT` and `GTE`. So a query that returns one series per pod passes only when every pod passes. A query that returns an empty vector, or a ratio over zero requests, makes the metric inconclusive with the message `no data`. It's also inconclusive when its query fails or returns a range vector.

| Verdict | When | What happens |
| --- | --- | --- |
| Pass | Every metric passed. | The step advances, unless its `pause.duration` bake is still running. |
| Fail | At least one metric breached its threshold. | `analysisFailedChecks` goes up by one, and the canary rolls back when it reaches `failureLimit`. A pass doesn't reset the count, and each step starts at 0. |
| Inconclusive | No metric breached, and at least one was inconclusive. | Depends on [`onInconclusive`](#oninconclusive). It never counts toward `failureLimit`. |

At the last step, the canary completes on the first passing sample after [`scaleDownDelaySeconds`](canary-progression.md#fields), when set.

The alpha `kubectl ome rollout promote --override-analysis --yes` passes an analysis gate during its warm-up, sampling or bake: see [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#step-3-override-an-analysis-gate).

An analysis rollback works like a [manual rollback](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#roll-back-the-canary): every instance returns to the stable revision, and the component stays there until a different target revision appears. OME records the rejected revision in `rolledBackRevisionHash`, and `promote` and `rollback` then refuse with `action refused: no applicable active rollout or lifecycle work was observed`.

## `onInconclusive`

| `onInconclusive` | An inconclusive sample |
| --- | --- |
| `Hold`, or unset | Holds the step. When the analysis stalls, the canary's phase turns `Failed`. |
| `Rollback` | Rolls the canary back. |
| `RollbackOnStall` | Holds the step. When the analysis stalls, the canary rolls back. |

With `Rollback`, a Prometheus outage, a missing token Secret or a `no data` result rolls the canary back.

The analysis stalls when its samples stay inconclusive for longer than the canary's [ready timeout](canary-progression.md#readytimeout), 15 minutes with the chart's defaults.

A `Failed` canary stays at its step, even after you fix the metrics source, and `promote --override-analysis` refuses there. A new revision doesn't start a fresh canary either, which is a known bug. Roll the canary back with the alpha `kubectl ome rollout rollback`, then apply your fix, as [When a step never becomes ready](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md#when-a-step-never-becomes-ready) shows.

## Analysis in status

OME records the analysis in `status.components.<component>.canary`, where `<component>` is the group's `router`, or else its `engine`. The phase is in `status.components.<component>.rolloutPhase`. While a step waits on its analysis, [`kubectl ome rollout status`](../kubectl-ome/rollout.md#status-output-fields) shows GATE `Analysis`, and the [`promote` preview](../kubectl-ome/rollout.md#promote-and-rollback) shows the analysis state.

| Field | What it holds |
| --- | --- |
| `currentStep` | The current step's index, from 0. Once the canary completes, it equals the number of steps. |
| `stepEnteredTime` | When the step's new instances were ready. The warm-up and the bake count from it. |
| `analysisFailedChecks` | The failing samples at this step. |
| `lastEvaluationTime` | When the last judged sample finished. |
| `lastConclusiveEvaluationTime` | When the last pass or fail at this step finished. |
| `metricResults` | One entry per metric of the last judged sample. |
| `rolledBackRevisionHash` | The revision that a rollback rejected, by the analysis or by hand. |

Each entry of `metricResults` holds the metric's `name`, `threshold` and `operator`, its worst `value`, whether it `passed`, and the `time` of the sample. An inconclusive metric has `passed: false`, no `value`, and a `message` with its cause:

| What `metricResults` shows | Cause |
| --- | --- |
| The message `no data` | The query returned no numbers. |
| A message that starts with `template error:` | The query uses an unknown variable or a malformed template. |
| Another message | The query's error: the source is unreachable, it refused the token, or the sample ran out of [`queryTimeout`](#controller-settings). |
| One entry named `auth`, with a message that starts with `resolve bearer token:` | OME couldn't read the token from its Secret. |
| One entry named `prometheus` | OME couldn't build a client for the source's address. |

## Controller metrics

The controller exports these metrics on its metrics endpoint:

| Metric | Labels | What it measures |
| --- | --- | --- |
| `ome_canary_current_step` | `namespace`, `isvc`, `component` | The current step's index, from 0. It drops to 0 when the canary completes. |
| `ome_canary_traffic_weight` | `namespace`, `isvc`, `component` | The last `traffic` weight that OME recorded. It drops to 0 when the canary completes or rolls back. |
| `ome_canary_step_total` | `namespace`, `isvc`, `component` | Step advances. |
| `ome_canary_complete_total` | `namespace`, `isvc` | Canaries that completed on the new revision. |
| `ome_canary_analysis_evaluations_total` | `namespace`, `isvc`, `component`, `result` | Judged samples, by `result`: `pass`, `fail` or `inconclusive`. |
| `ome_canary_analysis_metric_value` | `namespace`, `isvc`, `component`, `metric` | The last value of each metric, kept when a sample has none. |
| `ome_canary_analysis_failed_checks` | `namespace`, `isvc`, `component` | The failing samples at the step, as of the last sample. |
| `ome_canary_rollback_total` | `namespace`, `isvc`, `component`, `cause` | Rollbacks, by `cause`: `analysis` or `manual`. |
| `ome_canary_sampler_inflight` | None | Samples running now. |
| `ome_canary_sampler_queue_depth` | None | Samples waiting for a free [`maxConcurrency`](#controller-settings) slot. |
| `ome_canary_sampler_starved_total` | None | Samples that had to wait for a slot. |

When a rollback finishes, OME records a Normal event with the reason `RolloutRunClosed` and the message `run <run ID> closed: RolledBack`. `ome_canary_rollback_total` tells analysis and manual rollbacks apart.

## Where the metrics come from

The canary's `prometheus` block picks the source:

| The canary's `prometheus` | OME queries |
| --- | --- |
| Unset, or without `providerRef` and `serverAddress` | The binding that `canaryAnalysis.defaultProvider` names, when it's set and bound. Otherwise `canaryAnalysis.bundledPrometheusAddress`. |
| `providerRef` | The binding that `providerRef.name` names in the `metricProviders` configuration. |
| `serverAddress` | That address. |

### Controller settings

The `canaryAnalysis` key of the `inferenceservice-config` ConfigMap sets the defaults that analysis uses. With the ome-resources chart, you set them under `ome.controller.canaryAnalysis`:

| Key | Chart default | What it sets | A change applies |
| --- | --- | --- | --- |
| `bundledPrometheusAddress` | `http://ome-prometheus.<release namespace>.svc:9090` | The fallback source for canaries that name none. | Within 30 seconds |
| `defaultProvider` | Empty | The `metricProviders` binding for canaries that name no source. An unbound name falls back to `bundledPrometheusAddress`. | Within 30 seconds |
| `queryTimeout` | `5s` | How long all the queries of one sample may take together. | Within 30 seconds |
| `maxConcurrency` | `8` | How many samples run at once, across all InferenceServices. Raise it when `ome_canary_sampler_queue_depth` stays above 0. | After a controller restart |
| `cacheTTL` | `2m` | How long a finished sample's result stays usable. | After a controller restart |

A Helm upgrade that changes these settings restarts the controller, so they all apply. After a [`kubectl edit`](../../guides/operate-ome/configure-the-controller.md#tune-the-config-cache) of the ConfigMap, the last column applies, where 30 seconds is the default of `ome.controller.configCacheTTL`. A `queryTimeout` or `cacheTTL` that isn't a positive duration, or a `maxConcurrency` below 1, falls back to its default.

If you run without the bundled Prometheus, set `bundledPrometheusAddress` or `defaultProvider`, or canaries that name no source get only inconclusive samples. The chart fills in the bundled address even with `prometheus.enabled: false`, and the kustomize manifests in `config/` leave it empty.

### Metric provider bindings

The top-level `metricProviders` key of the same ConfigMap binds provider names to endpoints, for canaries and for the triggers of the alpha [AutoscalerPolicy](../../concepts/serving/autoscaler-policy.md).

!!! warning "An invalid binding stops updates"
    A binding without `serverAddress` makes the whole `metricProviders` key invalid. Until you fix it, OME stops creating or updating the serving workloads of every InferenceService that sets `spec.rollout` or still has `status.rollout`. The controller logs `fails to load metricProviders config`. While the AutoscalerPolicy CRD is installed, every InferenceService stops instead, with `fails to load autoscalerPolicy config`. [InferenceServices stop updating](../../guides/operate-ome/metrics.md#inferenceservices-stop-updating) shows how to find the binding.

With the ome-resources chart, you set the key under `ome.metricProviders`:

```yaml
ome:
  metricProviders:
    cluster-prometheus:
      serverAddress: "http://prometheus.monitoring.svc:9090"
      authSecretRef:
        name: prometheus-bearer
        key: token
      headers:
        X-Scope-OrgID: tenant-a
```

| Field | Required | What it sets |
| --- | --- | --- |
| `serverAddress` | Yes | The base URL of the Prometheus HTTP API. |
| `authSecretRef` | No | A Secret, by `name` and `key`, that holds a bearer token. Each namespace that uses the binding needs its own copy. |
| `headers` | No | HTTP headers for every query. |

The older `ome.autoscalerPolicy.metricProviders` value counts only while the top-level key is absent.

### When a provider isn't bound

| The name that isn't bound | What happens |
| --- | --- |
| `providerRef`, when the rollout starts | The rollout parks, and the previous revision keeps serving. It starts soon after you add the binding. |
| `providerRef`, during the rollout | Samples go to `bundledPrometheusAddress`, without a token or headers. |
| `defaultProvider` | Samples go to `bundledPrometheusAddress`, with the canary's `authRef` and `headers`. Nothing reports it. |

A parked rollout sets the `RolloutPlanReady` condition to `False`, with the reason `ProviderUnbound` and the message `groups[<N>]: metric provider "<name>" is not bound in this cluster's metricProviders configuration`. OME also records a `RolloutPlanParked` Warning event.

## What admission rejects {#what-admission-checks}

The API server enforces the required fields, the allowed values and the counts in [Fields](#fields). It also rejects a `prometheus` block that sets both `providerRef` and `serverAddress`, with `at most one of providerRef and serverAddress may be set`. The OME webhook rejects these, where `<where>` is `spec.rollout.groups[<N>].canary` for an InferenceService and `spec.canary` for a RolloutPolicy:

| The webhook rejects | Message |
| --- | --- |
| An `interval` that isn't positive, such as `0s` | `<where>.steps[<i>].analysis: interval must be > 0 (AnalysisInvalid)` |
| An empty metric name | `<where>.steps[<i>].analysis.metrics[<j>].name must not be empty (AnalysisInvalid)` |
| A query that's empty or only whitespace | `<where>.steps[<i>].analysis.metrics[<j>] ("<name>"): query must not be empty (AnalysisInvalid)` |
| A threshold that isn't a number | `<where>.steps[<i>].analysis.metrics[<j>] ("<name>"): threshold "<threshold>" must be numeric (AnalysisInvalid)` |

A RolloutPolicy must name its metrics source through a binding, so its webhook also rejects these:

| The webhook rejects | Message |
| --- | --- |
| A `serverAddress` | `spec.canary.prometheus.serverAddress is not allowed in a policy body — a cluster-local URL defeats portability; name a providerRef bound in the operator's metricProviders configuration instead (RolloutPolicyInvalid)` |
| An `authRef` | `spec.canary.prometheus.authRef is not allowed in a policy body — auth comes from the providerRef's cluster binding (RolloutPolicyInvalid)` |
| A step with `analysis` and no `providerRef` | `spec.canary.prometheus.providerRef is required when any step declares analysis — a policy must never ship a gate with no resolvable metrics source (RolloutPolicyInvalid)` |

OME checks a policy's canary again when the rollout starts, and parks the rollout if it fails, with the reason `PolicyNotReady` or `PlanInvalid`: see [When a rollout parks](../../concepts/rollouts-and-traffic/rollout-policy.md#fail-closed-parking).

## Related pages

- [Canary progression](canary-progression.md): steps, capacity, traffic weights and the other gates.
- [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md): override an analysis gate, or roll back.
- [kubectl ome rollout](../kubectl-ome/rollout.md): the rollout commands.
- [Rollout policy](../../concepts/rollouts-and-traffic/rollout-policy.md): shared rollout settings, in alpha.
- [Configure the controller](../../guides/operate-ome/configure-the-controller.md): change the `inferenceservice-config` ConfigMap.
- [Collect metrics](../../guides/operate-ome/metrics.md): the bundled Prometheus, and scraping the controller.
- [RolloutAnalysis](../api/ome.v1beta1.md#ome-io-v1beta1-RolloutAnalysis) and [AnalysisPrometheus](../api/ome.v1beta1.md#ome-io-v1beta1-AnalysisPrometheus): the API reference.
