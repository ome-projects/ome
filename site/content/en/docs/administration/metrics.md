---
title: "Bundled Prometheus"
linkTitle: "Bundled Prometheus"
weight: 20
description: >
  The short-retention Prometheus the ome-resources chart deploys by default: what it scrapes, what depends on it, and how to tune or disable it.
---

The `ome-resources` chart deploys a single-replica Prometheus named `ome-prometheus` in the release namespace by default (`prometheus.enabled: true`). In-cluster consumers reach it through its ClusterIP Service:

```
http://ome-prometheus.<release-namespace>.svc:9090
```

It exists for one purpose: to be the metrics source for OME features that need recent time series — KEDA `prometheus` autoscaling triggers and canary metric analysis. It is **not** a long-term observability install, and its defaults reflect that: 30-minute retention, non-durable `emptyDir` storage, a single replica, and no Alertmanager, recording rules, or `remote_write`. Keep dashboards and alerting on your own monitoring stack.

## What depends on it

Two OME features default to the bundled instance:

- **KEDA autoscaling.** KEDA `prometheus` triggers rendered from an [AutoscalerPolicy](/ome/docs/concepts/autoscaler_policy/) resolve their endpoint through the `ome.metricProviders` Helm value. There is no built-in address, but the bundled Service is the natural binding target on clusters without their own Prometheus:

  ```yaml
  ome:
    metricProviders:
      cluster-prometheus:
        serverAddress: "http://ome-prometheus.ome.svc:9090"
  ```

- **Canary analysis.** When `ome.controller.canaryAnalysis.bundledPrometheusAddress` is empty (the default), the chart renders it as `http://ome-prometheus.<release-namespace>.svc:<prometheus.service.port>`. A canary rollout that names neither its own `serverAddress` nor a `providerRef` queries the bundled instance.

## What gets installed

With `prometheus.enabled: true` the chart renders:

| Resource | Name | Purpose |
|----------|------|---------|
| Deployment | `ome-prometheus` | Single replica, `Recreate` strategy, `prom/prometheus:v3.0.1`, runs as non-root |
| Service | `ome-prometheus` | ClusterIP on port `9090` (`prometheus.service.port`) |
| ConfigMap | `ome-prometheus-config` | The generated `prometheus.yml` |
| ServiceAccount + ClusterRole/Binding | `ome-prometheus` | `get`/`list`/`watch` on pods, services, and endpoints, cluster-wide, for target discovery; no write verbs |
| PersistentVolumeClaim | `ome-prometheus` | Only when `prometheus.persistence.enabled=true` and no `existingClaim` is named |

A ConfigMap change rolls the Deployment through a checksum annotation on the next `helm upgrade`. With the default `emptyDir` storage, every pod replacement starts with an empty TSDB — acceptable for autoscaling input, where only the last few minutes matter.

## What it scrapes

The generated configuration has a 15-second scrape interval (`prometheus.scrapeInterval`) and three jobs, plus any you append yourself.

### `ome-inferenceservice-pods`

Discovers pods carrying the `ome.io/inferenceservice` label — the label every OME deployment mode stamps on the pods it creates for an InferenceService. The label selector is applied **server-side** at discovery, so the Kubernetes watch only ever caches InferenceService pods, not every pod in the cluster.

Discovered pods are then filtered:

- Only `Running` **and** ready pods are kept. A pod still loading model weights has no listening metrics endpoint yet, so admitting it would only record scrape failures.
- With the default `requireScrapeAnnotation: false`, every InferenceService pod is scraped unless it opts out with `prometheus.io/scrape: "false"`. OME's pre-configured runtimes stamp `prometheus.io/scrape: "true"` on their pods anyway.
- With `requireScrapeAnnotation: true`, scraping becomes opt-in: a pod must carry `prometheus.io/scrape: "true"` or `ome.io/enable-prometheus-scraping: "true"`, and an explicit `prometheus.io/scrape: "false"` still wins. Enable this on large or shared clusters to bound the scrape population.

The scrape address and path honor the standard `prometheus.io/port` and `prometheus.io/path` pod annotations; ServingRuntime pod templates are the usual place these are set. Each kept target gets `namespace`, `pod`, `inferenceservice`, `component`, and `revision_hash` labels attached — `revision_hash` (from the `ome.io/revision-hash` pod label) lets a canary or dashboard query scope to the pods one rollout created rather than every pod of the component.

This job is also where the optional `prometheus.scrapeLimits` (sample, target, label, and body-size limits) and `prometheus.metricRelabelConfigs` apply.

### `ome-control-plane`

Discovers pods **only in the release namespace** and keeps those annotated `prometheus.io/scrape: "true"`. The controller-manager Deployment and the model-agent DaemonSet carry that annotation by default, so control-plane metrics (reconcile rates, work queues, webhook results) are available for the bundled instance without further configuration.

### `ome-prometheus` (self-scrape)

Enabled by default (`prometheus.selfScrape.enabled: true`); retains Prometheus's own process and TSDB metrics for capacity planning. All series additionally carry the `prometheus.externalLabels` set (default `cluster: ome`).

### Extra jobs

`prometheus.extraScrapeConfigs` appends complete raw `scrape_configs` entries for signals outside the InferenceService pod set — an ingress proxy, a shared exporter:

```yaml
prometheus:
  extraScrapeConfigs:
    - job_name: ingress-proxies
      kubernetes_sd_configs:
        - role: pod
          namespaces:
            names: [some-gateway-namespace]
      relabel_configs:
        - source_labels: [__meta_kubernetes_pod_annotation_prometheus_io_scrape]
          regex: "true"
          action: keep
```

Scope every extra job by namespace or an equivalent selector: unlike the generated InferenceService job, a job without a server-side bound makes the discovery informer cache every pod in the cluster.

## Tuning

### Retention

```yaml
prometheus:
  retention: 30m      # time retention for persisted TSDB blocks
  retentionSize: 6GB  # size guard; empty disables it
```

The 30-minute window is sized to KEDA query windows and canary analysis, not dashboards. Two Prometheus storage caveats are worth knowing when you resize:

- Time retention only deletes **persisted blocks**. The mutable head block and WAL can span Prometheus's two-hour block duration, so disk usage exceeds what `retention: 30m` suggests.
- `retentionSize` counts the head and WAL toward the limit but can only delete persisted blocks. Keep it below roughly 80–85% of the volume capacity (the default `6GB` is about 70% of the 8Gi `emptyDir` cap) so compaction and the WAL have headroom.

### Storage and persistence

By default the TSDB lives on an `emptyDir` capped at `prometheus.storage.sizeLimit: 8Gi`, and history does not survive pod replacement. If the retention window must survive restarts, switch to a PVC:

```yaml
prometheus:
  persistence:
    enabled: true
    size: 16Gi
    storageClassName: ""   # empty uses the cluster default
    existingClaim: ""      # takes precedence over a chart-managed claim
```

The Deployment is a single replica with a `Recreate` strategy, so a `ReadWriteOnce` claim is sufficient.

### Namespace scoping

`prometheus.scrapeNamespaces` (default empty = all namespaces) hard-scopes InferenceService pod discovery to a namespace list. The label selector already bounds the discovery cache to InferenceService pods, but on large multi-tenant clusters, scoping to the namespaces you actually autoscale bounds scrape cardinality too:

```yaml
prometheus:
  scrapeNamespaces:
    - team-a
    - team-b
```

### Memory sizing

Prometheus memory scales with active series: scrape targets × series per pod. LLM-serving pods expose high-cardinality metrics (per-request histograms, KV-cache gauges, per-model labels), so a few hundred InferenceService pods can push the head block past the limit and into an OOM crashloop — and the WAL replays the oversized head on every restart, so the crashloop persists.

The default `3Gi` limit covers a moderate fleet (roughly 500 InferenceService pods). For larger fleets, prefer cutting cardinality over raising memory:

- Scope `scrapeNamespaces` to the namespaces you autoscale.
- Use `prometheus.metricRelabelConfigs` to keep only the series your autoscaling and canary PromQL actually reference.
- Set `prometheus.scrapeLimits` (per-target `sampleLimit`, pool-wide `targetLimit`, label limits) as guardrails. Prometheus fails the whole scrape pool when `targetLimit` is exceeded, so set it above the observed post-relabel target count.
- Optionally set `prometheus.goMemLimit` (for example `2GiB`) below the container limit to reserve non-heap headroom, and cap query cost with `prometheus.query.maxConcurrency`, `maxSamples`, and `timeout`.

## Disabling it

If your cluster already runs a Prometheus that scrapes OME pods, disable the bundled instance and point OME's consumers at yours:

```yaml
prometheus:
  enabled: false

ome:
  metricProviders:
    cluster-prometheus:
      serverAddress: "http://prometheus.monitoring.svc:9090"
  controller:
    canaryAnalysis:
      bundledPrometheusAddress: "http://prometheus.monitoring.svc:9090"
```

Set both explicitly: `bundledPrometheusAddress` falls back to the bundled Service address when empty — even with the deployment disabled — so leaving it unset points canary analysis at an endpoint that no longer exists. `metricProviders` bindings never fall back (an unbound provider name is a render error), but any binding you wrote against the bundled Service needs the same rebinding.

Your Prometheus must collect the series those queries use — InferenceService pods advertise themselves through the standard `prometheus.io/scrape`/`port`/`path` annotations, and for the control plane the chart offers a `serviceMonitor` (controller-manager) and `podMonitor` (model-agent) for prometheus-operator-based stacks.

## Key values

| Value | Default | Meaning |
|-------|---------|---------|
| `prometheus.enabled` | `true` | Deploy the bundled Prometheus |
| `prometheus.retention` | `30m` | Time retention for persisted blocks |
| `prometheus.retentionSize` | `6GB` | Size-based retention guard |
| `prometheus.scrapeInterval` | `15s` | Global scrape interval |
| `prometheus.requireScrapeAnnotation` | `false` | Make InferenceService scraping opt-in |
| `prometheus.scrapeNamespaces` | `[]` | Namespaces to discover InferenceService pods in (empty = all) |
| `prometheus.extraScrapeConfigs` | `[]` | Additional raw scrape jobs |
| `prometheus.metricRelabelConfigs` | `[]` | Series filtering for the InferenceService job |
| `prometheus.scrapeLimits.*` | unset | Per-scrape and target-pool guardrails |
| `prometheus.storage.sizeLimit` | `8Gi` | `emptyDir` cap when persistence is off |
| `prometheus.persistence.enabled` | `false` | Store the TSDB on a PVC |
| `prometheus.resources.limits.memory` | `3Gi` | Sized for roughly 500 InferenceService pods |
| `prometheus.goMemLimit` | `""` | Optional Go soft memory limit |
| `prometheus.service.port` | `9090` | Service-facing port consumers dial |

See `charts/ome-resources/values.yaml` for the full list, including query guards, external labels, self-scrape, image, and scheduling knobs.
