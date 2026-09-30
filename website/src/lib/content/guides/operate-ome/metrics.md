---
title: Collect metrics
description: Tune or turn off the short-retention Prometheus that the ome-resources chart deploys for KEDA autoscaling and canary analysis, or scrape OME with your own.
since: v1.3
---

By default, the `ome-resources` chart runs a Prometheus server, `ome-prometheus`, that scrapes your InferenceService pods and OME's own components. Two features read from it: [canary analysis](../../reference/rollouts/canary-analysis.md) on [OMENative](../../concepts/omenative/overview.md) components, when a canary names no other source, and [KEDA triggers](../../concepts/serving/component-autoscaling.md#keda) that point at `http://ome-prometheus.ome.svc:9090`. It keeps 30 minutes of data on a temporary volume: enough for those decisions, too little for dashboards or long-term monitoring. For those, [use your own Prometheus](#use-your-own-prometheus), next to the bundled one or in its place.

<div class="prerequisites" markdown>

- OME installed with the `ome-crd` and `ome-resources` charts, version 1.3.0 or later, as the releases `ome-crd` and `ome` in the namespace `ome`. See [Install OME](../../getting-started/install.md).
- Helm and `kubectl` access to the cluster.
- The values file that you installed `ome-resources` with. `helm get values ome -n ome` prints the values that you set.
- `curl` and `jq`, for Step 1 and troubleshooting.

</div>

## What it scrapes

Prometheus runs three scrape jobs, every 15 seconds by default (`prometheus.scrapeInterval`):

| Job | What it scrapes |
| --- | --- |
| `ome-inferenceservice-pods` | The pods of your [InferenceServices](../../concepts/serving/inference-services.md), in all namespaces or in the ones that `prometheus.scrapeNamespaces` lists. |
| `ome-control-plane` | OME's components: the pods in the chart's namespace with the annotation `prometheus.io/scrape: "true"`. |
| `ome-prometheus` | Prometheus itself, while `prometheus.selfScrape.enabled` is `true`, the default. |

The chart lets Prometheus read pods, Services and Endpoints in all namespaces. To scrape more, add jobs to `prometheus.extraScrapeConfigs`. Limit them to the namespaces that they need: a cluster-wide job makes Prometheus hold all the cluster's pods in memory.

### InferenceService pods

The job finds pods by the label `ome.io/inferenceservice`, which OME sets on all InferenceService pods. It scrapes a pod while the pod is running and ready.

By default, it scrapes all these pods except the ones annotated `prometheus.io/scrape: "false"`. On large or shared clusters, set `prometheus.requireScrapeAnnotation: true` to scrape only the pods annotated `prometheus.io/scrape: "true"`. Opt pods in with that annotation, not with `ome.io/enable-prometheus-scraping`: see [A target is down](#a-target-is-down).

It scrapes the port in the pod's `prometheus.io/port` annotation, at the path in `prometheus.io/path`, or `/metrics` by default. Most catalog runtimes set these annotations in their `engineConfig.annotations`, and yours can too.

It adds these labels to the series that it scrapes:

| Label | Value |
| --- | --- |
| `namespace` | The pod's namespace. |
| `pod` | The pod's name. |
| `inferenceservice` | The InferenceService's name, from the pod's `ome.io/inferenceservice` label. |
| `component` | The pod's `component` label, for example `engine`. |
| `revision_hash` | The pod's `ome.io/revision-hash` label, which OMENative sets to the hash of the pod's revision. |

KEDA triggers and canary queries select series by these labels. A canary query can match `revision_hash` to compare the new revision's pods with the stable revision's.

### OME's components

The `ome-control-plane` job reads the same port and path annotations, and adds the `namespace` and `pod` labels:

| Component | The bundled Prometheus scrapes | A Prometheus Operator stack uses |
| --- | --- | --- |
| Controller manager | All replicas, on port 8080. | `serviceMonitor.enabled: true` |
| Model agent | Its pods, on port 8080, while `modelAgent.enabled` is `true`. | `podMonitor.enabled: true`: see [Metrics](model-agent.md#health-checks-and-metrics). |
| Quota manager | Its pods in `ome`, while its ServiceMonitor is off. | Its ServiceMonitor: see [Metrics](accelerator-quota.md#metrics). |
| [Alfred](../../concepts/scheduling/alfred.md) (alpha) | Its pods in `ome`. | Its ServiceMonitor: see [Alfred metrics and events](../../reference/scheduling/alfred-metrics-and-events.md). |
| [OME scheduler](../../concepts/scheduling/ome-scheduler.md) (alpha) | Nothing: its metrics need HTTPS and authorization. | Its ServiceMonitor: see [Metrics](ome-scheduler.md#metrics). |

[Servers and metrics](../../reference/operate-ome/controller-manager-flags.md#servers-and-metrics) describes the controller's metrics endpoint, and [Controller metrics](../../reference/rollouts/canary-analysis.md#controller-metrics) the canary metrics on it.

## Step 1: Check the targets

Forward a local port to the Prometheus Service:

```bash
kubectl port-forward -n ome service/ome-prometheus 9090:9090
```

```output
Forwarding from 127.0.0.1:9090 -> 9090
Forwarding from [::1]:9090 -> 9090
```

From another terminal, list the active targets:

```bash
curl -s localhost:9090/api/v1/targets | jq -r '.data.activeTargets[] | [.scrapePool, .labels.pod // "-", .health] | @tsv'
```

Each line shows a target's job, pod and health: `up`, `down`, or `unknown` before its first scrape. Expect lines for the pods that [What it scrapes](#what-it-scrapes) covers, and one `ome-prometheus` line with `-` for the pod. If a pod is missing or a target is down, see [Troubleshooting](#troubleshooting).

List the labels of the InferenceService targets:

```bash
curl -s localhost:9090/api/v1/targets | jq -r '.data.activeTargets[] | select(.labels.inferenceservice) | .labels | [.namespace, .inferenceservice, .component, .revision_hash // "-", .pod] | @tsv'
```

Each line shows a pod's namespace, InferenceService, component, revision hash (`-` if none) and name.

## Step 2: Tune retention, storage and scope

By default, Prometheus keeps 30 minutes of data, up to 6GB, in an 8Gi `emptyDir` volume that's lost when the pod is replaced. Its 3Gi memory limit suits about 500 InferenceService pods. On a larger fleet, scrape less before you add memory.

This example keeps two hours of data on a volume that survives restarts, and scrapes the InferenceService pods of one namespace:

```yaml title="values.yaml"
prometheus:
  retention: 2h
  retentionSize: 12GB
  persistence:
    enabled: true
    size: 16Gi
  scrapeNamespaces:
    - llama-demo
```

- `persistence` replaces the `emptyDir` with a 16Gi PersistentVolumeClaim, `ome-prometheus`, from the default StorageClass. [Key values](#key-values) lists the claim's other settings.
- `retentionSize: 12GB` fits the larger volume. Keep it below 80 to 85% of the volume's size, as the default 6GB does for 8Gi.
- `scrapeNamespaces` limits the InferenceService job to `llama-demo`. The `ome-control-plane` job still scrapes the namespace `ome`.

To cut memory further, set `requireScrapeAnnotation: true`, or drop series with `metricRelabelConfigs`. This value keeps two of vLLM's series and drops the rest, so first check which series your triggers and queries read:

```yaml title="values.yaml"
prometheus:
  metricRelabelConfigs:
    - source_labels: [__name__]
      regex: "vllm:num_requests_waiting|vllm:num_requests_running"
      action: keep
```

!!! warning "The upgrade restarts Prometheus"
    The upgrade replaces the Prometheus pod, and stops the old pod first. Until the new pod is ready, canary samples are inconclusive and KEDA can't read its triggers. With the default `emptyDir`, the new pod starts with no data.

Add these values to your values file: `helm upgrade` resets any value that's missing from it. Upgrade with your current chart version, from the `CHART` column of `helm list -n ome`:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm reports that the release `ome` has been upgraded. Wait for the new Prometheus pod:

```bash
kubectl rollout status deployment ome-prometheus -n ome
```

When the new pod is running, the command ends with:

```output
deployment "ome-prometheus" successfully rolled out
```

Print the arguments of the `prometheus` container, one per line:

```bash
kubectl get deployment ome-prometheus -n ome \
  -o jsonpath='{range .spec.template.spec.containers[?(@.name=="prometheus")].args[*]}{@}{"\n"}{end}'
```

With the values above and other values at their defaults, the output is:

```output
--config.file=/etc/prometheus/prometheus.yml
--storage.tsdb.path=/prometheus
--storage.tsdb.retention.time=2h
--storage.tsdb.retention.size=12GB
--web.enable-lifecycle
--web.listen-address=:9090
```

Restart the port-forward, which stopped with the old pod, and rerun the labels command from Step 1. Its first column now reads `llama-demo`.

## Use your own Prometheus

Run your own Prometheus next to the bundled one for dashboards and long-term storage, or in its place. ServiceMonitors, PodMonitors and the chart's [alerts](alerting.md) need a Prometheus that the Prometheus Operator runs. To replace the bundled one, set up scraping first, then turn it off.

### Scrape InferenceService pods

Your Prometheus needs to add the [labels](#inferenceservice-pods) that queries select by. Any Prometheus can copy the bundled scrape job, and on a Prometheus Operator stack, OMENative components can use OME's PodMonitors.

=== "Copy the scrape job"

    Render the bundled scrape configuration:

    ```bash
    helm template ome oci://ghcr.io/moirai-internal/charts/ome-resources \
      --namespace ome --version 1.3.0 --show-only templates/prometheus/configmap.yaml
    ```

    Helm prints the ConfigMap `ome-prometheus-config`, rendered with the chart's defaults. Copy the `ome-inferenceservice-pods` job from its `prometheus.yml` into your Prometheus's `scrape_configs`. Your Prometheus needs permission to list and watch pods in the namespaces that it scrapes.

=== "Use OME's PodMonitors"

    OME creates a PodMonitor for each InferenceService component when the PodMonitor CRD was installed before the controller started: see [The PodMonitor CRD](../../getting-started/install.md#the-podmonitor-crd). It scrapes `/metrics` on the first container's port named `metrics`. Without one, it uses that container's first port if the port has a name, or else a port named `http`.

    For OMENative components, set `ome.podMonitor` so that your Prometheus selects the PodMonitors, and they add the three labels:

    ```yaml title="values.yaml"
    ome:
      podMonitor:
        labels:
          release: prometheus
        relabelings:
          - sourceLabels: ["__meta_kubernetes_pod_label_ome_io_inferenceservice"]
            targetLabel: inferenceservice
          - sourceLabels: ["__meta_kubernetes_pod_label_component"]
            targetLabel: component
          - sourceLabels: ["__meta_kubernetes_pod_label_ome_io_revision_hash"]
            targetLabel: revision_hash
    ```

    - `labels`: the labels that your Prometheus's `podMonitorSelector` matches.
    - `relabelings`: relabelings for the PodMonitors' endpoints. These three add `inferenceservice`, `component` and `revision_hash`.
    - `metricRelabelings`, not shown: filters for the scraped series.

    `ome.podMonitor` applies only to OMENative components. For RawDeployment components, the default for a component without a leader or workers, and for `MultiNode` (deprecated) components, copy the scrape job.

    Upgrade the release with the `helm upgrade` command from [Step 2](#step-2-tune-retention-storage-and-scope).

To check the result, run the labels command from [Step 1](#step-1-check-the-targets) against your Prometheus.

### Scrape OME's components

On a Prometheus Operator stack, turn on the monitors in the [OME's components](#omes-components) table, and add the labels that your Prometheus selects to their `additionalLabels`. The controller's ServiceMonitor, `ome-controller-manager`, needs the Prometheus Operator's `monitoring.coreos.com/v1` CRDs. [Key values](#key-values) lists its settings.

A Prometheus that scrapes pods by their `prometheus.io/scrape` annotation finds the controller, model agent, quota manager and Alfred without monitors. Use one of the two ways, or it scrapes these pods twice.

### Turn off the bundled Prometheus

Point canary analysis and KEDA at your Prometheus in the same upgrade that sets `prometheus.enabled: false`. This example uses a Prometheus behind the Service `prometheus` in the namespace `monitoring`:

```yaml title="values.yaml"
prometheus:
  enabled: false
ome:
  controller:
    canaryAnalysis:
      bundledPrometheusAddress: "http://prometheus.monitoring.svc:9090"
  metricProviders:
    cluster-prometheus:
      serverAddress: "http://prometheus.monitoring.svc:9090"
```

- `bundledPrometheusAddress` is the source for canaries that name none, and can be any Prometheus. If it's empty, the chart still renders the bundled address, so those canaries' samples are all [inconclusive](../../reference/rollouts/canary-analysis.md#oninconclusive). To send a bearer token or headers, name a binding in `ome.controller.canaryAnalysis.defaultProvider` instead.
- `ome.metricProviders` binds the names that canaries and alpha [AutoscalerPolicy](../../concepts/serving/autoscaler-policy.md#metric-providers) triggers give in `providerRef`: see [Metric provider bindings](../../reference/rollouts/canary-analysis.md#metric-provider-bindings). Point the bindings that named the bundled Prometheus at yours.
- In your InferenceServices, change the `serverAddress` of the KEDA triggers that point at `ome-prometheus`.

!!! warning "Give every binding a serverAddress"
    A binding without `serverAddress` makes the whole `ome.metricProviders` key invalid. The controller then stops creating or updating the serving workloads of InferenceServices that set `spec.rollout`, or of all InferenceServices while the AutoscalerPolicy CRD is installed. See [InferenceServices stop updating](#inferenceservices-stop-updating).

The upgrade deletes the bundled Prometheus and its data, including the claim `ome-prometheus` if the chart created it. A claim that you named in `persistence.existingClaim` stays, and so do your ServiceMonitors, PodMonitors and alert rules.

Upgrade the release with the `helm upgrade` command from [Step 2](#step-2-tune-retention-storage-and-scope). Then print the controller's canary settings:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.canaryAnalysis}'
```

```output
{
  "bundledPrometheusAddress": "http://prometheus.monitoring.svc:9090",
  "defaultProvider": "",
  "queryTimeout": "5s",
  "maxConcurrency": 8,
  "cacheTTL": "2m"
}
```

The upgrade also restarts the controller manager, which then uses the new addresses.

## Key values

The `prometheus` values configure the bundled Prometheus:

| Value | Default | What it sets |
| --- | --- | --- |
| `prometheus.enabled` | `true` | Deploys Prometheus. |
| `prometheus.retention` | `30m` | How long to keep data. |
| `prometheus.retentionSize` | `6GB` | The size at which the oldest data goes. Empty turns the limit off. |
| `prometheus.scrapeInterval` | `15s` | How often to scrape. |
| `prometheus.scrapeNamespaces` | `[]` | The namespaces that the InferenceService job scrapes. Empty means all. |
| `prometheus.requireScrapeAnnotation` | `false` | Scrapes only the InferenceService pods that opt in. |
| `prometheus.metricRelabelConfigs` | `[]` | The `metric_relabel_configs` of the InferenceService job. |
| `prometheus.scrapeLimits` | None | Limits on the InferenceService job's scrapes: `sampleLimit`, `targetLimit` and others. |
| `prometheus.extraScrapeConfigs` | `[]` | Complete scrape jobs, appended to the chart's. |
| `prometheus.resources` | Requests `100m` and `512Mi`, limits `500m` and `3Gi` | The container's CPU and memory. |
| `prometheus.goMemLimit` | Empty | A soft memory limit, `GOMEMLIMIT`. Set it below the container's memory limit. |
| `prometheus.storage.sizeLimit` | `8Gi` | The `emptyDir` volume's size, when persistence is off. |
| `prometheus.persistence.enabled` | `false` | Keeps the data on a PersistentVolumeClaim. |
| `prometheus.persistence.size` | `16Gi` | The size of the chart's claim. |
| `prometheus.persistence.storageClassName` | Empty | The StorageClass of the chart's claim. Empty uses the cluster's default. |
| `prometheus.persistence.existingClaim` | Empty | Your own claim, used instead of the chart's. |
| `prometheus.image.repository`, `prometheus.image.tag` | `docker.io/prom/prometheus`, `v3.0.1` | The image. `global.hub` doesn't apply to it: see [The Prometheus pod pulls from Docker Hub](../../getting-started/private-registries.md#the-prometheus-pod-pulls-from-docker-hub). |
| `prometheus.nodeSelector`, `prometheus.tolerations`, `prometheus.affinity` | Empty | Where the Prometheus pod runs. |

The `serviceMonitor` values configure the controller's ServiceMonitor:

| Value | Default | What it sets |
| --- | --- | --- |
| `serviceMonitor.enabled` | `false` | Whether the chart renders the ServiceMonitor `ome-controller-manager`. |
| `serviceMonitor.interval` | `30s` | How often Prometheus scrapes the controller. |
| `serviceMonitor.scrapeTimeout` | `10s` | How long a scrape may take. |
| `serviceMonitor.additionalLabels` | `{}` | Labels on the ServiceMonitor, for your Prometheus's `serviceMonitorSelector` to match. |
| `serviceMonitor.relabelings` | `[]` | Relabelings applied before the scrape. |
| `serviceMonitor.metricRelabelings` | `[]` | Relabelings applied to the scraped series. |

## Troubleshooting

### A pod is missing from the targets

Check that the pod is running and ready. Then check its `prometheus.io/scrape` annotation, and the `requireScrapeAnnotation` and `scrapeNamespaces` values: see [What it scrapes](#what-it-scrapes).

### A target is down

List the targets whose last scrape failed, with the URL that Prometheus scraped and the error:

```bash
curl -s localhost:9090/api/v1/targets | jq -r '.data.activeTargets[] | select(.health == "down") | [.scrapePool, .scrapeUrl, .lastError] | @tsv'
```

Compare a target's port and path with its pod's annotations, and with the port that the pod serves metrics on.

A port of `9091` or `9088`, where no OME container serves metrics, comes from the annotation `ome.io/enable-prometheus-scraping: "true"`, or from the chart value `ome.metricsaggregator.enablePrometheusScraping: "true"`. Remove the annotation, or set the value back to `"false"`, its default. Then opt the pod in with `prometheus.io/scrape: "true"`.

### Prometheus keeps restarting

A Prometheus that runs out of memory can run out again as it restarts and reloads recent data. Scrape less, as in [Step 2](#step-2-tune-retention-storage-and-scope), then raise its memory in `prometheus.resources`.

### InferenceServices stop updating

Look for a binding without `serverAddress` in the logs of all controller replicas:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'has no serverAddress'
```

Each line names the binding, in a `fails to load metricProviders config` or `fails to load autoscalerPolicy config` error. Add the binding's `serverAddress` and upgrade again.

## Next steps

- [Set up alerting](alerting.md): turn on the chart's alerts, on a Prometheus that the Prometheus Operator runs.
- [Canary metric analysis](../../reference/rollouts/canary-analysis.md): write the queries that gate a canary's steps.
- [Component autoscaling](../../concepts/serving/component-autoscaling.md#keda): scale a component on a `prometheus` trigger.
