# ome-resources

![Version: 0.1.0](https://img.shields.io/badge/Version-0.1.0-informational?style=flat-square) ![Type: application](https://img.shields.io/badge/Type-application-informational?style=flat-square) ![AppVersion: 1.16.0](https://img.shields.io/badge/AppVersion-1.16.0-informational?style=flat-square)

OME Resources and Controller

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| global.imagePullSecrets | list | `[]` |  |
| global.hub | string | `"ghcr.io/moirai-internal"` |  |
| modelAgent.health.port | int | `8080` |  |
| modelAgent.hostPath | string | `"/mnt/data/models"` |  |
| modelAgent.image.pullPolicy | string | `"Always"` |  |
| modelAgent.image.repository | string | `"model-agent"` |  |
| modelAgent.image.tag | string | `"v0.1.2"` |  |
| modelAgent.nodeSelector | object | `{}` |  |
| modelAgent.extraVolumeMounts | list | `[]` |  |
| modelAgent.extraVolumes | list | `[]` |  |
| modelAgent.numDownloadWorkers | int | `2` |  |
| modelAgent.numHighPriorityWorkers | int | `1` |  |
| modelAgent.priorityClassName | string | `""` |  |
| modelAgent.resources.limits.cpu | string | `"10"` |  |
| modelAgent.resources.limits.memory | string | `"100Gi"` |  |
| modelAgent.resources.requests.cpu | string | `"10"` |  |
| modelAgent.resources.requests.memory | string | `"100Gi"` |  |
| modelAgent.serviceAccountName | string | `"ome-model-agent"` |  |
| modelAgent.samePathReuseWaitTimeout | string | `"30m"` |  |
| modelAgent.taskSchedulerCapacity | int | `4096` |  |
| modelAgent.tolerations | list | `[{"key":"nvidia.com/gpu","operator":"Exists","effect":"NoSchedule"}]` |  |
| ome.controller.affinity | object | `{}` |  |
| ome.controller.deploymentMode | string | `"RawDeployment"` |  |
| ome.controller.image | string | `"ome-manager"` |  |
| ome.controller.modelDownloadScheduling.servingDemandPriorityEnabled | bool | `false` |  |
| ome.controller.ingressGateway.additionalIngressDomains | string | `nil` |  |
| ome.controller.ingressGateway.disableIngressCreation | bool | `true` |  |
| ome.controller.ingressGateway.domain | string | `"svc.cluster.local"` |  |
| ome.controller.ingressGateway.domainTemplate | string | `"{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}"` |  |
| ome.controller.ingressGateway.enableGatewayAPI | bool | `false` |  |
| ome.controller.ingressGateway.ingressGateway.className | string | `"istio"` |  |
| ome.controller.ingressGateway.ingressGateway.gateway | string | `"istio-system/ingress-gateway"` |  |
| ome.controller.ingressGateway.ingressGateway.gatewayService | string | `"istio-ingressgateway.istio-system.svc.cluster.local"` |  |
| ome.controller.ingressGateway.omeIngressGateway | string | `""` |  |
| ome.controller.ingressGateway.pathTemplate | string | `""` |  |
| ome.controller.ingressGateway.urlScheme | string | `"http"` |  |
| ome.controller.nodeSelector | object | `{}` |  |
| ome.controller.replicaCount | int | `3` |  |
| ome.controller.resources.limits.cpu | int | `2` |  |
| ome.controller.resources.limits.memory | string | `"4Gi"` |  |
| ome.controller.resources.requests.cpu | int | `2` |  |
| ome.controller.resources.requests.memory | string | `"4Gi"` |  |
| ome.controller.tag | string | `"v0.1.2"` |  |
| ome.controller.tolerations | list | `[]` |  |
| ome.controller.topologySpreadConstraints | list | `[]` |  |
| ome.metricsaggregator.enableMetricAggregation | string | `"false"` |  |
| ome.metricsaggregator.enablePrometheusScraping | string | `"false"` |  |
| ome.multiclusterAccess.enabled | bool | `true` | Install ServiceAccount, token Secret, and scoped RBAC for InferenceDeploymentOperator. |
| ome.omeAgent.authType | string | `"InstancePrincipal"` |  |
| ome.omeAgent.compartmentId | string | `"ocid1.compartment.oc1..dummy-compartment"` |  |
| ome.omeAgent.fineTunedAdapter.cpuLimit | int | `15` |  |
| ome.omeAgent.fineTunedAdapter.cpuRequest | int | `15` |  |
| ome.omeAgent.fineTunedAdapter.memoryLimit | string | `"320Gi"` |  |
| ome.omeAgent.fineTunedAdapter.memoryRequest | string | `"300Gi"` |  |
| ome.omeAgent.image | string | `"ome-agent"` |  |
| ome.omeAgent.modelInit.cpuLimit | int | `15` |  |
| ome.omeAgent.modelInit.cpuRequest | int | `15` |  |
| ome.omeAgent.modelInit.memoryLimit | string | `"180Gi"` |  |
| ome.omeAgent.modelInit.memoryRequest | string | `"150Gi"` |  |
| ome.omeAgent.region | string | `"ap-osaka-1"` |  |
| ome.omeAgent.tag | string | `"v0.1.2"` |  |
| ome.omeAgent.vaultId | string | `"ocid1.vault.oc1.ap-osaka-1.dummy.dummy-vault"` |  |
| ome.version | string | `"v0.1.2"` |  |
| prometheus.enabled | bool | `true` | Bundle a single-replica Prometheus for KEDA autoscaling input. See [Bundled Prometheus](#bundled-prometheus). |
| prometheus.externalLabels | object | `{"cluster":"ome"}` | Labels identifying this Prometheus to remote-write, federation, and alerting consumers. |
| prometheus.goMemLimit | string | `""` | Optional Go soft memory limit, for example `9GiB`; leave headroom below the container limit for mmap and other non-heap memory. |
| prometheus.listenPort | int | `9090` | Internal Prometheus HTTP listen port. |
| prometheus.metricRelabelConfigs | list | `[]` | Prometheus `metric_relabel_configs` for the InferenceService scrape job. |
| prometheus.persistence.accessModes | list | `["ReadWriteOnce"]` | Access modes for a chart-managed Prometheus PVC. |
| prometheus.persistence.annotations | object | `{}` | Annotations for a chart-managed Prometheus PVC. |
| prometheus.persistence.enabled | bool | `false` | Use a PVC instead of `emptyDir` for the TSDB. |
| prometheus.persistence.existingClaim | string | `""` | Existing PVC to mount when persistence is enabled; empty creates `ome-prometheus`. |
| prometheus.persistence.size | string | `"16Gi"` | Requested size for a chart-managed Prometheus PVC. |
| prometheus.persistence.storageClassName | string | `""` | StorageClass for a chart-managed PVC; empty uses the cluster default. |
| prometheus.query.maxConcurrency | int | `0` | Maximum concurrent PromQL queries; zero uses the Prometheus default. |
| prometheus.query.maxSamples | int | `0` | Maximum samples a single query may load; zero uses the Prometheus default. |
| prometheus.query.timeout | string | `""` | PromQL query timeout; empty uses the Prometheus default. |
| prometheus.requireScrapeAnnotation | bool | `false` | Require an explicit scrape opt-in through `prometheus.io/scrape` or `ome.io/enable-prometheus-scraping`. |
| prometheus.retention | string | `"30m"` | Persisted-block retention window. It does not bound the mutable head/WAL. |
| prometheus.retentionSize | string | `"6GB"` | TSDB size-retention guard. Keep below 80-85% of the selected volume capacity; empty disables it. |
| prometheus.scrapeInterval | string | `"15s"` |  |
| prometheus.scrapeLimits.bodySizeLimit | string | `""` | Maximum uncompressed response body accepted per InferenceService scrape; empty disables it. |
| prometheus.scrapeLimits.keepDroppedTargets | int | `0` | Maximum dropped InferenceService targets retained in memory; zero uses the Prometheus default. |
| prometheus.scrapeLimits.labelLimit | int | `0` | Maximum labels accepted per scraped sample; zero disables it. |
| prometheus.scrapeLimits.labelNameLengthLimit | int | `0` | Maximum scraped label-name length; zero disables it. |
| prometheus.scrapeLimits.labelValueLengthLimit | int | `0` | Maximum scraped label-value length; zero disables it. |
| prometheus.scrapeLimits.sampleLimit | int | `0` | Maximum samples accepted from one InferenceService target per scrape; zero disables it. |
| prometheus.scrapeLimits.targetLimit | int | `0` | Maximum active targets allowed in the InferenceService scrape pool; zero disables it. |
| prometheus.scrapeNamespaces | list | `[]` | Namespaces in which to discover InferenceService pods; empty discovers them cluster-wide. |
| prometheus.scrapeTimeout | string | `"10s"` | Per-target scrape timeout. |
| prometheus.selfScrape.enabled | bool | `true` | Scrape Prometheus process and TSDB metrics for capacity planning. |
| prometheus.image.repository | string | `"prom/prometheus"` |  |
| prometheus.image.tag | string | `"v3.0.1"` |  |
| prometheus.resources.limits.cpu | string | `"500m"` |  |
| prometheus.resources.limits.memory | string | `"3Gi"` | Sized for a moderate fleet (~500 ISVC pods); larger fleets should scope `scrapeNamespaces` and/or trim scraped metrics. |
| prometheus.resources.requests.cpu | string | `"100m"` |  |
| prometheus.resources.requests.memory | string | `"512Mi"` |  |
| prometheus.service.port | int | `9090` |  |
| prometheus.service.type | string | `"ClusterIP"` |  |
| prometheus.storage.sizeLimit | string | `"8Gi"` | emptyDir size cap. Covers active head, WAL, and compaction overlap for a moderate high-cardinality inference fleet. |

## Bundled Prometheus

The chart enables a single-replica Prometheus alongside the controller by
default. It exists for one purpose: to be the metrics source that KEDA
`prometheus` triggers point at when an InferenceService's
[OEP-0013][oep-0013] autoscaler asks for one.

Keep it enabled when your cluster does not already run a Prometheus that
scrapes OME pods. Operators with an existing cluster Prometheus should disable
the bundled instance and configure their KEDA triggers' `serverAddress` to
point at the existing instance.

```sh
helm upgrade ome-resources ./charts/ome-resources \
  --set prometheus.enabled=false
```

KEDA triggers then address it via the cluster-local Service DNS:

```yaml
trigger:
  type: prometheus
  metadata:
    serverAddress: http://ome-prometheus.<release-namespace>.svc:9090
    query: <your-promql>
```

**What it is NOT**: this bundled Prometheus is not a long-term
observability install. Defaults reflect that explicitly:

- Single replica, no HA.
- Non-durable `emptyDir` storage by default; replacement pods start with an
  empty TSDB. Set `prometheus.persistence.enabled=true` and optionally provide
  `prometheus.persistence.existingClaim` when history must survive replacement.
- 8 GiB default `emptyDir` cap (`prometheus.storage.sizeLimit`) to accommodate
  active head, WAL, and compaction overlap; override it for the fleet's actual
  series count and ingestion rate.
- 30 minute time retention (`prometheus.retention`) for persisted blocks,
  sized to KEDA query windows rather than dashboards or alerts. Prometheus's
  mutable head and WAL can span its two-hour production block duration, so
  time retention alone is not a disk bound.
- 6 GB size retention (`prometheus.retentionSize`) below the 8 GiB
  `emptyDir` cap. Prometheus counts the head/WAL toward this policy but cannot
  delete them; large fleets must also increase `prometheus.storage.sizeLimit`
  or reduce scrape cardinality/rate.
- No Alertmanager, no recording rules, no `remote_write`.
- Discovers OME-owned pods through the `ome.io/inferenceservice` label. Set
  `prometheus.requireScrapeAnnotation=true` on large or shared clusters to
  require `prometheus.io/scrape: "true"` or
  `ome.io/enable-prometheus-scraping: "true"`. An explicit
  `prometheus.io/scrape: "false"` takes precedence.
- Exposes optional limits for each InferenceService scrape, the target pool,
  and PromQL query execution.

Large clusters should enable the guards explicitly:

```yaml
prometheus:
  requireScrapeAnnotation: true
  scrapeLimits:
    sampleLimit: 10000
    targetLimit: 5000
    labelLimit: 100
    labelNameLengthLimit: 128
    labelValueLengthLimit: 2048
    bodySizeLimit: 50MB
    keepDroppedTargets: 100
  query:
    maxConcurrency: 8
    maxSamples: 10000000
    timeout: 30s
```

Prometheus fails the scrape pool when `targetLimit` is exceeded, so choose it
above the observed post-relabel target count and alert before that threshold.

Use `prometheus.metricRelabelConfigs` to retain only metrics referenced by
autoscaling and canary PromQL. For example:

```yaml
prometheus:
  metricRelabelConfigs:
    - source_labels: [__name__]
      regex: "http_requests_total|request_queue_size"
      action: keep
```

Size memory from active series rather than retention:

```text
projected series = expected targets × measured series per target
memory limit     = 1.3 × (1 GiB + projected series × 4 KiB) + query headroom
```

After deployment, replace the planning estimate with observed peaks from
`prometheus_tsdb_head_series` and container working-set memory. Size the
request above the normal P95 and the limit above both the P99 and WAL-replay
peak. Time retention below Prometheus's roughly two-hour mutable head window
primarily changes persisted-block disk, not steady-state head memory.

Estimate retained block storage as `samples/sec × retention seconds × 1.5–2
bytes/sample`, then add the measured head and WAL footprint plus 30–50%
compaction headroom. Keep `retentionSize` below 80–85% of the volume size.

Enabling persistence replaces `emptyDir` with a fresh claim; it does not migrate
existing data. A chart-managed claim follows the Helm release lifecycle. Use an
existing claim when storage must remain independently managed, and fix memory
and cardinality limits before preserving a WAL that cannot be replayed.

For long-term observability, deploy a separate full Prometheus stack
(kube-prometheus-stack, etc.) and leave `prometheus.enabled=false`.

[oep-0013]: ../../oeps/0013-autoscaling/README.md

## Multicluster Access

`ome.multiclusterAccess.enabled=true` installs a ServiceAccount and long-lived
token Secret that a control-plane InferenceDeploymentOperator can use to manage
OME resources in this workload cluster. The RBAC is scoped to writing derived
`InferenceService` resources, plus read-only access to the `InferenceReplica`,
`ClusterServingRuntime`, `Pod` and `PodGroup` resources the control plane reads
to observe admission. Placement stalls in `Racing` without the
`InferenceReplica` read: the per-component IR status is the admission signal
that selects a winner.

## Default Runtime

The chart always installs `ClusterServingRuntime/default-runtime`.
InferenceDeploymentOperator-generated `InferenceService` resources reference
this runtime and supply only per-deployment overrides such as image, checkpoint,
resources, env, args, and replica envelopes.
