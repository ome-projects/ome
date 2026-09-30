---
title: Set up alerting
description: "Enable the ome-resources chart's five P0 PrometheusRule alerts, which are off by default, and tune their thresholds and selectors."
since: v1.3
---

The `ome-resources` chart ships five Prometheus alerts for OME failures that should page someone at once. They cover a controller that's down or stalled, a failing admission webhook, an [InferenceService](../../concepts/serving/inference-services.md) with no healthy backend, and rollouts that fail across many InferenceServices. They're off by default: turn them on once your Alertmanager routes their `severity` label to someone.

As shipped, [OMEWebhookFailing](#omewebhookfailing) never fires, and [OMEServiceUnavailable](#omeserviceunavailable) fires only if your router exports the series that it reads.

<div class="prerequisites" markdown>

- The `ome-resources` chart at version 1.3.0 or later, installed as the release `ome` in the namespace `ome`. See [Install OME](../../getting-started/install.md).
- The values file that you installed `ome-resources` with. If you don't have it, `helm get values ome -n ome` prints the values that you set.
- The [Prometheus Operator](https://github.com/prometheus-operator/prometheus-operator) and a Prometheus that it manages, which sends its alerts to an Alertmanager. The examples use kube-prometheus-stack, installed as the release `kube-prometheus-stack` in the namespace `monitoring`.

</div>

!!! warning "The bundled Prometheus doesn't evaluate the alerts"
    The Prometheus that the chart runs by default, `ome-prometheus`, loads no rules and has no Alertmanager. The alerts need a Prometheus that the Prometheus Operator manages. For the bundled Prometheus, see [Collect metrics](metrics.md).

## Step 1: Turn on the alerts

A Prometheus uses the chart's PrometheusRule and ServiceMonitor only when its selectors match their labels and the namespace `ome`. Print the selectors of each Prometheus:

```bash
kubectl get prometheus -A -o custom-columns="NAMESPACE:.metadata.namespace,NAME:.metadata.name,\
RULES:.spec.ruleSelector,RULE-NAMESPACES:.spec.ruleNamespaceSelector,\
MONITORS:.spec.serviceMonitorSelector,MONITOR-NAMESPACES:.spec.serviceMonitorNamespaceSelector"
```

Read the output this way:

| Output | In `RULES` and `MONITORS` | In `RULE-NAMESPACES` and `MONITOR-NAMESPACES` |
| --- | --- | --- |
| `<none>` | Selects nothing | Selects only the Prometheus's own namespace |
| `map[]` | Selects every object | Selects every namespace |
| A label selector | Selects objects with its labels | Selects namespaces with its labels |

By default, a kube-prometheus-stack Prometheus selects objects by the label `release`: `map[matchLabels:map[release:kube-prometheus-stack]]`. Add the labels that your Prometheus selects to both objects with `additionalLabels`, and turn both on:

```yaml title="values.yaml"
prometheusRule:
  enabled: true
  additionalLabels:
    release: kube-prometheus-stack
  runbookUrlBase: "https://runbooks.example.com/ome"
serviceMonitor:
  enabled: true
  additionalLabels:
    release: kube-prometheus-stack
```

`runbookUrlBase` is optional: see [Runbook links](#runbook-links). For the ServiceMonitor's other settings, see [Key values](metrics.md#key-values) in Collect metrics.

`helm upgrade` resets every value that isn't in the file to the chart's default, so keep your other install values in it. Upgrade the release with the chart version that you already run, 1.3.0 here:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm reports that the release `ome` has been upgraded. It adds the PrometheusRule `ome-p0-alerts` and the ServiceMonitor `ome-controller-manager`, and leaves the controller's pods as they are.

## Step 2: Check the rules

List the alerts in the PrometheusRule:

```bash
kubectl get prometheusrule ome-p0-alerts -n ome \
  -o jsonpath='{range .spec.groups[*].rules[*]}{.alert}{"\n"}{end}'
```

```output
OMEControllerDown
OMEWebhookFailing
OMEServiceUnavailable
OMEMassRolloutFailure
OMEReconcileStalled
```

To check that Prometheus loaded the rules, forward a local port to it:

```bash
kubectl port-forward svc/prometheus-operated 9090:9090 -n monitoring
```

```output
Forwarding from 127.0.0.1:9090 -> 9090
Forwarding from [::1]:9090 -> 9090
```

Open `http://localhost:9090/alerts` in a browser. The page lists the group `ome.p0` with the five alerts. If the group is missing, see [Prometheus doesn't load the rules](#prometheus-doesnt-load-the-rules).

Then check that Prometheus scrapes the controller: run the query `up{pod=~"ome-controller-manager.*"}` in the Prometheus UI. It returns one series per controller replica, with the value `1`. If it returns nothing, OMEControllerDown fires after 5 minutes: see [OMEControllerDown fires as soon as you turn it on](#omecontrollerdown-fires-as-soon-as-you-turn-it-on).

## The five alerts

Each alert fires when its condition holds for the time in the table. Route the alerts in Alertmanager on their labels: `tier: p0`, the `component` in the table, and `severity`, which is `critical` by default.

| Alert | Component | Holds for | Condition |
| --- | --- | --- | --- |
| [OMEControllerDown](#omecontrollerdown) | `control-plane` | 5 minutes | No controller replica is scraped, or none holds the leader Lease. |
| [OMEWebhookFailing](#omewebhookfailing) | `webhook` | 5 minutes | More than 10% of admission requests get a 5xx status. |
| [OMEServiceUnavailable](#omeserviceunavailable) | `router` | 5 minutes | An InferenceService has backends registered with its routers, but no router sees a healthy one. |
| [OMEMassRolloutFailure](#omemassrolloutfailure) | `coordination` | 15 minutes | More than half of the rollout groups that finished in the last 15 minutes failed, in at least 3 InferenceServices. |
| [OMEReconcileStalled](#omereconcilestalled) | `control-plane` | 10 minutes | The controller's work queues grow while it reconciles almost nothing. |

### OMEControllerDown

No controller replica is working, so no InferenceService deploys, scales or rolls out. The alert fires when Prometheus can't scrape any replica, or when the replicas run but none holds the leader Lease.

When it fires:

1. Check that the pods run: `kubectl get pods -n ome -l control-plane=ome-controller-manager`. If they run and you've just turned the alerts on, see [OMEControllerDown fires as soon as you turn it on](#omecontrollerdown-fires-as-soon-as-you-turn-it-on).
2. Read every replica's log: `kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1`. To find the leader, see [Tune leader election](configure-the-controller.md#tune-leader-election).
3. If a pod's `RESTARTS` count went up, read the run that ended: `kubectl logs ome-controller-manager-7c9d5b8f4-kx2mq -n ome --previous`, with your pod's name. A leader that can't renew the Lease logs `Failed to start manager`, with the error `leader election lost`, and restarts.

```text title="Expression"
absent(up{pod=~"ome-controller-manager.*"}) == 1
or max(up{pod=~"ome-controller-manager.*"}) == 0
or max(leader_election_master_status{pod=~"ome-controller-manager.*"}) == 0
```

### OMEWebhookFailing

The alert counts the admission requests that OME's webhooks answer with a 5xx status. The webhooks never answer that way, so as shipped the alert never fires. To leave it out, set `prometheusRule.rules.webhookFailing.enabled` to `false`.

While the webhook can't be reached, the API server rejects InferenceService creates and updates. If that's because no controller replica runs, [OMEControllerDown](#omecontrollerdown) fires.

```text title="Expression"
sum(rate(controller_runtime_webhook_requests_total{pod=~"ome-controller-manager.*",code=~"5.." }[5m]))
/
sum(rate(controller_runtime_webhook_requests_total{pod=~"ome-controller-manager.*"}[5m]))
> 0.1
```

### OMEServiceUnavailable

An InferenceService has backends registered with its routers, but no router sees a healthy one, so requests to it fail.

The alert reads `router_pool_healthy_count` and `router_pool_total_count`, with the labels `namespace` and `inferenceservice`. OME doesn't export them, so the alert fires only if your router does.

When it fires, check the engine pods before the routers: a backend that routers drop is usually an engine that stopped passing its health check. See [Troubleshoot an InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md).

```text title="Expression"
(max by (namespace, inferenceservice) (router_pool_healthy_count{}) == 0)
and
(max by (namespace, inferenceservice) (router_pool_total_count{}) > 0)
```

### OMEMassRolloutFailure

More than half of the rollout groups that finished in the last 15 minutes failed, in at least 3 InferenceServices. Failures across many workloads at once point at the control plane, or at something the workloads share.

The alert counts the blue-green and rolling-update [rollout groups](../../concepts/rollouts-and-traffic/rollout-groups.md) of [OMENative](../../concepts/omenative/overview.md) components. Canary groups don't count. It counts InferenceServices by name, so two with the same name in different namespaces count once.

The InferenceService's namespace is in the label `exported_namespace`, because `namespace` holds the controller's. When the alert fires, list the groups that failed with this query in the Prometheus UI:

```text
sum by (exported_namespace, isvc, group) (increase(ome_omenative_rollout_group_transition_total{to="Failed"}[15m])) > 0
```

For example, `{exported_namespace="demo", group="0", isvc="qwen2-5-7b"}` is group `0` of the InferenceService `qwen2-5-7b`, in the namespace `demo`. Check the rollouts of a few of the listed InferenceServices, as in [A change doesn't roll out](../troubleshoot/troubleshoot-an-inferenceservice.md#step-4-check-the-rollout) in Troubleshoot an InferenceService.

```text title="Expression"
(
  sum(rate(ome_omenative_rollout_group_transition_total{pod=~"ome-controller-manager.*",to="Failed" }[15m]))
  /
  sum(rate(ome_omenative_rollout_group_transition_total{pod=~"ome-controller-manager.*",to=~"Idle|Staged|Failed" }[15m]))
  > 0.5
)
and
(
  count(
    sum by (namespace, isvc) (
      rate(ome_omenative_rollout_group_transition_total{pod=~"ome-controller-manager.*",to="Failed" }[15m])
    ) > 0
  ) >= 3
)
```

### OMEReconcileStalled

The controller is up and looks healthy, but it's stuck: its work queues grow while it finishes almost no reconciles. The alert fires when these conditions hold for 10 minutes:

- More than `minQueueDepth` items wait in the work queues.
- The controller finished at most `maxReconcileRate` reconciles per second over the last 10 minutes, counting failed ones.
- The queue depth rose over the last 10 minutes.

The alert adds up the work of all OME's controllers, so one stuck controller fires it only when the others are nearly idle too.

When it fires, read every replica's log for a blocked call to the API server or a reconcile that never finishes: `kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1`. Then restart the controller: `kubectl rollout restart deployment/ome-controller-manager -n ome`. If it's slow rather than stuck, see [Tell whether the manager is throttled](configure-the-controller.md#tell-whether-the-manager-is-throttled).

```text title="Expression"
(sum(workqueue_depth{pod=~"ome-controller-manager.*"}) > 1)
and
(sum(rate(controller_runtime_reconcile_total{pod=~"ome-controller-manager.*"}[10m])) <= 0.01)
and
(deriv(sum(workqueue_depth{pod=~"ome-controller-manager.*"})[10m:]) > 0)
```

## Settings

All the values are under `prometheusRule`.

| Value | Default | Description |
| --- | --- | --- |
| `enabled` | `false` | Renders the PrometheusRule `ome-p0-alerts`. |
| `additionalLabels` | `{}` | Labels for the PrometheusRule object, not the alerts, for a Prometheus's `ruleSelector` to match. |
| `interval` | `30s` | How often Prometheus evaluates the alerts. |
| `severity` | `critical` | The `severity` label of all five alerts. |
| `runbookUrlBase` | `""` | The start of each alert's `runbook_url` annotation. |
| `selectors.controlPlane` | `pod=~"ome-controller-manager.*"` | PromQL label matchers for the controller's series. |
| `selectors.router` | `""` | PromQL label matchers for the router series. Empty matches every router. |

Each alert has its own values under `rules.<key>`. `enabled` is `true` by default, and `duration` is how long the condition holds before the alert fires.

| Alert | Key | `duration` | Thresholds |
| --- | --- | --- | --- |
| OMEControllerDown | `controllerDown` | `5m` | None |
| OMEWebhookFailing | `webhookFailing` | `5m` | `ratioThreshold: 0.1` |
| OMEServiceUnavailable | `serviceUnavailable` | `5m` | None |
| OMEMassRolloutFailure | `massRolloutFailure` | `15m` | `ratioThreshold: 0.5` and `minInferenceServices: 3` |
| OMEReconcileStalled | `reconcileStalled` | `10m` | `minQueueDepth: 1` and `maxReconcileRate: 0.01` |

Keep `rules.controllerDown.duration` well above a minute. After the leader's pod goes away, no replica leads until the Lease expires, about a minute later with the chart's settings.

### Runbook links

Each alert's `runbook_url` annotation is `runbookUrlBase`, then `#` and the alert's name in lowercase. With the value from Step 1, OMEControllerDown links to `https://runbooks.example.com/ome#omecontrollerdown`. Give your runbook a section for each of the five anchors, or set `runbookUrlBase` to this page's URL: its alert headings have the same ids.

### Scope the alerts to one cluster

When one Prometheus holds several clusters' series, the default selectors match every cluster's controller, and the alerts combine them. OMEControllerDown, for example, fires only when no cluster has a working controller. Add a label that tells the clusters apart to both selectors. Your value replaces the default, so keep the pod matcher:

```yaml title="values.yaml"
prometheusRule:
  selectors:
    controlPlane: 'pod=~"ome-controller-manager.*",cluster="prod-us-1"'
    router: 'cluster="prod-us-1"'
```

Don't set `selectors.controlPlane` to an empty string: the chart would then render invalid PromQL.

## Troubleshooting

### Helm can't find the PrometheusRule kind

`helm upgrade` fails with an error that includes `no matches for kind "PrometheusRule" in version "monitoring.coreos.com/v1"`, or the same for `ServiceMonitor`. The cluster doesn't have the Prometheus Operator's CRDs. Install the Prometheus Operator before you turn on the alerts, or leave `prometheusRule.enabled` and `serviceMonitor.enabled` at `false`.

### Prometheus doesn't load the rules

The PrometheusRule `ome-p0-alerts` exists, but the Prometheus's alerts page doesn't list the group `ome.p0`. Print the rule's labels, and compare them with the selectors from [Step 1](#step-1-turn-on-the-alerts):

```bash
kubectl get prometheusrule ome-p0-alerts -n ome --show-labels
```

If they don't match, change `additionalLabels` and upgrade again, add the labels that the namespace selector wants to the namespace `ome`, or change the Prometheus's selectors.

### OMEControllerDown fires as soon as you turn it on

Five minutes after you turn the alerts on, OMEControllerDown fires while the controller runs. Prometheus doesn't scrape the controller, or its series lack a `pod` label that matches `selectors.controlPlane`. Check that the ServiceMonitor exists:

```bash
kubectl get servicemonitor ome-controller-manager -n ome --show-labels
```

If kubectl prints a `NotFound` error, set `serviceMonitor.enabled` to `true` and upgrade again. If it shows the ServiceMonitor, check that the Prometheus's `serviceMonitorSelector` and `serviceMonitorNamespaceSelector` match its labels and the namespace `ome`, as in [Step 1](#step-1-turn-on-the-alerts). If your Prometheus scrapes the controller another way, set `selectors.controlPlane` to match the label that holds the pod's name.

## Clean up

To remove the alerts, set `prometheusRule.enabled` and `serviceMonitor.enabled` to `false` in your values file. Keep `serviceMonitor.enabled` if something else, such as a dashboard, uses the controller's series. Then upgrade the release as in Step 1:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm deletes the PrometheusRule `ome-p0-alerts`, and the ServiceMonitor `ome-controller-manager` if you turned it off. Check that the rule is gone:

```bash
kubectl get prometheusrule ome-p0-alerts -n ome
```

```output
Error from server (NotFound): prometheusrules.monitoring.coreos.com "ome-p0-alerts" not found
```

## Next steps

- [Collect metrics](metrics.md): tune the Prometheus that the chart deploys, and scrape OME's metrics.
- [Troubleshoot an InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md): find out why an InferenceService isn't ready or its rollout is stuck.
- [Configure the controller](configure-the-controller.md): tune leader election and reconcile throughput, which OMEControllerDown and OMEReconcileStalled watch.
