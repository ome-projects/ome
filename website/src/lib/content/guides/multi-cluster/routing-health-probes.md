---
title: Configure routing health probes
description: "Configure the alpha end-to-end probe that sets a workload cluster's TrafficMap weight to zero after repeated failures, for every InferenceService or for one."
status: preview
since: v1.3
---

A routing health probe takes traffic away from a workload cluster when requests stop getting through to it, even while its pods are Ready. The control plane sends a request to an [InferenceService](../../concepts/serving/inference-services.md)'s endpoint on each of its workload clusters. After enough failures in a row, it sets that cluster's weight in the service's [TrafficMap](../../concepts/rollouts-and-traffic/traffic-map.md) to zero, and the other clusters take its traffic. That catches what readiness misses: a broken ingress, DNS record, certificate or route. You set the probe for every routed InferenceService in the `ome-resources` chart, or for one service in its `spec.routing.probe`.

Multi-cluster routing is alpha, still in development, and off by default. Its API, the probe settings included, can change without notice. [Step 1](#step-1-set-a-probe-for-every-service) turns routing on along with the probe.

<div class="prerequisites" markdown>

- A control-plane cluster running OME v1.3 or later, installed with the `ome-resources` chart as the release `ome` in the namespace `ome`, with `ome.multicluster.enabled: true` and `ome.multicluster.role: control-plane`. You need Helm and `kubectl` access to it.
- Workload clusters registered on the control plane as [WorkloadClusters](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster). The examples call them `worker-a` and `worker-b`.
- The InferenceService `qwen3-0-6b` in the namespace `qwen3-0-6b` on the control plane, placed on both workload clusters. Step 2's manifest shows its `placement` block. `kubectl get inferenceservice qwen3-0-6b -n qwen3-0-6b -o jsonpath='{.status.placement.phase}'` prints `Placed`.
- Network access from the controller's pods on the control plane to each workload cluster's endpoint for the InferenceService.

</div>

## How the probe works

Every `period`, OME sends one request, with the probe's `method`, to the `endpoint` in each cluster's TrafficMap entry, followed by the probe's `path`. The endpoint is the `status.url` of the service's copy on that cluster, so the probe URL depends on the cluster's ingress, like the capacity poll URLs in [The report format](tune-routing-capacity-polling.md#the-report-format). On `worker-a`, with Gateway API and the default shared host, `/v1/models` gives `http://llm.worker-a.example.com/qwen3-0-6b/qwen3-0-6b/v1/models`. OME doesn't follow redirects. An attempt gets one of three results:

| Result | When |
| --- | --- |
| `Passing` | The status is in `acceptStatuses`. |
| `Failing` | The status is in `gateStatuses`, or the request fails: a DNS, connection or TLS error, or no full response within `timeout`. |
| `Unknown` | Any other status, such as a redirect. It leaves the counts and the gate as they are, so it never moves traffic. |

After `failureThreshold` `Failing` results in a row, OME gates the cluster: its weight drops to zero, and its entry's `healthy` turns `false`. After `successThreshold` `Passing` results in a row, OME lifts the gate and restores the weight. Until a cluster is gated, its weight follows its ready replicas, as it would without a probe.

A gated cluster stays gated when the controller restarts. Changing any probe setting lifts the gates and starts the counts over. A failing cluster then gets traffic until it fails `failureThreshold` times in a row again.

## Step 1: Set a probe for every service

Set a probe for all routed InferenceServices under `ome.multicluster.config.routing.probe` in your `ome-resources` values file. This example turns routing on and probes `/v1/models` every 10 seconds:

```yaml title="values.yaml"
ome:
  multicluster:
    enabled: true
    role: control-plane
    config:
      routing:
        enabled: true
        probe:
          path: /v1/models
          method: GET
          acceptStatuses: [200]
          gateStatuses: [404, 500, 502, 503, 504]
          period: 10s
          timeout: 3s
          failureThreshold: 3
          successThreshold: 2
          allFailedPolicy: PreserveTraffic
```

Keep your other values in the same file: `helm upgrade` resets the values that aren't in it to the chart's defaults.

| Field | What it sets |
| --- | --- |
| `path` | The path that OME appends to the cluster's endpoint, starting with `/`. Pick one that answers without a body or credentials, such as `/v1/models`. |
| `method` | `GET`, `HEAD` or `POST`. |
| `acceptStatuses` | The statuses that count as `Passing`. |
| `gateStatuses` | The statuses that count as `Failing`. It can't hold a status from `acceptStatuses`, or 401, 403 or 429, which mean that the endpoint is up but busy or wants credentials. |
| `period` | How often OME probes a cluster, such as `10s`. At least `1s`, the chart's `routing.observer.minPeriod`. |
| `timeout` | How long an attempt can take. Shorter than `period`. |
| `failureThreshold` | How many `Failing` results in a row gate a cluster. |
| `successThreshold` | How many `Passing` results in a row lift the gate. |
| `allFailedPolicy` | When all the clusters are gated, `PreserveTraffic` weights them as if there were no probe, and `Drain` leaves them at zero. See [When every cluster fails](#when-every-cluster-fails). |

With these values, a failing cluster is gated about 20 seconds after its first failure, and gets its weight back after two passes in a row.

The probe has no defaults, so set all nine fields. By default, the chart sets no probe. A missing or invalid field stops the controller from starting, as [The controller rollout doesn't finish](#the-controller-rollout-doesnt-finish) shows.

Upgrade the release with the chart version that you run. The probe needs 1.3.0 or later:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Until v1.3 is released, upgrade from [a checkout of `main`](../../getting-started/install.md#install-from-source) instead.

Helm reports that the release `ome` has been upgraded. It can also warn that it's ignoring the non-table value `[]` for `ome-resources.ome.multicluster.config.routing.probe`: that's the chart's empty default, so ignore it.

The controller restarts with the probe. Check that it's back up:

```bash
kubectl rollout status deployment ome-controller-manager -n ome
```

When the rollout finishes, kubectl prints:

```output
deployment "ome-controller-manager" successfully rolled out
```

## Step 2: Set a probe for one service

Give an InferenceService its own probe in `spec.routing.probe` when its runtime answers on another path or needs another `allFailedPolicy`, or when the chart sets no probe. This manifest probes `qwen3-0-6b` every 30 seconds, and drains it when all its clusters fail:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: qwen3-0-6b
  namespace: qwen3-0-6b
spec:
  model:
    name: qwen3-0-6b
  runtime:
    name: srt-qwen3-0-6b
  engine:
    minReplicas: 1
    maxReplicas: 1
  placement:
    mode: All
    clusterSelector: "metadata.name in (worker-a,worker-b)"
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

`spec.routing.probe` replaces the chart's probe as a whole, so set all nine fields: the API server rejects a probe that's missing one.

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b configured
```

## Step 3: Check the probe state

OME records the probe results on the InferenceService's TrafficMap, which has the same name and namespace. List each cluster's weight, health and last probe result:

```bash
kubectl get trafficmap qwen3-0-6b -n qwen3-0-6b \
  -o jsonpath='{range .spec.entries[*]}{.cluster}{" "}{.weight}{" "}{.healthy}{" "}{.probe.result}{" "}{.probe.message}{"\n"}{end}'
```

When `worker-b` has answered 503 three times in a row, and `worker-a` passes:

```output
worker-a 1 true Passing HTTP 200
worker-b 0 false Failing HTTP 503
```

With both clusters passing, both lines end in `true Passing HTTP 200`.

`weight` is the cluster's share of the traffic, and `healthy` is `true` when the cluster has ready replicas and isn't gated. The entry's `probe` block holds:

| Field | What it holds |
| --- | --- |
| `result` | The last attempt's result: `Passing`, `Failing` or `Unknown`. |
| `message` | What the last attempt got: the status, as in `HTTP 503`, or the error. |
| `gated` | `true` while the cluster is gated, even after `result` turns `Passing`, until `successThreshold` passes in a row. |
| `consecutiveFailures` | The cluster's `Failing` results in a row. |
| `lastProbeTime` | When the last attempt finished. If it stops moving, OME isn't probing the cluster. |

`gated` and `consecutiveFailures` appear only while they're set. An entry gets a `probe` block once OME has probed its cluster. See [`TrafficMapProbe`](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-TrafficMapProbe) for all its fields.

[`kubectl ome placement endpoint`](../../reference/kubectl-ome/placement.md#endpoint), which reads the alpha multi-cluster features, also shows the clusters' weights, recorded probe results and gates.

## When every cluster fails

When all the service's clusters are gated, OME follows the probe's `allFailedPolicy`:

- `PreserveTraffic`: OME ignores the gates, and weights the clusters as it would without a probe. The entries still show `healthy: false`. The `Routable` condition stays `True`, with the reason `AllHomesProbeFailed`.
- `Drain`: all weights stay at zero. The `Routable` condition turns `False`, with the reason `AllHomesProbeFailed`.

A failure on the control plane's side, such as its egress or DNS, fails all the clusters at once, while they may be serving normally. `PreserveTraffic` keeps traffic flowing through such a failure. Choose `Drain` only when sending traffic to clusters that all fail their probe is worse than sending none.

Read the outcome in the `Routable` condition. With the `Drain` probe from Step 2, when both clusters are gated:

```bash
kubectl get trafficmap qwen3-0-6b -n qwen3-0-6b \
  -o 'custom-columns=ROUTABLE:.status.conditions[?(@.type=="Routable")].status,REASON:.status.conditions[?(@.type=="Routable")].reason,MESSAGE:.status.conditions[?(@.type=="Routable")].message'
```

```output
ROUTABLE   REASON                MESSAGE
False      AllHomesProbeFailed   2 home(s) have conclusive probe failures; all route weights are zero
```

With `PreserveTraffic`, the message is `2 home(s) have conclusive probe failures; preserving traffic among homes with ready capacity`.

## Turn the probe off

To stop probing one InferenceService, whatever the chart sets, replace the `probe` block in `isvc.yaml` with `disabled: true`:

```yaml
spec:
  routing:
    probe:
      disabled: true
```

Apply the file again:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b configured
```

OME then stops probing, lifts the gates, and leaves the `probe` block out of the entries. To take the service out of routing instead, and delete its TrafficMap, set `spec.routing.enabled: false`.

To go back to the chart's probe, delete `probe` from `spec.routing`, and apply the file again. To stop the chart's probe, delete its block from your values file, and run the `helm upgrade` command from Step 1 again. InferenceServices with their own `spec.routing.probe` keep it.

## Troubleshooting

### The controller rollout doesn't finish

With an unusable multi-cluster setting, the new controller pods exit when they start and keep restarting, and `kubectl rollout status` ends with `deployment "ome-controller-manager" exceeded its progress deadline`. Find the setting in the controller's logs:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'multi-cluster configuration'
```

The error starts with `invalid multi-cluster configuration:` and names the setting, as in `routing.probe.timeout (10s) must be shorter than routing.probe.period (10s)`. For a probe block that leaves out a string field, it starts with `load multi-cluster configuration: unable to parse multicluster config json`. Fix the value in your values file, and run `helm upgrade` again.

### The TrafficMap doesn't pick up the probe

`kubectl apply` accepts a status that's in both `acceptStatuses` and `gateStatuses`, but OME refuses it afterwards. Until you fix it, OME stops probing the service's clusters, and [its TrafficMap stops updating](../../concepts/rollouts-and-traffic/traffic-map.md#where-it-comes-from): it keeps its last weights and probe results. A new InferenceService gets no TrafficMap. Find the error in the controller's logs:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'validate routing policy'
```

The error reads like `validate routing policy: spec.routing.probe status 503 is in both acceptStatuses and gateStatuses`. Take the status out of one list, and apply `isvc.yaml` again.

### A cluster is gated

Read the cluster's `probe.message` with the command in [Step 3](#step-3-check-the-probe-state):

| Message | What to check |
| --- | --- |
| `HTTP 404` | The path is wrong, or the cluster's route is missing. Compare `path` with the entry's `endpoint`. |
| Another status in `gateStatuses`, such as `HTTP 503` | The cluster's gateway, route or engine is failing. Fix the cluster. |
| `transport error:` or `response body error:`, then the error | The request failed: a DNS error, a refused connection, an untrusted certificate or a timeout. Check that the controller's pods can reach the endpoint, and raise `timeout` if it's slow. The probe trusts the public certificate authorities in the controller image, and has no setting for a private one. |

When all the clusters fail at once, check the controller's egress first.

A cluster whose result stays `Unknown`, with a message such as `HTTP 301 (not in accept or gate list)`, is never gated. Add the status to one of the lists, or choose a path that answers with a listed status.

## Next steps

- [Traffic map](../../concepts/rollouts-and-traffic/traffic-map.md): how OME builds the TrafficMap and works out the clusters' weights.
- [Tune routing capacity polling](tune-routing-capacity-polling.md): cap a cluster's weight at the replicas that its endpoint reports it can serve.
- [Drain a workload cluster](drain-a-workload-cluster.md): hold a workload cluster at zero weight with `kubectl ome traffic drain`, whatever its probe says.
