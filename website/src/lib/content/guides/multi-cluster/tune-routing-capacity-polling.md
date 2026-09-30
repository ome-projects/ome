---
title: Tune routing capacity polling
description: Cap a workload cluster's TrafficMap weight at the replicas that its endpoint reports it can serve, with an alpha poll set in the chart or for one InferenceService.
status: preview
since: v1.3
---

A capacity poll lets a workload cluster cap its share of an [InferenceService](../../concepts/serving/inference-services.md)'s traffic at what it can serve. The control plane asks the cluster how many of the service's replicas can serve now, and counts no more than that in the cluster's [TrafficMap](../../concepts/rollouts-and-traffic/traffic-map.md) weight. Use it when a cluster knows better than the control plane what can serve: for example, how many of its [engine and decoder](../../concepts/serving/inference-services.md#components) pairs work together during a rollout. A report can lower a cluster's weight but never raise it, and a failed poll leaves the weight as it would be without one.

Multi-cluster routing is alpha, still in development, and off by default. Its API, the capacity settings included, can change without notice. You turn it on with the `ome-resources` chart's values `ome.multicluster.enabled`, `ome.multicluster.role` and `ome.multicluster.config.routing.enabled`, as [Step 1](#step-1-set-the-operator-defaults) shows.

<div class="prerequisites" markdown>

- A control-plane cluster running OME v1.3 or later, installed with the `ome-resources` chart as the release `ome` in the namespace `ome`, with multi-cluster on in the control-plane role. You need Helm and `kubectl` access to it.
- Workload clusters registered on the control plane as [WorkloadClusters](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster). The examples call them `worker-a` and `worker-b`.
- The InferenceService `qwen3-0-6b` in the namespace `qwen3-0-6b` on the control plane, placed on both workload clusters with the `placement` block in Step 2's manifest. Check it with `kubectl get inferenceservice qwen3-0-6b -n qwen3-0-6b -o jsonpath='{.status.placement.phase}'`, which prints `Placed`.
- A capacity reporter for the InferenceService on each workload cluster, reachable from the controller's pods at the URL in [The report format](#the-report-format). OME doesn't provide one.

</div>

## How the poll caps a cluster's weight

A workload cluster that has admitted the InferenceService and reports an endpoint for it is a home, with an entry in the TrafficMap. OME polls a home every `period`, keeps its last `samples` readings, and takes the `quorum`-th lowest as its ceiling. With `quorum: 2`, the readings 4, 1 and 4 give a ceiling of 4, since one low reading can't set it alone, and 1, 4 and 1 give 1.

The entry's `capacity.allocated` becomes the lower of the admitted replicas and the ceiling, and [the weight follows from it](../../concepts/rollouts-and-traffic/traffic-map.md#how-ome-sets-the-weights). A ceiling of 0 takes all of the home's traffic away, even with a probe's `allFailedPolicy: PreserveTraffic`.

After a failed poll, the home has no ceiling, and goes back to its admitted replicas until `quorum` polls in a row succeed. So a network fault can leave a home with more traffic than it can serve for a while, but can't take its traffic away. A home also starts over with no readings when the controller restarts or changes leader, when you change a capacity setting, or when its endpoint changes.

## The report format

A poll is one request, with the poll's `method` and no body, to the entry's `endpoint` followed by the poll's `path`. The endpoint is the `status.url` of the service's copy on that workload cluster, so the poll URL depends on the cluster's ingress. For `worker-a`:

| Workload cluster's ingress | Poll URL |
| --- | --- |
| Gateway API with [per-service subdomains](../networking/gateway-host-schemes.md#per-service-subdomains) | `http://qwen3-0-6b.qwen3-0-6b.worker-a.example.com/capacity` |
| Gateway API with the default [shared host](../networking/gateway-host-schemes.md#shared-host-with-a-path-prefix) | `http://llm.worker-a.example.com/qwen3-0-6b/qwen3-0-6b/capacity` |
| Kubernetes Ingress | `http://qwen3-0-6b.qwen3-0-6b.worker-a.example.com:8080/capacity`, on the engine Service's port, not your Ingress controller's |

OME's HTTPRoute or Ingress sends all paths under the endpoint to the model server, which answers the poll with a 404. So add a route of your own that sends the poll URL to your reporter. For an HTTPRoute on the same Gateway, the Gateway API gives your longer path precedence over OME's. For a separate Ingress, precedence depends on your Ingress controller.

Your reporter answers with status 200 and a JSON body in the `Report` format, the only one that OME reads:

```json
{"servable": 4, "observedAt": "2026-09-28T10:00:00Z"}
```

| Field | What it holds |
| --- | --- |
| `servable` | Required. How many replicas the home can serve now, as a whole number, 0 or more. For separate engine and decoder components, count the pairs that work together. |
| `observedAt` | Required. When the reporter worked out the count, in RFC 3339 with a time zone. A report fails once it's `maxAge` old, or when it's from the future. |

- Count replicas, not utilization or request slots: OME compares the count with the admitted replicas.
- Answer at the path itself, with no login: the poll sends no credentials, and OME doesn't follow redirects.
- Keep the reporter's clock in step with the control plane's.

## Step 1: Set the operator defaults

Set the poll for all routed InferenceServices under `ome.multicluster.config.routing.capacity`, in your `ome-resources` values file. Do this once they all have reporters: a service without one falls back on all its homes, and keeps its `CapacityFallback` condition `True`. To poll only some services, leave the block out, and give each a poll as in [Step 2](#step-2-set-a-poll-for-one-service).

This example turns routing on, with the poll from the chart's comments:

```yaml title="values.yaml"
ome:
  multicluster:
    enabled: true
    role: control-plane
    config:
      routing:
        enabled: true
        capacity:
          path: /capacity
          method: GET
          format: Report
          samples: 10
          quorum: 2
          period: 10s
          timeout: 3s
          maxAge: 30s
```

Keep your other values, such as a [routing health probe](routing-health-probes.md), in the same file: `helm upgrade` resets the values that aren't in it to the chart's defaults.

| Field | What it sets |
| --- | --- |
| `path` | The path added to a home's endpoint, starting with `/`. Setting it turns the poll on. |
| `method` | `GET` or `POST`. |
| `format` | `Report`. |
| `options` | Leave it out: `Report` takes none. |
| `samples` | How many readings to keep for each home. At least 1. |
| `quorum` | Which reading, from the lowest, sets the ceiling. At least 1, and at most `samples`. |
| `period` | How often to poll, as a duration such as `10s`. |
| `timeout` | How long a poll can take. Shorter than `period`. |
| `maxAge` | How long a reading counts, from its `observedAt`. At least `quorum` times `period`. |

Once you set `path`, every field but `options` is required, or the controller doesn't start: see [The controller rollout doesn't finish](#the-controller-rollout-doesnt-finish).

To choose the values:

- Raise `quorum` when successive polls can reach different reporters behind a load balancer. A drop then applies only when `quorum` readings agree, after at least `quorum` polls.
- Give `maxAge` room above `quorum` times `period` for the time that the reporter holds a figure before it answers. A reporter that freezes loses its home's ceiling within `maxAge`.

Upgrade the release with the chart version that you already run, 1.3.0 or later:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm reports that the release `ome` has been upgraded, and the controller restarts with the new settings. Check that it's back up:

```bash
kubectl rollout status deployment ome-controller-manager -n ome
```

When the rollout finishes, kubectl prints:

```output
deployment "ome-controller-manager" successfully rolled out
```

## Step 2: Set a poll for one service

Give one InferenceService its own poll in its `spec.routing.capacity`, with or without a poll in the chart. It replaces the chart's poll as a whole, so set every field but `options`. For endpoints that spread polls across several reporters, this poll keeps four readings, needs three to agree before it lowers a ceiling, and lets a reading count for four periods:

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
    minReplicas: 2
    maxReplicas: 2
  placement:
    mode: All
    clusterSelector: "metadata.name in (worker-a,worker-b)"
  routing:
    capacity:
      path: /capacity
      method: GET
      format: Report
      samples: 4
      quorum: 3
      period: 10s
      timeout: 3s
      maxAge: 40s
```

The API server rejects a poll that's missing a field, with the message `an enabled capacity poll must specify path, method, format, period, timeout, samples, quorum, and maxAge`. It accepts some other mistakes: see [The TrafficMap doesn't pick up the poll](#the-trafficmap-doesnt-pick-up-the-poll).

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b configured
```

The new settings start the homes over with no readings, so they get their ceilings from the third poll on, about 20 seconds later.

## Step 3: Watch the capacity state

Check the TrafficMap, which has the InferenceService's name and namespace. Its `CapacityFallback` condition sums up the homes:

```bash
kubectl get trafficmap qwen3-0-6b -n qwen3-0-6b \
  -o 'custom-columns=FALLBACK:.status.conditions[?(@.type=="CapacityFallback")].status,REASON:.status.conditions[?(@.type=="CapacityFallback")].reason,MESSAGE:.status.conditions[?(@.type=="CapacityFallback")].message'
```

```output
FALLBACK   REASON                      MESSAGE
False      EndpointCapacityAvailable   endpoint capacity is available for all 2 home(s)
```

While any home has no ceiling, it's `True` with the reason `EndpointCapacityUnavailable`: see [A home falls back to its admitted replicas](#a-home-falls-back-to-its-admitted-replicas). [Traffic map](../../concepts/rollouts-and-traffic/traffic-map.md#capacityfallback) lists its other reasons.

List each home's weight and capacity:

```bash
kubectl get trafficmap qwen3-0-6b -n qwen3-0-6b \
  -o jsonpath='{range .spec.entries[*]}{.cluster}{" "}{.weight}{" "}{.capacity.allocated}{" "}{.capacity.ready}{" "}{.capacity.source}{" "}{.capacity.reported}{"\n"}{end}'
```

When `worker-a` reports 3 and `worker-b` reports 1, and both have two ready replicas:

```output
worker-a 2 2 2 ControlPlane 3
worker-b 1 1 2 Endpoint 1
```

On `worker-b`, the ceiling of 1 lowers `allocated` from 2 to 1, so `source` is `Endpoint`. On `worker-a`, the ceiling of 3 is above the two admitted replicas, so `source` stays `ControlPlane`. Its `reported` value is 3, which a failed poll would leave empty. The weights 2 and 1 give `worker-a` two thirds of the traffic.

OME leaves out `allocated` and `ready` when they're 0, so a missing number means 0. [Traffic map](../../concepts/rollouts-and-traffic/traffic-map.md#read-a-trafficmap) describes the fields.

While the poll is on, the gauge `ome_trafficmap_capacity_fallback_homes`, with the labels `namespace` and `trafficmap`, counts the homes without a ceiling. Alert when it stays above 0.

[`kubectl ome placement endpoint`](../../reference/kubectl-ome/placement.md#endpoint) shows the condition and a code for each home's fallback reason and, with `-o wide`, its capacity source. The command reads alpha features.

## Turn the poll off

To turn the poll off for one InferenceService, whatever the chart sets, replace the `capacity` block in `isvc.yaml` with `disabled: true`:

```yaml
spec:
  routing:
    capacity:
      disabled: true
```

Apply the file again:

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b configured
```

OME stops polling the service's homes, and `CapacityFallback` turns `False` with the reason `CapacityPollingDisabled`. Don't use `spec.routing.enabled: false` for this: it takes the service out of routing, and OME deletes its TrafficMap.

To stop the chart's poll, remove the `capacity` block from your values file, and run `helm upgrade` again. OME keeps polling for InferenceServices that set their own `spec.routing.capacity`.

## Troubleshooting

### The controller rollout doesn't finish

With an unusable multi-cluster setting, the new controller pods exit when they start, and `kubectl rollout status` fails with `deployment "ome-controller-manager" exceeded its progress deadline`. Find the setting in the controller's log:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'invalid multi-cluster configuration'
```

The error names the setting. For example:

- A period without a unit gives `routing.capacity.period: "10" is not a duration (use forms like "30s", "5m")`.
- A timeout as long as the period gives `routing.capacity.timeout (10s) must be shorter than routing.capacity.period (10s)`.

Fix the value in your values file, and run `helm upgrade` again.

### The TrafficMap doesn't pick up the poll

`kubectl apply` accepts some mistakes in `spec.routing.capacity` that OME refuses later. Until you fix them, OME stops polling and probing the service's homes, and [its TrafficMap stops updating](../../concepts/rollouts-and-traffic/traffic-map.md#where-it-comes-from): it keeps its last weights, and `Published` turns `False` with the reason `InvalidOwner`. A new InferenceService gets no TrafficMap. A [probe](routing-health-probes.md) error has the same effect. Find the error in the controller's log:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep -E 'validate routing policy|resolve (capacity|probe) policy'
```

For example:

| Problem | Error |
| --- | --- |
| `maxAge` is shorter than `quorum` times `period` | `validate routing policy: spec.routing.capacity.maxAge (20s) must span at least quorum (3) period intervals` |
| A format other than `Report`, such as `report` | `resolve capacity policy: spec.routing.capacity.format "report" is not registered in this build (available: [Report])` |

Fix the field, and apply `isvc.yaml` again.

### A home falls back to its admitted replicas

When the `CapacityFallback` condition is `True`, its message counts the homes without a ceiling, as in `1 of 2 home(s) are using control-plane capacity; inspect spec.entries[].capacity.fallbackReason`. List each home's reason:

```bash
kubectl get trafficmap qwen3-0-6b -n qwen3-0-6b \
  -o jsonpath='{range .spec.entries[*]}{.cluster}{": "}{.capacity.fallbackReason}{"\n"}{end}'
```

When `worker-b`'s poll reaches the model server instead of your reporter, and `worker-a` has its ceiling:

```output
worker-a:
worker-b: capacity endpoint returned HTTP 404
```

The reason says what to fix:

| `fallbackReason` | What it means |
| --- | --- |
| `no capacity report yet`, or `capacity report awaiting quorum (1/3 accepted samples)` | Nothing's wrong. The home gets its ceiling once `quorum` polls in a row succeed. |
| `capacity report is stale (at least configured maximum age 40s)`, or `capacity report awaiting quorum after stale samples expired (2/3 accepted samples)` | The reporter updates less often than `maxAge`, has frozen, or has a clock that runs behind. Fix it, or raise `maxAge`. |
| `capacity report timestamp is in the future` | The reporter's clock runs ahead of the controller's. |
| `capacity endpoint unreachable:`, then the error | A DNS, connection or TLS error, or no answer within `timeout`. Check the network path from the controller's pods, or raise `timeout`. |
| `capacity endpoint returned HTTP`, then the status | 404: the path doesn't reach your reporter, as [The report format](#the-report-format) explains. 3xx: OME doesn't follow redirects. 401 or 403: the poll sends no credentials. |
| `capacity response is not a valid report:`, then the error, `capacity report has no servable count`, `capacity report has no observedAt stamp`, or `capacity report is negative (-1)` | The body isn't the report that [The report format](#the-report-format) describes, or is empty, as with `method: HEAD`. A negative count isn't read as 0. |
| `reading capacity response:` or `capacity request did not complete:`, then the error | The body is too long, or the poll ran out of time. |
| `capacity target is invalid:`, `capacity request URL is invalid:` or `capacity request could not be built:`, then the error | OME can't build a request from the entry's `endpoint` and the `path`. |
| `capacity observation could not be submitted:`, then the error | The controller was starting or stopping, as in a restart. It clears on its own. |

## Next steps

- [Traffic map](../../concepts/rollouts-and-traffic/traffic-map.md): how OME builds the TrafficMap and works out each home's weight.
- [Configure routing health probes](routing-health-probes.md): set a cluster's weight to zero when its endpoint stops answering.
- [Weight traffic for heterogeneous clusters](weight-traffic-for-heterogeneous-clusters.md): count one cluster's replicas for more than another's.
- [Drain a workload cluster](drain-a-workload-cluster.md): hold a workload cluster at zero weight with `kubectl ome traffic drain`, whatever its capacity.
- [Publish a global endpoint](publish-a-global-endpoint.md): serve the InferenceService on one hostname that splits its traffic by the TrafficMap's weights.
