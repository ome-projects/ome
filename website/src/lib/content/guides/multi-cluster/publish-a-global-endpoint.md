---
title: Publish a global endpoint
description: Give an alpha multi-cluster InferenceService one global hostname that splits its traffic across workload clusters by its TrafficMap's weights.
status: preview
since: v1.3
---

A global endpoint gives a multi-cluster [InferenceService](../../concepts/serving/inference-services.md) one hostname for its clients, whichever workload clusters serve it. OME writes the weights of the service's [TrafficMap](../../concepts/rollouts-and-traffic/traffic-map.md) into a Gateway API HTTPRoute on the control-plane cluster, and your Gateway there splits requests across the workload clusters by those weights. The examples publish the InferenceService `chat`, in the namespace `prod`, at `chat.prod.global.example.com`.

Multi-cluster serving, routing and publishing are alpha, still in development, and off by default. Their APIs and settings can change without notice.

<div class="prerequisites" markdown>

- A control-plane cluster running a v1.3 development source build, installed with the `ome-resources` chart as the release `ome` in the namespace `ome`. Start with [Register a workload cluster](register-a-workload-cluster.md) for matching images, CRDs, role profiles and the stable control-plane ID. Then enable `ome.multicluster.config.routing.enabled=true`, as in [Turn on routing](../../concepts/rollouts-and-traffic/traffic-map.md#turn-on-routing).
- Helm, and `kubectl` with the control-plane cluster as its current context.
- The Gateway API CRDs there, and an implementation that accepts `ExternalName` Services as backends and a `URLRewrite` filter on a single backend. The Gateway API leaves both to each implementation, and advises against `ExternalName` backends, so check yours.
- A Gateway for the global hostnames, here `global` in `gateway-system`, with an HTTP listener on port 80 whose hostname is `*.global.example.com` or unset. DNS records, such as a wildcard, point the hostnames at it, and the listener's `allowedRoutes` admits routes from `prod`: by default, a listener admits only its Gateway's namespace.
- Two registered [WorkloadClusters](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-WorkloadCluster), here `worker-a` and `worker-b`. Repeat the [registration workflow](register-a-workload-cluster.md) with distinct member credentials and names, and verify placement before configuring global routing.
- On each, OME's ingress creation turned on, with Gateway API routes, per-service subdomains and a domain of its own, as in [Switch schemes cluster-wide](../networking/gateway-host-schemes.md#switch-schemes-cluster-wide). The control-plane Gateway has to reach `chat` there on port 80, at `http://chat.prod.worker-a.example.com/` and `http://chat.prod.worker-b.example.com/`. If it can't resolve those hostnames, see [EndpointSlice fallback](#endpointslice-fallback).
- The InferenceService `chat` in `prod`, with no router or decoder, and [eligible for routing](../../concepts/rollouts-and-traffic/traffic-map.md#turn-on-routing). Its [`spec.placement.mode`](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-PlacementSpec) is `All` or `Split`, because the default, `Single`, places it on one workload cluster. Once it's placed on both workload clusters, `kubectl get trafficmap chat -n prod -o jsonpath='{.spec.entries[*].cluster}'` prints `worker-a worker-b`.

</div>

## How publishing works

For each TrafficMap, the controller's built-in publisher, `gatewayapi`, writes these objects on the control-plane cluster, by default in the InferenceService's namespace, with [labels](../../reference/api/labels-and-annotations.md#endpoint-publisher-labels) such as `ome.io/placement-endpoint-isvc: chat`:

- An HTTPRoute named after the InferenceService, here `chat-global`, attached to the `globalGateway` Gateway. It matches the global hostname at every path, and has a backend for each TrafficMap entry, at the entry's weight, 0 included.
- For each backend, an `ExternalName` Service named `tmb-` and a hash. It points at the host of the URL that the entry's workload cluster reports for the service, such as `chat.prod.worker-a.example.com`.

The route keeps the client's path, so `http://chat.prod.global.example.com/v1/models` reaches the engine as sent. A service with a router or a decoder belongs on the [shared host](../networking/gateway-host-schemes.md#shared-host-with-a-path-prefix) instead, as [Per-service subdomains](../networking/gateway-host-schemes.md#per-service-subdomains) warns, and its clients add its path prefix, such as `/prod/chat/`.

The map's [`Published`](../../concepts/rollouts-and-traffic/traffic-map.md#published) condition means that the publisher wrote the route. The route's own status says whether the Gateway accepted it, and [Step 2](#step-2-check-the-publication) checks both.

## Step 1: Configure publishing {#step-1-configure-publishing}

Set the publisher's settings under `ome.multicluster.config.endpoint`, in the values file that you install `ome-resources` with:

```yaml title="values.yaml"
ome:
  multicluster:
    enabled: true
    role: control-plane
    config:
      endpoint:
        globalGateway: gateway-system/global
        backendPort: 80
        globalHostTemplate: "{{ .Name }}.{{ .Namespace }}.global.example.com"
        gatewayBackend:
          rewriteHostname: true
      routing:
        enabled: true
```

Keep the other values that you installed with, such as a probe from [Configure routing health probes](routing-health-probes.md), in the same file. Otherwise `helm upgrade` sets them back to the chart's defaults.

| Value | Default | What it sets |
| --- | --- | --- |
| `globalGateway` | Empty | The Gateway for the routes, as `namespace/name`, or `name` in the route's namespace. |
| `backendPort` | `0` | The workload clusters' Gateway port that the routes send to. Required with `globalGateway`. |
| `globalHostTemplate` | Empty | The global hostnames. See [Choose the global host](#choose-the-global-host). |
| `gatewayBackend.rewriteHostname` | `false` | `true` gives each backend a `URLRewrite` filter that sets the `Host` header to its workload hostname. |

This setup needs `rewriteHostname`: OME's routes on a workload cluster match only that cluster's hostnames, so a request with the `Host` header `chat.prod.global.example.com` gets HTTP 404 there. [Optional settings](#optional-settings) covers the other `endpoint` values.

### Choose the global host

`globalHostTemplate` is a Go template that can use only the InferenceService's `{{ .Name }}` and `{{ .Namespace }}`. Quote it in YAML, and keep double quotes and backslashes out of it: the chart copies it into JSON unescaped.

The annotation `ome.io/global-host` on an InferenceService wins over the template, and takes effect without a restart. With the chart's default, an empty template, only annotated InferenceServices get a hostname:

```yaml
metadata:
  annotations:
    ome.io/global-host: chat.global.example.com
```

Either way, the hostname has to:

- Be a valid, lowercase DNS subdomain name, which can start with `*.`, and not an IP address, or `Published` gets the reason [`InvalidPlan`](#published-is-false-with-reason-invalidplan).
- Match a listener of the Gateway, here `*.global.example.com`, and resolve to the Gateway in DNS.
- Be free: if another route on the Gateway lists exactly the same hostname, the reason is [`ClaimRejected`](#published-is-false-with-reason-claimrejected).

### Upgrade the release

Upgrade the release with the chart version you already run, 1.3.0 here:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

The chart restarts the controller, which reads the settings only when it starts. Wait for the rollout:

```bash
kubectl rollout status deployment ome-controller-manager -n ome
```

```output
deployment "ome-controller-manager" successfully rolled out
```

If it fails, see [The controller rollout doesn't finish](#the-controller-rollout-doesnt-finish). Then check the route's hostname and Gateway, and each backend's `Host` header and weight:

```bash
kubectl get httproute chat-global -n prod -o jsonpath='{.spec.hostnames[0]}{" "}{.spec.parentRefs[0].namespace}{"/"}{.spec.parentRefs[0].name}{"\n"}{range .spec.rules[0].backendRefs[*]}{.filters[0].urlRewrite.hostname}{" weight="}{.weight}{"\n"}{end}'
```

```output
chat.prod.global.example.com gateway-system/global
chat.prod.worker-a.example.com weight=1
chat.prod.worker-b.example.com weight=1
```

## Step 2: Check the publication

Check the map's `Published` condition:

```bash
kubectl get trafficmap chat -n prod -o jsonpath='{range .status.conditions[?(@.type=="Published")]}{.status}{" "}{.reason}{": "}{.message}{"\n"}{end}'
```

```output
True Published: TrafficMap is published by publisher "gatewayapi"
```

If it isn't `True`, see [Troubleshooting](#troubleshooting). Then check that the Gateway accepted the route:

```bash
kubectl get httproute chat-global -n prod -o jsonpath='{range .status.parents[*].conditions[*]}{.type}{"="}{.status}{" "}{.reason}{"\n"}{end}'
```

The output should include `Accepted=True` and `ResolvedRefs=True`. If it doesn't, see [Requests to the global hostname fail](#requests-to-the-global-hostname-fail). Last, send a request to the global hostname:

```bash
curl -s http://chat.prod.global.example.com/v1/models
```

The engine on one of the workload clusters answers with its list of models.

## Optional settings

After changing one of these settings, rerun the [`helm upgrade` command](#upgrade-the-release), and wait for the rollout.

### Backend TLS

When the workload clusters' Gateways accept only HTTPS, turn on `tls`. The publisher then writes a BackendTLSPolicy for each backend Service, so that the control-plane Gateway uses TLS and checks the workload hostname's certificate against the system's trusted CAs. It needs:

- The BackendTLSPolicy CRD at `gateway.networking.k8s.io/v1`, in the standard channel from Gateway API v1.4.0, and an implementation that supports it. Without the CRD, the reason is [`ClaimRejected`](#published-is-false-with-reason-claimrejected).
- A certificate on each workload cluster's Gateway for its workload hostnames, from a CA that the control-plane Gateway's implementation trusts as a system CA.
- `backendPort` set to the HTTPS port, such as 443.

```yaml
gatewayBackend:
  rewriteHostname: true
  tls:
    enabled: true
    wellKnownCACertificates: System
```

Keep `rewriteHostname` on: the policy sets the TLS server name, not the `Host` header. Turning `tls` off deletes the policies. Check them after the upgrade:

```bash
kubectl get backendtlspolicies -n prod -l ome.io/placement-endpoint-isvc=chat -o jsonpath='{range .items[*]}{.metadata.labels.ome\.io/placement-cluster}{" "}{.spec.validation.hostname}{"\n"}{end}'
```

```output
worker-b chat.prod.worker-b.example.com
worker-a chat.prod.worker-a.example.com
```

### EndpointSlice fallback

When the control-plane Gateway can't resolve the workload hostnames, turn on `endpointSlices`. The publisher then copies the IP addresses from the `status.addresses` of each workload cluster's serving Gateway into an EndpointSlice for the backend Service. The Services stay `ExternalName`, so this helps only an implementation that accepts them and reads their EndpointSlices. It needs:

- IP addresses in those `status.addresses`. Hostname addresses don't count.
- Permission for each WorkloadCluster's credentials to `get` `gateways` and `httproutes` in `gateway.networking.k8s.io`. On a workload cluster, the `ome-resources` value `ome.multiclusterAccess.enabled: true` grants it through the ClusterRole `ome-multicluster-access` to the identities in `ome.multiclusterAccess.subjects`.

```yaml
gatewayBackend:
  rewriteHostname: true
  endpointSlices:
    enabled: true
    addressRefreshInterval: 1m
```

`addressRefreshInterval` is required: the publisher rereads the addresses at least that often.

!!! warning "One failing cluster stops every update"
    The publisher resolves the addresses of all the entries, weight 0 included, before it writes anything. If one fails, for example on a disconnected cluster, the reason is `ApplyFailed`, and the route keeps its last weights, even when the TrafficMap moves traffic away from that cluster.

Check the EndpointSlices after the upgrade. The example Gateways report `203.0.113.10` on `worker-a` and `198.51.100.10` on `worker-b`:

```bash
kubectl get endpointslices -n prod -l ome.io/placement-endpoint-isvc=chat -o jsonpath='{range .items[*]}{.metadata.labels.ome\.io/placement-cluster}{" "}{.endpoints[*].addresses[0]}{"\n"}{end}'
```

```output
worker-b 198.51.100.10
worker-a 203.0.113.10
```

### Route namespace

`routeNamespace` puts all the routes and their objects in one namespace, which the Gateway's listener has to admit. The route of an InferenceService from another namespace gets a hash in its name: `chat` in `prod` gets `chat-ba4cb5d6-global`.

!!! warning "Choose routeNamespace before you publish"
    Changing `routeNamespace` doesn't move published routes. Each old route keeps its last weights and its hostname, so the new route gets `ClaimRejected` until you delete the old one. Delete the old Services, EndpointSlices and BackendTLSPolicies too.

## Troubleshooting

For `InvalidPlan`, `ClaimRejected` and `ApplyFailed`, the controller's log has the error:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'TrafficMap publisher reconciliation failed'
```

The publisher retries on its own: a failed read or write with backoff, and other failures at its resync interval, `routing.publisher.resyncInterval` (`1m` in the chart). It also retries when the map or the InferenceService changes. After a fix, check `Published` again.

### The controller rollout doesn't finish

The controller exits at startup when a multi-cluster setting is unusable, so `kubectl rollout status` fails at the deployment's progress deadline, 10 minutes by default. The log names each unusable setting:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'invalid multi-cluster configuration'
```

| Error starts with | Fix |
| --- | --- |
| `endpoint.backendPort:` | Set `backendPort`, from 1 to 65535, whenever `globalGateway` is set. |
| `endpoint.globalGateway:` | Set `globalGateway`, or turn the `gatewayBackend` settings off. |
| `endpoint.gatewayBackend.tls.` | Set both `tls.enabled: true` and `wellKnownCACertificates: System`, or neither. |
| `endpoint.gatewayBackend.endpointSlices.` | Set both `endpointSlices.enabled: true` and an `addressRefreshInterval` with a unit, such as `1m`, or neither. |

### Published is False with reason Withdrawn

The publisher withdraws a map, and deletes its route and Services, when `globalGateway` or `backendPort` isn't set, or the InferenceService has no global hostname. Without the annotation, the template has to render a hostname.

The controller reads the values from the `multicluster` entry of the `inferenceservice-config` ConfigMap when it starts. Check them there:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.multicluster}' | grep -E '"(globalGateway|backendPort|globalHostTemplate)"'
```

```output
    "globalHostTemplate": "{{ .Name }}.{{ .Namespace }}.global.example.com",
    "globalGateway": "gateway-system/global",
    "backendPort": 80,
```

### Published is False with reason InvalidPlan

| Error contains | Likely cause | Fix |
| --- | --- | --- |
| `global hostname:` | The hostname isn't a valid DNS subdomain name, or is an IP address. | Fix the annotation or the template. |
| `parse globalHostTemplate` or `render globalHostTemplate` | The template doesn't parse, or uses a field other than `.Name` and `.Namespace`. | Fix the template. |
| `Gateway API global gateway` | `globalGateway` isn't `name` or `namespace/name`. | Fix it. |
| `at most 16 backendRefs` | The map has more than 16 entries, but a route rule holds at most 16 backends. | Place the service on 16 workload clusters or fewer. |
| `still has legacy endpoint finalizer` | The older publisher hasn't cleaned up this InferenceService. | See [LegacyLifecycleActive](#published-is-false-with-reason-legacylifecycleactive). |

### Published is False with reason ClaimRejected

| Error contains | Likely cause | Fix |
| --- | --- | --- |
| `belongs to another source` | An object that the publisher didn't write for this InferenceService has one of its names. | Delete or rename it. |
| `is already published by HTTPRoute` | Another route on the Gateway has the same hostname, perhaps the publisher's old one after a [`routeNamespace` change](#route-namespace). | Change one hostname, or delete the old route. |
| Starts with `read` or `list` | The publisher can't read an object that it checks, for example a BackendTLSPolicy when its CRD is missing. | Install the CRD, or fix the API server error. |

### Published is False with reason ApplyFailed

Writing or deleting one of the publisher's objects failed, and the log has the API server's error. With the [EndpointSlice fallback](#endpointslice-fallback), the error can start with `resolve backend addresses for cluster "worker-b"` and contain one of these:

| Error contains | Likely cause | Fix |
| --- | --- | --- |
| `workload cluster is not connected` | The control plane has no working connection to that WorkloadCluster. | Check its `Ready` condition. |
| `read serving HTTPRoute` or `read serving Gateway` | The route or its Gateway isn't on the workload cluster yet, or the WorkloadCluster's credentials lack `get` permission for it. | Wait for the route, or grant `ome-multicluster-access`. |
| `has no IP status addresses` | The Gateway reports no IP address. | Wait for one, or turn the fallback off. |

### Published is False with reason LegacyLifecycleActive

The older endpoint publisher, which runs while routing is off and reads the same `endpoint` values, puts the finalizer `ome.io/placement-endpoint` on each InferenceService that it publishes. While any InferenceService carries it, the TrafficMap publisher publishes only the maps that it published before. Only the older publisher removes it, so turn routing off until it has. The global hostnames have no route from the first upgrade to the last.

1. In `values.yaml`, remove `globalGateway` and the `gatewayBackend` settings, and set `routing.enabled: false`. Keep `routeNamespace` if you set it: the older publisher looks for its routes there. Upgrade the release. OME deletes the maps, and the older publisher withdraws its routes and removes its finalizers.
2. Repeat this command until it prints nothing:

   ```bash
   kubectl get inferenceservices -A -o jsonpath='{range .items[*]}{.metadata.namespace}{"/"}{.metadata.name}{" "}{.metadata.finalizers}{"\n"}{end}' | grep 'ome.io/placement-endpoint'
   ```

3. Restore `globalGateway`, your `gatewayBackend` settings and `routing.enabled: true`, and upgrade the release again.

### Requests to the global hostname fail

| Symptom | Likely cause | Fix |
| --- | --- | --- |
| The hostname doesn't resolve. | No DNS record. | Add one. |
| `Accepted` is `False`, reason `NotAllowedByListeners`. | The listener's `allowedRoutes` doesn't admit the route's namespace. | Admit it. |
| `Accepted` is `False`, reason `NoMatchingListenerHostname`. | No listener's hostname matches the global hostname. | Add a matching listener, or change the hostname. |
| HTTP 404. | The request reached a Gateway without a route for its hostname and path. | Turn on `rewriteHostname`, as in [Step 1](#step-1-configure-publishing). On the shared host, add the path prefix. |
| HTTP 500, and `ResolvedRefs` is `False`. | The implementation rejects the backends, for example because it doesn't support `ExternalName` Services. | Check its support for `ExternalName` backends. |
| The connection to a workload cluster fails. | A blocked network path to its Gateway, a wrong `backendPort`, or HTTPS with `tls` off. | Check the path, the port and [Backend TLS](#backend-tls). |

## Clean up

To stop publishing, remove `globalGateway` and the `gatewayBackend` settings from `values.yaml`, and upgrade the release. The publisher deletes each route and its Services, and routing keeps writing the maps. Keep `routeNamespace` if you set it, or the old routes stay behind at weight 0 until their maps are deleted.

To stop publishing one InferenceService instead, set `spec.routing.enabled: false` on it. OME deletes its TrafficMap once the publisher has deleted the route and its Services.

Either way, check that the route is gone:

```bash
kubectl get httproute chat-global -n prod
```

```output
Error from server (NotFound): httproutes.gateway.networking.k8s.io "chat-global" not found
```

## Next steps

- [Traffic map](../../concepts/rollouts-and-traffic/traffic-map.md): how routing works out each cluster's weight, and every reason of the `Published` condition.
- [Configure routing health probes](routing-health-probes.md): take traffic away from a workload cluster whose endpoint stops answering.
- [Tune routing capacity polling](tune-routing-capacity-polling.md): lower a workload cluster's weight to the capacity that its endpoints report.
- [Drain a workload cluster](drain-a-workload-cluster.md): hold a workload cluster at zero weight for maintenance.
- [Choose a Gateway API host scheme](../networking/gateway-host-schemes.md): the hostnames that each workload cluster gives its InferenceServices.
