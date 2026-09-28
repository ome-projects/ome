---
title: "Global Endpoint Publishing"
linkTitle: "Global Endpoint Publishing"
weight: 13
date: 2026-09-27
description: >
  Configure where a multi-cluster InferenceService's global route is published — the global host, the Gateway it attaches to, the backend port, and how the global gateway reaches each workload cluster's gateway.
---

In a multi-cluster installation, the control plane can publish each placed
InferenceService on a single **global host**: it programs a Gateway API
`HTTPRoute` on the control-plane cluster, attached to an operator-designated
**global Gateway**, whose backends point at each serving workload cluster's
own ingress. Clients call one hostname; the global gateway splits traffic
across the clusters that currently serve the model.

Global endpoint publishing is part of OME's multi-cluster support, which is
still under active development. The feature is **alpha** and may change
without notice; do not build production automation on it yet.

This page is about **configuring the publication target and its backends**.
Reading the resulting routing table is covered by the
[TrafficMap](/ome/docs/concepts/traffic_map/) concept page, and selecting
ingress gateways *inside* one cluster is covered by
[Multiple Ingress Gateways](/ome/docs/administration/multiple-gateways/) —
neither of those is this page's subject.

## How it works

For every InferenceService that is **Placed** with at least one addressable
serving cluster *and* resolves a global host (see below), the publisher
creates, on the control-plane cluster:

- one **HTTPRoute** named `<isvc-name>-global`, attached to the configured
  global Gateway, matching the global host at path prefix `/`;
- one **Service** per serving cluster, named `<isvc-name>-global-<cluster>`,
  which by default is a `type: ExternalName` alias of that cluster's ingress
  hostname (the host of the derived service's `status.url` on that cluster),
  with the configured backend port.

The route carries one `backendRef` per serving cluster. When TrafficMap
routing is enabled, the backend weights are the TrafficMap's capacity-aware,
health-gated weights; otherwise each cluster's live ready-replica count is
used, falling back to an equal split while no cluster reports ready replicas
yet. The same `endpoint` configuration drives both cases — with routing
disabled a legacy endpoint publisher realizes it directly, with routing
enabled the TrafficMap publisher does (and per-cluster backend Services get
collision-resistant `tmb-<digest>` names instead).

A service that is not publishable — not Placed, no addressable cluster yet,
or no global host — has any previously published route torn down rather than
left pointing at a stale winner. When routing is enabled, the TrafficMap's
`Published` condition reports this as `Withdrawn` (for example, "the service
resolves no global hostname"). Published resources carry the
`ome.io/managed-by: placement-endpoint` label, so you can list them:

```bash
kubectl get httproute,service -n prod -l ome.io/managed-by=placement-endpoint
```

## Configuration

Publishing is configured in the `endpoint` block of the `multicluster` key in
the `inferenceservice-config` ConfigMap (in the OME controller namespace,
`ome` by default). The block is read **once at manager startup**; changing it
requires a manager restart. With the Helm chart, set the same fields under
`ome.multicluster.config.endpoint` — the chart rolls the controller when the
rendered block changes.

| Field | Description |
|-------|-------------|
| `globalHostTemplate` | Go `text/template` rendering the global host for a service, evaluated against `{{.Name}}` and `{{.Namespace}}` (for example `"{{.Name}}.{{.Namespace}}.global.example.com"`). Empty means only services carrying the `ome.io/global-host` annotation publish. |
| `globalGateway` | The Gateway the published HTTPRoute attaches to, in `namespace/name` form. Empty disables the publisher entirely (a no-op — OME never invents a gateway). A bare name without a slash resolves to the route's own namespace. |
| `routeNamespace` | Namespace the HTTPRoute and its backing resources are created in. Empty uses each InferenceService's own namespace. The namespace must already exist; OME does not create it. |
| `backendPort` | The port on each workload cluster's ingress that the global host forwards to — a single port shared by every cluster (it becomes the backend Service port and the route's `backendRef` port). Required whenever `globalGateway` is set. |
| `gatewayBackend` | How the global gateway connects to each workload cluster's gateway: hostname rewrite, backend TLS, and the direct-address fallback. See below. |

There are **no in-code defaults** for the host template, gateway, namespace,
or port: they are deployment-identity values, and an absent value means the
service is simply not published rather than a baked-in host or gateway being
substituted. A half-finished configuration is refused at startup — naming a
gateway without a port fails with
`invalid multi-cluster configuration: endpoint.backendPort: must be set when
endpoint.globalGateway is configured`.

A minimal working configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: ome
data:
  multicluster: |
    {
      "endpoint": {
        "globalHostTemplate": "{{.Name}}.{{.Namespace}}.global.example.com",
        "globalGateway": "ome/global-gateway",
        "backendPort": 443
      }
    }
```

## The global host

Two sources can yield a service's global host, checked in order:

1. **The `ome.io/global-host` annotation** on the (control-plane)
   InferenceService pins an explicit host for that one service, overriding
   the template. Changing or removing the annotation re-publishes or
   withdraws the route on the next reconcile.
2. **`endpoint.globalHostTemplate`** renders the host from the service's
   name and namespace.

When neither yields a host, the service is **not published** — that is the
supported way to keep an individual service (template empty, no annotation)
or the whole installation (template empty) off the global gateway while the
rest of the endpoint configuration stays in place. A template that fails to
parse or render is an operator configuration error: it is logged at reconcile
time (`endpoint: resolve target failed`) and the service is not published
until the template is fixed.

The global Gateway itself is yours to provide, on the control-plane cluster.
Standard Gateway API attachment rules apply: it needs a listener whose
hostname matches the rendered global hosts (a wildcard such as
`*.global.example.com` covers a per-service template) and whose
`allowedRoutes` admits routes from the route namespace(s) — each service's
own namespace by default, or the single `routeNamespace` if you set one.

## Connecting to each workload cluster's gateway

The published route's backends are the *workload clusters' gateways*, not
pods — traffic leaves the global gateway toward each cluster's ingress
hostname. The `endpoint.gatewayBackend` block configures that hop using
standard Gateway API behavior; everything in it is off by default, and the
baseline ExternalName Service representation is retained throughout.

| Field | Description |
|-------|-------------|
| `rewriteHostname` | Rewrite the forwarded `Host` header to the selected cluster's ingress hostname. |
| `tls.enabled` | Manage one `BackendTLSPolicy` per serving cluster, enabling HTTPS origination from the global gateway to each cluster's gateway. |
| `tls.wellKnownCACertificates` | The Gateway API trust-root set written verbatim to each policy. Required when `tls.enabled`; the currently supported standard value is `"System"`. |
| `endpointSlices.enabled` | Publish each workload cluster gateway's IP addresses in EndpointSlices, for environments where the global gateway cannot use the ExternalName's DNS answer. |
| `endpointSlices.addressRefreshInterval` | How often published gateway addresses are re-read, as a Go duration string. Required when EndpointSlices are enabled. |

Any of these features requires `globalGateway` to be set, and the paired
fields are validated together at startup: TLS settings without
`tls.enabled: true` (and vice versa), or an EndpointSlice refresh interval
without `endpointSlices.enabled: true` (and vice versa), are startup errors,
not silent fallbacks.

### Hostname rewrite

Requests arrive at the global gateway addressed to the *global* host, but
each workload cluster's gateway routes on its own per-cluster hostname. With
`rewriteHostname: true` every `backendRef` gets a `URLRewrite` filter that
replaces the `Host` header with that cluster's ingress hostname before
forwarding, so the request matches the per-cluster route on arrival. Leave it
off only if your workload cluster gateways accept the global host directly.

### Backend TLS

With `tls.enabled: true` the publisher writes one `BackendTLSPolicy` per
serving cluster (named after the backend Service and targeting it), telling
the global gateway to originate TLS to that cluster's gateway and validate
its certificate against the configured trust roots for the cluster's ingress
hostname. This is independent of how the backend Service obtains addresses —
it applies to both the ExternalName baseline and the EndpointSlice fallback.

The `BackendTLSPolicy` CRD (`gateway.networking.k8s.io/v1`) must be installed
on the control-plane cluster and supported by your gateway implementation;
enabling TLS without the CRD makes publication fail explicitly.

### EndpointSlice direct-address fallback

Some gateway deployments cannot reach the address an ExternalName Service's
DNS name resolves to (split-horizon DNS, cluster-local synthetic answers, or
an implementation that refuses ExternalName backends). With
`endpointSlices.enabled: true` the publisher bypasses DNS: over OME's
existing WorkloadCluster connections it reads the derived service's generated
HTTPRoute on each serving cluster, follows it to its parent Gateway, and
copies that Gateway's advertised IP `status.addresses` into EndpointSlices
(`…-ipv4` / `…-ipv6`, attached to the backend Service), so the global gateway
routes to the literal data-plane IPs. Hostname-type status addresses are
ignored — a serving Gateway advertising no IP addresses is a publish error,
not an empty backend.

Addresses are re-read on the required `addressRefreshInterval` cadence, so a
workload cluster gateway whose address changes converges within one interval.
The workload-cluster credential must be able to `get` Gateways and HTTPRoutes
on each serving cluster; the chart's multicluster-access ClusterRole grants
this read-only access.

```yaml
"gatewayBackend": {
  "rewriteHostname": true,
  "tls": {
    "enabled": true,
    "wellKnownCACertificates": "System"
  },
  "endpointSlices": {
    "enabled": true,
    "addressRefreshInterval": "1m"
  }
}
```

## Verifying a publication

```bash
kubectl get httproute chat-global -n prod -o yaml
```

Check that the route's `hostnames` carries the expected global host, its
`parentRefs` names the configured gateway, and each `backendRef` points at a
`<isvc>-global-<cluster>` Service with the configured port. When TrafficMap
routing is enabled, the map's `Published` condition and `status.gatewayRef`
report the same route — see
[Traffic Map](/ome/docs/concepts/traffic_map/) for reading them, and
[Routing Health Probes](/ome/docs/administration/routing-health-probes/) for
the health inputs that gate the weights.

Two behavioral notes for operations:

- A published InferenceService carries a **finalizer**: deletion waits until
  the publisher has removed the route and every per-cluster backing resource,
  so nothing keeps serving a deleted service's global host.
- If you set a shared `routeNamespace`, resource names embed a short digest
  of the source namespace and name (for example `chat-1a2b3c4d-global`), so
  same-named services from different namespaces never collide. The
  `ome.io/placement-endpoint-isvc` and
  `ome.io/placement-endpoint-isvc-namespace` labels on every published
  resource identify the source exactly.
