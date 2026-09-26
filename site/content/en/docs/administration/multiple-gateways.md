---
title: "Multiple Ingress Gateways"
linkTitle: "Multiple Ingress Gateways"
weight: 56
description: >
  Attach the HTTPRoutes OME generates to more than one Gateway — for example an
  internal and an external gateway — and read the per-gateway endpoints
  published in status.addresses.
---

When OME serves ingress through the Gateway API (`enableGatewayAPI: true` and
`disableIngressCreation: false` in the `ingress` block of the
`inferenceservice-config` ConfigMap), every generated `HTTPRoute` attaches to
the **primary gateway** named by `omeIngressGateway`. The
`additionalIngressGateways` list attaches those same routes to extra gateways
— each with a hostname rendered from that gateway's own domain — so a single
InferenceService can be reachable through, say, an internal gateway and an
external one without any per-service configuration.

Each gateway also contributes one entry to the InferenceService's
`status.addresses`, tagged with an operator-declared **class** (`"internal"`,
`"external"`, ...), so clients and platform tooling can discover every
endpoint and pick one by class.

## Configuration

Three fields in the `ingress` configuration drive multi-gateway attachment:

| Field | Type | Description |
|-------|------|-------------|
| `omeIngressGateway` | string | The primary gateway as a `namespace/name` parentRef. Its hostname is rendered from the top-level `ingressDomain`. Required for Gateway API ingress. |
| `omeIngressGatewayClass` | string | Class tagged onto the primary gateway's `status.addresses` entry. No built-in default; empty means the primary entry carries no class. |
| `additionalIngressGateways` | list | Extra gateways every route also attaches to. Empty (the default) preserves single-gateway behavior. |

Each `additionalIngressGateways` entry has:

| Field | Required | Description |
|-------|----------|-------------|
| `omeIngressGateway` | yes | The gateway parentRef in `namespace/name` form. |
| `ingressDomain` | yes | The domain used to render **this gateway's** route hostname, under the same [host scheme](/ome/docs/administration/gateway-host-schemes/) as the primary (shared host prefix or per-ISVC subdomain). |
| `class` | no | Class tagged onto this gateway's `status.addresses` entry. Empty means the entry carries no class. |

Classes are free-form but must be valid RFC-1123 DNS labels
(`[a-z0-9]([-a-z0-9]*[a-z0-9])?`); the value is copied verbatim into status,
so a malformed class is rejected when the controller loads the configuration
(`invalid ingress config - additionalIngressGateways[0].class ... must be an
RFC-1123 label`). Avoid the value `cluster-local` — it is the conventional
class of the in-cluster endpoint that OME always appends to
`status.addresses`.

### Helm values

The `ome-resources` chart renders these fields from
`ome.controller.ingressGateway`:

```yaml
# values.yaml (ome-resources)
ome:
  controller:
    ingressGateway:
      enableGatewayAPI: true
      disableIngressCreation: false
      omeIngressGateway: "envoy-gateway-system/internal-gateway"
      omeIngressGatewayClass: "internal"
      domain: internal.example.com        # → ingressDomain (primary)
      sharedHostPrefix: "llm"
      additionalIngressGateways:
        - omeIngressGateway: "envoy-gateway-system/external-gateway"
          ingressDomain: "external.example.com"
          class: "external"
```

The rendered ConfigMap fragment:

```yaml
data:
  ingress: |-
    {
      "enableGatewayAPI": true,
      "disableIngressCreation": false,
      "omeIngressGateway": "envoy-gateway-system/internal-gateway",
      "omeIngressGatewayClass": "internal",
      "ingressDomain": "internal.example.com",
      "sharedHostPrefix": "llm",
      "additionalIngressGateways": [
        {
          "omeIngressGateway": "envoy-gateway-system/external-gateway",
          "ingressDomain": "external.example.com",
          "class": "external"
        }
      ]
    }
```

After changing the ConfigMap, restart the controller so it picks up the new
configuration:

```bash
kubectl rollout restart deployment/ome-controller-manager -n ome
```

## The generated routes

With the configuration above, every route OME generates for an
InferenceService — the top-level route and the per-component routes — carries
one `parentRef` **and** one hostname per gateway, primary first, then each
additional gateway in list order. For `llama-chat` in namespace `prod` under
the default shared-host scheme:

```yaml
spec:
  parentRefs:
    - group: gateway.networking.k8s.io
      kind: Gateway
      namespace: envoy-gateway-system
      name: internal-gateway
    - group: gateway.networking.k8s.io
      kind: Gateway
      namespace: envoy-gateway-system
      name: external-gateway
  hostnames:
    - llm.internal.example.com          # rendered from the primary's ingressDomain
    - llm.external.example.com          # rendered from the additional gateway's ingressDomain
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /prod/llama-chat/
      # ... urlRewrite filter and backendRefs as in the single-gateway case
```

Hostname rendering follows whichever
[host scheme](/ome/docs/administration/gateway-host-schemes/) is active,
applied per gateway with that gateway's own domain:

- **Shared host + path prefix (default):** `<sharedHostPrefix>.<domain>` per
  gateway — `llm.internal.example.com` and `llm.external.example.com` above.
- **Per-ISVC subdomain (`perISVCSubdomain: true`):** `domainTemplate` rendered
  with each gateway's domain — `llama-chat.prod.internal.example.com` and
  `llama-chat.prod.external.example.com`.

Two Gateway API semantics to plan for:

- **Scope each Gateway's listeners to its own domain.** `spec.hostnames`
  applies to every `parentRef` — the route itself does not pair hostname #1
  with gateway #1. The pairing becomes effective through listener hostname
  intersection: a gateway only serves the route hostnames that intersect its
  listeners' `hostname`. Set the internal gateway's listener to
  `*.internal.example.com` (or the exact shared host) and the external
  gateway's to `*.external.example.com`, so neither serves the other's
  hostname.
- **Every gateway must accept the routes.** Each listed gateway needs
  `allowedRoutes` permitting HTTPRoutes from the InferenceService's namespace.
  OME requires **all** parents to report the route as accepted before it sets
  the `IngressReady` condition to `True` — a route rejected by just one
  gateway keeps the InferenceService not ready. Check
  `kubectl get httproute <name> -n <namespace> -o yaml` and look at
  `status.parents` for the rejecting gateway's condition.

## Reading status.addresses

The Gateway API ingress strategy publishes every reachable endpoint in
`status.addresses`, derived from the same builder that produces the routes so
status can never disagree with the actual routing. The list contains, in
order:

1. One entry per gateway — primary first, then each additional gateway in
   list order. Each entry's `name` is that gateway's operator-declared class
   (omitted when the class is empty), and its `url` is
   `<urlScheme>://<hostname><path>` for that gateway.
2. A final `cluster-local` entry: the in-cluster Service address
   (`<name>-router.<namespace>.svc.cluster.local` when `spec.router` is set,
   otherwise `<name>-engine.<namespace>.svc.cluster.local`).

The two long-standing status fields are projections of this list:

- `status.url` is the **primary gateway** entry — including the
  load-bearing `/<namespace>/<service>/` path in the shared-host scheme.
- `status.address` is the **cluster-local** entry.

For the example configuration above:

```yaml
status:
  url: http://llm.internal.example.com/prod/llama-chat/
  address:
    url: http://llama-chat-engine.prod.svc.cluster.local
  addresses:
    - name: internal
      url: http://llm.internal.example.com/prod/llama-chat/
    - name: external
      url: http://llm.external.example.com/prod/llama-chat/
    - name: cluster-local
      url: http://llama-chat-engine.prod.svc.cluster.local
```

To select an endpoint by class:

```bash
# All endpoints with their classes
kubectl get inferenceservice llama-chat -n prod \
  -o jsonpath='{range .status.addresses[*]}{.name}{"\t"}{.url}{"\n"}{end}'

# Just the external endpoint
kubectl get inferenceservice llama-chat -n prod \
  -o jsonpath='{.status.addresses[?(@.name=="external")].url}'
```

When `omeIngressGatewayClass` is unset, the primary entry has no `name`;
identify it as the entry whose `url` equals `status.url`. Without any
`additionalIngressGateways`, the list still appears — one gateway entry plus
`cluster-local` — so tooling can rely on `status.addresses` in single-gateway
clusters too. `status.addresses` is populated only by the Gateway API ingress
path, not by the Kubernetes Ingress or Istio VirtualService strategies.

## Overriding the gateway set

The cluster-wide gateway set can be replaced for all InferenceServices in a
namespace (`namespaceIngressGateways` in the same `ingress` configuration) or
for a single InferenceService (the `ome.io/ingress-gateway` and
`ome.io/ingress-additional-gateways` annotations, which take precedence over
the namespace default). The `status.addresses` semantics described above are
unchanged — entries reflect whichever gateways the routes actually attach to.

## Next steps

- [Gateway API Host Schemes](/ome/docs/administration/gateway-host-schemes/) —
  how the hostname and path of each route are rendered from a gateway's
  domain.
- [Ingress Administration](/ome/docs/administration/ingress/) — the full
  ingress configuration reference and Gateway API enablement.
