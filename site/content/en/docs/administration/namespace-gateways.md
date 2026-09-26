---
title: "Per-Namespace Ingress Gateways"
linkTitle: "Per-Namespace Ingress Gateways"
weight: 56
description: >
  Route each namespace's InferenceServices through that namespace's own
  Gateway, and override the gateway selection for a single service with
  annotations.
---

When OME serves ingress through the Gateway API (`enableGatewayAPI: true` and
`disableIngressCreation: false` in the `ingress` block of the
`inferenceservice-config` ConfigMap), every generated `HTTPRoute` attaches to
one or more Gateways via `parentRefs`. By default that is a single
cluster-wide gateway, but the selection can be layered:

- **Cluster default** — the `omeIngressGateway` field (plus optional
  `additionalIngressGateways`) applies to every InferenceService.
- **Per-namespace override** — the `namespaceIngressGateways` map replaces
  the cluster default for all InferenceServices in a given namespace, so each
  namespace can own a Gateway with its own TLS certificate and DNS.
- **Per-service override** — the `ome.io/ingress-gateway` and
  `ome.io/ingress-additional-gateways` annotations win over both.

Gateway selection is independent of the hostname/path scheme of the routes;
see [Gateway API Host Schemes](/ome/docs/administration/gateway-host-schemes/)
for that.

## How a gateway entry is specified

Everywhere a gateway can be configured, it is the same object shape:

| Field               | Required | Meaning |
|---------------------|----------|---------|
| `omeIngressGateway` | yes      | The Gateway the route attaches to, in `namespace/name` form. Becomes the route's `parentRef`. |
| `ingressDomain`     | yes      | The domain used to render **this gateway's** hostname on the route: `<sharedHostPrefix>.<ingressDomain>` in the shared-host scheme, or the `domainTemplate` rendering (for example `<name>.<namespace>.<ingressDomain>`) in the per-ISVC subdomain scheme. |
| `class`             | no       | Free-form label ("internal", "external", ...) attached to this gateway's endpoint in the InferenceService status. Must be an RFC-1123 label; the controller rejects the ConfigMap at load time otherwise. Empty means the endpoint carries no class. |

There is no fallback between the fields: a per-namespace or additional
gateway entry that sets `omeIngressGateway` but omits `ingressDomain`
renders an empty domain into that gateway's hostname, so always set both.

## Precedence

The primary gateway and the additional-gateways list are resolved
**independently**, each on its own three-level chain (highest first):

**Primary gateway** (first `parentRef`, first hostname):

1. `ome.io/ingress-gateway` annotation on the InferenceService.
2. `namespaceIngressGateways[<namespace>].primary`, when its
   `omeIngressGateway` is non-empty.
3. Cluster default: the `omeIngressGateway` and `ingressDomain` config fields.

**Additional gateways** (extra `parentRefs`/hostnames after the primary):

1. `ome.io/ingress-additional-gateways` annotation on the InferenceService.
2. `namespaceIngressGateways[<namespace>].additional`, when present. An
   explicit empty list (`additional: []`) means "no additional gateways" for
   that namespace; omitting the key keeps the cluster default.
3. Cluster default: the `additionalIngressGateways` config field.

Because the two chains are independent, annotating only
`ome.io/ingress-gateway` on a service still leaves the namespace's
`additional` override in effect for that service, and vice versa.

The same resolution drives the endpoints published in the InferenceService
status, so `status.url` and the per-gateway endpoint list always match what
the routes actually attach to.

## Per-namespace gateways

`namespaceIngressGateways` maps an InferenceService's **namespace** to the
gateway(s) its routes attach to. All fields live in the `ingress` key of the
`inferenceservice-config` ConfigMap, rendered by the `ome-resources` chart
from `ome.controller.ingressGateway`:

```yaml
# values.yaml (ome-resources)
ome:
  controller:
    ingressGateway:
      enableGatewayAPI: true
      disableIngressCreation: false
      # Namespace-scoped hostnames (see Gateway API Host Schemes).
      perISVCSubdomain: true
      # Cluster default for namespaces not listed below.
      omeIngressGateway: "envoy-gateway-system/tenant-int-gw"
      domain: example.com                # → ingressDomain
      namespaceIngressGateways:
        prod:
          primary:
            omeIngressGateway: "envoy-gateway-system/prod-int-gw-https"
            ingressDomain: "example.com"
          additional:
            - omeIngressGateway: "envoy-gateway-system/prod-ext-gw-https"
              ingressDomain: "ext.example.com"
              class: "external"
```

The rendered ConfigMap fragment:

```yaml
data:
  ingress: |-
    {
      "enableGatewayAPI": true,
      "disableIngressCreation": false,
      "perISVCSubdomain": true,
      "omeIngressGateway": "envoy-gateway-system/tenant-int-gw",
      "ingressDomain": "example.com",
      "namespaceIngressGateways": {
        "prod": {
          "primary": {
            "omeIngressGateway": "envoy-gateway-system/prod-int-gw-https",
            "ingressDomain": "example.com"
          },
          "additional": [
            {
              "omeIngressGateway": "envoy-gateway-system/prod-ext-gw-https",
              "ingressDomain": "ext.example.com",
              "class": "external"
            }
          ]
        }
      }
    }
```

With this configuration, an InferenceService in `prod` gets routes with two
`parentRefs` (`prod-int-gw-https` and `prod-ext-gw-https`) and one hostname
per gateway, while services in every other namespace keep the single
cluster-default `tenant-int-gw`. The chart default is an empty map
(`namespaceIngressGateways: {}`), meaning cluster defaults everywhere.

Semantics of an override entry:

- `primary` takes effect only when its `omeIngressGateway` is non-empty; an
  empty value keeps the cluster-default primary gateway (useful when a
  namespace only needs different *additional* gateways). When it does take
  effect, both the `parentRef` **and** the hostname domain come from the
  entry.
- `additional: []` removes the cluster-default additional gateways for the
  namespace; leaving `additional` out keeps them.

After changing the ConfigMap, restart the controller so it picks up the new
configuration:

```bash
kubectl rollout restart deployment/ome-controller-manager -n ome
```

### Per-namespace TLS gateways

The typical use is one Gateway per namespace, each terminating TLS with its
own certificate, served by a single OME controller. Pair it with the
per-ISVC subdomain scheme (`perISVCSubdomain: true`): the hostname is
rendered from `domainTemplate`, which already embeds the namespace
(`{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}` by chart default), so a
service in `prod` keeps its `<name>.prod.example.com` host no matter which
gateway serves it. Give the `prod` Gateway a listener for
`*.prod.example.com` with that namespace's certificate, point the wildcard
DNS record at that Gateway, and list the gateway under
`namespaceIngressGateways.prod` with the **same** `ingressDomain` as the
cluster default — the hostnames do not change, only the `parentRefs` do.

Under the default shared-host scheme the hostname is
`<sharedHostPrefix>.<ingressDomain>` with no namespace in it, so
per-namespace gateways there either share one host (and DNS cannot split
traffic by namespace) or need a distinct `ingressDomain` per namespace
override, which changes the host clients call.

Two things per-namespace gateways do **not** change:

- The Gateway must permit the attachment. A route in `prod` attaching to a
  Gateway in `envoy-gateway-system` is a cross-namespace `parentRef`, so the
  Gateway listener's `allowedRoutes.namespaces` must allow routes from the
  service's namespace, or the route stays unaccepted.
- The map key is the InferenceService's namespace, not the Gateway's. The
  Gateways themselves can live anywhere.

## Per-service annotations

A single InferenceService can override the resolved gateways with two
annotations:

| Annotation                          | Overrides                                          | Value |
|-------------------------------------|----------------------------------------------------|-------|
| `ome.io/ingress-gateway`            | Primary gateway (`parentRef` only)                 | Gateway in `namespace/name` form |
| `ome.io/ingress-additional-gateways`| Additional gateways (parentRefs **and** hostnames) | JSON array of `{"omeIngressGateway", "ingressDomain", "class"}` objects |

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: prod
  annotations:
    ome.io/ingress-gateway: "envoy-gateway-system/special-gw"
    ome.io/ingress-additional-gateways: >-
      [{"omeIngressGateway": "envoy-gateway-system/partner-gw",
        "ingressDomain": "partner.example.com", "class": "external"}]
spec:
  model:
    name: llama-3-1-70b-instruct
  runtime:
    name: srt-llama-3-1-70b-instruct
```

Annotation semantics to be aware of:

- `ome.io/ingress-gateway` replaces only the primary `parentRef`. The primary
  hostname keeps the resolved `ingressDomain` (the cluster default, or the
  `ome.io/ingress-domain` annotation if set) — a namespace override's domain
  is *not* used once this annotation is present. If the annotated gateway
  serves a different domain, set `ome.io/ingress-domain` alongside it.
- The **presence** of either annotation disables the corresponding
  per-namespace override, even with an empty value. So
  `ome.io/ingress-gateway: ""` pins one service in an overridden namespace
  back to the cluster-default primary gateway, and
  `ome.io/ingress-additional-gateways: "[]"` gives it no additional gateways
  at all.
- A malformed `ome.io/ingress-additional-gateways` value (invalid JSON) is
  silently ignored: the service falls back to the **cluster-default**
  additional gateways — not the namespace override, since the annotation is
  still present. There is no admission-time validation of the JSON, so check
  the generated routes after annotating.

## Verifying the attachment

Inspect the generated routes — one `parentRef` and one hostname per resolved
gateway, primary first:

```bash
kubectl get httproute -n prod \
  -o custom-columns='NAME:.metadata.name,GATEWAYS:.spec.parentRefs[*].name,HOSTS:.spec.hostnames[*]'
```

For the `prod` example above:

```
NAME              GATEWAYS                               HOSTS
llama-chat        prod-int-gw-https,prod-ext-gw-https    llama-chat.prod.example.com,llama-chat.prod.ext.example.com
llama-chat-engine prod-int-gw-https,prod-ext-gw-https    llama-chat.prod.example.com,llama-chat.prod.ext.example.com
```

Confirm each Gateway accepted the route in
`status.parents` of the HTTPRoute (`kubectl get httproute <name> -n prod -o
yaml`); an `Accepted: False` condition on one parent usually means that
Gateway's `allowedRoutes` does not permit the service's namespace.

## Next steps

- [Gateway API Host Schemes](/ome/docs/administration/gateway-host-schemes/)
  — shared host vs per-ISVC subdomain, which determines the hostname each
  gateway entry's `ingressDomain` renders into.
- [Ingress Administration](/ome/docs/administration/ingress/) — the general
  ingress configuration reference and the other `ome.io/ingress-*`
  annotations.
