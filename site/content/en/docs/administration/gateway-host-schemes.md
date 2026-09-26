---
title: "Gateway API Host Schemes"
linkTitle: "Gateway API Host Schemes"
weight: 55
description: >
  Choose between a shared hostname with per-service path prefixes and a
  per-InferenceService subdomain for the HTTPRoutes OME generates.
---

When OME serves ingress through the Gateway API (`enableGatewayAPI: true` and
`disableIngressCreation: false` in the `ingress` block of the
`inferenceservice-config` ConfigMap), it generates one `HTTPRoute` per
InferenceService component plus a top-level route. The hostname and path of
those routes follow one of two schemes:

- **Shared host + path prefix (default).** Every route in the cluster uses one
  shared hostname, `<sharedHostPrefix>.<ingressDomain>`, and each
  InferenceService is disambiguated by a `/<namespace>/<service>/` path prefix
  that is rewritten away before the request reaches the backend.
- **Per-ISVC subdomain (opt-in).** Each InferenceService is reached at its own
  hostname — the same host published in `status.url`, rendered from
  `domainTemplate` — and its routes match at the root path `/` with no rewrite.

The scheme is selected cluster-wide by the `perISVCSubdomain` field of the
ingress configuration and can be overridden per service with annotations. Both
schemes only shape the **hostname and path** of the generated routes; which
Gateway(s) the routes attach to is configured separately (the
`omeIngressGateway` field and related gateway settings).

## Shared host + path prefix (default)

With `perISVCSubdomain: false` (the default), every generated route carries the
single shared hostname:

```
<sharedHostPrefix>.<ingressDomain>
```

The `sharedHostPrefix` value has **no built-in default in the OME binary** —
the Helm chart supplies it (`ome-resources` ships `sharedHostPrefix: "llm"`),
so the chart values are the source of truth for your cluster. An empty prefix
yields the bare `ingressDomain` as the shared host.

Each InferenceService is then addressed by path. For an InferenceService named
`llama-chat` in namespace `prod` with `ingressDomain: example.com` and the
chart-default prefix, OME generates these routes:

| HTTPRoute            | Path prefix match           | Backend Service      |
|----------------------|-----------------------------|----------------------|
| `llama-chat`         | `/prod/llama-chat/`         | Router if the service has one, otherwise engine |
| `llama-chat-engine`  | `/prod/llama-chat-engine/`  | `llama-chat-engine`  |
| `llama-chat-router`  | `/prod/llama-chat-router/`  | `llama-chat-router` (only when `spec.router` is set) |
| `llama-chat-decoder` | `/prod/llama-chat-decoder/` | `llama-chat-decoder` (only when `spec.decoder` is set) |

All of them share the host `llm.example.com`. Each route rule carries a
`urlRewrite` filter that replaces the matched prefix with `/`, so the backend
never sees the routing prefix:

```
GET http://llm.example.com/prod/llama-chat/v1/chat/completions
                          └────────┬─────┘
                        stripped by urlRewrite
→ backend receives GET /v1/chat/completions
```

The top-level route (named exactly after the InferenceService) is the intended
entry point; the per-component routes make the engine, router, and decoder
individually addressable under their own path prefixes.

`status.url` reflects the same scheme: it is the shared host plus the
top-level path, for example `http://llm.example.com/prod/llama-chat/`.

This scheme needs only **one DNS record and one TLS certificate** for the
shared host, which is why it is the default: adding an InferenceService never
requires new DNS or certificates.

## Per-ISVC subdomain (opt-in)

With `perISVCSubdomain: true`, each InferenceService gets its own hostname,
rendered from `domainTemplate` — the same renderer that produces
`status.url`, so the route host and the published URL cannot drift. With the
chart-default template `{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}`,
the service above is reached at:

```
llama-chat.prod.example.com
```

Routes match at the root path `/` and carry **no** `urlRewrite` filter — the
host alone identifies the service, so the request path is passed through to
the backend unchanged:

```
GET http://llama-chat.prod.example.com/v1/chat/completions
→ backend receives GET /v1/chat/completions
```

`status.url` is the same host at path `/`, for example
`http://llama-chat.prod.example.com/`.

Two operational consequences to plan for:

- **DNS and TLS must cover every subdomain.** In practice this means a
  wildcard DNS record (for example `*.prod.example.com`, or
  `*.example.com` with a flatter `domainTemplate`) and a matching wildcard
  certificate on the Gateway listener.
- **Components are not path-addressable.** In this scheme *all* of an
  InferenceService's routes (top-level, engine, router, decoder) render the
  same hostname and match at `/` — there is no per-component path. Address
  the service by its host; the top-level route's backend (router if present,
  otherwise engine) is the entry point.

## Switching schemes cluster-wide

Both fields live in the `ingress` key of the `inferenceservice-config`
ConfigMap, which the `ome-resources` Helm chart renders from
`ome.controller.ingressGateway`:

```yaml
# values.yaml (ome-resources)
ome:
  controller:
    ingressGateway:
      enableGatewayAPI: true
      disableIngressCreation: false
      domain: example.com                # → ingressDomain
      domainTemplate: "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}"
      # Default scheme: shared host + path prefix.
      perISVCSubdomain: false
      # Host label for the shared host: "<sharedHostPrefix>.<domain>".
      # Empty string => bare <domain>. No default in the binary; this chart
      # value is the source of truth.
      sharedHostPrefix: "llm"
```

The rendered ConfigMap fragment:

```yaml
data:
  ingress: |-
    {
      "enableGatewayAPI": true,
      "disableIngressCreation": false,
      "ingressDomain": "example.com",
      "domainTemplate": "{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}",
      "perISVCSubdomain": false,
      "sharedHostPrefix": "llm"
    }
```

To switch the whole cluster to per-ISVC subdomains, set
`perISVCSubdomain: true` (and make sure wildcard DNS and certificates are in
place). `sharedHostPrefix` is ignored while the per-ISVC scheme is active.
After changing the ConfigMap, restart the controller so it picks up the new
configuration, then verify the generated routes:

```bash
kubectl rollout restart deployment/ome-controller-manager -n ome

# Host and path of the generated routes
kubectl get httproute -n <namespace> \
  -o custom-columns='NAME:.metadata.name,HOSTS:.spec.hostnames[*],PATH:.spec.rules[0].matches[0].path.value'
```

Existing HTTPRoutes are reconciled to the new scheme; clients calling the old
hostname or path shape must move to the new one.

## Per-service overrides

A single InferenceService can deviate from the cluster default with two
annotations, which override the corresponding config fields for that service's
routes and `status.url`:

| Annotation                          | Overrides          | Semantics |
|-------------------------------------|--------------------|-----------|
| `ome.io/ingress-per-isvc-subdomain` | `perISVCSubdomain` | `"true"` enables the per-ISVC subdomain scheme for this service; any other value (including `"false"`) selects the shared-host scheme — so it works in both directions. |
| `ome.io/ingress-shared-host-prefix` | `sharedHostPrefix` | Applies whenever the annotation is **present**, so an explicit empty value means "no prefix" (bare `ingressDomain`) for this service. |

For example, to expose just one service on its own subdomain while the rest of
the cluster stays on the shared host:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: prod
  annotations:
    ome.io/ingress-per-isvc-subdomain: "true"
spec:
  model:
    name: llama-3-1-70b-instruct
  runtime:
    name: srt-llama-3-1-70b-instruct
```

Or to give one service a different shared-host label (its routes then use
`experimental.example.com` instead of `llm.example.com`, still with the
`/<namespace>/<service>/` path prefixes):

```yaml
metadata:
  annotations:
    ome.io/ingress-shared-host-prefix: "experimental"
```

The related `ome.io/ingress-domain` and `ome.io/ingress-domain-template`
annotations (see [Ingress Administration](/ome/docs/administration/ingress/))
also feed the per-ISVC subdomain rendering, since the host comes from
`domainTemplate`.

## Verifying which scheme is active

Inspect a generated route — the two schemes are easy to tell apart:

```bash
kubectl get httproute <isvc-name> -n <namespace> -o yaml
```

Shared-host scheme (default):

```yaml
spec:
  hostnames:
    - llm.example.com                  # shared across all services
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /prod/llama-chat/   # per-service path
      filters:
        - type: URLRewrite             # strips the prefix
          urlRewrite:
            path:
              type: ReplacePrefixMatch
              replacePrefixMatch: /
```

Per-ISVC subdomain scheme:

```yaml
spec:
  hostnames:
    - llama-chat.prod.example.com      # per-service host = status.url host
  rules:
    - matches:
        - path:
            type: PathPrefix
            value: /                   # root path, no URLRewrite filter
```

`kubectl get inferenceservice <name> -o jsonpath='{.status.url}'` shows the
externally callable URL under either scheme, since the status URL is derived
from the same host and path logic as the routes.

## Next steps

- [Ingress Administration](/ome/docs/administration/ingress/) — the full
  ingress configuration reference, including the annotation override list and
  Gateway API enablement.
- [Ingress and External Access](/ome/docs/concepts/ingress/) — user-facing
  concepts for reaching InferenceServices.
