---
title: "Configure Request Timeouts on Generated Routes"
linkTitle: "Configure Route Timeouts"
weight: 20
date: 2026-09-26
description: >
  Control the Gateway API request timeout on the HTTPRoutes OME generates, and avoid Envoy Gateway's built-in 15-second default truncating long-running inference.
---

This page shows you how request timeouts are set on the HTTPRoutes that OME generates for an InferenceService, how to change the timeout per component or cluster-wide, and how to disable it entirely. It applies to Gateway API mode (`enableGatewayAPI: true` in the ingress configuration); the Kubernetes Ingress and Istio VirtualService paths do not carry an OME-managed request timeout.

The one thing to internalize up front: **an omitted timeout is not "unbounded"**. When OME emits no `timeouts` block on a route rule, your gateway's own default applies — and Envoy Gateway's built-in route timeout is **15 seconds**, which truncates long generations and streaming responses mid-flight. To get an unbounded request, the route must carry an *explicit* `"0s"` timeout, which Gateway API defines as "disable the request timeout". OME gives you both knobs and never hardcodes a value of its own.

## Before you begin

You need to have the following:

- A Kubernetes cluster with OME installed and Gateway API ingress enabled (`enableGatewayAPI: true` in the `ingress` block of the `inferenceservice-config` ConfigMap)
- A Gateway API implementation such as Envoy Gateway
- `kubectl` configured to communicate with your cluster

See [Ingress Administration](/ome/docs/administration/ingress/) for the general ingress configuration reference.

## How the timeout is resolved

Every rule of a generated HTTPRoute gets its `spec.rules[].timeouts.request` value from a three-level resolution, highest priority first:

1. **Per-component override** — `spec.engine.timeoutSeconds`, `spec.router.timeoutSeconds`, or `spec.decoder.timeoutSeconds` on the InferenceService. If set, this value wins and is emitted as `"<n>s"`.
2. **Cluster default** — `defaultRouteTimeoutSeconds` in the `ingress` block of the `inferenceservice-config` ConfigMap. Any value `>= 0` is emitted as `"<n>s"`; `0` emits `"0s"`, which explicitly disables the request timeout.
3. **Nothing configured** — if the per-component field is unset and `defaultRouteTimeoutSeconds` is absent (or negative), OME leaves `timeouts` off the route rule entirely. The gateway's own default then applies: for Envoy Gateway that is the built-in 15-second route timeout, **not** unbounded.

The OME binary has no built-in default for `defaultRouteTimeoutSeconds` — the value always comes from the ConfigMap. The `ome-resources` Helm chart supplies `defaultRouteTimeoutSeconds: 0` out of the box, so on a chart-installed cluster the request timeout is **disabled by default** (recommended for inference, where generations and streams routinely run past any fixed budget; the gateway's idle timeout still reaps dead connections). Setting the chart value to `null` omits the field from the ConfigMap, which lands you on level 3 — the Envoy 15s default.

### Which routes get which value

In Gateway API mode OME generates one HTTPRoute per deployed component plus a top-level route:

| HTTPRoute         | Created when          | `timeoutSeconds` field consulted                          |
|-------------------|-----------------------|-----------------------------------------------------------|
| `<isvc>-engine`   | Always                | `spec.engine.timeoutSeconds`                              |
| `<isvc>-router`   | `spec.router` is set  | `spec.router.timeoutSeconds`                              |
| `<isvc>-decoder`  | `spec.decoder` is set | `spec.decoder.timeoutSeconds`                             |
| `<isvc>` (top-level) | Always             | Router's value if a router is deployed, otherwise the engine's |

Each route falls back independently to the cluster default when its component field is unset.

## Set a cluster-wide default

Through the `ome-resources` Helm chart:

```yaml
ome:
  controller:
    ingressGateway:
      # Applied to every generated route whose InferenceService doesn't set
      # spec.<component>.timeoutSeconds. 0 disables the request timeout.
      defaultRouteTimeoutSeconds: 600
```

Or directly in the `inferenceservice-config` ConfigMap:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: inferenceservice-config
  namespace: ome
data:
  ingress: |
    {
      "enableGatewayAPI": true,
      "defaultRouteTimeoutSeconds": 600
    }
```

The controller re-reads the ConfigMap through a short-lived cache, so no restart is needed for new InferenceServices. An existing route only changes when its InferenceService is next reconciled; restart the controller (`kubectl rollout restart deployment/ome-controller-manager -n ome`) to force a refresh of all routes at once.

## Override the timeout for one service

Set `timeoutSeconds` on the component whose route you want to change:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-batch
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
    timeoutSeconds: 1800   # 30 minutes for this service's routes
```

Because no router is deployed here, both the `llama-batch-engine` route and the top-level `llama-batch` route get `request: 1800s`. With a router, set `spec.router.timeoutSeconds` as well — the top-level route follows the router, and the engine route keeps following `spec.engine.timeoutSeconds`.

## Disable the timeout entirely

Set the value to `0` — cluster-wide or per component:

```yaml
spec:
  engine:
    timeoutSeconds: 0
```

This emits an explicit `request: "0s"` on the route rule, which Gateway API implementations (including Envoy Gateway) treat as "no request timeout". Do **not** try to disable the timeout by removing the field and the cluster default: an absent `timeouts` block means the gateway's own 15s default takes over.

## Verify

Inspect the generated route:

```bash
kubectl get httproute llama-batch-engine -n llama-demo \
  -o jsonpath='{.spec.rules[0].timeouts}'
```

Expected output with a resolved timeout of 1800 seconds:

```
{"request":"1800s"}
```

With the timeout disabled you see `{"request":"0s"}`. If the command prints nothing at all, no timeout is configured at either level — long-running requests through Envoy Gateway will be cut off after 15 seconds with a `504`; set `defaultRouteTimeoutSeconds` (or a per-component `timeoutSeconds`) to `0` or an explicit budget.

## Related but different: connection-timeout annotations

The route request timeout described here bounds a single HTTP request end to end. The `ome.io/timeout-*` InferenceService annotations (for example `ome.io/timeout-idle`) are a separate mechanism: they configure connection-level timeouts on the gateway's backend traffic policy and do not affect `spec.rules[].timeouts.request` on the HTTPRoute.

## Next steps

- Review [Ingress Administration](/ome/docs/administration/ingress/) for the rest of the `ingress` configuration block
- See [Ingress and External Access](/ome/docs/concepts/ingress/) for how routes map to components
