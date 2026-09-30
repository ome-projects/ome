---
title: Configure route timeouts
description: "Set the request timeout on OME's HTTPRoutes for one InferenceService or the whole cluster, or turn it off so long generations can finish."
since: v1.3
---

Set how long the gateway gives an [InferenceService](../../concepts/serving/inference-services.md) to answer, so long generations and streamed responses can finish. OME puts this request timeout on the Gateway API [HTTPRoutes](../../concepts/rollouts-and-traffic/ingress.md#routes) that it creates. You set it for one InferenceService with a component's `timeoutSeconds`, or for the whole cluster with `defaultRouteTimeoutSeconds`. The timeout covers the whole request and response, so a stream must finish within it. The chart's default, `0`, turns the timeout off.

<div class="prerequisites" markdown>

- OME v1.3 or later, installed with the `ome-resources` chart as the release `ome` in the namespace `ome`, and Helm and `kubectl` access to the cluster. Until v1.3 is released, run [a build of `main`](../../getting-started/install.md#install-from-source).
- Ingress creation turned on with Gateway API routing, as in [Configure ingress](configure-ingress.md). Both are off by default. With a Kubernetes Ingress instead, OME sets no request timeout.
- Envoy Gateway, running your Gateway. [Through the Gateway API](../../concepts/rollouts-and-traffic/ingress.md#through-the-gateway-api) lists the version that OME needs, and what else HTTPRoutes need.
- The InferenceService `qwen3-0-6b` in the namespace `qwen3-0-6b`, Ready. [Serve your first model](../../getting-started/serve-your-first-model.md) creates it.

</div>

## Where a route's timeout comes from

Each HTTPRoute takes its timeout from the component that it sends traffic to, using the first of these that's set:

1. `timeoutSeconds` on the component: `spec.engine`, `spec.router` or `spec.decoder`.
2. `defaultRouteTimeoutSeconds`, the cluster-wide default in the `ingress` entry of the `inferenceservice-config` ConfigMap.
3. Otherwise, the route has no timeout of its own, and the gateway's default applies: 15 seconds on Envoy Gateway.

Both settings take whole seconds from 0 to 99999. The routes of `qwen3-0-6b` take their timeouts from these fields:

| HTTPRoute | Timeout from |
| --- | --- |
| `qwen3-0-6b`, which clients use | `spec.router.timeoutSeconds` with a router, otherwise `spec.engine.timeoutSeconds` |
| `qwen3-0-6b-engine` | `spec.engine.timeoutSeconds` |
| `qwen3-0-6b-router` | `spec.router.timeoutSeconds`, with a router |
| `qwen3-0-6b-decoder` | `spec.decoder.timeoutSeconds`, with a decoder |

`0` turns the timeout off, while leaving both settings unset gives the gateway's default. The `ome-resources` chart sets `defaultRouteTimeoutSeconds` to `0`, so after a Helm install, the timeout is off unless a component sets its own. The ConfigMap in the kustomize manifests leaves the setting out, so the gateway's default applies.

Turn the timeout off for long generations and streamed responses whose length you can't predict, or keep a limit longer than your longest request. With `0`, OME writes `0s` on the route, which Gateway API asks implementations to treat as no timeout, or as the longest one they support.

The `ome.io/timeout-idle`, `ome.io/timeout-max-connection-duration` and `ome.io/timeout-tcp-connect` annotations set connection timeouts, not the request timeout. See [Timeout annotations](../../reference/api/traffic-annotations.md#timeout-annotations).

## Step 1: Set the timeout for one InferenceService

Set `timeoutSeconds` on a component to give its routes their own timeout. This is the InferenceService `qwen3-0-6b` from [Before you begin](#before-you-begin), with a 30-minute timeout on the engine:

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
    timeoutSeconds: 1800
```

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/qwen3-0-6b configured
```

This InferenceService has no router, so the engine's value applies to both its routes, `qwen3-0-6b-engine` and `qwen3-0-6b`. With a router, also set `spec.router.timeoutSeconds`: the top-level route sends traffic to the router. Changing `timeoutSeconds` updates only the routes, so the engine's pods keep running.

List the InferenceService's HTTPRoutes with the timeout of each:

```bash
kubectl get httproute -n qwen3-0-6b \
  -o 'custom-columns=NAME:.metadata.name,TIMEOUT:.spec.rules[*].timeouts.request'
```

```output
NAME                TIMEOUT
qwen3-0-6b          1800s
qwen3-0-6b-engine   1800s
```

`0s` means that the timeout is off. `<none>` means that the route has no timeout of its own, so the gateway's default applies.

To go back to the cluster-wide default, remove `timeoutSeconds` and apply the file again.

## Step 2: Set a cluster-wide default

For a cluster-wide limit, set `defaultRouteTimeoutSeconds` under `ome.controller.ingressGateway` in the values file that you install `ome-resources` with. This example allows 10 minutes:

```yaml title="values.yaml"
ome:
  controller:
    ingressGateway:
      disableIngressCreation: false
      enableGatewayAPI: true
      omeIngressGateway: envoy-gateway-system/ome-gateway
      defaultRouteTimeoutSeconds: 600
```

Replace `envoy-gateway-system/ome-gateway` with your Gateway. Keep your other install values in the file too: `helm upgrade` resets any value you leave out to the chart's default.

`helm list -n ome` shows the installed version in its `CHART` column. Upgrade the release with the chart version you already run, 1.3.0 in this example:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Until v1.3 is released, upgrade from [a checkout of `main`](../../getting-started/install.md#install-from-source) instead.

Helm prints `Release "ome" has been upgraded. Happy Helming!`. The upgrade restarts the controller, which then applies the new default to every route whose component sets no `timeoutSeconds`.

With a kustomize install, add `"defaultRouteTimeoutSeconds": 600` to the `ingress` entry of the `inferenceservice-config` ConfigMap instead, and restart the controller, as [Kustomize installs](configure-ingress.md#kustomize-installs) shows. Keep the entry's `ingressGateway` and `ingressService` keys, even with Gateway API, or the restarted controller exits.

Check the ConfigMap:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.ingress}' | grep defaultRouteTimeoutSeconds
```

```output
    "defaultRouteTimeoutSeconds": 600,
```

## Troubleshooting

### Long requests time out

Check the routes with the command in [Step 1](#step-1-set-the-timeout-for-one-inferenceservice), and find the `TIMEOUT` value:

| `TIMEOUT` | Cause | Fix |
| --- | --- | --- |
| `<none>` | The route has no timeout of its own, so the gateway's default applies: 15 seconds on Envoy Gateway. | Set a timeout, as in [Step 1](#step-1-set-the-timeout-for-one-inferenceservice) or [Step 2](#step-2-set-a-cluster-wide-default). |
| Shorter than your longest request | The timeout is too short. | Raise the component's `timeoutSeconds`, or the default if the component doesn't set one. |
| What you expect, or `0s` | Another limit on the request's path, such as a load balancer in front of the gateway, or the client's own timeout. | Raise that limit. |

### The route keeps its old timeout

A component's `timeoutSeconds` wins over `defaultRouteTimeoutSeconds`. `kubectl get inferenceservice qwen3-0-6b -n qwen3-0-6b -o jsonpath='{.spec.engine.timeoutSeconds}'` prints the engine's value, or nothing when it isn't set.

If you edited the ConfigMap by hand, restart the controller to apply the change to every InferenceService:

```bash
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

If a `timeoutSeconds` is negative, or either setting is above 99999, the API server rejects the route. The InferenceService can still show Ready, with no event, but the controller's log shows the error:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'fails to reconcile ingress'
```

The error names the route, as in `failed to perform dry-run update for engine HttpRoute qwen3-0-6b-engine`, followed by the API server's reason. Set a value in range, and OME retries on its own. To turn the timeout off, use `0`.

### No HTTPRoutes for the InferenceService

If `kubectl get ingress -n qwen3-0-6b` lists `qwen3-0-6b`, either `enableGatewayAPI` is `false`, or that Ingress is left from before you set it to `true` and serves until you delete it. For other causes, see [OME doesn't create a route](configure-ingress.md#ome-doesnt-create-a-route).

## Next steps

- [Configure ingress](configure-ingress.md): choose between Kubernetes Ingress and Gateway API, and set the rest of the `ingress` entry.
- [Traffic annotations](../../reference/api/traffic-annotations.md): the `ome.io` annotations that tune circuit breaking, retries and connection timeouts for an InferenceService's backends.
