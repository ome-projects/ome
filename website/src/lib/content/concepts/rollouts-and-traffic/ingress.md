---
title: Ingress and external access
description: "How clients outside the cluster reach an InferenceService: the Service that OME creates by default, or the Kubernetes Ingress or Gateway API HTTPRoutes it creates when you turn on ingress."
---

With ingress creation on, OME exposes each [InferenceService](../serving/inference-services.md) outside the cluster through a [Kubernetes Ingress](https://kubernetes.io/docs/concepts/services-networking/ingress/) or [Gateway API](https://gateway-api.sigs.k8s.io/) HTTPRoutes, and reports the address that clients use in `status.url`. Ingress creation is off by default. Until you turn it on, OME puts a Service in front of the model, and you decide how to expose it.

You set ingress with the `ome-resources` chart values under `ome.controller.ingressGateway`, which the chart writes to the `inferenceservice-config` ConfigMap in the `ome` namespace. [Configure ingress](../../guides/networking/configure-ingress.md) walks through the setup and lists every setting. The examples use an InferenceService named `llama-chat` in the namespace `llama-demo`.

## What OME creates for external access

Two of the values decide what OME creates for each InferenceService:

| `disableIngressCreation` | `enableGatewayAPI` | What OME creates | Use it when |
| --- | --- | --- | --- |
| `true`, the default | Either | [The external Service](#the-external-service) | You expose the model yourself, or keep it inside the cluster. |
| `false` | `false`, the default | [A Kubernetes Ingress](#through-a-kubernetes-ingress) | You run an ingress controller, such as ingress-nginx. |
| `false` | `true` | [HTTPRoutes](#through-the-gateway-api), and a default BackendTrafficPolicy | You run Envoy Gateway. You also get a shared hostname, several Gateways and route timeouts. |

The annotation `ome.io/ingress-disable-creation` overrides `disableIngressCreation` for one InferenceService: `"true"` turns ingress creation off, and any other value turns it on. `enableGatewayAPI` has no annotation, so it applies to every InferenceService. [Per-service overrides](../../guides/networking/configure-ingress.md#per-service-overrides) lists the annotations for the other settings.

OME copies the InferenceService's annotations to its Ingress or HTTPRoutes, so annotations for your ingress controller or Gateway go on the InferenceService. OME updates a route's annotations only when the route's spec changes too.

HTTPRoutes need v1.3 or later. On v1.2.2, `enableGatewayAPI: true` makes the ingress step fail for every InferenceService, so use an Ingress.

Turning ingress creation on deletes the external Service, so move clients in the cluster to a route, or to a component's Service such as `llama-chat-engine`. Turning it off, or switching between an Ingress and HTTPRoutes, leaves the old Ingress or HTTPRoutes serving traffic until you delete them, as [Turn off ingress creation](../../guides/networking/configure-ingress.md#turn-off-ingress-creation) shows.

The Ingress and HTTPRoutes send requests to the Services that OME creates for the InferenceService's [components](../serving/inference-services.md#components): `<name>-engine`, `<name>-decoder` and `<name>-router`. So they work the same for [OMENative](../omenative/overview.md) components as for `RawDeployment` ones. Keep InferenceService names to 56 characters, or 55 with a decoder. With longer names, the routes point at Services that don't exist.

## The external Service

With ingress creation off, OME puts a `ClusterIP` Service named after the InferenceService in front of its router, or its engine when there's no router. [Cluster-local](#cluster-local-services) InferenceServices don't get one. The Service listens on the runtime's port, 8080 for `llama-chat`.

To reach the model from outside the cluster, set the annotation `ome.io/service-type` to `LoadBalancer` or `NodePort`, or put your own Ingress or Gateway in front of the Service. [Expose a service without ingress](../../guides/networking/expose-without-ingress.md) walks through it.

OME treats any Service with the InferenceService's name as its own, and rewrites or deletes it, so give your own Services other names.

## Through a Kubernetes Ingress

OME creates one Ingress, named after the InferenceService, with the IngressClass in `ome.controller.ingressGateway.ingressGateway.className`. The chart sets it to `istio`, so change it to your ingress controller's class, such as `nginx`. Each rule matches the path prefix `/` on a hostname of its own.

### Hostnames {#hostnames-and-urls}

With `domain: example.com`, the Ingress for `llama-chat` has these hostnames:

| Hostname | Sends requests to | When |
| --- | --- | --- |
| `llama-chat.llama-demo.example.com` | `llama-chat-router`, or `llama-chat-engine` when there's no router | Always |
| `llama-chat-router.llama-demo.example.com` | `llama-chat-router` | With a router |
| `llama-chat-engine.llama-demo.example.com` | `llama-chat-engine` | With a router or a decoder |
| `llama-chat-decoder.llama-demo.example.com` | `llama-chat-decoder` | With a decoder |

The hostnames come from `domainTemplate`, a Go template that defaults to `{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}`. In the template, `.Name` is the InferenceService's name for the top-level hostname, and the component's Service name for the others.

## Through the Gateway API {since=v1.3}

To create HTTPRoutes, set these values in the `ome-resources` chart:

```yaml
ome:
  controller:
    ingressGateway:
      disableIngressCreation: false
      enableGatewayAPI: true
      domain: example.com
      omeIngressGateway: envoy-gateway-system/internal-gateway
```

- `domain` is the domain that the hostnames end in. The chart sets `svc.cluster.local`, so set a domain that your clients can resolve.
- `omeIngressGateway` is the Gateway that the HTTPRoutes attach to, as `<namespace>/<name>`. The chart leaves it empty, and the API server rejects HTTPRoutes without it.

Turning on `enableGatewayAPI` takes a controller restart. `helm upgrade` restarts the controller when the values change. If you edit the ConfigMap directly instead, restart it yourself:

```bash
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

HTTPRoutes also need:

- a Gateway listener whose `allowedRoutes.namespaces.from` is `All`, or a `Selector` that matches the InferenceService's namespace. The default, `Same`, admits only routes in the Gateway's namespace;
- the Gateway API CRDs. If you install them after the controller starts, restart it, or `IngressReady` lags behind the Gateway;
- Envoy Gateway v1.7 or later, for the default BackendTrafficPolicy that OME writes for an InferenceService without [traffic intent](traffic-policy.md#defaults-when-you-declare-nothing).

If OME can't write a route or the policy, it still creates the workloads, but the InferenceService's status stops updating. [Troubleshooting](../../guides/networking/configure-ingress.md#troubleshooting) shows how to find the cause in the controller's log.

### Routes

For `llama-chat`, OME creates up to four HTTPRoutes, all with the hostname `llm.example.com` in the default scheme:

| HTTPRoute | Path prefix | Sends requests to |
| --- | --- | --- |
| `llama-chat` | `/llama-demo/llama-chat/` | `llama-chat-router`, or `llama-chat-engine` when there's no router |
| `llama-chat-engine` | `/llama-demo/llama-chat-engine/` | `llama-chat-engine` |
| `llama-chat-router` | `/llama-demo/llama-chat-router/` | `llama-chat-router`, with a router |
| `llama-chat-decoder` | `/llama-demo/llama-chat-decoder/` | `llama-chat-decoder`, with a decoder |

Clients use the top-level route, `llama-chat`. The other routes each reach one component, bypassing the router. OME creates a component's route once the component is ready.

Each route rewrites its path prefix to `/`, so a request for `/llama-demo/llama-chat/v1/models` reaches the model server as `/v1/models`. It also sets the headers `OMe-Isvc-Name` and `OME-Isvc-Namespace` to the InferenceService's name and namespace.

A route's request timeout is its component's `timeoutSeconds`, such as `spec.engine.timeoutSeconds`, or else `defaultRouteTimeoutSeconds`. The chart sets `defaultRouteTimeoutSeconds` to `0`, which turns the timeout off. With `null`, the Gateway's default applies, and Envoy's 15 seconds cuts long generations short. `timeoutSeconds` applies only to HTTPRoutes. [Configure route timeouts](../../guides/networking/configure-route-timeouts.md) has examples.

### Hostnames

OME builds the hostnames in one of two schemes:

- In the shared scheme, the default, every HTTPRoute has the hostname `<sharedHostPrefix>.<domain>`, which is `llm.example.com` with the chart's `sharedHostPrefix: llm`, and a path prefix of its own.
- In the per-service scheme, which `perISVCSubdomain: true` turns on, an InferenceService's HTTPRoutes use a hostname from `domainTemplate`, such as `llama-chat.llama-demo.example.com`, and match the path prefix `/` with no rewrite.

!!! warning "Routes overlap in the per-service scheme"
    In the per-service scheme, all the routes of an InferenceService match the same hostname and path, and one of them takes every request. With a router or a decoder, requests can reach the wrong component. Keep those InferenceServices on the shared scheme, as [Choose a Gateway API host scheme](../../guides/networking/gateway-host-schemes.md) describes.

`additionalIngressGateways` attaches the routes to more Gateways, each with its own domain, and `namespaceIngressGateways` picks the Gateways for the InferenceServices in a namespace. See [Use multiple gateways](../../guides/networking/multiple-gateways.md) and [Use per-namespace gateways](../../guides/networking/namespace-gateways.md).

## Send requests

With the values in [Through the Gateway API](#through-the-gateway-api), create `llama-chat`. It serves the pre-configured model `llama-3-2-1b-instruct`, as [A minimal InferenceService](../serving/inference-services.md#a-minimal-inferenceservice) does:

```yaml title="llama-chat.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  engine: {}
```

Apply the file:

```bash
kubectl apply -f llama-chat.yaml
```

```output
Warning: Runtime vllm-llama-3-2-1b-instruct will be auto-selected for model llama-3-2-1b-instruct
inferenceservice.ome.io/llama-chat created
```

Once the engine is ready, OME creates two HTTPRoutes: `llama-chat`, which clients use, and `llama-chat-engine`. It also writes a default BackendTrafficPolicy named `llama-chat`. List the routes:

```bash
kubectl get httproutes -n llama-demo
```

```output
NAME                HOSTNAMES             AGE
llama-chat          ["llm.example.com"]   4m
llama-chat-engine   ["llm.example.com"]   4m
```

Read the address from the status:

```bash
kubectl get inferenceservice llama-chat -n llama-demo -o jsonpath='{.status.url}'
```

```output
http://llm.example.com/llama-demo/llama-chat/
```

`llm.example.com` must resolve to your Gateway's address, through DNS or an entry in `/etc/hosts`. List the models that the InferenceService serves:

```bash
curl -s http://llm.example.com/llama-demo/llama-chat/v1/models | jq -r '.data[].id'
```

```output
vllm-model
```

The runtime serves the model as `vllm-model`, which it sets with `--served-model-name`. Send chat completions to `http://llm.example.com/llama-demo/llama-chat/v1/chat/completions`, with `"model": "vllm-model"` in the body.

## Status

For `llama-chat`, with `domain: example.com` and the chart's other values, `status.url` is:

| What OME creates | `status.url` |
| --- | --- |
| The external Service | `http://llama-chat.llama-demo.svc.cluster.local:8080` |
| A Kubernetes Ingress | `http://llama-chat.llama-demo.example.com:8080` |
| HTTPRoutes | `http://llm.example.com/llama-demo/llama-chat/` |

- With an Ingress, the port is the router's or the engine's Service port. Send requests to your ingress controller's port instead.
- With HTTPRoutes, `status.url` is the address on the primary Gateway. With more Gateways, `status.addresses` lists the address on each, as [Use multiple gateways](../../guides/networking/multiple-gateways.md) shows.
- With HTTPRoutes, send requests to `status.url`. The routes don't serve the component URLs in `status.components`.

### When IngressReady is True

The InferenceService is `Ready` only when `IngressReady` is `True`, as [Status](../serving/inference-services.md#status) describes:

| What OME creates | `IngressReady` is `True` when |
| --- | --- |
| The external Service | Always, with the reason `IngressDisabled`. |
| A Kubernetes Ingress | OME has created the Ingress. It waits for the router, or the decoder when there's no router, or else the engine. It doesn't check your ingress controller. |
| HTTPRoutes | Every declared component is ready. The Gateway has accepted every HTTPRoute, with no `False` condition. The Gateway can reject the default BackendTrafficPolicy without changing `IngressReady`. |

When `IngressReady` is `False`, its reason and message say why:

| Reason | Message | Meaning |
| --- | --- | --- |
| `ComponentNotReady` | `Target service not ready for ingress creation`, or `engine component not ready for HTTPRoute creation` | OME creates the Ingress or the HTTPRoute once the component is ready. |
| `ParentStatusNotAvailable` | `engine HTTPRoute awaiting gateway programming`, or `Engine HttpRouteNotReady` | The Gateway hasn't reported on the HTTPRoute yet. |
| The Gateway's reason, such as `NotAllowedByListeners` | `Engine`, then the Gateway's message | The Gateway reports a `False` condition on the HTTPRoute. |

The messages name the component, the engine in these examples. [Troubleshooting](../../guides/networking/configure-ingress.md#troubleshooting) in Configure ingress says what to do.

## Security

OME doesn't set up TLS. Terminate it at your Gateway's listeners or in your ingress controller. The `urlScheme` value and the annotation `ome.io/ingress-url-scheme` change only the scheme that OME reports. Set `https` when you terminate TLS, so that `status.url` matches.

Nothing authenticates requests by default. To control who can send them, use your Gateway's or ingress controller's authentication.

!!! warning "A LoadBalancer Service exposes the model to anyone who can reach it"
    With `ome.io/service-type: LoadBalancer`, the external Service gets an address outside the cluster, often a public one, and nothing authenticates the requests sent to it. Keep the load balancer internal with your cloud's annotations, which OME copies to the Service, or put an authenticating gateway in front of the model.

## Cluster-local services

The label `networking.knative.dev/visibility: cluster-local` stops OME from creating the external Service, and makes OME delete an existing one. The label doesn't stop an Ingress or HTTPRoutes. To keep an InferenceService inside the cluster whatever the config says, add the label and the annotation `ome.io/ingress-disable-creation: "true"`. OME then sets no `status.url`, and clients in the cluster use the router's or the engine's Service, such as `http://llama-chat-engine.llama-demo.svc.cluster.local:8080`.

On v1.2.2, while ingress creation is off, a cluster-local InferenceService never reports `Ready`. Every reconcile fails after OME creates or updates its serving workload, and OME records an `InternalError` warning event. This is a known bug. With ingress creation off, the external Service is a `ClusterIP` Service, so on v1.2.2, leave the label off: the model stays inside the cluster without it.

## Next steps

- [Configure ingress](../../guides/networking/configure-ingress.md): turn on an Ingress or HTTPRoutes, and set the domain, the Gateway and the other settings.
- [Expose a service without ingress](../../guides/networking/expose-without-ingress.md): make the external Service a `LoadBalancer` or `NodePort` Service.
- [Choose a Gateway API host scheme](../../guides/networking/gateway-host-schemes.md): pick the shared or the per-service scheme.
- [Configure route timeouts](../../guides/networking/configure-route-timeouts.md): keep long generations from timing out.
- [Traffic policy](traffic-policy.md): set load balancing and session affinity at the gateway.
