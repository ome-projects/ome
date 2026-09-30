---
title: Use multiple gateways
description: "Attach OME's HTTPRoutes to more than one Gateway, such as an internal and an external one, and read the gateways' addresses in status."
since: v1.3
---

Serve your [InferenceServices](../../concepts/serving/inference-services.md) through more than one Gateway, such as an internal gateway for clients on your network and an external one for clients outside it. OME attaches a service's HTTPRoutes to all the gateways you list, each with its own hostname, and lists their addresses in the service's status. [Ingress and external access](../../concepts/rollouts-and-traffic/ingress.md) explains how OME builds the routes.

<div class="prerequisites" markdown>

- OME v1.3 or later, installed from the `ome-resources` chart as the Helm release `ome` in the namespace `ome`, and Helm and `kubectl` access to the cluster.
- Ingress creation turned on with Gateway API routing, and `internal-gateway` as the primary gateway. See [Configure ingress](configure-ingress.md).
- Two Gateways in `envoy-gateway-system`: `internal-gateway` for `internal.example.com`, and `external-gateway` for `external.example.com`. Each needs a listener that allows HTTPRoutes from every namespace that runs InferenceServices, `prod` in these examples, with a hostname that covers only its own domain: `*.internal.example.com` and `*.external.example.com`.
- An InferenceService `llama-chat` in `prod` whose engine is ready, with no router.

</div>

## Step 1: List the gateways in the Helm values

Give the primary gateway a class, which names its address in the InferenceService's status, and list the external gateway in `additionalIngressGateways`:

```yaml title="values.yaml"
ome:
  controller:
    ingressGateway:
      disableIngressCreation: false
      enableGatewayAPI: true
      omeIngressGateway: envoy-gateway-system/internal-gateway
      omeIngressGatewayClass: internal
      domain: internal.example.com
      additionalIngressGateways:
        - omeIngressGateway: envoy-gateway-system/external-gateway
          ingressDomain: external.example.com
          class: external
```

Entries in `additionalIngressGateways` have these fields, and `omeIngressGatewayClass` is the primary gateway's `class`:

| Field | What it sets |
| --- | --- |
| `omeIngressGateway` | The gateway, as `namespace/name`. Always set it. |
| `ingressDomain` | The domain of the service's hostname on this gateway. Always set it. |
| `class` | Optional. The name of the gateway's address: a lowercase RFC 1123 label. Avoid `cluster-local`, which names the in-cluster address. |

For a kustomize install, set the same keys in the ConfigMap, with `ingressDomain` for `domain`: see [Kustomize installs](configure-ingress.md#kustomize-installs).

With the default shared host, the service's hostnames are `llm.internal.example.com` and `llm.external.example.com`. [Choose a Gateway API host scheme](gateway-host-schemes.md) covers the other scheme.

!!! warning "Every InferenceService joins the external gateway"
    The upgrade attaches the routes of every InferenceService, engine routes included, to `external-gateway`, and a service is `Ready` only when all its gateways accept its routes. To keep a service off it, use [per-namespace gateways](namespace-gateways.md) or [an annotation](#override-the-gateways-for-one-service).

Add the values to your `ome-resources` values file, and upgrade the release. Keep the installed chart version, which `helm list -n ome` shows in its `CHART` column, so that OME doesn't move ahead of the `ome-crd` chart. Here it's `1.3.0`:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm prints `Release "ome" has been upgraded. Happy Helming!`, and the controller restarts. Check the gateways in the `inferenceservice-config` ConfigMap:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.ingress}' \
  | grep -E 'omeIngressGateway|additionalIngressGateways'
```

```output
    "omeIngressGateway" : "envoy-gateway-system/internal-gateway",
    "omeIngressGatewayClass" : "internal",
    "additionalIngressGateways": [{"class":"external","ingressDomain":"external.example.com","omeIngressGateway":"envoy-gateway-system/external-gateway"}],
```

Until both gateways accept the updated routes, the InferenceServices' `IngressReady` condition is `False` with the reason `ParentStatusNotAvailable`, or with a gateway's reason when it rejects a route. If the new controller pod keeps restarting, see [The controller keeps restarting after the upgrade](#the-controller-doesnt-start-after-the-upgrade).

## Step 2: Check the generated routes

List the gateways and hostnames on the routes of `llama-chat` and its engine:

```bash
kubectl get httproute llama-chat llama-chat-engine -n prod \
  -o custom-columns='NAME:.metadata.name,GATEWAYS:.spec.parentRefs[*].name,HOSTNAMES:.spec.hostnames[*]'
```

```output
NAME                GATEWAYS                            HOSTNAMES
llama-chat          internal-gateway,external-gateway   llm.internal.example.com,llm.external.example.com
llama-chat-engine   internal-gateway,external-gateway   llm.internal.example.com,llm.external.example.com
```

The routes list the primary gateway first, with one hostname per gateway in the same order. A router or a decoder gets its own route too, on the same gateways. OME creates a component's route only once the component is ready.

The listener hostnames from [Before you begin](#before-you-begin) keep each gateway to its own hostname: a listener with a hostname [ignores the route's hostnames that don't match it](https://gateway-api.sigs.k8s.io/api-types/httproute/#hostnames). A listener without one would serve both.

## Step 3: Read the gateway addresses

`status.addresses` lists an address for each gateway, named after the gateway's class if it has one, and then the in-cluster address:

```bash
kubectl get inferenceservice llama-chat -n prod \
  -o jsonpath='{range .status.addresses[*]}{.name}{": "}{.url}{"\n"}{end}'
```

```output
internal: http://llm.internal.example.com/prod/llama-chat/
external: http://llm.external.example.com/prod/llama-chat/
cluster-local: http://llama-chat-engine.prod.svc.cluster.local
```

The `cluster-local` entry points at the engine's Service, or the router's when there is one. All the entries use the scheme in `urlScheme`, `http` by default, even when only one gateway serves HTTPS.

`status.url` holds the primary gateway's address, and `status.address` the cluster-local one. To read one gateway's address, select its entry by name:

```bash
kubectl get inferenceservice llama-chat -n prod \
  -o jsonpath='{.status.addresses[?(@.name=="external")].url}'
```

```output
http://llm.external.example.com/prod/llama-chat/
```

An address is listed even when its gateway hasn't accepted the route, but `IngressReady` stays `False` until all the gateways have: see [A route is missing from a gateway](#a-route-is-missing-from-a-gateway). To send a request, use an address as in [Configure ingress](configure-ingress.md#step-3-send-a-request). The [`InferenceServiceStatus`](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-InferenceServiceStatus) reference lists the fields.

## Override the gateways for one service

These annotations change the gateways of one InferenceService. The two gateway annotations take precedence over [per-namespace gateways](namespace-gateways.md#per-service-annotations), even with an empty value.

| Annotation | Value | Effect |
| --- | --- | --- |
| `ome.io/ingress-gateway` | A gateway, as `namespace/name` | Replaces the primary gateway, which keeps the cluster-wide domain and class. An empty value keeps the cluster-wide gateway. |
| `ome.io/ingress-additional-gateways` | A JSON array of entries like those in `additionalIngressGateways` | Replaces the whole list of additional gateways. `[]` removes them all, and an empty value keeps the cluster-wide list. |
| `ome.io/ingress-domain` | A domain | Replaces the primary gateway's domain, or every gateway's with [per-service subdomains](gateway-host-schemes.md#per-service-subdomains). |

To keep `llama-chat` off the external gateway, set `ome.io/ingress-additional-gateways` to `[]`:

```bash
kubectl annotate inferenceservice llama-chat -n prod ome.io/ingress-additional-gateways='[]'
```

```output
inferenceservice.ome.io/llama-chat annotated
```

The routes now attach to the internal gateway only:

```bash
kubectl get httproute llama-chat llama-chat-engine -n prod \
  -o custom-columns='NAME:.metadata.name,GATEWAYS:.spec.parentRefs[*].name,HOSTNAMES:.spec.hostnames[*]'
```

```output
NAME                GATEWAYS           HOSTNAMES
llama-chat          internal-gateway   llm.internal.example.com
llama-chat-engine   internal-gateway   llm.internal.example.com
```

To expose only some services, leave `additionalIngressGateways` out of your values, and give those services the external gateway:

```yaml
metadata:
  annotations:
    ome.io/ingress-additional-gateways: '[{"omeIngressGateway": "envoy-gateway-system/external-gateway", "ingressDomain": "external.example.com", "class": "external"}]'
```

OME ignores a malformed value without an error, so check the routes after you set it, as in [Step 2](#step-2-check-the-generated-routes).

## Troubleshooting

### A route is missing from a gateway

First check which gateways the route names:

```bash
kubectl get httproute llama-chat -n prod \
  -o jsonpath='{range .spec.parentRefs[*]}{.namespace}{"/"}{.name}{"\n"}{end}'
```

```output
envoy-gateway-system/internal-gateway
envoy-gateway-system/external-gateway
```

Each line must match a Gateway's namespace and name. A configured gateway without a slash, like `external-gateway`, becomes the Gateway `external-gateway` in the namespace `external-gateway`.

Then check which gateways have processed it:

```bash
kubectl get httproute llama-chat -n prod \
  -o jsonpath='{range .status.parents[*]}{.parentRef.name}{":"}{range .conditions[*]}{" "}{.type}{"="}{.status}{end}{"\n"}{end}'
```

```output
internal-gateway: Accepted=True ResolvedRefs=True
external-gateway: Accepted=True ResolvedRefs=True
```

| What you see | Cause | Fix |
| --- | --- | --- |
| The gateway isn't in `spec.parentRefs` | An annotation on the service or a `namespaceIngressGateways` entry for `prod` replaced it, or a route update failed. | Check the service's [annotations](#override-the-gateways-for-one-service) and the [per-namespace gateways](namespace-gateways.md). For a failed update, see the log below. |
| The gateway has no line in `status.parents` | The Gateway is missing, or no controller manages it. `IngressReady` stays `False` with the reason `ParentStatusNotAvailable`. | Find the Gateway with `kubectl get gateway -A`, and check that its controller runs. |
| A condition is `False` | The gateway can't serve the route. `IngressReady` is `False` with the same reason. | Allow routes from `prod` in the gateway's listener, and match its hostname to the route's. See [IngressReady is False](namespace-gateways.md#ingressready-is-false). |

Search the controller's log for failed updates:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1 | grep 'fails to reconcile ingress'
```

Each line is a failed update and gives the reason.

### The routes ignore the annotation

If the routes still list the cluster-wide additional gateways after you set `ome.io/ingress-additional-gateways`, its value is empty or OME couldn't parse it. It must be a JSON array, even for one gateway, with its keys and strings in double quotes. OME ignores unknown keys, so a misspelled key, such as `gateway` for `omeIngressGateway`, leaves that field empty. An empty gateway or domain makes the route update fail: see [A route is missing from a gateway](#a-route-is-missing-from-a-gateway). Fix the value, and check the routes again as in [Step 2](#step-2-check-the-generated-routes).

### The controller keeps restarting after the upgrade {#the-controller-doesnt-start-after-the-upgrade}

If the new controller pod logs `Failed to initialize ingress configuration` with an `invalid ingress config` error, a class isn't a lowercase RFC 1123 label. The error names the field, such as `additionalIngressGateways[0].class`. Until you fix it, the old controller pods stop creating and updating routes, the Deployments of RawDeployment components, and the LeaderWorkerSets of `MultiNode` (deprecated) ones. Fix the class in your values file, and run the upgrade in [Step 1](#step-1-list-the-gateways-in-the-helm-values) again.

## Clean up

Remove the annotation from `llama-chat`:

```bash
kubectl annotate inferenceservice llama-chat -n prod ome.io/ingress-additional-gateways-
```

```output
inferenceservice.ome.io/llama-chat annotated
```

To take all services off the external gateway, delete `additionalIngressGateways` from your values file, and run the upgrade in [Step 1](#step-1-list-the-gateways-in-the-helm-values) again.

## Next steps

- [Use per-namespace gateways](namespace-gateways.md): attach the routes of one namespace to its own gateways.
- [Choose a Gateway API host scheme](gateway-host-schemes.md): serve each InferenceService on its own hostname instead of a path on a shared one.
