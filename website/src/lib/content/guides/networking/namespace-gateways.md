---
title: Use per-namespace gateways
description: "Route each namespace's InferenceServices through that namespace's own Gateway, and override the gateway for a single service with annotations."
since: v1.3
---

Give the [InferenceServices](../../concepts/serving/inference-services.md) in one namespace their own Gateway and domain, so that their listeners, TLS certificate and DNS stay separate from the rest of the cluster. You map the namespace `prod` to the Gateway `prod-gw` in the Helm values, check its routes, and then override the Gateways of one InferenceService with an annotation. By default, every InferenceService's [HTTPRoutes](../../concepts/rollouts-and-traffic/ingress.md) attach to the cluster's Gateway.

<div class="prerequisites" markdown>

- OME v1.3 or later, installed with the `ome-resources` chart in the namespace `ome`, and `kubectl` access to the cluster. See [Install OME](../../getting-started/install.md).
- Ingress creation turned on with the Gateway API. It's off by default. See [Configure ingress](configure-ingress.md).
- An InferenceService in `prod` whose engine is ready. The examples use `llama-chat`, which has no router.
- Envoy Gateway, with two Gateways in the namespace `envoy-gateway-system`: `shared-gw`, the cluster's Gateway, for the domain `example.com`, and `prod-gw` for `prod.example.com`.
- A DNS record that sends `llm.prod.example.com` to `prod-gw`. `kubectl get gateway prod-gw -n envoy-gateway-system` shows the Gateway's address.
- A listener on `prod-gw` that allows routes from `prod`. By default, a listener allows routes only from its own Gateway's namespace. This listener does, for any host in `prod.example.com`:

```yaml
listeners:
  - name: http
    protocol: HTTP
    port: 80
    hostname: "*.prod.example.com"
    allowedRoutes:
      namespaces:
        from: Selector
        selector:
          matchLabels:
            kubernetes.io/metadata.name: prod
```

</div>

## Step 1: Map `prod` to its Gateway {#step-1-map-namespaces-to-gateways}

Add an entry for `prod` to your `ome-resources` values file. With Gateway API routing from [Configure ingress](configure-ingress.md), the ingress values look like this:

```yaml title="values.yaml"
ome:
  controller:
    ingressGateway:
      disableIngressCreation: false
      enableGatewayAPI: true
      domain: example.com
      omeIngressGateway: envoy-gateway-system/shared-gw
      namespaceIngressGateways:
        prod:
          primary:
            omeIngressGateway: envoy-gateway-system/prod-gw
            ingressDomain: prod.example.com
```

`namespaceIngressGateways` maps a namespace to an entry for its InferenceServices. `primary` and each Gateway in `additional` take the last three fields:

| Key | What it sets |
| --- | --- |
| The key, here `prod` | The InferenceServices' namespace, not the Gateway's. |
| `primary` | Replaces the cluster-wide `omeIngressGateway`, `domain` and `omeIngressGatewayClass` together, when it sets `omeIngressGateway`. A field you leave out stays empty, even if the cluster-wide value is set. |
| `additional` | Optional. More Gateways, after the primary, in place of the cluster-wide `additionalIngressGateways`. Set it to `[]` for none, or leave it out to keep the cluster-wide list. |
| `omeIngressGateway` | The Gateway, as `namespace/name`. |
| `ingressDomain` | The domain of the host on this Gateway. Always set it. |
| `class` | Optional. Names this Gateway's address in `status.addresses`, such as `internal`. It must be a lowercase RFC 1123 label. |

Give the namespace a domain of its own, so that DNS can send its host to `prod-gw`. With the default shared host, the InferenceServices in `prod` share the host `llm.prod.example.com`, and those in other namespaces stay on `shared-gw` at `llm.example.com`. With [per-service subdomains](gateway-host-schemes.md#per-service-subdomains) and the default `domainTemplate`, the host already names the namespace, so the entry can reuse the cluster-wide domain: `ingressDomain: example.com` gives `llama-chat.prod.example.com`.

For HTTPS, add a certificate for `*.prod.example.com` to an HTTPS listener on `prod-gw`. The URLs in status take their scheme from the cluster-wide `urlScheme`, or from an InferenceService's `ome.io/ingress-url-scheme` annotation.

!!! warning "Existing routes move"
    The upgrade moves the HTTPRoutes of every InferenceService in `prod` from `shared-gw` to `prod-gw` in place, and changes their host from `llm.example.com` to `llm.prod.example.com`. Tell clients about the new URLs before you upgrade.

Upgrade at the installed chart version, so that OME doesn't move ahead of the `ome-crd` chart. The `CHART` column of `helm list -n ome` shows it, here `ome-resources-1.3.0`:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm prints `Release "ome" has been upgraded. Happy Helming!` and the new revision. The upgrade restarts the controller, which then applies the entry to the InferenceServices in `prod`.

Check the entry that the controller reads:

```bash
kubectl get configmap inferenceservice-config -n ome \
  -o jsonpath='{.data.ingress}' | jq .namespaceIngressGateways
```

```output
{
  "prod": {
    "primary": {
      "ingressDomain": "prod.example.com",
      "omeIngressGateway": "envoy-gateway-system/prod-gw"
    }
  }
}
```

## Step 2: Check the routes

List the HTTPRoutes in `prod` with their Gateways and hosts:

```bash
kubectl get httproute -n prod \
  -o 'custom-columns=NAME:.metadata.name,GATEWAY:.spec.parentRefs[*].name,HOSTNAME:.spec.hostnames[*]'
```

```output
NAME                GATEWAY   HOSTNAME
llama-chat          prod-gw   llm.prod.example.com
llama-chat-engine   prod-gw   llm.prod.example.com
```

Both routes now name `prod-gw`, at the new host. An InferenceService with a router or a decoder has a route for that component too.

Check that the InferenceService's URL uses the new host, and that `IngressReady` is `True`. If it stays `False`, see [IngressReady is False](#ingressready-is-false).

```bash
kubectl get inferenceservice llama-chat -n prod \
  -o 'custom-columns=URL:.status.url,INGRESS:.status.conditions[?(@.type=="IngressReady")].status'
```

```output
URL                                            INGRESS
http://llm.prod.example.com/prod/llama-chat/   True
```

To send a request, use the Gateway API request in [Configure ingress](configure-ingress.md#step-3-send-a-request), with the host `llm.prod.example.com` and the path `/prod/llama-chat/`.

## Override the Gateways for one InferenceService {#per-service-annotations}

Two annotations on an InferenceService override the entry for its namespace. OME picks the primary Gateway and the additional Gateways separately, each from the first row that sets it:

| Source | Primary Gateway | Additional Gateways |
| --- | --- | --- |
| The InferenceService's annotations | `ome.io/ingress-gateway` | `ome.io/ingress-additional-gateways` |
| The entry for its namespace | `primary`, when it sets `omeIngressGateway` | `additional`, when set |
| The cluster-wide values | `omeIngressGateway` | `additionalIngressGateways` |

So an InferenceService in `prod` with only `ome.io/ingress-additional-gateways` keeps `prod-gw` as its primary. An empty annotation still skips its part of the entry, and so does an `ome.io/ingress-additional-gateways` value that isn't a JSON list. The cluster-wide value applies instead. `status.url` is the address on the primary Gateway, and `status.addresses` lists one for each Gateway, named by its `class`.

`ome.io/ingress-gateway` replaces only the Gateway. The host on it uses the cluster-wide `domain`, or the InferenceService's `ome.io/ingress-domain`, and its address takes the cluster-wide `omeIngressGatewayClass`, not the entry's `class`. [Override the gateways for one service](multiple-gateways.md#override-the-gateways-for-one-service) describes the annotations' values.

Say a Gateway `partner-gw` serves `partner.example.com`, and its listener allows routes from `prod`. To serve `llama-chat` through it too, at `llm.partner.example.com`:

```bash
kubectl annotate inferenceservice llama-chat -n prod \
  ome.io/ingress-additional-gateways='[{"omeIngressGateway":"envoy-gateway-system/partner-gw","ingressDomain":"partner.example.com"}]'
```

```output
inferenceservice.ome.io/llama-chat annotated
```

Check that the route now attaches to both Gateways, the primary first:

```bash
kubectl get httproute llama-chat -n prod \
  -o 'custom-columns=GATEWAY:.spec.parentRefs[*].name,HOSTNAME:.spec.hostnames[*]'
```

```output
GATEWAY              HOSTNAME
prod-gw,partner-gw   llm.prod.example.com,llm.partner.example.com
```

`IngressReady` waits for all of a route's Gateways, and turns `True` once both `prod-gw` and `partner-gw` accept the routes. If `partner-gw` is missing or no controller manages it, `IngressReady` stays `False` with the reason `ParentStatusNotAvailable`, and so does the InferenceService's `Ready` condition. To check each Gateway, see [A route is missing from a gateway](multiple-gateways.md#a-route-is-missing-from-a-gateway).

## Troubleshooting

### Routes still attach to the cluster's Gateway

When the routes in `prod` still name `shared-gw`, check for these causes:

- The entry's key isn't `prod`, the InferenceServices' namespace.
- The entry has no `primary.omeIngressGateway`. An entry with only `additional` keeps the cluster's primary Gateway.
- The InferenceService has an `ome.io/ingress-gateway` annotation, even an empty one. `kubectl get inferenceservice llama-chat -n prod -o jsonpath='{.metadata.annotations}'` prints its annotations.
- You edited the ConfigMap instead of upgrading the release. Restart the controller, which then updates all the InferenceServices: `kubectl rollout restart deployment ome-controller-manager -n ome`.

### IngressReady is False

Read the reason and message of the InferenceService's `IngressReady` condition:

```bash
kubectl get inferenceservice llama-chat -n prod \
  -o 'custom-columns=REASON:.status.conditions[?(@.type=="IngressReady")].reason,MESSAGE:.status.conditions[?(@.type=="IngressReady")].message'
```

For example, when a Gateway hasn't accepted the engine route yet:

```output
REASON                     MESSAGE
ParentStatusNotAvailable   Engine HttpRouteNotReady
```

The message starts with the route that isn't ready: `Engine` for `llama-chat-engine`, `TopLevel` for `llama-chat`. When a Gateway turns a route down, the reason and message come from the Gateway. These are the reasons you're most likely to see:

| Reason | Cause | Fix |
| --- | --- | --- |
| `ParentStatusNotAvailable` | A Gateway on the route, primary or additional, hasn't accepted it yet. If this lasts, the Gateway is missing or misspelled, or no controller manages it. | Compare the route's Gateways with those that `kubectl get gateway -A` lists. |
| `NotAllowedByListeners` | No listener on the Gateway allows routes from `prod`. | Allow `prod` in a listener's `allowedRoutes`, as in [Before you begin](#before-you-begin). |
| `NoMatchingListenerHostname` | No listener's hostname matches the route's host. | Set a listener hostname that matches the host, as in [Before you begin](#before-you-begin), or fix the entry's `ingressDomain`. |

[When IngressReady is True](../../concepts/rollouts-and-traffic/ingress.md#when-ingressready-is-true) lists the other reasons.

### The controller doesn't start after the upgrade

A `class` that isn't a lowercase RFC 1123 label, or a value of the wrong type, stops the controller from loading its ingress config. A new controller pod logs `Failed to initialize ingress configuration` and exits. For a bad class, the error starts with `invalid ingress config` and names the field, such as `namespaceIngressGateways["prod"].primary.class`. An `additional` written as a map gives `unable to parse ingress config json` instead.

Controller pods that are still running hit the same error. They stop creating and updating routes, and serving workloads that run as Deployments or, in the deprecated `MultiNode` mode, LeaderWorkerSets. Fix the values, and run the upgrade in [Step 1](#step-1-map-namespaces-to-gateways) again.

## Clean up

Remove the annotation from `llama-chat`:

```bash
kubectl annotate inferenceservice llama-chat -n prod ome.io/ingress-additional-gateways-
```

```output
inferenceservice.ome.io/llama-chat annotated
```

To move `prod` back to the cluster's Gateway, delete its entry from `namespaceIngressGateways` in your values file, and run the upgrade in [Step 1](#step-1-map-namespaces-to-gateways) again. The routes in `prod` go back to `shared-gw`, at the host `llm.example.com`.

## Next steps

- [Use multiple gateways](multiple-gateways.md) to attach the cluster's routes to more than one Gateway.
- [Configure ingress](configure-ingress.md) for the other ingress settings.
