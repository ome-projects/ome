---
title: Configure ingress
description: Configure how OME exposes InferenceServices through Kubernetes Ingress or Gateway API with the ingress block of the inferenceservice-config ConfigMap.
---

Turn on ingress creation, and OME exposes your InferenceServices outside the cluster through a Kubernetes Ingress or Gateway API HTTPRoutes, at a hostname under your domain. You set a few values of the `ome-resources` chart, deploy an InferenceService, and send it a request through its route. For how the pieces fit together, see [Ingress and external access](../../concepts/rollouts-and-traffic/ingress.md).

Ingress creation is off by default, and OME creates a Service named after each InferenceService instead. To reach that Service from outside the cluster without an Ingress controller or a Gateway, see [Expose a service without ingress](expose-without-ingress.md).

<div class="prerequisites" markdown>

- OME installed with the `ome-resources` Helm chart, as the release `ome` in the namespace `ome`, and the values file you installed it with. See [Install OME](../../getting-started/install.md). For kustomize installs, see [Kustomize installs](#kustomize-installs).
- For Kubernetes Ingress, an Ingress controller and its IngressClass. `kubectl get ingressclass` lists the classes. The examples use the class `nginx`.
- For Gateway API, OME v1.3 or later, the Gateway API CRDs, and Envoy Gateway. Until v1.3 is released, run [a build of `main`](../../getting-started/install.md#install-from-source).
- For Gateway API, a Gateway for the routes. The examples use the Gateway `ome-gateway` in the namespace `envoy-gateway-system`, with an HTTP listener on port 80 that allows routes from the namespace `llama-demo`. If the listener sets a hostname, it must match `llm.example.com`.
- The BaseModel `llama-3-2-1b-instruct` in the namespace `llama-demo`, and the ClusterServingRuntime `srt-llama-3-2-1b-instruct`, as in [Serve models from a PVC](../deploy-models/serve-models-from-pvc.md) up to its Step 3. To use an InferenceService that you already run, skip the apply in Step 2, and change the names in the commands.
- Optionally, DNS records that point your hostnames at the Ingress controller or Gateway. Step 3 uses `curl --resolve` instead.

</div>

## Choose an ingress type

`enableGatewayAPI` picks the ingress type for the whole cluster. By default it's `false`, and OME creates Kubernetes Ingresses. Set it to `true`, and OME creates Gateway API HTTPRoutes instead.

| | Kubernetes Ingress | Gateway API |
| --- | --- | --- |
| Served by | Your Ingress controller, through the IngressClass in `ingressGateway.className` | Your Gateway, named in `omeIngressGateway` as `<namespace>/<name>` |
| Hostnames | One per InferenceService, `<name>.<namespace>.<domain>` by default, plus one per component when there's a router or decoder | By default, one shared host, `llm.<domain>`, with a path prefix per InferenceService. Optionally, one hostname per InferenceService |
| Also supports | | Several Gateways, Gateways chosen by namespace, route timeouts, and sticky routing by header |
| Versions | v1.2.2 and later | Since v1.3 |

Use Gateway API if you run Envoy Gateway and OME v1.3 or later. Otherwise, use a Kubernetes Ingress with the Ingress controller you run, such as ingress-nginx. `RawDeployment`, [OMENative](../../concepts/omenative/overview.md) and `MultiNode` (deprecated) InferenceServices all get routes. A `VirtualDeployment` InferenceService gets none.

## Step 1: Turn on ingress creation

The ingress settings are values of the `ome-resources` chart, under `ome.controller.ingressGateway`. The chart writes them into the `ingress` key of the `inferenceservice-config` ConfigMap, which the controller reads. Add the values for your ingress type to your values file:

=== "Kubernetes Ingress"

    ```yaml title="values.yaml"
    ome:
      controller:
        ingressGateway:
          disableIngressCreation: false
          domain: example.com
          ingressGateway:
            className: nginx
    ```

    OME creates an Ingress with the IngressClass `nginx` for each InferenceService, at the hostname `<name>.<namespace>.example.com`.

=== "Gateway API"

    ```yaml title="values.yaml"
    ome:
      controller:
        ingressGateway:
          disableIngressCreation: false
          enableGatewayAPI: true
          domain: example.com
          omeIngressGateway: envoy-gateway-system/ome-gateway
    ```

    OME attaches the HTTPRoutes to the Gateway `ome-gateway` in `envoy-gateway-system`. The routes share the host `llm.example.com`, and each InferenceService gets the path prefix `/<namespace>/<name>/`.

Replace `example.com` with a domain whose DNS you control. [Configuration reference](#configuration-reference) lists the other settings.

OME doesn't set up TLS or authenticate requests. Terminate TLS and authenticate requests at your Ingress controller or Gateway, and set `urlScheme: https` so that `status.url` matches. See [Security](../../concepts/rollouts-and-traffic/ingress.md#security).

!!! warning
    Turning on ingress creation deletes the Service named after each existing InferenceService. Clients in the cluster that call `<name>.<namespace>.svc.cluster.local` must switch to the route, or to a component's Service, such as `llama-3-2-1b-instruct-engine`.

Upgrade the release with the chart version that's installed, so that OME stays in step with the `ome-crd` chart. The `CHART` column of `helm list -n ome` shows the version, such as `ome-resources-1.2.2`. On v1.2.2:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.2.2 -f values.yaml
```

On a build of `main`, upgrade from your checkout instead, as in [Install from source](../../getting-started/install.md#install-from-source):

```bash
helm upgrade --install ome ./charts/ome-resources --namespace ome -f values.yaml
```

Helm reports that the release `ome` has been upgraded, with the status `deployed`. Since v1.3, the upgrade restarts the controller when these values change, and the controller applies them to every InferenceService as it starts. On v1.2.2, restart it yourself:

```bash
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

Check what the chart wrote into the ConfigMap:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.ingress}' \
  | jq '{disableIngressCreation, enableGatewayAPI, ingressDomain, ingressClassName, omeIngressGateway}'
```

=== "Kubernetes Ingress"

    ```output
    {
      "disableIngressCreation": false,
      "enableGatewayAPI": false,
      "ingressDomain": "example.com",
      "ingressClassName": "nginx",
      "omeIngressGateway": ""
    }
    ```

=== "Gateway API"

    ```output
    {
      "disableIngressCreation": false,
      "enableGatewayAPI": true,
      "ingressDomain": "example.com",
      "ingressClassName": "istio",
      "omeIngressGateway": "envoy-gateway-system/ome-gateway"
    }
    ```

    `ingressClassName` keeps the chart's default. Gateway API ignores it.

## Step 2: Deploy an InferenceService

Create an InferenceService that serves the BaseModel:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
    kind: BaseModel
  runtime:
    name: srt-llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 1
```

```bash
kubectl apply -f isvc.yaml
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct created
```

OME creates the routes once the engine is ready. With Gateway API, `Ready` also waits for the Gateway to accept them. Wait for it:

```bash
kubectl wait --for=condition=Ready inferenceservice/llama-3-2-1b-instruct -n llama-demo --timeout=30m
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct condition met
```

List the routes that OME created:

=== "Kubernetes Ingress"

    ```bash
    kubectl get ingress llama-3-2-1b-instruct -n llama-demo \
      -o 'custom-columns=CLASS:.spec.ingressClassName,HOST:.spec.rules[*].host,SERVICE:.spec.rules[*].http.paths[*].backend.service.name,PORT:.spec.rules[*].http.paths[*].backend.service.port.number'
    ```

    ```output
    CLASS   HOST                                           SERVICE                        PORT
    nginx   llama-3-2-1b-instruct.llama-demo.example.com   llama-3-2-1b-instruct-engine   8080
    ```

    The Ingress sends requests for the InferenceService's hostname to the engine's Service, or to the router's when there is one. With a router or decoder, each component also gets a hostname of its own, such as `llama-3-2-1b-instruct-engine.llama-demo.example.com`.

=== "Gateway API"

    ```bash
    kubectl get httproute -n llama-demo \
      -o 'custom-columns=NAME:.metadata.name,HOST:.spec.hostnames[*],PATH:.spec.rules[*].matches[*].path.value,BACKEND:.spec.rules[*].backendRefs[*].name'
    ```

    ```output
    NAME                           HOST              PATH                                        BACKEND
    llama-3-2-1b-instruct          llm.example.com   /llama-demo/llama-3-2-1b-instruct/          llama-3-2-1b-instruct-engine
    llama-3-2-1b-instruct-engine   llm.example.com   /llama-demo/llama-3-2-1b-instruct-engine/   llama-3-2-1b-instruct-engine
    ```

    The route named after the InferenceService leads to the router, or to the engine when there's no router, and each component gets a route of its own. The routes strip their path prefix, so `/llama-demo/llama-3-2-1b-instruct/v1/chat/completions` reaches the engine as `/v1/chat/completions`.

    For an InferenceService without a [traffic policy](../../concepts/rollouts-and-traffic/traffic-policy.md#defaults-when-you-declare-nothing) of its own, OME also creates a BackendTrafficPolicy named after it. The policy makes Envoy Gateway hash the `x-routing-key` and `X-SMG-Routing-Key` headers, so that requests with the same values reach the same pod.

## Step 3: Send a request

The InferenceService's status has the URL to call:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-demo -o jsonpath='{.status.url}{"\n"}'
```

=== "Kubernetes Ingress"

    ```output
    http://llama-3-2-1b-instruct.llama-demo.example.com:8080
    ```

    The URL carries the port of the engine's Service. Send requests to your Ingress controller's HTTP port instead, usually 80.

=== "Gateway API"

    ```output
    http://llm.example.com/llama-demo/llama-3-2-1b-instruct/
    ```

Send a chat completion request through the route. The address `203.0.113.10` stands for your Ingress controller's or Gateway's address, which `kubectl get ingress llama-3-2-1b-instruct -n llama-demo` or `kubectl get gateway ome-gateway -n envoy-gateway-system` shows in its `ADDRESS` column. With DNS records in place, leave out `--resolve`.

=== "Kubernetes Ingress"

    ```bash
    curl -s --resolve llama-3-2-1b-instruct.llama-demo.example.com:80:203.0.113.10 \
      http://llama-3-2-1b-instruct.llama-demo.example.com/v1/chat/completions \
      -H "Content-Type: application/json" \
      -d '{"model": "meta-llama/Llama-3.2-1B-Instruct", "messages": [{"role": "user", "content": "In one sentence, what is an Ingress?"}], "max_tokens": 64}' \
      | jq -r '.choices[0].message.content'
    ```

=== "Gateway API"

    ```bash
    curl -s --resolve llm.example.com:80:203.0.113.10 \
      http://llm.example.com/llama-demo/llama-3-2-1b-instruct/v1/chat/completions \
      -H "Content-Type: application/json" \
      -d '{"model": "meta-llama/Llama-3.2-1B-Instruct", "messages": [{"role": "user", "content": "In one sentence, what is an Ingress?"}], "max_tokens": 64}' \
      | jq -r '.choices[0].message.content'
    ```

`jq` prints the model's answer, which varies from run to run. If the command prints nothing or an error, see [Requests don't reach the service](#requests-dont-reach-the-service).

## Configuration reference

The settings are values of the `ome-resources` chart under `ome.controller.ingressGateway`. Where a setting has an annotation, the annotation overrides it for one InferenceService:

| Helm value | Chart default | Annotation | Effect |
| --- | --- | --- | --- |
| `disableIngressCreation` | `true` | `ome.io/ingress-disable-creation` | `true` turns ingress creation off, and OME creates a Service instead. See [Turn off ingress creation](#turn-off-ingress-creation). |
| `enableGatewayAPI` | `false` | | `true` creates HTTPRoutes instead of an Ingress. Since v1.3. |
| `domain` | `svc.cluster.local` | `ome.io/ingress-domain` | Domain of the hostnames, `.IngressDomain` in `domainTemplate`. |
| `domainTemplate` | `{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}` | `ome.io/ingress-domain-template` | Go template for Ingress hostnames, and for HTTPRoute hostnames when `perISVCSubdomain` is `true`. |
| `urlScheme` | `http` | `ome.io/ingress-url-scheme` | Scheme of the URLs in the InferenceService's status. Set `https` when your Ingress controller or Gateway terminates TLS. |
| `ingressGateway.className` | `istio` | | IngressClass of the Ingress. |
| `omeIngressGateway` | `""` | `ome.io/ingress-gateway` | Gateway for the HTTPRoutes, as `<namespace>/<name>`. Gateway API needs it. |
| `omeIngressGatewayClass` | `""` | | Name of the primary Gateway's entry in `status.addresses`, a lowercase label such as `internal`. Since v1.3. |
| `perISVCSubdomain` | `false` | `ome.io/ingress-per-isvc-subdomain` | `true` gives an InferenceService's HTTPRoutes a hostname of its own, from `domainTemplate`, in place of the shared host. See [Choose a Gateway API host scheme](gateway-host-schemes.md). Since v1.3. |
| `sharedHostPrefix` | `llm` | `ome.io/ingress-shared-host-prefix` | First label of the shared host, `<sharedHostPrefix>.<domain>`. Empty means the bare domain. Since v1.3. |
| `additionalIngressGateways` | `[]` | `ome.io/ingress-additional-gateways` | More Gateways for the HTTPRoutes. See [Use multiple gateways](multiple-gateways.md). Since v1.3. |
| `namespaceIngressGateways` | `{}` | | Gateways for the InferenceServices in a namespace. See [Use per-namespace gateways](namespace-gateways.md). Since v1.3. |
| `consistentHashHeaders` | `["x-routing-key", "X-SMG-Routing-Key"]` | `ome.io/ingress-consistent-hash-headers` | Headers that the default BackendTrafficPolicy hashes, so that requests with the same values reach the same pod. Since v1.3. |
| `defaultRouteTimeoutSeconds` | `0` | | Request timeout of the HTTPRoutes, in seconds. `0` turns it off. See [Configure route timeouts](configure-route-timeouts.md). Since v1.3. |

Leave `ingressGateway.gateway`, `ingressGateway.gatewayService`, `pathTemplate`, `additionalIngressDomains` and `disableIstioVirtualHost` at the chart's values. OME ignores them, but a wrong value stops the controller from starting, as [The controller keeps restarting](#the-controller-keeps-restarting) describes.

`domainTemplate` can use `.Name`, `.Namespace`, `.IngressDomain`, `.Annotations` and `.Labels`, and must render a valid DNS name. [Hostnames](../../concepts/rollouts-and-traffic/ingress.md#hostnames-and-urls) shows the names it renders.

### Per-service overrides

[Ingress annotations](../../reference/api/labels-and-annotations.md#ingress-annotations) gives the values that the annotations take. For example, to move one InferenceService to another domain:

```bash
kubectl annotate inferenceservice llama-3-2-1b-instruct -n llama-demo ome.io/ingress-domain=internal.example.com
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct annotated
```

Its Ingress hostname becomes `llama-3-2-1b-instruct.llama-demo.internal.example.com`. With Gateway API, its shared host becomes `llm.internal.example.com`, which the Gateway's listener must accept.

OME copies the InferenceService's annotations to its routes, so annotations for your Ingress controller or Gateway, such as ingress-nginx's, go on the InferenceService. Set them when you create it: OME copies a later change to an existing route only along with a change to the route's spec.

### Kustomize installs

With a kustomize install, edit the `ingress` key of the `inferenceservice-config` ConfigMap in the `ome` namespace, apply it, and restart the controller as in Step 1. Its settings have the names of the Helm values, except for four: `domain` is `ingressDomain`, `ingressGateway.className` is `ingressClassName`, `ingressGateway.gateway` is `ingressGateway`, and `ingressGateway.gatewayService` is `ingressService`.

The kustomize ConfigMap leaves out `sharedHostPrefix`, `consistentHashHeaders` and `defaultRouteTimeoutSeconds`. Without them, the shared host is the bare domain, OME hashes only `x-routing-key`, and Envoy's 15-second request timeout applies. To match a Helm install, add the three keys with the chart defaults.

## Turn off ingress creation

To turn ingress creation off for the whole cluster, set `disableIngressCreation: true` in your values file, and upgrade the release as in Step 1, including the restart on v1.2.2.

To turn it off for one InferenceService, annotate it:

```bash
kubectl annotate inferenceservice llama-3-2-1b-instruct -n llama-demo ome.io/ingress-disable-creation=true
```

```output
inferenceservice.ome.io/llama-3-2-1b-instruct annotated
```

While ingress creation is off for the cluster, `ome.io/ingress-disable-creation: "false"` turns it on for one InferenceService.

OME leaves the routes it created in place, and they keep serving traffic. Delete them yourself:

=== "Kubernetes Ingress"

    ```bash
    kubectl delete ingress llama-3-2-1b-instruct -n llama-demo
    ```

    ```output
    ingress.networking.k8s.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
    ```

=== "Gateway API"

    ```bash
    kubectl delete httproute llama-3-2-1b-instruct llama-3-2-1b-instruct-engine -n llama-demo
    kubectl delete backendtrafficpolicy llama-3-2-1b-instruct -n llama-demo
    ```

    ```output
    httproute.gateway.networking.k8s.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
    httproute.gateway.networking.k8s.io "llama-3-2-1b-instruct-engine" deleted from llama-demo namespace
    backendtrafficpolicy.gateway.envoyproxy.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
    ```

OME creates the Service named after the InferenceService instead, and sets `IngressReady` to `True` with the reason `IngressDisabled`. Check the Service:

```bash
kubectl get service llama-3-2-1b-instruct -n llama-demo \
  -o 'custom-columns=NAME:.metadata.name,TYPE:.spec.type,PORT:.spec.ports[*].port'
```

```output
NAME                    TYPE        PORT
llama-3-2-1b-instruct   ClusterIP   8080
```

The InferenceService's `status.url` becomes `http://llama-3-2-1b-instruct.llama-demo.svc.cluster.local:8080`. To reach the Service from outside the cluster, see [Expose a service without ingress](expose-without-ingress.md).

## Troubleshooting

Start with the InferenceService's `IngressReady` condition:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-demo \
  -o 'custom-columns=READY:.status.conditions[?(@.type=="IngressReady")].status,REASON:.status.conditions[?(@.type=="IngressReady")].reason,MESSAGE:.status.conditions[?(@.type=="IngressReady")].message'
```

For example, while ingress creation is off:

```output
READY   REASON            MESSAGE
True    IngressDisabled   Ingress creation is disabled, using external service for access
```

`True` with no reason means that OME created the routes, and with Gateway API, that the Gateway accepted them. [When IngressReady is True](../../concepts/rollouts-and-traffic/ingress.md#when-ingressready-is-true) explains the other reasons.

### OME doesn't create a route

- With the reason `IngressDisabled`, ingress creation is off. Check the ConfigMap as in Step 1, and look for the `ome.io/ingress-disable-creation` annotation on the InferenceService. If the ConfigMap has `"disableIngressCreation": false`, restart the controller.
- With the reason `ComponentNotReady`, the router, decoder or engine isn't ready, and OME creates its route when it is. See [Troubleshoot an InferenceService](../troubleshoot/troubleshoot-an-inferenceservice.md).
- If the condition is missing, or doesn't change after you restart the controller, an error stops OME before it updates the status. Read the logs of every controller replica:

```bash
kubectl logs -n ome -l control-plane=ome-controller-manager --tail=-1
```

Look for errors that name the InferenceService:

- `no kind is registered for the type v1.HTTPRoute`: Gateway API on v1.2.2, or on a controller that started before you set `enableGatewayAPI: true`. Use Kubernetes Ingress on v1.2.2, and restart the controller on later releases.
- `no matches for kind "BackendTrafficPolicy"`: Envoy Gateway's CRDs are missing. OME creates the HTTPRoutes, but the InferenceService doesn't become `Ready`. Install Envoy Gateway, then restart the controller.
- `invalid domain name`: see [The hostname is missing or wrong](#the-hostname-is-missing-or-wrong).

### The controller keeps restarting

If the controller's pods restart over and over, and their logs show `Failed to initialize ingress configuration`, the controller rejects the `ingress` key. The error after that message says why. Since v1.3, `ingressGateway.gateway` and `ingressGateway.gatewayService` must be set, as they are in the chart. A list in `additionalIngressDomains` breaks the key on any release. Fix the value in your values file, and upgrade the release.

### The hostname is missing or wrong

- With Kubernetes Ingress, `status.url` stays empty until OME creates the Ingress, once the component is ready.
- A hostname under `svc.cluster.local` means that `domain` still has the chart's default. One under `example.com` that you didn't set means that `domain` is empty. Set `domain` as in Step 1.
- With Gateway API, a shared host without `llm.` means that `sharedHostPrefix` is empty, as in kustomize installs, or that the InferenceService's `ome.io/ingress-shared-host-prefix` annotation is empty.
- If an InferenceService has a hostname of its own where you expected the shared host, or the other way around, check `perISVCSubdomain` and its annotation. See [Choose a Gateway API host scheme](gateway-host-schemes.md).
- `invalid domain name` in the controller's logs means that `domainTemplate`, or the `ome.io/ingress-domain` or `ome.io/ingress-domain-template` annotation, renders an invalid hostname. Until you fix it, OME doesn't create or update the routes, or the serving workload of `RawDeployment` and `MultiNode` (deprecated) components.

### Requests don't reach the service

- Check that the hostname resolves to your Ingress controller or Gateway, or use `curl --resolve` as in Step 3.
- With Kubernetes Ingress, `IngressReady` turns `True` as soon as OME creates the Ingress, whether or not your Ingress controller serves it. Check that `ingressGateway.className` names an IngressClass that `kubectl get ingressclass` lists, then read `kubectl describe ingress llama-3-2-1b-instruct -n llama-demo` and the Ingress controller's log.
- With Gateway API, read the route's status with `kubectl describe httproute llama-3-2-1b-instruct -n llama-demo`. `NotAllowedByListeners` means that no listener of the Gateway allows routes from the InferenceService's namespace. Set the listener's `allowedRoutes.namespaces.from` to `All`, or to `Selector` with a selector that matches the namespace. `NoMatchingListenerHostname` means that no listener's hostname matches the route's.
- With Gateway API, if `IngressReady` stays `False` with the reason `ParentStatusNotAvailable`, no Gateway has reported on the route. Check that `omeIngressGateway` names an existing Gateway as `<namespace>/<name>`.
- With the shared host, requests must start with the path prefix, `/<namespace>/<name>/`. A request to `/v1/chat/completions` on the shared host matches no route.
- If long requests fail after about 15 seconds, the route has no timeout of its own, and Envoy's 15-second default applies. Set `defaultRouteTimeoutSeconds`, or the component's `timeoutSeconds`, as [Configure route timeouts](configure-route-timeouts.md) shows.

### Requests with the same header reach different pods

With Gateway API, `IngressReady` can be `True` while Envoy Gateway hasn't accepted the BackendTrafficPolicy. Read the policy's status:

```bash
kubectl describe backendtrafficpolicy llama-3-2-1b-instruct -n llama-demo
```

The `Status` section holds what Envoy Gateway reported about the policy.

## Clean up

Delete the InferenceService. The routes, BackendTrafficPolicy and Service that OME created for it go with it:

```bash
kubectl delete inferenceservice llama-3-2-1b-instruct -n llama-demo
```

```output
inferenceservice.ome.io "llama-3-2-1b-instruct" deleted from llama-demo namespace
```

To stop OME creating routes, turn ingress creation off as in [Turn off ingress creation](#turn-off-ingress-creation).

## Next steps

- [Choose a Gateway API host scheme](gateway-host-schemes.md): a shared host with path prefixes, or a hostname per InferenceService.
- [Use multiple gateways](multiple-gateways.md): attach the routes to more than one Gateway, such as an internal and an external one.
- [Configure route timeouts](configure-route-timeouts.md): keep Envoy's 15-second default from cutting off long requests.
