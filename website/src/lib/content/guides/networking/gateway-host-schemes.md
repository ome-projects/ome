---
title: Choose a Gateway API host scheme
description: Choose between one shared hostname with per-service path prefixes and a subdomain per InferenceService for the HTTPRoutes OME generates.
since: v1.3
---

With Gateway API routing turned on, OME exposes an [InferenceService](../../concepts/serving/inference-services.md) through HTTPRoutes on your Gateway, at a path under one shared hostname or at a hostname of its own. You set this host scheme for the cluster, and can override it for one service.

| | Shared host with a path prefix (default) | Per-service subdomains |
|---|---|---|
| URL of `llama-chat` in `prod` | `http://llm.example.com/prod/llama-chat/` | `http://llama-chat.prod.example.com/` |
| Path the backend receives | The path after the prefix | The path as sent |
| An address for each component | Yes, such as `/prod/llama-chat-engine/` | No |
| Certificates for HTTPS | One, for the shared host | A wildcard per namespace, by default |
| Services with a router or decoder | Work | Requests can reach the wrong component |

Keep the shared host unless clients need each service on its own hostname, with the path as sent. Choose before clients depend on the URLs, because switching changes them.

<div class="prerequisites" markdown>

- OME v1.3 or later, installed with Helm as the release `ome` in the namespace `ome`, and `kubectl` access to the cluster. On v1.2.2, OME can't create HTTPRoutes. See [Install OME](../../getting-started/install.md).
- Gateway API routing turned on with the Helm values `disableIngressCreation: false`, `enableGatewayAPI: true` and `omeIngressGateway`, which names your Gateway. Ingress creation is off by default. See [Configure ingress](configure-ingress.md). The examples use the Gateway `envoy-gateway-system/ome-gateway`.
- A DNS domain you control, set as the Helm value `domain`. The chart's default, `svc.cluster.local`, isn't one. The examples use `example.com`. For HTTPS, add the certificate to the Gateway's listener yourself, and set the Helm value `urlScheme: https` so that `status.url` starts with `https`.
- An InferenceService with no router or decoder. The examples use `llama-chat` in the namespace `prod`, which serves the [ClusterBaseModel](../../concepts/models/base-models.md) `llama-3-2-1b-instruct` with the [ClusterServingRuntime](../../concepts/runtimes/serving-runtimes.md) `srt-llama-3-2-1b-instruct`. With the service from [Serve your first model](../../getting-started/serve-your-first-model.md), use `qwen3-0-6b` as the name and the namespace.

</div>

## Shared host with a path prefix

By default, all HTTPRoutes use one hostname, `<sharedHostPrefix>.<domain>`. The chart sets `sharedHostPrefix` to `llm`, which gives `llm.example.com`, and an empty prefix gives the bare domain, `example.com`.

A path prefix, `/<namespace>/<route>/`, picks the route. Clients use the entry route, `/prod/llama-chat/`, which leads to the router, or to the engine when there's no router. Each component also gets its own route once it's ready, such as `/prod/llama-chat-engine/`. The routes strip their prefix, so a request for `/prod/llama-chat/v1/chat/completions` reaches the backend as `/v1/chat/completions`. [Routes](../../concepts/rollouts-and-traffic/ingress.md#routes) lists them all.

The shared host needs one DNS record that points `llm.example.com` at the Gateway, and one certificate when the listener serves HTTPS. A new InferenceService needs neither.

## Per-service subdomains

With `perISVCSubdomain: true`, each InferenceService gets its own hostname, rendered from the Helm value `domainTemplate`. All of the service's routes use that hostname and match at `/`, so the backend receives the path as sent.

Each hostname needs a DNS record that resolves to the Gateway, and a certificate when the listener serves HTTPS. A wildcard certificate covers one label, so pick the template by the certificates you want:

| `domainTemplate` | Hostname | Wildcard certificate |
|---|---|---|
| `{{ .Name }}.{{ .Namespace }}.{{ .IngressDomain }}`, the default | `llama-chat.prod.example.com` | `*.prod.example.com`, one per namespace |
| `{{ .Name }}-{{ .Namespace }}.{{ .IngressDomain }}` | `llama-chat-prod.example.com` | `*.example.com`, one for the cluster |

With the second template, a service's name and namespace, with the hyphen, must fit in 63 characters, and no two services may join into the same hostname. A template can use `.Name`, `.Namespace`, `.IngressDomain`, `.Annotations` and `.Labels`, and must render a fully qualified domain name.

!!! warning "Services with a router or decoder"
    In this scheme, all of a service's routes match the same hostname and path `/`, and which one gets the requests depends on when OME created the routes. With a router or a decoder, requests can reach the wrong component. Keep those services on the shared host, as in [Step 1](#step-1-keep-services-with-a-router-or-decoder-on-the-shared-host).

## Step 1: Keep services with a router or decoder on the shared host

If you're switching to per-service subdomains, annotate each InferenceService that has a router or a decoder, so that it stays on the shared host.

!!! warning "Changing an annotation replaces the pods"
    In [RawDeployment mode](../../concepts/architecture/deployment-modes.md), the default for a single-node engine, adding or changing an annotation on a running InferenceService replaces its pods in a rolling update. By default, a new pod starts before an old one stops, so each component needs room for one more pod, such as a free GPU for the engine.

```bash
kubectl annotate inferenceservice <name> -n <namespace> ome.io/ingress-per-isvc-subdomain=false
```

```output
inferenceservice.ome.io/<name> annotated
```

## Step 2: Switch the cluster's scheme {#switch-schemes-cluster-wide}

OME updates the existing HTTPRoutes in place, so the old hostnames and paths stop working as soon as it does. Set up the DNS records and certificates for the new scheme first.

The Gateway's listener must also accept the new hostnames. A listener for `llm.example.com` doesn't accept `llama-chat.prod.example.com`, but one for `*.example.com`, or one with no hostname, does. A route whose hostname matches no listener reports `NoMatchingListenerHostname` in its status.

To switch to per-service subdomains, add `perISVCSubdomain: true` to the values file you install `ome-resources` with:

```yaml title="values.yaml"
ome:
  controller:
    ingressGateway:
      domain: example.com
      omeIngressGateway: envoy-gateway-system/ome-gateway
      disableIngressCreation: false
      enableGatewayAPI: true
      perISVCSubdomain: true
```

To switch back, set `perISVCSubdomain: false`.

Upgrade the release at the installed chart version, so that OME stays in step with the `ome-crd` chart. `helm list -n ome` shows it in the `CHART` column, here `ome-resources-1.3.0`:

```bash
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm prints `Release "ome" has been upgraded. Happy Helming!` and the release's new revision. Because the ConfigMap changed, the upgrade restarts the controller, which moves all HTTPRoutes to the new scheme, except where an annotation overrides it.

Check the setting in the ConfigMap:

```bash
kubectl get configmap inferenceservice-config -n ome -o jsonpath='{.data.ingress}' \
  | grep -E '"(ingressDomain|perISVCSubdomain|sharedHostPrefix)"'
```

```output
    "ingressDomain"  : "example.com",
    "perISVCSubdomain": true,
    "sharedHostPrefix": "llm",
```

With a kustomize install, add `"perISVCSubdomain": true` to the `ingress` key of the `inferenceservice-config` ConfigMap, apply it, and restart the controller, as [Kustomize installs](configure-ingress.md#kustomize-installs) shows.

## Step 3: Check which scheme is active

An annotation can override the cluster's scheme, so check the service's own routes:

```bash
kubectl get httproute llama-chat llama-chat-engine -n prod \
  -o 'custom-columns=NAME:.metadata.name,HOSTS:.spec.hostnames[*],PATH:.spec.rules[0].matches[0].path.value'
```

=== "Shared host"

    ```output
    NAME                HOSTS             PATH
    llama-chat          llm.example.com   /prod/llama-chat/
    llama-chat-engine   llm.example.com   /prod/llama-chat-engine/
    ```

=== "Per-service subdomains"

    ```output
    NAME                HOSTS                         PATH
    llama-chat          llama-chat.prod.example.com   /
    llama-chat-engine   llama-chat.prod.example.com   /
    ```

Then check the URL the service reports:

```bash
kubectl get inferenceservice llama-chat -n prod -o jsonpath='{.status.url}'
```

=== "Shared host"

    ```output
    http://llm.example.com/prod/llama-chat/
    ```

=== "Per-service subdomains"

    ```output
    http://llama-chat.prod.example.com/
    ```

`status.addresses` lists the service's URL at every Gateway, and its address inside the cluster: see [Use multiple gateways](multiple-gateways.md).

## Override the scheme for one service

Two annotations on an InferenceService override the cluster's settings for its routes and `status.url`:

| Helm value | Chart default | Annotation for one service | Effect |
|---|---|---|---|
| `perISVCSubdomain` | `false` | `ome.io/ingress-per-isvc-subdomain` | `true` gives each service its own hostname. On the annotation, `"true"` does, and any other value, such as `"false"`, keeps the service on the shared host. The value is case-sensitive. |
| `sharedHostPrefix` | `llm` | `ome.io/ingress-shared-host-prefix` | The first label of the shared host. Empty gives the bare domain. No effect on a service with its own hostname. |

Adding an annotation to a running service can replace its pods, as [Step 1](#step-1-keep-services-with-a-router-or-decoder-on-the-shared-host) warns, so set the annotations when you create a service. For example, to give `llama-chat` its own hostname while the rest of the cluster stays on the shared host:

```bash
kubectl annotate inferenceservice llama-chat -n prod ome.io/ingress-per-isvc-subdomain=true
```

```output
inferenceservice.ome.io/llama-chat annotated
```

Its routes then use `llama-chat.prod.example.com` and match at `/`. With `ome.io/ingress-shared-host-prefix=experimental` instead, it moves to a shared host of its own, `experimental.example.com`, with the same path prefixes.

`ome.io/ingress-domain` and `ome.io/ingress-domain-template` override `domain` and `domainTemplate` for one service: see [Ingress annotations](../../reference/api/labels-and-annotations.md#ingress-annotations).

## Troubleshooting

### The routes keep the old scheme

- Check the ConfigMap, as in Step 2. After you edit it yourself, restart the controller:

    ```bash
    kubectl rollout restart deployment ome-controller-manager -n ome
    ```

    ```output
    deployment.apps/ome-controller-manager restarted
    ```

- Check the service's `ome.io/ingress-per-isvc-subdomain` annotation, which overrides the cluster's scheme.
- `invalid domain name` in the controller's logs means that `domainTemplate`, or a domain annotation, renders an invalid hostname. Until you fix it, OME stops creating or updating the service's routes, and the serving workload of its RawDeployment and MultiNode (deprecated) components. See [The hostname is missing or wrong](configure-ingress.md#the-hostname-is-missing-or-wrong).

## Next steps

- [Configure ingress](configure-ingress.md): turn on ingress creation, and set the Gateway, the domain and the URL scheme.
- [Use multiple gateways](multiple-gateways.md): attach the routes to more than one Gateway, and read each Gateway's URL in the service's status.
- [Use per-namespace gateways](namespace-gateways.md): give each namespace its own Gateway, with its own certificate.
- [Ingress and external access](../../concepts/rollouts-and-traffic/ingress.md): what OME creates for external access, including the Kubernetes Ingress it uses when Gateway API routing is off.
- [Publish a global endpoint](../multi-cluster/publish-a-global-endpoint.md): give a multi-cluster InferenceService one hostname across workload clusters that use per-service subdomains. Multi-cluster routing is alpha, still in development and off by default.
