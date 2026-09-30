---
title: "Ingress and External Access"
date: 2024-06-21
weight: 40
description: >
  Configure external access to your AI inference services through ingress controllers, load balancers, and service routing.
---

## What is Ingress in OME?

Ingress in OME provides external access to your AI inference services running inside the Kubernetes cluster. When you deploy an InferenceService, OME automatically creates the appropriate ingress resources based on your deployment mode and cluster configuration, allowing external clients to make API calls to your models.

Think of ingress as the "front door" to your AI services - it handles incoming HTTP requests from outside the cluster and routes them to the correct model endpoints inside your cluster.

## Deployment Modes and Ingress Types

OME supports two different ingress strategies depending on how you deploy your inference services:

### Raw Deployment Mode
**Best for**: Consistent workloads, dedicated resources, custom configurations

Raw deployments use standard Kubernetes ingress controllers:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  annotations:
    ome.io/deployment-mode: "RawDeployment"
spec:
  engine:
    model: llama-3-70b-instruct
    resources:
      requests:
        nvidia.com/gpu: 2
```

Depending on the cluster's ingress configuration, this generates either Gateway API **HTTPRoutes** or a **Kubernetes Ingress** resource (see [Component-Based Routing](#component-based-routing) below). The externally callable URL is published in the service's `status.url`, for example:
```
https://llama-chat.your-namespace.your-cluster.com
```

### MultiNode Mode
**Best for**: Large models requiring multiple GPUs, distributed inference

MultiNode deployments support the same ingress options as Raw deployments:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-405b
  annotations:
    ome.io/deployment-mode: "MultiNode"
spec:
  engine:
    model: llama-3-405b-instruct
    resources:
      requests:
        nvidia.com/gpu: 8
    parallelism: 4  # Distributed across 4 nodes
```

## Component-Based Routing

OME's inference services can include multiple components that work together: the engine, plus an optional router and decoder. OME does **not** route components by request path — there are no special `/v1/router/...` or `/v1/decoder/...` URL paths. Instead, it creates one route per deployed component plus a top-level route that serves as the client entry point.

How those routes are addressed depends on the cluster's ingress mode (the `enableGatewayAPI` setting in the `inferenceservice-config` ConfigMap — a cluster-wide choice, not a per-service one). For an InferenceService named `llama-chat` in namespace `your-namespace`:

- **Gateway API mode**: one HTTPRoute per component, named `llama-chat` (top-level), `llama-chat-engine`, `llama-chat-router`, and `llama-chat-decoder`. Under the default **shared-host scheme**, every route carries the cluster's shared hostname (for example `llm.example.com`) and matches the path prefix `/<namespace>/<route-name>/`, which is stripped before the request reaches the backend — so the decoder is reached at `https://llm.example.com/your-namespace/llama-chat-decoder/...`. Under the opt-in **per-ISVC subdomain scheme**, all of the service's routes share one hostname rendered from `domainTemplate` (for example `llama-chat.your-namespace.example.com`) and match at the root path `/`, so individual components are **not** separately addressable. See [Gateway API Host Schemes](/ome/docs/administration/gateway-host-schemes/) for both schemes in detail.
- **Kubernetes Ingress mode**: a single Ingress whose rules route by per-component **hostname** — each component's service name rendered through `domainTemplate`, for example `llama-chat-decoder.your-namespace.example.com` — with every rule matching at the root path `/`.

Which routes exist, and where the top-level route points, follows from the components you deploy:

### Engine Only (Basic Inference)
```yaml
spec:
  engine:
    model: llama-3-70b-instruct
```
**Routing**:
- Top-level route → Engine service
- Gateway API mode also creates the `llama-chat-engine` route → Engine service

### Engine + Router (Advanced Inference)
```yaml
spec:
  engine:
    model: llama-3-70b-instruct
  router:
    template:
      spec:
        containers:
        - name: router
          image: custom-router:latest
```
**Routing**:
- Top-level route → Router service (the router forwards requests on to the engine)
- `llama-chat-router` route → Router service
- `llama-chat-engine` route → Engine service

### Engine + Decoder (Post-Processing)
```yaml
spec:
  engine:
    model: llama-3-70b-instruct
  decoder:
    template:
      spec:
        containers:
        - name: decoder
          image: custom-decoder:latest
```
**Routing**:
- Top-level route → Engine service
- `llama-chat-engine` route → Engine service
- `llama-chat-decoder` route → Decoder service

### Full Pipeline (Engine + Router + Decoder)
```yaml
spec:
  engine:
    model: llama-3-70b-instruct
  router:
    template:
      spec:
        containers:
        - name: router
          image: custom-router:latest
  decoder:
    template:
      spec:
        containers:
        - name: decoder
          image: custom-decoder:latest
```
**Routing**:
- Top-level route → Router service
- `llama-chat-router` route → Router service
- `llama-chat-engine` route → Engine service
- `llama-chat-decoder` route → Decoder service

## Making API Calls

Once your inference service is deployed and ingress is configured, you can make API calls using standard HTTP clients. The examples below use a per-service hostname; under the default Gateway API shared-host scheme the same request goes to `https://llm.example.com/your-namespace/llama-chat/...` instead. The URL published in the service's `status.url` always reflects the active scheme:

### Basic Text Generation
```bash
# Get the ingress URL
kubectl get inferenceservice llama-chat -o jsonpath='{.status.url}'

# Make a completion request
curl -X POST https://llama-chat.your-namespace.example.com/v1/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-3-70b-instruct",
    "prompt": "Explain quantum computing in simple terms:",
    "max_tokens": 150,
    "temperature": 0.7
  }'
```

### Chat Completions
```bash
curl -X POST https://llama-chat.your-namespace.example.com/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-3-70b-instruct",
    "messages": [
      {"role": "user", "content": "What is machine learning?"}
    ],
    "max_tokens": 200
  }'
```

### Component-Specific Endpoints

Each deployed component is reached through its own route — by path prefix under the default Gateway API shared-host scheme, or by hostname in Kubernetes Ingress mode (see [Component-Based Routing](#component-based-routing)). The path you append is the component's own API path, such as `/health` or `/v1/completions`; there are no OME-specific `/v1/router/...` or `/v1/decoder/...` endpoints.

Under the default Gateway API shared-host scheme (shared host `llm.example.com`, service `llama-chat` in namespace `your-namespace`):

```bash
# Call the engine directly, bypassing the router
curl -X POST https://llm.example.com/your-namespace/llama-chat-engine/v1/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-3-70b-instruct",
    "prompt": "Direct engine request",
    "max_tokens": 100
  }'

# Per-component health checks
curl -H "Accept: application/json" \
  https://llm.example.com/your-namespace/llama-chat-engine/health
curl -H "Accept: application/json" \
  https://llm.example.com/your-namespace/llama-chat-router/health
curl -H "Accept: application/json" \
  https://llm.example.com/your-namespace/llama-chat-decoder/health
```

The `/<namespace>/<service>/` prefix is stripped before the request reaches the component, so the engine above receives `/v1/completions`.

In Kubernetes Ingress mode, address each component by its hostname instead:

```bash
curl -H "Accept: application/json" \
  https://llama-chat-engine.your-namespace.example.com/health
curl -H "Accept: application/json" \
  https://llama-chat-router.your-namespace.example.com/health
curl -H "Accept: application/json" \
  https://llama-chat-decoder.your-namespace.example.com/health
```

Under the per-ISVC subdomain Gateway API scheme, components are not individually addressable: all of the service's routes share one hostname and match at `/`, so requests to that host reach the top-level backend (router if present, otherwise engine).

### Model Information
```bash
# List available models
curl -H "Accept: application/json" \
  https://llama-chat.your-namespace.example.com/v1/models

# Get model details
curl -H "Accept: application/json" \
  https://llama-chat.your-namespace.example.com/v1/models/llama-3-70b-instruct
```

## Ingress Configuration Options

### Custom Ingress Class
```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  annotations:
    ome.io/ingress-class: "nginx"  # Use nginx instead of default istio
spec:
  engine:
    model: llama-3-70b-instruct
```

### Cluster-Local Services (Internal Only)
```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: internal-llama
  labels:
    networking.knative.dev/visibility: cluster-local
spec:
  engine:
    model: llama-3-70b-instruct
```

This creates a service accessible only from within the cluster:
```bash
# From inside the cluster
curl -X POST http://internal-llama.your-namespace.svc.cluster.local/v1/completions \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-3-70b-instruct",
    "prompt": "Internal cluster request",
    "max_tokens": 100
  }'
```

### Load Balancer Services
```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  annotations:
    ome.io/disable-ingress: "true"
    ome.io/service-type: "LoadBalancer"
spec:
  engine:
    model: llama-3-70b-instruct
```

## Troubleshooting Ingress

### Check Service Status
```bash
# Check inference service status
kubectl get inferenceservice llama-chat -o yaml

# Look for ingress readiness condition
kubectl get inferenceservice llama-chat -o jsonpath='{.status.conditions[?(@.type=="IngressReady")]}'
```

### Verify Ingress Resources
```bash
# Check ingress resources
kubectl get ingress -l ome.io/inferenceservice=llama-chat

# For Gateway API
kubectl get httproute -l ome.io/inferenceservice=llama-chat
```

### Test Internal Connectivity
```bash
# Test engine service directly
kubectl port-forward service/llama-chat-engine 8080:80
curl -H "Accept: application/json" http://localhost:8080/health

# Test router service (if exists)
kubectl port-forward service/llama-chat 8080:80
curl -H "Accept: application/json" http://localhost:8080/health
```

### Common Issues

**Ingress Not Created**: Check that ingress isn't disabled and components are ready:
```bash
kubectl get inferenceservice llama-chat -o yaml | grep -A 5 "conditions:"
```

**503/404 Errors**: Verify target services exist and are healthy:
```bash
kubectl get services -l ome.io/inferenceservice=llama-chat
kubectl get pods -l ome.io/inferenceservice=llama-chat
```

**DNS Issues**: Ensure your ingress controller is properly configured with DNS:
```bash
kubectl get ingress llama-chat -o yaml
nslookup llama-chat.your-namespace.example.com
```

## Per-Service Ingress Configuration

While cluster-wide ingress settings are configured in the OME ConfigMap, you can override specific ingress settings per InferenceService using annotations. This allows flexible customization without changing cluster defaults.

### Available Annotation Overrides

| Annotation | Purpose | Example |
|------------|---------|---------|
| `ome.io/ingress-domain-template` | Custom domain template | `{{.Name}}.{{.Namespace}}.ml.example.com` |
| `ome.io/ingress-domain` | Fixed ingress domain | `ml-services.example.com` |
| `ome.io/ingress-additional-domains` | Additional domains (comma-separated) | `backup.example.com,alt.example.com` |
| `ome.io/ingress-url-scheme` | URL scheme override | `https` |
| `ome.io/ingress-path-template` | Custom path template | `/models/{{.Name}}/{{.Namespace}}` |
| `ome.io/ingress-disable-istio-virtualhost` | Disable Istio VirtualService | `"true"` |
| `ome.io/ingress-disable-creation` | Skip ingress creation entirely | `"true"` |

## Security Considerations

### Authentication
OME ingress supports various authentication methods:

```bash
# Using API keys (if configured)
curl -X POST https://llama-chat.your-namespace.example.com/v1/completions \
  -H "Authorization: Bearer your-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-3-70b-instruct",
    "prompt": "API key authentication",
    "max_tokens": 100
  }'

# Using basic auth (if configured)
curl -X POST https://llama-chat.your-namespace.example.com/v1/completions \
  -u username:password \
  -H "Content-Type: application/json" \
  -d '{
    "model": "llama-3-70b-instruct",
    "prompt": "Basic auth request",
    "max_tokens": 100
  }'
```

### TLS/SSL
All external ingress endpoints should use HTTPS in production. Check your ingress controller documentation for TLS certificate configuration.

## Next Steps

- **[Administration Guide](/docs/administration/ingress/)** - Configure ingress controllers and networking
- **[Gateway API Host Schemes](/ome/docs/administration/gateway-host-schemes/)** - How generated routes are addressed: shared host with path prefixes, or per-service subdomains
- **[Serving Runtime](/docs/concepts/serving_runtime/)** - Understand the underlying serving infrastructure
- **[InferenceService](/docs/concepts/inference_service/)** - Complete InferenceService configuration reference

For production deployments and cluster configuration, see the [Administration](/docs/administration/) section.
