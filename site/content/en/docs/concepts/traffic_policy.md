---
title: "Traffic Policy"
linkTitle: "Traffic Policy"
weight: 33
description: >
  Choose the load-balancing algorithm and session affinity for an InferenceService via spec.traffic, and read the resolved policy back from status.traffic.
---

The **`spec.traffic`** block on an InferenceService declares its steady-state load-balancing policy: which algorithm distributes requests across pods, what request attribute (header, cookie, or source IP) pins a session to one pod, and — on Envoy Gateway — whether a request header may override endpoint selection entirely. OME translates the block into the backend policy resource native to whatever gateway is installed and reports the result under `status.traffic`.

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
spec:
  model:
    name: llama-3-70b-instruct
  engine:
    minReplicas: 2
    maxReplicas: 16
  traffic:
    algorithm: ConsistentHash
    consistentHash:
      type: Header
      headers:
        - name: x-session-id
```

One policy covers the whole service: the emitted resource targets every OME-managed HTTPRoute — the top-level route (`llama-chat`), the engine route (`llama-chat-engine`), and the decoder/router routes when those components exist — so behavior is the same whether clients call the top-level URL or a per-component subdomain.

`spec.traffic` is the typed core of traffic management. Less common knobs (circuit breaker, retries, sub-second timeouts, and the `ome.io/btp.*` / `ome.io/dr.*` pass-through escape hatches) live as `ome.io/*` annotations on the InferenceService; setting any of them also counts as declaring traffic intent for everything described below.

> **Note:** progressive rollout (canary and blue-green traffic splitting) is a separate concern configured on `spec.rollout`, not here.

## How OME applies the policy

At controller startup, OME probes the cluster for backend-policy CRDs and picks exactly one **translator** for the whole process, in priority order:

| Priority | CRD probed | Translator | Emitted resource |
|----------|-----------|------------|------------------|
| 1 | `gateway.envoyproxy.io/v1alpha1` `BackendTrafficPolicy` | `envoy-gateway` | One `BackendTrafficPolicy` named after the InferenceService, with `targetRefs` listing every OME-managed HTTPRoute |
| 2 | `networking.istio.io/v1` `DestinationRule` | `istio` | One `DestinationRule` named after the InferenceService, targeting the top-level Service host (`<name>.<namespace>.svc.cluster.local`) |
| 3 | neither | `noop` | Nothing — declared intent is ignored and surfaced as `BackendPolicyReady=False` / `NoTranslatorAvailable` |

On the rare cluster that ships both, Envoy Gateway wins — its `BackendTrafficPolicy` is the more expressive backend. Translator selection is a process-level decision; it is not overridable per service.

The emitted policy carries a controller owner reference to the InferenceService, so deleting the service garbage-collects it. Removing every traffic knob from a live service deletes the emitted policy and clears `status.traffic` on the next reconcile.

## Choosing an algorithm

`spec.traffic.algorithm` accepts four values:

| Algorithm | Behavior |
|-----------|----------|
| `RoundRobin` | Distributes connections evenly across backend endpoints in order. |
| `LeastRequest` | Sends each new request to the endpoint with the fewest active requests. Envoy Gateway's own default. |
| `Random` | Picks an endpoint at random. |
| `ConsistentHash` | Hashes a request attribute and consistently routes equal hashes to the same endpoint (session affinity). Requires the `consistentHash` block. |

```yaml
spec:
  traffic:
    algorithm: LeastRequest
```

When `algorithm` is unset (but some other traffic knob is declared), OME emits the policy without a load-balancer type and the gateway implementation's own default applies — `LeastRequest` for Envoy Gateway. `status.traffic.algorithm` reports `Default` in that case so you can tell "OME defaulted" apart from any explicit choice.

## Session affinity with ConsistentHash

`consistentHash` selects the hash source. It is required when `algorithm: ConsistentHash` and rejected at admission with any other algorithm (`MissingConsistentHashSpec` / `UnexpectedConsistentHashSpec`). Exactly one hash source is allowed per the `type`:

**Header** — hash one or more request headers:

```yaml
spec:
  traffic:
    algorithm: ConsistentHash
    consistentHash:
      type: Header
      headers:
        - name: x-tenant-id
        - name: x-session-id
```

With more than one header, translators that support multi-header hashing (Envoy Gateway) concatenate the headers before hashing. When the active translator hashes on a single header only (Istio), a multi-header spec is rejected at admission (`UnsupportedMultiHeaderHash`) rather than silently hashing on a subset — that would quietly change session affinity.

**Cookie** — hash a named cookie:

```yaml
spec:
  traffic:
    algorithm: ConsistentHash
    consistentHash:
      type: Cookie
      cookie:
        name: session-affinity
        ttlSeconds: 3600
```

When `ttlSeconds` is greater than zero, the gateway auto-issues the cookie on requests that lack it. When zero or unset, the client is responsible for sending the cookie.

**SourceIP** — hash the client source IP; no sub-fields, and `headers` / `cookie` are forbidden:

```yaml
spec:
  traffic:
    algorithm: ConsistentHash
    consistentHash:
      type: SourceIP
```

## Endpoint override (Envoy Gateway only)

`endpointOverride` routes a request to the exact pod named by a request header, falling back to the configured algorithm when the header is absent or the named endpoint is unhealthy. This is useful when an external scheduler (for example a prefill/decode router) has already picked the pod:

```yaml
spec:
  traffic:
    algorithm: LeastRequest
    endpointOverride:
      type: Header
      headers:
        - name: x-endpoint-hostport
```

Each header value must be `<host>:<port>` or `<ip>:<port>` (for example `x-endpoint-hostport: 10.0.0.5:30000`). The feature maps to Envoy Gateway's `loadBalancer.endpointOverride`; there is no Istio DestinationRule equivalent, so on an Istio cluster the field is dropped and surfaced through the `BackendPolicyUnsupportedFields` condition (see below). `type: Metadata` is a reserved value — admission rejects it (`ReservedEndpointOverrideType`) until a translator implements it.

## What applies when you declare nothing

When an InferenceService declares no traffic intent at all — no `spec.traffic` and no `ome.io/*` traffic annotation — `status.traffic` stays empty, and on clusters using Gateway API ingress the ingress reconciler emits a **default** `BackendTrafficPolicy` instead: `ConsistentHash` on the headers named by the operator-level ingress config `consistentHashHeaders` (default `x-routing-key`). Requests carrying the same `x-routing-key` value stick to the same pod; requests without the header are not pinned to any particular pod.

You can change the default's headers per service without declaring full traffic intent, via a comma-separated annotation:

```yaml
metadata:
  annotations:
    ome.io/ingress-consistent-hash-headers: "x-tenant-id, x-session-id"
```

The default policy targets only the top-level HTTPRoute, merges into any Gateway-level parent policy (`mergeType: StrategicMerge`), and reports its acceptance through the `IngressReady` condition rather than `status.traffic`.

The moment you declare any traffic intent, the per-service traffic reconciler takes ownership and the default is replaced entirely by your declared policy. Note the consequence: declaring, say, just a retry annotation removes the `x-routing-key` session affinity — the algorithm reverts to the gateway default unless you also declare it.

## Translator support matrix

Not every gateway backend implements every field. Envoy Gateway's `BackendTrafficPolicy` covers the full typed surface; Istio's `DestinationRule` covers a subset:

| Capability | Envoy Gateway | Istio |
|------------|---------------|-------|
| `algorithm` (all four values) | ✓ | ✓ (`ROUND_ROBIN` / `LEAST_REQUEST` / `RANDOM` / `consistentHash`) |
| `consistentHash.type: Header` (single header) | ✓ | ✓ |
| `consistentHash` on multiple headers | ✓ | ✗ — rejected at admission |
| `consistentHash.type: Cookie` | ✓ | ✓ |
| `consistentHash.type: SourceIP` | ✓ | ✓ |
| `endpointOverride.type: Header` | ✓ | ✗ — dropped, surfaced in status |
| `endpointOverride.type: Metadata` | ✗ reserved | ✗ reserved |

Unsupported intent is handled two ways, by design:

- **Rejected at admission** when partial application would silently change behavior: the reserved `endpointOverride.type: Metadata` (always), and multi-header hashing when the active translator hashes a single header.
- **Admitted but surfaced in status** when the field is dropped wholesale (for example `endpointOverride` on an Istio cluster): the service still deploys, and the `BackendPolicyUnsupportedFields` condition names the active translator and lists exactly what was ignored.

The Istio translator also targets only the top-level Service hostname — per-component Services (engine, decoder, router) are not covered by the emitted DestinationRule; hand-author additional DestinationRules if you need per-component behavior there.

## Reading status.traffic

`status.traffic` is populated only when traffic intent is declared; otherwise it is absent entirely. A healthy Envoy Gateway example:

```yaml
status:
  traffic:
    algorithm: ConsistentHash
    backendPolicyResource:
      apiVersion: gateway.envoyproxy.io/v1alpha1
      kind: BackendTrafficPolicy
      name: llama-chat
    targetedHTTPRoutes:
      - llama-chat
      - llama-chat-engine
    conditions:
      - type: BackendPolicyReady
        status: "True"
        reason: AcceptedByGateway
```

| Field | Meaning |
|-------|---------|
| `algorithm` | The resolved algorithm: `RoundRobin`, `LeastRequest`, `Random`, `ConsistentHash`, or `Default` when no algorithm was declared and the gateway's own default applies. |
| `backendPolicyResource` | `apiVersion` / `kind` / `name` of the OME-emitted policy, in the same namespace as the InferenceService. |
| `targetedHTTPRoutes` | The HTTPRoute names the emitted policy targets — no need to inspect the policy resource to see its scope. |
| `conditions` | Translation, conflict, and gateway-acceptance state (below). |

### The BackendPolicyReady condition

`BackendPolicyReady` tracks the emitted policy end to end — `True` only once the gateway controller has acknowledged it:

| Status | Reason | Meaning |
|--------|--------|---------|
| `True` | `AcceptedByGateway` | The gateway controller accepted the emitted policy. |
| `Unknown` | `Pending` | Policy emitted; awaiting the gateway controller's acceptance signal (normal right after a change). |
| `False` | `GatewayRejected` | The gateway controller rejected the policy; the message passes through the gateway's own reason. |
| `False` | `ConflictingPolicy` | A policy with the OME-managed name (the InferenceService name) already exists and is not owned by this service — hand-authored or owned by something else. OME refuses to overwrite it. |
| `False` | `TranslationFailed` | The active translator errored turning the intent into a policy resource; the message carries the error. |
| `False` | `NoTranslatorAvailable` | No supported backend-policy CRD is installed; the declared intent is ignored. |

### The BackendPolicyUnsupportedFields condition

Added (with `status: "True"`, reason `UnsupportedField`) only when the active translator dropped operator-declared fields or annotations — for example an `endpointOverride` on an Istio cluster, or an `ome.io/dr.*` annotation on an Envoy Gateway cluster. The message names the active translator and lists every dropped item. Absence of the condition means nothing was dropped.

## Conflicts and per-component overrides

OME names its emitted policy after the InferenceService. If a resource with that name already exists and is not controller-owned by the service, OME defers to it and reports `ConflictingPolicy` rather than overwriting.

This deference is also the supported path for per-component routing: when one component needs a different policy, hand-author a separate `BackendTrafficPolicy` under a *different* name targeting only that component's HTTPRoute (for example `llama-chat-decoder`). It coexists with the OME-emitted policy without conflict.

## Reference

- API fields: [`TrafficSpec` and `TrafficStatus`](/ome/docs/reference/ome.v1beta1)
- Related concepts: [Inference Service](/ome/docs/concepts/inference_service), [Ingress and External Access](/ome/docs/concepts/ingress)
