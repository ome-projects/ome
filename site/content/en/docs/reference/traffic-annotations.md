---
title: Traffic Annotations
linkTitle: Traffic Annotations
weight: 3
description: >
  Reference for the ome.io/* annotations that tune backend traffic resilience on an
  InferenceService — circuit breaking, retries, and connection timeouts — plus the raw
  pass-through prefixes for Envoy Gateway and Istio.
---

InferenceService traffic management has two layers:

- **Typed core** — `spec.traffic` holds the load-balancing configuration (algorithm,
  consistent hashing, endpoint override).
- **Annotation extension** — long-tail resilience knobs that did not graduate to the
  typed API live as `ome.io/*` annotations on the InferenceService metadata.

This page is the reference for the annotation extension. All keys are validated by the
InferenceService admission webhook, resolved by the controller's traffic translator, and
threaded into a single backend policy resource per InferenceService.

For other OME annotations, see [Labels and Annotations](/ome/docs/reference/labels-and-annotations).

## How annotations become gateway configuration

At startup the OME manager selects exactly one **traffic translator** for the lifetime of
the controller process, by probing for backend-policy CRDs in priority order:

1. **Envoy Gateway** — the `gateway.envoyproxy.io/v1alpha1` `BackendTrafficPolicy` CRD is
   installed.
2. **Istio** — the `networking.istio.io/v1` `DestinationRule` CRD is installed.
3. **Noop** — fallback when neither CRD is available.

Envoy Gateway wins on clusters that ship both, because `BackendTrafficPolicy` is the more
expressive backend. The selection is process-level, not per-InferenceService.

When an InferenceService declares any traffic intent (`spec.traffic` or any of the
annotations below), the translator emits one policy resource named after the
InferenceService, in the same namespace, with a controller owner reference:

- The **Envoy Gateway** translator emits a `BackendTrafficPolicy` whose `spec.targetRefs`
  lists the OME-managed HTTPRoutes: `<name>`, `<name>-engine`, plus `<name>-decoder` and
  `<name>-router` when those components exist.
- The **Istio** translator emits a `DestinationRule` with
  `spec.host: <name>.<namespace>.svc.cluster.local` — the top-level Service. Per-component
  Services are not covered; hand-author additional DestinationRules if you need
  per-component behavior.
- The **Noop** translator emits nothing. Annotations are still admitted, but the
  InferenceService reports `status.traffic` condition `BackendPolicyReady=False` with
  reason `NoTranslatorAvailable`.

Removing all traffic intent deletes the previously emitted policy resource.

## Circuit breaker annotations

All values are integers.

| Annotation | BackendTrafficPolicy (Envoy Gateway) | DestinationRule (Istio) |
|------------|--------------------------------------|-------------------------|
| `ome.io/circuit-breaker-max-connections` | `spec.circuitBreaker.maxConnections` | `spec.trafficPolicy.connectionPool.tcp.maxConnections` |
| `ome.io/circuit-breaker-max-parallel-requests` | `spec.circuitBreaker.maxParallelRequests` | `spec.trafficPolicy.connectionPool.http.http2MaxRequests` |
| `ome.io/circuit-breaker-max-pending-requests` | `spec.circuitBreaker.maxPendingRequests` | `spec.trafficPolicy.connectionPool.http.http1MaxPendingRequests` |
| `ome.io/circuit-breaker-max-parallel-retries` | `spec.circuitBreaker.maxParallelRetries` | not supported (dropped) |
| `ome.io/circuit-breaker-per-endpoint-max-connections` | `spec.circuitBreaker.perEndpoint.maxConnections` | not supported (dropped) |

Setting `ome.io/circuit-breaker-per-endpoint-max-connections` to a value greater than 1
together with `spec.traffic.algorithm: ConsistentHash` produces an admission **warning**
(not a rejection): a per-endpoint cap above 1 usually defeats sticky routing.

## Retry annotations

Retries are supported by the Envoy Gateway translator only. The Istio `DestinationRule`
API does not model retries (they live on VirtualService), so on an Istio cluster all three
keys are admitted but dropped, and surface in the
[`BackendPolicyUnsupportedFields` condition](#when-the-active-translator-cannot-honor-a-key).

| Annotation | Value | BackendTrafficPolicy (Envoy Gateway) |
|------------|-------|--------------------------------------|
| `ome.io/retry-attempts` | integer | `spec.retry.numRetries` |
| `ome.io/retry-on` | comma-separated condition tokens | `spec.retry.retryOn.triggers` |
| `ome.io/retry-per-try-timeout` | Go duration (e.g. `2s`) | `spec.retry.perRetry.timeout` |

`ome.io/retry-on` accepts these tokens (case-sensitive):

- `5xx`
- `reset`
- `gateway-error`
- `connect-failure`
- `retriable-status-codes`

Any other token is rejected at admission (`InvalidRetryOn`).

**Cross-rule:** `ome.io/retry-attempts` greater than 0 requires `ome.io/retry-on` to be
set; otherwise admission rejects the object (`MissingRetryOn`).

## Timeout annotations

All values are Go duration strings (`30s`, `1m30s`, `500ms`). These cover connection-level
timeouts; the **request-level timeout is not an annotation** — it stays on the typed
per-component field `spec.<component>.timeoutSeconds` and is applied to the generated
HTTPRoute, not to the backend policy.

| Annotation | BackendTrafficPolicy (Envoy Gateway) | DestinationRule (Istio) |
|------------|--------------------------------------|-------------------------|
| `ome.io/timeout-idle` | `spec.timeout.http.connectionIdleTimeout` | `spec.trafficPolicy.connectionPool.http.idleTimeout` |
| `ome.io/timeout-max-connection-duration` | `spec.timeout.http.maxConnectionDuration` | not supported (dropped) |
| `ome.io/timeout-tcp-connect` | `spec.timeout.tcp.connectTimeout` | `spec.trafficPolicy.connectionPool.tcp.connectTimeout` |

## Pass-through prefixes

For implementation-specific fields OME does not type-model, two annotation prefixes write
values verbatim into the emitted policy resource:

| Prefix | Active translator | Written to |
|--------|-------------------|------------|
| `ome.io/btp.<path>` | Envoy Gateway | `BackendTrafficPolicy` `spec.<path>` |
| `ome.io/dr.<path>` | Istio | `DestinationRule` `spec.trafficPolicy.<path>` |

`<path>` is a dotted field path. For example:

```yaml
metadata:
  annotations:
    # Envoy Gateway: BackendTrafficPolicy spec.loadBalancer.slowStart.window
    ome.io/btp.loadBalancer.slowStart.window: "30s"
```

```yaml
metadata:
  annotations:
    # Istio: DestinationRule spec.trafficPolicy.outlierDetection.consecutive5xxErrors
    ome.io/dr.outlierDetection.consecutive5xxErrors: "5"
```

Pass-through semantics:

- Values are best-effort coerced to a scalar: integer, then boolean, then float, falling
  back to the raw string.
- Pass-throughs are applied **last**, so they win over any structured field the
  translator wrote at the same path — this is the documented escape hatch for overriding
  OME's own output.
- OME performs **no schema validation** of the path or value. If the gateway controller
  rejects the emitted resource, the InferenceService reports `BackendPolicyReady=False`
  with reason `GatewayRejected` and the gateway's message.
- Each prefix belongs to one translator. Using a prefix the **active** translator does not
  own — for example `ome.io/dr.*` on an Envoy Gateway cluster — is rejected at admission
  (`UnsupportedPassthrough`), listing the supported prefixes.
- Applied pass-through paths are echoed in the `BackendPolicyReady` condition message
  (for example `backend policy emitted with 1 pass-through field(s)
  ([loadBalancer.slowStart.window]); awaiting gateway acceptance`) so you can audit what
  was stitched in without reading the policy resource.

## Admission validation

The InferenceService validating webhook checks every annotation before the object is
persisted:

- **Type checks.** Circuit-breaker keys and `ome.io/retry-attempts` must parse as
  integers (`InvalidIntValue`); timeout keys and `ome.io/retry-per-try-timeout` must
  parse as Go durations (`InvalidDuration`); `ome.io/retry-on` must be a non-empty list
  of the supported tokens (`InvalidRetryOn`).
- **Did-you-mean.** An unrecognized key in the `ome.io/` namespace that is within edit
  distance 2 of a documented traffic annotation is rejected with a suggestion
  (`UnknownTrafficAnnotation`), catching typos like `ome.io/retry-attempt`. Other
  `ome.io/*` keys (OME has many non-traffic annotations) and annotations in foreign
  namespaces are ignored by this check.
- **Cross-rules.** `retry-attempts > 0` without `retry-on` is an error; the sticky-routing
  per-endpoint cap check above is a warning.
- **Pass-through capability.** The webhook is wired with the active translator's
  supported pass-through prefixes, so a wrong-backend pass-through fails fast at
  admission rather than at reconcile time.

Note the asymmetry: pass-through prefixes are capability-checked at admission, but the
per-key annotations are **not** — for example `ome.io/retry-attempts` on an Istio cluster
is admitted with only its value validated, then dropped at translation time and surfaced
through status. This keeps InferenceService manifests portable across clusters with
different gateway implementations.

## When the active translator cannot honor a key

Admitted keys the active translator cannot represent are not silently lost. The
reconciler compares everything you declared against the translator's supported set and
adds a `BackendPolicyUnsupportedFields` condition to `status.traffic`:

```yaml
status:
  traffic:
    algorithm: Default
    backendPolicyResource:
      apiVersion: networking.istio.io/v1
      kind: DestinationRule
      name: llama-chat
    targetedHTTPRoutes:
      - llama-chat
      - llama-chat-engine
    conditions:
      - type: BackendPolicyReady
        status: "True"
        reason: AcceptedByGateway
        message: backend policy accepted by gateway controller
      - type: BackendPolicyUnsupportedFields
        status: "True"
        reason: UnsupportedField
        message: 'translator "istio" does not honor 2 operator-declared annotation(s)
          [ome.io/retry-attempts ome.io/retry-on]'
```

The condition names the active translator and lists every dropped typed `spec.traffic`
field and annotation key. It follows positive-polarity convention: it is **omitted
entirely** when nothing was dropped, and also omitted when translation failed or the Noop
translator is active — in those cases `BackendPolicyReady` already explains the
situation.

`BackendPolicyReady` is the primary condition; its reasons are:

| Reason | Status | Meaning |
|--------|--------|---------|
| `AcceptedByGateway` | `True` | The gateway controller accepted the emitted policy. |
| `Pending` | `Unknown` | Policy emitted; awaiting the gateway controller's acceptance signal. |
| `GatewayRejected` | `False` | The gateway controller rejected the policy (e.g. a bad pass-through path); the gateway's message is passed through. |
| `TranslationFailed` | `False` | The translator could not turn the intent into a policy resource. |
| `ConflictingPolicy` | `False` | A policy with the OME-managed name already exists and is not owned by this InferenceService; OME did not overwrite it. |
| `NoTranslatorAvailable` | `False` | No backend-policy CRD is installed; traffic intent is ignored. |

`status.traffic` also reports `backendPolicyResource` (the emitted resource's apiVersion,
kind, and name) and `targetedHTTPRoutes` — the OME-managed HTTPRoute names computed for
the InferenceService. For Envoy Gateway these are the policy's `spec.targetRefs`; the
Istio `DestinationRule` instead targets the Service host as described above.

## Conflicting hand-authored policies

If a backend policy resource already exists with the name OME would use (the
InferenceService name) and is not owned by the InferenceService, OME refuses to overwrite
it and reports `BackendPolicyReady=False` with reason `ConflictingPolicy`. The
hand-authored resource stays authoritative.

The `ome.io/managed-by-conflict-acked: "true"` annotation is the escape hatch for
acknowledging such a conflict deliberately. Its value is validated at admission (must be
`"true"` or `"false"`). In the current alpha phase of the traffic-policy engine, OME
always defers to the pre-existing resource whether or not the annotation is set — the
annotation exists so manifests can opt out of the stricter admission-time rejection
planned for later phases without changes.

## Example

An InferenceService on an Envoy Gateway cluster with circuit breaking, retries, and
connection timeouts:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: default
  annotations:
    ome.io/circuit-breaker-max-connections: "1024"
    ome.io/circuit-breaker-max-pending-requests: "256"
    ome.io/retry-attempts: "3"
    ome.io/retry-on: "connect-failure,reset,5xx"
    ome.io/retry-per-try-timeout: "10s"
    ome.io/timeout-idle: "5m"
    ome.io/timeout-tcp-connect: "10s"
    # Escape hatch: any BackendTrafficPolicy field OME does not type-model.
    ome.io/btp.loadBalancer.slowStart.window: "30s"
spec:
  model:
    name: llama-3-70b-instruct
  runtime:
    name: srt-llama-3-70b-instruct
```

OME emits a `BackendTrafficPolicy` named `llama-chat` targeting the `llama-chat` and
`llama-chat-engine` HTTPRoutes, with the annotations resolved into
`spec.circuitBreaker`, `spec.retry`, and `spec.timeout`, and the `ome.io/btp.*` value
stitched into `spec.loadBalancer.slowStart.window`.

## Next steps

- [Labels and Annotations](/ome/docs/reference/labels-and-annotations) — all other OME
  labels and annotations.
- [Ingress Administration](/ome/docs/administration/ingress) — gateway and ingress
  configuration, including the HTTPRoutes the emitted policies target.
