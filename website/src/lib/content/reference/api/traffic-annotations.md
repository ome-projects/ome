---
title: Traffic annotations
description: "These ome.io annotations tune circuit breaking, retries and timeouts for an InferenceService's backends, with pass-through prefixes for Envoy Gateway and Istio."
since: v1.3
---

Annotations on an [InferenceService](../../concepts/serving/inference-services.md) set circuit breakers, retries and connection timeouts for its backends, so you can cap the load on its pods, retry failed requests and close idle connections. OME writes them into one backend policy for your gateway: a `gateway.envoyproxy.io/v1alpha1` BackendTrafficPolicy on Envoy Gateway, or a `networking.istio.io/v1` DestinationRule on Istio. [Pass-through annotations](#pass-through-prefixes) set the policy's other fields, and [`ome.io/service-type`](#service-type-annotation) sets the type of the Services that OME creates. For load balancing and session affinity, use `spec.traffic`: see [Traffic policy](../../concepts/rollouts-and-traffic/traffic-policy.md).

OME writes the policy while the InferenceService has traffic intent: `spec.traffic`, or a circuit breaker, retry, timeout or pass-through annotation. It deletes the policy when you remove them all. `ome.io/service-type` and `ome.io/load-balancer-ip` set only Services, so they aren't traffic intent. The policy takes effect on Envoy Gateway when ingress uses the Gateway API, and on Istio while ingress creation is off, which is the default. The controller picks the gateway when it starts, and prefers Envoy Gateway when both are installed: see [Where the policy takes effect](../../concepts/rollouts-and-traffic/traffic-policy.md#which-gateway-ome-writes-for).

Annotation values are strings, so quote numbers in YAML: `"1024"`, not `1024`. Retry and timeout durations take the units `ns`, `us`, `ms`, `s`, `m` and `h`, as in `10s` or `1m30s`. OME writes them in normal form, so `5m` becomes `5m0s`. When you leave out a circuit breaker, retry or timeout annotation, the gateway's default applies. The circuit breaker, retry, timeout and pass-through annotations need v1.3, while `ome.io/service-type` and `ome.io/load-balancer-ip` work on v1.2.2 too.

!!! warning "Changing these annotations on a running InferenceService"
    OME copies the InferenceService's annotations to its components' pod templates, so adding, changing or removing one replaces the pods of a `RawDeployment` component. An OMENative component keeps its running pods, and the change doesn't start a rollout. With Gateway API ingress, OME's default policy hashes requests on routing-key headers, and traffic intent replaces it. To keep the hashing, also set consistent hashing in `spec.traffic`, as [Defaults when you declare nothing](../../concepts/rollouts-and-traffic/traffic-policy.md#defaults-when-you-declare-nothing) shows.

## Circuit breaker annotations

Circuit breakers cap the load that the gateway sends to the InferenceService's pods. The values are whole numbers. For what the fields do and their defaults, see Envoy Gateway's [BackendTrafficPolicy](https://gateway.envoyproxy.io/docs/api/extension_types/) or Istio's [DestinationRule](https://istio.io/latest/docs/reference/config/networking/destination-rule/).

| Annotation | Limits | Envoy Gateway field | Istio field |
| --- | --- | --- | --- |
| `ome.io/circuit-breaker-max-connections` | Connections to all the pods | `spec.circuitBreaker.maxConnections` | `spec.trafficPolicy.connectionPool.tcp.maxConnections` |
| `ome.io/circuit-breaker-max-parallel-requests` | Requests in flight | `spec.circuitBreaker.maxParallelRequests` | `spec.trafficPolicy.connectionPool.http.http2MaxRequests` |
| `ome.io/circuit-breaker-max-pending-requests` | Requests waiting for a connection | `spec.circuitBreaker.maxPendingRequests` | `spec.trafficPolicy.connectionPool.http.http1MaxPendingRequests` |
| `ome.io/circuit-breaker-max-parallel-retries` | Retries in flight | `spec.circuitBreaker.maxParallelRetries` | Not supported |
| `ome.io/circuit-breaker-per-endpoint-max-connections` | Connections to each pod | `spec.circuitBreaker.perEndpoint.maxConnections` | Not supported |

With `spec.traffic.algorithm: ConsistentHash`, the webhook [warns](#admission-validation) about `ome.io/circuit-breaker-per-endpoint-max-connections` above 1.

## Retry annotations

Retries resend a request that failed. OME writes them only for Envoy Gateway. On Istio, it lists them in [`BackendPolicyUnsupportedFields`](#when-the-translator-cant-honor-a-key).

| Annotation | Value | Envoy Gateway field |
| --- | --- | --- |
| `ome.io/retry-attempts` | A whole number. Above 0, it needs `ome.io/retry-on`. | `spec.retry.numRetries` |
| `ome.io/retry-on` | A comma-separated list of `5xx`, `reset`, `gateway-error`, `connect-failure` and `retriable-status-codes`. Case-sensitive. | `spec.retry.retryOn.triggers` |
| `ome.io/retry-per-try-timeout` | A duration | `spec.retry.perRetry.timeout` |

OME can't set `spec.retry.retryOn.httpStatusCodes`, the status codes that `retriable-status-codes` retries on: `ome.io/retry-on` takes only condition names, and pass-throughs set only single values.

## Timeout annotations

| Annotation | Limits | Envoy Gateway field | Istio field |
| --- | --- | --- | --- |
| `ome.io/timeout-idle` | How long a connection can stay idle | `spec.timeout.http.connectionIdleTimeout` | `spec.trafficPolicy.connectionPool.http.idleTimeout` |
| `ome.io/timeout-max-connection-duration` | How long a connection can stay open | `spec.timeout.http.maxConnectionDuration` | Not supported |
| `ome.io/timeout-tcp-connect` | How long to wait for a new connection | `spec.timeout.tcp.connectTimeout` | `spec.trafficPolicy.connectionPool.tcp.connectTimeout` |

These annotations set connection timeouts. OME sets the request timeout on its HTTPRoutes from each component's `timeoutSeconds` or a cluster-wide default: see [Configure route timeouts](../../guides/networking/configure-route-timeouts.md). For a per-try timeout, use `ome.io/retry-per-try-timeout`.

## Example

This InferenceService uses the BaseModel and runtime from [Serve models from a PVC](../../guides/deploy-models/serve-models-from-pvc.md), in the namespace `llama-demo`.

```yaml title="llama-chat.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
  annotations:
    ome.io/circuit-breaker-max-connections: "1024"
    ome.io/circuit-breaker-max-pending-requests: "256"
    ome.io/retry-attempts: "3"
    ome.io/retry-on: "connect-failure,reset,5xx"
    ome.io/retry-per-try-timeout: "30s"
    ome.io/timeout-idle: "5m"
    ome.io/timeout-tcp-connect: "10s"
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
kubectl apply -f llama-chat.yaml
```

```output
inferenceservice.ome.io/llama-chat created
```

What OME writes depends on the gateway:

=== "Envoy Gateway"

    OME writes a BackendTrafficPolicy `llama-chat` with this spec.

    ```yaml
    spec:
      targetRefs:
      - group: gateway.networking.k8s.io
        kind: HTTPRoute
        name: llama-chat
      - group: gateway.networking.k8s.io
        kind: HTTPRoute
        name: llama-chat-engine
      circuitBreaker:
        maxConnections: 1024
        maxPendingRequests: 256
      retry:
        numRetries: 3
        perRetry:
          timeout: 30s
        retryOn:
          triggers:
          - connect-failure
          - reset
          - 5xx
      timeout:
        http:
          connectionIdleTimeout: 5m0s
        tcp:
          connectTimeout: 10s
    ```

    Once OME has written the policy, `status.traffic` names it:

    ```bash
    kubectl get inferenceservice llama-chat -n llama-demo -o jsonpath='{.status.traffic.backendPolicyResource.kind}'
    ```

    ```output
    BackendTrafficPolicy
    ```

=== "Istio"

    OME writes a DestinationRule `llama-chat` with this spec, and lists the three retry annotations in `BackendPolicyUnsupportedFields`:

    ```yaml
    spec:
      host: llama-chat.llama-demo.svc.cluster.local
      trafficPolicy:
        connectionPool:
          tcp:
            maxConnections: 1024
            connectTimeout: 10s
          http:
            http1MaxPendingRequests: 256
            idleTimeout: 5m0s
    ```

    Once OME has written the policy, `status.traffic` names it:

    ```bash
    kubectl get inferenceservice llama-chat -n llama-demo -o jsonpath='{.status.traffic.backendPolicyResource.kind}'
    ```

    ```output
    DestinationRule
    ```

=== "Neither"

    OME writes nothing.

    ```bash
    kubectl get inferenceservice llama-chat -n llama-demo -o jsonpath='{.status.traffic.conditions[?(@.type=="BackendPolicyReady")].reason}'
    ```

    ```output
    NoTranslatorAvailable
    ```

## Pass-through prefixes

A pass-through annotation sets any field of the policy. Its key is a prefix, then the field's path, with dots between its parts:

| Prefix | Gateway | OME sets |
| --- | --- | --- |
| `ome.io/btp.` | Envoy Gateway | `spec.<path>` of the BackendTrafficPolicy |
| `ome.io/dr.` | Istio | `spec.trafficPolicy.<path>` of the DestinationRule |

OME converts the value to a whole number, a boolean or a decimal, trying them in that order, and keeps it as a string when none fits. So a pass-through can't set a list, an object, or a string that looks like a number or a boolean. OME applies pass-throughs last, so they win over any annotation or `spec.traffic` field that sets the same field.

=== "Envoy Gateway"

    ```yaml
    metadata:
      annotations:
        ome.io/retry-attempts: "3"
        ome.io/retry-on: "connect-failure"
        ome.io/btp.retry.numRetries: "5"
    ```

    OME writes `numRetries: 5` under `spec.retry`: the pass-through overrides `ome.io/retry-attempts`.

=== "Istio"

    ```yaml
    metadata:
      annotations:
        ome.io/dr.outlierDetection.consecutive5xxErrors: "5"
        ome.io/dr.outlierDetection.baseEjectionTime: "30s"
    ```

    OME writes `consecutive5xxErrors: 5` and `baseEjectionTime: 30s` under `spec.trafficPolicy.outlierDetection` of the DestinationRule.

The webhook rejects the other gateway's prefix, but doesn't check paths or values. When a value has the wrong type, the API server rejects the policy, and OME records a [`TrafficReconcileError`](#status) event. A misspelled path is dropped. To check what OME wrote, read the policy, for example with `kubectl get backendtrafficpolicy llama-chat -n llama-demo -o yaml`.

## Status and events {#status}

OME reports the policy it wrote in `status.traffic`, with a `BackendPolicyReady` condition for whether the gateway accepted it: see [Read the traffic status](../../concepts/rollouts-and-traffic/traffic-policy.md#read-the-traffic-status). On Envoy Gateway, the condition stays `Pending` even after the gateway accepts the policy. To see the gateway's verdict, read the conditions under `status.ancestors` in the BackendTrafficPolicy. This is a known bug.

When OME can't write the policy, it records a `TrafficReconcileError` event, and the InferenceService's status stops changing until the error clears. [The traffic status doesn't change](../../concepts/rollouts-and-traffic/traffic-policy.md#the-traffic-status-doesnt-change) lists the causes and how to fix them.

### Unsupported annotations {#when-the-translator-cant-honor-a-key}

On Istio, OME leaves out the retry annotations and the ones marked Not supported, and writes the rest. It lists what it left out in the condition `BackendPolicyUnsupportedFields`. With the three retry annotations:

```yaml
- type: BackendPolicyUnsupportedFields
  status: "True"
  reason: UnsupportedField
  message: 'translator "istio" does not honor 3 operator-declared annotation(s) [ome.io/retry-attempts ome.io/retry-on ome.io/retry-per-try-timeout]'
```

On Istio, the message also lists [endpoint override](../../concepts/rollouts-and-traffic/traffic-policy.md#endpoint-override) when you set it.

## Admission validation

The admission webhook checks the circuit breaker, retry and timeout annotations, and pass-through prefixes, when you create or update an InferenceService. When it rejects one, kubectl prints `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:`, then a message that ends in a reason in parentheses:

```text
annotation "ome.io/retry-attempts" set to 3 requires "ome.io/retry-on" to specify retry conditions (MissingRetryOn)
```

| Reason | Cause |
| --- | --- |
| `InvalidIntValue` | A circuit breaker annotation or `ome.io/retry-attempts` isn't a whole number. |
| `InvalidDuration` | A timeout annotation or `ome.io/retry-per-try-timeout` isn't a valid duration: `30` needs a unit, as in `30s`. |
| `InvalidRetryOn` | `ome.io/retry-on` is empty, has an empty item, as in `5xx,,reset`, or has an item outside the five conditions, such as `503`. |
| `MissingRetryOn` | `ome.io/retry-attempts` is above 0 without `ome.io/retry-on`. |
| `UnsupportedPassthrough` | A pass-through uses the other gateway's prefix. The message lists the prefix to use. |
| `UnknownTrafficAnnotation` | An `ome.io/` key, such as `ome.io/retry-attempt`, is one or two characters off a [key the webhook knows](labels-and-annotations.md). The message names that key. |
| `InvalidBoolValue` | `ome.io/managed-by-conflict-acked` has a value other than `true` or `false`. OME ignores this annotation. |

With `spec.traffic.algorithm: ConsistentHash`, the webhook admits `ome.io/circuit-breaker-per-endpoint-max-connections` above 1, and kubectl prints this after `Warning:`:

```text
annotation "ome.io/circuit-breaker-per-endpoint-max-connections"=4 combined with traffic.algorithm=ConsistentHash usually defeats sticky routing; set to 1 to keep sessions on a single pod
```

## Service type annotation

`ome.io/service-type` sets the type of the Services that OME creates for an InferenceService: `ClusterIP`, the default, `NodePort` or `LoadBalancer`. Values are case-sensitive, and any other value gives `ClusterIP`.

```yaml
metadata:
  annotations:
    ome.io/service-type: LoadBalancer
```

The type applies to the [Service named after the InferenceService](../../concepts/rollouts-and-traffic/ingress.md#the-external-service), which exists only while ingress creation is off. OME copies the InferenceService's annotations that start with `service.beta.kubernetes.io/`, `cloud.google.com/` or `service.kubernetes.io/` to that Service. On a `NodePort` or `LoadBalancer` Service, OME resets changes that you make, such as setting `externalTrafficPolicy`. Give Services of your own other names: OME changes or deletes any Service with the InferenceService's name.

The type also applies to each component Service, such as `llama-chat-engine`, when OME creates it, so with `LoadBalancer` each component can get a load balancer of its own. A later change doesn't reach an existing component Service. `ome.io/load-balancer-ip` sets `spec.loadBalancerIP` on component Services of type `LoadBalancer`, not on the Service named after the InferenceService. [Expose a service without ingress](../../guides/networking/expose-without-ingress.md) shows how to use `ome.io/service-type`, and how to keep the component Services internal.

## Related pages

- [Traffic policy](../../concepts/rollouts-and-traffic/traffic-policy.md): the typed `spec.traffic` fields, and where the policy takes effect.
- [`kubectl ome traffic`](../kubectl-ome/traffic.md): show the policy that OME wrote and its status.
- [Configure route timeouts](../../guides/networking/configure-route-timeouts.md): the request timeout on OME's HTTPRoutes.
- [Expose a service without ingress](../../guides/networking/expose-without-ingress.md): expose an InferenceService with `ome.io/service-type`.
- [Configure ingress](../../guides/networking/configure-ingress.md): turn on ingress creation.
- [Labels and annotations](labels-and-annotations.md): the other labels and annotations OME reads and sets.
- [OME API](ome.v1beta1.md#ome-io-v1beta1-TrafficStatus): the `status.traffic` fields.
