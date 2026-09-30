---
title: Traffic policy
description: "Set an InferenceService's load balancing, session affinity and endpoint override in spec.traffic. OME writes them into the gateway's backend policy and reports the result in status."
since: v1.3
---

A traffic policy decides which of an [InferenceService](../serving/inference-services.md)'s pods serves each request. Use it to keep a conversation on one pod, to prefer the least busy pods, or to let the client pick the pod. You declare it in `spec.traffic`. OME writes it into your gateway's backend policy, an Envoy Gateway BackendTrafficPolicy or an Istio DestinationRule, and reports the result in `status.traffic`.

You set circuit breaking, retries and connection timeouts with [traffic annotations](../../reference/api/traffic-annotations.md), which OME writes into the same policy. A traffic policy doesn't split requests between revisions. During a canary, the new revision's share of requests follows its share of ready Instances, as [Canary progression](../../reference/rollouts/canary-progression.md#traffic-split) explains.

## Where the policy takes effect {#which-gateway-ome-writes-for}

When the controller starts, it looks for Envoy Gateway's BackendTrafficPolicy CRD in `gateway.envoyproxy.io/v1alpha1`, then for Istio's DestinationRule CRD in `networking.istio.io/v1`. It writes policies for the first gateway it finds:

| Gateway | OME writes | The policy takes effect when |
| --- | --- | --- |
| Envoy Gateway | A BackendTrafficPolicy for the InferenceService's HTTPRoutes | [Ingress creation](ingress.md#through-the-gateway-api) is on and uses the Gateway API. The `ome-resources` chart turns both off by default. |
| Istio | A DestinationRule for the [Service named after the InferenceService](ingress.md#the-external-service) | Ingress creation is off, which is the default, and the InferenceService isn't cluster-local. |

That Service leads to the router, or to the engine when there's no router. On a cluster with neither CRD, `status.traffic` reports `NoTranslatorAvailable`.

The controller logs its choice as `Selected traffic translator`. It picks only when it starts, so restart it after you install or remove a gateway's CRDs. With OME in the `ome` namespace:

```bash
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

After a switch from Istio to Envoy Gateway, the DestinationRules that OME wrote keep applying until you delete them, for example with `kubectl delete destinationrule llama-chat -n llama-demo`.

## Set a traffic policy {#how-ome-applies-the-policy}

The examples use the `llama-demo` namespace and the pre-configured model `llama-3-2-1b-instruct`, as in [A minimal InferenceService](../serving/inference-services.md#a-minimal-inferenceservice). This InferenceService runs two engine pods, and keeps each chat session on one of them:

```yaml title="llama-chat.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 2
    maxReplicas: 2
  traffic:
    algorithm: ConsistentHash
    consistentHash:
      type: Header
      headers:
        - name: x-session-id
```

Apply the file:

```bash
kubectl apply -f llama-chat.yaml
```

```output
Warning: Runtime vllm-llama-3-2-1b-instruct will be auto-selected for model llama-3-2-1b-instruct
inferenceservice.ome.io/llama-chat created
```

- `algorithm: ConsistentHash` sends the requests that share the value of a request attribute to the same pod.
- `consistentHash` makes that attribute the `x-session-id` header. Every request with the same session ID reaches the same pod, as long as the set of pods doesn't change.

OME acts on traffic intent: any field in `spec.traffic`, or any circuit breaker, retry, timeout or pass-through annotation. Without intent, [the defaults](#defaults-when-you-declare-nothing) apply.

OME names the policy after the InferenceService, creates it in the same namespace, and owns it. It overwrites changes you make to the policy by hand, and deletes the policy when you remove all traffic intent. Kubernetes deletes the policy with the InferenceService. A change to `spec.traffic` during a rollout can wait until the rollout finishes.

One policy covers the whole InferenceService, and `spec.traffic` has no per-component settings. To give one component other settings, write a policy of your own under another name. Under Istio, that's a DestinationRule for the component's Service, such as `llama-chat-engine`. Under Envoy Gateway, your BackendTrafficPolicy targets a route that OME's policy also targets, and Envoy Gateway's rules for overlapping policies decide which settings apply.

## Read the traffic status

`status.traffic` shows what OME did with your intent. On Envoy Gateway, after OME writes the policy for `llama-chat.yaml`:

```bash
kubectl get inferenceservice llama-chat -n llama-demo -o custom-columns='ALGORITHM:.status.traffic.algorithm,POLICY:.status.traffic.backendPolicyResource.kind,READY:.status.traffic.conditions[?(@.type=="BackendPolicyReady")].status,REASON:.status.traffic.conditions[?(@.type=="BackendPolicyReady")].reason'
```

```output
ALGORITHM        POLICY                 READY     REASON
ConsistentHash   BackendTrafficPolicy   Unknown   Pending
```

The `BackendPolicyReady` condition is one of:

| Status | Reason | Meaning |
| --- | --- | --- |
| `Unknown` | `Pending` | OME wrote the policy, and the gateway hasn't reported on it yet. |
| `True` | `AcceptedByGateway` | The gateway accepted the policy. |
| `False` | `GatewayRejected` | The gateway rejected the policy. The message gives the gateway's reason, when it has one. |
| `False` | `NoTranslatorAvailable` | The controller found no gateway CRD when it started. |

On Envoy Gateway, `BackendPolicyReady` stays `Pending` even after the gateway accepts the policy. To see the gateway's verdict, read the conditions under `status.ancestors` in the policy, for example with `kubectl get backendtrafficpolicy llama-chat -n llama-demo -o yaml`. This is a known bug. On Istio, the condition follows the DestinationRule's `Reconciled` condition, and stays `Pending` until Istio sets one.

When the gateway can't apply a field or annotation that you declared, OME leaves it out of the policy and lists it in a `BackendPolicyUnsupportedFields` condition. Under Istio, that happens to [endpoint override](#endpoint-override) and to [some traffic annotations](../../reference/api/traffic-annotations.md#when-the-translator-cant-honor-a-key).

## Choose a load-balancing algorithm

`spec.traffic.algorithm` picks how the gateway chooses a pod for each request. Every value works on both Envoy Gateway and Istio:

| Algorithm | How the gateway picks a pod |
| --- | --- |
| `RoundRobin` | Each pod in turn. |
| `LeastRequest` | Prefers the pods with the fewest active requests. This suits requests whose lengths vary widely, as LLM requests do. |
| `Random` | A pod at random. |
| `ConsistentHash` | The same pod for requests with the same header, cookie or client address. |

For example:

```yaml
spec:
  traffic:
    algorithm: LeastRequest
```

Without `algorithm`, the gateway uses its own default, LeastRequest on Envoy Gateway, and `status.traffic.algorithm` shows `Default`.

## Session affinity with ConsistentHash

With `algorithm: ConsistentHash`, the gateway hashes a request attribute and sends the requests with the same hash to the same pod. Hashing on a session ID keeps a conversation on the engine that has already cached its prompt prefix. Adding or removing a pod moves some sessions to other pods. `consistentHash.type` picks the attribute:

| Type | Hashes | Fields |
| --- | --- | --- |
| `Header` | The values of one or more request headers. | `headers`, a list of entries with a `name`. |
| `Cookie` | The value of a cookie. | `cookie.name`, and optionally `cookie.ttlSeconds`. |
| `SourceIP` | The client's IP address, as the gateway sees it. | None. |

=== "Header"

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

    Two or more headers need Envoy Gateway v1.7 or later. Istio hashes on a single header, so under Istio, the webhook rejects a second one.

=== "Cookie"

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

    With `ttlSeconds` above zero, the gateway sets the cookie, with that lifetime, when a request arrives without it. Otherwise, your clients send the cookie themselves. Older Istio releases require a cookie lifetime, so under Istio, set `ttlSeconds` above zero.

=== "SourceIP"

    ```yaml
    spec:
      traffic:
        algorithm: ConsistentHash
        consistentHash:
          type: SourceIP
    ```

## Endpoint override

`endpointOverride` lets the client pick the pod, for example when an external scheduler has already chosen one. With this setting, a request that carries an `x-endpoint-hostport` header goes to the pod that the header names:

```yaml
spec:
  traffic:
    algorithm: LeastRequest
    endpointOverride:
      type: Header
      headers:
        - name: x-endpoint-hostport
```

The header's value is the pod's address and port, such as `10.0.0.5:8080`. The vLLM runtime in these examples listens on port 8080. When the header is missing or names an unhealthy pod, the gateway falls back to `algorithm`, so always declare one.

Only Envoy Gateway supports endpoint override. Under Istio, the `BackendPolicyUnsupportedFields` condition names it:

```text
translator "istio" does not honor 1 typed spec.traffic field(s) [spec.traffic.endpointOverride.type=Header]
```

## Defaults when you declare nothing

Without [traffic intent](#how-ome-applies-the-policy), the gateway uses its own defaults, and the InferenceService has no `status.traffic`.

The exception is [ingress through the Gateway API](ingress.md#through-the-gateway-api). There, OME writes a default BackendTrafficPolicy, which needs Envoy Gateway v1.7 or later. It hashes on the `x-routing-key` and `X-SMG-Routing-Key` headers. To hash on others, set [`consistentHashHeaders`](../../guides/networking/configure-ingress.md#configuration-reference), or the `ome.io/ingress-consistent-hash-headers` annotation on one InferenceService. That annotation changes the default policy instead of replacing it.

Traffic intent replaces the default policy entirely, and removing the intent brings it back. So a traffic annotation on its own, such as `ome.io/circuit-breaker-max-connections`, ends the header hashing. To keep it, also declare `algorithm: ConsistentHash` with those headers.

## Troubleshooting

### The webhook rejects the InferenceService

`kubectl` prints `admission webhook "inferenceservice.ome-webhook-server.validator" denied the request:`, then a message that ends in a reason in parentheses:

```text
spec.traffic.consistentHash is required when spec.traffic.algorithm=ConsistentHash (MissingConsistentHashSpec)
```

| Reason | Cause |
| --- | --- |
| `MissingConsistentHashSpec` | `algorithm: ConsistentHash` without a `consistentHash` block. |
| `UnexpectedConsistentHashSpec` | A `consistentHash` block with another algorithm, or with none. |
| `MissingHashKey` | A `Header` hash that's missing `headers`, or a `Cookie` hash that's missing `cookie`. |
| `MultipleHashKeys` | A key that doesn't match the type, such as `cookie` with `type: Header`, or any key with `SourceIP`. |
| `UnsupportedMultiHeaderHash` | Two or more hash headers under Istio. |
| `MissingEndpointOverrideKey` | An `endpointOverride` of `type: Header` without `headers`. |
| `ReservedEndpointOverrideType` | An `endpointOverride` of `type: Metadata`, which is reserved. |

[Traffic annotations](../../reference/api/traffic-annotations.md#admission-validation) lists the checks on annotations.

### The traffic status doesn't change

When OME can't write the policy, it records a `TrafficReconcileError` event on the InferenceService and retries. That happens when another object has the policy's name, or when the API server rejects the policy. Until the error clears, the InferenceService's whole status, `status.traffic` included, keeps its earlier values. Read the events:

```bash
kubectl get events -n llama-demo --field-selector reason=TrafficReconcileError -o custom-columns='OBJECT:.involvedObject.name,MESSAGE:.message'
```

```output
OBJECT       MESSAGE
llama-chat   backend policy with the OME-managed name exists but is not owned by this InferenceService: llama-demo/llama-chat already exists with no controller owner reference (hand-authored)
```

Here, a policy that someone wrote by hand already has the name `llama-chat`. Delete it, or re-create it under another name, and OME writes its policy when it retries.

## Next steps

- [Ingress and external access](ingress.md): the routes and the Service that the policy applies to.
- [Configure ingress](../../guides/networking/configure-ingress.md): turn on Gateway API ingress with Envoy Gateway.
- [Traffic annotations](../../reference/api/traffic-annotations.md): circuit breaking, retries, timeouts and pass-through fields.
- [kubectl ome traffic](../../reference/kubectl-ome/traffic.md): show the traffic status that OME reports for an InferenceService, and compare it with what you declared.
