---
title: "Autoscaler Policy"
linkTitle: "Autoscaler Policy"
weight: 32
description: >
  Define a reusable KEDA or HPA autoscaler template once and attach it to InferenceService components by reference.
---

An **AutoscalerPolicy** is a reusable, parameterized autoscaler template. Instead of copying the same inline `autoscaler` block into every InferenceService, a platform team writes the KEDA or HPA configuration once as a namespaced `AutoscalerPolicy` object, and each InferenceService component opts in with a one-line reference:

```yaml
spec:
  engine:
    autoscalerPolicyRef:
      name: token-throughput
```

At reconcile time the controller renders the referenced template into a concrete per-component autoscaler block — indistinguishable in shape from an inline block — and feeds it to the existing autoscaler dispatch. Creating a policy actuates nothing by itself: the per-component ref is the only attachment mechanism.

> **Alpha:** The AutoscalerPolicy API is alpha and may change without notice. The feature is off by default and must be enabled at install time (see below).

## Enabling the feature

The CRD, its validating webhook, the status controller RBAC, and the operator config block are gated together behind a single Helm value, set on **both** charts:

```bash
helm upgrade --install ome-crd charts/ome-crd --set ome.autoscalerPolicy.enabled=true
helm upgrade --install ome charts/ome-resources --set ome.autoscalerPolicy.enabled=true
```

The controller manager probes for the `AutoscalerPolicy` CRD at startup; on a cluster where the feature is not installed, the InferenceService admission webhook rejects any `autoscalerPolicyRef` outright rather than letting it silently no-op.

## Defining a policy

A policy carries exactly one template, selected by `spec.class` — either `KEDA` (a parameterized ScaledObject template) or `HPA` (a verbatim HorizontalPodAutoscaler configuration). `External` and `None` are inline-only classes: "someone else scales this component" is a per-service statement, not a reusable behavior. A policy that needs different behavior for different components is two policies.

### KEDA template

```yaml
apiVersion: ome.io/v1beta1
kind: AutoscalerPolicy
metadata:
  name: token-throughput
  namespace: my-team
spec:
  class: KEDA
  keda:
    triggers:
      - type: prometheus
        providerRef:
          name: cluster-prometheus     # bound to an endpoint by cluster config
        metricType: AverageValue
        metadata:
          query: 'sum(rate(request_success_total{namespace="{{ .Namespace }}",isvc="{{ .ISVCName }}"}[2m]))'
          threshold: "20"
          ignoreNullValues: "false"    # must be explicit on prometheus triggers
    cooldownPeriod: 120
    fallback:
      failureThreshold: 3
      replicas:
        fromComponent: MaxReplicas     # fail toward the component's own ceiling
```

Trigger metadata values are string templates over a **closed variable set** — plain `{{ .Var }}` field access and literal text only. Functions, pipelines, variables, and control flow are rejected at admission, and an unknown variable fails the render rather than interpolating an empty string.

| Variable | Value |
|----------|-------|
| `{{ .Namespace }}` | The consuming InferenceService's namespace |
| `{{ .ISVCName }}` | The consuming InferenceService's name |
| `{{ .Component }}` | The consuming component: `engine`, `decoder`, or `router` |
| `{{ .MinReplicas }}` | The component's effective `minReplicas` after defaulting |
| `{{ .MaxReplicas }}` | The component's effective `maxReplicas` after defaulting |
| `{{ .TargetName }}` | The scale-target name, `<isvc>-<component>` |

Replica bounds themselves stay on each consuming component (`minReplicas` / `maxReplicas` in the component spec); the policy renders against them. The typed `fallback.replicas` source sets exactly one of `value` (a fixed count) or `fromComponent` (`MinReplicas` or `MaxReplicas`), so the fallback count is derived per consumer instead of hard-coded fleet-wide.

### HPA template

```yaml
apiVersion: ome.io/v1beta1
kind: AutoscalerPolicy
metadata:
  name: cpu-60
  namespace: my-team
spec:
  class: HPA
  hpa:
    metrics:
      - type: Resource
        resource:
          name: cpu
          target:
            type: Utilization
            averageUtilization: 60
```

The `hpa` block is passed through verbatim (no templating). It is optional: `class: HPA` with no `hpa` block renders the same default the controller applies to a policy-less component — a single CPU=80% utilization metric.

### What admission rejects

The validating webhook runs the full spec validation on every CREATE/UPDATE and joins all findings into a single denial, so an invalid policy never lands where a consumer could reference it:

- `class: KEDA` without a `keda` template, or a `keda`/`hpa` block that contradicts the class.
- `enforcement: Required` — a reserved shape; only `Default` (consumers opt in per component, inline blocks outrank the policy) is implemented.
- Forbidden template constructs and unknown template variables.
- The trigger metadata keys `serverAddress` and `authModes` — endpoints and auth are provider-owned (see below), so a policy author can never point a scaler at an arbitrary address.
- A `prometheus` trigger without a `providerRef`, or without an explicit `ignoreNullValues` (the KEDA default of `true` silently treats "no series" as a healthy zero).
- `queryReturnsDesiredReplicas: true` with a `metricType` other than `AverageValue`.
- A sample render failure, a rendered prometheus query that does not parse as PromQL, or the known precedence trap `sum(x) > bool 0 * N` (which parses as `sum(x) > bool (0*N)`).

## Metric providers

A KEDA trigger's `providerRef` names a **logical provider**, never an endpoint. The name is bound to a cluster-local address (and optional credentials) by cluster configuration — the `ome.metricProviders` Helm value, rendered into the `inferenceservice-config` ConfigMap:

```yaml
# charts/ome-resources values
ome:
  metricProviders:
    cluster-prometheus:
      serverAddress: "http://ome-prometheus.ome.svc:9090"
      # authSecretRef:          # optional bearer token
      #   name: prometheus-bearer
      #   key: token
```

The rendered trigger gets the bound `serverAddress` injected; when `authSecretRef` is set, the controller materializes a KEDA `TriggerAuthentication` named `ome-metric-provider-<provider>` in the consumer namespace and wires it as the trigger's `authenticationRef` (OME never reads the secret — KEDA resolves it at scrape time). One policy object therefore works on every cluster: the same template renders against each cluster's own bindings. An unbound provider name is a render error, which fails closed (see below) — there is no built-in default endpoint.

## Attaching a policy to an InferenceService

Each component (`engine`, `decoder`, `router`) attaches individually via `autoscalerPolicyRef`, which names an `AutoscalerPolicy` in the InferenceService's own namespace:

```yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: my-team
spec:
  model:
    name: llama-3-70b-instruct
  runtime:
    name: srt-llama-3
  engine:
    minReplicas: 2
    maxReplicas: 16
    autoscalerPolicyRef:
      name: token-throughput
```

`kind` defaults to `AutoscalerPolicy` and is the only accepted value — `ClusterAutoscalerPolicy` is a reserved shape rejected at admission until a cluster-scoped twin ships.

> **Note:** MultiNode components have no autoscaler dispatch, so a policy ref on a MultiNode component is inert; the component's `AutoscalerResolved` condition reports `False` with reason `UnsupportedDeploymentMode`.

## Precedence: inline wins

The per-component autoscaler resolves through a fixed chain; the first layer that provides a block wins:

1. **Inline** — `spec.<component>.autoscaler` on the InferenceService.
2. **Policy** — the rendered `autoscalerPolicyRef`.
3. **Runtime** — the resolved ServingRuntime's component-level `autoscaler` block.
4. **Legacy** — on RawDeployment only, the deprecated `ome.io/autoscalerClass` annotation (a policy ref outranks it).
5. **Default** — an HPA with a single CPU=80% utilization metric.

An inline block and a policy ref **may coexist, and the inline block always wins** — including when the policy machinery is broken, so the escape hatch works precisely when it is needed. This coexistence is the documented preview and rollback mechanism:

- **Preview:** add the ref while keeping the inline block. The component keeps running on the inline configuration, its `AutoscalerResolved` condition reports `True` / `InlinePrecedence`, and `status.components.<component>.autoscaler.shadowedPolicyRef.wouldRenderDigest` shows what the policy would produce if the inline block were removed.
- **Adopt:** remove the inline block; the policy render takes over (`RenderedFromPolicy`).
- **Rollback:** restore the inline block — an atomic, policy-free rollback that needs no policy edit.

## Fail-closed behavior

When a component carries a ref but no block can be produced — the policy was deleted out from under it, its content is invalid, its provider is unbound on this cluster, or it renders KEDA on a cluster without the KEDA CRDs — the component **holds**:

- The last-known-good scaler keeps running unchanged; trigger content is frozen.
- The component's `AutoscalerResolved` condition goes `False` with a named reason and the message `holding last-known-good scaler: ...`.
- Resolution never falls through to the runtime or default layer, which would silently swap the scaler class (for a GPU fleet held at max by a fail-to-max policy, a default CPU HPA would be a scale-to-min during a policy outage).
- The reconcile itself keeps succeeding — a hold is degraded state, not an error, so the service's other concerns (image updates, replica edits) are unaffected.

| `AutoscalerResolved` reason | Meaning |
|------------------------------|---------|
| `RenderedFromPolicy` | `True` — the policy rendered this component's live scaler. |
| `InlinePrecedence` | `True` — an inline block outranks the ref; the shadow preview lives in `shadowedPolicyRef`. |
| `PolicyNotFound` | The named policy does not exist (or the feature is not enabled on this cluster). |
| `PolicyInvalid` | The policy's content failed validation or rendering. |
| `AuthNotFound` | The provider's auth material could not be resolved. |
| `ClassUnavailable` | The policy renders class KEDA but the KEDA CRDs are not installed. |
| `UnsupportedDeploymentMode` | MultiNode components have no autoscaler dispatch; the ref is inert. |

Recovery is watch-driven: fixing the policy (or the provider binding) un-holds every consumer on their next reconcile.

## Conditions and status

**On the policy** (`status`):

| Field / condition | Meaning |
|-------------------|---------|
| `Ready` condition | `True` when every template parses, passes the structural allowlist, sample-renders, and names a resolvable provider shape. Failure reasons: `ParseError`, `ForbiddenTemplateNode`, `ForbiddenMetadataKey`, `ProviderUnknown`, `PromQLInvalid`. |
| `InUse` condition | `True` while at least one component references the policy (`Attached` / `NoConsumers`). |
| `attachedComponents` | Count of components in the namespace currently referencing the policy. |
| `portableDigest` | Canonical digest of the defaulted spec; equal across clusters iff the specs are semantically identical. |

Policy status is bounded by design — counts and digests, never consumer name lists. Per-consumer truth lives on each InferenceService.

**On the consuming InferenceService** (`status.components.<component>.autoscaler`, written only for components that carry a ref):

| Field | Meaning |
|-------|---------|
| `specSource` | Which layer won: `isvc`, `policy`, `runtime`, `legacy`, or `default`. |
| `policy` | Provenance of the live rendered block: policy `name`, `observedGeneration`, `portableDigest`, and `resolvedDigest` (a digest of the rendered block plus effective bounds and bound endpoint — "did my policy edit land here?"). During a hold, the last successful provenance stays visible so you can see which render is standing. |
| `shadowedPolicyRef` | Set when an inline block outranks the ref; `wouldRenderDigest` previews the policy's render. |
| `AutoscalerResolved` condition | Per-component resolution state, reasons as in the table above. |

## Deleting a policy

The webhook denies deletion while any InferenceService component in the namespace still references the policy, naming the referencing components in the error. To proceed, either remove the refs first, or force it with a break-glass annotation set by a prior, reviewable update:

```bash
kubectl annotate autoscalerpolicy token-throughput ome.io/allow-in-use-delete=true
kubectl delete autoscalerpolicy token-throughput
```

If a referenced policy does disappear anyway (break-glass, or a webhook outage window), consumers fail closed as described above rather than losing their scalers. Deletion is always allowed in a terminating namespace so teardown cannot wedge.

## Observing policies

`kubectl get` prints class, readiness, and attachment from the status the controller maintains:

```bash
$ kubectl get autoscalerpolicies
NAME               CLASS   READY   ATTACHED   AGE
token-throughput   KEDA    True    3          2d
cpu-60             HPA     True    0          2d
```

The [OME CLI](/ome/docs/tasks/kubectl-ome) supports the same resource — `kubectl ome get autoscalerpolicies` (aliases `autoscalerpolicy`, `ap`); `-o wide` adds the portable digest, the `Ready` reason, the `InUse` state, and status freshness.

## Reference

- API fields: [`AutoscalerPolicy` and `ComponentExtensionSpec`](/ome/docs/reference/ome.v1beta1)
- Related concept: [Inference Service](/ome/docs/concepts/inference_service)
