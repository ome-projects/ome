---
title: Autoscaler policy
description: An AutoscalerPolicy is an alpha, reusable set of KEDA or HPA autoscaling settings that InferenceService components share by name.
since: v1.3
---

An AutoscalerPolicy lets the InferenceServices in a namespace share one set of autoscaling settings: [KEDA](https://keda.sh) triggers, or HorizontalPodAutoscaler metrics and behavior. An [InferenceService](inference-services.md) component names the policy in `autoscalerPolicyRef`, and OME builds its autoscaler from the policy, filling in the component's names and replica bounds. Edit the policy, and OME updates every component that uses it. AutoscalerPolicy is alpha and off by default: [Turn on the feature](#turn-on-the-feature) shows how to turn it on.

Use a policy when several InferenceServices should scale the same way. A runtime can also carry autoscaling settings for the InferenceServices that use it, and a policy outranks them.

The examples use the `llama-demo` namespace and the `llama-3-2-1b-instruct` InferenceService from [A minimal InferenceService](inference-services.md#a-minimal-inferenceservice), and assume OME runs in the `ome` namespace.

## Turn on the feature

Set `ome.autoscalerPolicy.enabled: true` in both the `ome-crd` and `ome-resources` charts, version 1.3.0 or later. A `prometheus` trigger also needs a [metric provider](#metric-providers), and the chart binds none by default. This values file turns the feature on, and binds the examples' provider, `cluster-prometheus`, to the bundled Prometheus:

```yaml title="values.yaml"
ome:
  autoscalerPolicy:
    enabled: true
  metricProviders:
    cluster-prometheus:
      serverAddress: "http://ome-prometheus.ome.svc:9090"
```

Add these values to the values file you install both charts with. Upgrade `ome-crd` first, then `ome-resources`, at the chart version you run:

```bash
helm upgrade --install ome-crd oci://ghcr.io/moirai-internal/charts/ome-crd \
  --namespace ome --version 1.3.0 -f values.yaml
helm upgrade --install ome oci://ghcr.io/moirai-internal/charts/ome-resources \
  --namespace ome --version 1.3.0 -f values.yaml
```

Helm reports that each release has been upgraded.

!!! danger "Turning the feature off deletes every policy"
    Keep `enabled: true` in the values file: an `ome-crd` upgrade without it deletes the CRD and every AutoscalerPolicy with it, as [Configure the controller](../../guides/operate-ome/configure-the-controller.md#turn-on-rolloutpolicy-and-autoscalerpolicy) warns.

The `ome-resources` upgrade restarts the controller, which looks for the CRD and for KEDA only when it starts. A `KEDA` policy needs KEDA in the cluster: if you install KEDA later, or install the AutoscalerPolicy CRD without the chart, restart the controller yourself:

```bash
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

Check that the CRD and the webhook are installed:

```bash
kubectl get crd autoscalerpolicies.ome.io -o name
kubectl get validatingwebhookconfiguration autoscalerpolicy.ome.io -o name
```

```output
customresourcedefinition.apiextensions.k8s.io/autoscalerpolicies.ome.io
validatingwebhookconfiguration.admissionregistration.k8s.io/autoscalerpolicy.ome.io
```

## Define a policy

This policy scales a component on the number of requests waiting in vLLM's queue, which the [bundled Prometheus](../../guides/operate-ome/metrics.md#what-it-scrapes) collects and labels with each pod's InferenceService and component:

```yaml title="queue-depth.yaml"
apiVersion: ome.io/v1beta1
kind: AutoscalerPolicy
metadata:
  name: queue-depth
  namespace: llama-demo
spec:
  class: KEDA
  keda:
    triggers:
      - type: prometheus
        providerRef:
          name: cluster-prometheus
        metricType: AverageValue
        metadata:
          query: 'sum(vllm:num_requests_waiting{namespace="{{ .Namespace }}",inferenceservice="{{ .ISVCName }}",component="{{ .Component }}"})'
          threshold: "5"
          ignoreNullValues: "false"
    fallback:
      failureThreshold: 3
      replicas:
        fromComponent: MaxReplicas
```

Apply the file:

```bash
kubectl apply -f queue-depth.yaml
```

```output
autoscalerpolicy.ome.io/queue-depth created
```

For each component that references it, OME renders this policy into a KEDA ScaledObject:

- The query sums the waiting requests across the component's pods. With `metricType: AverageValue`, KEDA divides the sum by the replica count, so it adds replicas to keep about 5 waiting requests per replica.
- `ignoreNullValues: "false"` makes an empty query result an error, instead of a zero.
- When the query fails more than 3 times in a row, KEDA scales the component to its `maxReplicas`.

The values in a trigger's `metadata` can use these variables, which OME fills in for each component:

| Variable | Value |
| --- | --- |
| `{{ .Namespace }}` | The InferenceService's namespace. |
| `{{ .ISVCName }}` | The InferenceService's name. |
| `{{ .Component }}` | The component: `engine`, `decoder` or `router`. |
| `{{ .MinReplicas }}` | The component's minimum replica count. |
| `{{ .MaxReplicas }}` | The component's maximum replica count. |
| `{{ .TargetName }}` | `<isvc>-<component>`, such as `llama-3-2-1b-instruct-engine`. |

A value holds literal text and these variables only: no template functions, pipelines or conditionals. The other fields:

| Field | What it sets |
| --- | --- |
| `class` | `KEDA`, which needs `keda.triggers`, or `HPA`. |
| `providerRef` | The [metric provider](#metric-providers) of a `prometheus` trigger. Required, as is an explicit `ignoreNullValues`. OME sets the trigger's `serverAddress` and `authModes` from the provider, so a trigger can't set them. |
| `keda.fallback.replicas` | A fixed `value`, or `fromComponent: MaxReplicas` or `MinReplicas`. |
| `keda.pollingInterval`, `cooldownPeriod`, `idleReplicaCount`, `advanced` | Copied to the ScaledObject as they are. |
| `hpa` | The HorizontalPodAutoscaler's `metrics` and `behavior`, copied as they are. Without it, the HorizontalPodAutoscaler keeps CPU use at 80%. |

OME rejects a policy that breaks these rules when you create or update it, including an invalid PromQL query. [AutoscalerPolicySpec](../../reference/api/ome.v1beta1.md#ome-io-v1beta1-AutoscalerPolicySpec) in the API reference lists every field.

This `HPA` policy keeps CPU use at 60%, and when it scales down, it keeps the highest replica count recommended in the last 5 minutes:

```yaml title="cpu-60.yaml"
apiVersion: ome.io/v1beta1
kind: AutoscalerPolicy
metadata:
  name: cpu-60
  namespace: llama-demo
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
    behavior:
      scaleDown:
        stabilizationWindowSeconds: 300
```

```bash
kubectl apply -f cpu-60.yaml
```

```output
autoscalerpolicy.ome.io/cpu-60 created
```

## Attach a policy to an InferenceService

Name the policy in a component's `autoscalerPolicyRef`. This version of `isvc.yaml` attaches `queue-depth` to the engine, and sets the engine's replica bounds:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-3-2-1b-instruct
  namespace: llama-demo
spec:
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 1
    maxReplicas: 4
    autoscalerPolicyRef:
      name: queue-depth
```

Apply the file:

```bash
kubectl apply -f isvc.yaml
```

```output
Warning: Runtime vllm-llama-3-2-1b-instruct will be auto-selected for model llama-3-2-1b-instruct
inferenceservice.ome.io/llama-3-2-1b-instruct configured
```

The engine still has OME's default HorizontalPodAutoscaler, and a default KEDA install rejects a ScaledObject for a workload that another HorizontalPodAutoscaler scales. Until you delete OME's, the InferenceService's ingress and status stop updating. Delete it, as [Switch from HPA to KEDA](component-autoscaling.md#switch-from-hpa-to-keda) explains:

```bash
kubectl delete hpa llama-3-2-1b-instruct-engine -n llama-demo
```

```output
horizontalpodautoscaler.autoscaling "llama-3-2-1b-instruct-engine" deleted from llama-demo namespace
```

OME then creates the ScaledObject `scaledobject-llama-3-2-1b-instruct-engine`, and KEDA creates its own HorizontalPodAutoscaler for it. Read the rendered server address, query and fallback replica count:

```bash
kubectl get scaledobject scaledobject-llama-3-2-1b-instruct-engine -n llama-demo \
  -o jsonpath='{.spec.triggers[0].metadata.serverAddress}{"\n"}{.spec.triggers[0].metadata.query}{"\n"}{.spec.fallback.replicas}{"\n"}'
```

```output
http://ome-prometheus.ome.svc:9090
sum(vllm:num_requests_waiting{namespace="llama-demo",inferenceservice="llama-3-2-1b-instruct",component="engine"})
4
```

The InferenceService reports where the engine's autoscaler came from:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-demo \
  -o custom-columns='SOURCE:.status.components.engine.autoscaler.specSource,POLICY:.status.components.engine.autoscaler.policy.name,RESOLVED:.status.components.engine.autoscaler.conditions[?(@.type=="AutoscalerResolved")].reason'
```

```output
SOURCE   POLICY        RESOLVED
policy   queue-depth   RenderedFromPolicy
```

The `engine`, `decoder` and `router` can each name a policy, the same one or different ones. The webhooks reject:

- any reference until the controller has found the CRD at startup, with a message that starts with `AutoscalerPolicyFeatureDisabled`;
- `autoscalerPolicyRef` in a runtime, because references go only in an InferenceService;
- `minReplicas: 0` with only a reference: [scale to zero](../../guides/scale-and-migrate/scale-to-zero-with-keda.md) needs the component's own `autoscaler` with `class: KEDA`.

## Inline settings win

A component's inline `autoscaler` outranks its `autoscalerPolicyRef`, as [Which setting wins](component-autoscaling.md#which-setting-wins) lists. Set an inline `autoscaler` beside the reference to try other settings, to roll back, or to end a [hold](#fail-closed-behavior), without removing the reference. This engine keeps its reference but scales on CPU use:

```yaml
spec:
  engine:
    minReplicas: 1
    maxReplicas: 4
    autoscalerPolicyRef:
      name: queue-depth
    autoscaler:
      class: HPA
      hpa:
        metrics:
          - type: Resource
            resource:
              name: cpu
              target:
                type: Utilization
                averageUtilization: 70
```

OME replaces the ScaledObject with a HorizontalPodAutoscaler, and reports the reference as shadowed:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-demo \
  -o custom-columns='SOURCE:.status.components.engine.autoscaler.specSource,SHADOWED:.status.components.engine.autoscaler.shadowedPolicyRef.name,RESOLVED:.status.components.engine.autoscaler.conditions[?(@.type=="AutoscalerResolved")].reason'
```

```output
SOURCE   SHADOWED      RESOLVED
isvc     queue-depth   InlinePrecedence
```

To go back to the policy, remove the `autoscaler` block, then delete OME's HorizontalPodAutoscaler again, as in [Attach a policy to an InferenceService](#attach-a-policy-to-an-inferenceservice).

## Fail-closed behavior

When OME can't use a component's policy, the component holds: it keeps its last autoscaler, and never falls back to the runtime's settings or the default. OME holds in these cases:

| Reason | When |
| --- | --- |
| `PolicyNotFound` | The policy is missing from the InferenceService's namespace, or the controller started before the AutoscalerPolicy CRD was installed. |
| `PolicyInvalid` | The policy fails to render for the component, for example because it names an unbound provider. |
| `ClassUnavailable` | The policy's class is `KEDA`, and KEDA's CRDs were missing when the controller started. |

While a component holds, OME keeps creating and updating its workload, and only the autoscaler is frozen. The component keeps the autoscaler that its policy last rendered; on a RawDeployment component, changes to `minReplicas` and `maxReplicas` still reach it. If the policy never rendered for the component, OME leaves its current autoscaler, if any, as it is.

The component's `AutoscalerResolved` condition is `False` with the hold's reason, and its message starts with `holding last-known-good scaler:`. When a hold starts, OME also records an `AutoscalerPolicyHold` Warning event on the InferenceService, such as `engine autoscaler is holding last-known-good (PolicyNotFound): AutoscalerPolicy "queue-depth" not found in namespace llama-demo`.

A held component recovers once you create or fix the policy, or bind its provider. If the CRD or KEDA was missing when the controller started, restart the controller after you install it.

## Metric providers

A `prometheus` trigger names a metric provider in `providerRef` instead of a Prometheus address, so the same policy works in any cluster that binds the name. The cluster administrator binds provider names in the `ome-resources` value `ome.metricProviders`, as [Turn on the feature](#turn-on-the-feature) does, and canary analysis uses the same [bindings](../../reference/rollouts/canary-analysis.md#metric-provider-bindings).

If a binding has an `authSecretRef`, create its Secret in each namespace that uses the provider. Autoscaling triggers don't send the binding's `headers`. OME picks up a change to the bindings on its own.

!!! warning "Every binding needs a serverAddress"
    While the feature is on, one binding without `serverAddress` stops OME from creating or updating the workload of any InferenceService, including the ones that don't use the provider. [InferenceServices stop updating](../../guides/operate-ome/metrics.md#inferenceservices-stop-updating) shows how to find the binding.

## Conditions and status

A policy's status says whether it's valid and how many components use it:

| Field | Description |
| --- | --- |
| `conditions`, type `Ready` | `True` with the reason `TemplatesValid` when the policy is valid and its providers are bound. Otherwise `False`, with the reason `ProviderUnknown` or that of the first validation problem. |
| `conditions`, type `InUse` | `True` with the reason `Attached` when a component references the policy, otherwise `False` with the reason `NoConsumers`. |
| `attachedComponents` | How many components reference the policy. An engine and a decoder count as two. |

List the policies with their `Ready` reason:

```bash
kubectl get autoscalerpolicies -n llama-demo \
  -o custom-columns='NAME:.metadata.name,CLASS:.spec.class,READY:.status.conditions[?(@.type=="Ready")].status,REASON:.status.conditions[?(@.type=="Ready")].reason,ATTACHED:.status.attachedComponents'
```

```output
NAME          CLASS   READY   REASON           ATTACHED
cpu-60        HPA     True    TemplatesValid   <none>
queue-depth   KEDA    True    TemplatesValid   1
```

ATTACHED is `<none>` while the policy is unused. [`kubectl ome get autoscalerpolicies`](../../reference/kubectl-ome/get.md#autoscalerpolicies-columns) shows the same status.

On the InferenceService, `status.components.<component>.autoscaler.policy` names the policy that rendered the component's autoscaler, and `shadowedPolicyRef` names a reference that an inline `autoscaler` outranks. [Read the result in status](component-autoscaling.md#read-the-result-in-status) describes the other fields.

OME sets `AutoscalerResolved` only on components with a reference:

| Status | Reason | When |
| --- | --- | --- |
| `True` | `RenderedFromPolicy` | The policy rendered the component's autoscaler. |
| `True` | `InlinePrecedence` | An inline `autoscaler` outranks the reference. |
| `False` | `PolicyNotFound`, `PolicyInvalid` or `ClassUnavailable` | The component holds; see [Fail-closed behavior](#fail-closed-behavior). |
| `False` | `UnsupportedDeploymentMode` | The component is [`MultiNode`](../architecture/deployment-modes.md) (deprecated), which gets no autoscaler. |

## Delete a policy

The webhook refuses to delete a policy that a component references. With the engine using `queue-depth`, as in [Attach a policy to an InferenceService](#attach-a-policy-to-an-inferenceservice):

```bash
kubectl delete autoscalerpolicy queue-depth -n llama-demo
```

```output
Error from server (Forbidden): admission webhook "autoscalerpolicy.ome-webhook-server.validator" denied the request: AutoscalerPolicy "queue-depth" is referenced by 1 component(s): llama-3-2-1b-instruct/engine; remove the refs first, or set the ome.io/allow-in-use-delete="true" annotation to force deletion
```

Remove the references first. A component whose reference you remove takes its settings from the next source in [Which setting wins](component-autoscaling.md#which-setting-wins).

To delete a policy that components still reference, annotate it first:

```bash
kubectl annotate autoscalerpolicy queue-depth -n llama-demo ome.io/allow-in-use-delete=true
kubectl delete autoscalerpolicy queue-depth -n llama-demo
```

```output
autoscalerpolicy.ome.io/queue-depth annotated
autoscalerpolicy.ome.io "queue-depth" deleted from llama-demo namespace
```

The components that referenced it then hold with the reason `PolicyNotFound`, and keep their last autoscaler:

```bash
kubectl get inferenceservice llama-3-2-1b-instruct -n llama-demo \
  -o custom-columns='SOURCE:.status.components.engine.autoscaler.specSource,POLICY:.status.components.engine.autoscaler.policy.name,RESOLVED:.status.components.engine.autoscaler.conditions[?(@.type=="AutoscalerResolved")].reason'
```

```output
SOURCE   POLICY        RESOLVED
policy   queue-depth   PolicyNotFound
```

## Next steps

- [Component autoscaling](component-autoscaling.md): the `autoscaler` block that a policy renders into, and which setting wins.
- [InferenceService](inference-services.md): the components that reference a policy.
- [kubectl ome autoscale](../../reference/kubectl-ome/autoscale.md): show the autoscaling in effect for each component, and which source supplied it.
