---
title: Rollout policy
description: A RolloutPolicy is an alpha, reusable canary, blue-green or rolling-update progression that InferenceService rollout groups attach by reference.
since: v1.3
---

A RolloutPolicy lets the [InferenceServices](../serving/inference-services.md) in a namespace share one rollout progression: a canary with its steps and metric checks, a blue-green rollout or a rolling update. A [rollout group](rollout-groups.md) names the policy in `policyRef` instead of setting the progression inline. Each rollout follows the policy as it was when the rollout started. Use a policy when several InferenceServices should roll out the same way, such as behind the same latency check. RolloutPolicy is alpha, and off until you [turn on the feature](#turn-on-the-feature).

The examples use the `llama-demo` namespace, serve the pre-configured model `llama-3-2-1b-instruct` as [A minimal InferenceService](../serving/inference-services.md#a-minimal-inferenceservice) does, and assume OME runs in the `ome` namespace.

## Turn on the feature

Set `ome.rolloutPolicy.enabled: true` in both the `ome-crd` and `ome-resources` charts, version 1.3.0 or later, to install the RolloutPolicy CRD and its validating webhook. A policy's metric checks also need a [metric provider](../../reference/rollouts/canary-analysis.md#metric-provider-bindings), and the chart binds none by default. This values file turns the feature on, and binds the examples' provider, `cluster-prometheus`, to the bundled Prometheus:

```yaml title="values.yaml"
ome:
  rolloutPolicy:
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

The controller looks for the CRD only when it starts. Until it restarts, the InferenceService webhook rejects any `policyRef` with `RolloutPolicyRefUnsupported`. Restart it after both upgrades:

```bash
kubectl rollout restart deployment ome-controller-manager -n ome
```

```output
deployment.apps/ome-controller-manager restarted
```

Check that the CRD and the webhook are installed:

```bash
kubectl get crd rolloutpolicies.ome.io -o name
kubectl get validatingwebhookconfiguration rolloutpolicy.ome.io -o name
```

```output
customresourcedefinition.apiextensions.k8s.io/rolloutpolicies.ome.io
validatingwebhookconfiguration.admissionregistration.k8s.io/rolloutpolicy.ome.io
```

!!! danger "Turning the feature off deletes every policy"
    Keep `enabled: true` in the values file: an `ome-crd` upgrade without it deletes the CRD and every RolloutPolicy with it. Remove every `policyRef` before you turn the feature off, or OME stops updating the serving workloads of the InferenceServices that still have one.

## Define a policy

A policy's `spec` holds one progression: `canary`, `blueGreen` or `rollingUpdate`. This canary checks the new revision's p95 time to first token before it finishes the rollout:

```yaml title="canary-strict.yaml"
apiVersion: ome.io/v1beta1
kind: RolloutPolicy
metadata:
  name: canary-strict
  namespace: llama-demo
spec:
  canary:
    prometheus:
      providerRef:
        name: cluster-prometheus
    steps:
      - capacity: "25%"
        traffic: 10
        pause:
          duration: 10m
        analysis:
          interval: 1m
          failureLimit: 2
          metrics:
            - name: ttft-p95
              query: >-
                histogram_quantile(0.95, sum by (le) (rate(vllm:time_to_first_token_seconds_bucket{namespace="{{.Namespace}}", inferenceservice="{{.ISVCName}}", revision_hash="{{.CanaryRevision}}"}[5m])))
              operator: LTE
              threshold: "1"
      - capacity: "100%"
        traffic: 100
```

Apply the file:

```bash
kubectl apply -f canary-strict.yaml
```

```output
rolloutpolicy.ome.io/canary-strict created
```

- Step 1 moves 25% of the replicas, rounded up, to the new revision: 1 of the 2 engine replicas in the next section's example.
- Once that replica is ready, OME samples the query at most once per `interval`, here 1 minute. A sample above the 1-second `threshold` fails, and the second failure rolls the component back to the previous revision. The first passing sample after the 10-minute `pause` moves the rollout to step 2.
- Step 2 moves every replica to the new revision, and the rollout completes.
- `providerRef` names a metric provider instead of a Prometheus address, so the policy works in any cluster that binds the name.
- The bundled Prometheus labels each series with the pod's revision, so `revision_hash="{{.CanaryRevision}}"` selects the new revision's pods. [Query template variables](../../reference/rollouts/canary-analysis.md#query-template-variables) lists the other variables.

A policy's query must suit every InferenceService that uses it. This one reads vLLM's time-to-first-token histogram, so it suits services on vLLM runtimes.

!!! warning "Requests follow capacity, not the traffic weight"
    OME records a step's `traffic` weight in status, but the new revision gets requests in proportion to its ready replicas, which the step's `capacity` sets. During step 1 of the next section's example, that's about half the requests, not 10%.

This policy holds a rolling update instead, with up to 25% extra replicas and no unavailable ones during the rollout:

```yaml title="gentle-roll.yaml"
apiVersion: ome.io/v1beta1
kind: RolloutPolicy
metadata:
  name: gentle-roll
  namespace: llama-demo
spec:
  rollingUpdate:
    maxSurge: "25%"
    maxUnavailable: 0
```

```bash
kubectl apply -f gentle-roll.yaml
```

```output
rolloutpolicy.ome.io/gentle-roll created
```

A blue-green policy sets `blueGreen: {}`, which has no fields.

### Rules that keep a policy portable {#what-admission-rejects}

The policy webhook checks a progression with the same rules as an inline one, which [Canary progression](../../reference/rollouts/canary-progression.md#what-admission-rejects), [Canary metric analysis](../../reference/rollouts/canary-analysis.md#what-admission-checks) and [Rollout groups](rollout-groups.md#what-admission-rejects) list. A policy has to work in any InferenceService and any cluster, so the webhook also rejects:

- a step `capacity` given as a count, such as `3`, instead of a percentage;
- `prometheus.serverAddress` or `prometheus.authRef`;
- a step with `analysis` in a canary that has no `providerRef`.

## Attach a policy to a rollout group

Name the policy in a rollout group's `policyRef`. This InferenceService rolls its engine out with `canary-strict`:

```yaml title="isvc.yaml"
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama-chat
  namespace: llama-demo
spec:
  deploymentMode: OMENative
  model:
    name: llama-3-2-1b-instruct
  engine:
    minReplicas: 2
    maxReplicas: 2
  rollout:
    groups:
      - components: [engine]
        policyRef:
          name: canary-strict
          progression: canary
```

```bash
kubectl apply -f isvc.yaml
```

```output
Warning: Runtime vllm-llama-3-2-1b-instruct will be auto-selected for model llama-3-2-1b-instruct
inferenceservice.ome.io/llama-chat created
```

- `policyRef.name` names a RolloutPolicy in the InferenceService's namespace.
- `policyRef.progression` is required, and must match the policy's kind, or the rollout [parks](#fail-closed-parking) with `ProgressionMismatch`.
- Every component in a rollout group must use the `OMENative` [deployment mode](../serving/inference-services.md#how-ome-picks-the-deployment-mode), which the example sets. Without it, the webhook rejects a canary group with `CanaryRequiresOMENative`, and any other group with `CoordinationRequiresOMENative`.
- You can create the InferenceService before the policy. A rollout that starts before the policy exists parks until you create it.

Check which progression the group would use if a rollout started now:

```bash
kubectl get inferenceservice llama-chat -n llama-demo -o custom-columns='SOURCE:.status.rollout.groups[0].source,POLICY:.status.rollout.groups[0].policyRef.name'
```

```output
SOURCE   POLICY
Policy   canary-strict
```

The next change that gives the engine a new revision, such as a new image, rolls out with the policy's canary. [`kubectl ome rollout status`](../../reference/kubectl-ome/rollout.md#status) shows its current step.

## When edits take effect {#when-the-reference-is-resolved}

A rollout starts when a change gives a component in a group a new revision. OME then pins the plan into the InferenceService's `status.rollout.activeRun`. The rollout keeps that plan until it completes or rolls back. OME pins inline progressions the same way, even with the feature off. Edits to the policy, or to the group in `spec.rollout`, apply to the next rollout. When you edit a policy in use, the webhook warns you of this.

While a rollout runs on an older plan, the InferenceService's `RolloutPlanDrift` condition is `True`, with the reason `PolicyNewerThanRun`, or `SpecNewerThanRun` after an edit to the group. To apply the edit now and keep the rollout's progress, [repin the plan](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md) with the alpha `kubectl ome rollout repin`.

If another change gives a component a newer revision during a rollout, OME closes the rollout as `Superseded` and starts a new one with the current policy. Run [`kubectl ome rollout explain`](../../reference/kubectl-ome/rollout.md#explain-output-fields) to see the policy and generation that a rollout follows.

You can't change the kind of a policy in use, such as from `canary` to `rollingUpdate`. Create a new policy, and move the references to it.

## Inline settings win

A group that sets its own `canary`, `blueGreen` or `rollingUpdate` as well as a `policyRef` runs its own progression instead of the policy's. Its `source` in `status.rollout.groups` then shows `Inline`. Use this to try another progression, or to let a parked rollout go ahead, without editing a policy that other services share. Remove the inline progression to go back to the policy.

## When a rollout parks {#fail-closed-parking}

If OME can't resolve a group's plan when a rollout would start, it parks the rollout: the new revision waits, and the previous revision keeps serving. It never falls back to the blue-green default, which would skip the policy's checks.

| Reason | Cause |
| --- | --- |
| `PolicyNotFound` | The referenced RolloutPolicy is missing from the namespace. |
| `PolicyNotReady` | The policy fails validation, for example because it was created while its webhook wasn't running. |
| `ProgressionMismatch` | The reference's `progression` doesn't match the policy, for example `canary` for a `rollingUpdate` policy. |
| `ProviderUnbound` | A canary's `providerRef`, inline or from a policy, names a provider that isn't bound in `ome.metricProviders`. |
| `PlanInvalid` | The progression that the group would run fails validation. |

The InferenceService's `RolloutPlanReady` condition is then `False` with the reason, and OME records a `RolloutPlanParked` warning event. Fix the cause, or [give the group an inline progression](#inline-settings-win), and OME starts the rollout on its own.

## Conditions and status

A RolloutPolicy reports two conditions:

| Condition | Status and reason | Meaning |
| --- | --- | --- |
| `Ready` | `True`, `BodyValid` | The policy is valid, and any metric provider it names is bound. |
| `Ready` | `True`, `ProviderUnbound` | The policy is valid, but its metric provider isn't bound. A rollout that uses it parks. |
| `Ready` | `False`, `BodyInvalid` | The policy fails validation. |
| `InUse` | `True`, `Attached` | A rollout group references the policy. |
| `InUse` | `False`, `NoConsumers` | No rollout group references the policy. |

Check each policy's conditions, and how many rollout groups reference it:

```bash
kubectl get rolloutpolicies -n llama-demo -o custom-columns='NAME:.metadata.name,READY:.status.conditions[?(@.type=="Ready")].status,REASON:.status.conditions[?(@.type=="Ready")].reason,IN-USE:.status.conditions[?(@.type=="InUse")].status,REFS:.status.attachedGroups'
```

```output
NAME            READY   REASON      IN-USE   REFS
canary-strict   True    BodyValid   True     1
gentle-roll     True    BodyValid   False    <none>
```

[`kubectl ome get rolloutpolicies`](../../reference/kubectl-ome/get.md#rolloutpolicies-columns) shows the same status, with the progression.

The InferenceService reports two conditions for its rollout plan:

| Condition | Status and reason |
| --- | --- |
| `RolloutPlanReady` | `True` with `Pinned` while a rollout is in progress, and with `NoActiveRun` between rollouts. `False` while the rollout is parked, with a reason from [When a rollout parks](#fail-closed-parking). |
| `RolloutPlanDrift` | `True` with `PolicyNewerThanRun` or `SpecNewerThanRun` while the plan that a rollout would pin now differs from the pinned plan. `False` with `InSync` otherwise. |

With `-o wide`, `kubectl get inferenceservice` adds PLAN-SOURCE, the pinned source of the first group, and PLAN-DRIFT, the status of `RolloutPlanDrift`.

## Delete a policy

The policy webhook rejects deleting a policy that a rollout group references:

```bash
kubectl delete rolloutpolicy canary-strict -n llama-demo
```

```output
Error from server (Forbidden): admission webhook "rolloutpolicy.ome-webhook-server.validator" denied the request: RolloutPolicy "canary-strict" is referenced by 1 InferenceService(s): llama-chat; remove the refs first, or set the ome.io/allow-in-use-delete="true" annotation to force deletion
```

Remove the references first. To keep a group's canary instead of the blue-green default, copy the policy's body into the group as an inline progression.

To delete a policy that's still referenced, set the `ome.io/allow-in-use-delete` annotation first:

```bash
kubectl annotate rolloutpolicy canary-strict -n llama-demo ome.io/allow-in-use-delete=true
kubectl delete rolloutpolicy canary-strict -n llama-demo
```

```output
rolloutpolicy.ome.io/canary-strict annotated
rolloutpolicy.ome.io "canary-strict" deleted from llama-demo namespace
```

A rollout in progress keeps its pinned plan. The next rollout parks with `PolicyNotFound` until you recreate the policy or give the group an inline progression.

## Next steps

- [Rollout groups](rollout-groups.md): group components, and choose how each group rolls out.
- [Canary progression](../../reference/rollouts/canary-progression.md): every canary step field, and how a step advances.
- [Canary metric analysis](../../reference/rollouts/canary-analysis.md): how analysis samples metrics and decides.
- [Promote or roll back a canary](../../guides/roll-out-changes/promote-or-roll-back-a-canary.md): act on a canary at its gate.
- [Repin a drifted rollout plan](../../guides/roll-out-changes/repin-a-drifted-rollout-plan.md): apply a policy edit to the rollout in progress.
