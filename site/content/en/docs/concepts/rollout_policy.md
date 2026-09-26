---
title: "Rollout Policy"
linkTitle: "Rollout Policy"
weight: 33
description: >
  Define a reusable rollout progression once and attach it to InferenceService rollout groups by reference.
---

A **RolloutPolicy** is a reusable rollout progression. Instead of copying the same canary ladder into every InferenceService, a platform team writes the progression once as a namespaced `RolloutPolicy` object, and each rollout group opts in with a reference:

```yaml
spec:
  rollout:
    groups:
      - components: [engine]
        policyRef:
          name: canary-strict
          progression: canary
```

The policy carries **behavior only** — exactly one `canary`, `blueGreen`, or `rollingUpdate` body, reusing the same types an inline `spec.rollout.groups[]` progression uses. It never carries `components`, `order`, `soak`, `maintainRatio`, or `pairingProtocol`: those are consumer shape and state, which is what lets one policy serve every component topology in a fleet. Creating a policy actuates nothing by itself; the group-level ref is the only attachment mechanism.

> **Alpha:** The RolloutPolicy API is alpha and may change without notice. The feature is off by default and must be enabled at install time (see below).

## Enabling the feature

The CRD and its validating webhook are gated together behind a single Helm value, set on **both** charts:

```bash
helm upgrade --install ome-crd charts/ome-crd --set ome.rolloutPolicy.enabled=true
helm upgrade --install ome charts/ome-resources --set ome.rolloutPolicy.enabled=true
```

The controller manager probes for the `RolloutPolicy` CRD at startup. On a cluster where the feature is not installed, the InferenceService admission webhook rejects any `policyRef` outright (`RolloutPolicyRefUnsupported`) rather than letting it silently no-op; a ref that lands anyway (version skew, webhook outage) parks the rollout fail-closed at run open — see below.

The `ome.controller.rollout` operator config block is deliberately **not** gated, because it tunes inline rollout plans too:

```yaml
# charts/ome-resources values
ome:
  controller:
    rollout:
      maxPinnedPlanBytes: 16384   # cap on any pinned progression body; 0 = uncapped
      defaultReadyTimeout: "15m"
```

`maxPinnedPlanBytes` caps the JSON-rendered size of a progression body — enforced on policy bodies at policy admission and on inline bodies at InferenceService admission, both with reason `PlanTooLarge` — because the effective plan is pinned into each consumer's status at run open, so an oversized body would bloat every status write. The value is read once at manager startup; a change lands on the next restart.

## Defining a policy

A policy sets exactly one progression body. A canary policy:

```yaml
apiVersion: ome.io/v1beta1
kind: RolloutPolicy
metadata:
  name: canary-strict
  namespace: my-team
spec:
  canary:
    prometheus:
      providerRef:
        name: cluster-prometheus     # bound to an endpoint by cluster config
    steps:
      - capacity: "25%"
        traffic: 10
        analysis:
          interval: 60s
          failureLimit: 2
          metrics:
            - name: error-rate
              query: 'sum(rate(request_errors_total{service="{{.CanaryService}}"}[2m])) / sum(rate(request_total{service="{{.CanaryService}}"}[2m]))'
              operator: LTE
              threshold: "0.05"
      - capacity: "100%"
        traffic: 100
```

A rollingUpdate policy is just the budget; a blueGreen policy is `blueGreen: {}` (the type carries no fields today):

```yaml
apiVersion: ome.io/v1beta1
kind: RolloutPolicy
metadata:
  name: gentle-roll
  namespace: my-team
spec:
  rollingUpdate:
    maxSurge: "25%"
    maxUnavailable: 0
```

### What admission rejects

The validating webhook runs two rule families on every CREATE/UPDATE, so an invalid policy never lands where a consumer could reference it:

1. **The plan rules an inline block faces** — shared validation functions, so policy admission and InferenceService admission can never disagree about what is admissible. For a canary: non-empty steps, non-decreasing traffic reaching 100 on the final step, well-formed analysis (positive interval, `failureLimit >= 1`, at least one metric with a numeric threshold). For a rollingUpdate: `maxSurge` and `maxUnavailable` must not both resolve to zero (a deadlock).
2. **Policy-only portability restrictions** — a policy body is fleet data that rides to any consumer on any cluster, so it must not carry service-specific or cluster-local values:
   - Canary step capacities must be **percentages**. An absolute count (`capacity: "3"`) is service-specific; services that need absolute counts keep their canary inline.
   - The metrics source must be a **`providerRef`** naming a logical provider bound in the operator's [metric providers configuration](/ome/docs/concepts/autoscaler_policy/#metric-providers) (the same bindings AutoscalerPolicy triggers use). A raw `serverAddress` or per-service `authRef` is rejected — a cluster-local URL inside a fleet-portable object defeats portability and is an SSRF surface.
   - A `providerRef` is **required** when any step declares analysis, so a policy can never ship a gate with no resolvable metrics source.

Additionally, a body whose JSON rendering exceeds the configured `maxPinnedPlanBytes` is denied (`PlanTooLarge`).

On UPDATE, the **progression kind is frozen while any InferenceService references the policy**: consumers' rollout shape rules were admitted against the declared kind, so a canary-to-blueGreen edit in place would invalidate every one of them. Ship a kind change as a new versioned policy name and flip the refs. Any other body edit on a referenced policy is admitted with a warning — in-flight runs keep their pinned plan, so the edit takes effect at each consumer's next run.

## Attaching a policy to a rollout group

Each `spec.rollout.groups[]` entry attaches individually via `policyRef`, which names a `RolloutPolicy` in the InferenceService's own namespace:

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
  rollout:
    groups:
      - components: [engine]
        policyRef:
          name: canary-strict
          progression: canary
```

`progression` is **required**: it declares the referenced policy's kind (`canary`, `blueGreen`, or `rollingUpdate`) so every shape-dependent rollout rule — group sequencing, `soak` placement, `maintainRatio` rejection on canary groups — evaluates at InferenceService admission without dereferencing the policy. A ref-carrying group counts as its declared kind everywhere those rules apply. If the declaration turns out to contradict the policy's actual body, the rollout parks at run open (`ProgressionMismatch`); it can never mis-execute.

`kind` defaults to `RolloutPolicy` and is the only accepted value — `ClusterRolloutPolicy` is a reserved shape rejected at admission until a cluster-scoped twin ships.

## Run pinning: when the ref is resolved

A rollout run opens when a component's target revision actually changes. At run open the controller renders the **effective plan** — per group, the inline progression or the referenced policy's body, verbatim — re-validates the composed result (the belt against skew: the body passed its own admission, but this instance is what will execute), checks that any `providerRef` is bound on this cluster, and pins the plan into `status.rollout.activeRun` with per-group provenance:

| `status.rollout.activeRun.plan.groups[]` field | Meaning |
|---|---|
| `source` | `Inline` or `Policy` — where the pinned progression came from. |
| `policyRef` | The policy's identity when `source: Policy`. |
| `policyGeneration` | The policy's generation at pin time. |
| `portableDigest` | Digest of the pinned body (`rp1:...`), for both sources. |
| `group` | The resolved group executors consume — components plus exactly one inline progression; a pinned group never carries a `policyRef`. |

Executors consume the pinned plan for the duration of the run. **Spec and policy edits are inert mid-run**: the `RolloutPlanDrift` condition goes `True` (`PolicyNewerThanRun` / `SpecNewerThanRun`) while the live render differs from the pinned one, and the edit takes effect at the next run open — or immediately via the one-shot `ome.io/rollout-repin` annotation, whose value is the expected render digest (compare-and-swap against a racing edit; the literal `now` skips the check). A repin may only hold or tighten exposure, never raise it.

## Precedence: inline wins

A `policyRef` is a **sibling** of the inline progression one-of, not an arm of it: a ref and one inline progression may coexist, and the inline block always wins — including when the policy machinery is broken, so the escape hatch works precisely when it is needed. This coexistence is the documented preview and rollback mechanism, surfaced through `status.rollout.groups[]`, the always-current resolution view written every reconcile:

- **Preview:** add the ref while keeping the inline block. The group keeps resolving from the inline body (`source: Inline`), and `shadowedPolicyRef.wouldPinDigest` shows the digest a run would pin if the inline block were removed. An inline body and a policy body with identical content produce the **same digest**, so `wouldPinDigest` matching the group's `observedDigest` proves the cutover is a no-op.
- **Adopt:** remove the inline block; the group resolves from the policy (`source: Policy`).
- **Rollback:** restore the inline block — an atomic, policy-free rollback that needs no policy edit. An in-flight run keeps its pinned plan either way.

## Fail-closed parking

When a group's **only** progression is a ref that cannot resolve, the rollout **parks at run open**: the new revision is minted but held, no update gate opens, no traffic moves, and the previous revision keeps serving. A parked rollout never falls back to the default blueGreen — silently removing a declared gate is the failure this API exists to prevent. The `RolloutPlanReady` condition on the InferenceService goes `False` with a named reason, and a `RolloutPlanParked` warning event is emitted:

| `RolloutPlanReady` reason | Meaning |
|---|---|
| `Pinned` | `True` — a run is open on the pinned plan. |
| `NoActiveRun` | `True` — no rollout in progress. |
| `PolicyNotFound` | The named policy does not exist in the namespace. |
| `PolicyNotReady` | The policy exists but its body fails validation (skew or break-glass writes). |
| `ProgressionMismatch` | The ref's declared `progression` does not match the policy's actual body. |
| `ProviderUnbound` | The composed body names a metric provider not bound in this cluster's `metricProviders` configuration. |
| `PlanInvalid` | The composed plan fails re-validation, or a stored ref exists on a cluster without the feature. |

Recovery is watch-driven: the controller watches RolloutPolicy objects and re-enqueues referencing InferenceServices, so creating or fixing the policy un-parks consumers on their next reconcile (with a periodic requeue as backstop).

## Conditions and status on the policy

The status controller maintains pure observation — a policy status write can never move traffic:

| Field / condition | Meaning |
|-------------------|---------|
| `Ready` condition | `True` when the body passes the same plan validation an inline block faces plus the policy-only restrictions (`BodyValid`), `False` with `BodyInvalid` otherwise. A valid body whose provider is unbound **on this cluster** stays `True` with reason `ProviderUnbound` — clusters in one fleet legitimately bind different provider sets, but a run opened here parks until the binding exists. |
| `InUse` condition | `True` while at least one rollout group references the policy (`Attached` / `NoConsumers`) — the same signal the deletion webhook uses. |
| `attachedGroups` | Count of rollout **groups** (not InferenceServices) in the namespace referencing the policy; one service referencing it from two groups counts twice. |
| `portableDigest` | Digest of the canonicalized spec (`rp1:...`): equal across clusters iff the specs match, so fleet drift is one field-compare away. |

Status is bounded by design — counts and digests, never consumer name lists. Per-consumer truth lives on each InferenceService under `status.rollout`.

## Deleting a policy

The webhook denies deletion while any InferenceService rollout group in the namespace still references the policy, naming up to ten referencing services in the error. To proceed, either remove the refs first, or force it with a break-glass annotation set by a prior, reviewable update:

```bash
kubectl annotate rolloutpolicy canary-strict ome.io/allow-in-use-delete=true
kubectl delete rolloutpolicy canary-strict
```

If a referenced policy does disappear anyway (break-glass, or a webhook outage window), consumers park fail-closed as described above — an in-flight run keeps executing its pinned plan; the next run open parks with `PolicyNotFound`. Deletion is always allowed in a terminating namespace so teardown cannot wedge.

## Observing policies

`kubectl get` prints readiness, digest, and attachment from the status the controller maintains:

```bash
$ kubectl get rolloutpolicies
NAME            READY   DIGEST             REFS   AGE
canary-strict   True    rp1:3f9c0d2ab1e4   2      2d
gentle-roll     True    rp1:a1b2c3d4e5f6   0      2d
```

The [OME CLI](/ome/docs/tasks/kubectl-ome) supports the same resource — `kubectl ome get rolloutpolicies` (aliases `rolloutpolicy`, `rp`) adds a PROGRESSION column; `-o wide` adds the `Ready` reason, the `InUse` state, and status freshness.

## Reference

- Related concept: [Autoscaler Policy](/ome/docs/concepts/autoscaler_policy) — the sibling pattern for reusable autoscaler templates, including the shared metric-provider bindings.
- Related concept: [Inference Service](/ome/docs/concepts/inference_service)
