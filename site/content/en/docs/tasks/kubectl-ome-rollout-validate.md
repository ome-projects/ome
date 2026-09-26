---
title: "Validate Rollout Configuration Before You Rely on It"
linkTitle: "kubectl-ome rollout validate"
weight: 21
date: 2026-09-26
description: >
  Pre-check an InferenceService's rollout, traffic and autoscaling configuration with kubectl ome rollout validate, and script against its Valid, Invalid and Unverifiable outcomes.
---

`kubectl ome rollout validate INFERENCESERVICE` is a read-only preflight
check. It fetches the named InferenceService with exactly one API `get` —
nothing else is read and nothing is mutated — and prints one
**RolloutValidationReport** covering the stored rollout, traffic and
autoscaling configuration. Use it before you pause, promote, repin or
otherwise act on a rollout, or wire it into CI to catch configuration that
the controller would reject or silently fail to resolve.

For installing the plugin, the shared connection flags and the full
exit-code contract, see the [kubectl-ome Plugin](/docs/tasks/kubectl-ome/)
page.

## Running it

```bash
kubectl ome rollout validate chat              # compact check matrix (default)
kubectl ome rollout validate chat -o wide      # adds evidence, freshness and issue codes
kubectl ome rollout validate chat -o json      # full typed report
kubectl ome rollout validate chat -o yaml
```

Any other `-o` value, a missing or extra argument, or a name that is not a
valid DNS-1123 subdomain is rejected before a single API request is made
(exit 1). Default table lines stay within 80 characters and wide lines
within 120.

A healthy service with no rollout groups, no traffic intent and no
autoscaler policy references looks like this:

```
CHECK             COMP     RESULT          SOURCE
OVERALL           -        Valid           Computed/Current
ROLLOUT-REFS      -        Valid           Computed/Current
ROLLOUT-PLAN      -        Valid           Computed/Current
ROLLOUT-ORDER     -        Valid           Computed/Current
ROLLOUT-RESOLVE   -        NotApplicable   Computed/NotApplicable
TRAFFIC-SPEC      -        Valid           Computed/Current
TRAFFIC-READY     -        NotApplicable   Computed/NotApplicable
SCALING-POLICY    -        Valid           Computed/Current
AUTOSCALER-SPEC   -        Valid           Computed/Current
AUTOSCALER-SPEC   engine   Valid           Computed/Current
```

Each row is one bounded check with a result of `Valid`, `Invalid`,
`Unverifiable` or `NotApplicable`. The `SOURCE` column is
`<evidence>/<freshness>`: `Computed` checks are derived from the stored
spec alone and are always `Current`; `Reported` checks re-examine status
the controller wrote, whose freshness may be `Current`, `Stale` or
`Unverifiable`; `Unavailable` means the status evidence the check needed
does not exist. The `OVERALL` row is the aggregate: `Invalid` if any check
is Invalid, otherwise `Unverifiable` if any check is Unverifiable,
otherwise `Valid`. `NotApplicable` checks never affect it.

## Exit codes

The report is always written to stdout **before** its assertion is
evaluated, so even a failing run leaves you a complete report to inspect:

| Outcome | Exit code | stderr |
| --- | --- | --- |
| `Valid` | 0 | — |
| `Invalid` | 2 | `error: rollout validation found invalid configuration` |
| `Unverifiable` | 2 | `error: rollout validation could not verify all prerequisites` |
| API, projection or output failure | 1 | `error: <message>` |

This follows the plugin-wide
[exit-code mapping](/docs/tasks/kubectl-ome/#exit-codes): exit 2 is the
read-only "checked, and it is not so" signal, and exit 1 means the command
could not complete its observation — do not treat any partial output as a
report. Because Invalid and Unverifiable share exit 2, scripts that need
to tell them apart must read `content.summary.state` from the report:

```bash
kubectl ome rollout validate chat -o json > validate.json
case $? in
  0) echo "valid" ;;
  2) jq -r '.content.summary.state' validate.json ;;
  *) echo "validation did not run; ignore validate.json" >&2 ;;
esac
```

## What each check verifies

The command deliberately performs no reads beyond the InferenceService
itself: it does not fetch RolloutPolicies, AutoscalerPolicies, HPAs or
pods. Cluster-dependent facts count as verified only when
generation-current controller status stored on the InferenceService proves
them; everything else is reported `Unverifiable` rather than assumed.

| Row | Check | Runs when | Verifies |
| --- | --- | --- | --- |
| `ROLLOUT-REFS` | RolloutReferences | always | every rollout group `policyRef` has a non-empty DNS-1123 name, kind `RolloutPolicy` (or empty), and a progression of `canary`, `blueGreen` or `rollingUpdate` |
| `ROLLOUT-PLAN` | RolloutPlan | always | the stored plan is bounded and well formed: at most 3 groups, each with 1–3 components and at most one progression shape; canary groups within 20 steps and 10 analysis metrics per step; canary, coordination and lifecycle validation pass |
| `ROLLOUT-ORDER` | RolloutOrdering | always | the plan makes no ordering promise the controller does not enforce — for example a group `order` list is Invalid, because the components in a group advance together |
| `ROLLOUT-RESOLVE` | RolloutResolution | `spec.rollout` declares groups, or an active run is recorded | the controller's `RolloutPlanReady` condition and, for an active run, the pinned plan record are present, unique and internally consistent |
| `TRAFFIC-SPEC` | TrafficSpec | always | `spec.traffic` and the traffic-related annotations are valid (separate `TrafficSpecInvalid` and `TrafficAnnotationInvalid` issue codes) |
| `TRAFFIC-READY` | TrafficReadiness | the spec declares a traffic algorithm, consistent hash or endpoint override, or any traffic-related annotation | `status.traffic` carries exactly one generation-current `BackendPolicyReady` condition that is `True`, no unsupported-fields condition, an algorithm matching the spec, and a backend policy reference naming this service as an Envoy Gateway `BackendTrafficPolicy` or Istio `DestinationRule` |
| `SCALING-POLICY` | ScalingPolicy | always | `spec.scalingPolicy` is valid |
| `AUTOSCALER-SPEC` (`COMP -`) | AutoscalerSpec | always | the service-level autoscaler configuration: config shape, annotation conflicts, target utilization and scale-to-zero settings |
| `AUTOSCALER-SPEC` (per component) | AutoscalerSpec | for each declared component (engine, decoder, router) | the component's autoscaler shape, min/max replica bounds and `autoscalerPolicyRef` name and kind |
| `AUTOSCALER-RESOLVE` | AutoscalerResolution | a component sets `autoscalerPolicyRef` | the component's `AutoscalerResolved` status condition is generation-current, consistent with the declared reference, and `True` |

### A declared rollout is at best Unverifiable

When `spec.rollout` declares groups, the `ROLLOUT-RESOLVE` check can never
report `Valid`. `RolloutPlanReady` is a condition without an
observed-generation field, so even a well-formed, `True` condition cannot
be bound to the generation you are validating, and the check ends
`Unverifiable` with the issue code
`RolloutResolutionFreshnessUnverifiable`. Plans that depend on resources
this command does not read — an unpinned `RolloutPolicy` reference or a
canary step with `analysis` — end `Unverifiable` with
`RolloutPrerequisiteUnverifiable` for the same reason.

For such services exit 0 is unreachable by design. Treat exit 2 whose only
issues are those two freshness codes as the expected healthy outcome, and
anything reporting `Invalid`, `RolloutResolutionMissing`,
`RolloutResolutionMalformed` or `RolloutResolutionFailed` as a real
problem.

## Wide output and issue codes

`-o wide` splits the `SOURCE` column into `EVIDENCE` and `FRESHNESS` and
adds an `ISSUES` column with the typed codes scoped to each row (long
lists collapse to the first code plus `,+N`). The codes are a closed,
stable set — there are no free-text messages to parse:

| Code | Meaning |
| --- | --- |
| `RolloutReferenceInvalid`, `RolloutPlanInvalid`, `RolloutOrderingInvalid` | the corresponding spec check failed |
| `RolloutResolutionMissing` | a rollout is declared but no `RolloutPlanReady` condition exists |
| `RolloutResolutionMalformed` | the `RolloutPlanReady` condition or pinned active-run record is duplicated or inconsistent |
| `RolloutResolutionFailed` | `RolloutPlanReady` is `False` (policy not found, not ready, plan invalid, ...) |
| `RolloutResolutionStale` | rollout resolution evidence predates the current generation |
| `RolloutResolutionFreshnessUnverifiable` | the stored resolution looks correct but cannot be bound to this generation |
| `RolloutPrerequisiteUnverifiable` | the plan depends on a resource this command does not read |
| `TrafficSpecInvalid`, `TrafficAnnotationInvalid`, `ScalingPolicyInvalid`, `AutoscalerSpecInvalid`, `AutoscalerReferenceInvalid` | the corresponding spec field or annotation failed validation |
| `TrafficEvidenceMissing`, `AutoscalerEvidenceMissing` | traffic intent or a policy reference is declared but the status evidence does not exist |
| `TrafficEvidenceStale`, `AutoscalerEvidenceStale` | the status conditions predate the current generation |
| `TrafficEvidenceMalformed`, `AutoscalerEvidenceMalformed` | the status conditions contradict themselves or the spec |
| `TrafficNotReady` | `BackendPolicyReady` is not `True`, or unsupported fields were reported |
| `AutoscalerResolutionFailed` | `AutoscalerResolved` is `False` (policy not found, invalid, auth not found, class unavailable) |
| `ChecksTruncated`, `IssuesTruncated` | the report bounds (16 checks, 32 issues) were exceeded |
| `EvidenceMalformed` | a value outside the closed schema was encountered and clamped |

## The JSON report

`-o json` and `-o yaml` emit the same typed document
(`apiVersion: cli.ome.io/v1alpha1`, `kind: RolloutValidationReport`):
`metadata` names the subject, `sources` lists the single InferenceService
read with its UID and generation, `content` holds `summary.state`,
`checks[]` and `issues[]`, and `warnings[]` carries `PartialData` when the
overall state is Unverifiable and `StaleEvidence` when any check found
stale status. Apart from the `collectedAt` timestamps, the document is
deterministic for a given InferenceService — canonically ordered, bounded,
and built entirely from closed enums and resource names rather than
free-text messages — so it is safe to script against.

## RBAC

The command needs only `get` on `inferenceservices`, which the baseline
reader role on the [plugin page](/docs/tasks/kubectl-ome/#required-rbac)
already grants. Unlike `autoscale status`, it never reads HPAs or KEDA
objects, so no extra rules are required.

## Related pages

- [Pause and Resume a Rollout](/docs/tasks/pause-and-resume-a-rollout/) and
  [Promote or Roll Back a Canary](/docs/tasks/promote-or-rollback-a-canary/)
  — the action commands to run after validation passes.
- [Repin a Drifted Rollout Plan](/docs/tasks/repin-a-drifted-rollout-plan/)
  — when the pinned active-run record no longer matches the spec.
- `kubectl ome rollout status` — progress of a rollout that is already
  running, per group and component.
