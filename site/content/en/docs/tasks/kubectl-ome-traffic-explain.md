---
title: "Verify Traffic Intent with kubectl ome traffic explain"
linkTitle: "kubectl-ome traffic explain"
weight: 21
date: 2026-09-26
description: >
  Check that the traffic behavior you declared on an InferenceService is supported and reported as realized by the controller.
---

`kubectl ome traffic explain` answers one question for one InferenceService:
**does what I declared match what the controller reports?** It compares three
layers — your declared traffic intent, controller-reported support for that
intent, and controller-reported realization evidence — and rolls them up into
a single summary state with named checks and issue codes.

This is a different question from `kubectl ome traffic status`, which prints
the controller-reported traffic evidence (routes, endpoints, weights) without
comparing it to anything. Use `status` to see what is reported now; use
`explain` to verify that your declaration is honored.

## Before you begin

- Install the [kubectl-ome plugin](/ome/docs/tasks/kubectl-ome/).
- The command is read-only: it performs a single `get` on the
  InferenceService and nothing else. The `kubectl-ome-reader` ClusterRole
  from the plugin page is sufficient.

## What the command does — and deliberately does not do

The command reads exactly one InferenceService object and projects a report
from its `spec` and `status`. It never queries emitted backend policies,
HTTPRoutes, Services, endpoints, or pods, so **controller status is the
ceiling of what it can prove**: a `Consistent` result means the controller
reports your intent as accepted and realized, not that packets are flowing
the way you declared. Verifying live data-plane behavior requires a request
against the service itself.

The output is also deliberately bounded. Raw annotation values, header and
cookie names, condition messages, credentials, UIDs, and resource versions
are never printed — selectors appear only as a variant and an input count
(for example `Header inputs=2`), and extension annotations only as a kind
and a count.

## What counts as declared intent

The command treats the following as traffic intent on the InferenceService:

- Typed fields under `spec.traffic`: `algorithm`, `consistentHash`,
  `endpointOverride`.
- Recognized `ome.io/` traffic annotations: the circuit-breaker
  (`ome.io/circuit-breaker-*`), retry (`ome.io/retry-*`), and timeout
  (`ome.io/timeout-*`) keys, plus the passthrough prefixes `ome.io/btp.*`
  (Envoy Gateway BackendTrafficPolicy) and `ome.io/dr.*` (Istio
  DestinationRule). These show up as `EXTENSION` counts, never as values.
- A canary rollout group in the effective rollout configuration, which makes
  the `canary-weight` check applicable.

An InferenceService with none of these produces the `NoIntent` state — there
is nothing to verify.

## Run the command

```bash
kubectl ome traffic explain chat -n prod
```

For a service whose declared intent is fully honored:

```
LAYER       STATE           VALUE           SOURCE
SUMMARY     Consistent      -               Computed/Current
INTENT      Declared        RoundRobin      Declared/Current
SUPPORT     Honored         -               Computed/Current
TRANSLATE   Computed        envoy-gateway   Computed/Current
REALIZE     Reported        r=1 e=0 w=0     Reported/Current
CHECK       Match           algorithm       Computed/Current
CHECK       Match           policy          Computed/Current
CHECK       NotApplicable   canary-weight   Computed/Current
```

Reading the rows:

- **SUMMARY** — the roll-up verdict for the whole report (states below).
- **INTENT** — whether traffic intent is `Absent`, `Declared`, or `Invalid`,
  with the declared algorithm.
- **SUPPORT** — whether the controller reports the declared intent as
  accepted: `Honored`, `Pending`, `Rejected`, `Partial`, `Unavailable`,
  `Invalid`, or `NotApplicable` when nothing is declared.
- **TRANSLATE** — the traffic translator inferred from the reported
  backend-policy resource: `envoy-gateway` for an Envoy Gateway
  BackendTrafficPolicy, `istio` for an Istio DestinationRule.
- **REALIZE** — controller-reported realization evidence, summarized as
  route (`r`), endpoint (`e`), and weight-allocation (`w`) counts.
- **CHECK** — one comparison per field: `algorithm` (declared vs. reported
  algorithm), `policy` (declared policy intent vs. reported backend-policy
  acceptance), and `canary-weight` (declared canary vs. reported canary
  observation). Each check is `Match`, `Mismatch`, `Unverifiable`,
  `Invalid`, or `NotApplicable`.
- **ISSUE** — machine-readable issue codes explaining any degraded state.

Every row carries a `SOURCE` cell of the form `Evidence/Freshness`.
Evidence says where a value came from: `Declared` (your spec), `Reported`
(controller status), `Computed` (derived by the CLI from the other two), or
`Unavailable`. Freshness says how trustworthy it is: `Current` (the
reporting condition's `observedGeneration` equals the object's current
generation), `Stale` (a valid but older generation — the controller has not
caught up with your latest edit), `Unverifiable`, or `Unavailable`.

## Summary states

| State | Meaning |
| --- | --- |
| `Consistent` | Every applicable check matches on current controller-reported evidence. |
| `NoIntent` | No traffic intent is declared; there is nothing to verify. |
| `Pending` | The controller has not yet accepted or rejected the backend policy (policy readiness reported `Unknown`). Re-run after reconciliation. |
| `Unsupported` | The controller accepted the policy but reports some declared fields as unsupported, or rejected it because no traffic translator is available for the ingress in use. |
| `Mismatch` | Reported evidence contradicts the declaration — for example the reported algorithm differs from the declared one, or the policy was rejected. |
| `Partial` | The verdict could not be fully established: some evidence is stale, unverifiable, truncated, or realization evidence is missing while support looks fine. |
| `Unavailable` | Intent is declared but the controller reports no traffic status at all. |
| `Invalid` | The declared spec or annotations fail validation, or the reported evidence is self-contradictory. |

Only `Consistent` (and `NoIntent`, vacuously) means the controller confirms
your declaration. Everything else names the layer that broke the chain.

## Example: catching a mismatch

Here the InferenceService declares `algorithm: LeastRequest`, but the
controller reports that it realized `RoundRobin`:

```
LAYER       STATE           VALUE               SOURCE
SUMMARY     Mismatch        -                   Computed/Current
INTENT      Declared        LeastRequest        Declared/Current
SUPPORT     Honored         -                   Computed/Current
TRANSLATE   Computed        envoy-gateway       Computed/Current
REALIZE     Reported        r=1 e=0 w=0         Reported/Current
CHECK       Mismatch        algorithm           Computed/Current
CHECK       Match           policy              Computed/Current
CHECK       NotApplicable   canary-weight       Computed/Current
ISSUE       -               AlgorithmMismatch   Computed/Unverifiable
```

Support is `Honored` — the controller accepted a backend policy — yet the
`algorithm` check fails on current evidence, so the summary is `Mismatch`
rather than `Consistent`. This is exactly the case `traffic status` alone
would not surface: the status looks healthy unless you compare it with the
declaration.

## Issue codes

| Code | Meaning |
| --- | --- |
| `DeclaredSpecInvalid` | `spec.traffic` fails validation. |
| `DeclaredAnnotationsInvalid` | A recognized traffic annotation fails validation. |
| `AlgorithmMismatch` | Declared and reported algorithms disagree on current evidence. |
| `IntentNotReported` | Intent is declared but the controller published no support evidence. |
| `UnsupportedDeclaredFields` | The controller currently reports declared fields the gateway cannot honor. |
| `NoTranslatorAvailable` | The policy was rejected because no traffic translator matches the ingress in use. |
| `PolicyRejected` | The controller reports the backend policy as rejected for another reason. |
| `ReportedEvidenceInvalid` | The reported traffic status itself is invalid. |
| `ReportedEvidenceStale` | At least one reported condition is from an older generation of the object. |
| `RealizationUnavailable` | Intent is declared but no routes, endpoints, allocations, or canaries are reported. |

## Output formats and scripting

`-o table` (default), `-o wide`, `-o json`, and `-o yaml` are supported.
Wide output adds the report and source metadata, the declared
consistent-hash and endpoint-override features, per-annotation-kind
extension counts, and each reported condition with its generation and
transition time.

The command exits non-zero only for lookup and usage errors (the
InferenceService not found, access forbidden, an invalid output format); a
`Mismatch` or `Unsupported` verdict still exits `0`. Human-readable output
is not a stable scripting interface before GA, so gate automation on the
JSON schema instead:

```bash
kubectl ome traffic explain chat -n prod -o json \
  | jq -e '.content.summary.state == "Consistent"'
```

API errors are sanitized to fixed phrases (`not found`, `forbidden`,
`request timed out`, …); the command never relays raw API-server message
text.
