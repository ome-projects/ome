---
title: "Read the kubectl ome traffic status Report"
linkTitle: "kubectl-ome traffic status"
weight: 21
date: 2026-09-26
description: >
  What kubectl ome traffic status reports about routes, endpoints, weights and canary traffic — and why controller-reported evidence is not proof of data-plane realization.
---

`kubectl ome traffic status INFERENCESERVICE` prints one read-only
**TrafficStatusReport** for an InferenceService: the resolved load-balancing
algorithm, the emitted backend policy and the HTTPRoutes it targets, the
service's reported endpoints, per-component revision traffic weights, and the
state of any canary run. It is the drill-down behind the one-line `Traffic`
row in [`kubectl ome status`](/ome/docs/tasks/kubectl-ome-status), and the
follow-up command that [`traffic drain` /
`undrain`](/ome/docs/tasks/drain-traffic-from-a-workload-cluster) print after
a guarded annotation write. For installing the plugin and the RBAC it needs,
see [kubectl-ome Plugin](/ome/docs/tasks/kubectl-ome).

## Usage

```bash
kubectl ome traffic status chat -n prod            # compact table (default)
kubectl ome traffic status chat -n prod -o wide    # every entry, one row each
kubectl ome traffic status chat -n prod -o json    # full typed report
kubectl ome traffic status chat -n prod -o yaml
```

The single argument is the InferenceService name, validated as a DNS-1123
subdomain before any API request; any other `-o` value is rejected the same
way. The command accepts the standard kubectl connection flags
(`--kubeconfig`, `--context`, `-n`).

`-o json` and `-o yaml` emit the same data as a typed `TrafficStatusReport`
(`apiVersion: cli.ome.io/v1alpha1`, an alpha contract). Script against
`-o json` — the human-readable tables are not a stable interface.

## What the report claims — and what it doesn't

The command performs **exactly one read**: a `get` of the named
InferenceService, whose returned name, namespace and UID must match the
request. It does not query backend policies, HTTPRoutes, Services, gateways
or pods, and it never sends a request to any endpoint it prints. Everything
in the report is therefore *controller-reported evidence* — what the OME
controller wrote into the InferenceService status at some reconcile — not
proof that a data plane has realized it:

- **A route name is a status entry, not a programmed route.** The `ROUTE`
  rows echo `status.traffic.targetedHTTPRoutes`; the report does not verify
  that those HTTPRoutes exist, are accepted by a gateway, or carry traffic.
- **An endpoint is echoed, never probed.** `ENDPOINT` rows come from the
  service's reported addresses. A listed URL proves the controller recorded
  it, not that it currently resolves or answers.
- **Weights are the controller's programmed split, not observed traffic.**
  `WEIGHT` rows echo the per-component revision targets from status. The
  live distribution can differ while a gateway converges or while a revision
  is unhealthy.
- **`POLICY-READY True/AcceptedByGateway` is itself a report.** It means the
  controller recorded that the gateway accepted the emitted policy — the
  gateway's state may have changed since.

The `SOURCE` column makes this explicit for every value (see
[The SOURCE column](#the-source-column-evidence-and-freshness)). Nothing in
this report ever reaches the `Observed` evidence level used elsewhere in the
plugin, because nothing is verified against a live object.

## The default table

A typical table for a service with an Envoy Gateway policy and an engine
canary at step 2 of 4:

```
FIELD         COMP    VALUE                    SOURCE
STATE         -       Reported                 Computed/Unverifiable
TRANSLATOR    -       envoy-gateway            Computed/Current
ALGORITHM     -       RoundRobin               Reported/Current
POLICY-READY  -       True/AcceptedByGateway   Reported/Current
UNSUPPORTED   -       None                     Reported/Current
ROUTES        -       1                        Reported/Current
ENDPOINTS     -       2                        Reported/Unverifiable
CANARY        engine  2/4 @ 25%                Reported/Unverifiable
WEIGHT        engine  stable:a1b2c3d4=75%      Reported/Unverifiable
WEIGHT        engine  canary:e5f6a7b8=25%      Reported/Unverifiable
```

### Summary rows

- **`STATE`** — the report's overall verdict; see
  [States](#states) below.
- **`TRANSLATOR`** — `envoy-gateway`, `istio` or `Unavailable`. This is
  *inferred* (evidence `Computed`) from the exact group/version/kind of the
  emitted policy reference: `gateway.envoyproxy.io/v1alpha1`
  `BackendTrafficPolicy` proves Envoy Gateway, `networking.istio.io/v1`
  `DestinationRule` proves Istio. No emitted policy, or an unrecognized
  kind, yields `Unavailable` — the command never guesses from cluster state.
- **`ALGORITHM`** — the resolved load-balancing algorithm from
  `status.traffic.algorithm`: `Default`, `RoundRobin`, `LeastRequest`,
  `Random` or `ConsistentHash`. `Default` means no algorithm was set on
  `spec.traffic` and the gateway implementation default applies. Any other
  string in status renders as `Unknown` with an `AlgorithmInvalid` issue.
- **`POLICY-READY`** — `<status>/<reason>` of the `BackendPolicyReady`
  condition. Reasons are an allowlist (`AcceptedByGateway`, `Pending`,
  `ConflictingPolicy`, `NoTranslatorAvailable`, `GatewayRejected`,
  `TranslationFailed`); arbitrary condition reasons and messages are never
  copied into the report. `Unknown/NotReported` means no condition exists.
- **`UNSUPPORTED`** — whether `spec.traffic` fields were silently dropped in
  translation. `Present` means the controller reported a
  `BackendPolicyUnsupportedFields` condition: the emitted policy omits those
  fields. `None` is shown only when current evidence proves it; anything
  short of proof is `Unknown`.

### Count and detail rows

- **`ROUTES` / `ENDPOINTS`** — counts of the vetted entries; the individual
  names and URLs appear in `-o wide`, `-o json` and `-o yaml`.
- **`CANARY`** — one row per active canary unit:
  `<step>/<total> @ <weight>%`, where the step is 1-based for display and
  the weight is `observedTrafficWeight` — the external traffic weight the
  controller reports it has programmed for the canary revision. A completed
  run shows `<total>/<total> @ 100%`.
- **`WEIGHT`** — one row per revision target per component (engine, decoder,
  router): `<role>:<revision hash>=<percent>%`. Roles are `stable`, `canary`
  and `other`; during a canary run the roles are assigned by matching the
  target's revision hash against the canary status' stable and canary
  hashes, otherwise the stable role follows the component's
  `latestRolledoutRevision`.
- **`ISSUE`** — typed diagnostic codes; see
  [Issues and warnings](#issues-and-warnings).

The default table keeps every line within 80 columns; revision names are
shown as their 8-character hashes for that reason.

## The wide view: `-o wide`

`-o wide` replaces the count rows with one row per vetted entry and adds the
rows the compact table omits:

```
POLICY     -       gateway.envoyproxy.io/v1alpha1/BackendTrafficPolicy/prod/chat               Reported/Current
ROUTE      -       chat                                                                        Reported/Current
ENDPOINT   -       https://chat.prod.example/                                                  Reported/Unverifiable
CANARY     engine  2/4 @ 25%                                                                   Reported/Unverifiable
TARGET     engine  stable:chat-engine-rev-a1b2c3d4=75%                                         Reported/Unverifiable
TARGET     engine  canary:chat-engine-rev-e5f6a7b8=25%                                         Reported/Unverifiable
CONDITION  -       BackendPolicyReady=True/AcceptedByGateway gen=7 at=2026-09-26T09:59:00Z     Reported/Current
```

Only the compact table is bound to 80 columns; wide rows run as long as their
values.

- **`POLICY`** — the emitted backend policy reference
  (`apiVersion/kind/namespace/name`) from
  `status.traffic.backendPolicyResource`. This is the object OME emitted; it
  is not read back.
- **`TARGET`** — the same allocations as `WEIGHT`, with full revision names
  instead of hashes.
- **`CONDITION`** — each retained condition (`BackendPolicyReady`,
  `BackendPolicyUnsupportedFields`) with its `observedGeneration` and
  transition time, so you can see exactly which generation the evidence is
  bound to.

## The SOURCE column: evidence and freshness

Every value carries `<evidence>/<freshness>`:

- **Evidence** is `Reported` (copied from controller-written status) or
  `Computed` (derived locally from reported values — the state verdict and
  the translator inference).
- **Freshness** says whether the value is bound to the current
  `metadata.generation`. `Current` means the backing condition's
  `observedGeneration` equals the spec generation; `Stale` means it is
  older (you changed the spec and the controller has not caught up).
  **`Unverifiable` means the field carries no generation marker at all** —
  endpoints, weights and canary status have none, so those rows stay
  `Unverifiable` even when neighboring conditions are `Current`. That is a
  property of the API, not a defect of your service. `Unavailable` marks
  values with no backing evidence.

Fields with no generation marker are echoed as reported; the condition rows
are the only place freshness can actually be established.

## States

| `STATE` | Meaning |
| --- | --- |
| `Reported` | `status.traffic` exists, `BackendPolicyReady` is `True/AcceptedByGateway` at the current generation, and nothing was partial. The controller reports a fully translated, gateway-accepted configuration — still not data-plane proof. |
| `Pending` | `BackendPolicyReady` is `Unknown/Pending` at the current generation: translation or gateway acceptance has not completed. |
| `Degraded` | `BackendPolicyReady` is `False` at the current generation — the reason says why (`ConflictingPolicy`, `NoTranslatorAvailable`, `GatewayRejected`, `TranslationFailed`). |
| `Partial` | Some evidence was stale, truncated or missing — including a `BackendPolicyReady` condition not bound to the current generation. |
| `Unavailable` | No `status.traffic` at all, or no `BackendPolicyReady` condition. `status.traffic` is only populated when traffic intent is declared via `spec.traffic` or an `ome.io/*` traffic annotation, so `Unavailable` with a `TrafficStatusMissing` issue is normal for a service that declares none. |
| `Invalid` | Reported evidence was malformed or self-contradictory; the `ISSUE` rows carry the specifics. |

## Where the evidence comes from, and its bounds

All values are read from the one InferenceService, deduplicated, sorted and
bounded; whatever exceeds a bound is dropped and marked with a truncation
issue, never silently cut mid-value.

| Evidence | Read from | Bound |
| --- | --- | --- |
| Algorithm, policy, routes, conditions | `status.traffic` | 4 route names |
| Endpoints | `status.addresses`, falling back to `status.url` and `status.address.url` | 16 URLs, each ≤ 512 bytes with a path ≤ 256 bytes |
| Canary rows | the effective rollout plan plus per-unit canary status | 3 canary groups, plans of ≤ 20 steps |
| Weights | `status.components.<engine\|decoder\|router>.traffic` | 8 revision targets per component |

Endpoints are sanitized before they are printed: only `http`/`https` URLs
with a valid host survive, and credentials, query strings and fragments never
enter the report. An entry that fails sanitization is dropped with an
`EndpointInvalid` issue.

Canary evidence is only echoed when it passes the same plan validation the
controller applies before pinning a canary, plus consistency checks between
the plan, the canary status and the component's traffic targets — a status
shape the controller could never produce is rejected as `CanaryInvalid`
rather than displayed. When more than one canary unit runs concurrently,
each gets its own `CANARY` row (and its own entry in the JSON `canaries`
list; a single run uses the singular `canary` field). Neither state nor
weight is ever aggregated across units.

## Issues and warnings

`ISSUE` rows carry typed, message-free codes:

| Code | Meaning |
| --- | --- |
| `TrafficStatusMissing` | No `status.traffic` — normal when no traffic intent is declared. |
| `AlgorithmInvalid` | `status.traffic.algorithm` is outside the closed set. |
| `PolicyReferenceInvalid` / `PolicyKindUnsupported` | The emitted policy reference has a malformed name, or a kind other than the two recognized ones. |
| `PolicyConditionMissing` | `status.traffic` exists without a `BackendPolicyReady` condition. |
| `ConditionInvalid` / `ConditionConflict` | A condition is malformed (impossible status/reason pairing, unusable `observedGeneration`, missing transition time), or duplicated. |
| `RouteInvalid` / `EndpointInvalid` | A route name or endpoint URL failed validation and was dropped. |
| `RoutesTruncated` / `EndpointsTruncated` / `AllocationsTruncated` | A bound cut off entries; counts are lower bounds. |
| `CanaryInvalid` | Canary plan or status failed validation or consistency checks. |
| `AllocationInvalid` / `AllocationConflict` | Revision targets are malformed (percent outside 0–100, weights not summing to 100, unparsable revision name, ambiguous stable revision) or name the same revision twice. |
| `UnknownComponentStatus` | A component other than engine, decoder or router carries status. |
| `StatusCombinationInvalid` | Reported fields contradict each other — routes without a policy, `Ready=True` without a policy, `NoTranslatorAvailable` alongside an emitted policy. |

Report-level warnings summarize the same conditions: `PartialData`,
`StaleEvidence` and `Truncated`.

## Required RBAC

The command only ever issues one `get` of the InferenceService, which the
baseline read-only role on the
[kubectl-ome page](/ome/docs/tasks/kubectl-ome/#required-rbac) already
grants. It is a pure read: exit code 0 on success, 1 when the observation
could not be completed — it never exits 2 or 3.

## Verifying realization

When you need more than controller-reported evidence, follow the references
the report gives you with ordinary kubectl reads: `kubectl get` the emitted
policy named in the `POLICY` row and the HTTPRoutes in the `ROUTE` rows and
inspect *their* status conditions, and send a request to an endpoint
yourself. `traffic status` deliberately stops at the InferenceService
boundary so that what it prints is exactly what the controller claimed — no
more, no less.
