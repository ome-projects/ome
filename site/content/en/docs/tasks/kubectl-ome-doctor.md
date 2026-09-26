---
title: "Diagnosing an OME Installation"
linkTitle: "kubectl-ome doctor"
weight: 21
description: >
  Verify from your kubeconfig context that a cluster's OME installation is discoverable and consistent with kubectl ome admin doctor
---

`kubectl ome admin doctor` answers one question from your current kubeconfig
context: are the APIs and control-plane objects an OME installation needs
discoverable and consistent on this cluster? It is a bounded, read-only
diagnostic — it makes at most nine sequential GET requests, never LISTs,
never reads Secrets or ConfigMap content, and never mutates anything.

## What doctor reads

Every run performs the same fixed reads:

1. Seven API discovery documents, one per group version: `v1`, `apps/v1`,
   `ome.io/v1beta1`, `autoscaling/v2`, `keda.sh/v1alpha1`,
   `gateway.networking.k8s.io/v1` and `kueue.x-k8s.io/v1beta1`.
2. The `ome-controller-manager` Deployment in `--ome-namespace`
   (default `ome`), by exact name.
3. Only with `--isvc NAME`: that one named InferenceService in the effective
   workload namespace (`-n`, or your kubeconfig context's namespace).

From the discovery documents it checks a fixed catalog of resources — the
required ones are `pods`, `events` and `configmaps` (`v1`), `deployments`
(`apps/v1`), and `inferenceservices`, `basemodels`, `servingruntimes`,
`clusterbasemodels` and `clusterservingruntimes` (`ome.io/v1beta1`) — and
verifies each advertised resource's kind and scope match what OME expects.
Optional resources (InferenceReplica, AutoscalerPolicy, HPA, KEDA
ScaledObject, Gateway API, Kueue, and others) are reported the same way but
never fail the run. From the manager Deployment it extracts the `manager`
container's image tag as a version candidate and compares it against the CLI
version.

## Run it

```bash
# Check API discoverability and the control plane in namespace "ome"
kubectl ome admin doctor

# Also inspect one named InferenceService in namespace team-a
kubectl ome admin doctor --isvc chat -n team-a --ome-namespace ome
```

The command accepts no positional arguments. Besides the standard kubectl
connection flags, it takes:

| Flag | Meaning |
| --- | --- |
| `--isvc NAME` | Also GET exactly this InferenceService in the workload namespace |
| `--ome-namespace` | Namespace of the OME control plane (default `ome`) |
| `-o table\|wide\|json\|yaml` | Output format (default `table`) |

Each request is capped at 10 seconds and the whole command at 30 seconds. A
shorter `--request-timeout` is honored; a longer one is capped at 10 seconds
per request.

## Reading the report

A run with `--isvc chat` prints a table like this (rows elided — a real
run lists every catalog resource and every feature subject):

```
SUBJECT                    EVIDENCE         DETAIL
Doctor                     Incomplete       GET-only; not authorization
Context                    local            Current kubeconfig context
Workload namespace         team-a           Only selected named GET
OME namespace              ome              Named manager Deployment GET
inferenceservices          Available        Required
...
ManagerDeployment          Available        ome/ome-controller-manager
SelectedInferenceService   Available        team-a/chat
CLI version                v1.2.0           CanonicalCandidate
Manager image candidate    v1.2.3           SelectedStableTag
Operator compatibility     Unverifiable     No canonical running version
Image-tag comparison       WithinOneMinor   Computed; not compatibility
Lifecycle                  Present          Generation unverifiable
Traffic                    Present          Generation unverifiable
```

A healthy installation reads as `Available` on every required API row and
both named reads, with zero required-API violations. Do not wait for the
summary state to say `Complete`: running-operator compatibility has no
canonical source and is always counted as unavailable evidence, so even a
fully healthy cluster reports `Incomplete`. `Violations` means a required
API is provably missing.

Availability values on API rows distinguish proof from uncertainty:

- `Available` — the resource is advertised at the queried version with the
  expected kind and scope.
- `NotDiscoverableAtVersion` — the discovery document was read successfully
  and the resource is provably absent (or the whole group version returned
  NotFound). On a required API this is a violation.
- `Unavailable` with a `Reason` (`Forbidden`, `Timeout`, `Throttled`,
  `MalformedPayload`, …) — doctor could not prove presence or absence. A
  forbidden discovery read is *not* treated as a missing API.

Without `--isvc`, the `SelectedInferenceService` read shows `NotRequested`
and all feature rows show `NotSelected`. With `--isvc`, feature rows report
whether that service's status carries each block (`Traffic`, `Canary`,
`Rollout`, `Autoscaling`, `MigrationHistory`, `RuntimePin`, …) as `Present`
or `AbsentOnSelectedObject` — presence of reported status, not health or
freshness, which stay `Unverifiable`.

For scripting, use the machine formats — the human tables are not a stable
interface. The exit code plus `requiredAPIViolations` are the reliable
success signals; `unavailableEvidence` counts every row that is not
`Available`/`Present` (including the always-unverifiable compatibility fact
and, without `--isvc`, the not-requested read and not-selected features), so
a nonzero value alone is not a failure:

```bash
kubectl ome admin doctor -o json \
  | jq .content.summary.requiredAPIViolations
# 0
```

## Exit codes

| Exit code | Meaning |
| --- | --- |
| `0` | A report was produced. Optional or unproven evidence may still be unavailable — inspect the report. |
| `1` | No usable report: invalid flags or namespaces, unusable kubeconfig/context, the selected `--isvc` InferenceService was unreadable, cancellation or the 30-second deadline, or writing the report failed. |
| `2` | A report was produced **and** at least one required API is proven not discoverable at its queried version. |

Two edge cases follow from the proof rules above:

- Forbidden or timed-out discovery never causes exit `2`, because it does
  not prove an API is missing; those rows are `Unavailable` and the command
  exits `0`.
- An unreadable manager Deployment is a typed unavailable diagnostic and
  still exits `0`, but an unreadable `--isvc` selection exits `1` with no
  report, since the run was explicitly asked to describe that object.

## Required RBAC

The seven discovery reads are covered for any authenticated user by the
default `system:discovery` binding on standard clusters. The two named GETs
need `get` on `deployments` (`apps`) in the OME namespace and, with
`--isvc`, `get` on `inferenceservices` (`ome.io`) in the workload namespace —
both already granted by the baseline reader role on the
[kubectl-ome plugin page](/docs/tasks/kubectl-ome/).

## What doctor does not tell you

Doctor reports evidence, not guarantees. An advertised API does not prove
you may read or write it — doctor performs no access reviews. The manager
image tag is only a declared candidate from the Deployment spec; the
image-tag comparison (`WithinOneMinor`, `OutsideOneMinor`, `MajorMismatch`)
is computed from tags, not proof of the running operator version or
compatibility. Doctor probes no controller health and reads no logs; for
deeper evidence on one service, use `kubectl ome status` and the rollout,
autoscale, traffic and migration commands.
