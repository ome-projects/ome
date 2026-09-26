# OEP-0012: Model Artifact Eviction and Rehydration

<!-- toc -->
- [Summary](#summary)
- [Motivation](#motivation)
  - [Goals](#goals)
  - [Non-Goals](#non-goals)
- [Proposal](#proposal)
- [Design Details](#design-details)
- [Test Plan](#test-plan)
- [Graduation Criteria](#graduation-criteria)
- [Drawbacks and Alternatives](#drawbacks-and-alternatives)
- [Implementation History](#implementation-history)
<!-- /toc -->

## Summary

Reclaim a PerNode Model's local files without deleting its BaseModel or
ClusterBaseModel. Reuse Model annotations, status and existing agent cleanup;
add no new CRD or InferenceService startup protocol.

## Motivation

Idle artifacts consume node storage while their model definitions should remain
available. This proposal separates file residency from Model lifetime.

### Goals

Keep stable Model identity while reclaiming local storage; safely restore bytes
and allow serving as soon as one eligible node validates the current request.

### Non-Goals

Automatic LRU eligibility, usage tracking, caller workflows and service admission
coordination are out of scope. No ISVC/Pod demand scanning is added to model-agent.

## Proposal

1. An operator confirms eviction eligibility and sets
   `ome.io/artifact-residency: Evicted`. The operator coordinates active use and
   pending service creation. The agent checks local file safety, not serving demand.
2. After conflicting writers settle, each agent withdraws local readiness and
   runs restartable cleanup under existing locks. Referenced shared files remain.
3. To restore, a caller atomically removes eviction intent and sets a fresh unique
   `ome.io/artifact-rehydration-id`, using a Kubernetes-label-valid token.
   Concurrent callers join the current request; retries retain its identity.
4. Agents settle older cleanup on its recorded paths, validate or download bytes,
   and report the captured Model UID, request ID and Node UID.
5. The first eligible node with a valid current-request Ready report makes the
   Model Ready and advances `status.rehydration.completedRequestID`. Slow nodes
   continue independently. The field retains the last completion as history.
6. Before creating an ordinary InferenceService, the caller verifies the expected
   Model UID, no eviction intent, Ready and completion of the current request.
   Every consuming Pod requires both normal Ready and current-request node labels.

Eligibility uses the Model's storage selector and required node affinity.
An old completion alone is not readiness. Existing PVC, Sharded and merged-weight
exclusions remain; overlay-model and workload readiness are unchanged.

## Design Details

- Preserve UID/path ownership, exact cleanup receipts, original cleanup paths,
  live and persisted references (including Local readers), and fail-closed checks.
- Writers and deletion use compatible cross-process path-family locks. Cancellation
  waits for the writer to exit before cleanup. Completed receipts do not authorize
  deletion of subsequently restored or reused bytes.
- Recheck intent and ownership before destructive changes and final publication.
  Stale requests and same-name replacement Models or Nodes cannot publish readiness.
- Periodic recovery repairs missed work. Cancelling one caller does not cancel
  restoration shared with other callers.

## Test Plan

Unit, race and controller tests cover interrupted cleanup, competing writers,
shared/Local references, changed paths, reused bytes, stale receipt/request
publication, identity replacement, first-node completion, historical completion,
and single/multi-node selector precedence. Live qualification must additionally
exercise eviction/restoration and restart recovery with compatible components.

## Graduation Criteria

Qualify cleanup, recovery and both scheduling modes before rollout. Deploy
compatible CRDs, controllers and agents before callers use the protocol.
Disabling a caller feature does not clear residency or restoration state;
downgrading to unaware components while this metadata is active is unsupported.
Automatic LRU requires a separate eligibility and admission design.

## Drawbacks and Alternatives

Restoration adds latency and conservative safety checks may require operator
repair. Deleting/recreating CRs loses stable identity; agent demand checks mix
policy with file safety; pre-creating an ISVC would require a startup gate.
The proposal instead keeps waiting with the caller and reuses local cleanup.

## Implementation History

2026-09-24: initial proposal. 2026-09-25: simplify completion to first-node Ready
and reuse cleanup mechanisms. Provisional; no release or live validation claimed.
