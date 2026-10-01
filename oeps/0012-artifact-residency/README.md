# OEP-0012: Node-local Artifact Residency

## Summary

A `BaseModel` or `ClusterBaseModel` annotated with
`ome.io/artifact-residency: Evicted` requests release of its node-local Direct or Shared
artifact without deleting the model resource or remote source. The node model
entry remains with status `Evicted` and the model UID that owns that
acknowledgement. Eviction requires a current Hugging Face or OCI source and
recorded node ownership. Local, PVC, vendor, and unknown ownership are not
authorized for eviction.

## Design

Scout queues eviction on add and update events. The existing task tracker
cancels older downloads and waits for their writers to finish. Eviction uses
the existing Shared parent/child file locks and the compatible Direct path lock
for the parent directory. It performs no serving-workload discovery: deciding
whether eviction is appropriate belongs to the caller.

Before cleanup, the agent verifies the live model UID, eviction intent and
source/download inputs, and the Node UID pinned at startup. It strictly removes
the model's readiness label and writes `Updating`; rejected or ambiguous
readiness withdrawal blocks file deletion. Live identity and intent checks
repeat at destructive boundaries and ConfigMap conflict retries. Download
completion, Shared repair, and cache reconstruction also guard actual Ready
publication while the eviction annotation is current.

Shared reference release and pending cleanup receipts retain their existing
semantics. Current model paths, other Shared children, filesystem symlinks,
parsed relationships, and other pending receipts protect both child links and
parent files. Local URI paths and ordinary download destinations count as
borrowers. Failed ownership lookups preserve files. A pending receipt finishes
at its recorded old paths, even if the model's eligible remote source or
destination changes before retry. A current Local or unsupported source blocks
cleanup, including pending cleanup and duplicate acknowledgement.

Under cleanup operation protection, one ConfigMap CAS clears the exact receipt,
clears obsolete parsed artifact relationships, and records `Evicted` with the
model UID. Other consumers may still keep the same bytes resident. Ordinary
Delete continues to remove the model entry rather than acknowledge eviction.
There is no retained dead parent reference or layout-history field.

Direct eviction accepts canonical directories under the model store with a
named agent entry containing parsed configuration or a completed `Ready`
status. Ordinary OCI can produce the latter when best-effort parsing fails;
HF identity metadata is not required. Recorded artifact paths must match the
destination; child relationships, ambiguous layouts, and borrowers block
deletion. The agent persists `directArtifactPendingDeletion` with the model
UID and original canonical path, bound to the entry's `nodeUID`, before removing
files through the existing deletion path. Compatible locks and receipt checks
prevent Direct and Shared writers from overwriting unfinished cleanup. Retry
and ordinary Delete consume the original path. Delete preserves reserved files
and files whose current source is Local, PVC, or vendor, then releases cleanup
authority. Completion removes the receipt, including its cached copy, so a
duplicate eviction cannot delete bytes subsequently reused at that path.

For an eligible HF or OCI source, removing eviction intent and setting a nonempty
`ome.io/artifact-rehydration-id` queues normal reuse-capable work. The request
must be a Kubernetes label value. Pending cleanup first settles at its recorded
paths under current restoration authority. Existing source and download policy
choose the resulting layout; completed eviction is not layout authority.

Each request validates bytes under the existing file locks. Healthy parents
reattach without download; missing or corrupt bytes use existing Shared repair.
Tracked siblings participate in coordinated repair only when current identity,
source, and references agree. Failed readiness withdrawal, Direct or Local
borrowers, and unrecorded child links block replacement. Resident Direct
directories are never adopted.

Direct HF requests validate the immutable source manifest at the configured
managed directory. Healthy files are reused in place, including files with
borrowers; missing or corrupt files download only after readiness withdrawal
and the existing Direct reference and pending-cleanup checks. Inspection errors
preserve bytes. Non-reuse HF policies remain Direct. After completed eviction,
an absent, reuse-eligible destination may select Shared without layout history.

Direct OCI requests inspect the selected Object Storage listing and verify
each local file using the existing size and integrity checks. No HF identity
is required. Healthy copies are reused; repair removes only files proven
invalid, after inspecting the complete listing and passing the same Direct
write checks. Listing or inspection errors, unsafe names, and empty selections
cannot authorize repair or Ready. Validation uses the existing pod-wide limit.
The attempt propagates cancellation into the OCI download and retains its
compatible lock until all transfer workers exit, and through report publication.
Active multipart response reads are interrupted; standard downloads finish the
current file. In-flight SDK requests retain their retry policy and may finish
before cancellation takes effect.

After validation and attachment, the agent persists `Ready`, `modelUID`,
`artifactRehydrationID`, and `nodeUID` in its model entry before publishing the
paired Node Ready and request labels. The request label is
`ome.io/artifact-<first 24 SHA-256 bytes of ModelUID in hex>` with the request as
its value. Publication checks the live model UID, exact request, source inputs,
placement, and startup Node UID on retries; Node patches also test UID and
resourceVersion. Startup recovery and sibling repair cannot manufacture request
proof. Ordinary Delete removes the request label while preserving a replacement
model's Ready label.

The existing initial and periodic ConfigMap reconciliation pass recovers missing
current-request reports or either paired label by queuing a normal HF or OCI
download. It revalidates bytes before reporting, including after lost responses,
exhausted retries, agent restart, or sibling repair. Exact reports with both
labels are skipped before queue and worker admission. Queued, active, and delayed
work coalesces per model; explicit download and override intent takes precedence
over periodic recovery. Resident Direct directories are validated in place,
including when their report is missing; Local references still protect repairs.
Periodic OCI recovery reconstructs Scout's TensorRT-LLM serving-model shape
filter from the current Node, so it validates and downloads the same subset.

## Boundaries and follow-up

This phase does not implement Local restoration, controller or CRD changes,
serving-workload checks, or cross-node coordination.
Model annotations express intent; each agent acknowledges only its own node.
Existing resident files are not migrated or adopted into Shared storage.

Ordinary requests without a rehydration ID retain their existing download and
reuse behavior. A rehydration ID on an unsupported path cannot acknowledge
restoration. The configured destination and source contents remain authoritative.

## Validation

Model-agent tests cover last and non-last children, Direct and Local borrowers,
pending old paths, path aliases, failed lookups, identity and intent changes,
strict readiness withdrawal, duplicate eviction, interrupted cleanup and lost
responses, cache loss, startup repair, and concurrent downloads. Restoration
tests cover healthy reuse, missing and corrupt parents, coordinated sibling
repair, stale requests and identities, report conflicts, lost publication
responses, and interrupted cleanup or attachment across restart. The agent and
command packages run with Go's race detector. Direct HF tests also exercise
healthy reuse, incomplete and same-size corrupt files, failed inspection,
borrowers appearing during withdrawal, cleanup restart, and layout reselection.
Direct OCI tests exercise the actual source path with ordinary metadata,
whole-list inspection, integrity failures, shape filtering, cancellation,
pending cleanup, live authority changes, and recovery of missing reports.
Cluster deployment testing is outside this change.
