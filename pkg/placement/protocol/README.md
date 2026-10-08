# Member execution authority

The source encodes an execution annotation as a versioned envelope containing
the allocation policy. Version `1` carries the plan ID/revision, source and
cluster identities, and the surge pause. Version `2` also requires a demand
contract: the resolved resource/flavor fingerprint and exactly one rendering
hash for each declared Engine/Decoder component. These versions identify fixed
wire formats; they are not operator configuration or behavior defaults.

Version `3` carries each resolved component's replica floor and may also carry
the demand contract. Engine and Decoder floors are independent counts whose
ratio follows the source minimums; the Engine count is the home's replica unit,
and Router has an independent floor. An unpaused contract accepts explicit zero
floors so members can retain their service and external autoscaler while no
replicas are running. Omitted, negative, or mismatched floors do not authorize
projection. A surge pause requires positive floors for every component;
observed autoscaled replicas do not substitute for that guarantee.

The member CRD must accept zero floor values before such a contract is written.
A positive-only member reader rejects it, so source placement must wait for a
complete policy acknowledgment before crediting application. Ordinary local
services ignore the execution annotation and retain their scaling policy.

Members accept these envelopes and existing flat policies without demand.
Demand is invalid in a flat policy or a version `1` envelope; version `2` is
invalid without it. Unknown versions,
unknown fields, incomplete authority, or an origin mismatch return an error.
Fields cannot be ignored safely when they may constrain replica growth. A reader
that only understands flat policies receives no valid allocation authority from
the envelope and fails policy validation.

The ISVC controller validates participating services before configuration reads,
finalizer changes, status updates, and component dispatch. This applies to every
deployment mode. Deletion can continue through the existing cleanup path.
Ordinary services without an origin marker ignore the execution annotation.

Engine and Decoder compare a contracted rendering with the actual rendered pod
specs immediately before workload projection. The hash includes component,
deployment mode, pod-set names and counts, complete pod specs, and live
RuntimeClass identity and configuration. RuntimeClass overhead, required node
selection, and tolerations are normalized on copies. Changed or unavailable
classes, conflicting authored values, and mismatched renderings stop projection.
The replica floor is excluded so allocation changes can reuse one unit contract.
The `primary` and `workers` pod-set names and SHA-256 encoding are part of this
comparison format, not scheduling defaults.

Install the member CRD and controller role (including RuntimeClass `get` access),
and upgrade member controllers before the source emits version `2`.
A schema that lacks demand prunes it from InferenceReplica storage;
an application observation must compare the complete stored policy with the
expected policy and must not credit that partial result. Version `1` readers
reject version `2`; flat-only readers cannot validate either envelope.

The source persists a demand contract inside each capacity allocation. Its
fingerprint must match the normalized pool demand, and its component renderings
participate in plan identity. Member writes carry this accepted contract; a
fresh computation cannot replace it without a new plan. Before writing, the
source rereads the complete allocation, including its pool evidence and contract.
A matching plan ID and revision alone cannot authorize a partially pruned plan.

An outgoing capacity home retains its last contract while its existing spec
drains. This allows the member to acknowledge pause or release even when the
source's current component inventory differs. A switch to static Split removes
capacity authority through a new execution revision. Serving observations can
change independently without invalidating the persisted allocation.

This check establishes agreement at component projection. It does not freeze a
RuntimeClass after verification, validate arbitrary admission mutations, or
establish deployment-backend support for shared surge accounting. Source-side
capacity activation still requires a provider that joins fresh hardware and
resolved member demand, with dependency checks and application fences. Nominal
weights do not require already-running workloads to establish initial placement.
Persisted contracts alone do not activate capacity-based placement.
