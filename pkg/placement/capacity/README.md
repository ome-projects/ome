# Nominal replica capacity

`MeasureUnit` measures the configured accelerator resources in a rendered
Engine/Decoder replica unit. Each component supplies named sets of identical
pods and an explicit count. A leader and its workers are separate sets. Router
is absent from this input because it follows a per-home replica policy.

The caller must supply the complete member rendering, including runtime and
service overrides, accelerator selection, worker counts, and any admission
resources such as RuntimeClass overhead. `InputFingerprint` identifies those
resolved dependencies. This helper does not render a service, resolve admission
configuration, or verify that a member applied the templates.

Accounting uses Kubernetes' pod request calculation: regular containers run
together; restartable init containers overlap subsequent initialization and
the application; sequential init containers contribute their peak; pod overhead
is added after that peak. Omitted accelerator requests inherit their limits,
matching API defaulting. Explicit requests must equal limits. Each set's pod
request is multiplied by its count with checked whole-unit arithmetic.

Missing configuration, invalid quantities, arithmetic overflow, dynamic resource
claims, unsupported pod-level resources, and nonzero unconfigured extended
resources return no result. An unconfigured extended resource might be another
accelerator, so it cannot be silently discarded. CPU and memory do not contribute
to accelerator demand. A component with no accelerators is retained, but the
whole unit must have positive accelerator demand to be normalized.

`UnitDemand` retains independent pod sets and their scheduling constraints for
resource/flavor attribution. It does not combine sets just because they request
the same resource: an Engine and Decoder may require different flavors. Its
fingerprint includes rendering dependencies, configured resource names, pod
counts, and complete templates. Pod-set and configuration ordering are canonical;
init-container ordering is preserved because it affects demand. Inputs are not
mutated and returned templates do not alias them.

## Hardware flavor attribution

`AttributeUnit` maps each measured pod set to exactly one ResourceFlavor. It
uses required node selectors and node-affinity expressions on label keys present
in the member's complete flavor catalog. This follows Kueue's label projection,
without consulting queue resource groups or quota to decide placement weights.
Preferred affinity, node field constraints, and labels outside the catalog
remain scheduling inputs; they do not select a hardware pool. In particular,
model-readiness labels remain in the rendered templates for the member scheduler.
Multiple compatible flavors are unknown, including overlapping flavors where
the pod's requirements do not identify one pool. No flavor ordering or available
hardware amount resolves that ambiguity.

The member root must report the complete configured resource/flavor matrix,
including explicit zeros. Every current row must agree with live flavor UIDs,
node labels, and the shared mapping fingerprint. The fingerprint includes unused
flavors because they can change which pool owns a node. Historical rows without
attribution cannot prove capacity. Required pools must have complete attribution;
unattributed nodes cannot silently contribute to a weight.

The reporter attributes nodes to their unique most-specific flavor and reports
ties as incomplete. Empty-valued flavor labels are rejected by this consumer:
the report's label comparison cannot distinguish an absent label from a present
empty value. The quota reporter itself accepts such labels; only capacity
attribution rejects them.

Engine, Decoder, and worker demand sharing a pool is combined in whole units.
Different accelerator resources remain separate. The resulting demand fingerprint
includes the rendered unit, complete mapping, and combined pool demands. Hardware
totals, observation times, and quota amounts do not enter that fingerprint.

## Fresh hardware observations

After unambiguous flavor attribution, `Reader` combines demand sharing a
resource/flavor pool and normalizes identified, fresh per-cluster allocatable
reports to whole replica units. It takes the minimum ratio across required
pools. It never subtracts usage or quota. Missing or incompatible evidence
rejects the whole matched-set observation; the controller must retain its last
accepted plan. Aggregate hardware counts do not prove gang topology, CPU/memory
fit, or quota admission.

The hardware reader leaves `Sample.DemandContract` unset. A caller must join the
resolver's contract to that sample before proposing capacity authority. The
source rejects missing contracts, different demand fingerprints, incomplete
component inventories, or weights inconsistent with the recorded pools. It
persists the complete evidence and copies the accepted contract into member
execution policies. Heartbeat times and report versions do not change plan
identity; hardware, mapping, and rendering changes do.
