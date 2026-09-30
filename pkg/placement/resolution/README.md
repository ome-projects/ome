# Member runtime resolution

`Resolver` reads the runtime and model inputs for an intended member service.
It follows the member's explicit-runtime precedence, automatic catalog ranking,
runtime inheritance, pin drift acknowledgements, and component merge rules.
Runtime configuration fills only components declared by the service.

The caller supplies a direct client bound to a verified member registration and
the member's operator namespace when reading configuration or runtime revisions.
For an existing derived service, the caller must verify ownership and supply its
current identity as `standing`. The resolver reads pin status from that live
object. A nil `standing` requires the intended service to be absent on the member.
Neither input objects nor API objects are written.

Pinning predicts the live spec for an initial pin or an acknowledged advance
without creating a ControllerRevision. Existing pins must have matching runtime
name, kind, and namespace metadata. Ambiguous scope, missing revisions, disabled
inputs, unacknowledged drift, and failed reads return no resolution.

Each resolution retains object observations, missing-object lookups, and the
selected catalog's membership. The standing service observation includes its
spec, identity, metadata, pinned revision, and runtime sync token. Unrelated
serving status and managed-field updates do not invalidate runtime resolution.
Other resources retain strict UID/resource-version observations. `Runtime.Check`
repeats those reads with its caller's context. Replacement, deletion, input
mutation, newly shadowing objects, partial catalogs, and API errors invalidate
the observation. Concurrent checks use separate read objects. This is
conservative revalidation, not an atomic snapshot across Kubernetes objects.

The returned components precede deployment defaults, accelerator selection, and
pod rendering. They are not final accelerator demand. Capacity consumers must
derive complete Engine/Decoder pod and gang shapes, resolve flavor attribution,
and verify member application independently. The runtime hash identifies the
selected runtime spec; it does not include service overrides, model data, or
the member's rendered workloads.

## Rendered replica units

`Resolver.ResolveUnit` extends runtime resolution through the member's
Engine/Decoder renderer and accelerator accounting. It reads the configured
member operator namespace, applies deployment defaults, selects each component's
AcceleratorClass, resolves overlays and fine-tuned weight inputs, and calls the
same pod rendering methods as the member. No workloads or status are written.
Router is excluded from the placement unit.

The member ConfigMap must explicitly configure `acceleratorResources`.
`modelCache.provider` supplies the model-cache provider, following the member's
field name and provider-name normalization. Configuration, AcceleratorClasses,
model inputs, optional Service lookups, and RuntimeClasses join the dependency
checks. A failed read invalidates the observation even if a member helper
tolerates that optional lookup.

RawDeployment contributes its primary pod. OMENative and MultiNode gangs
contribute a leader and the resolved worker count. A declared leader/worker pair
with size omitted resolves to one worker under the member's defaulting helper.
Incomplete gangs and unsupported deployment modes return no unit. Component
replica floors do not affect the per-replica demand fingerprint.

RuntimeClass overhead, node selectors, and tolerations are resolved explicitly;
conflicting authored values return an error. The returned templates otherwise
precede workload projection and pod admission. Model readiness selectors remain
in the templates. Aggregate hardware weights do not establish that these
scheduling constraints can be satisfied.

`ResolvedUnit.Check` verifies the identified read inputs. Resource versions and
AcceleratorClass status participate in that check, but do not change the demand
fingerprint by themselves. Configuration identity/content, selected accelerator
specifications, runtime/model inputs, RuntimeClasses, and rendered pod shapes do.

## Attributed accelerator demand

`Resolver.ResolveDemand` extends the rendered unit with the live ResourceFlavor
catalog and the local AcceleratorQuota root named by the caller. It requires a
live, identified root with no parent, and verifies its resource/flavor attribution
against the complete catalog. Quota amounts and current hardware totals do not
choose flavors or size per-replica demand. Unknown or ambiguous mappings return
no result. See the [capacity contract](../capacity/README.md) for label projection
and report completeness requirements.

`ResolvedDemand.Report` retains the root identity and resource version. Both the
root and complete flavor catalog participate in `Check`, including changes during
resolution. A root heartbeat invalidates the old observation but does not change
freshly resolved demand. Replacing or changing a flavor's labels changes the
mapping fingerprint, including flavors the service does not use.

Before authorizing a capacity plan, the caller must match this report identity
and version to the capacity reader's fleet evidence, revalidate dependencies and
registration identity, and prove the member's actual applied/admitted demand.
The resolver does not infer other admission mutations or prove that a member has
refreshed its configuration. Placement uses this resolver for member preflight,
full-home floors, and capacity demand. Local service reconciliation uses its
existing path.
