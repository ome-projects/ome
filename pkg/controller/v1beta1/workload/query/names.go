package query

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/constants"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// boundedServiceName guarantees a Service name fits the DNS1035 label
// limit. Names within the limit pass through unchanged; longer names are
// truncated to a deterministic, collision-resistant `{hash}-{suffix}`
// form via the shared constants helper. Truncated names cannot be parsed
// back to their owner in the Service→owner event mappers, so those owners
// fall back to the timer-poll reconcile path — correct, just not
// event-driven.
func boundedServiceName(name string) string {
	return constants.TruncateNameWithMaxLength(name, validation.DNS1035LabelMaxLength)
}

// PodName returns the stable pod name for one ordinal of one Runner
// of one Instance — <isvc>-<comp>-<inst-idx>-<runner>-<ordinal>. Stable
// across reconciles so Create can dedup desired-vs-existing by name
// alone.
func PodName(isvc string, component workload.ComponentType, instanceIdx int32, runner string, ordinal int32) string {
	return fmt.Sprintf("%s-%s-%d-%s-%d", isvc, component, instanceIdx, runner, ordinal)
}

// HeadlessServiceName returns the per-Component headless service name —
// <isvc>-<comp>-headless. Used by every op to look up the service that
// publishes the pods it's about to mutate, and by status_aggregate to
// scan EndpointSlices for ready endpoints.
func HeadlessServiceName(isvc string, component workload.ComponentType) string {
	return boundedServiceName(fmt.Sprintf("%s-%s-headless", isvc, component))
}

// StableServiceName returns the per-Component stable (non-headless,
// non-revision-scoped) Service name — <isvc>-<comp>. Matches the
// Service-name convention RawDeployment / MultiNode use (see
// constants.{Engine,Decoder,Router}ServiceName) so the existing ingress
// strategies — which point HTTPRoute / Ingress backendRefs at
// <isvc>-<comp> — work for OMENative without a per-mode strategy.
//
// The Service selects all OMENative pods for the Component regardless
// of revision; it is the cross-revision aggregate ingress target. The
// per-revision Services (PerRevisionServiceName) remain available for
// the weighted-routing path.
func StableServiceName(isvc string, component workload.ComponentType) string {
	return fmt.Sprintf("%s-%s", isvc, component)
}

// PerRevisionServiceName returns the per-revision routed Service name
// for one (owner, component, revisionHash) tuple — format
// `<owner>-<component>-rev-<hash>`, bounded to the DNS1035 label limit.
// The coordination package delegates to this copy; both must produce
// identical names so dispatch-path lookups line up.
func PerRevisionServiceName(ownerName string, component workload.ComponentType, revisionHash string) string {
	return boundedServiceName(fmt.Sprintf("%s-%s-rev-%s", ownerName, component, revisionHash))
}

// PerRevisionHeadlessServiceName returns the per-revision headless
// Service name — `<owner>-<component>-rev-<hash>-headless`, bounded to
// the DNS1035 label limit. Bounding the full name (rather than
// appending `-headless` to the already-bounded routing name) keeps the
// headless name distinct from and independent of the routing name.
func PerRevisionHeadlessServiceName(ownerName string, component workload.ComponentType, revisionHash string) string {
	return boundedServiceName(fmt.Sprintf("%s-%s-rev-%s-headless", ownerName, component, revisionHash))
}

// InstanceSubdomain returns the per-Instance DNS subdomain stamped
// into the OME_INSTANCE_SUBDOMAIN env var. Format is
// <isvc>-<component>-<instance-idx>; today this is metadata-only — the
// Pod's Spec.Subdomain still points at HeadlessServiceName — but the
// env var carries the per-instance form so workload code can address
// its own Instance independently of the shared Service name.
func InstanceSubdomain(isvc string, component workload.ComponentType, instanceIdx int32) string {
	return fmt.Sprintf("%s-%s-%d", isvc, component, instanceIdx)
}

// PodGroupName returns the deterministic name of the scheduler-plugins
// PodGroup that gates one multi-pod Instance — <isvc>-<component>-<idx>,
// bounded to the 63-char label-value limit because the same string is
// stamped on every member pod as the LabelPodGroup value. Names within
// the limit match the InstanceSubdomain shape on purpose: operators
// inspecting `kubectl get pg <name>` can pivot directly to
// `kubectl get pods -l <subdomain>` without re-deriving the prefix.
// Single-pod Instances do NOT get a PodGroup; callers must check
// inst.TotalPods() > 1 before invoking the builder.
func PodGroupName(isvc string, component workload.ComponentType, instanceIdx int32) string {
	return constants.TruncateNameWithMaxLength(
		fmt.Sprintf("%s-%s-%d", isvc, component, instanceIdx),
		validation.LabelValueMaxLength)
}
