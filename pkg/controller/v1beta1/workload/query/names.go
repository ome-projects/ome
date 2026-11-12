package query

import (
	"fmt"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/constants"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// boundedServiceName guarantees a Service name fits the DNS1035 label
// limit. Names within the limit pass through unchanged; longer names are
// truncated to a deterministic, collision-resistant `{hash}-{suffix}`
// form via the shared constants helper. A replica's headless Service
// resolves through its owner reference even when truncated; a truncated
// name cannot be parsed back to its owner, so a Service found only by
// name parse falls back to the timer-poll reconcile path — correct, just
// not event-driven.
func boundedServiceName(name string) string {
	return constants.TruncateNameWithMaxLength(name, validation.DNS1035LabelMaxLength)
}

// int32Digits is the most bytes the decimal form of an int32 takes, the
// sign included.
const int32Digits = len("-2147483648")

// PodName returns the stable pod name for one ordinal of one Runner
// of one Instance — <isvc>-<comp>-<inst-idx>-<runner>-<ordinal>. Stable
// across reconciles so Create can dedup desired-vs-existing by name
// alone. The Create pass reads every Instance's names on every pass, so
// the name is written into one allocation of its own length rather than
// formatted.
func PodName(isvc string, component workload.ComponentType, instanceIdx int32, runner string, ordinal int32) string {
	var indexBuf, ordinalBuf [int32Digits]byte
	index := strconv.AppendInt(indexBuf[:0], int64(instanceIdx), 10)
	ordinalDigits := strconv.AppendInt(ordinalBuf[:0], int64(ordinal), 10)
	var name strings.Builder
	name.Grow(len(isvc) + len(component) + len(index) + len(runner) + len(ordinalDigits) + 4)
	name.WriteString(isvc)
	name.WriteByte('-')
	name.WriteString(string(component))
	name.WriteByte('-')
	name.Write(index)
	name.WriteByte('-')
	name.WriteString(runner)
	name.WriteByte('-')
	name.Write(ordinalDigits)
	return name.String()
}

// PodNameIs reports whether name is PodName(isvc, component, instanceIdx,
// runner, ordinal), read piece by piece against the name as it stands. The
// Create pass compares every pod of a Component with the names its
// Instance asks for on every pass, so no name is built for the compare.
func PodNameIs(name, isvc string, component workload.ComponentType, instanceIdx int32, runner string, ordinal int32) bool {
	rest, ok := CutPodNamePrefix(name, isvc, component, instanceIdx)
	if !ok {
		return false
	}
	read, ok := PodNameOrdinal(rest, runner)
	return ok && read == ordinal
}

// CutPodNamePrefix cuts the part of a pod name every pod of an Instance
// shares, <isvc>-<component>-<index>-, off the front of name; what is left
// of a pod of the Instance is <runner>-<ordinal>.
func CutPodNamePrefix(name, isvc string, component workload.ComponentType, instanceIdx int32) (string, bool) {
	rest, ok := cutNamePart(name, isvc)
	if !ok {
		return "", false
	}
	if rest, ok = cutNamePart(rest, string(component)); !ok {
		return "", false
	}
	return cutNameNumber(rest, instanceIdx)
}

// PodNameOrdinal reads the ordinal off rest, what CutPodNamePrefix leaves
// of a pod name: ok exactly when rest is <runner>-<ordinal> with the
// ordinal spelled as PodName spells it.
func PodNameOrdinal(rest, runner string) (int32, bool) {
	digits, ok := cutNamePart(rest, runner)
	if !ok {
		return 0, false
	}
	return decimalInt32(digits)
}

// decimalInt32 reads text as the decimal PodName writes for an int32: a
// minus for a negative number, then digits with no leading zero, within
// the type.
func decimalInt32(text string) (int32, bool) {
	if len(text) > 0 && text[0] == '-' {
		value, ok := decimalDigits(text[1:])
		if !ok || value == 0 || value > 1<<31 {
			return 0, false
		}
		return int32(-value), true
	}
	return instanceIndex(text)
}

// cutNamePart cuts part and the '-' after it off the front of name.
func cutNamePart(name, part string) (string, bool) {
	if len(name) <= len(part) || name[:len(part)] != part || name[len(part)] != '-' {
		return "", false
	}
	return name[len(part)+1:], true
}

// cutNameNumber cuts the decimal form of number and the '-' after it off
// the front of name.
func cutNameNumber(name string, number int32) (string, bool) {
	var digits [int32Digits]byte
	text := strconv.AppendInt(digits[:0], int64(number), 10)
	if len(name) <= len(text) || name[:len(text)] != string(text) || name[len(text)] != '-' {
		return "", false
	}
	return name[len(text)+1:], true
}

// HeadlessServiceName returns the per-Component headless service name —
// <isvc>-<comp>-headless. Used by every op to look up the service that
// publishes the pods it's about to mutate, and by status/aggregate.go to
// scan EndpointSlices for ready endpoints.
func HeadlessServiceName(isvc string, component workload.ComponentType) string {
	return boundedServiceName(fmt.Sprintf("%s-%s-headless", isvc, component))
}

// PerRevisionServiceName returns the per-revision routed Service name
// for one (owner, component, revisionHash) tuple — format
// `<owner>-<component>-rev-<hash>`, bounded to the DNS1035 label limit.
// The coordination package delegates to this copy; both must produce
// identical names so dispatch-path lookups line up.
func PerRevisionServiceName(ownerName string, component workload.ComponentType, revisionHash string) string {
	return boundedServiceName(fmt.Sprintf("%s-%s-rev-%s", ownerName, component, revisionHash))
}

// ControllerRevisionName is the name of the ControllerRevision a workload
// mints for one template: `<owner>-<component>-<hash>`, the form
// RevisionHashFromControllerRevisionName reads the hash back from. Every
// composer of a revision name goes through here so a lookup by name and
// the name the replica wrote agree byte for byte.
func ControllerRevisionName(ownerName string, component workload.ComponentType, revisionHash string) string {
	return fmt.Sprintf("%s-%s-%s", ownerName, component, revisionHash)
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

// PodGroupIndex reads the Instance index back off a PodGroup name of the
// service and Component: ok exactly when name is PodGroupName(isvc,
// component, index) in its plain <isvc>-<component>-<index> form, the form
// every name within the label limit takes. A name the limit hashed,
// another service's or Component's name, and an index spelled with a sign
// or a leading zero read as none. The PodGroup pass reads it off every
// listed pod's label, so it allocates nothing.
func PodGroupIndex(name, isvc string, component workload.ComponentType) (int32, bool) {
	prefix := len(isvc) + 1 + len(component) + 1
	if len(name) > validation.LabelValueMaxLength || len(name) <= prefix ||
		name[:len(isvc)] != isvc || name[len(isvc)] != '-' ||
		name[len(isvc)+1:prefix-1] != string(component) || name[prefix-1] != '-' {
		return 0, false
	}
	return instanceIndex(name[prefix:])
}

// maxInstanceIndex is the largest index an int32 holds, as text.
const maxInstanceIndex = "2147483647"

// instanceIndex reads the decimal text of an Instance index as PodGroupName
// writes it: digits alone, no sign, no leading zero, within an int32.
func instanceIndex(text string) (int32, bool) {
	index, ok := decimalDigits(text)
	if !ok || index > 1<<31-1 {
		return 0, false
	}
	return int32(index), true
}

// decimalDigits reads text as a number: digits alone, no leading zero, at
// most as many as the largest int32 has.
func decimalDigits(text string) (int64, bool) {
	if text == "" || len(text) > len(maxInstanceIndex) || (len(text) > 1 && text[0] == '0') {
		return 0, false
	}
	var value int64
	for i := 0; i < len(text); i++ {
		digit := text[i]
		if digit < '0' || digit > '9' {
			return 0, false
		}
		value = value*10 + int64(digit-'0')
	}
	return value, true
}

// RoutedServiceForPod returns the per-revision *routed* Service name to
// gate drain against for pod. Read from the pod's ome.io/revision-hash
// label (stamped by Render). Empty string when the label is missing —
// caller treats that as "no routed Service to check" and proceeds
// straight to delete.
//
// We deliberately do NOT use the Component's headless Service here.
// That Service sets PublishNotReadyAddresses=true so peer-discovery DNS
// resolves before pods are Ready; kube-proxy then publishes the
// endpoint with Conditions.Ready=true regardless of the pod's actual
// Ready state, which would make drain.IsPodDrained wait forever. The
// per-revision routed Service (created by the coordination layer with
// PublishNotReadyAddresses=false) reflects the controller-owned
// ome.io/serving gate correctly.
//
// plan.Component type flows from the caller so migrate.go can address
// per-revision Services without importing v1beta1 itself.
func RoutedServiceForPod(ownerName string, component workload.ComponentType, pod *corev1.Pod) string {
	if pod == nil {
		return ""
	}
	hash := pod.Labels[LabelRevisionHash]
	if hash == "" {
		return ""
	}
	// Returning a routing-Service name for a worker would wedge the Migrate
	// surge in-rotation gate forever — IsPodInRotation(worker) can never
	// become true since the worker is not an endpoint — and is a no-op for
	// the source drain gate (a worker is never routed, so it's trivially
	// drained). Skip it so the gates assert only the routable leader; worker
	// readiness is already covered upstream by AllPodsRuntimeReady, and
	// worker availability by the headless-Service-based AvailablePodCount.
	if !RoutedRunner(pod) {
		return ""
	}
	return PerRevisionServiceName(ownerName, component, hash)
}

// RoutedRunner reports whether pod's runner can be a member of the
// per-revision ROUTING Service. For a multi-pod (gang) Component that
// Service pins runner=leader,pod-ordinal=0
// (coordination.BuildPerRevisionRoutingService): workers run
// distributed-init peers and never serve customer traffic, so a worker is
// never in rotation and its readiness says nothing about whether the
// Instance serves. A single-pod Instance's one runner is routed.
func RoutedRunner(pod *corev1.Pod) bool {
	return pod != nil && pod.Labels[LabelRunner] != workload.RunnerWorker
}
