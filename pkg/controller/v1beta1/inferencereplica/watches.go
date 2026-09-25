package inferencereplica

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/workqueue"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// irKind is the Kind string on the controller owner reference the
// projector stamps on every Pod the IR-managed path creates. The pod
// event handler uses it to enqueue the owning InferenceReplica.
const irKind = "InferenceReplica"

// perRevisionServiceInfix is the segment PerRevisionServiceName inserts
// between the <isvc>-<component> prefix and the revision hash suffix
// (`<isvc>-<component>-rev-<hash>`). The EndpointSlice mapper strips it
// to recover the owning IR name.
const perRevisionServiceInfix = "-rev-"

// headlessServiceSuffix is the trailing segment of the per-Component
// headless Service name (`<isvc>-<component>-headless`).
const headlessServiceSuffix = "-headless"

// podGroupPredicate ignores gang-scheduler status churn while preserving
// changes that can affect reconciliation or complete terminal cleanup.
func podGroupPredicate() predicate.Predicate {
	return predicate.Funcs{
		CreateFunc: func(event.CreateEvent) bool { return true },
		UpdateFunc: func(e event.UpdateEvent) bool {
			if e.ObjectOld == nil || e.ObjectNew == nil {
				return true
			}
			return e.ObjectOld.GetGeneration() != e.ObjectNew.GetGeneration() ||
				!equality.Semantic.DeepEqual(e.ObjectOld.GetLabels(), e.ObjectNew.GetLabels()) ||
				!equality.Semantic.DeepEqual(e.ObjectOld.GetAnnotations(), e.ObjectNew.GetAnnotations()) ||
				!equality.Semantic.DeepEqual(e.ObjectOld.GetOwnerReferences(), e.ObjectNew.GetOwnerReferences()) ||
				!equality.Semantic.DeepEqual(e.ObjectOld.GetFinalizers(), e.ObjectNew.GetFinalizers()) ||
				!equality.Semantic.DeepEqual(e.ObjectOld.GetDeletionTimestamp(), e.ObjectNew.GetDeletionTimestamp()) ||
				podGroupVerdictChanged(e.ObjectOld, e.ObjectNew)
		},
		DeleteFunc:  func(event.DeleteEvent) bool { return true },
		GenericFunc: func(event.GenericEvent) bool { return false },
	}
}

// podGroupVerdictChanged reports whether a status update carries the one
// reading an Instance acts on: the gang scheduler's phase, which is what
// says a group has to be rebuilt. Everything else on the status is
// running member tallies nothing consults, and admitting those would
// wake the owner on every member transition.
func podGroupVerdictChanged(oldObj, newObj client.Object) bool {
	before, okOld := oldObj.(*schedulingv1alpha1.PodGroup)
	after, okNew := newObj.(*schedulingv1alpha1.PodGroup)
	if !okOld || !okNew {
		return false
	}
	return before.Status.Phase != after.Status.Phase
}

// EndpointSliceToIR maps an EndpointSlice for an OMENative drain Service
// (the per-Component headless Service or a per-revision routed Service)
// back to its owning InferenceReplica reconcile key.
//
// The SurgeThenDrain / RecreatePod drain step gates old-pod deletion on
// drain.IsPodDrained, which reads these Services' EndpointSlices. Without
// this watch the IR controller only re-observes kube-proxy convergence on
// the workloadops.UpdateRequeueInterval timer tick — turning every drain
// into a fixed poll-interval wait instead of an event-driven step. The
// ISVC controller already watches EndpointSlices (EndpointSliceToISVC),
// but that enqueues the ISVC, not the IR that actually runs the drain.
//
// Returns an empty slice for any slice that doesn't target an
// OMENative drain Service — the watch is unfiltered, so the mapper does
// all the filtering by name.
func EndpointSliceToIR(ctx context.Context, obj client.Object) []reconcile.Request {
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		return nil
	}
	serviceName := slice.Labels[discoveryv1.LabelServiceName]
	irName, ok := irNameFromDrainServiceName(serviceName)
	if !ok {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: slice.Namespace,
			Name:      irName,
		},
	}}
}

// irNameFromDrainServiceName parses an OMENative drain Service name into
// the owning IR name (`<isvc>-<component>`), or ok=false when the name
// isn't a recognized OMENative drain Service.
//
// Two shapes are recognized (query.HeadlessServiceName /
// query.PerRevisionServiceName):
//   - `<isvc>-<component>-headless`
//   - `<isvc>-<component>-rev-<hash>`
//
// Both reduce to `<isvc>-<component>`, which is exactly the IR name
// (irprojector.InferenceReplicaName is `<isvc>-<component>`). The
// `-<component>` suffix is matched against the known component-type set
// so an arbitrary `-headless` Service doesn't masquerade as
// OMENative-owned.
func irNameFromDrainServiceName(name string) (string, bool) {
	if name == "" {
		return "", false
	}
	var prefix string
	switch {
	case strings.HasSuffix(name, headlessServiceSuffix):
		prefix = strings.TrimSuffix(name, headlessServiceSuffix)
	case strings.Contains(name, perRevisionServiceInfix):
		// Revision hashes are hex (no `-rev-` token), so the FIRST
		// occurrence bounds the <isvc>-<component> prefix. The
		// component-suffix match below rejects any false positive.
		prefix = name[:strings.Index(name, perRevisionServiceInfix)]
	default:
		return "", false
	}
	for _, component := range []v1beta1.ComponentType{
		v1beta1.RouterComponent,
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
	} {
		suffix := "-" + string(component)
		if !strings.HasSuffix(prefix, suffix) {
			continue
		}
		// prefix is already `<isvc>-<component>` == the IR name; the
		// component-suffix match only validates it's OMENative-owned and
		// guards against an empty isvc segment.
		if strings.TrimSuffix(prefix, suffix) == "" {
			return "", false
		}
		return prefix, true
	}
	return "", false
}

// managedByOMENativePredicate keeps the pod watch to OMENative-managed
// pods only (`ome.io/managed-by=OMENative`), so non-OMENative pod churn
// in shared namespaces doesn't wake the handler.
//
// On Update it ADDITIONALLY drops events that don't touch any field the
// IR reconcile reacts to. At thousands of pods per cluster the kubelet emits a
// continuous stream of status heartbeats (timestamps, observedGeneration,
// resource metrics) whose only effect is bumping ResourceVersion;
// forwarding those produces a self-sustaining reconcile flood.
// ResourceVersionChangedPredicate can't help — RV always changes
// — so podReconcileRelevantChanged does a real field comparison.
// Create / Delete still always enqueue (they move the expectations
// counter and must reach the handler).
func managedByOMENativePredicate() predicate.Predicate {
	managed := func(obj client.Object) bool {
		return obj.GetLabels()[query.LabelManagedBy] == query.ManagedByOMENative
	}
	return predicate.Funcs{
		CreateFunc:  func(e event.CreateEvent) bool { return managed(e.Object) },
		DeleteFunc:  func(e event.DeleteEvent) bool { return managed(e.Object) },
		GenericFunc: func(e event.GenericEvent) bool { return managed(e.Object) },
		UpdateFunc: func(e event.UpdateEvent) bool {
			// Honor the managed-by gate on the new object first; if the
			// label was just stripped (old managed, new not) we still want
			// the transition to reach the handler so it can react.
			if !managed(e.ObjectNew) && !managed(e.ObjectOld) {
				return false
			}
			oldPod, ok1 := e.ObjectOld.(*corev1.Pod)
			newPod, ok2 := e.ObjectNew.(*corev1.Pod)
			if !ok1 || !ok2 {
				// Not a Pod pair (shouldn't happen on a Pod watch) — fail
				// open and enqueue rather than silently swallow.
				return true
			}
			return podReconcileRelevantChanged(oldPod, newPod)
		},
	}
}

// podReconcileRelevantChanged reports whether an old→new Pod transition
// touched any field the IR reconcile / workload dispatcher actually reads:
//
//   - Status.Phase — Instance phase aggregation.
//   - ContainersReady condition — CountReadyPods / AllPodsRuntimeReady.
//   - PodReady condition — the bar every path stamps Ready on
//     (query.PodSetPromotable). Kubelet folds the serving gate into it in a
//     status write that touches nothing else, so without this the promote
//     waits for the next poll instead of the observation.
//   - ome.io/serving condition — CountServingPods / RatioBalanced gate.
//   - ContainerStatuses / InitContainerStatuses — terminal-failure
//     detection (CrashLoopBackOff / ImagePullBackOff escalation).
//   - DeletionTimestamp — drain / recreate ordering.
//   - Spec.SchedulingGates (via types.PodAdmissionGated) — drives the
//     edge-triggered InstanceReadyTimeout park/restart on Kueue gate-exit
//     (a gate-removal update changes only SchedulingGates, Phase stays
//     Pending, so without this it would be dropped).
//   - Spec.NodeName — feeds the current scheduling and placement
//     observation (a scheduler bind sets NodeName with Phase still Pending).
//   - OwnerReferences — enqueueOwner target.
//   - The OMENative routing labels (managed-by, isvc, component,
//     instance-index, revision-hash) the handler keys the expectations
//     cache and per-revision convergence on.
//
// Heartbeat-only updates (timestamps, hostIP/podIP refresh, resource
// metrics, observedGeneration) leave all of these untouched and are
// dropped. A miss only ever delays a reconcile until the next genuine
// change or the periodic resync, never drops state permanently.
func podReconcileRelevantChanged(oldPod, newPod *corev1.Pod) bool {
	if oldPod == nil || newPod == nil {
		return true
	}
	if oldPod.Status.Phase != newPod.Status.Phase {
		return true
	}
	if (oldPod.DeletionTimestamp == nil) != (newPod.DeletionTimestamp == nil) {
		return true
	}
	if workloadtypes.PodAdmissionGated(oldPod) != workloadtypes.PodAdmissionGated(newPod) {
		return true
	}
	if oldPod.Spec.NodeName != newPod.Spec.NodeName {
		return true
	}
	if podreadiness.IsContainersReady(oldPod) != podreadiness.IsContainersReady(newPod) {
		return true
	}
	if podreadiness.IsPodReady(oldPod) != podreadiness.IsPodReady(newPod) {
		return true
	}
	if podreadiness.IsServing(oldPod) != podreadiness.IsServing(newPod) {
		return true
	}
	if containerStatusesRelevantChanged(oldPod.Status.ContainerStatuses, newPod.Status.ContainerStatuses) {
		return true
	}
	if containerStatusesRelevantChanged(oldPod.Status.InitContainerStatuses, newPod.Status.InitContainerStatuses) {
		return true
	}
	if !equality.Semantic.DeepEqual(oldPod.OwnerReferences, newPod.OwnerReferences) {
		return true
	}
	return routingLabelsChanged(oldPod, newPod)
}

// containerStatusesRelevantChanged reports whether old→new container
// statuses differ in any field the IR reconcile / workload dispatcher
// actually reads. It replaces a reflection-based DeepEqual over the whole
// ContainerStatus slice — which walks ImageID, ContainerID, Resources,
// Started, and every nested timestamp that churns on each kubelet
// heartbeat — with a compare over only the read fields. The full set the
// workload package reads (and why each matters for an enqueue):
//
//   - Name — the matching key; also read by termination + image-diff.
//   - Image — in-place update convergence: ops.podRuntimeImagesMatch
//     declares an in-place image roll done only once kubelet stamps the
//     new image into Status.ContainerStatuses[*].Image, so that flip MUST
//     wake the reconcile or the rollout stalls until the periodic resync.
//   - State.Waiting (Reason+Message) — terminal-failure / stuck-pod
//     escalation (evidence.PodStuckInTerminalWaiting, types.PodTermination).
//   - State.Terminated (ExitCode+Reason+Message) — crash detection
//     (types.PodTermination).
//   - LastTerminationState.Terminated (ExitCode+Reason+Message) — a
//     CrashLoopBackOff pod shows its crash here while live State is
//     Waiting; types.nonZeroTerminated reads it.
//
// Fields deliberately NOT compared because nothing in the IR/workload
// reconcile reads them, and they're the heartbeat churn this avoids:
// ImageID, ContainerID, RestartCount, per-container Ready (readiness is
// taken from the ContainersReady pod condition, compared separately
// above), Resources, Started, and the Running/Terminated StartedAt/
// FinishedAt timestamps.
//
// A length or per-name mismatch (containers added/removed/reordered)
// always enqueues. A miss only ever delays a reconcile to the next
// genuine change or the resync, never drops state permanently.
func containerStatusesRelevantChanged(oldCS, newCS []corev1.ContainerStatus) bool {
	if len(oldCS) != len(newCS) {
		return true
	}
	oldByName := make(map[string]*corev1.ContainerStatus, len(oldCS))
	for i := range oldCS {
		oldByName[oldCS[i].Name] = &oldCS[i]
	}
	for i := range newCS {
		n := &newCS[i]
		o, ok := oldByName[n.Name]
		if !ok {
			return true
		}
		if o.Image != n.Image {
			return true
		}
		if waitingRelevantChanged(o.State.Waiting, n.State.Waiting) {
			return true
		}
		if terminatedRelevantChanged(o.State.Terminated, n.State.Terminated) {
			return true
		}
		if terminatedRelevantChanged(o.LastTerminationState.Terminated, n.LastTerminationState.Terminated) {
			return true
		}
	}
	return false
}

// waitingRelevantChanged compares the Waiting fields the escalator reads
// (Reason for terminal-state classification, Message for the LastFailure
// diagnostic record).
func waitingRelevantChanged(a, b *corev1.ContainerStateWaiting) bool {
	if (a == nil) != (b == nil) {
		return true
	}
	if a == nil {
		return false
	}
	return a.Reason != b.Reason || a.Message != b.Message
}

// terminatedRelevantChanged compares the Terminated fields the failure
// extractor reads (ExitCode + Reason + Message). StartedAt/FinishedAt and
// the signal/containerID are intentionally ignored.
func terminatedRelevantChanged(a, b *corev1.ContainerStateTerminated) bool {
	if (a == nil) != (b == nil) {
		return true
	}
	if a == nil {
		return false
	}
	return a.ExitCode != b.ExitCode || a.Reason != b.Reason || a.Message != b.Message
}

// routingLabelsChanged compares only the OMENative labels the handler
// routes / keys on. Comparing the full label map would re-flood on
// orthogonal label churn (e.g. a node-selector relabel) the reconcile
// doesn't care about.
func routingLabelsChanged(oldPod, newPod *corev1.Pod) bool {
	for _, k := range routingLabelKeys {
		if oldPod.Labels[k] != newPod.Labels[k] {
			return true
		}
	}
	return false
}

// routingLabelKeys are the labels podReconcileRelevantChanged diffs — the
// tuple workloadKeyFromPod / enqueueOwner / per-revision convergence read.
var routingLabelKeys = []string{
	query.LabelManagedBy,
	constants.InferenceServicePodLabelKey,
	constants.OMEComponentLabel,
	query.LabelInstanceIdx,
	query.LabelRevisionHash,
}

// newPodEventHandler returns an EventHandler that updates the
// Expectations cache on pod adds/deletes BEFORE enqueueing the owning
// InferenceReplica. The order matters: if the enqueue fired first the
// next reconcile could read a stale expectation entry and stay parked
// behind the create/delete gate until the 2-minute TTL.
//
// The IR controller owns this handler because it dispatches the
// workload pipeline (ops.surgeUpdate / recreateUpdate / Migrate) whose
// destructive steps gate on ExpectationsCache().Satisfied(); without an
// observer feeding the SAME cache the dispatcher reads, those gates only
// release on the TTL and every surge/recreate stalls (Instance pinned at
// Phase=Updating, readyReplicas=0).
func newPodEventHandler(exp *workloadtypes.Expectations) handler.EventHandler {
	return &podEventHandler{expectations: exp}
}

type podEventHandler struct {
	expectations *workloadtypes.Expectations
}

func (h *podEventHandler) Create(ctx context.Context, evt event.TypedCreateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pod, ok := evt.Object.(*corev1.Pod)
	if !ok {
		return
	}
	h.observe(pod, true)
	h.enqueueOwner(pod, q)
}

func (h *podEventHandler) Update(ctx context.Context, evt event.TypedUpdateEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	// Update events don't move the expectations counter (creates/deletes
	// do). Still enqueue so per-pod transitions like ContainersReady=True
	// and serving-gate flips drive the owning IR reconcile forward.
	if pod, ok := evt.ObjectNew.(*corev1.Pod); ok {
		h.enqueueOwner(pod, q)
	}
}

func (h *podEventHandler) Delete(ctx context.Context, evt event.TypedDeleteEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	pod, ok := evt.Object.(*corev1.Pod)
	if !ok {
		return
	}
	h.observe(pod, false)
	h.enqueueOwner(pod, q)
}

func (h *podEventHandler) Generic(ctx context.Context, evt event.TypedGenericEvent[client.Object], q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	if pod, ok := evt.Object.(*corev1.Pod); ok {
		h.enqueueOwner(pod, q)
	}
}

// observe drives ObservedCreate / ObservedDelete on the expectations
// cache, keyed identically to the dispatcher's ExpectCreates /
// ExpectDeletes: (namespace, parent-ISVC name, component, instance).
// The parent-ISVC name is the pod's ome.io/inferenceservice label, which
// equals workload.ReconcileInput.Key.OwnerName on the IR path
// (inferencereplica/convert.go sets OwnerName = ParentRef.Name).
func (h *podEventHandler) observe(pod *corev1.Pod, isAdd bool) {
	isvc, component, idx, ok := workloadKeyFromPod(pod)
	if !ok {
		return
	}
	if isAdd {
		h.expectations.ObservedCreate(pod.Namespace, isvc, component, idx)
		return
	}
	h.expectations.ObservedDelete(pod.Namespace, isvc, component, idx)
}

// enqueueOwner pushes a reconcile request for the InferenceReplica
// controller-ref on the pod. Pods the IR-managed path creates carry the
// IR as their controller owner; pods owned by something else (e.g. the
// legacy ISVC-direct path) are skipped — observe already ran and is a
// harmless no-op against this controller's cache.
func (h *podEventHandler) enqueueOwner(obj client.Object, q workqueue.TypedRateLimitingInterface[reconcile.Request]) {
	for _, ref := range obj.GetOwnerReferences() {
		if ref.Controller == nil || !*ref.Controller {
			continue
		}
		if ref.Kind != irKind {
			continue
		}
		q.Add(reconcile.Request{
			NamespacedName: types.NamespacedName{
				Namespace: obj.GetNamespace(),
				Name:      ref.Name,
			},
		})
		return
	}
}

// workloadKeyFromPod pulls (parent-ISVC name, component, instanceIdx)
// from pod labels — the tuple the workload Expectations cache keys on.
// Returns ok=false when any required label is missing/unparsable.
func workloadKeyFromPod(pod *corev1.Pod) (string, workloadtypes.ComponentType, int32, bool) {
	if pod == nil || pod.Labels == nil {
		return "", "", 0, false
	}
	if pod.Labels[query.LabelManagedBy] != query.ManagedByOMENative {
		return "", "", 0, false
	}
	isvc := pod.Labels[constants.InferenceServicePodLabelKey]
	if isvc == "" {
		return "", "", 0, false
	}
	comp := pod.Labels[constants.OMEComponentLabel]
	if comp == "" {
		return "", "", 0, false
	}
	idx, ok := query.InstanceIdxFromLabels(pod)
	if !ok {
		return "", "", 0, false
	}
	return isvc, workloadtypes.ComponentType(comp), idx, true
}
