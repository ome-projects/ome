// Package drain reads a Service's EndpointSlices to answer, for one pod,
// whether it is still in traffic: IsPodDrained (safe to delete or
// patch), IsPodRouted (still published) and IsPodInRotation (eligible
// for new traffic).
//
// Boundary: pure leaf. Only Kubernetes API machinery. Knows no
// OMENative-specific shape — every entry point takes a plain
// client.Reader + (namespace, serviceName, pod).
package drain

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsPodDrained reports whether pod is no longer routable through
// serviceName: no endpoint of the Service's EndpointSlices names the pod
// with Conditions.Ready true. Once true, kube-proxy has removed the pod's
// address or marked it not ready, and new connections to serviceName
// stop landing on the pod. Connections already open and the runtime's
// own shutdown are the caller's, through the pod's termination grace.
//
// With no EndpointSlice at all the answer is read from the Service: a
// Service that does not exist routes nothing, so the pod is drained; a
// Service whose slices are not written yet may still route to the pod,
// so it is not.
//
// reader is a client.Reader so a caller may pass the live apiserver
// reader when cache lag would declare a drain done too early. The cached
// client works as well and is what tests use.
func IsPodDrained(ctx context.Context, reader client.Reader, namespace, serviceName string, pod *corev1.Pod) (bool, error) {
	return NewBatcher(reader, namespace).IsPodDrained(ctx, serviceName, pod)
}

// IsPodInRotation reports whether serviceName publishes pod as an
// endpoint eligible for new traffic: Conditions.Ready true and not
// Terminating. A terminating endpoint does not count: kube-proxy keeps it
// for the requests in flight and sends it no new ones, so a replacement
// confirmed through it would take over nothing.
func IsPodInRotation(ctx context.Context, reader client.Reader, namespace, serviceName string, pod *corev1.Pod) (bool, error) {
	return NewBatcher(reader, namespace).IsPodInRotation(ctx, serviceName, pod)
}

// Batcher reads each Service's EndpointSlices once and answers every
// pod's drain, routed and rotation readings from that one read, so the
// pods of one pass are all read against the same endpoints. The list and
// its error are held for the Batcher's lifetime; so is the Service
// lookup an empty list needs. Not safe for concurrent use; scoped to one
// reconcile pass.
type Batcher struct {
	reader    client.Reader
	namespace string
	// endpoints is each Service's EndpointSlice list as read this pass,
	// indexed by the pods it names.
	endpoints map[string]serviceEndpoints
	// absent is the drain gate's answer for each Service that had no
	// EndpointSlice when it was read.
	absent map[string]serviceAbsence
}

// serviceEndpoints is one Service's EndpointSlice list as read this pass,
// or the error the read returned.
type serviceEndpoints struct {
	index endpointIndex
	empty bool
	err   error
}

// serviceAbsence is the drain gate's answer for a Service with no
// EndpointSlice: drained when the Service is gone, not drained while it
// exists; or the error the lookup returned.
type serviceAbsence struct {
	drained bool
	err     error
}

// NewBatcher returns a Batcher bound to reader and namespace.
func NewBatcher(reader client.Reader, namespace string) *Batcher {
	return &Batcher{
		reader:    reader,
		namespace: namespace,
		endpoints: map[string]serviceEndpoints{},
		absent:    map[string]serviceAbsence{},
	}
}

// IsPodDrained reports whether pod is no longer routable through
// serviceName, off the Batcher's one read of the Service. The answer is
// the package-level IsPodDrained's.
func (b *Batcher) IsPodDrained(ctx context.Context, serviceName string, pod *corev1.Pod) (bool, error) {
	if err := requireTarget("IsPodDrained", serviceName, pod); err != nil {
		return false, err
	}
	endpoints := b.read(ctx, serviceName)
	if endpoints.err != nil {
		return false, endpoints.err
	}
	if endpoints.empty {
		absence := b.readAbsence(ctx, serviceName)
		if absence.err != nil {
			return false, absence.err
		}
		return absence.drained, nil
	}
	return !endpoints.index.ready.contains(pod), nil
}

// IsPodRouted reports whether pod is still published through
// serviceName, off the same read as the drain gate.
//
// An empty slice list is not a reading here. The drain gate resolves it
// against the Service's existence, the right answer for a delete gate:
// no Service, no traffic path, safe to proceed. For an observation of
// the pod, a Service that is missing or not yet written is a fact about
// the Service and says nothing about whether the pod stopped serving, so
// this view reports the pod routed.
func (b *Batcher) IsPodRouted(ctx context.Context, serviceName string, pod *corev1.Pod) (bool, error) {
	if err := requireTarget("IsPodRouted", serviceName, pod); err != nil {
		return false, err
	}
	endpoints := b.read(ctx, serviceName)
	if endpoints.err != nil {
		return false, endpoints.err
	}
	if endpoints.empty {
		return true, nil
	}
	return endpoints.index.ready.contains(pod), nil
}

// IsPodInRotation reports whether serviceName publishes pod as an
// endpoint eligible for new traffic, Ready and not Terminating, off the
// same read as the drain gate. With no slice naming the pod, the Service
// absent or the endpoints controller behind, no new request reaches the
// pod through serviceName and it is not in rotation.
func (b *Batcher) IsPodInRotation(ctx context.Context, serviceName string, pod *corev1.Pod) (bool, error) {
	if err := requireTarget("IsPodInRotation", serviceName, pod); err != nil {
		return false, err
	}
	endpoints := b.read(ctx, serviceName)
	if endpoints.err != nil {
		return false, endpoints.err
	}
	return endpoints.index.available.contains(pod), nil
}

// RoutesNothing reports whether serviceName carries no traffic path at
// all: no EndpointSlice is labelled for it and the Service does not exist,
// the shape of a replica whose per-revision routed Service no
// InferenceService controller creates. It is the drain gate's absence
// rule asked on its own, for a reader that must see a pod published
// before it acts: when IsPodInRotation says no, the reader goes on only
// when there is no Service for the pod to be published in. A Service that
// exists with no slice yet is one the endpoints controller is behind on,
// and routes.
func (b *Batcher) RoutesNothing(ctx context.Context, serviceName string) (bool, error) {
	if serviceName == "" {
		return false, fmt.Errorf("RoutesNothing: empty serviceName")
	}
	endpoints := b.read(ctx, serviceName)
	if endpoints.err != nil {
		return false, endpoints.err
	}
	if !endpoints.empty {
		return false, nil
	}
	absence := b.readAbsence(ctx, serviceName)
	if absence.err != nil {
		return false, absence.err
	}
	return absence.drained, nil
}

// requireTarget is the argument check every reading starts with: a pod
// and a Service to read it against. reading names the caller in the error.
func requireTarget(reading, serviceName string, pod *corev1.Pod) error {
	if pod == nil {
		return fmt.Errorf("%s: nil pod", reading)
	}
	if serviceName == "" {
		return fmt.Errorf("%s: empty serviceName", reading)
	}
	return nil
}

// read lists the Service's EndpointSlices at most once per Batcher, the
// error included: a pass that could not read them must not read them
// again and answer differently halfway through. The list is indexed as
// it is read.
func (b *Batcher) read(ctx context.Context, serviceName string) serviceEndpoints {
	if endpoints, ok := b.endpoints[serviceName]; ok {
		return endpoints
	}
	slices, err := EndpointSlicesForService(ctx, b.reader, b.namespace, serviceName)
	endpoints := serviceEndpoints{err: err}
	if err == nil {
		endpoints.empty = len(slices) == 0
		endpoints.index = indexEndpoints(slices)
	}
	b.endpoints[serviceName] = endpoints
	return endpoints
}

// readAbsence resolves, at most once per Batcher, what an empty slice
// list means for the drain gate (drainedWhenSliceListEmpty).
func (b *Batcher) readAbsence(ctx context.Context, serviceName string) serviceAbsence {
	if absence, ok := b.absent[serviceName]; ok {
		return absence
	}
	drained, err := drainedWhenSliceListEmpty(ctx, b.reader, b.namespace, serviceName)
	absence := serviceAbsence{drained: drained, err: err}
	b.absent[serviceName] = absence
	return absence
}

// endpointIndex is the pods a Service's endpoints name, in the two
// senses the readings ask: ready is every pod kube-proxy still routes
// to, a terminating one included; available is every pod eligible for
// new traffic, Ready and not Terminating. available is a subset of ready.
type endpointIndex struct {
	ready     podTargets
	available podTargets
}

// indexEndpoints reads every endpoint of slices once into the index.
func indexEndpoints(slices []discoveryv1.EndpointSlice) endpointIndex {
	index := endpointIndex{ready: newPodTargets(), available: newPodTargets()}
	for i := range slices {
		for j := range slices[i].Endpoints {
			ep := &slices[i].Endpoints[j]
			if !endpointIsReady(*ep) {
				continue
			}
			index.ready.add(ep.TargetRef)
			if EndpointAvailable(*ep) {
				index.available.add(ep.TargetRef)
			}
		}
	}
	return index
}

// podTargets is a set of pods named by endpoint TargetRefs. A TargetRef
// with no namespace names a pod of that name in any namespace; one with
// a namespace names exactly that pod. The TargetRef is the pod's stable
// identity, where an address can be reused by a later pod.
type podTargets struct {
	exact        map[client.ObjectKey]struct{}
	anyNamespace map[string]struct{}
}

func newPodTargets() podTargets {
	return podTargets{
		exact:        map[client.ObjectKey]struct{}{},
		anyNamespace: map[string]struct{}{},
	}
}

// add records the pod ref names; a ref of another kind names none.
func (t podTargets) add(ref *corev1.ObjectReference) {
	if ref == nil || (ref.Kind != "" && ref.Kind != "Pod") {
		return
	}
	if ref.Namespace == "" {
		t.anyNamespace[ref.Name] = struct{}{}
		return
	}
	t.exact[client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}] = struct{}{}
}

// contains reports whether some endpoint named pod.
func (t podTargets) contains(pod *corev1.Pod) bool {
	if _, ok := t.anyNamespace[pod.Name]; ok {
		return true
	}
	_, ok := t.exact[client.ObjectKeyFromObject(pod)]
	return ok
}

// drainedWhenSliceListEmpty is the drain gate's answer for a Service
// with no EndpointSlice: a Service that does not exist routes nothing,
// so the pod is drained; a Service that exists may have slices the
// endpoints controller or the cache has not written yet, so the pod is
// not drained and the caller comes back.
func drainedWhenSliceListEmpty(ctx context.Context, reader client.Reader, namespace, serviceName string) (bool, error) {
	svc := &corev1.Service{}
	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: serviceName}, svc)
	if apierrors.IsNotFound(err) {
		// A replica with no per-revision Service, such as a standalone
		// replica whose Services no InferenceService controller creates,
		// takes this branch on every drain.
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("drainedWhenSliceListEmpty: get service %s/%s: %w", namespace, serviceName, err)
	}
	return false, nil
}

// EndpointSlicesForService lists the EndpointSlices in namespace that
// belong to serviceName, by the kubernetes.io/service-name label the
// in-tree endpointslice controller writes on every slice it manages.
//
// The slices are the cache's own objects, not copies: every reader keeps
// the pod names it reads and writes nothing through them, so the cache is
// asked not to copy every endpoint on every read. A live reader answers the
// same request from the apiserver.
//
// Exported so status.AvailablePodSet, which folds rotation across every
// pod into a single map, reads the same label query.
func EndpointSlicesForService(ctx context.Context, reader client.Reader, namespace, serviceName string) ([]discoveryv1.EndpointSlice, error) {
	list := &discoveryv1.EndpointSliceList{}
	if err := reader.List(ctx, list,
		client.InNamespace(namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: serviceName},
		client.UnsafeDisableDeepCopy,
	); err != nil {
		return nil, fmt.Errorf("list EndpointSlices for service %s/%s: %w", namespace, serviceName, err)
	}
	return list.Items, nil
}

// endpointIsReady reports whether kube-proxy still routes Service
// traffic to the endpoint. A terminating endpoint with Ready true still
// counts, so a drain is not declared done while connections in flight
// are still routed. A nil Ready is read as ready, per the discovery/v1
// contract: producers should set Conditions.Ready, and a slice that
// omits it is routable, as kube-proxy reads it.
func endpointIsReady(ep discoveryv1.Endpoint) bool {
	if ep.Conditions.Ready == nil {
		return true
	}
	return *ep.Conditions.Ready
}

// EndpointAvailable reports whether the endpoint is eligible for new
// traffic: Ready true and Terminating not true. It drives the status
// AvailablePodCount counter and the rotation reading.
//
// Distinct from endpointIsReady because of the EndpointSlice tri-state:
// a terminating pod whose probes still pass reports Ready, Serving and
// Terminating all true. kube-proxy keeps it for the requests in flight
// and sends it no new ones, so counting it Available would inflate
// AvailableReplicas, and a replacement confirmed through it would take
// over from a pod about to disappear. A nil Terminating is read as not
// terminating.
func EndpointAvailable(ep discoveryv1.Endpoint) bool {
	if !endpointIsReady(ep) {
		return false
	}
	if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
		return false
	}
	return true
}
