// Package drain owns the EndpointSlice-convergence signals OMENative
// uses to gate destructive pod operations: IsPodDrained (safe to
// delete?) and IsPodInRotation (surge eligible for new traffic?).
//
// Boundary: pure leaf. Only Kubernetes API machinery. Knows nothing
// about ReconcileParams or any OMENative-specific shape — every entry
// point takes a plain client.Reader + (namespace, serviceName, pod).
package drain

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// IsPodDrained reports whether pod is no longer routable through serviceName.
// It returns true when, across every EndpointSlice for serviceName in the
// pod's namespace, either:
//   - no endpoint targets pod, or
//   - the endpoint that targets pod has Conditions.Ready != true.
//
// The returned bool is the EndpointSlice-convergence signal used during
// drain: once true, kube-proxy will have removed (or marked not-ready)
// the pod's address and new connections to serviceName stop landing on
// this pod. Existing connections and runtime-level shutdown are out of
// scope — the caller layers gracePeriodSeconds on top of this signal.
//
// reader is taken as client.Reader (not client.Client) so callers may
// pass a live API reader to bypass the cached controller-runtime read
// when cache lag would make a drain falsely declare done. The cached
// client works as well and is what tests use.
func IsPodDrained(ctx context.Context, reader client.Reader, namespace, serviceName string, pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("IsPodDrained: nil pod")
	}
	if serviceName == "" {
		return false, fmt.Errorf("IsPodDrained: empty serviceName")
	}

	slices, err := EndpointSlicesForService(ctx, reader, namespace, serviceName)
	if err != nil {
		return false, err
	}
	if len(slices) == 0 {
		// Distinguish "no Service at all" (drain trivially complete —
		// nothing routes to anything) from "Service exists but slices
		// haven't propagated yet" (cache cold-start, kube-controller-
		// manager lag, no matching pods yet). The latter looks the
		// same as the former without this disambiguation, which would
		// fail open and report drain done while a serving pod was
		// about to get an image patch or delete.
		return drainedWhenSliceListEmpty(ctx, reader, namespace, serviceName)
	}

	return podDrainedInSlices(slices, pod), nil
}

// Batcher records one drain observation per Service for a whole
// gang/drain loop. Each observation lists EndpointSlices once, indexes
// routable Pod target names, and caches the empty-slice Service lookup.
// Per-Pod checks are therefore O(1) after O(E) observation construction.
//
// Semantics are identical to IsPodDrained, including Ready=nil and
// empty TargetRef.Namespace handling. Results and read errors are held
// for the Batcher lifetime so every Pod in a wave is judged against the
// same observation.
//
// Not safe for concurrent use; scoped to one reconcile pass.
type Batcher struct {
	reader    client.Reader
	namespace string
	services  map[string]serviceDrainObservation
}

type serviceDrainObservation struct {
	hasEndpointSlices    bool
	drainedWithoutSlices bool
	routableTargets      routablePodTargets
	err                  error
}

// routablePodTargets mirrors endpointTargetsPod without retaining or scanning
// the complete EndpointSlice payload for every Pod check. A TargetRef with an
// empty Namespace matches a same-named Pod in any namespace.
type routablePodTargets struct {
	exactNamespace map[client.ObjectKey]struct{}
	anyNamespace   map[string]struct{}
}

// NewBatcher returns a Batcher bound to reader + namespace. Callers
// invoke IsPodDrained per pod; the underlying LIST runs at most once
// per distinct serviceName.
func NewBatcher(reader client.Reader, namespace string) *Batcher {
	return &Batcher{
		reader:    reader,
		namespace: namespace,
		services:  map[string]serviceDrainObservation{},
	}
}

func (b *Batcher) observeService(ctx context.Context, serviceName string) serviceDrainObservation {
	if observation, ok := b.services[serviceName]; ok {
		return observation
	}

	observation := serviceDrainObservation{}
	slices, err := EndpointSlicesForService(ctx, b.reader, b.namespace, serviceName)
	if err != nil {
		observation.err = err
		b.services[serviceName] = observation
		return observation
	}
	if len(slices) == 0 {
		observation.drainedWithoutSlices, observation.err = drainedWhenSliceListEmpty(ctx, b.reader, b.namespace, serviceName)
		b.services[serviceName] = observation
		return observation
	}

	observation.hasEndpointSlices = true
	observation.routableTargets = indexRoutablePodTargets(slices)
	b.services[serviceName] = observation
	return observation
}

// IsPodDrained reports whether pod is no longer routable through
// serviceName, reusing the memoized slice list. Identical semantics to
// the package-level IsPodDrained.
func (b *Batcher) IsPodDrained(ctx context.Context, serviceName string, pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("IsPodDrained: nil pod")
	}
	if serviceName == "" {
		return false, fmt.Errorf("IsPodDrained: empty serviceName")
	}
	observation := b.observeService(ctx, serviceName)
	if observation.err != nil {
		return false, observation.err
	}
	if !observation.hasEndpointSlices {
		return observation.drainedWithoutSlices, nil
	}
	return !observation.routableTargets.contains(pod), nil
}

func indexRoutablePodTargets(slices []discoveryv1.EndpointSlice) routablePodTargets {
	targets := routablePodTargets{
		exactNamespace: make(map[client.ObjectKey]struct{}),
		anyNamespace:   make(map[string]struct{}),
	}
	for i := range slices {
		for j := range slices[i].Endpoints {
			ep := &slices[i].Endpoints[j]
			if !endpointIsReady(*ep) || ep.TargetRef == nil {
				continue
			}
			ref := ep.TargetRef
			if ref.Kind != "" && ref.Kind != "Pod" {
				continue
			}
			if ref.Namespace == "" {
				targets.anyNamespace[ref.Name] = struct{}{}
				continue
			}
			targets.exactNamespace[client.ObjectKey{Namespace: ref.Namespace, Name: ref.Name}] = struct{}{}
		}
	}
	return targets
}

func (targets routablePodTargets) contains(pod *corev1.Pod) bool {
	if _, ok := targets.anyNamespace[pod.Name]; ok {
		return true
	}
	_, ok := targets.exactNamespace[client.ObjectKeyFromObject(pod)]
	return ok
}

// podDrainedInSlices is the package-level check used by IsPodDrained: a pod is
// drained when no endpoint targeting it across slices reports Ready. Assumes
// slices is non-empty; the caller owns empty-list disambiguation. Batcher builds
// the equivalent indexed representation once per Service.
func podDrainedInSlices(slices []discoveryv1.EndpointSlice, pod *corev1.Pod) bool {
	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
			if !endpointTargetsPod(ep, pod) {
				continue
			}
			if endpointIsReady(ep) {
				return false
			}
		}
	}
	return true
}

// drainedWhenSliceListEmpty resolves the ambiguous "no slices"
// observation by checking whether the Service itself exists. Service
// absent → drained (no traffic path). Service present → slices
// haven't been materialized yet → conservative: NOT drained, caller
// requeues.
func drainedWhenSliceListEmpty(ctx context.Context, reader client.Reader, namespace, serviceName string) (bool, error) {
	svc := &corev1.Service{}
	err := reader.Get(ctx, client.ObjectKey{Namespace: namespace, Name: serviceName}, svc)
	if apierrors.IsNotFound(err) {
		// No Service → no kube-proxy routing → drain is trivially
		// complete. With the services reconciler in place this branch
		// fires only when the Service hasn't been created yet on this
		// reconcile pass, which is a controller bug — the service
		// reconciliation runs before any op that needs drain.
		return true, nil
	}
	if err != nil {
		return false, fmt.Errorf("drainedWhenSliceListEmpty: get service %s/%s: %w", namespace, serviceName, err)
	}
	// Service exists, slice list empty. Could mean (a) no pods match
	// the selector yet, (b) kube-controller-manager hasn't created
	// slices yet, (c) informer cache cold-start. Any of those is a
	// transient state where we should not declare drain complete.
	return false, nil
}

// IsPodInRotation reports whether pod has at least one endpoint Ready
// AND non-terminating — eligible for NEW traffic. Migrate uses this to
// confirm the surge is receiving traffic before draining the source so
// the swap has no zero-routable-endpoint window. Terminating endpoints
// don't count: kube-proxy keeps them for in-flight requests but won't
// send new traffic, and swapping onto a terminating surge would be
// pointless.
func IsPodInRotation(ctx context.Context, reader client.Reader, namespace, serviceName string, pod *corev1.Pod) (bool, error) {
	if pod == nil {
		return false, fmt.Errorf("IsPodInRotation: nil pod")
	}
	if serviceName == "" {
		return false, fmt.Errorf("IsPodInRotation: empty serviceName")
	}
	slices, err := EndpointSlicesForService(ctx, reader, namespace, serviceName)
	if err != nil {
		return false, err
	}
	for _, slice := range slices {
		for _, ep := range slice.Endpoints {
			if !endpointTargetsPod(ep, pod) {
				continue
			}
			if EndpointAvailable(ep) {
				return true, nil
			}
		}
	}
	return false, nil
}

// EndpointSlicesForService lists EndpointSlices in namespace owned by
// serviceName via the standard kubernetes.io/service-name label that the
// in-tree endpointslice controller stamps on every slice it manages.
//
// Exported so status_aggregate's availablePodSet helper (which folds
// rotation across every pod into a single map) can reuse the same
// label query and slice walk.
func EndpointSlicesForService(ctx context.Context, reader client.Reader, namespace, serviceName string) ([]discoveryv1.EndpointSlice, error) {
	list := &discoveryv1.EndpointSliceList{}
	if err := reader.List(ctx, list,
		client.InNamespace(namespace),
		client.MatchingLabels{discoveryv1.LabelServiceName: serviceName},
	); err != nil {
		return nil, fmt.Errorf("list EndpointSlices for service %s/%s: %w", namespace, serviceName, err)
	}
	return list.Items, nil
}

// endpointTargetsPod matches an EndpointSlice endpoint to pod by TargetRef.
// TargetRef is preferred over Addresses[] because IP addresses can be
// reused across pods, while TargetRef carries the stable Pod identity.
func endpointTargetsPod(ep discoveryv1.Endpoint, pod *corev1.Pod) bool {
	ref := ep.TargetRef
	if ref == nil {
		return false
	}
	if ref.Kind != "" && ref.Kind != "Pod" {
		return false
	}
	if ref.Namespace != "" && ref.Namespace != pod.Namespace {
		return false
	}
	return ref.Name == pod.Name
}

// endpointIsReady reports whether the endpoint is currently receiving
// new Service traffic. Drives the drain wait: a pod is considered
// drained once every slice entry targeting it reports Ready=false.
// Terminating endpoints are deliberately considered "still receiving"
// when Ready=true so the controller doesn't declare drain complete
// while kube-proxy is still routing in-flight connections.
//
// A nil Ready pointer is treated as ready per the discovery/v1
// contract: producers SHOULD set Conditions.Ready, but if a slice
// omits it the endpoint is presumed routable, matching kube-proxy's
// behavior.
func endpointIsReady(ep discoveryv1.Endpoint) bool {
	if ep.Conditions.Ready == nil {
		return true
	}
	return *ep.Conditions.Ready
}

// EndpointAvailable reports whether the endpoint is eligible for NEW
// traffic — Ready=true AND Terminating!=true. Drives the status
// `AvailablePodCount` counter and `IsPodInRotation`'s surge check.
//
// Distinct from endpointIsReady because of the EndpointSlice
// tri-state contract: a terminating pod whose probes still pass
// reports Ready=true + Serving=true + Terminating=true. kube-proxy
// keeps it in rotation for in-flight requests but won't send NEW
// requests. Counting it as Available would inflate AvailableReplicas;
// treating it as in-rotation would let Migrate swap onto a pod that's
// about to disappear.
//
// A nil Terminating pointer is treated as not-terminating (the
// common steady-state shape).
func EndpointAvailable(ep discoveryv1.Endpoint) bool {
	if !endpointIsReady(ep) {
		return false
	}
	if ep.Conditions.Terminating != nil && *ep.Conditions.Terminating {
		return false
	}
	return true
}
