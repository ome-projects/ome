package coordination

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"knative.dev/pkg/apis"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/drain"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// Reasons of the TrafficEndpointsReady condition.
const (
	// TrafficEndpointsWaiting: a revision has serving pods the endpoints
	// controller has not listed yet; the split withholds its share.
	TrafficEndpointsWaiting = "WaitingForEndpoints"
	// TrafficEndpointsListed: every serving pod is a Ready endpoint of its
	// revision's routed Service.
	TrafficEndpointsListed = "EndpointsListed"
)

// endpointWait is one revision whose serving pods are not all Ready
// endpoints of its routed Service yet.
type endpointWait struct {
	component v1beta1.ComponentType
	hash      string
	// service is the revision's routed Service, named once per revision
	// and read for every one of its pods.
	service string
	// ready counts the revision's routed pods that serve by the gate;
	// listed counts those the endpoints controller publishes as Ready.
	ready, listed int32
}

// servingPodsListedByRevision counts, per Component and revision, the pods
// the data plane can reach: podReadyAndServing holds and the Instance's
// routed pod is a Ready endpoint of the revision's routed Service, named
// once per revision and read once per Service through rotation. The
// endpoints controller lists a pod from the same PodReady the gate folds
// into, after a delay of its own, and no request reaches the pod before
// then; a share moved onto the revision earlier would go to a Service with
// nothing behind it. A pod the controller still lists after its gate went
// off is leaving and counts for nothing, as before. The waits name every
// revision with a serving pod still unlisted, in Component then revision
// order. pods are the observation's, the informer cache's own objects when
// the cached client listed them: the reading reads them and writes nothing
// through them, and keeps none past the pass.
func servingPodsListedByRevision(ctx context.Context, rotation *drain.Batcher, isvc *v1beta1.InferenceService, components []v1beta1.ComponentType, pods map[v1beta1.ComponentType][]*corev1.Pod) (map[v1beta1.ComponentType]map[string]int32, []endpointWait, error) {
	out := make(map[v1beta1.ComponentType]map[string]int32, len(components))
	var waits []endpointWait
	for _, c := range components {
		prefix := prefixFor(isvc, c)
		counts := map[string]*endpointWait{}
		kept := make([]*corev1.Pod, 0, len(pods[c]))
		for _, pod := range pods[c] {
			hash := pod.Labels[query.LabelRevisionHash]
			if hash == "" || !query.RoutedRunner(pod) || !podReadyAndServing(pod) {
				kept = append(kept, pod)
				continue
			}
			wait := counts[hash]
			if wait == nil {
				wait = &endpointWait{component: c, hash: hash, service: PerRevisionServiceName(prefix, c, hash)}
				counts[hash] = wait
			}
			listed, err := rotation.IsPodInRotation(ctx, wait.service, pod)
			if err != nil {
				return nil, nil, fmt.Errorf("read endpoints of %s revision %s: %w", c, hash, err)
			}
			wait.ready++
			if !listed {
				continue
			}
			wait.listed++
			kept = append(kept, pod)
		}
		out[c] = servingPodsByRevision(kept)
		for _, hash := range sortedKeys(counts) {
			if wait := counts[hash]; wait.listed < wait.ready {
				waits = append(waits, *wait)
			}
		}
	}
	return out, waits, nil
}

func sortedKeys(m map[string]*endpointWait) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// SetTrafficEndpointsReady writes the TrafficEndpointsReady condition:
// False naming every revision whose serving pods wait for the endpoints
// controller, True once none waits. A service that never waited carries
// no condition; the True side only resolves an earlier wait.
func SetTrafficEndpointsReady(isvc *v1beta1.InferenceService, waits []endpointWait) {
	if isvc == nil {
		return
	}
	condType := apis.ConditionType(v1beta1.TrafficEndpointsReady)
	if len(waits) == 0 {
		existing := isvc.Status.GetCondition(condType)
		if existing == nil || existing.Status == corev1.ConditionTrue {
			return
		}
		isvc.Status.SetCondition(condType, &apis.Condition{
			Type:   condType,
			Status: corev1.ConditionTrue,
			Reason: TrafficEndpointsListed,
		})
		return
	}
	isvc.Status.SetCondition(condType, &apis.Condition{
		Type:    condType,
		Status:  corev1.ConditionFalse,
		Reason:  TrafficEndpointsWaiting,
		Message: endpointWaitMessage(waits),
	})
}

// endpointWaitMessage spells the waits for an operator, one revision per
// clause: "engine revision abc123: waiting for endpoints: 2 pods Ready, 0 listed".
func endpointWaitMessage(waits []endpointWait) string {
	parts := make([]string, 0, len(waits))
	for _, w := range waits {
		parts = append(parts, fmt.Sprintf("%s revision %s: waiting for endpoints: %s Ready, %d listed",
			w.component, w.hash, podCount(w.ready), w.listed))
	}
	return strings.Join(parts, "; ")
}

func podCount(n int32) string {
	if n == 1 {
		return "1 pod"
	}
	return fmt.Sprintf("%d pods", n)
}
