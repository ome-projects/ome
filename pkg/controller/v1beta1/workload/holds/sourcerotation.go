package holds

import (
	"context"
	"sort"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// sourceUnroutedHold is the surge's report on its own source. A
// SurgeThenDrain cycle keeps the source in rotation until it reaches the
// drain step; that ordering is the no-downtime guarantee. So a source
// already out of rotation while the step is still Surge was taken out by
// something outside this rollout, and the Instance is serving nothing
// while its replacement comes up.
//
// The roll continues — the replacement is the only way back to capacity
// — but the state is named on the operation, and it stops the escalation
// pass from reading the pod set as healthy capacity.
//
// Only a row the update pass owns can report one: only a surge takes a
// source out.
var sourceUnroutedHold = authority{
	token:  types.WaitingReasonSourceUnrouted,
	mayOwn: updateOwnerOnly,
	report: reportSourceRotation,
}

func updateOwnerOnly(owner types.RowOwner, _ types.ComponentPlan) bool {
	return owner == types.OwnerUpdate
}

// reportSourceRotation reads the rotation of the sources a surge in
// flight is replacing. Nothing to report once the step has moved past
// Surge — that is where the surge takes the source out itself — or once
// at least one source is back in rotation: one routed source is enough,
// the Instance is taking traffic.
func reportSourceRotation(ctx context.Context, in PassInput, row Row) (reading, error) {
	op := row.Status.Operation
	if op == nil || op.Type != types.InstanceOperationUpdate || op.Step != types.UpdateStepSurge {
		return reading{release: true}, nil
	}
	own, _, err := in.rowPods(ctx, row)
	if err != nil {
		return reading{}, err
	}
	sources := sourcePods(own, op.TargetRevision)
	if len(sources) == 0 {
		return reading{release: true}, nil
	}
	var unrouted *corev1.Pod
	for _, pod := range sources {
		routed, err := sourceRouted(ctx, in, pod)
		if err != nil {
			return reading{}, err
		}
		if routed {
			return reading{release: true}, nil
		}
		if unrouted == nil {
			unrouted = pod
		}
	}
	return reading{waiting: true, evidence: types.SourceUnroutedTermination(unrouted)}, nil
}

// sourcePods are the Instance's pods that are NOT on the revision the
// surge in flight is committed to — the capacity the replacement is
// there to take over from.
//
// Ordered by NAME rather than by bucket order, which comes from an
// informer map iteration and differs between passes; naming a different
// pod every pass is exactly what the edge trigger exists to prevent.
func sourcePods(pods []*corev1.Pod, surgeTarget string) []*corev1.Pod {
	hash := query.RevisionFromName(surgeTarget).Hash()
	if hash == "" {
		return nil
	}
	var out []*corev1.Pod
	for _, pod := range pods {
		if pod != nil && pod.Labels[query.LabelRevisionHash] != hash {
			out = append(out, pod)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

// sourceRouted reports whether pod is still taking traffic, read through
// BOTH views the rollout owns: the lifecycle serving gate the controller
// writes, and the pod's endpoint in the per-revision routed Service,
// which is the same convergence signal the drain step waits on. Either
// view saying "out" is out — a gate still True with no routable endpoint
// is not capacity, and neither is the reverse.
//
// An absent Service, or a Service whose slices have not propagated, is
// NOT a reading: it is a fact about the Service and says nothing about
// whether the pod stopped serving. The endpoint view abstains there and
// the serving gate is the only one that speaks, so a cold start cannot
// be misreported as a source someone took out of rotation.
func sourceRouted(ctx context.Context, in PassInput, pod *corev1.Pod) (bool, error) {
	if pod == nil || pod.DeletionTimestamp != nil || !podreadiness.IsServing(pod) {
		return false, nil
	}
	serviceName := query.RoutedServiceForPod(in.Input.Key.OwnerName, in.Plan.Component, pod)
	if serviceName == "" {
		// No routed Service to read the pod's membership from; the gate is
		// the only view there is.
		return true, nil
	}
	return in.drainer.IsPodRouted(ctx, serviceName, pod)
}
