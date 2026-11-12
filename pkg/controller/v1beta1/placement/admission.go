package placement

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// componentIRStatuses reads decoded member status and verifies positive
// admission against live Pods. Missing components have no admission credit;
// unreadable inventories hold placement as unknown.
func componentIRStatuses(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService) (map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus, error) {
	out, claims, err := readComponentReplicas(ctx, reads, isvc)
	if err != nil {
		return nil, err
	}
	if len(claims) == 0 {
		return out, nil
	}
	// Pods are listed after every component snapshot, so a claimed cohort is
	// verified against an inventory at least as current as the claim.
	pods, err := listMemberPods(ctx, reads, isvc)
	if err != nil {
		return nil, err
	}
	for _, c := range declaredComponents(isvc) {
		ir, claimed := claims[c]
		if !claimed {
			continue
		}
		if out[c], err = verifiedComponentAdmission(ctx, reads, ir, pods); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// readComponentReplicas reads each declared component once and returns its
// status by component, nil for a component that does not exist, beside the
// replicas that claim admission or readiness. Such a claim is verified
// against the pod inventory, so the replica must name the service as its
// controller.
func readComponentReplicas(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService) (map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus, map[v1beta1.ComponentType]*v1beta1.InferenceReplica, error) {
	out := make(map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus)
	claims := map[v1beta1.ComponentType]*v1beta1.InferenceReplica{}
	for _, c := range declaredComponents(isvc) {
		// The predicates below inspect per-Instance rows, so the status is
		// read through the decoded accessor; a payload that cannot be decoded
		// is a read error and holds the placement like any other read failure.
		ir, _, err := irprojector.DecodedComponentIRFor(ctx, reads, isvc, c)
		if err != nil {
			return nil, nil, err
		}
		if ir == nil {
			out[c] = nil
			continue
		}
		out[c] = &ir.Status
		if !claimsAdmission(&ir.Status) {
			continue
		}
		if !controlledByService(ir, isvc) {
			return nil, nil, fmt.Errorf("component %q has unverified service ownership", ir.Name)
		}
		claims[c] = ir
	}
	return out, claims, nil
}

// claimsAdmission reports a status that credits an admitted Instance or a
// ready replica; the credit counts only once live pods confirm it.
func claimsAdmission(st *v1beta1.InferenceReplicaStatus) bool {
	return componentHasAdmittedInstance(st) || st.ReadyReplicas != 0
}

// controlledByService reports whether ir names isvc as its controller by UID,
// kind and API version. A service without a UID controls nothing.
func controlledByService(ir *v1beta1.InferenceReplica, isvc *v1beta1.InferenceService) bool {
	owner := metav1.GetControllerOf(ir)
	return isvc.UID != "" && owner != nil && owner.UID == isvc.UID &&
		owner.Kind == "InferenceService" && owner.APIVersion == v1beta1.SchemeGroupVersion.String()
}

// listMemberPods lists the service's pods on the member. A list the server
// cut short cannot verify any cohort and is refused.
func listMemberPods(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService) ([]corev1.Pod, error) {
	pods := &corev1.PodList{}
	if err := reads.List(ctx, pods, client.InNamespace(isvc.Namespace), client.MatchingLabels{constants.InferenceServicePodLabelKey: isvc.Name}); err != nil {
		return nil, err
	}
	if pods.Continue != "" {
		return nil, fmt.Errorf("member pod inventory is incomplete")
	}
	return pods.Items, nil
}

// verifiedComponentAdmission limits a claiming replica's status to the
// admission and readiness its own pods, complete gangs included, confirm.
func verifiedComponentAdmission(ctx context.Context, reads client.Reader, ir *v1beta1.InferenceReplica, pods []corev1.Pod) (*v1beta1.InferenceReplicaStatus, error) {
	owned := podsControlledBy(pods, ir)
	gangSizes, err := memberGangSizes(ctx, reads, ir, owned)
	if err != nil {
		return nil, err
	}
	return verifiedMemberAdmission(ir, owned, gangSizes)
}

// podsControlledBy returns the pods whose controller is ir.
func podsControlledBy(pods []corev1.Pod, ir *v1beta1.InferenceReplica) []corev1.Pod {
	owned := []corev1.Pod{}
	for _, pod := range pods {
		if owner := metav1.GetControllerOf(&pod); owner != nil && owner.UID == ir.UID {
			owned = append(owned, pod)
		}
	}
	return owned
}

// instanceStatusReader pairs a member-cluster reader with this controller's
// row decoder so componentIRStatuses decodes under the configured bound.
func (r *Reconciler) instanceStatusReader(reads client.Reader) client.Reader {
	return irstatus.NewReader(reads, r.InstanceStatusDecoder)
}

// AllComponentsAdmitted reports whether EVERY component the derived ISVC declares
// in its spec has at least one admitted Instance. This is the win
// signal: a cluster wins only once Kueue has admitted all of the service's
// components there. For a PD service (engine + decoder) this prevents declaring a
// winner while the engine is admitted but the decoder is still gated — which
// would otherwise delete the losers prematurely and strand a half-placed
// service. A service with no declared components is never a winner. `statuses` is
// the authoritative per-component IR status assembled by componentIRStatuses.
func AllComponentsAdmitted(isvc *v1beta1.InferenceService, statuses map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus) bool {
	declared := declaredComponents(isvc)
	if len(declared) == 0 {
		return false
	}
	for _, comp := range declared {
		if !componentHasAdmittedInstance(statuses[comp]) {
			return false
		}
	}
	return true
}

// admittedReplicaCount returns how many instances in an IR status are admitted
// (their pods cleared the Kueue gate) — the per-member admitted-replica count
// Split accounts against the desired floor. Zero for a nil status.
func admittedReplicaCount(st *v1beta1.InferenceReplicaStatus) int32 {
	if st == nil {
		return 0
	}
	var n int32
	for i := range st.InstanceStatuses {
		if st.InstanceStatuses[i].Admitted {
			n++
		}
	}
	return n
}

// componentHasAdmittedInstance reports whether an authoritative IR status has at
// least one Instance that has left the Kueue admission gate. A nil status (IR
// not found yet) reports false.
func componentHasAdmittedInstance(st *v1beta1.InferenceReplicaStatus) bool {
	if st == nil {
		return false
	}
	for _, inst := range st.InstanceStatuses {
		if inst.Admitted {
			return true
		}
	}
	return false
}

// declaredComponents returns the components present on the ISVC spec.
func declaredComponents(isvc *v1beta1.InferenceService) []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	if isvc.Spec.Engine != nil {
		out = append(out, v1beta1.EngineComponent)
	}
	if isvc.Spec.Decoder != nil {
		out = append(out, v1beta1.DecoderComponent)
	}
	if isvc.Spec.Router != nil {
		out = append(out, v1beta1.RouterComponent)
	}
	return out
}
