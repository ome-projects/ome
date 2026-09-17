package placement

import (
	"context"

	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

// componentIRStatuses fetches the authoritative InferenceReplica status for
// every declared component of isvc via the given workload-cluster reader. The
// derived ISVC and its per-component InferenceReplicas live on the same workload
// cluster, so `reads` must be that cluster's client. A missing IR yields a nil
// map entry, which the predicates treat as "no status yet" — the same way the
// pre-migration code treated an absent ISVC status copy. The authoritative IR
// status is the source of truth; the ISVC's mirrored LifecycleStatus is no
// longer read.
func componentIRStatuses(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService) (map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus, error) {
	out := make(map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus)
	for _, c := range declaredComponents(isvc) {
		// The predicates below inspect per-Instance rows, so the status is
		// read through the decoded accessor; a payload that cannot be decoded
		// is a read error and holds the placement like any other read failure.
		st, err := irprojector.DecodedComponentIRStatus(ctx, reads, isvc.Namespace, isvc.Name, c)
		if err != nil {
			return nil, err
		}
		out[c] = st
	}
	return out, nil
}

// instanceStatusReader pairs a member-cluster reader with this controller's
// row decoder so componentIRStatuses decodes under the configured bound.
func (r *Reconciler) instanceStatusReader(reads client.Reader) client.Reader {
	return irstatus.NewReader(reads, r.InstanceStatusDecoder)
}

// AnyInstanceAdmitted reports whether any component of the (derived) ISVC has at
// least one Instance that has left the Kueue admission gate. Retained as a
// low-level predicate (and for single-component callers); the placement
// winner uses AllComponentsAdmitted, which is correct for multi-component (PD)
// services. `statuses` is the authoritative per-component IR status assembled by
// componentIRStatuses.
func AnyInstanceAdmitted(statuses map[v1beta1.ComponentType]*v1beta1.InferenceReplicaStatus) bool {
	for _, st := range statuses {
		if componentHasAdmittedInstance(st) {
			return true
		}
	}
	return false
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
// (their pods cleared the Kueue gate) — the per-home admitted-replica count
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
