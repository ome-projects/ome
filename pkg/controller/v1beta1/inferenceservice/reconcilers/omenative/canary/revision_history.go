package canary

import (
	"context"

	appsv1 "k8s.io/api/apps/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// memberRevisionHistory lists the ControllerRevisions minted for one member:
// every revision under the member's replica prefix and Component, whichever
// object controls it. The retention sweep keeps a revision while a live name
// (the replica's pair or an Instance row) carries it, and a configured count
// of the newest ones after that, so a revision missing here is one no
// Instance of the member runs.
func memberRevisionHistory(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType) ([]appsv1.ControllerRevision, error) {
	list := &appsv1.ControllerRevisionList{}
	sel := labels.SelectorFromSet(labels.Set{
		constants.InferenceServicePodLabelKey: irprojector.RoleReplicaPrefix(isvc, comp),
		constants.OMEComponentLabel:           string(comp),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	})
	if err := reads.List(ctx, list, client.InNamespace(isvc.Namespace), client.MatchingLabelsSelector{Selector: sel}); err != nil {
		return nil, err
	}
	return list.Items, nil
}

// revisionRetained reports whether the member's history still carries a
// ControllerRevision with the hash. Read through the cache the answer can
// only lag a loss: a ControllerRevision is immutable from create, and one a
// record names was created before the record was written.
func revisionRetained(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) (bool, error) {
	history, err := memberRevisionHistory(ctx, reads, isvc, comp)
	if err != nil {
		return false, err
	}
	want := query.RevisionFromHash(hash)
	for i := range history {
		if query.RevisionOf(&history[i]).Same(want) {
			return true, nil
		}
	}
	return false, nil
}
