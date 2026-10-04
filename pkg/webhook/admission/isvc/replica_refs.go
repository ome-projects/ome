package isvc

import (
	"context"
	"fmt"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
)

// replicaRefRoles is the order the referenced-form checks walk the roles.
var replicaRefRoles = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}

// validateReplicaRefs applies the referenced-form rules: the shape rules, the
// block fixed at create (oldIsvc is nil on create) and, on create, the live
// checks per named replica: it exists in the service's namespace, is
// standalone, serves the role's component, and no other InferenceService in
// the namespace names it. An update never reads: its references cannot have
// changed. The reads are live, yet two services naming one replica in the
// same moment can both pass; both then front it until the owner deletes one.
// A failed read is an internal error (500), as on the collision check. A
// validator without a Reader skips the live checks.
func (v *InferenceServiceValidator) validateReplicaRefs(ctx context.Context, oldIsvc, isvc *v1beta1.InferenceService) error {
	if err := validation.ValidateReplicaRefs(&isvc.Spec); err != nil {
		return err
	}
	if oldIsvc != nil {
		return validation.ValidateReplicaRefsUpdate(&oldIsvc.Spec, &isvc.Spec)
	}
	if isvc.Spec.ReplicaRefs == nil || v.Reader == nil {
		return nil
	}
	referrers, err := v.replicaReferrers(ctx, isvc)
	if err != nil {
		return err
	}
	for _, c := range replicaRefRoles {
		name := validation.ReferencedReplica(&isvc.Spec, c)
		if name == "" {
			continue
		}
		if err := v.validateReferencedReplica(ctx, isvc, c, name, referrers[name]); err != nil {
			return err
		}
	}
	return nil
}

// validateReferencedReplica reads the replica a role names and checks that
// it exists, is standalone and serves the role's component; referrer is the
// other InferenceService in the namespace that already names it, or "".
func (v *InferenceServiceValidator) validateReferencedReplica(ctx context.Context, isvc *v1beta1.InferenceService, c v1beta1.ComponentType, name, referrer string) error {
	field := validation.ReplicaRefsField(c)
	ir := &v1beta1.InferenceReplica{}
	err := v.Reader.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: name}, ir)
	switch {
	case apierrors.IsNotFound(err):
		return fmt.Errorf("%s names InferenceReplica %s/%s, which does not exist; create it first", field, isvc.Namespace, name)
	case err != nil:
		return apierrors.NewInternalError(fmt.Errorf("read InferenceReplica %s/%s: %v", isvc.Namespace, name, err))
	}
	if parent := validation.ProjectedBy(ir); parent != "" {
		return fmt.Errorf("%s names InferenceReplica %s/%s, which InferenceService %s projects; name a standalone replica", field, isvc.Namespace, name, parent)
	}
	if ir.Spec.Component != c {
		return fmt.Errorf("%s names InferenceReplica %s/%s, whose component is %s", field, isvc.Namespace, name, ir.Spec.Component)
	}
	if referrer != "" {
		return fmt.Errorf("%s names InferenceReplica %s/%s, which InferenceService %s already fronts; a replica has one service", field, isvc.Namespace, name, referrer)
	}
	return nil
}

// replicaReferrers maps each replica another InferenceService of the
// namespace fronts to that service. The list is live and the namespace is
// the whole search space, since a service names replicas of its namespace
// only; the first service in list order wins a replica named twice.
func (v *InferenceServiceValidator) replicaReferrers(ctx context.Context, isvc *v1beta1.InferenceService) (map[string]string, error) {
	list := &v1beta1.InferenceServiceList{}
	if err := v.Reader.List(ctx, list, client.InNamespace(isvc.Namespace)); err != nil {
		return nil, apierrors.NewInternalError(fmt.Errorf("list InferenceServices in %s: %v", isvc.Namespace, err))
	}
	referrers := map[string]string{}
	for i := range list.Items {
		other := &list.Items[i]
		if other.Name == isvc.Name {
			continue
		}
		for _, c := range replicaRefRoles {
			name := validation.ReferencedReplica(&other.Spec, c)
			if _, taken := referrers[name]; name != "" && !taken {
				referrers[name] = other.Name
			}
		}
	}
	return referrers, nil
}
