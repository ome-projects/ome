package components

import (
	"context"

	"github.com/pkg/errors"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/pdb"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// ReconcileOMENativePDB ensures the pod-level disruption budget for one
// OMENative Component using the committed InferenceReplica replica count.
func ReconcileOMENativePDB(
	ctx context.Context,
	b *BaseComponentFields,
	componentType v1beta1.ComponentType,
	ir *v1beta1.InferenceReplica,
	request pdb.Request,
) error {
	if request.Budget != nil {
		desiredPods, err := pdb.DesiredPodCount(ir)
		if err != nil {
			return errors.Wrapf(err, "compute desired pod count for OMENative %s", componentType)
		}
		normalizedBudget, err := pdb.NormalizeOMENativeBudget(request.Budget, desiredPods)
		if err != nil {
			return errors.Wrapf(err, "normalize OMENative PodDisruptionBudget for %s", componentType)
		}
		request.Budget = normalizedBudget
	}
	reader := b.APIReader
	if reader == nil {
		reader = b.Client
	}
	cutoverReady, err := pdb.OMENativeCutoverReady(ctx, reader, ir)
	if err != nil {
		return errors.Wrapf(err, "read OMENative cutover readiness for %s", componentType)
	}
	request.SelectorCutoverReady = cutoverReady

	_, err = pdb.NewPDBReconcilerWithReader(b.Client, reader, b.Scheme).Reconcile(
		ctx,
		request,
	)
	if err != nil {
		return errors.Wrapf(err, "reconcile OMENative PodDisruptionBudget for %s", componentType)
	}
	return nil
}

func omeNativeComponentSelector(isvc *v1beta1.InferenceService, componentType v1beta1.ComponentType) map[string]string {
	return omeNativePodSelector(isvc.Name, componentType)
}

// omeNativePodSelector selects the OMENative pods of one pod set by its
// replica's name prefix (the ome.io/inferenceservice label value) and role.
func omeNativePodSelector(podPrefix string, componentType v1beta1.ComponentType) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: podPrefix,
		constants.OMEComponentLabel:           string(componentType),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
}
