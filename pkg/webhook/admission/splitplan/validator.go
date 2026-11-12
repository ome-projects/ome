// Package splitplan holds the control plane's InferenceService validator for
// placement mode Split.
package splitplan

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"k8s.io/apimachinery/pkg/api/equality"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement"
)

// ValidatorPath is the path the control plane serves the Split plan
// validator on.
const ValidatorPath = "/validate-ome-io-v1beta1-inferenceservice-splitplan"

// Validator refuses, on the control plane, a ClusterAffinity service
// in placement mode Split that the placement planner would hold: it projects
// the service onto every registered WorkloadCluster, as the placer does, and
// reports each distinct planner error. Replicas left unassigned because no
// registration matches are admitted, since registering a cluster later
// clears them. The check reads the live registrations; a failed read admits
// the service with a warning, so an unreachable API never blocks a write. A
// validator without a Reader admits every service.
type Validator struct {
	Reader client.Reader
}

var _ admission.Validator[*v1beta1.InferenceService] = &Validator{}

func (v *Validator) ValidateCreate(ctx context.Context, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	return v.validate(ctx, nil, isvc)
}

// ValidateUpdate admits a service already under deletion, and one whose spec
// did not change. Otherwise it ratchets: a stored service the planner already
// holds keeps accepting edits while it still cannot plan.
func (v *Validator) ValidateUpdate(ctx context.Context, oldIsvc, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	if !oldIsvc.DeletionTimestamp.IsZero() || equality.Semantic.DeepEqual(oldIsvc.Spec, isvc.Spec) {
		return nil, nil
	}
	return v.validate(ctx, oldIsvc, isvc)
}

func (v *Validator) ValidateDelete(_ context.Context, _ *v1beta1.InferenceService) (admission.Warnings, error) {
	return nil, nil
}

func (v *Validator) validate(ctx context.Context, oldIsvc, isvc *v1beta1.InferenceService) (admission.Warnings, error) {
	if v.Reader == nil || !plannedBySplit(isvc) {
		return nil, nil
	}
	list := &v1beta1.WorkloadClusterList{}
	if err := v.Reader.List(ctx, list); err != nil {
		if apimeta.IsNoMatchError(err) {
			return nil, nil
		}
		return admission.Warnings{fmt.Sprintf("the Split plan was not checked: list WorkloadClusters: %v", err)}, nil
	}
	if len(list.Items) == 0 {
		return nil, nil
	}
	errs := splitPlanErrors(isvc, list.Items)
	if len(errs) == 0 {
		return nil, nil
	}
	if oldIsvc != nil && plannedBySplit(oldIsvc) && len(splitPlanErrors(oldIsvc, list.Items)) > 0 {
		return nil, nil
	}
	return nil, fmt.Errorf("the placement planner would hold this Split plan across the %d registered WorkloadClusters: %s", len(list.Items), strings.Join(errs, "; "))
}

// plannedBySplit reports whether the placement planner divides the service's
// replicas across clusters: explicit ClusterAffinity placement in mode Split.
func plannedBySplit(isvc *v1beta1.InferenceService) bool {
	p := isvc.Spec.Placement
	return p != nil && p.Policy == v1beta1.PlacementPolicyClusterAffinity && p.Mode == v1beta1.PlacementModeSplit
}

// splitPlanErrors projects the service onto every registered cluster and
// returns each distinct planner error once; most concern the whole plan and
// repeat for every cluster.
func splitPlanErrors(isvc *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) []string {
	var errs []string
	for _, cluster := range clusters {
		_, err := placement.ProjectSplitReplicas(isvc, clusters, cluster.Name)
		var unassigned *placement.UnassignedReplicasError
		if err == nil || errors.As(err, &unassigned) {
			continue
		}
		if message := err.Error(); !slices.Contains(errs, message) {
			errs = append(errs, message)
		}
	}
	return errs
}
