package placement

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

type placementInputContextKey struct{}

type placementInputSnapshot struct {
	source     types.NamespacedName
	uid        types.UID
	generation int64
	condition  policyCondition
}

// Match diagnostics describe registry membership, independently of member
// health and admission. An unmatched OR term does not invalidate the intent.
func withPlacementInputCondition(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) context.Context {
	condition := placementInputCondition(source)
	if condition.cond.Status == corev1.ConditionTrue && source.Spec.Placement.UsesClusterAffinity() && len(source.Spec.Placement.ClusterAffinity) > 0 {
		selector, err := placementSelector(source)
		if err != nil {
			condition.cond.Status = corev1.ConditionFalse
			condition.cond.Reason = "InvalidPlacementIntent"
			condition.cond.Message = err.Error()
		} else {
			matched := make([]bool, len(source.Spec.Placement.ClusterAffinity))
			for i := range clusters {
				if !clusters[i].DeletionTimestamp.IsZero() {
					continue
				}
				match, _ := selector.Match(&clusters[i])
				for _, index := range match.TermIndexes {
					matched[index] = true
				}
			}
			var unmatched []int
			for index, found := range matched {
				if !found {
					unmatched = append(unmatched, index)
				}
			}
			if len(unmatched) > 0 {
				condition.cond.Message += fmt.Sprintf("; clusterAffinity terms %v match no registered WorkloadCluster (zero-based indexes)", unmatched)
			}
		}
	}
	return context.WithValue(ctx, placementInputContextKey{}, placementInputSnapshot{
		source: client.ObjectKeyFromObject(source), uid: source.UID, generation: source.Generation, condition: condition,
	})
}

func placementInputForWrite(ctx context.Context, source *v1beta1.InferenceService) policyCondition {
	input, ok := ctx.Value(placementInputContextKey{}).(placementInputSnapshot)
	if ok && input.source == client.ObjectKeyFromObject(source) && input.uid == source.UID && input.generation == source.Generation {
		return input.condition
	}
	return placementInputCondition(source)
}
