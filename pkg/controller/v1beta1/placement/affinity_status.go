package placement

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/placement/affinity"
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
	if condition.cond.Status == corev1.ConditionTrue && selectsByAffinity(source) {
		condition.cond = withAffinityMatchDiagnostics(condition.cond, source, clusters)
	}
	return context.WithValue(ctx, placementInputContextKey{}, placementInputSnapshot{
		source: client.ObjectKeyFromObject(source), uid: source.UID, generation: source.Generation, condition: condition,
	})
}

// selectsByAffinity reports whether the service names at least one
// clusterAffinity term under the policy that reads them.
func selectsByAffinity(source *v1beta1.InferenceService) bool {
	return source.Spec.Placement.UsesClusterAffinity() && len(source.Spec.Placement.ClusterAffinity) > 0
}

// withAffinityMatchDiagnostics adds to a valid intent's condition the
// clusterAffinity terms no registered WorkloadCluster matches. A selector
// that does not compile turns the condition invalid instead.
func withAffinityMatchDiagnostics(cond apis.Condition, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster) apis.Condition {
	selector, err := placementSelector(source)
	if err != nil {
		cond.Status = corev1.ConditionFalse
		cond.Reason = "InvalidPlacementIntent"
		cond.Message = err.Error()
		return cond
	}
	unmatched := unmatchedAffinityTerms(selector, len(source.Spec.Placement.ClusterAffinity), clusters)
	if len(unmatched) > 0 {
		cond.Message += fmt.Sprintf("; clusterAffinity terms %v match no registered WorkloadCluster (zero-based indexes)", unmatched)
	}
	return cond
}

// unmatchedAffinityTerms lists the zero-based indexes of the terms no
// registered cluster matches. A cluster being deleted is not registered.
func unmatchedAffinityTerms(selector *affinity.Selector, terms int, clusters []v1beta1.WorkloadCluster) []int {
	matched := make([]bool, terms)
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
	return unmatched
}

func placementInputForWrite(ctx context.Context, source *v1beta1.InferenceService) policyCondition {
	input, ok := ctx.Value(placementInputContextKey{}).(placementInputSnapshot)
	if ok && input.source == client.ObjectKeyFromObject(source) && input.uid == source.UID && input.generation == source.Generation {
		return input.condition
	}
	return placementInputCondition(source)
}
