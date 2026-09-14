package runtimeinheritance

import (
	"context"
	"fmt"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// Descendants returns every runtime that inherits from root, directly or
// transitively, excluding root itself. An empty Namespace denotes a
// cluster-scoped ClusterServingRuntime, on root and in the result alike.
//
// This is the inverse of Resolve, and exists for event fan-out: because a
// runtime's effective spec is resolved by walking its chain on every read,
// an edit to root changes what every runtime below it renders, and each of
// those needs to be reconciled.
//
// Two deliberate approximations, both chosen so the traversal can only ever
// return too much. A missed dependent leaves a stale workload running with
// no event left to correct it; a spurious one costs a reconcile that finds
// nothing to do.
//
//   - Edges match on the inherit-from name alone, ignoring the scope
//     shadowing Resolve applies (a same-namespace parent hides a
//     cluster-scoped one of the same name). Honouring it here would also
//     silently drop dependents at the moment a shadowing parent is deleted.
//   - Depth is uncapped. A chain longer than RuntimeInheritMaxDepth is
//     invalid, and its members need the reconcile that reports so.
//
// Scope is enforced where it cannot cause a miss: a ServingRuntime is only
// inheritable from within its own namespace, so a namespaced node never
// expands across namespaces and a namespaced root reaches nothing outside
// its own. A cluster-scoped root reaches both scopes.
//
// Traversal visits each runtime once, so a cycle terminates rather than
// hanging the event handler.
func Descendants(ctx context.Context, c client.Reader, root types.NamespacedName) ([]types.NamespacedName, error) {
	byParentName, err := childrenByParentName(ctx, c, root)
	if err != nil {
		return nil, err
	}

	visited := sets.New(root)
	queue := []types.NamespacedName{root}
	var out []types.NamespacedName
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		for _, child := range byParentName[current.Name] {
			if current.Namespace != "" && child.Namespace != current.Namespace {
				continue
			}
			if visited.Has(child) {
				continue
			}
			visited.Insert(child)
			out = append(out, child)
			queue = append(queue, child)
		}
	}
	return out, nil
}

// childrenByParentName indexes the runtimes that could appear below root
// under the parent name each declares, so the traversal costs a fixed
// number of List calls regardless of chain depth or width.
//
// Only the scopes root can actually reach are listed. A ClusterServingRuntime
// resolves its parent against cluster scope alone, so a namespaced root has
// no cluster-scoped children and needs no ClusterServingRuntime list; and
// since its descendants are all confined to its own namespace, its
// ServingRuntime list narrows to that namespace.
//
// The lists skip the cache's defensive deep copy: a ServingRuntimeSpec is
// large, and only name, namespace and annotations are read here. Nothing is
// mutated and nothing outlives the call — the keys built below hold copies
// of the two strings that matter.
func childrenByParentName(ctx context.Context, c client.Reader, root types.NamespacedName) (map[string][]types.NamespacedName, error) {
	byParentName := map[string][]types.NamespacedName{}

	if root.Namespace == "" {
		var csrs v1beta1.ClusterServingRuntimeList
		if err := c.List(ctx, &csrs, client.UnsafeDisableDeepCopy); err != nil {
			return nil, fmt.Errorf("list ClusterServingRuntimes: %w", err)
		}
		for i := range csrs.Items {
			if parent := csrs.Items[i].Annotations[constants.RuntimeInheritFromAnnotationKey]; parent != "" {
				byParentName[parent] = append(byParentName[parent], types.NamespacedName{Name: csrs.Items[i].Name})
			}
		}
	}

	srScope := []client.ListOption{client.UnsafeDisableDeepCopy}
	if root.Namespace != "" {
		srScope = append(srScope, client.InNamespace(root.Namespace))
	}
	var srs v1beta1.ServingRuntimeList
	if err := c.List(ctx, &srs, srScope...); err != nil {
		return nil, fmt.Errorf("list ServingRuntimes: %w", err)
	}
	for i := range srs.Items {
		if parent := srs.Items[i].Annotations[constants.RuntimeInheritFromAnnotationKey]; parent != "" {
			byParentName[parent] = append(byParentName[parent], types.NamespacedName{
				Namespace: srs.Items[i].Namespace,
				Name:      srs.Items[i].Name,
			})
		}
	}

	return byParentName, nil
}
