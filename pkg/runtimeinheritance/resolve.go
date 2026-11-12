package runtimeinheritance

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// RuntimeRef is the scope-agnostic slice of a runtime CR the resolver
// needs. Callers project their CR (cluster or namespaced) into this
// shape.
type RuntimeRef struct {
	Name       string
	Spec       *v1beta1.ServingRuntimeSpec
	ParentName string
}

// Fetcher looks up a parent runtime by name. Return ErrParentNotFound
// (or a wrapped error errors.Is matches) when the parent doesn't exist.
type Fetcher func(ctx context.Context, name string) (*RuntimeRef, error)

// ErrParentNotFound signals the named parent doesn't exist; the
// resolver wraps it as ParentNotFoundError.
var ErrParentNotFound = errors.New("parent runtime not found")

// ParentNotFoundError carries the missing parent name + chain walked.
type ParentNotFoundError struct {
	Parent string
	Chain  []string
}

func (e *ParentNotFoundError) Error() string {
	return fmt.Sprintf("inherit-from parent %q not found (chain so far: %v)", e.Parent, e.Chain)
}

// CycleError carries the chain that closed the cycle.
type CycleError struct {
	Cycle []string
}

func (e *CycleError) Error() string {
	return fmt.Sprintf("inheritance cycle detected: %v", e.Cycle)
}

// MaxDepthExceededError carries the chain walked up to the limit.
type MaxDepthExceededError struct {
	MaxDepth int
	Chain    []string
}

func (e *MaxDepthExceededError) Error() string {
	return fmt.Sprintf("inheritance chain exceeds max depth %d (chain: %v)", e.MaxDepth, e.Chain)
}

// Resolve walks start's inherit-from chain to its root via fetch and
// merges bottom-up. Returns the effective spec and the root-first
// chain of names walked.
func Resolve(ctx context.Context, start *RuntimeRef, fetch Fetcher, maxDepth int) (*v1beta1.ServingRuntimeSpec, []string, error) {
	if start == nil {
		return nil, nil, errors.New("runtimeinheritance.Resolve: start is nil")
	}

	// The walk collects the chain child first; the errors report it in
	// that order.
	visited := map[string]bool{start.Name: true}
	chain := []string{start.Name}
	specs := []*v1beta1.ServingRuntimeSpec{start.Spec}

	current := start
	for current.ParentName != "" {
		if len(chain) >= maxDepth {
			return nil, nil, &MaxDepthExceededError{MaxDepth: maxDepth, Chain: chain}
		}
		if visited[current.ParentName] {
			return nil, nil, &CycleError{Cycle: append(chain, current.ParentName)}
		}
		parent, err := fetch(ctx, current.ParentName)
		if err != nil {
			if errors.Is(err, ErrParentNotFound) {
				return nil, nil, &ParentNotFoundError{Parent: current.ParentName, Chain: chain}
			}
			return nil, nil, fmt.Errorf("fetch parent %q: %w", current.ParentName, err)
		}
		visited[parent.Name] = true
		chain = append(chain, parent.Name)
		specs = append(specs, parent.Spec)
		current = parent
	}

	// The merge runs root first, each child over the merged ancestors.
	slices.Reverse(chain)
	slices.Reverse(specs)

	effective := specs[0].DeepCopy()
	for i := 1; i < len(specs); i++ {
		merged, err := Merge(effective, specs[i])
		if err != nil {
			return nil, nil, fmt.Errorf("merge %s onto effective: %w", chain[i], err)
		}
		effective = merged
	}
	return effective, chain, nil
}
