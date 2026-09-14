// Package observation provides bounded, deterministic Kubernetes evidence
// collection for composite kubectl-ome diagnostics.
package observation

import (
	"context"
	"fmt"
	"sort"
	"sync"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"

	"sigs.k8s.io/ome/pkg/cli/paging"
)

// Collection is a bounded immutable-by-value snapshot of one Kubernetes
// resource kind.
type Collection[T any] struct {
	Items     []T
	Requests  int
	Truncated bool
}

// ObjectRef identifies one source whose Warning events should be observed.
type ObjectRef struct {
	Namespace string
	Kind      string
	Name      string
	UID       types.UID
	// Priority is applied before the deterministic identity order when a
	// target cap is reached. Larger values win; it does not affect identity.
	Priority int
}

// EventLimits bounds target fan-out, concurrent requests, and pagination for
// each target.
type EventLimits struct {
	Paging        paging.Limits
	MaxTargets    int
	MaxConcurrent int
}

// SourceFailure preserves an optional-source error without discarding
// evidence collected from other targets.
type SourceFailure struct {
	Target ObjectRef
	Err    error
}

// EventCollection is a deterministic aggregate of per-target Warning events.
type EventCollection struct {
	Items          []corev1.Event
	Requests       int
	Truncated      bool
	SkippedTargets int
	Failures       []SourceFailure
}

// CollectPods lists a service's pods once, with bounded typed pagination.
func CollectPods(ctx context.Context, pods coreclient.PodsGetter, namespace, labelSelector string, limits paging.Limits) (Collection[corev1.Pod], error) {
	selector, err := labels.Parse(labelSelector)
	if err != nil {
		return Collection[corev1.Pod]{Items: []corev1.Pod{}}, fmt.Errorf("parse pod label selector: %w", err)
	}
	result, err := paging.ListBounded(ctx, metav1.ListOptions{LabelSelector: labelSelector}, limits,
		func(requestCtx context.Context, opts metav1.ListOptions) (paging.Page[corev1.Pod], error) {
			list, listErr := pods.Pods(namespace).List(requestCtx, opts)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[corev1.Pod]{}, requestErr
			}
			if listErr != nil {
				return paging.Page[corev1.Pod]{}, listErr
			}
			if list == nil {
				return paging.Page[corev1.Pod]{}, fmt.Errorf("Pod list returned nil")
			}
			return paging.Page[corev1.Pod]{Items: list.Items, Continue: list.Continue}, nil
		})
	items := make([]corev1.Pod, 0, len(result.Items))
	for _, pod := range result.Items {
		if pod.Namespace == namespace && selector.Matches(labels.Set(pod.Labels)) {
			items = append(items, *pod.DeepCopy())
		}
	}
	collection := Collection[corev1.Pod]{
		Items:     items,
		Requests:  result.Pages,
		Truncated: result.Truncated,
	}
	sort.SliceStable(collection.Items, func(i, j int) bool {
		left, right := collection.Items[i], collection.Items[j]
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		return left.Name < right.Name
	})
	return collection, err
}

// CollectWarningEvents observes Warning events for a bounded target set.
// Individual source failures are returned in the collection; cancellation of
// the parent context aborts the entire operation.
func CollectWarningEvents(ctx context.Context, events coreclient.EventsGetter, targets []ObjectRef, limits EventLimits) (EventCollection, error) {
	result := EventCollection{
		Items:    []corev1.Event{},
		Failures: []SourceFailure{},
	}
	if limits.MaxTargets <= 0 {
		return result, fmt.Errorf("event target limit must be positive")
	}
	if limits.MaxConcurrent <= 0 {
		return result, fmt.Errorf("event concurrency limit must be positive")
	}
	for index, target := range targets {
		if target.Namespace == "" {
			return result, fmt.Errorf("event target %d namespace must not be empty", index)
		}
		if target.Kind == "" {
			return result, fmt.Errorf("event target %d kind must not be empty", index)
		}
		if target.Name == "" {
			return result, fmt.Errorf("event target %d name must not be empty", index)
		}
	}

	ordered := append([]ObjectRef(nil), targets...)
	sort.SliceStable(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		if left.Priority != right.Priority {
			return left.Priority > right.Priority
		}
		if left.Namespace != right.Namespace {
			return left.Namespace < right.Namespace
		}
		if left.Kind != right.Kind {
			return left.Kind < right.Kind
		}
		if left.Name != right.Name {
			return left.Name < right.Name
		}
		return left.UID < right.UID
	})
	unique := make([]ObjectRef, 0, len(ordered))
	seenTargets := make(map[objectRefIdentity]struct{}, len(ordered))
	for _, target := range ordered {
		identity := objectRefIdentity{Namespace: target.Namespace, Kind: target.Kind, Name: target.Name, UID: target.UID}
		if _, duplicate := seenTargets[identity]; duplicate {
			continue
		}
		seenTargets[identity] = struct{}{}
		unique = append(unique, target)
	}
	ordered = unique
	if len(ordered) > limits.MaxTargets {
		result.Truncated = true
		result.SkippedTargets = len(ordered) - limits.MaxTargets
		ordered = ordered[:limits.MaxTargets]
	}

	type targetResult struct {
		collection paging.Result[corev1.Event]
		err        error
	}
	collected := make([]targetResult, len(ordered))
	jobs := make(chan int)
	workerCount := limits.MaxConcurrent
	if workerCount > len(ordered) {
		workerCount = len(ordered)
	}
	var workers sync.WaitGroup
	workers.Add(workerCount)
	for range workerCount {
		go func() {
			defer workers.Done()
			for index := range jobs {
				collected[index].collection, collected[index].err = collectWarningEventsForTarget(ctx, events, ordered[index], limits.Paging)
			}
		}()
	}
	for index := range ordered {
		jobs <- index
	}
	close(jobs)
	workers.Wait()

	seen := make(map[string]struct{})
	for index, target := range ordered {
		observed := collected[index]
		result.Requests += observed.collection.Pages
		result.Truncated = result.Truncated || observed.collection.Truncated
		sort.SliceStable(observed.collection.Items, func(i, j int) bool {
			left, right := observed.collection.Items[i], observed.collection.Items[j]
			if left.Namespace != right.Namespace {
				return left.Namespace < right.Namespace
			}
			return left.Name < right.Name
		})
		for _, event := range observed.collection.Items {
			if !eventMatchesTarget(event, target) {
				continue
			}
			key := string(event.UID)
			if key == "" {
				key = event.Namespace + "/" + event.Name
			}
			if _, duplicate := seen[key]; duplicate {
				continue
			}
			seen[key] = struct{}{}
			result.Items = append(result.Items, *event.DeepCopy())
		}
		if observed.err != nil {
			result.Failures = append(result.Failures, SourceFailure{Target: target, Err: observed.err})
		}
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	return result, nil
}

func collectWarningEventsForTarget(ctx context.Context, events coreclient.EventsGetter, target ObjectRef, limits paging.Limits) (paging.Result[corev1.Event], error) {
	selectorFields := fields.Set{
		"involvedObject.kind": target.Kind,
		"involvedObject.name": target.Name,
		"type":                corev1.EventTypeWarning,
	}
	if target.UID != "" {
		selectorFields["involvedObject.uid"] = string(target.UID)
	}
	base := metav1.ListOptions{FieldSelector: selectorFields.AsSelector().String()}
	return paging.ListBounded(ctx, base, limits,
		func(requestCtx context.Context, opts metav1.ListOptions) (paging.Page[corev1.Event], error) {
			list, err := events.Events(target.Namespace).List(requestCtx, opts)
			if requestErr := requestCtx.Err(); requestErr != nil {
				return paging.Page[corev1.Event]{}, requestErr
			}
			if err != nil {
				return paging.Page[corev1.Event]{}, err
			}
			if list == nil {
				return paging.Page[corev1.Event]{}, fmt.Errorf("Event list returned nil")
			}
			return paging.Page[corev1.Event]{Items: list.Items, Continue: list.Continue}, nil
		})
}

func eventMatchesTarget(event corev1.Event, target ObjectRef) bool {
	if event.Namespace != target.Namespace || event.Type != corev1.EventTypeWarning || event.InvolvedObject.Kind != target.Kind || event.InvolvedObject.Name != target.Name {
		return false
	}
	if event.InvolvedObject.Namespace != "" && event.InvolvedObject.Namespace != target.Namespace {
		return false
	}
	return target.UID == "" || event.InvolvedObject.UID == target.UID
}

type objectRefIdentity struct {
	Namespace string
	Kind      string
	Name      string
	UID       types.UID
}
