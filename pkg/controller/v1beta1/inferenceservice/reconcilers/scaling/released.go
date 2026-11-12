package scaling

import (
	"context"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// A follower that stops following (the policy removed, its ratio dropped, the
// mode switched to Independent, the anchor moved onto it) keeps the count the
// coordinator left it at. When the Component declares autoscaler class None
// nothing else owns that count, so the coordinator records the Component as
// released and the projector keeps the count until the operator edits the
// Component's minReplicas or a policy names it again. A released Component
// with an autoscaler of its own is not recorded: that autoscaler owns the
// count from the pass it comes back.

// releasedFollower is one Component that stopped following.
type releasedFollower struct {
	component v1beta1.ComponentType
	// ratio is the ratio the Component last followed at.
	ratio resource.Quantity
	// named is the fixed count the Component's minReplicas names.
	named int32
	// current is the count the replica carried before the projectors ran.
	current int32
	// kept is true when the coordinator keeps the count: the Component
	// declares autoscaler class None, so no autoscaler owns it.
	kept bool
	// firstPass is true in the pass the Component stopped following, the
	// one pass that warns.
	firstPass bool
}

// releasedFollowers reads the followers the last pass recorded and keeps the
// ones the policy no longer names. A standing record ends when the spec names
// a new minReplicas for the Component or an autoscaler takes its count.
func releasedFollowers(ctx context.Context, reader client.Reader, isvc *v1beta1.InferenceService,
	policy *v1beta1.ScalingPolicy, components map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec,
	modes map[v1beta1.ComponentType]constants.DeploymentModeType) (map[v1beta1.ComponentType]releasedFollower, error) {
	out := map[v1beta1.ComponentType]releasedFollower{}
	previous := isvc.Status.ScalingPolicy
	if previous == nil {
		return out, nil
	}
	for _, entry := range previous.Followers {
		component := entry.Component
		if Follows(policy, component, components, modes) || componentIssue(component, components, modes) != "" {
			continue
		}
		named := FollowerBounds(components[component]).Min
		kept := fixedCount(components[component])
		if entry.Released {
			if kept && entry.Target == named {
				out[component] = releasedFollower{component: component, ratio: entry.Ratio, named: named, current: entry.Current, kept: true}
			}
			continue
		}
		ir, err := readProjectedReplica(ctx, reader, isvc, component)
		if apierrors.IsNotFound(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		current := ptr.Deref(ir.Spec.Replicas, 0)
		if kept && current == named {
			continue
		}
		out[component] = releasedFollower{component: component, ratio: entry.Ratio, named: named, current: current, kept: kept, firstPass: true}
	}
	return out, nil
}

// fixedCount reports whether the projector writes the Component's count from
// its minReplicas: the Component declares autoscaler class None.
func fixedCount(c *v1beta1.ComponentExtensionSpec) bool {
	return c != nil && c.Autoscaler != nil && c.Autoscaler.Class == v1beta1.AutoscalerNone
}

// Released reports whether the Component stopped following and the
// coordinator keeps the count it carries.
func (t *Targets) Released(component v1beta1.ComponentType) bool {
	if t == nil {
		return false
	}
	return t.released[component].kept
}

// releasedStatus records each released Component the coordinator keeps a
// count for, with the count its replica carries now, and warns once, in the
// pass a Component stops following, about who owns its count from here. The
// notes repeat the records in words for the Ready condition.
func (t *Targets) releasedStatus(ctx context.Context, reader client.Reader, recorder record.EventRecorder,
	isvc *v1beta1.InferenceService) ([]v1beta1.FollowerScalingStatus, []string, error) {
	keys := make([]v1beta1.ComponentType, 0, len(t.released))
	for component := range t.released {
		keys = append(keys, component)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	var entries []v1beta1.FollowerScalingStatus
	var notes []string
	for _, component := range keys {
		r := t.released[component]
		current := r.current
		if r.kept {
			ir, err := readProjectedReplica(ctx, reader, isvc, component)
			if apierrors.IsNotFound(err) {
				continue
			}
			if err != nil {
				return nil, nil, err
			}
			current = ptr.Deref(ir.Spec.Replicas, 0)
		}
		note := releasedNote(r, current)
		if r.firstPass && recorder != nil {
			recorder.Event(isvc, corev1.EventTypeWarning, "ScalingFollowerReleased", note)
		}
		if !r.kept || current == r.named {
			continue
		}
		notes = append(notes, note)
		entries = append(entries, v1beta1.FollowerScalingStatus{Component: component, Ratio: r.ratio.DeepCopy(),
			Target: r.named, Current: current, Released: true})
	}
	return entries, notes, nil
}

func releasedNote(r releasedFollower, current int32) string {
	if !r.kept {
		return fmt.Sprintf("%s stopped following; its autoscaler owns its count from here (%d Instances)", r.component, current)
	}
	return fmt.Sprintf("%s stopped following and keeps %d Instances; its minReplicas names %d; a new minReplicas, an autoscaler or a policy moves it",
		r.component, current, r.named)
}
