package mutate

import (
	"context"
	"reflect"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/validation"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/pinnedevidence"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

// CollectScalePinnedTargets adds only the exact status-selected IR proofs
// required by a valid pinned run. No ActiveRun adds no other IR. At most two
// siblings are read; no sibling Scale, listing or policy acquisition occurs.
func CollectScalePinnedTargets(ctx context.Context, client omeclient.OmeV1beta1Interface, evidence ScaleEvidence, clock reportv1alpha1.Clock) (ScaleEvidence, error) {
	if ctx == nil || client == nil || evidence.replica == nil || !evidence.source.MatchesParent(evidence.parent) || len(evidence.replicas) != 1 {
		return ScaleEvidence{}, ErrScaleEvidence
	}
	if err := ctx.Err(); err != nil {
		return ScaleEvidence{}, SafeAPIError(err)
	}
	parent := evidence.parent
	if parent.Status.Rollout == nil || parent.Status.Rollout.ActiveRun == nil {
		return evidence, nil
	}
	if !pinnedevidence.ValidActiveRun(parent) || len(parent.Status.Rollout.ActiveRun.TargetRevisions) > 3 {
		return ScaleEvidence{}, ErrStale
	}
	result := evidence
	result.replicas = map[v1beta1.ComponentType]*v1beta1.InferenceReplica{evidence.replica.Spec.Component: evidence.replica.DeepCopy()}
	for _, target := range parent.Status.Rollout.ActiveRun.TargetRevisions {
		ref := parent.Status.Components[target.Component].ScaleTargetRef
		if !validScaleReplicaRef(ref) {
			return ScaleEvidence{}, ErrScaleEvidence
		}
		if target.Component == evidence.replica.Spec.Component {
			if ref.Name != evidence.replica.Name {
				return ScaleEvidence{}, ErrScaleEvidence
			}
			continue
		}
		requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		replica, err := client.InferenceReplicas(parent.Namespace).Get(requestCtx, ref.Name, metav1.GetOptions{})
		if err == nil {
			err = requestCtx.Err()
		}
		cancel()
		if err != nil {
			return ScaleEvidence{}, SafeAPIError(err)
		}
		if err := inspectScaleTargetReplica(parent, replica, target.Component, ref.Name, clock); err != nil {
			return ScaleEvidence{}, err
		}
		result.replicas[target.Component] = replica.DeepCopy()
	}
	if err := ctx.Err(); err != nil {
		return ScaleEvidence{}, SafeAPIError(err)
	}
	return result, nil
}

func validScaleReplicaRef(ref *v1beta1.ScaleTargetRef) bool {
	return ref != nil && ref.APIVersion == "ome.io/v1beta1" && ref.Kind == "InferenceReplica" && ref.Name != "" && len(validation.IsDNS1123Subdomain(ref.Name)) == 0
}

func inspectScaleTargetReplica(parent *v1beta1.InferenceService, replica *v1beta1.InferenceReplica, component v1beta1.ComponentType, name string, clock reportv1alpha1.Clock) error {
	if replica == nil || replica.Name != name || replica.Spec.Component != component {
		return ErrScaleEvidence
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	row, err := inspectReplica(replica, parent, []string{string(component)}, clock.Now())
	if err != nil {
		return err
	}
	if !row.complete || replica.Spec.Replicas == nil || *replica.Spec.Replicas < 1 {
		return ErrScaleEvidence
	}
	return nil
}

func (e ScaleEvidence) pinnedSources() ReplicaEvidence {
	result := ReplicaEvidence{sources: map[v1beta1.ComponentType]string{}}
	for component, replica := range e.replicas {
		revision := replica.Status.UpdateRevision
		if revision == "" {
			revision = replica.Status.CurrentRevision
		}
		result.sources[component] = strings.TrimPrefix(revision, replica.Name+"-")
	}
	return result
}

// Revalidate re-GETs each actually used IR, retaining exact key/component,
// UID/RV, generation and complete private snapshot. This narrows a race; it
// is not a multi-object transaction. Only the selected IR receives a CAS.
func (e ScaleEvidence) Revalidate(ctx context.Context, client omeclient.OmeV1beta1Interface, clock reportv1alpha1.Clock) error {
	if ctx == nil || client == nil || e.replica == nil || !e.source.MatchesParent(e.parent) || len(e.replicas) < 1 || len(e.replicas) > 3 {
		return ErrScaleEvidence
	}
	// Fixed component order also makes refusal/request traces deterministic.
	for _, component := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		observed := e.replicas[component]
		if observed == nil {
			continue
		}
		if err := ctx.Err(); err != nil {
			return SafeAPIError(err)
		}
		requestCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		current, err := client.InferenceReplicas(e.parent.Namespace).Get(requestCtx, observed.Name, metav1.GetOptions{})
		if err == nil {
			err = requestCtx.Err()
		}
		cancel()
		if err != nil {
			return SafeAPIError(err)
		}
		if err := inspectScaleTargetReplica(e.parent, current, component, observed.Name, clock); err != nil {
			return err
		}
		if current.UID != observed.UID || current.ResourceVersion != observed.ResourceVersion || current.Generation != observed.Generation ||
			!reflect.DeepEqual(current.Spec, observed.Spec) || !reflect.DeepEqual(current.Status, observed.Status) ||
			!reflect.DeepEqual(current.OwnerReferences, observed.OwnerReferences) || !reflect.DeepEqual(current.Annotations, observed.Annotations) {
			return ErrStale
		}
	}
	if err := ctx.Err(); err != nil {
		return SafeAPIError(err)
	}
	return nil
}
