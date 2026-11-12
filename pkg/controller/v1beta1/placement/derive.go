package placement

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

// DeriveISVC builds a derived InferenceService from a control-plane source
// ISVC. The derived object is suitable for placement onto a workload cluster:
//   - Identity (name/ns) preserved so the worker cluster can address it.
//   - Server-side fields cleared (UID, ResourceVersion, generation, etc.).
//   - Origin markers added (PlacementOriginLabel, PlacementOriginUIDAnnotation).
//   - Control-plane identity stamped (PlacementControlPlaneLabel) when controlPlaneID
//     is non-empty, so the GC sweep only reaps deriveds THIS control plane created.
//   - Kueue gating stamped on component pod metadata (the queue-name label).
//   - Control-plane-only directives removed (routing, placement, and
//     rollout verbs) so the worker reconciler does not (re)act on the control
//     plane's decisions.
//
// localQueue is the operator-configured LocalQueue used when the source carries
// no LocalQueueAnnotation; with neither set the queue label is left unstamped.
func DeriveISVC(src *v1beta1.InferenceService, controlPlaneID, localQueue string) *v1beta1.InferenceService {
	d := src.DeepCopy()
	clearControlPlaneState(d)
	setOriginLabels(d, src.UID, controlPlaneID)
	stripControlPlaneAnnotations(d)
	if src.Spec.Placement.UsesClusterAffinity() {
		d.Annotations[constants.PlacementPolicy] = string(v1beta1.PlacementPolicyClusterAffinity)
	}
	if queue := derivedQueue(src, localQueue); queue != "" {
		setQueueLabelOnComponents(&d.Spec, queue)
	}
	return d
}

// clearControlPlaneState removes what the control plane's apiserver owns
// (identity, generation, lifecycle and status) and the two spec blocks only
// the control plane reconciles, routing and placement.
func clearControlPlaneState(d *v1beta1.InferenceService) {
	d.ResourceVersion = ""
	d.UID = ""
	d.Generation = 0
	d.CreationTimestamp = metav1.Time{}
	d.DeletionTimestamp = nil
	d.OwnerReferences = nil
	d.Finalizers = nil
	d.ManagedFields = nil
	d.Status = v1beta1.InferenceServiceStatus{}
	d.Spec.Routing = nil
	d.Spec.Placement = nil
}

// setOriginLabels records the source's UID as a label and an annotation and,
// when the operator configured one, the control plane's identity, so a GC
// sweep over a shared workload cluster removes only the derived objects this
// control plane made. An empty id leaves that label unset.
func setOriginLabels(d *v1beta1.InferenceService, sourceUID types.UID, controlPlaneID string) {
	if d.Labels == nil {
		d.Labels = make(map[string]string)
	}
	d.Labels[PlacementOriginLabel] = string(sourceUID)
	if controlPlaneID != "" {
		d.Labels[PlacementControlPlaneLabel] = controlPlaneID
	}
	if d.Annotations == nil {
		d.Annotations = make(map[string]string)
	}
	d.Annotations[PlacementOriginUIDAnnotation] = string(sourceUID)
}

// stripControlPlaneAnnotations drops the directives only the control plane
// reads: the placement selectors, the rollout operator verbs, the placement
// policy and the annotation families of the source's deployer and of the
// routing publisher. The worker reconciler must not act on them.
func stripControlPlaneAnnotations(d *v1beta1.InferenceService) {
	for _, k := range controlPlaneOnlyAnnotations {
		delete(d.Annotations, k)
	}
	delete(d.Annotations, constants.PlacementPolicy)
	for k := range d.Annotations {
		if hasControlPlaneOnlyPrefix(k) {
			delete(d.Annotations, k)
		}
	}
}

func hasControlPlaneOnlyPrefix(key string) bool {
	for _, p := range controlPlaneOnlyAnnotationPrefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// derivedQueue is the Kueue queue the derived workload joins: the source's
// annotation first, then the operator-configured queue. The queue names a
// resource the operator provisioned, so with neither set it is empty.
func derivedQueue(src *v1beta1.InferenceService, localQueue string) string {
	if queue := src.Annotations[LocalQueueAnnotation]; queue != "" {
		return queue
	}
	return localQueue
}

// setQueueLabelOnComponents puts the queue label on the pod metadata of the
// engine, the decoder and the router, whichever the spec declares.
func setQueueLabelOnComponents(spec *v1beta1.InferenceServiceSpec, queue string) {
	if spec.Engine != nil {
		stampQueue(&spec.Engine.ComponentExtensionSpec, queue)
	}
	if spec.Decoder != nil {
		stampQueue(&spec.Decoder.ComponentExtensionSpec, queue)
	}
	if spec.Router != nil {
		stampQueue(&spec.Router.ComponentExtensionSpec, queue)
	}
}

// inflateRolloutGroups rewrites every ref-carrying rollout group on a derived
// ISVC into its rendered inline form, so members receive plain inline groups
// and need no RolloutPolicy objects, informers, or sync pipeline:
//   - a ref-only group gets the referenced policy's progression body written
//     inline (rolloutpolicy.ComposeGroup), the ref cleared, and a provenance
//     entry recorded in the plan-source annotation
//     ("<groupIndex>=<policyName>@<portableDigest>", semicolon-separated) so
//     the member's run open reports the same identity a locally-resolved ref
//     would;
//   - a group with an inline arm keeps its inline body verbatim (inline
//     outranks the ref) with the ref cleared, and gets NO provenance entry;
//   - a group with no ref is left completely untouched.
//
// policies maps policy name to the body+digest the preflight resolved. A
// ref-only group whose policy is not staged is an error, never a partial
// write: the caller must fail the derive rather than place a half-inflated
// spec (a member would silently execute the default progression where a gate
// was declared). d is the already-deep-copied derived object; the plan-source
// annotation was stripped by DeriveISVC, so what this writes is the only value.
func inflateRolloutGroups(d *v1beta1.InferenceService, policies map[string]resolvedRolloutPolicy) error {
	if d.Spec.Rollout == nil {
		return nil
	}
	var provenance []string
	for i := range d.Spec.Rollout.Groups {
		g := &d.Spec.Rollout.Groups[i]
		if g.PolicyRef == nil {
			continue
		}
		if hasInlineProgression(g) {
			// Inline outranks the ref: the body derives verbatim and only the
			// (member-unresolvable) ref is dropped.
			composed, err := rolloutpolicy.ComposeGroup(g, nil)
			if err != nil {
				return fmt.Errorf("rollout group %d: %w", i, err)
			}
			d.Spec.Rollout.Groups[i] = composed
			continue
		}
		name := g.PolicyRef.Name
		pol, ok := policies[name]
		if !ok || pol.spec == nil {
			return fmt.Errorf("rollout group %d references RolloutPolicy %q with no resolved body staged; refusing to derive a half-inflated spec", i, name)
		}
		composed, err := rolloutpolicy.ComposeGroup(g, pol.spec)
		if err != nil {
			return fmt.Errorf("rollout group %d: %w", i, err)
		}
		d.Spec.Rollout.Groups[i] = composed
		provenance = append(provenance, fmt.Sprintf("%d=%s@%s", i, name, pol.digest))
	}
	if len(provenance) > 0 {
		if d.Annotations == nil {
			d.Annotations = make(map[string]string, 1)
		}
		d.Annotations[constants.RolloutPlanSourceAnnotation] = strings.Join(provenance, ";")
	}
	return nil
}

// hasInlineProgression reports whether the group carries an inline progression
// arm (which then outranks a coexisting PolicyRef).
func hasInlineProgression(g *v1beta1.RolloutGroup) bool {
	return g.Canary != nil || g.BlueGreen != nil || g.RollingUpdate != nil
}

// stampQueue adds the Kueue queue-name label to a component's pod metadata.
// That label is what Kueue's pod integration keys on to gate the pods; it is the
// sole Kueue stamp the derived ISVC needs.
func stampQueue(comp *v1beta1.ComponentExtensionSpec, queue string) {
	if comp.Labels == nil {
		comp.Labels = make(map[string]string)
	}
	comp.Labels[constants.KueueQueueLabelKey] = queue
}
