package placement

import (
	"fmt"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

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
//   - Control-plane-only directives removed (placement selectors + rollout verbs)
//     so the worker reconciler does not (re)act on the control plane's decisions.
//
// localQueue is the operator-configured LocalQueue used when the source carries
// no LocalQueueAnnotation; with neither set the queue label is left unstamped.
func DeriveISVC(src *v1beta1.InferenceService, controlPlaneID, localQueue string) *v1beta1.InferenceService {
	d := src.DeepCopy()

	// Clear server-side / control-plane state.
	d.ResourceVersion = ""
	d.UID = ""
	d.Generation = 0
	d.CreationTimestamp = metav1.Time{}
	d.DeletionTimestamp = nil
	d.OwnerReferences = nil
	d.Finalizers = nil
	d.ManagedFields = nil
	d.Status = v1beta1.InferenceServiceStatus{}

	// Resolve queue name: per-ISVC annotation, then the operator-configured queue.
	queue := src.Annotations[LocalQueueAnnotation]
	if queue == "" {
		queue = localQueue
	}

	// Add origin markers.
	if d.Labels == nil {
		d.Labels = make(map[string]string)
	}
	d.Labels[PlacementOriginLabel] = string(src.UID)
	// Stamp the control-plane identity so GC across shared workload clusters can
	// tell our deriveds apart from another control plane's. Config-driven, no
	// in-code default: an empty id leaves the label unset (single-CP behavior).
	if controlPlaneID != "" {
		d.Labels[PlacementControlPlaneLabel] = controlPlaneID
	}
	if d.Annotations == nil {
		d.Annotations = make(map[string]string)
	}
	d.Annotations[PlacementOriginUIDAnnotation] = string(src.UID)
	// Strip control-plane-only directives so they do not ride along to the
	// worker: the placement selectors, the rollout operator verbs, and the
	// source's GitOps bookkeeping.
	for _, k := range controlPlaneOnlyAnnotations {
		delete(d.Annotations, k)
	}
	for k := range d.Annotations {
		for _, p := range controlPlaneOnlyAnnotationPrefixes {
			if strings.HasPrefix(k, p) {
				delete(d.Annotations, k)
				break
			}
		}
	}

	// Stamp Kueue gating on each component. An unresolved queue leaves the label
	// off — the queue names an operator-provisioned resource, so there is no
	// in-code default to guess.
	if queue != "" {
		if d.Spec.Engine != nil {
			stampQueue(&d.Spec.Engine.ComponentExtensionSpec, queue)
		}
		if d.Spec.Decoder != nil {
			stampQueue(&d.Spec.Decoder.ComponentExtensionSpec, queue)
		}
		if d.Spec.Router != nil {
			stampQueue(&d.Spec.Router.ComponentExtensionSpec, queue)
		}
	}

	return d
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

// setDerivedReplicas pins Split's apportioned replica band on a derived ISVC's
// scalable components: MinReplicas becomes this home's share of the floor (Kueue
// admits as many as fit), and MaxReplicas becomes the per-cluster ceiling so the
// home can autoscale UP locally under load but no further. maxPer > 0 is the hard
// ceiling from spec.placement.split.maxReplicasPerCluster; maxPer <= 0 means no
// cap was declared, so the component's own MaxReplicas stands (only raised to
// keep Max >= Min). The apportioned share is always <= maxPer (splitApportion
// caps it), so Min <= Max holds. Applied to Engine and, when present, Decoder (a
// PD pair scales 1:1).
func setDerivedReplicas(d *v1beta1.InferenceService, replicas, maxPer int32) {
	n := int(replicas)
	apply := func(c *v1beta1.ComponentExtensionSpec) {
		c.MinReplicas = ptr.To(n)
		switch {
		case maxPer > 0:
			c.MaxReplicas = int(maxPer)
		case c.MaxReplicas < n:
			c.MaxReplicas = n
		}
	}
	if d.Spec.Engine != nil {
		apply(&d.Spec.Engine.ComponentExtensionSpec)
	}
	if d.Spec.Decoder != nil {
		apply(&d.Spec.Decoder.ComponentExtensionSpec)
	}
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
