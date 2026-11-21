package revision

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// ComposeMeta is the labels and annotations a pod rendered from the stored
// revision carries. A key a revision hashes belongs to the revision: it takes
// the stored revision's value, and it is absent when the stored revision does
// not record it, even if the current template has it, because a newer
// revision added that key. A key no revision hashes keeps following the
// current template: the admission plane's queue labels, the placement
// provenance labels, and the annotations inherited from the owner. owned is
// the metadata the current template hashes; it tells the two kinds apart. A
// current key in owned belongs to a revision, and every other current key
// does not. The inputs are never written to.
func ComposeMeta(current, owned, stored *metav1.ObjectMeta) *metav1.ObjectMeta {
	if current == nil && stored == nil {
		return nil
	}
	var ownedLabels, ownedAnnotations map[string]string
	if owned != nil {
		ownedLabels, ownedAnnotations = owned.Labels, owned.Annotations
	}
	out := &metav1.ObjectMeta{}
	if current != nil {
		out.Labels = cloneWithout(current.Labels, ownedLabels)
		out.Annotations = cloneWithout(current.Annotations, ownedAnnotations)
	}
	if stored != nil {
		out.Labels = overlay(out.Labels, stored.Labels)
		out.Annotations = overlay(out.Annotations, stored.Annotations)
	}
	return out
}

// cloneWithout copies in, leaving out every key drop names.
func cloneWithout(in, drop map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		if _, dropped := drop[k]; dropped {
			continue
		}
		out[k] = v
	}
	return out
}

// overlay writes every entry of over into base and returns the result.
func overlay(base, over map[string]string) map[string]string {
	if len(over) == 0 {
		return base
	}
	if base == nil {
		base = make(map[string]string, len(over))
	}
	for k, v := range over {
		base[k] = v
	}
	return base
}
