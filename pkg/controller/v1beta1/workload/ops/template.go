package ops

import (
	"context"
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// podTemplate is what one pod set is rendered from, paired with the
// revision its pods are stamped with. A pod set renders exactly the
// revision it stamps: the label routes it, counts it and drains it as
// that revision, so the content must be that revision's. Only the
// constructors below build the pair, so no caller can stamp one revision
// while rendering another.
type podTemplate struct {
	revision        query.RevisionID
	podSpec         *corev1.PodSpec
	workerPodSpec   *corev1.PodSpec
	meta            *metav1.ObjectMeta
	pairingProtocol string
}

// desiredTemplate is the Component's current template, stamped with rev.
// It is the template of the roll target and of nothing else: a caller
// stamping any other revision renders storedTemplate. A zero rev leaves
// the pods unlabeled, for a row that records no revision at all.
func desiredTemplate(input workload.ReconcileInput, plan workload.ComponentPlan, rev query.RevisionID) podTemplate {
	return podTemplate{
		revision:        rev,
		podSpec:         input.DesiredSpec.PodSpec,
		workerPodSpec:   input.DesiredSpec.WorkerPodSpec,
		meta:            input.DesiredSpec.PodTemplateObjectMeta,
		pairingProtocol: plan.PairingProtocol,
	}
}

// pinnedTemplate is the template an operation pinned to revision name
// renders: the current template when the pin is the roll target, else
// the pinned revision's stored template. found is false when the pinned
// revision has no ControllerRevision left.
func pinnedTemplate(ctx context.Context, reads client.Reader, input workload.ReconcileInput, plan workload.ComponentPlan, target *appsv1.ControllerRevision, name string) (podTemplate, bool, error) {
	if target != nil && name == target.Name {
		return desiredTemplate(input, plan, query.RevisionOf(target)), true, nil
	}
	return storedTemplate(ctx, reads, input, target, name)
}

// storedTemplate is the template the named ControllerRevision records,
// stamped with that revision. target is the roll target: it tells the
// renderer which keys of the current template's metadata belong to a
// revision (see revisionMeta). found is false when the revision is gone;
// the caller decides what a rebuild with nothing to render from does.
// The roll target is never read through here: the Component's current
// template IS that revision's, and callers with the target in hand go
// through desiredTemplate.
func storedTemplate(ctx context.Context, reads client.Reader, input workload.ReconcileInput, target *appsv1.ControllerRevision, name string) (podTemplate, bool, error) {
	payload, err := loadRevisionPayload(ctx, reads, input.Key.Namespace, name)
	if err != nil {
		return podTemplate{}, false, err
	}
	if payload == nil {
		return podTemplate{}, false, nil
	}
	tmpl, err := templateFromPayload(input, target, name, payload)
	if err != nil {
		return podTemplate{}, false, err
	}
	return tmpl, true, nil
}

// templateFromPayload is storedTemplate for a payload the caller already
// holds. The pairing protocol is revision-owned, so a pod rendered from a
// stored revision carries that revision's protocol, not the current
// spec's.
func templateFromPayload(input workload.ReconcileInput, target *appsv1.ControllerRevision, name string, payload *revision.DataPayload) (podTemplate, error) {
	if payload == nil || payload.PodSpec == nil {
		return podTemplate{}, fmt.Errorf("revision %s records no pod template", name)
	}
	owned, err := revisionOwnedMeta(target)
	if err != nil {
		return podTemplate{}, err
	}
	tmpl := podTemplate{
		revision:      query.RevisionFromName(name),
		podSpec:       payload.PodSpec,
		workerPodSpec: payload.WorkerPodSpec,
		meta:          revisionMeta(input.DesiredSpec.PodTemplateObjectMeta, owned, payload.PodMeta),
	}
	if payload.PairingProtocol != nil {
		tmpl.pairingProtocol = *payload.PairingProtocol
	}
	return tmpl, nil
}

// revisionOwnedMeta is the pod-template metadata the roll target hashed:
// the labels and annotations of the current template that belong to a
// revision. nil when no target is in hand or it records no metadata, in
// which case no key of the current template is treated as a revision's.
func revisionOwnedMeta(target *appsv1.ControllerRevision) (*metav1.ObjectMeta, error) {
	payload, err := loadControllerRevisionPayload(target)
	if err != nil {
		return nil, fmt.Errorf("roll target %s: %w", target.Name, err)
	}
	if payload == nil {
		return nil, nil
	}
	return payload.PodMeta, nil
}

// revisionMeta composes the labels and annotations a pod of a stored
// revision carries. A key a revision hashes belongs to the revision: it
// takes the stored revision's value and is absent when the stored
// revision does not record it, even if the current template does — that
// is a key a newer revision added. A key no revision hashes — the
// admission plane's queue labels, the annotations inherited from the
// owner — keeps following the current template. The renderer tells the
// two apart by owned, the metadata the roll target recorded: a current
// key the target hashed is a revision's; every other current key is not.
func revisionMeta(current, owned, stored *metav1.ObjectMeta) *metav1.ObjectMeta {
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

// announceRevisionGone reports, once per attempt on the row, that a
// rebuild of Instance idx cannot render revision name because its
// ControllerRevision is gone. The rebuild creates nothing: rendering the
// current template under that revision's label would route, count and
// drain one revision while running another. The attempt's own deadline
// bounds the wait.
func announceRevisionGone(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, name string) error {
	announced, err := status.Announce(ctx, input, idx, workload.EventReasonRepairRevisionGone)
	if err != nil || !announced {
		return err
	}
	workload.RecordWarning(deps.Recorder, workload.EventTarget(input), workload.EventReasonRepairRevisionGone,
		"OMENative %s cannot rebuild revision %s: its ControllerRevision is gone, so no pod is created from the current template under that revision; the attempt waits for its deadline",
		workload.InstanceKey(input.Key.Component, idx), name)
	return nil
}
