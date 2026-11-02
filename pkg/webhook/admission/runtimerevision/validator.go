// Package runtimerevision implements an immutability validator on
// apps/v1.ControllerRevisions. For revisions that OME wrote
// (identified by the ome.io/created-by=ome-controller annotation),
// updates may only mutate annotations and other non-identity metadata:
// .data, .labels, and .revision keep their creation-time values.
// Revisions OME didn't create (StatefulSet/DaemonSet-owned) pass
// through unchanged.
//
// Allowing annotation changes is load-bearing: the GC controller marks
// eligible-since via an annotation update, and built-in K8s controllers
// add finalizers, ownerRefs, deletionTimestamp, etc. as part of normal
// lifecycle. Locking those down would freeze GC and break deletion.
package runtimerevision

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/constants"
)

// +kubebuilder:webhook:verbs=update,path=/validate-apps-v1-controllerrevision,mutating=false,failurePolicy=fail,groups="apps",resources=controllerrevisions,versions=v1,name=controllerrevision-immutability.ome-webhook-server.validator,sideEffects=None,admissionReviewVersions=v1

var log = logf.Log.WithName("controllerrevision-immutability")

type ImmutabilityValidator struct {
	Decoder admission.Decoder
}

func (v *ImmutabilityValidator) Handle(_ context.Context, req admission.Request) admission.Response {
	if req.Operation != admissionv1.Update {
		return admission.Allowed("")
	}
	oldObj := &appsv1.ControllerRevision{}
	if err := v.Decoder.DecodeRaw(req.OldObject, oldObj); err != nil {
		log.Error(err, "decode old ControllerRevision")
		return admission.Errored(http.StatusBadRequest, err)
	}
	// Not an OME-written revision → pass through (StatefulSet /
	// DaemonSet manage their own).
	if oldObj.Annotations[constants.RuntimeRevisionCreatedByKey] != constants.RuntimeRevisionCreatedByOMEValue {
		return admission.Allowed("")
	}
	newObj := &appsv1.ControllerRevision{}
	if err := v.Decoder.Decode(req, newObj); err != nil {
		log.Error(err, "decode new ControllerRevision")
		return admission.Errored(http.StatusBadRequest, err)
	}
	// Data is the serialized ServingRuntimeSpec; tampering with it would
	// silently change what every InferenceService on this revision
	// reconciles against.
	// K8s' built-in ControllerRevision strategy already enforces this;
	// the check is defense-in-depth.
	if !bytes.Equal(oldObj.Data.Raw, newObj.Data.Raw) {
		return admission.Denied(fmt.Sprintf(
			"ControllerRevision %s/%s is owned by OME; .data is immutable",
			oldObj.Namespace, oldObj.Name))
	}
	// Labels carry the cross-check inputs (ome.io/runtime-of,
	// /revision-hash, /runtime-of-kind, /runtime-of-namespace). The GC
	// groups by runtime-of, and the controller refuses a fixed revision
	// whose label doesn't match the requested runtime; relabeling would
	// let a revision masquerade as another runtime's.
	if !reflect.DeepEqual(oldObj.Labels, newObj.Labels) {
		return admission.Denied(fmt.Sprintf(
			"ControllerRevision %s/%s is owned by OME; .labels are immutable",
			oldObj.Namespace, oldObj.Name))
	}
	// Revision int64 is the ControllerRevision API's monotonic id.
	// Also K8s-enforced; checked here so an end-to-end test against the
	// webhook covers the contract regardless of upstream changes.
	if oldObj.Revision != newObj.Revision {
		return admission.Denied(fmt.Sprintf(
			"ControllerRevision %s/%s is owned by OME; .revision is immutable",
			oldObj.Namespace, oldObj.Name))
	}
	return admission.Allowed("")
}
