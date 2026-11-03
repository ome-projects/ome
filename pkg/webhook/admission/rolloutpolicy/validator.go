// Package rolloutpolicy validates RolloutPolicy writes and guards against
// in-use deletion.
//
// CREATE/UPDATE run the shared progression-body validation (the same plan
// rules an inline block faces, plus the policy-only portability
// restrictions) and enforce the operator-configured rendered-size cap — the
// body is pinned into each consumer's status at run open, so an oversized
// policy would bloat every consumer's status writes. UPDATE additionally
// freezes the progression KIND while any InferenceService references the
// policy (consumers' shape rules were admitted against the declared kind, so
// an in-place kind change would invalidate every one of them) and warns on
// referenced body edits (edits are inert for pinned runs; the next run picks
// them up). DELETE is denied while any InferenceService rollout group in the
// namespace references the policy: a webhook refusal is immediate and names
// its reason, whereas a finalizer would accept the delete and park the
// object in Terminating, wedging GitOps prune.
package rolloutpolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	admissionv1 "k8s.io/api/admission/v1"
	apiequality "k8s.io/apimachinery/pkg/api/equality"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/validation"
	"sigs.k8s.io/ome/pkg/webhook/admission/policyguard"
)

var log = logf.Log.WithName("rolloutpolicy-validation-webhook")

// maxNamedReferencingISVCs is the cap the shared guard puts on the
// InferenceServices an in-use deletion denial names.
const maxNamedReferencingISVCs = policyguard.MaxNamedReferences

// +kubebuilder:webhook:verbs=create;update;delete,path=/validate-ome-io-v1beta1-rolloutpolicy,mutating=false,failurePolicy=fail,groups=ome.io,resources=rolloutpolicies,versions=v1beta1,name=rolloutpolicy.ome-webhook-server.validator,sideEffects=None,admissionReviewVersions=v1

// +kubebuilder:object:generate=false

// Validator validates RolloutPolicy admission requests. Admission is UX, not
// the invariant: break-glass, version skew, and webhook-outage windows can
// still land a broken or missing policy, and the consumer-side fail-closed
// park at run open covers those — this handler exists so the ordinary path
// fails fast with a named reason instead.
type Validator struct {
	Client  client.Reader
	Decoder admission.Decoder

	// MaxPlanBytes caps the JSON-rendered spec size; a body over the cap is
	// denied with PlanTooLarge. Zero means uncapped — the value is operator
	// configuration threaded in at registration, never an in-code default.
	MaxPlanBytes int
}

func (v *Validator) Handle(ctx context.Context, req admission.Request) admission.Response {
	switch req.Operation {
	case admissionv1.Create:
		_, earlyResponse := v.validateWrite(req)
		if earlyResponse != nil {
			return *earlyResponse
		}
		return admission.Allowed("")
	case admissionv1.Update:
		return v.validateUpdate(ctx, req)
	case admissionv1.Delete:
		return v.guard().ValidateDelete(ctx, req)
	default:
		return admission.Allowed("")
	}
}

// guard binds the admission rules the policy kinds share to this kind.
func (v *Validator) guard() policyguard.Guard[*v1beta1.RolloutPolicy] {
	return policyguard.Guard[*v1beta1.RolloutPolicy]{
		Client:  v.Client,
		Decoder: v.Decoder,
		Log:     log,
		Kind:    validation.RolloutPolicyKind,
		New:     func() *v1beta1.RolloutPolicy { return &v1beta1.RolloutPolicy{} },
		ValidateSpec: func(policy *v1beta1.RolloutPolicy) error {
			return validation.ValidateRolloutPolicySpec(&policy.Spec)
		},
		Referencing: v.referencingInferenceServices,
		Referent:    "InferenceService",
	}
}

// validateWrite runs the checks shared by CREATE and UPDATE: the full
// cluster-state-free spec validation, then the rendered-size cap. A non-nil
// response short-circuits the caller.
func (v *Validator) validateWrite(req admission.Request) (*v1beta1.RolloutPolicy, *admission.Response) {
	policy, earlyResponse := v.guard().ValidateWrite(req)
	if earlyResponse != nil {
		return nil, earlyResponse
	}
	if v.MaxPlanBytes > 0 {
		raw, err := json.Marshal(policy.Spec)
		if err != nil {
			log.Error(err, "Failed to render RolloutPolicy spec for the size cap", "name", req.Name, "namespace", req.Namespace)
			response := admission.Errored(http.StatusInternalServerError, err)
			return nil, &response
		}
		if len(raw) > v.MaxPlanBytes {
			response := admission.Denied(fmt.Sprintf(
				"spec renders to %d bytes, exceeding the configured cap of %d — the rendered plan is pinned into each consumer's status at run open, so an oversized body would bloat every status write (%s)",
				len(raw), v.MaxPlanBytes, validation.ReasonRolloutPlanTooLarge))
			return nil, &response
		}
	}
	return policy, nil
}

// validateUpdate layers the reference-aware rules on top of validateWrite:
// a progression-kind change is rejected while any InferenceService
// references the policy (kind changes ship as a new versioned policy name),
// and any other referenced body change is admitted with a warning — active
// runs keep their pinned plan, so the edit is inert until each consumer's
// next run.
func (v *Validator) validateUpdate(ctx context.Context, req admission.Request) admission.Response {
	policy, earlyResponse := v.validateWrite(req)
	if earlyResponse != nil {
		return *earlyResponse
	}
	old := &v1beta1.RolloutPolicy{}
	if err := v.Decoder.DecodeRaw(req.OldObject, old); err != nil {
		log.Error(err, "Failed to decode prior RolloutPolicy on update", "name", req.Name, "namespace", req.Namespace)
		return admission.Errored(http.StatusBadRequest, err)
	}
	if apiequality.Semantic.DeepEqual(old.Spec, policy.Spec) {
		return admission.Allowed("")
	}
	referencing, err := v.referencingInferenceServices(ctx, req.Namespace, req.Name)
	if err != nil {
		log.Error(err, "Failed to list InferenceServices for RolloutPolicy update", "name", req.Name, "namespace", req.Namespace)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if len(referencing) == 0 {
		return admission.Allowed("")
	}
	oldKind, _ := old.Spec.Progression()
	newKind, _ := policy.Spec.Progression()
	if oldKind != newKind {
		return admission.Denied(fmt.Sprintf(
			"progression kind cannot change from %q to %q while %d InferenceService(s) reference this policy — consumers' rollout shape rules were admitted against the declared kind; ship the new kind as a new versioned policy name and flip the refs",
			oldKind, newKind, len(referencing)))
	}
	return admission.Allowed("").WithWarnings(fmt.Sprintf(
		"%d InferenceService(s) in this namespace reference RolloutPolicy %q; in-flight rollout runs keep their pinned plan, so this edit takes effect at each consumer's next run",
		len(referencing), req.Name))
}

// referencingInferenceServices lists every same-namespace InferenceService
// with at least one spec.rollout.groups[].policyRef naming this policy, in
// list order. Each InferenceService appears once however many of its groups
// reference the policy.
func (v *Validator) referencingInferenceServices(ctx context.Context, namespace, name string) ([]string, error) {
	consumers := &v1beta1.InferenceServiceList{}
	if err := v.Client.List(ctx, consumers, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var referencing []string
	for i := range consumers.Items {
		isvc := &consumers.Items[i]
		for _, g := range isvc.Spec.GetRolloutGroups() {
			ref := g.PolicyRef
			if ref == nil || ref.Name != name {
				continue
			}
			// A reserved cluster-scoped kind cannot name this namespaced
			// policy; only the namespaced kind (or its empty default) counts.
			if ref.Kind != "" && ref.Kind != validation.RolloutPolicyKind {
				continue
			}
			referencing = append(referencing, isvc.Name)
			break
		}
	}
	return referencing, nil
}
