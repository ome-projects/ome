// Package autoscalerpolicy validates AutoscalerPolicy writes and guards
// against in-use deletion.
//
// CREATE/UPDATE run the full cluster-state-free spec validation (structure,
// reserved shapes, template allowlist, sample render, PromQL parse) so an
// invalid policy never lands where a consumer could reference it. DELETE is
// denied while any InferenceService component in the namespace references
// the policy: a webhook refusal is immediate and names its reason, whereas a
// finalizer would accept the delete and park the object in Terminating,
// wedging GitOps prune and teaching operators to strip finalizers.
package autoscalerpolicy

import (
	"context"
	"fmt"

	admissionv1 "k8s.io/api/admission/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"sigs.k8s.io/ome/pkg/validation"
	"sigs.k8s.io/ome/pkg/webhook/admission/policyguard"
)

var log = logf.Log.WithName("autoscalerpolicy-validation-webhook")

// +kubebuilder:webhook:verbs=create;update;delete,path=/validate-ome-io-v1beta1-autoscalerpolicy,mutating=false,failurePolicy=fail,groups=ome.io,resources=autoscalerpolicies,versions=v1beta1,name=autoscalerpolicy.ome-webhook-server.validator,sideEffects=None,admissionReviewVersions=v1

// +kubebuilder:object:generate=false

// Validator validates AutoscalerPolicy admission requests. Admission is
// UX, not the invariant: break-glass, version skew, and webhook-outage
// windows can still remove a referenced policy, and the consumer-side
// fail-closed freeze covers those — this handler exists so the ordinary
// path fails fast with a named reason instead.
type Validator struct {
	Client  client.Reader
	Decoder admission.Decoder
}

func (v *Validator) Handle(ctx context.Context, req admission.Request) admission.Response {
	switch req.Operation {
	case admissionv1.Create, admissionv1.Update:
		if _, earlyResponse := v.guard().ValidateWrite(req); earlyResponse != nil {
			return *earlyResponse
		}
		return admission.Allowed("")
	case admissionv1.Delete:
		return v.guard().ValidateDelete(ctx, req)
	default:
		return admission.Allowed("")
	}
}

// guard binds the admission rules the policy kinds share to this kind.
func (v *Validator) guard() policyguard.Guard[*v1beta1.AutoscalerPolicy] {
	return policyguard.Guard[*v1beta1.AutoscalerPolicy]{
		Client:  v.Client,
		Decoder: v.Decoder,
		Log:     log,
		Kind:    constants.AutoscalerPolicyKind,
		New:     func() *v1beta1.AutoscalerPolicy { return &v1beta1.AutoscalerPolicy{} },
		ValidateSpec: func(policy *v1beta1.AutoscalerPolicy) error {
			return validation.ValidateAutoscalerPolicySpec(&policy.Spec)
		},
		Referencing: v.referencingComponents,
		Referent:    "component",
	}
}

// referencingComponents lists every same-namespace InferenceService
// component whose autoscalerPolicyRef names this policy, as
// "<isvc>/<component>" in stable order (ISVCs in list order, components
// engine, decoder, router).
func (v *Validator) referencingComponents(ctx context.Context, namespace, name string) ([]string, error) {
	consumers := &v1beta1.InferenceServiceList{}
	if err := v.Client.List(ctx, consumers, client.InNamespace(namespace)); err != nil {
		return nil, err
	}
	var referencing []string
	for i := range consumers.Items {
		isvc := &consumers.Items[i]
		for _, componentType := range validation.ComponentRoles {
			ref := autoscaler.ComponentPolicyRef(isvc, componentType)
			if ref == nil || ref.Name != name {
				continue
			}
			// A reserved cluster-scoped kind cannot name this namespaced
			// policy; only the namespaced kind (or its empty default) counts.
			if ref.Kind != "" && ref.Kind != constants.AutoscalerPolicyKind {
				continue
			}
			referencing = append(referencing, fmt.Sprintf("%s/%s", isvc.Name, componentType))
		}
	}
	return referencing, nil
}
