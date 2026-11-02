package servingruntime

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	admissionv1 "k8s.io/api/admission/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/validation"
)

var log = logf.Log.WithName(constants.ServingRuntimeValidatorWebhookName)

const (
	InvalidPriorityError                        = "same priority assigned for the model format %s"
	InvalidPriorityServingRuntimeError          = "%s in the servingruntimes %s and %s in namespace %s"
	InvalidPriorityClusterServingRuntimeError   = "%s in the clusterservingruntimes %s and %s"
	PriorityIsNotSameServingRuntimeError        = "%s under the servingruntime %s"
	PriorityIsNotSameClusterServingRuntimeError = "%s under the clusterservingruntime %s"
	ChainsawInjectAnnotationNotAllowError       = "chainsaw inject annotation is not allowed"
	UnknownAcceleratorClassError                = "unknown accelerator classes referenced in AcceleratorRequirements: %v"
)

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-clusterservingruntime,mutating=false,failurePolicy=fail,groups=ome.io,resources=clusterservingruntimes,versions=v1beta1,name=clusterservingruntime.ome-webhook-server.validator

type ClusterServingRuntimeValidator struct {
	Client  client.Reader
	Decoder admission.Decoder
}

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-servingruntime,mutating=false,failurePolicy=fail,groups=ome.io,resources=servingruntimes,versions=v1beta1,name=servingruntime.ome-webhook-server.validator

type ServingRuntimeValidator struct {
	Client  client.Reader
	Decoder admission.Decoder
}

func (sr *ServingRuntimeValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	servingRuntime := &v1beta1.ServingRuntime{}
	if err := sr.Decoder.Decode(req, servingRuntime); err != nil {
		log.Error(err, "Failed to decode serving runtime", "name", servingRuntime.Name, "namespace", servingRuntime.Namespace)
		return admission.Errored(http.StatusBadRequest, err)
	}

	// Profile validation runs before the disabled-shortcut
	// below so a broken profile can't slip through "because it was
	// disabled anyway".
	if err := validateProfileMarker(servingRuntime.Annotations, servingRuntime.Spec.Disabled); err != nil {
		return admission.Denied(err.Error())
	}
	if err := validateNamespacedRuntimeInheritance(ctx, sr.Client, servingRuntime); err != nil {
		return admission.Denied(err.Error())
	}

	// Scaling-policy modes no controller applies are rejected before the
	// disabled-shortcut so a disabled runtime cannot store a policy the
	// update ratchet would then treat as pre-existing on re-enable.
	oldSRSpec := func() (*v1beta1.ServingRuntimeSpec, error) {
		old := &v1beta1.ServingRuntime{}
		if err := sr.Decoder.DecodeRaw(req.OldObject, old); err != nil {
			return nil, err
		}
		return &old.Spec, nil
	}
	if resp := validateRuntimeScalingPolicy(req, &servingRuntime.Spec, oldSRSpec); resp != nil {
		return *resp
	}

	// Policy refs attach on the InferenceService only; a ref stored on a
	// runtime component config would be silently inert. Checked before the
	// disabled-shortcut so a disabled runtime cannot store one either.
	if err := validation.ValidateRuntimeAutoscalerPolicyRefs(&servingRuntime.Spec); err != nil {
		return admission.Denied(err.Error())
	}
	if err := validation.ValidateRuntimeRouterCapacityEndpoint(&servingRuntime.Spec); err != nil {
		return admission.Denied(err.Error())
	}

	existing := &v1beta1.ServingRuntimeList{}
	if err := sr.Client.List(ctx, existing, client.InNamespace(servingRuntime.Namespace)); err != nil {
		log.Error(err, "Failed to get serving runtime list", "namespace", servingRuntime.Namespace)
		return admission.Errored(http.StatusInternalServerError, err)
	}

	if servingRuntime.Spec.IsDisabled() {
		return admission.Allowed("")
	}

	// Per-Component Autoscaler block validation. Same rules
	// applied to the ISVC's blocks; ServingRuntime can declare
	// autoscaler defaults that inherit to ISVCs, so the same shape
	// constraints apply.
	if err := validateRuntimeComponentAutoscalers(&servingRuntime.Spec); err != nil {
		return admission.Denied(err.Error())
	}

	// Validate that all referenced accelerator classes exist
	if err := validateAcceleratorClasses(ctx, sr.Client, &servingRuntime.Spec); err != nil {
		log.Info("Accelerator class validation failed", "name", servingRuntime.Name, "namespace", servingRuntime.Namespace, "error", err)
		return admission.Denied(err.Error())
	}

	// Spec-only checks run outside the loop so they still apply when the
	// namespace has no other runtimes.
	if err := validation.ValidateModelFormatPrioritySame(&servingRuntime.Spec); err != nil {
		return admission.Denied(fmt.Sprintf(PriorityIsNotSameServingRuntimeError, err.Error(), servingRuntime.Name))
	}
	if err := validateServingRuntimeAnnotations(&servingRuntime.Spec); err != nil {
		return admission.Denied(ChainsawInjectAnnotationNotAllowError)
	}

	for i := range existing.Items {
		if err := validateServingRuntimePriority(&servingRuntime.Spec, &existing.Items[i].Spec, servingRuntime.Name, existing.Items[i].Name); err != nil {
			return admission.Denied(fmt.Sprintf(InvalidPriorityServingRuntimeError, err.Error(), existing.Items[i].Name, servingRuntime.Name, servingRuntime.Namespace))
		}
	}
	return admission.Allowed("")
}

func (csr *ClusterServingRuntimeValidator) Handle(ctx context.Context, req admission.Request) admission.Response {
	clusterServingRuntime := &v1beta1.ClusterServingRuntime{}
	if err := csr.Decoder.Decode(req, clusterServingRuntime); err != nil {
		log.Error(err, "Failed to decode cluster serving runtime", "name", clusterServingRuntime.Name)
		return admission.Errored(http.StatusBadRequest, err)
	}

	if err := validateProfileMarker(clusterServingRuntime.Annotations, clusterServingRuntime.Spec.Disabled); err != nil {
		return admission.Denied(err.Error())
	}
	if err := validateClusterRuntimeInheritance(ctx, csr.Client, clusterServingRuntime); err != nil {
		return admission.Denied(err.Error())
	}

	// Scaling-policy check placement mirrors ServingRuntime.Handle: before
	// the disabled-shortcut so the ratchet cannot be seeded while disabled.
	oldCSRSpec := func() (*v1beta1.ServingRuntimeSpec, error) {
		old := &v1beta1.ClusterServingRuntime{}
		if err := csr.Decoder.DecodeRaw(req.OldObject, old); err != nil {
			return nil, err
		}
		return &old.Spec, nil
	}
	if resp := validateRuntimeScalingPolicy(req, &clusterServingRuntime.Spec, oldCSRSpec); resp != nil {
		return *resp
	}

	// Policy refs attach on the InferenceService only; see ServingRuntime.Handle.
	if err := validation.ValidateRuntimeAutoscalerPolicyRefs(&clusterServingRuntime.Spec); err != nil {
		return admission.Denied(err.Error())
	}
	if err := validation.ValidateRuntimeRouterCapacityEndpoint(&clusterServingRuntime.Spec); err != nil {
		return admission.Denied(err.Error())
	}

	existing := &v1beta1.ClusterServingRuntimeList{}
	if err := csr.Client.List(ctx, existing); err != nil {
		log.Error(err, "Failed to get cluster serving runtime list")
		return admission.Errored(http.StatusInternalServerError, err)
	}

	if clusterServingRuntime.Spec.IsDisabled() {
		return admission.Allowed("")
	}

	// Per-Component Autoscaler block validation; see ServingRuntime.Handle.
	if err := validateRuntimeComponentAutoscalers(&clusterServingRuntime.Spec); err != nil {
		return admission.Denied(err.Error())
	}

	// Validate that all referenced accelerator classes exist
	if err := validateAcceleratorClasses(ctx, csr.Client, &clusterServingRuntime.Spec); err != nil {
		log.Info("Accelerator class validation failed", "name", clusterServingRuntime.Name, "error", err)
		return admission.Denied(err.Error())
	}

	// Spec-only checks run outside the loop so they still apply when no
	// other ClusterServingRuntimes exist.
	if err := validation.ValidateModelFormatPrioritySame(&clusterServingRuntime.Spec); err != nil {
		return admission.Denied(fmt.Sprintf(PriorityIsNotSameClusterServingRuntimeError, err.Error(), clusterServingRuntime.Name))
	}
	if err := validateServingRuntimeAnnotations(&clusterServingRuntime.Spec); err != nil {
		return admission.Denied(ChainsawInjectAnnotationNotAllowError)
	}

	for i := range existing.Items {
		// Pass the existing CSR's name from the loop as
		// existingRuntimeName so the `existingRuntimeName ==
		// newRuntimeName` early-exit in validateServingRuntimePriority
		// only short-circuits self-update (the loop iteration that hits
		// the same CSR we're admitting), not every cross-CSR pairing.
		if err := validateServingRuntimePriority(&clusterServingRuntime.Spec, &existing.Items[i].Spec, existing.Items[i].Name, clusterServingRuntime.Name); err != nil {
			return admission.Denied(fmt.Sprintf(InvalidPriorityClusterServingRuntimeError, err.Error(), existing.Items[i].Name, clusterServingRuntime.Name))
		}
	}
	return admission.Allowed("")
}

func areSupportedModelFormatsEqual(m1, m2 v1beta1.SupportedModelFormat) bool {
	return strings.EqualFold(m1.Name, m2.Name) &&
		ptrDerefEqual(m1.Version, m2.Version) &&
		ptrDerefEqual(m1.Quantization, m2.Quantization) &&
		ptrDerefEqual(m1.ModelFramework, m2.ModelFramework) &&
		ptrDerefEqual(m1.ModelFormat, m2.ModelFormat) &&
		ptrDerefEqual(m1.ModelArchitecture, m2.ModelArchitecture)
}

// ptrDerefEqual treats two nil pointers as equal and two non-nil
// pointers as equal iff their dereferenced values match. Mixed nil /
// non-nil ⇒ unequal. Avoids hand-rolling the same triple-clause check
// at every comparison site.
func ptrDerefEqual[T comparable](a, b *T) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

func areModelSizeRangesEqual(range1, range2 *v1beta1.ModelSizeRangeSpec) bool {
	if range1 == nil || range2 == nil {
		return range1 == range2
	}
	return ptrDerefEqual(range1.Min, range2.Min) && ptrDerefEqual(range1.Max, range2.Max)
}

// validateServingRuntimeAnnotations is a placeholder for future
// pod-template annotation validation. Today it accepts everything; the
// caller still wraps the result so an enforcement path is available
// without webhook rewiring.
func validateServingRuntimeAnnotations(*v1beta1.ServingRuntimeSpec) error {
	return nil
}

// validateServingRuntimePriority rejects two runtimes that auto-select on
// the same (model format, size range) at the same priority — the runtime
// selector has no tiebreaker. Disabled runtimes and self-updates are
// skipped (the latter relies on existingRuntimeName == newRuntimeName).
func validateServingRuntimePriority(newSpec, existingSpec *v1beta1.ServingRuntimeSpec, existingRuntimeName, newRuntimeName string) error {
	if existingSpec.IsDisabled() || existingRuntimeName == newRuntimeName {
		return nil
	}
	if !anyProtocolOverlap(existingSpec.ProtocolVersions, newSpec.ProtocolVersions) {
		return nil
	}
	for _, existingModelFormat := range existingSpec.SupportedModelFormats {
		for _, newModelFormat := range newSpec.SupportedModelFormats {
			if !existingModelFormat.IsAutoSelectEnabled() || !newModelFormat.IsAutoSelectEnabled() {
				continue
			}
			if !areSupportedModelFormatsEqual(existingModelFormat, newModelFormat) {
				continue
			}
			if !areModelSizeRangesEqual(existingSpec.ModelSizeRange, newSpec.ModelSizeRange) {
				continue
			}
			if existingModelFormat.Priority != nil && newModelFormat.Priority != nil &&
				*existingModelFormat.Priority == *newModelFormat.Priority {
				return fmt.Errorf(InvalidPriorityError, newModelFormat.Name)
			}
		}
	}
	return nil
}

// validateRuntimeScalingPolicy checks the policy and its component ownership.
// Updates to unrelated fields retain the ratchet for stored policy modes.
func validateRuntimeScalingPolicy(req admission.Request, spec *v1beta1.ServingRuntimeSpec,
	decodeOldSpec func() (*v1beta1.ServingRuntimeSpec, error)) *admission.Response {
	deny := func(err error) *admission.Response {
		resp := admission.Denied(err.Error())
		return &resp
	}
	if req.Operation == admissionv1.Update && len(req.OldObject.Raw) > 0 {
		oldSpec, err := decodeOldSpec()
		if err != nil {
			log.Error(err, "Failed to decode prior runtime for scaling-policy validation", "name", req.Name, "namespace", req.Namespace)
			resp := admission.Errored(http.StatusBadRequest, err)
			return &resp
		}
		if err := validation.ValidateScalingPolicyUpdate(oldSpec.ScalingPolicy, spec.ScalingPolicy, "spec.scalingPolicy"); err != nil {
			return deny(err)
		}
		if err := validation.ValidateScalingPolicyRuntimeUpdate(oldSpec, spec); err != nil {
			return deny(err)
		}
		return nil
	}
	if err := validation.ValidateScalingPolicy(spec.ScalingPolicy, "spec.scalingPolicy"); err != nil {
		return deny(err)
	}
	if err := validation.ValidateScalingPolicyRuntime(spec); err != nil {
		return deny(err)
	}
	return nil
}

// validateRuntimeComponentAutoscalers runs the shared per-Component
// Autoscaler shape check against each Component on a ServingRuntime
// spec. The per-Component dispatch is shared with the InferenceService
// and InferenceReplica webhooks via
// validation.ValidateComponentsAutoscalers.
// Mirrors the per-Component dispatch on the ISVC webhook so a
// runtime-level Autoscaler block carrying a malformed shape is rejected
// at admission rather than silently inherited into an ISVC at
// controller time.
func validateRuntimeComponentAutoscalers(spec *v1beta1.ServingRuntimeSpec) error {
	if spec == nil {
		return nil
	}
	var checks []validation.ComponentAutoscalerCheck
	for _, r := range v1beta1.Roles {
		piece := v1beta1.PieceOf(spec, r.Type)
		if piece == nil {
			continue
		}
		checks = append(checks, validation.ComponentAutoscalerCheck{
			Name:        r.RuntimeField,
			Autoscaler:  piece.Extension.Autoscaler,
			MinReplicas: piece.Extension.MinReplicas,
		})
	}
	return validation.ValidateComponentsAutoscalers(checks)
}

func anyProtocolOverlap(a, b []constants.InferenceServiceProtocol) bool {
	for _, p := range a {
		if contains(b, p) {
			return true
		}
	}
	return false
}

func contains[T comparable](slice []T, element T) bool {
	for _, item := range slice {
		if item == element {
			return true
		}
	}
	return false
}

// validateAcceleratorClasses checks that all accelerator classes referenced in the runtime spec exist.
// This is a strict validation to ensure that:
// 1. Typos in accelerator class names are caught early
// 2. Runtime scheduling won't fail due to missing accelerator definitions
// 3. Cluster operators can safely create runtimes knowing their accelerator dependencies are met
func validateAcceleratorClasses(ctx context.Context, c client.Reader, spec *v1beta1.ServingRuntimeSpec) error {
	if spec.AcceleratorRequirements == nil || len(spec.AcceleratorRequirements.AcceleratorClasses) == 0 {
		return nil
	}

	// Fetch all AcceleratorClasses in a single API call for better performance
	allClasses := &v1beta1.AcceleratorClassList{}
	if err := c.List(ctx, allClasses); err != nil {
		return fmt.Errorf("failed to list accelerator classes: %w", err)
	}

	// Build a set for O(1) lookup
	existingClasses := make(map[string]bool, len(allClasses.Items))
	for _, ac := range allClasses.Items {
		existingClasses[ac.Name] = true
	}

	// Collect all missing classes to report them together
	var missingClasses []string
	for _, className := range spec.AcceleratorRequirements.AcceleratorClasses {
		if !existingClasses[className] {
			missingClasses = append(missingClasses, className)
		}
	}

	if len(missingClasses) > 0 {
		return fmt.Errorf(UnknownAcceleratorClassError, missingClasses)
	}

	return nil
}
