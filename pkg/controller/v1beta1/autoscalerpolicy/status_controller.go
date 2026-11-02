// Package autoscalerpolicy registers the AutoscalerPolicy status controller:
// the shared policy status controller with the AutoscalerPolicy rules for
// references, the digest, readiness and the InUse wording.
package autoscalerpolicy

import (
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/autoscalerpolicy/render"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/policystatus"
)

// +kubebuilder:rbac:groups=ome.io,resources=autoscalerpolicies,verbs=get;list;watch
// +kubebuilder:rbac:groups=ome.io,resources=autoscalerpolicies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=ome.io,resources=inferenceservices,verbs=get;list;watch

// PolicyRefIndexField is the InferenceService field index keyed by the
// distinct policy names a service's components reference. The
// InferenceService controller registers the index; this controller only
// queries it.
const PolicyRefIndexField = "spec.autoscalerPolicyRefs"

// policyRefComponents is every component that can carry a policy reference,
// in the order the InferenceService controller's index extractor visits
// them.
var policyRefComponents = v1beta1.RoleTypes()

// PolicyRefIndexer is the extractor for PolicyRefIndexField: the distinct
// policy names across a service's components.
func PolicyRefIndexer(obj client.Object) []string {
	return policystatus.ReferencedPolicies[*v1beta1.AutoscalerPolicy](kind{}, obj)
}

// SetupWithManager registers the status controller. It owns
// AutoscalerPolicy.status and nothing else: a policy's status is an
// observation, and a status write can never start a scaler.
func SetupWithManager(mgr ctrl.Manager, clientset kubernetes.Interface, cache *controllerconfig.ConfigCache) error {
	return policystatus.SetupWithManager[*v1beta1.AutoscalerPolicy](mgr, "autoscalerpolicy-status", kind{},
		policystatus.ConfigSource{Clientset: clientset, Cache: cache})
}

// kind describes the AutoscalerPolicy kind to the shared status controller.
type kind struct{}

func (kind) NewPolicy() *v1beta1.AutoscalerPolicy { return &v1beta1.AutoscalerPolicy{} }

func (kind) ReferenceIndexField() string { return PolicyRefIndexField }

// References lists the policy each component references, one entry per
// component, so a service that names one policy from the engine and the
// decoder lists it twice.
func (kind) References(isvc *v1beta1.InferenceService) []string {
	var names []string
	for _, componentType := range policyRefComponents {
		ref := autoscaler.ComponentPolicyRef(isvc, componentType)
		if ref == nil || ref.Name == "" {
			continue
		}
		names = append(names, ref.Name)
	}
	return names
}

func (kind) Digest(policy *v1beta1.AutoscalerPolicy) (string, error) {
	return render.PortableDigest(&policy.Spec)
}

func (kind) Status(policy *v1beta1.AutoscalerPolicy) policystatus.StatusFields {
	return policystatus.StatusFields{
		ObservedGeneration: &policy.Status.ObservedGeneration,
		PortableDigest:     &policy.Status.PortableDigest,
		Attached:           &policy.Status.AttachedComponents,
		Conditions:         &policy.Status.Conditions,
	}
}

// Ready derives Ready from the validation that needs no cluster plus the one
// cluster-local input: every metric provider the triggers name must be bound
// in the operator configuration. The boolean reports whether the
// configuration was read. The error return is transient only (a
// configuration fetch); invalid content is a condition, never an error.
func (kind) Ready(policy *v1beta1.AutoscalerPolicy, config policystatus.ConfigSource, digestErr error) (metav1.Condition, bool, error) {
	issues := render.ValidateSpec(&policy.Spec)
	if digestErr != nil {
		issues = append([]render.Issue{{
			Reason: v1beta1.AutoscalerPolicyReasonParseError,
			Detail: fmt.Sprintf("compute portable digest: %v", digestErr),
		}}, issues...)
	}
	if len(issues) > 0 {
		return metav1.Condition{
			Type:               v1beta1.AutoscalerPolicyReadyCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: policy.Generation,
			Reason:             issues[0].Reason,
			Message:            issues[0].Detail,
		}, false, nil
	}

	unbound, consulted, err := firstUnboundProvider(&policy.Spec, config)
	if err != nil {
		return metav1.Condition{}, consulted, err
	}
	if unbound != "" {
		return metav1.Condition{
			Type:               v1beta1.AutoscalerPolicyReadyCondition,
			Status:             metav1.ConditionFalse,
			ObservedGeneration: policy.Generation,
			Reason:             v1beta1.AutoscalerPolicyReasonProviderUnknown,
			Message:            fmt.Sprintf("metric provider %q is not bound in the operator configuration", unbound),
		}, consulted, nil
	}

	return metav1.Condition{
		Type:               v1beta1.AutoscalerPolicyReadyCondition,
		Status:             metav1.ConditionTrue,
		ObservedGeneration: policy.Generation,
		Reason:             v1beta1.AutoscalerPolicyReasonTemplatesValid,
		Message:            "templates validate and every referenced metric provider is bound",
	}, consulted, nil
}

// firstUnboundProvider returns the first provider the triggers name that the
// operator configuration does not bind, or "" when every one is bound. The
// boolean reports whether the configuration was read at all: it is read only
// when the spec names a provider, so a policy with no endpoint triggers
// needs no operator configuration to go Ready.
func firstUnboundProvider(spec *v1beta1.AutoscalerPolicySpec, config policystatus.ConfigSource) (string, bool, error) {
	referenced := referencedProviders(spec)
	if len(referenced) == 0 {
		return "", false, nil
	}
	cfg, err := controllerconfig.NewAutoscalerPolicyConfigCached(config.Cache, config.Clientset)
	if err != nil {
		return "", true, fmt.Errorf("load autoscalerPolicy config: %w", err)
	}
	for _, name := range referenced {
		if _, ok := cfg.MetricProviders[name]; !ok {
			return name, true, nil
		}
	}
	return "", true, nil
}

// referencedProviders collects the distinct provider names the spec's
// triggers bind, in first-reference order.
func referencedProviders(spec *v1beta1.AutoscalerPolicySpec) []string {
	if spec.Keda == nil {
		return nil
	}
	seen := map[string]bool{}
	var names []string
	for i := range spec.Keda.Triggers {
		ref := spec.Keda.Triggers[i].ProviderRef
		if ref == nil || ref.Name == "" || seen[ref.Name] {
			continue
		}
		seen[ref.Name] = true
		names = append(names, ref.Name)
	}
	return names
}

func (kind) InUse(generation int64, attached int32) metav1.Condition {
	if attached > 0 {
		return metav1.Condition{
			Type:               v1beta1.AutoscalerPolicyInUseCondition,
			Status:             metav1.ConditionTrue,
			ObservedGeneration: generation,
			Reason:             v1beta1.AutoscalerPolicyReasonAttached,
			Message:            fmt.Sprintf("%d component(s) reference this policy", attached),
		}
	}
	return metav1.Condition{
		Type:               v1beta1.AutoscalerPolicyInUseCondition,
		Status:             metav1.ConditionFalse,
		ObservedGeneration: generation,
		Reason:             v1beta1.AutoscalerPolicyReasonNoConsumers,
		Message:            "no component references this policy",
	}
}
