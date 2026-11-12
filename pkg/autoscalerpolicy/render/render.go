package render

import (
	"fmt"
	"strings"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Context is the closed, controller-derived variable set one render sees.
// Every value comes from the consuming InferenceService and its component's
// effective bounds (on a placement-derived object, the bounds of that member
// cluster), never from user-supplied free-form strings, so there is no
// escaping layer to get wrong.
type Context struct {
	Namespace   string
	ISVCName    string
	Component   string
	MinReplicas int32
	MaxReplicas int32
	// TargetName is the scale-target name (<isvc>-<component>).
	TargetName string
}

// ProviderBinding is the cluster-local resolution of a logical
// MetricProviderRef name, supplied by operator configuration. Policies never
// carry endpoints or credentials.
type ProviderBinding struct {
	// ServerAddress is injected as the trigger's serverAddress.
	ServerAddress string
	// AuthenticationName, when set, is wired as the rendered trigger's
	// authenticationRef (a TriggerAuthentication in the consumer namespace,
	// materialized by the controller from the provider's auth secret ref).
	AuthenticationName string
}

// Providers maps logical provider names to their cluster-local bindings.
type Providers map[string]ProviderBinding

// Result is one successful render.
type Result struct {
	// Autoscaler is a verbatim ComponentAutoscaler, indistinguishable in
	// shape from an inline block: downstream dispatch, ownership, and GC are
	// untouched by where the block came from.
	Autoscaler *v1beta1.ComponentAutoscaler
	// PortableDigest identifies the policy content (cluster-portable).
	PortableDigest string
	// ResolvedDigest identifies this render on this member cluster.
	ResolvedDigest string
}

// IsEndpointTriggerType reports whether a KEDA trigger type reaches a
// network endpoint and therefore requires a provider binding ("prometheus"
// in v1). Admission enforces ProviderRef presence for these types; the
// render layer re-checks so a skew-admitted policy still cannot produce an
// unbound endpoint trigger.
func IsEndpointTriggerType(triggerType string) bool {
	return strings.EqualFold(triggerType, "prometheus")
}

// Render materializes one component's ComponentAutoscaler from a policy.
// Pure and deterministic: the same policy generation, context and providers
// give the same output. An error is a fail-closed signal: the caller keeps
// the component's last-known-good scaler and raises a condition, and never
// falls through to a different scaler class.
func Render(policy *v1beta1.AutoscalerPolicy, providers Providers, rctx Context) (*Result, error) {
	return RenderWithCache(DefaultCache, policy, providers, rctx)
}

// RenderWithCache is Render with an explicit template cache (tests, CLI).
func RenderWithCache(cache *Cache, policy *v1beta1.AutoscalerPolicy, providers Providers, rctx Context) (*Result, error) {
	if policy == nil {
		return nil, fmt.Errorf("nil policy")
	}
	// Bounds must be effective (post-defaulting, post-derivation): an
	// unbounded max would render a scaler with no ceiling.
	if rctx.MaxReplicas <= 0 {
		return nil, fmt.Errorf("component %s has effective maxReplicas %d; policy rendering requires a positive ceiling", rctx.Component, rctx.MaxReplicas)
	}

	portable, err := PortableDigest(&policy.Spec)
	if err != nil {
		return nil, err
	}

	var rendered *v1beta1.ComponentAutoscaler
	var serverAddresses []string
	switch policy.Spec.Class {
	case v1beta1.AutoscalerHPA:
		rendered = &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerHPA,
			HPA:   policy.Spec.HPA.DeepCopy(),
		}
	case v1beta1.AutoscalerKEDA:
		if policy.Spec.Keda == nil {
			return nil, fmt.Errorf("policy %s: class KEDA with no keda template", policy.Name)
		}
		keda, addresses, err := renderKeda(cache, policy, providers, rctx)
		if err != nil {
			return nil, fmt.Errorf("policy %s: %w", policy.Name, err)
		}
		rendered = &v1beta1.ComponentAutoscaler{
			Class: v1beta1.AutoscalerKEDA,
			Keda:  keda,
		}
		serverAddresses = addresses
	default:
		return nil, fmt.Errorf("policy %s: unsupported class %q", policy.Name, policy.Spec.Class)
	}

	resolved, err := resolvedDigestFor(rendered, rctx, serverAddresses)
	if err != nil {
		return nil, err
	}
	return &Result{Autoscaler: rendered, PortableDigest: portable, ResolvedDigest: resolved}, nil
}

// kedaRender is one render of a KEDA template: the policy's compiled
// metadata templates, the cluster's provider bindings and the variables.
type kedaRender struct {
	compiled  *compiledPolicy
	providers Providers
	rctx      Context
}

// renderKeda renders the policy's KEDA template for one component: the
// scaler settings copied, every trigger rendered, and the fallback count
// resolved against the bounds. It also returns the server addresses the
// endpoint triggers were bound to, which enter the resolved digest.
func renderKeda(cache *Cache, policy *v1beta1.AutoscalerPolicy, providers Providers, rctx Context) (*v1beta1.KedaAutoscaler, []string, error) {
	spec := policy.Spec.Keda
	compiled, err := cache.compiledFor(policy)
	if err != nil {
		return nil, nil, err
	}
	render := kedaRender{compiled: compiled, providers: providers, rctx: rctx}

	out := &v1beta1.KedaAutoscaler{
		Advanced:         spec.Advanced.DeepCopy(),
		PollingInterval:  copyInt32(spec.PollingInterval),
		CooldownPeriod:   copyInt32(spec.CooldownPeriod),
		IdleReplicaCount: copyInt32(spec.IdleReplicaCount),
	}

	var serverAddresses []string
	for i := range spec.Triggers {
		trigger, serverAddress, err := render.trigger(i, &spec.Triggers[i])
		if err != nil {
			return nil, nil, err
		}
		if serverAddress != "" {
			serverAddresses = append(serverAddresses, serverAddress)
		}
		out.Triggers = append(out.Triggers, trigger)
	}

	if spec.Fallback != nil {
		replicas, err := resolveReplicaValue(&spec.Fallback.Replicas, rctx)
		if err != nil {
			return nil, nil, fmt.Errorf("fallback.replicas: %w", err)
		}
		out.Fallback = &kedav1.Fallback{
			FailureThreshold: spec.Fallback.FailureThreshold,
			Replicas:         replicas,
		}
	}
	return out, serverAddresses, nil
}

// trigger renders one trigger: its metadata through the templates, then,
// for an endpoint trigger, the provider's address and authentication. The
// bound server address is returned beside the trigger, empty when the
// trigger reaches no endpoint.
func (r kedaRender) trigger(index int, trigger *v1beta1.KedaTriggerTemplate) (kedav1.ScaleTriggers, string, error) {
	metadata, err := r.metadata(index, trigger.Metadata)
	if err != nil {
		return kedav1.ScaleTriggers{}, "", err
	}
	rendered := kedav1.ScaleTriggers{
		Type:       trigger.Type,
		Metadata:   metadata,
		MetricType: trigger.MetricType,
	}
	if !IsEndpointTriggerType(trigger.Type) {
		return rendered, "", nil
	}
	binding, err := r.boundProvider(index, trigger)
	if err != nil {
		return kedav1.ScaleTriggers{}, "", err
	}
	metadata["serverAddress"] = binding.ServerAddress
	if binding.AuthenticationName != "" {
		rendered.AuthenticationRef = &kedav1.AuthenticationRef{Name: binding.AuthenticationName}
		// The provider binding owns authentication as a whole: the mode is
		// bearer, beside the TriggerAuthentication the controller materializes.
		metadata["authModes"] = "bearer"
	}
	return rendered, binding.ServerAddress, nil
}

// metadata renders one trigger's metadata values against the variables. A
// provider-owned key is refused here as well as at admission, so a policy
// an older webhook admitted cannot carry an endpoint.
func (r kedaRender) metadata(index int, values map[string]string) (map[string]string, error) {
	metadata := make(map[string]string, len(values)+1)
	for key, value := range values {
		if IsForbiddenMetadataKey(key) {
			return nil, fmt.Errorf("trigger %d metadata key %q is provider-owned and not allowed in policies", index, key)
		}
		rendered, err := r.value(index, key, value)
		if err != nil {
			return nil, err
		}
		metadata[key] = rendered
	}
	return metadata, nil
}

// value executes one metadata template. The cache compiles every template
// of a policy generation; a key it does not hold is compiled on the spot.
func (r kedaRender) value(index int, key, value string) (string, error) {
	tmpl, ok := r.compiled.templates[templateKey(index, key)]
	if !ok {
		var err error
		tmpl, err = parseMetadataTemplate(templateKey(index, key), value)
		if err != nil {
			return "", fmt.Errorf("trigger %d metadata %q: %w", index, key, err)
		}
	}
	var sb strings.Builder
	if err := tmpl.Execute(&sb, r.rctx); err != nil {
		return "", fmt.Errorf("trigger %d metadata %q: render: %w", index, key, err)
	}
	return sb.String(), nil
}

// boundProvider is the cluster-local binding of an endpoint trigger's
// provider; the trigger must name one and the cluster must bind it.
func (r kedaRender) boundProvider(index int, trigger *v1beta1.KedaTriggerTemplate) (ProviderBinding, error) {
	if trigger.ProviderRef == nil || trigger.ProviderRef.Name == "" {
		return ProviderBinding{}, fmt.Errorf("trigger %d (%s): providerRef is required for endpoint trigger types", index, trigger.Type)
	}
	binding, ok := r.providers[trigger.ProviderRef.Name]
	if !ok || binding.ServerAddress == "" {
		return ProviderBinding{}, fmt.Errorf("trigger %d (%s): metric provider %q is not bound on this cluster", index, trigger.Type, trigger.ProviderRef.Name)
	}
	return binding, nil
}

// resolveReplicaValue materializes a typed ReplicaValueSource against the
// effective bounds. The target fields are int32, so the value is resolved
// as a number and never through a string template.
func resolveReplicaValue(source *v1beta1.ReplicaValueSource, rctx Context) (int32, error) {
	switch {
	case source.Value != nil && source.FromComponent == nil:
		return *source.Value, nil
	case source.FromComponent != nil && source.Value == nil:
		switch *source.FromComponent {
		case v1beta1.BoundsFieldMaxReplicas:
			return rctx.MaxReplicas, nil
		case v1beta1.BoundsFieldMinReplicas:
			return rctx.MinReplicas, nil
		default:
			return 0, fmt.Errorf("unknown component bounds field %q", *source.FromComponent)
		}
	default:
		return 0, fmt.Errorf("exactly one of value or fromComponent must be set")
	}
}

func copyInt32(v *int32) *int32 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}
