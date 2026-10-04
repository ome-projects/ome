package render

import (
	"fmt"
	"strings"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// Context is the closed, controller-derived variable set one render sees.
// All values come from the consuming InferenceService and its component's
// EFFECTIVE bounds — on a placement-derived object, the per-home bounds —
// never from user-supplied free-form strings, so no escaping layer exists to
// get wrong.
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
	// ResolvedDigest identifies this render (per-home provenance).
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
// Pure and deterministic: same policy generation + context + providers →
// same output. Errors are fail-closed signals — the caller must keep the
// component's last-known-good scaler and raise a condition, never fall
// through to a different scaler class.
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

func renderKeda(cache *Cache, policy *v1beta1.AutoscalerPolicy, providers Providers, rctx Context) (*v1beta1.KedaAutoscaler, []string, error) {
	spec := policy.Spec.Keda
	compiled, err := cache.compiledFor(policy)
	if err != nil {
		return nil, nil, err
	}

	out := &v1beta1.KedaAutoscaler{
		Advanced:         spec.Advanced.DeepCopy(),
		PollingInterval:  copyInt32(spec.PollingInterval),
		CooldownPeriod:   copyInt32(spec.CooldownPeriod),
		IdleReplicaCount: copyInt32(spec.IdleReplicaCount),
	}

	var serverAddresses []string
	for i := range spec.Triggers {
		trigger := &spec.Triggers[i]
		metadata := make(map[string]string, len(trigger.Metadata)+1)
		for key, value := range trigger.Metadata {
			// Provider-owned keys are rejected at admission; refuse them here
			// too so a skew-admitted policy cannot smuggle an endpoint in.
			if IsForbiddenMetadataKey(key) {
				return nil, nil, fmt.Errorf("trigger %d metadata key %q is provider-owned and not allowed in policies", i, key)
			}
			tmpl, ok := compiled.templates[templateKey(i, key)]
			if !ok {
				// Cache entries are compiled from the same generation; a miss
				// means the caller bypassed compilation — compile on the spot.
				tmpl, err = parseMetadataTemplate(templateKey(i, key), value)
				if err != nil {
					return nil, nil, fmt.Errorf("trigger %d metadata %q: %w", i, key, err)
				}
			}
			var sb strings.Builder
			if err := tmpl.Execute(&sb, rctx); err != nil {
				return nil, nil, fmt.Errorf("trigger %d metadata %q: render: %w", i, key, err)
			}
			metadata[key] = sb.String()
		}

		renderedTrigger := kedav1.ScaleTriggers{
			Type:       trigger.Type,
			Metadata:   metadata,
			MetricType: trigger.MetricType,
		}

		if IsEndpointTriggerType(trigger.Type) {
			if trigger.ProviderRef == nil || trigger.ProviderRef.Name == "" {
				return nil, nil, fmt.Errorf("trigger %d (%s): providerRef is required for endpoint trigger types", i, trigger.Type)
			}
			binding, ok := providers[trigger.ProviderRef.Name]
			if !ok || binding.ServerAddress == "" {
				return nil, nil, fmt.Errorf("trigger %d (%s): metric provider %q is not bound on this cluster", i, trigger.Type, trigger.ProviderRef.Name)
			}
			metadata["serverAddress"] = binding.ServerAddress
			serverAddresses = append(serverAddresses, binding.ServerAddress)
			if binding.AuthenticationName != "" {
				renderedTrigger.AuthenticationRef = &kedav1.AuthenticationRef{Name: binding.AuthenticationName}
				// The provider binding owns auth wholesale: the mode is fixed
				// to bearer alongside the materialized TriggerAuthentication.
				metadata["authModes"] = "bearer"
			}
		}

		out.Triggers = append(out.Triggers, renderedTrigger)
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

// resolveReplicaValue materializes a typed ReplicaValueSource against the
// effective bounds. Typed resolution, never string templates: the target
// fields are int32 and round-tripping numbers through templates would add a
// parse failure mode for no expressiveness.
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
