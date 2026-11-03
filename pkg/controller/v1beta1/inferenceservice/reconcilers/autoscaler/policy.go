package autoscaler

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/autoscalerpolicy/render"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
)

// Bounds is one component's effective replica band: the values dispatch
// stamps into the generated scaler AND the values policy templates render
// against. One shared computation, so rendered literals and stamped bounds
// cannot disagree within a reconcile.
type Bounds struct {
	Min int32
	Max int32
}

// EffectiveComponentBounds is the exported form of the dispatch bounds
// computation (min nil/<=0 -> 1; max clamped up to min).
func EffectiveComponentBounds(c *v1beta1.ComponentExtensionSpec) Bounds {
	minR, maxR := minMaxReplicas(c)
	return Bounds{Min: minR, Max: maxR}
}

// RawEffectiveComponentBounds mirrors the RawDeployment dispatch bounds
// (rawMinMaxReplicas) for the render side: a declared min of 0 is kept —
// scale-to-zero is legal for typed KEDA on Raw — instead of floored to 1,
// so a policy template's {{ .MinReplicas }} renders the same number the
// dispatch will stamp. Whether min 0 is actually dispatchable stays the
// dispatcher's call; render only needs the matching arithmetic.
func RawEffectiveComponentBounds(c *v1beta1.ComponentExtensionSpec) Bounds {
	minR := defaultDispatchMinReplicas
	if c != nil && c.MinReplicas != nil && *c.MinReplicas >= 0 {
		minR = int32(*c.MinReplicas)
	}
	var maxR int32
	if c != nil && c.MaxReplicas >= 0 {
		maxR = int32(c.MaxReplicas)
	}
	if maxR < minR {
		maxR = minR
	}
	if maxR < defaultDispatchMinReplicas {
		maxR = defaultDispatchMinReplicas
	}
	return Bounds{Min: minR, Max: maxR}
}

// PolicyOutcome is the result of resolving one component's policy ref.
// Exactly one of these states holds:
//   - Ref == nil: the component has no ref; the ordinary chain applies.
//   - Rendered != nil: the policy rendered; Provenance carries the digests.
//   - Hold == true: the ref is set but no block could be produced — the
//     caller fails closed (keep the last-known-good scaler, raise the
//     AutoscalerResolved condition with HoldReason, never fall through to a
//     different scaler class).
type PolicyOutcome struct {
	Ref        *v1beta1.AutoscalerPolicyRef
	Rendered   *v1beta1.ComponentAutoscaler
	Provenance *v1beta1.AutoscalerPolicyProvenance

	Hold       bool
	HoldReason string
	HoldDetail string
}

// RawResolved bundles one RawDeployment component's resolved autoscaler with
// its policy-layer state for the dispatch path.
type RawResolved struct {
	Autoscaler *v1beta1.ComponentAutoscaler
	FromPolicy bool
	Hold       bool

	// RequeueAfter is the periodic retry for a hold (see
	// PolicyResolver.HoldRequeueAfter). Zero when resolution succeeded or
	// the periodic requeue is disabled.
	RequeueAfter time.Duration
}

// PolicyResolver fetches and renders a component's referenced
// AutoscalerPolicy. One instance is built per reconcile from the cached
// operator config, so provider-binding edits take effect within one config
// TTL without a controller restart.
type PolicyResolver struct {
	Client client.Client

	// Providers are the cluster-local bindings for logical provider names.
	Providers render.Providers

	// ProviderAuth maps provider names to their bearer-token secret refs;
	// consumed by EnsureTriggerAuthentications.
	ProviderAuth map[string]*corev1.SecretKeySelector

	// Enabled reports whether the AutoscalerPolicy CRD is installed and the
	// feature is on. When false, a ref holds (fail closed) rather than
	// silently resolving down the chain — admission rejects refs on
	// non-enabled clusters, so this branch only fires on version skew.
	Enabled bool

	// KedaAvailable reports whether the KEDA CRDs are installed. A rendered
	// KEDA block on a KEDA-less cluster holds with ClassUnavailable instead
	// of failing dispatch on every pass.
	KedaAvailable bool

	// HoldRequeueAfter is the periodic requeue consumers apply while a ref
	// holds. Holds caused by operator-config state (e.g. an unbound provider
	// binding) produce no watch event toward the consumer, so the reconcile
	// re-runs once per operator-config cache TTL to observe the fix.
	// Non-positive disables the periodic requeue: with caching off every
	// resolve reads config live, so convergence on the next natural event
	// (or a manual touch) is acceptable.
	HoldRequeueAfter time.Duration
}

// ProviderBindingsFromConfig translates the operator config block into
// render bindings plus the auth map. TriggerAuthentication names are
// deterministic per provider, so rendering can wire authenticationRef
// without a cluster read.
func ProviderBindingsFromConfig(cfg *controllerconfig.AutoscalerPolicyConfig) (render.Providers, map[string]*corev1.SecretKeySelector) {
	providers := render.Providers{}
	auth := map[string]*corev1.SecretKeySelector{}
	if cfg == nil {
		return providers, auth
	}
	for name, provider := range cfg.MetricProviders {
		binding := render.ProviderBinding{ServerAddress: provider.ServerAddress}
		if provider.AuthSecretRef != nil {
			binding.AuthenticationName = TriggerAuthenticationName(name)
			auth[name] = provider.AuthSecretRef.DeepCopy()
		}
		providers[name] = binding
	}
	return providers, auth
}

// TriggerAuthenticationName is the deterministic name of the materialized
// per-namespace TriggerAuthentication for a logical provider.
func TriggerAuthenticationName(provider string) string {
	return "ome-metric-provider-" + provider
}

// Resolve produces the PolicyOutcome for one component. The error return is
// TRANSIENT only (apiserver failures) and must surface as a reconcile error
// so the normal retry path runs. Terminal states — NotFound, invalid
// content, unbound providers — come back as a Hold outcome instead: they do
// not heal by retrying, recovery is watch-driven when the policy changes,
// and returning them as errors would requeue-with-backoff forever and stall
// every other concern the reconcile owns (image updates, replica edits, IR
// projection) across all consumers of one shared object.
func (r *PolicyResolver) Resolve(ctx context.Context, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, bounds Bounds) (*PolicyOutcome, error) {
	ref := ComponentPolicyRef(isvc, component)
	if ref == nil {
		return nil, nil
	}
	outcome := &PolicyOutcome{Ref: ref.DeepCopy()}

	if r == nil || !r.Enabled {
		return hold(outcome, v1beta1.AutoscalerResolvedReasonPolicyNotFound,
			"the AutoscalerPolicy feature is not enabled on this cluster"), nil
	}
	if ref.Kind != "" && ref.Kind != "AutoscalerPolicy" {
		return hold(outcome, v1beta1.AutoscalerResolvedReasonPolicyInvalid,
			fmt.Sprintf("unsupported policy kind %q (ClusterAutoscalerPolicy is reserved)", ref.Kind)), nil
	}

	policy := &v1beta1.AutoscalerPolicy{}
	if err := r.Client.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: ref.Name}, policy); err != nil {
		if apierrors.IsNotFound(err) {
			return hold(outcome, v1beta1.AutoscalerResolvedReasonPolicyNotFound,
				fmt.Sprintf("AutoscalerPolicy %q not found in namespace %s", ref.Name, isvc.Namespace)), nil
		}
		return nil, fmt.Errorf("fetch AutoscalerPolicy %s/%s: %w", isvc.Namespace, ref.Name, err)
	}

	result, err := render.Render(policy, r.Providers, render.Context{
		Namespace:   isvc.Namespace,
		ISVCName:    isvc.Name,
		Component:   string(component),
		MinReplicas: bounds.Min,
		MaxReplicas: bounds.Max,
		// Matches the InferenceReplica naming formula (<isvc>-<component>),
		// which is also the scale-target name on the OMENative path.
		TargetName: isvc.Name + "-" + string(component),
	})
	if err != nil {
		return hold(outcome, v1beta1.AutoscalerResolvedReasonPolicyInvalid, err.Error()), nil
	}
	if result.Autoscaler.Class == v1beta1.AutoscalerKEDA && !r.KedaAvailable {
		return hold(outcome, v1beta1.AutoscalerResolvedReasonClassUnavailable,
			fmt.Sprintf("policy %q renders class KEDA but the KEDA CRDs are not installed", ref.Name)), nil
	}

	outcome.Rendered = result.Autoscaler
	outcome.Provenance = &v1beta1.AutoscalerPolicyProvenance{
		Name:               policy.Name,
		ObservedGeneration: policy.Generation,
		PortableDigest:     result.PortableDigest,
		ResolvedDigest:     result.ResolvedDigest,
	}
	return outcome, nil
}

func hold(outcome *PolicyOutcome, reason, detail string) *PolicyOutcome {
	outcome.Hold = true
	outcome.HoldReason = reason
	outcome.HoldDetail = detail
	return outcome
}

// ComponentPolicyRef dereferences the per-component policy ref. Nil when the
// ISVC, the component, or the ref is unset.
func ComponentPolicyRef(isvc *v1beta1.InferenceService, component v1beta1.ComponentType) *v1beta1.AutoscalerPolicyRef {
	if isvc == nil {
		return nil
	}
	if view := v1beta1.SpecOf(&isvc.Spec, component); view != nil {
		return view.Extension.AutoscalerPolicyRef
	}
	return nil
}

// EnsureTriggerAuthentications materializes one KEDA TriggerAuthentication
// per auth-bearing provider in the consumer namespace, wiring the operator-
// configured secret ref as a bearer token. Idempotent; called from the
// dispatch path (never the status writer) before a rendered block that uses
// an authenticated provider is dispatched. Secrets are never read here —
// KEDA resolves them at scrape time — so no token material transits OME.
func (r *PolicyResolver) EnsureTriggerAuthentications(ctx context.Context, namespace string, rendered *v1beta1.ComponentAutoscaler) error {
	if r == nil || rendered == nil || rendered.Keda == nil || len(r.ProviderAuth) == 0 {
		return nil
	}
	needed := map[string]*corev1.SecretKeySelector{}
	for i := range rendered.Keda.Triggers {
		authRef := rendered.Keda.Triggers[i].AuthenticationRef
		if authRef == nil {
			continue
		}
		for provider, secretRef := range r.ProviderAuth {
			if TriggerAuthenticationName(provider) == authRef.Name {
				needed[authRef.Name] = secretRef
			}
		}
	}
	for name, secretRef := range needed {
		desired := &kedav1.TriggerAuthentication{
			ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
			Spec: kedav1.TriggerAuthenticationSpec{
				SecretTargetRef: []kedav1.AuthSecretTargetRef{{
					Parameter: "bearerToken",
					Name:      secretRef.Name,
					Key:       secretRef.Key,
				}},
			},
		}
		existing := &kedav1.TriggerAuthentication{}
		err := r.Client.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, existing)
		switch {
		case apierrors.IsNotFound(err):
			if err := r.Client.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
				return fmt.Errorf("create TriggerAuthentication %s/%s: %w", namespace, name, err)
			}
		case err != nil:
			return fmt.Errorf("fetch TriggerAuthentication %s/%s: %w", namespace, name, err)
		default:
			if !equality.Semantic.DeepEqual(existing.Spec, desired.Spec) {
				existing.Spec = desired.Spec
				if err := r.Client.Update(ctx, existing); err != nil {
					return fmt.Errorf("update TriggerAuthentication %s/%s: %w", namespace, name, err)
				}
			}
		}
	}
	return nil
}

// MarshalLastRendered encodes a rendered block for the RawDeployment
// last-known-good annotation.
func MarshalLastRendered(block *v1beta1.ComponentAutoscaler) (string, error) {
	encoded, err := json.Marshal(block)
	if err != nil {
		return "", fmt.Errorf("encode last-rendered autoscaler: %w", err)
	}
	return string(encoded), nil
}

// UnmarshalLastRendered decodes the RawDeployment last-known-good
// annotation. Empty input yields nil (no LKG recorded).
func UnmarshalLastRendered(annotation string) (*v1beta1.ComponentAutoscaler, error) {
	if annotation == "" {
		return nil, nil
	}
	block := &v1beta1.ComponentAutoscaler{}
	if err := json.Unmarshal([]byte(annotation), block); err != nil {
		return nil, fmt.Errorf("decode last-rendered autoscaler annotation: %w", err)
	}
	return block, nil
}
