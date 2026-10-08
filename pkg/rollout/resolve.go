package rollout

import (
	"errors"
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

// Policies is what one reconcile observed of the RolloutPolicy objects an
// InferenceService's rollout groups reference. The run layer observes them
// once per pass and hands the same set to the run opener and to the
// effective view, so every reader resolves a reference the same way. The
// zero value observed nothing: no reference resolves through it.
type Policies struct {
	// Namespace is the InferenceService's namespace, where its references
	// are looked up.
	Namespace string
	// Enabled reports that the policy surface is installed on the cluster.
	// A reference cannot resolve without it, whatever ByName holds.
	Enabled bool
	// ByName holds every referenced policy that exists, keyed by name.
	ByName map[string]*v1beta1.RolloutPolicy
	// Invalid holds, per name, why an existing policy's body cannot be
	// pinned. The policy stays in ByName: status still reports its digest.
	Invalid map[string]error
}

// PoliciesOf is the set of the given policies, observed valid, in namespace.
func PoliciesOf(namespace string, policies ...*v1beta1.RolloutPolicy) Policies {
	out := Policies{Namespace: namespace, Enabled: true, ByName: map[string]*v1beta1.RolloutPolicy{}}
	for _, p := range policies {
		if p != nil {
			out.ByName[p.Name] = p
		}
	}
	return out
}

// Resolved is one spec group as the executors consume it: the group with
// exactly one inline progression and no reference, plus where the
// progression came from.
type Resolved struct {
	Group            v1beta1.RolloutGroup
	Source           v1beta1.RolloutPlanSource
	PolicyRef        *v1beta1.RolloutPolicyRef
	PolicyGeneration int64
}

// Unresolved is a group whose reference did not become a body. Reason is
// the RolloutPlanReady reason the run layer parks under; the effective view
// keeps such a group as declared, with no executable body.
type Unresolved struct {
	Reason  string
	Message string
}

func (u *Unresolved) Error() string { return u.Message }

// ResolveGroup is the one place a rollout group's policy reference becomes
// a body; the run opener and the effective view both resolve through it.
// An inline progression outranks a coexisting reference, and a group with
// neither resolves to the default blueGreen. index names the group in
// messages. A reference that does not resolve returns an *Unresolved.
func ResolveGroup(index int, g *v1beta1.RolloutGroup, policies Policies) (Resolved, error) {
	if rolloutpolicy.GroupSource(g) == v1beta1.RolloutPlanSourceInline {
		composed, err := rolloutpolicy.ComposeGroup(g, nil)
		if err != nil {
			return Resolved{}, &Unresolved{Reason: v1beta1.RolloutPlanReasonPlanInvalid, Message: fmt.Sprintf("groups[%d]: %v", index, err)}
		}
		return Resolved{Group: composed, Source: v1beta1.RolloutPlanSourceInline}, nil
	}
	name := g.PolicyRef.Name
	if !policies.Enabled {
		return Resolved{}, &Unresolved{Reason: v1beta1.RolloutPlanReasonPlanInvalid,
			Message: fmt.Sprintf("groups[%d].policyRef %q: the rollout policy feature is not enabled on this cluster, so the ref cannot resolve", index, name)}
	}
	policy := policies.ByName[name]
	if policy == nil {
		return Resolved{}, &Unresolved{Reason: v1beta1.RolloutPlanReasonPolicyNotFound,
			Message: fmt.Sprintf("groups[%d].policyRef %q: RolloutPolicy not found in namespace %s", index, name, policies.Namespace)}
	}
	if verr := policies.Invalid[name]; verr != nil {
		return Resolved{}, &Unresolved{Reason: v1beta1.RolloutPlanReasonPolicyNotReady,
			Message: fmt.Sprintf("groups[%d].policyRef %q: policy body is invalid: %v", index, name, verr)}
	}
	composed, err := rolloutpolicy.ComposeGroup(g, &policy.Spec)
	if err != nil {
		reason := v1beta1.RolloutPlanReasonPlanInvalid
		if errors.Is(err, rolloutpolicy.ErrProgressionMismatch) {
			reason = v1beta1.RolloutPlanReasonProgressionMismatch
		}
		return Resolved{}, &Unresolved{Reason: reason, Message: fmt.Sprintf("groups[%d]: %v", index, err)}
	}
	return Resolved{
		Group:            composed,
		Source:           v1beta1.RolloutPlanSourcePolicy,
		PolicyRef:        g.PolicyRef.DeepCopy(),
		PolicyGeneration: policy.Generation,
	}, nil
}

// resolveLive is the live spec with every policy-sourced group replaced by
// its resolved body. A group whose reference does not resolve stays as
// declared, with no executable body: its Components hold until it does. The
// spec itself is returned when no group resolves.
func resolveLive(spec *v1beta1.RolloutSpec, policies Policies) *v1beta1.RolloutSpec {
	if spec == nil {
		return nil
	}
	var out *v1beta1.RolloutSpec
	for i := range spec.Groups {
		g := &spec.Groups[i]
		if rolloutpolicy.GroupSource(g) != v1beta1.RolloutPlanSourcePolicy {
			continue
		}
		res, err := ResolveGroup(i, g, policies)
		if err != nil {
			continue
		}
		if out == nil {
			copied := *spec
			copied.Groups = append([]v1beta1.RolloutGroup(nil), spec.Groups...)
			out = &copied
		}
		out.Groups[i] = res.Group
	}
	if out == nil {
		return spec
	}
	return out
}
