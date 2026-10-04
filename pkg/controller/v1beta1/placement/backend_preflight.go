package placement

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/placement/plan"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/placement/resolution"
)

var errMemberAbsent = errors.New("standing member disappeared before refresh")

type backendContextKey struct{}
type backendTargetKey struct{}
type backendTarget struct {
	cluster   string
	uid       types.UID
	transport workloadcluster.SelectivelyCachingClient
}

type backendPreflight struct {
	sourceUID types.UID
	clusters  map[string]types.UID
	condition policyCondition
}

func backendCondition(status corev1.ConditionStatus, reason, message string) policyCondition {
	return policyCondition{condType: v1beta1.PlacementBackendReady, cond: apis.Condition{Type: v1beta1.PlacementBackendReady, Status: status, Reason: reason, Message: message}}
}

func unverifiedBackend(ctx context.Context, source *v1beta1.InferenceService) context.Context {
	return context.WithValue(ctx, backendContextKey{}, &backendPreflight{sourceUID: source.UID, condition: backendCondition(corev1.ConditionUnknown, "BackendNotObserved", "Member runtime backends have not been verified in this reconcile")})
}

// preflightBackends verifies runtime inputs independently of hardware reports.
// Per-member failures restrict eligibility; capacity needs the entire matched set.
func (r *Reconciler) preflightBackends(ctx context.Context, source *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, candidates []string) (context.Context, error) {
	state := &backendPreflight{sourceUID: source.UID, clusters: map[string]types.UID{}, condition: backendCondition(corev1.ConditionTrue, "OMENativeResolved", "Every target component resolves to OMENative")}
	desired, err := r.derivedFor(source)
	if err == nil {
		err = r.resolveBackendTargets(ctx, source, desired, clusters, candidates, state)
	}
	if err != nil {
		status, reason := corev1.ConditionUnknown, "BackendUnknown"
		if errors.Is(err, resolution.ErrUnsupportedBackend) {
			status, reason = corev1.ConditionFalse, "UnsupportedPlacementBackend"
		}
		state.condition = backendCondition(status, reason, err.Error())
		if placementMode(source) != v1beta1.PlacementModeSplitByCapacity && len(state.clusters) > 0 {
			err = nil
		}
	}
	return context.WithValue(ctx, backendContextKey{}, state), err
}

func (r *Reconciler) resolveBackendTargets(ctx context.Context, source, desired *v1beta1.InferenceService, clusters []v1beta1.WorkloadCluster, candidates []string, state *backendPreflight) error {
	selector, err := placementSelector(source)
	if err != nil {
		return err
	}
	split := placementMode(source) == v1beta1.PlacementModeSplit || placementMode(source) == v1beta1.PlacementModeSplitByCapacity
	verified := map[string]*resolution.Runtime{}
	seen := map[string]bool{}
	var failures []error
	for _, cluster := range clusters {
		_, matched := selector.Match(&cluster)
		if !cluster.DeletionTimestamp.IsZero() || !matched || (!split && !slices.Contains(candidates, cluster.Name)) {
			continue
		}
		if seen[cluster.Name] {
			return fmt.Errorf("duplicate backend target %q", cluster.Name)
		}
		seen[cluster.Name] = true
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		cl, err := r.plannedClient(cctx, cluster.Name, cluster.UID)
		var backend *resolution.Runtime
		if err == nil {
			backend, err = r.resolveMemberBackend(cctx, cl, source, desired)
		}
		cancel()
		if err != nil {
			failures = append(failures, fmt.Errorf("cluster %q backend: %w", cluster.Name, err))
			continue
		}
		state.clusters[cluster.Name], verified[cluster.Name] = cluster.UID, backend
	}
	for _, name := range slices.Sorted(maps.Keys(verified)) {
		backend := verified[name]
		cctx, cancel := context.WithTimeout(ctx, r.placeTimeout())
		err := backend.Check(cctx)
		if err == nil {
			_, err = r.plannedClient(cctx, name, state.clusters[name])
		}
		cancel()
		if err != nil {
			delete(state.clusters, name)
			failures = append(failures, fmt.Errorf("cluster %q backend inputs changed: %w", name, err))
		}
	}
	return errors.Join(failures...)
}

func (r *Reconciler) resolveMemberBackend(ctx context.Context, cl client.Client, source, desired *v1beta1.InferenceService) (*resolution.Runtime, error) {
	standing, err := getMemberService(ctx, cl, client.ObjectKeyFromObject(desired))
	if err != nil {
		return nil, err
	}
	if standing != nil && !isOurDerived(standing, source) {
		return nil, fmt.Errorf("backend target is not owned by this placement source")
	}
	return r.resolveStandingBackend(ctx, cl, desired, standing)
}

// resolveStandingBackend resolves the member runtime for desired against the
// owned member service standing, or against its absence when standing is nil.
func (r *Reconciler) resolveStandingBackend(ctx context.Context, cl client.Client, desired, standing *v1beta1.InferenceService) (*resolution.Runtime, error) {
	resolver := resolution.Resolver{Client: cl, OperatorNamespace: r.MemberOperatorNamespace}
	policy, err := protocol.FromDerived(desired)
	if err != nil {
		return nil, err
	}
	if policy != nil && len(policy.ReplicaFloors) > 0 {
		backend, _, err := resolver.ResolveHome(ctx, desired, standing)
		if err != nil {
			return nil, err
		}
		if err := protocol.CheckReplicaFloors(policy.ReplicaFloors, backend.Engine, backend.Decoder, backend.Router); err != nil {
			return nil, err
		}
		return backend, nil
	}
	return resolver.ResolveNative(ctx, desired, standing)
}

// backendClient uses the registration identity checked before fan-out. Direct
// callers still require a currently identified registry entry and transport.
func (r *Reconciler) backendClient(ctx context.Context, source *v1beta1.InferenceService, name string) (client.WithWatch, backendTarget, error) {
	state, _ := ctx.Value(backendContextKey{}).(*backendPreflight)
	var uid types.UID
	if state != nil {
		if state.sourceUID != source.UID || state.clusters[name] == "" {
			return nil, backendTarget{}, fmt.Errorf("member backend preflight has no authority for this source")
		}
		uid = state.clusters[name]
	} else {
		if r.APIReader == nil {
			return nil, backendTarget{}, fmt.Errorf("backend verification requires a direct registry reader")
		}
		cluster := &v1beta1.WorkloadCluster{}
		if err := r.APIReader.Get(ctx, client.ObjectKey{Name: name}, cluster); err != nil {
			return nil, backendTarget{}, err
		}
		uid = cluster.UID
	}
	cl, transport, err := r.plannedTransport(ctx, name, uid)
	return cl, backendTarget{cluster: name, uid: uid, transport: transport}, err
}

func (r *Reconciler) checkSourceSnapshot(ctx context.Context, source *v1beta1.InferenceService) error {
	if r.APIReader == nil {
		return fmt.Errorf("placement mutation requires a direct source reader")
	}
	current := &v1beta1.InferenceService{}
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(source), current); err != nil {
		return err
	}
	if !plan.SameSnapshot(source, current) || !equality.Semantic.DeepEqual(source.Spec, current.Spec) {
		return plan.ErrStaleSnapshot
	}
	return nil
}

func (r *Reconciler) checkBackendTransport(ctx context.Context, cluster string) error {
	target, ok := ctx.Value(backendTargetKey{}).(backendTarget)
	if !ok || target.cluster != cluster {
		return fmt.Errorf("member write has no verified transport")
	}
	_, current, err := r.plannedTransport(ctx, cluster, target.uid)
	if err != nil {
		return err
	}
	if current != target.transport {
		return fmt.Errorf("member connection changed during backend verification")
	}
	return nil
}

func verifiedBackendCandidates(ctx context.Context, candidates []string) []string {
	state, _ := ctx.Value(backendContextKey{}).(*backendPreflight)
	return slices.DeleteFunc(slices.Clone(candidates), func(name string) bool {
		return state == nil || state.clusters[name] == ""
	})
}
