package placement

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

const testPolicyName = "request-activity-v1"

// srcISVCWithRef is srcISVC plus an engine autoscalerPolicyRef, the input that
// activates the whole preflight/lift surface.
func srcISVCWithRef(selector string) *v1beta1.InferenceService {
	isvc := srcISVC(selector)
	isvc.Spec.Engine.AutoscalerPolicyRef = &v1beta1.AutoscalerPolicyRef{Name: testPolicyName}
	return isvc
}

// hpaPolicy is a minimal valid anchor/member policy fixture.
func hpaPolicy(name string) *v1beta1.AutoscalerPolicy {
	return &v1beta1.AutoscalerPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: name},
		Spec:       v1beta1.AutoscalerPolicySpec{Class: v1beta1.AutoscalerHPA},
	}
}

// maxConsumingPolicy derives its fallback replicas from the component's
// MaxReplicas, which trips the Split hard gate.
func maxConsumingPolicy(name string) *v1beta1.AutoscalerPolicy {
	return &v1beta1.AutoscalerPolicy{
		ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: name},
		Spec: v1beta1.AutoscalerPolicySpec{
			Class: v1beta1.AutoscalerKEDA,
			Keda: &v1beta1.KedaPolicyTemplate{
				Triggers: []v1beta1.KedaTriggerTemplate{{Type: "cpu", Metadata: map[string]string{"value": "80"}}},
				Fallback: &v1beta1.FallbackTemplate{
					FailureThreshold: 3,
					Replicas: v1beta1.ReplicaValueSource{
						FromComponent: func() *v1beta1.ComponentBoundsField { f := v1beta1.BoundsFieldMaxReplicas; return &f }(),
					},
				},
			},
		},
	}
}

// capabilityLabels returns candidate WorkloadCluster labels that satisfy both
// the match selector and the autoscaler-policy capability gate.
func capabilityLabels() map[string]string {
	return map[string]string{
		"gpu": "gb300",
		constants.WorkloadClusterAutoscalerPolicyLabel: WorkloadClusterAutoscalerPolicyCapability,
	}
}

// memberWith returns a fake member cluster client seeded with objs.
func memberWith(s *runtime.Scheme, objs ...client.Object) workloadcluster.SelectivelyCachingClient {
	c := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(objs...).Build()
	return workloadcluster.NewNeverCachingClient(c)
}

// flakyPolicyGetter fails member AutoscalerPolicy GETs with a plain
// (non-NotFound) error while *fail is set — a transient member read blip.
type flakyPolicyGetter struct {
	workloadcluster.SelectivelyCachingClient
	fail *bool
}

func (f flakyPolicyGetter) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.AutoscalerPolicy); ok && *f.fail {
		return errors.New("connection reset by peer")
	}
	return f.SelectivelyCachingClient.Get(ctx, key, obj, opts...)
}

// flakyAnchorReader fails control-plane AutoscalerPolicy reads with a plain
// error while *fail is set — a transient apiserver blip on the anchor GET.
type flakyAnchorReader struct {
	client.Reader
	fail *bool
}

func (f flakyAnchorReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.AutoscalerPolicy); ok && *f.fail {
		return errors.New("etcdserver: request timed out")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

// placedSrcISVCWithRef is srcISVCWithRef already Placed on cluster "a" with a
// published URL — the standing-winner shape the preflight must never evict.
func placedSrcISVCWithRef(selector string) *v1beta1.InferenceService {
	isvc := srcISVCWithRef(selector)
	url := &apis.URL{Scheme: "https", Host: "svc.example.com"}
	isvc.Status.Placement = &v1beta1.PlacementStatus{
		Cluster: "a", Phase: v1beta1.PlacementPhasePlaced,
		Endpoint: url,
		Candidates: []v1beta1.CandidatePlacement{{
			Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitted,
			Endpoint: url, AdmittedReplicas: 1, ReadyReplicas: 1,
		}},
	}
	isvc.Status.URL = url
	isvc.Status.SetConditions([]apis.Condition{{
		Type: apis.ConditionReady, Status: corev1.ConditionTrue, Reason: placementReadyReasonReady,
	}})
	return isvc
}

// ensurePlacedStatus re-applies the source's status on the control-plane copy
// when the fake client builder did not persist it at seed time.
func ensurePlacedStatus(t *testing.T, cp client.Client, src *v1beta1.InferenceService) {
	t.Helper()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: src.Namespace, Name: src.Name}, cur))
	if cur.Status.Placement == nil {
		cur.Status = src.Status
		require.NoError(t, cp.Status().Update(context.Background(), cur))
	}
}

// policyGetBlocker blocks AutoscalerPolicy GETs until the context expires,
// simulating a hung member apiserver for the timeout classification.
type policyGetBlocker struct {
	workloadcluster.SelectivelyCachingClient
}

func (b policyGetBlocker) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.AutoscalerPolicy); ok {
		<-ctx.Done()
		return ctx.Err()
	}
	return b.SelectivelyCachingClient.Get(ctx, key, obj, opts...)
}

func stagedPreflight(t *testing.T, r *Reconciler, uid types.UID) apis.Condition {
	t.Helper()
	cond, ok := r.policyState().preflightFor(uid)
	require.True(t, ok, "preflight condition must be staged")
	return cond.cond
}

func condOfType(conds []policyCondition, condType string) *apis.Condition {
	for i := range conds {
		if conds[i].condType == apis.ConditionType(condType) {
			return &conds[i].cond
		}
	}
	return nil
}

func TestPreflightPolicies_NoRefsIsNil(t *testing.T) {
	s := testScheme(t)
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}})
	assert.Nil(t, r.preflightPolicies(context.Background(), srcISVC("gpu=gb300"), []string{"a"}, nil))
	_, staged := r.policyState().preflightFor("uid-1")
	assert.False(t, staged, "no condition staged for a no-ref source")
}

// The zero-cost guard: a full reconcile of a no-ref source must not write any
// policy condition or candidate autoscaling state.
func TestReconcile_NoRefs_NoPolicyStatus(t *testing.T) {
	s := testScheme(t)
	w := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, cp := newPlacer(s, clusters, srcISVC("gpu=gb300"), readyWC("a", capabilityLabels()))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	assert.Nil(t, o.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition)))
	assert.Nil(t, o.Status.GetCondition(apis.ConditionType(v1beta1.AutoscalerPolicyAggregateCondition)))
	for _, c := range o.Status.Placement.Candidates {
		assert.Nil(t, c.Autoscaling)
	}
}

func TestPreflightPolicies_AnchorMissingHolds(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRef("gpu=gb300")
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc)
	r.ControlPlaneID = "cp-east"

	out := r.preflightPolicies(context.Background(), isvc, []string{"a"},
		[]v1beta1.WorkloadCluster{*readyWC("a", capabilityLabels())})
	require.NotNil(t, out)
	assert.True(t, out.hold)

	cond := stagedPreflight(t, r, isvc.UID)
	assert.Equal(t, corev1.ConditionFalse, cond.Status)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonPolicyMissing, cond.Reason)
	assert.Contains(t, cond.Message, "cp-east", "message must name the control plane")
}

func TestPreflightPolicies_CapabilityGate(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRef("gpu=gb300")
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": memberWith(s, hpaPolicy(testPolicyName)),
		"b": memberWith(s, hpaPolicy(testPolicyName)),
	}}
	r, _ := newPlacer(s, clusters, isvc, hpaPolicy(testPolicyName))

	// b carries the matching selector labels but not the capability label.
	wcs := []v1beta1.WorkloadCluster{
		*readyWC("a", capabilityLabels()),
		*readyWC("b", map[string]string{"gpu": "gb300"}),
	}
	out := r.preflightPolicies(context.Background(), isvc, []string{"a", "b"}, wcs)
	require.NotNil(t, out)
	assert.False(t, out.hold)
	assert.Equal(t, []string{"a"}, out.eligible)

	cond := stagedPreflight(t, r, isvc.UID)
	assert.Equal(t, corev1.ConditionTrue, cond.Status)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonPassed, cond.Reason)
	assert.Contains(t, cond.Message, "b ("+v1beta1.PlacementPolicyPreflightReasonCapabilityMissing)
}

func TestPreflightPolicies_MemberSkewTable(t *testing.T) {
	s := testScheme(t)

	mismatched := hpaPolicy(testPolicyName)
	mismatched.Spec.Class = v1beta1.AutoscalerKEDA
	mismatched.Spec.Keda = &v1beta1.KedaPolicyTemplate{
		Triggers: []v1beta1.KedaTriggerTemplate{{Type: "cpu", Metadata: map[string]string{"value": "80"}}},
	}

	cases := []struct {
		name       string
		member     workloadcluster.SelectivelyCachingClient
		wantReason string
	}{
		{"missing on member", memberWith(s), v1beta1.PlacementPolicyPreflightReasonPolicyMissing},
		{"digest mismatch", memberWith(s, mismatched), v1beta1.PlacementPolicyPreflightReasonDigestMismatch},
		{"member get timeout", policyGetBlocker{memberWith(s)}, v1beta1.PlacementPolicyPreflightReasonMemberGetTimeout},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := srcISVCWithRef("gpu=gb300")
			clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"a": tc.member}}
			r, _ := newPlacer(s, clusters, isvc, hpaPolicy(testPolicyName))
			r.policy = newPolicyPreflight(controllerconfig.AutoscalerPolicyPreflightConfig{})
			r.policy.memberGetTimeout = 20 * time.Millisecond

			out := r.preflightPolicies(context.Background(), isvc, []string{"a"},
				[]v1beta1.WorkloadCluster{*readyWC("a", capabilityLabels())})
			require.NotNil(t, out)
			assert.True(t, out.hold, "sole candidate ineligible must hold placement")

			cond := stagedPreflight(t, r, isvc.UID)
			assert.Equal(t, corev1.ConditionFalse, cond.Status)
			assert.Equal(t, tc.wantReason, cond.Reason)
			assert.Contains(t, cond.Message, "a (", "message must name the skipped member")
		})
	}
}

// A skewed member only narrows the candidate set; the condition stays True and
// names the skipped member.
func TestPreflightPolicies_SkewedMemberStillPasses(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRef("gpu=gb300")
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": memberWith(s, hpaPolicy(testPolicyName)),
		"b": memberWith(s), // policy not yet synced there
	}}
	r, _ := newPlacer(s, clusters, isvc, hpaPolicy(testPolicyName))

	wcs := []v1beta1.WorkloadCluster{*readyWC("a", capabilityLabels()), *readyWC("b", capabilityLabels())}
	out := r.preflightPolicies(context.Background(), isvc, []string{"a", "b"}, wcs)
	require.NotNil(t, out)
	assert.False(t, out.hold)
	assert.Equal(t, []string{"a"}, out.eligible)

	cond := stagedPreflight(t, r, isvc.UID)
	assert.Equal(t, corev1.ConditionTrue, cond.Status)
	assert.Contains(t, cond.Message, "b ("+v1beta1.PlacementPolicyPreflightReasonPolicyMissing)
}

// A candidate with no live client is left eligible: fan-out cannot reach it
// anyway this pass, and filtering it would tear down a sticky winner on a
// transport flap.
func TestPreflightPolicies_DisconnectedCandidateStaysEligible(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRef("gpu=gb300")
	r, _ := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{}}, isvc, hpaPolicy(testPolicyName))

	out := r.preflightPolicies(context.Background(), isvc, []string{"a"},
		[]v1beta1.WorkloadCluster{*readyWC("a", capabilityLabels())})
	require.NotNil(t, out)
	assert.False(t, out.hold)
	assert.Equal(t, []string{"a"}, out.eligible)
}

func TestPreflightPolicies_SplitHardGate(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity} {
		t.Run(string(mode), func(t *testing.T) {
			for _, tc := range []struct {
				name     string
				cap      int32
				wantHold bool
			}{
				{name: "unset ceiling holds", wantHold: true},
				{name: "explicit ceiling permits placement", cap: 45},
			} {
				t.Run(tc.name, func(t *testing.T) {
					s := testScheme(t)
					isvc := srcISVCWithRef("")
					isvc.Spec.Placement = &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: mode, Split: &v1beta1.SplitSpec{MaxReplicasPerCluster: tc.cap}}
					clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{"a": memberWith(s, maxConsumingPolicy(testPolicyName))}}
					r, _ := newPlacer(s, clusters, isvc, maxConsumingPolicy(testPolicyName))
					out := r.preflightPolicies(t.Context(), isvc, []string{"a"}, []v1beta1.WorkloadCluster{*readyWC("a", capabilityLabels())})
					if out == nil {
						t.Fatal("missing preflight result")
					}
					if diff := cmp.Diff(tc.wantHold, out.hold); diff != "" {
						t.Error(diff)
					}
					cond := stagedPreflight(t, r, isvc.UID)
					if tc.wantHold {
						if diff := cmp.Diff(v1beta1.PlacementPolicyPreflightReasonUnboundedSplitCeiling, cond.Reason); diff != "" {
							t.Error(diff)
						}
					} else if diff := cmp.Diff([]string{"a"}, out.eligible); diff != "" {
						t.Error(diff)
					}
				})
			}
		})
	}
}

func TestLiftCandidateAutoscaling(t *testing.T) {
	refs := []componentPolicyRef{{component: v1beta1.EngineComponent, policy: testPolicyName}}

	derivedWith := func(as *v1beta1.ComponentAutoscalerStatus) *v1beta1.InferenceService {
		return &v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Generation: 7},
			Spec: v1beta1.InferenceServiceSpec{
				Engine: &v1beta1.EngineSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: testPolicyName},
				}},
			},
			Status: v1beta1.InferenceServiceStatus{
				Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
					v1beta1.EngineComponent: {Autoscaler: as},
				},
			},
		}
	}

	t.Run("policy rendered and ready", func(t *testing.T) {
		got, reported, failedClosed := liftCandidateAutoscaling(refs, derivedWith(&v1beta1.ComponentAutoscalerStatus{
			SpecSource: "policy",
			Policy: &v1beta1.AutoscalerPolicyProvenance{
				Name: testPolicyName, PortableDigest: "pv1:abc", ResolvedDigest: "rv1:def",
			},
		}))
		assert.True(t, got.Ready)
		assert.True(t, reported)
		assert.False(t, failedClosed)
		assert.Equal(t, "rv1:def", got.Components[v1beta1.EngineComponent].ResolvedDigest)
		assert.True(t, got.Components[v1beta1.EngineComponent].Ready)
		require.Len(t, got.Policies, 1)
		assert.Equal(t, testPolicyName, got.Policies[0].Name)
		assert.Equal(t, "pv1:abc", got.Policies[0].PortableDigest)
	})

	t.Run("no autoscaler status reported", func(t *testing.T) {
		got, reported, failedClosed := liftCandidateAutoscaling(refs, derivedWith(nil))
		assert.False(t, got.Ready)
		assert.False(t, reported)
		assert.False(t, failedClosed)
		assert.Empty(t, got.Policies[0].PortableDigest)
	})

	t.Run("inline precedence counts as reported, not ready", func(t *testing.T) {
		got, reported, _ := liftCandidateAutoscaling(refs, derivedWith(&v1beta1.ComponentAutoscalerStatus{
			SpecSource: "isvc",
			ShadowedPolicyRef: &v1beta1.ShadowedAutoscalerPolicy{
				Name: testPolicyName, PortableDigest: "pv1:abc", WouldRenderDigest: "rv1:shadow",
			},
		}))
		assert.False(t, got.Ready)
		assert.True(t, reported)
		assert.Equal(t, "pv1:abc", got.Policies[0].PortableDigest)
	})

	t.Run("fail-closed member surfaces", func(t *testing.T) {
		_, _, failedClosed := liftCandidateAutoscaling(refs, derivedWith(&v1beta1.ComponentAutoscalerStatus{
			SpecSource: "policy",
			Conditions: []metav1.Condition{{
				Type: v1beta1.AutoscalerResolvedCondition, Status: metav1.ConditionFalse,
				ObservedGeneration: 7,
				Reason:             v1beta1.AutoscalerResolvedReasonPolicyNotFound,
			}},
		}))
		assert.True(t, failedClosed)
	})

	t.Run("derived spec with an old policy ref is not attributed to the current ref", func(t *testing.T) {
		derived := derivedWith(&v1beta1.ComponentAutoscalerStatus{
			SpecSource: "policy",
			Policy: &v1beta1.AutoscalerPolicyProvenance{
				Name: "old-policy", PortableDigest: "pv1:old", ResolvedDigest: "rv1:old",
			},
		})
		derived.Spec.Engine.AutoscalerPolicyRef.Name = "old-policy"
		got, reported, failedClosed := liftCandidateAutoscaling(refs, derived)
		assert.False(t, got.Ready)
		assert.False(t, reported)
		assert.False(t, failedClosed)
		assert.Empty(t, got.Components[v1beta1.EngineComponent].ResolvedDigest)
		require.Len(t, got.Policies, 1)
		assert.Equal(t, testPolicyName, got.Policies[0].Name)
		assert.Empty(t, got.Policies[0].PortableDigest)
	})

	t.Run("stale rendered status is not attributed to the current ref", func(t *testing.T) {
		got, reported, failedClosed := liftCandidateAutoscaling(refs, derivedWith(&v1beta1.ComponentAutoscalerStatus{
			SpecSource: "policy",
			Policy: &v1beta1.AutoscalerPolicyProvenance{
				Name: "old-policy", PortableDigest: "pv1:old", ResolvedDigest: "rv1:old",
			},
			Conditions: []metav1.Condition{{
				Type: v1beta1.AutoscalerResolvedCondition, Status: metav1.ConditionTrue,
				ObservedGeneration: 6,
			}},
		}))
		assert.False(t, got.Ready)
		assert.False(t, reported)
		assert.False(t, failedClosed)
		assert.Empty(t, got.Components[v1beta1.EngineComponent].ResolvedDigest)
		assert.Empty(t, got.Policies[0].PortableDigest)
	})

	t.Run("current failure is surfaced while old live provenance is retained", func(t *testing.T) {
		got, reported, failedClosed := liftCandidateAutoscaling(refs, derivedWith(&v1beta1.ComponentAutoscalerStatus{
			SpecSource: "policy",
			Policy: &v1beta1.AutoscalerPolicyProvenance{
				Name: "old-policy", PortableDigest: "pv1:old", ResolvedDigest: "rv1:old",
			},
			Conditions: []metav1.Condition{{
				Type: v1beta1.AutoscalerResolvedCondition, Status: metav1.ConditionFalse,
				ObservedGeneration: 7, Reason: v1beta1.AutoscalerResolvedReasonPolicyNotFound,
			}},
		}))
		assert.False(t, got.Ready)
		assert.False(t, reported)
		assert.True(t, failedClosed)
		assert.Empty(t, got.Components[v1beta1.EngineComponent].ResolvedDigest)
		assert.Empty(t, got.Policies[0].PortableDigest)
	})

	t.Run("stale shadow status is not attributed to the current ref", func(t *testing.T) {
		got, reported, failedClosed := liftCandidateAutoscaling(refs, derivedWith(&v1beta1.ComponentAutoscalerStatus{
			SpecSource: "isvc",
			ShadowedPolicyRef: &v1beta1.ShadowedAutoscalerPolicy{
				Name: "old-policy", PortableDigest: "pv1:old", WouldRenderDigest: "rv1:old",
			},
		}))
		assert.False(t, got.Ready)
		assert.False(t, reported)
		assert.False(t, failedClosed)
		assert.Empty(t, got.Policies[0].PortableDigest)
	})
}

func TestPolicyStatusForWrite_Detectors(t *testing.T) {
	isvc := srcISVCWithRef("gpu=gb300")
	base := time.Now()

	newState := func() *policyPreflight {
		pp := newPolicyPreflight(controllerconfig.AutoscalerPolicyPreflightConfig{SkewDeadlineSeconds: 60})
		pp.now = func() time.Time { return base }
		return pp
	}
	readyObs := policyHomeObservation{
		autoscaling: &v1beta1.CandidateAutoscalingStatus{Ready: true},
		placedSince: base.Add(-10 * time.Minute),
		allReported: true,
	}

	t.Run("all homes resolved", func(t *testing.T) {
		r := &Reconciler{policy: newState()}
		r.policy.recordHome(isvc.UID, "a", readyObs)
		res := &placementResult{candidates: []v1beta1.CandidatePlacement{{Cluster: "a"}}}
		agg := condOfType(r.policyStatusForWrite(isvc, res), v1beta1.AutoscalerPolicyAggregateCondition)
		require.NotNil(t, agg)
		assert.Equal(t, corev1.ConditionTrue, agg.Status)
		assert.Equal(t, v1beta1.AutoscalerPolicyAggregateReasonAllHomesResolved, agg.Reason)
		require.NotNil(t, res.candidates[0].Autoscaling, "lifted state attached to the candidate")
		assert.True(t, res.candidates[0].Autoscaling.Ready)
	})

	t.Run("resolve timeout past the skew deadline", func(t *testing.T) {
		r := &Reconciler{policy: newState()}
		// The skew clock anchors at the first observation, so drive the
		// injectable clock: observe 2 minutes before the evaluation pass.
		r.policy.now = func() time.Time { return base.Add(-2 * time.Minute) }
		r.policy.recordHome(isvc.UID, "a", policyHomeObservation{
			autoscaling: &v1beta1.CandidateAutoscalingStatus{},
		})
		r.policy.now = func() time.Time { return base }
		res := &placementResult{candidates: []v1beta1.CandidatePlacement{{Cluster: "a"}}}
		agg := condOfType(r.policyStatusForWrite(isvc, res), v1beta1.AutoscalerPolicyAggregateCondition)
		require.NotNil(t, agg)
		assert.Equal(t, corev1.ConditionFalse, agg.Status)
		assert.Equal(t, v1beta1.AutoscalerPolicyAggregateReasonResolveTimeout, agg.Reason)
		assert.Contains(t, agg.Message, "a")
	})

	t.Run("within the deadline stays true", func(t *testing.T) {
		r := &Reconciler{policy: newState()}
		r.policy.now = func() time.Time { return base.Add(-10 * time.Second) }
		r.policy.recordHome(isvc.UID, "a", policyHomeObservation{
			autoscaling: &v1beta1.CandidateAutoscalingStatus{},
		})
		r.policy.now = func() time.Time { return base }
		res := &placementResult{candidates: []v1beta1.CandidatePlacement{{Cluster: "a"}}}
		agg := condOfType(r.policyStatusForWrite(isvc, res), v1beta1.AutoscalerPolicyAggregateCondition)
		require.NotNil(t, agg)
		assert.Equal(t, corev1.ConditionTrue, agg.Status)
	})

	t.Run("member failed closed", func(t *testing.T) {
		r := &Reconciler{policy: newState()}
		obs := readyObs
		obs.failedClosed = true
		r.policy.recordHome(isvc.UID, "a", obs)
		res := &placementResult{candidates: []v1beta1.CandidatePlacement{{Cluster: "a"}}}
		agg := condOfType(r.policyStatusForWrite(isvc, res), v1beta1.AutoscalerPolicyAggregateCondition)
		require.NotNil(t, agg)
		assert.Equal(t, corev1.ConditionFalse, agg.Status)
		assert.Equal(t, v1beta1.AutoscalerPolicyAggregateReasonMemberFailedClose, agg.Reason)
	})

	t.Run("field pruned outranks the rest", func(t *testing.T) {
		r := &Reconciler{policy: newState()}
		r.policy.recordHome(isvc.UID, "a", readyObs)
		for i := 0; i < policyPruneRevertThreshold; i++ {
			r.policy.recordPrune(isvc.UID, "a", true)
		}
		res := &placementResult{candidates: []v1beta1.CandidatePlacement{{Cluster: "a"}}}
		agg := condOfType(r.policyStatusForWrite(isvc, res), v1beta1.AutoscalerPolicyAggregateCondition)
		require.NotNil(t, agg)
		assert.Equal(t, corev1.ConditionFalse, agg.Status)
		assert.Equal(t, v1beta1.AutoscalerPolicyAggregateReasonFieldPruned, agg.Reason)
		assert.Contains(t, agg.Message, "a")
	})

	t.Run("field pruned unlatches when the member leaves the fleet", func(t *testing.T) {
		r := &Reconciler{policy: newState()}
		r.policy.recordHome(isvc.UID, "a", readyObs)
		for i := 0; i < policyPruneRevertThreshold; i++ {
			r.policy.recordPrune(isvc.UID, "a", true)
		}
		// "a" is no longer in the pass's candidate/home set: the counter can
		// never reset via a re-apply there, so it must not hold the aggregate.
		res := &placementResult{candidates: []v1beta1.CandidatePlacement{{Cluster: "b"}}}
		agg := condOfType(r.policyStatusForWrite(isvc, res), v1beta1.AutoscalerPolicyAggregateCondition)
		require.NotNil(t, agg)
		assert.Equal(t, corev1.ConditionTrue, agg.Status)
		assert.Equal(t, v1beta1.AutoscalerPolicyAggregateReasonAllHomesResolved, agg.Reason)
	})

	t.Run("no refs returns nil and leaves candidates alone", func(t *testing.T) {
		r := &Reconciler{policy: newState()}
		res := &placementResult{candidates: []v1beta1.CandidatePlacement{{Cluster: "a"}}}
		assert.Nil(t, r.policyStatusForWrite(srcISVC("gpu=gb300"), res))
		assert.Nil(t, res.candidates[0].Autoscaling)
	})
}

func TestObservePolicyRefStamp_ConsecutiveCount(t *testing.T) {
	r := &Reconciler{}
	src := srcISVCWithRef("gpu=gb300")
	desired := DeriveISVC(src, "", "")

	pruned := srcISVC("") // same shape, ref pruned by the member apiserver
	pruned.ResourceVersion = "7"
	intact := srcISVCWithRef("")
	intact.ResourceVersion = "7"

	// Below the threshold: not flagged yet.
	for i := 0; i < policyPruneRevertThreshold-1; i++ {
		r.observePolicyRefStamp(src, "a", pruned, desired)
	}
	assert.Empty(t, r.policyState().prunedHomes(src.UID))

	// The threshold-th consecutive revert flags the home.
	r.observePolicyRefStamp(src, "a", pruned, desired)
	assert.Equal(t, []string{"a"}, r.policyState().prunedHomes(src.UID))

	// An intact live object resets the consecutive counter.
	r.observePolicyRefStamp(src, "a", intact, desired)
	assert.Empty(t, r.policyState().prunedHomes(src.UID))

	// A first-time create (no live object) is no evidence either way.
	fresh := srcISVC("")
	r.observePolicyRefStamp(src, "b", fresh, desired)
	assert.Empty(t, r.policyState().prunedHomes(src.UID))
}

// End-to-end through Reconcile: preflight passes, the winner's lifted policy
// state lands on candidates[].autoscaling, and both conditions are written on
// the source status.
func TestReconcile_PolicyLiftEndToEnd(t *testing.T) {
	s := testScheme(t)

	derived := srcISVCWithRef("")
	derived.UID = "member-uid"
	derived.CreationTimestamp = metav1.Now()
	derived.Labels = map[string]string{PlacementOriginLabel: "uid-1"}
	derived.Status = v1beta1.InferenceServiceStatus{
		Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {Autoscaler: &v1beta1.ComponentAutoscalerStatus{
				SpecSource: "policy",
				Policy: &v1beta1.AutoscalerPolicyProvenance{
					Name: testPolicyName, PortableDigest: "pv1:abc", ResolvedDigest: "rv1:def",
				},
			}},
		},
	}
	w := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).
		WithObjects(observedWorkerObjects(derived, irWithInstances(v1beta1.EngineComponent, true))...).
		WithObjects(hpaPolicy(testPolicyName)).Build()
	cur := &v1beta1.InferenceService{}
	require.NoError(t, w.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, cur))
	if len(cur.Status.Components) == 0 {
		cur.Status = derived.Status
		require.NoError(t, w.Status().Update(context.Background(), cur))
	}

	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, cp := newPlacer(s, clusters, srcISVCWithRef("gpu=gb300"), hpaPolicy(testPolicyName), readyWC("a", capabilityLabels()))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	require.NotNil(t, o.Status.Placement)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, o.Status.Placement.Phase)
	require.Len(t, o.Status.Placement.Candidates, 1)

	auto := o.Status.Placement.Candidates[0].Autoscaling
	require.NotNil(t, auto, "lifted per-home policy state must be published")
	assert.True(t, auto.Ready)
	assert.Equal(t, "rv1:def", auto.Components[v1beta1.EngineComponent].ResolvedDigest)
	require.Len(t, auto.Policies, 1)
	assert.Equal(t, "pv1:abc", auto.Policies[0].PortableDigest)

	pre := o.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition))
	require.NotNil(t, pre)
	assert.Equal(t, corev1.ConditionTrue, pre.Status)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonPassed, pre.Reason)

	agg := o.Status.GetCondition(apis.ConditionType(v1beta1.AutoscalerPolicyAggregateCondition))
	require.NotNil(t, agg)
	assert.Equal(t, corev1.ConditionTrue, agg.Status)
	assert.Equal(t, v1beta1.AutoscalerPolicyAggregateReasonAllHomesResolved, agg.Reason)
}

// End-to-end hold: a sole candidate whose member copy diverges keeps the
// placement Pending, fans nothing out, and surfaces the mismatch condition.
func TestReconcile_PolicyPreflightHoldsPlacement(t *testing.T) {
	s := testScheme(t)
	mismatched := hpaPolicy(testPolicyName)
	mismatched.Spec.Class = v1beta1.AutoscalerKEDA
	mismatched.Spec.Keda = &v1beta1.KedaPolicyTemplate{
		Triggers: []v1beta1.KedaTriggerTemplate{{Type: "cpu", Metadata: map[string]string{"value": "80"}}},
	}
	w := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(mismatched).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, cp := newPlacer(s, clusters, srcISVCWithRef("gpu=gb300"), hpaPolicy(testPolicyName), readyWC("a", capabilityLabels()))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.False(t, hasDerived(t, w), "no fan-out while the preflight holds")
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	assert.Equal(t, v1beta1.PlacementPhasePending, o.Status.Placement.Phase)

	pre := o.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition))
	require.NotNil(t, pre)
	assert.Equal(t, corev1.ConditionFalse, pre.Status)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonDigestMismatch, pre.Reason)
	assert.Contains(t, pre.Message, "a (")
}

// mismatchedPolicy is a member copy whose portable digest diverges from the
// hpaPolicy anchor (the in-place-edit sync window).
func mismatchedPolicy(name string) *v1beta1.AutoscalerPolicy {
	p := hpaPolicy(name)
	p.Spec.Class = v1beta1.AutoscalerKEDA
	p.Spec.Keda = &v1beta1.KedaPolicyTemplate{
		Triggers: []v1beta1.KedaTriggerTemplate{{Type: "cpu", Metadata: map[string]string{"value": "80"}}},
	}
	return p
}

// The standing winner must survive the digest window an in-place policy edit
// opens (anchor updated, member not yet synced): actuation stays held while its
// current placement status is observed at the poll cadence.
func TestReconcile_WinnerSurvivesDigestMismatchWindow(t *testing.T) {
	s := testScheme(t)
	w := workerWithReplicaCounts(t, s, "fresh.example.com", 2, 1)
	require.NoError(t, w.Create(context.Background(), mismatchedPolicy(testPolicyName)))
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	src := placedSrcISVCWithRef("gpu=gb300")
	r, cp := newPlacer(s, clusters, src, hpaPolicy(testPolicyName), readyWC("a", capabilityLabels()))
	ensurePlacedStatus(t, cp, src)

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, time.Second, res.RequeueAfter, "held at the poll cadence")

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase, "digest window must not evict the standing winner")
	assert.Equal(t, "a", p.Cluster)
	require.Len(t, p.Candidates, 1)
	assert.Equal(t, int32(2), p.Candidates[0].AdmittedReplicas)
	assert.Equal(t, int32(1), p.Candidates[0].ReadyReplicas)
	assert.Equal(t, "fresh.example.com", p.Candidates[0].Endpoint.Host)
	assert.Equal(t, "fresh.example.com", cpStatusURL(t, cp).Host)
}

// A transient member GET error on the standing winner's cluster (no prior
// terminal verdict in memory, e.g. right after a control-plane restart) holds
// actuation instead of re-racing while status observation continues.
func TestReconcile_WinnerSurvivesTransientMemberGetError(t *testing.T) {
	s := testScheme(t)
	fail := true
	w := workerWithReplicaCounts(t, s, "fresh.example.com", 2, 1)
	require.NoError(t, w.Create(context.Background(), hpaPolicy(testPolicyName)))
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": flakyPolicyGetter{workloadcluster.NewNeverCachingClient(w), &fail},
	}}
	src := placedSrcISVCWithRef("gpu=gb300")
	r, cp := newPlacer(s, clusters, src, hpaPolicy(testPolicyName), readyWC("a", capabilityLabels()))
	ensurePlacedStatus(t, cp, src)

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, time.Second, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase, "transient read error must not evict the standing winner")
	assert.Equal(t, "a", p.Cluster)
	assert.Equal(t, "fresh.example.com", cpStatusURL(t, cp).Host)
}

// A candidate the preflight terminally verified stays eligible across a
// transient member read error; the pass is flagged transient for a fast
// re-verify and the condition stays True.
func TestPreflightPolicies_TransientErrorKeepsLastEligibility(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRef("gpu=gb300")
	fail := false
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": flakyPolicyGetter{memberWith(s, hpaPolicy(testPolicyName)), &fail},
	}}
	r, _ := newPlacer(s, clusters, isvc, hpaPolicy(testPolicyName))
	wcs := []v1beta1.WorkloadCluster{*readyWC("a", capabilityLabels())}

	// Healthy pass: the member is terminally verified eligible.
	out := r.preflightPolicies(context.Background(), isvc, []string{"a"}, wcs)
	require.NotNil(t, out)
	assert.Equal(t, []string{"a"}, out.eligible)
	assert.False(t, out.transient)

	// Transient blip: the verified candidate is NOT evicted.
	fail = true
	out = r.preflightPolicies(context.Background(), isvc, []string{"a"}, wcs)
	require.NotNil(t, out)
	assert.False(t, out.hold)
	assert.Equal(t, []string{"a"}, out.eligible)
	assert.True(t, out.transient)
	cond := stagedPreflight(t, r, isvc.UID)
	assert.Equal(t, corev1.ConditionTrue, cond.Status)

	// Terminal skew after the blip still evicts: the memory only bridges
	// transient errors, it never overrides a real verdict.
	fail = false
	require.NoError(t, clusters.m["a"].Delete(context.Background(), hpaPolicy(testPolicyName)))
	out = r.preflightPolicies(context.Background(), isvc, []string{"a"}, wcs)
	require.NotNil(t, out)
	assert.True(t, out.hold)
	cond = stagedPreflight(t, r, isvc.UID)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonPolicyMissing, cond.Reason)
}

// A candidate with NO prior terminal verdict stays skipped on a transient
// error — there is no last-known eligibility to fall back to.
func TestPreflightPolicies_TransientErrorUnverifiedStaysSkipped(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRef("gpu=gb300")
	fail := true
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": flakyPolicyGetter{memberWith(s, hpaPolicy(testPolicyName)), &fail},
	}}
	r, _ := newPlacer(s, clusters, isvc, hpaPolicy(testPolicyName))

	out := r.preflightPolicies(context.Background(), isvc, []string{"a"},
		[]v1beta1.WorkloadCluster{*readyWC("a", capabilityLabels())})
	require.NotNil(t, out)
	assert.True(t, out.hold)
	assert.True(t, out.transient)
	cond := stagedPreflight(t, r, isvc.UID)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonMemberGetTimeout, cond.Reason)
}

// A transient control-plane blip on the anchor GET holds actuation while the
// standing home still refreshes from member state.
func TestReconcile_AnchorBlipDoesNotWipePlacedStatus(t *testing.T) {
	s := testScheme(t)
	w := workerWithReplicaCounts(t, s, "fresh.example.com", 2, 1)
	require.NoError(t, w.Create(context.Background(), hpaPolicy(testPolicyName)))
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	src := placedSrcISVCWithRef("gpu=gb300")
	r, cp := newPlacer(s, clusters, src, hpaPolicy(testPolicyName), readyWC("a", capabilityLabels()))
	ensurePlacedStatus(t, cp, src)
	fail := true
	r.APIReader = flakyAnchorReader{Reader: r.APIReader, fail: &fail}

	res, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	assert.Equal(t, time.Second, res.RequeueAfter)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase, "anchor blip must not write Pending over a Placed source")
	assert.Equal(t, "a", p.Cluster)
	assert.Equal(t, "fresh.example.com", cpStatusURL(t, cp).Host)
	_, staged := r.policyState().preflightFor(src.UID)
	assert.False(t, staged, "no condition staged on a transient anchor blip")
}

func TestReconcile_PolicyHoldKeepsPresentDeadmittedHomeObservableAfterGrace(t *testing.T) {
	s := testScheme(t)
	worker := workerWithGatedDerived(t, s)
	src := placedSrcISVCWithRef("gpu=gb300")
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}, src, readyWC("a", capabilityLabels()))
	r.winnerLostSince.Store(src.UID, time.Now().Add(-2*r.winnerLostGrace()))
	ensurePlacedStatus(t, cp, src)

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Empty(t, p.Cluster, "expired grace releases winner ownership")
	assert.Equal(t, v1beta1.PlacementPhaseAdmitting, p.Phase)
	require.Len(t, p.Candidates, 1, "the present home must remain observable")
	assert.Equal(t, "a", p.Candidates[0].Cluster)
	assert.Equal(t, v1beta1.CandidatePhaseAdmitting, p.Candidates[0].Phase)
	assert.Nil(t, p.Candidates[0].Endpoint)
	assert.Zero(t, p.Candidates[0].ReadyReplicas)

	derived := &v1beta1.InferenceService{}
	require.NoError(t, worker.Get(context.Background(), req().NamespacedName, derived))
	assert.Nil(t, derived.Spec.Engine.AutoscalerPolicyRef,
		"policy hold must not apply the blocked source spec")
}

func TestReconcile_PolicyHoldElectsObservedAdmittedCandidate(t *testing.T) {
	s := testScheme(t)
	worker := workerWithReplicaCounts(t, s, "fresh.example.com", 2, 1)
	src := srcISVCWithRef("gpu=gb300")
	src.Status.Placement = &v1beta1.PlacementStatus{
		Phase:      v1beta1.PlacementPhaseAdmitting,
		Candidates: []v1beta1.CandidatePlacement{{Cluster: "a", Phase: v1beta1.CandidatePhaseAdmitting}},
	}
	r, cp := newPlacer(s, fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(worker),
	}}, src, readyWC("a", capabilityLabels()))
	r.winnerLostSince.Store(src.UID, time.Now().Add(-2*r.winnerLostGrace()))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	p := cpPlacement(t, cp)
	require.NotNil(t, p)
	assert.Equal(t, "a", p.Cluster)
	assert.Equal(t, v1beta1.PlacementPhasePlaced, p.Phase)
	assert.Equal(t, "fresh.example.com", p.Endpoint.Host)
	_, armed := r.winnerLostSince.Load(src.UID)
	assert.False(t, armed, "a newly observed winner starts with a fresh grace clock")
}

// A reserved ref kind passes no preflight: every member webhook would deny
// the derived apply, so fan-out is refused with a named reason.
func TestReconcile_PreflightRejectsReservedRefKind(t *testing.T) {
	s := testScheme(t)
	isvc := srcISVCWithRef("gpu=gb300")
	isvc.Spec.Engine.AutoscalerPolicyRef.Kind = "ClusterAutoscalerPolicy"
	w := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, cp := newPlacer(s, clusters, isvc, hpaPolicy(testPolicyName), readyWC("a", capabilityLabels()))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.False(t, hasDerived(t, w), "no fan-out for a ref every member webhook would deny")
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	assert.Equal(t, v1beta1.PlacementPhasePending, o.Status.Placement.Phase)
	pre := o.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition))
	require.NotNil(t, pre)
	assert.Equal(t, corev1.ConditionFalse, pre.Status)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonInvalidRef, pre.Reason)
}

// An anchor policy that fails spec validation (reserved Required enforcement)
// holds placement with a named reason instead of fanning out into a fleet of
// member-webhook denials.
func TestReconcile_PreflightRejectsInvalidAnchorPolicy(t *testing.T) {
	s := testScheme(t)
	required := hpaPolicy(testPolicyName)
	required.Spec.Enforcement = v1beta1.PolicyEnforcementRequired
	w := emptyWorker(s)
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, cp := newPlacer(s, clusters, srcISVCWithRef("gpu=gb300"), required, readyWC("a", capabilityLabels()))

	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	assert.False(t, hasDerived(t, w), "no fan-out on an invalid anchor policy")
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	assert.Equal(t, v1beta1.PlacementPhasePending, o.Status.Placement.Phase)
	pre := o.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition))
	require.NotNil(t, pre)
	assert.Equal(t, corev1.ConditionFalse, pre.Status)
	assert.Equal(t, v1beta1.PlacementPolicyPreflightReasonInvalidPolicy, pre.Reason)
}

// Removing the last policy ref clears both policy conditions on the next
// status write and drops the per-source in-memory state — a rolled-back
// source must not carry a frozen final verdict forever.
func TestReconcile_RefRollbackClearsPolicyConditions(t *testing.T) {
	s := testScheme(t)
	w := fakeclient.NewClientBuilder().WithScheme(s).
		WithStatusSubresource(&v1beta1.InferenceService{}).WithObjects(mismatchedPolicy(testPolicyName)).Build()
	clusters := fakeClusters{m: map[string]workloadcluster.SelectivelyCachingClient{
		"a": workloadcluster.NewNeverCachingClient(w),
	}}
	r, cp := newPlacer(s, clusters, srcISVCWithRef("gpu=gb300"), hpaPolicy(testPolicyName), readyWC("a", capabilityLabels()))

	// First pass: digest mismatch writes both policy conditions.
	_, err := r.Reconcile(context.Background(), req())
	require.NoError(t, err)
	o := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, o))
	require.NotNil(t, o.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition)))
	require.NotNil(t, o.Status.GetCondition(apis.ConditionType(v1beta1.AutoscalerPolicyAggregateCondition)))

	// Rollback: drop the ref and reconcile again.
	o.Spec.Engine.AutoscalerPolicyRef = nil
	require.NoError(t, cp.Update(context.Background(), o))
	_, err = r.Reconcile(context.Background(), req())
	require.NoError(t, err)

	after := &v1beta1.InferenceService{}
	require.NoError(t, cp.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "svc"}, after))
	assert.Nil(t, after.Status.GetCondition(apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition)),
		"preflight condition cleared on rollback")
	assert.Nil(t, after.Status.GetCondition(apis.ConditionType(v1beta1.AutoscalerPolicyAggregateCondition)),
		"aggregate condition cleared on rollback")
	_, staged := r.policyState().preflightFor("uid-1")
	assert.False(t, staged, "per-source in-memory state forgotten")
}

// The skew clock anchors at the first observation of a ref-carrying derived,
// so adding a ref to a long-placed ISVC (the standard migration flow) does
// not start already past the deadline; a recreated derived (creation later
// than the anchor) restarts the clock.
func TestRecordHome_SkewClockAnchorsAtFirstObservation(t *testing.T) {
	isvc := srcISVCWithRef("gpu=gb300")
	base := time.Now()
	pp := newPolicyPreflight(controllerconfig.AutoscalerPolicyPreflightConfig{SkewDeadlineSeconds: 60})
	now := base
	pp.now = func() time.Time { return now }
	r := &Reconciler{policy: pp}
	aggregateAt := func() *apis.Condition {
		res := &placementResult{candidates: []v1beta1.CandidatePlacement{{Cluster: "a"}}}
		return condOfType(r.policyStatusForWrite(isvc, res), v1beta1.AutoscalerPolicyAggregateCondition)
	}

	// Ref added to a long-placed derived: creation time is far in the past,
	// but the clock starts now, at the first observation.
	oldDerived := policyHomeObservation{
		autoscaling: &v1beta1.CandidateAutoscalingStatus{},
		placedSince: base.Add(-24 * time.Hour),
	}
	pp.recordHome(isvc.UID, "a", oldDerived)
	agg := aggregateAt()
	require.NotNil(t, agg)
	assert.Equal(t, corev1.ConditionTrue, agg.Status, "migration flow must not start past the deadline")

	// The deadline still fires once the home stays silent past it.
	now = base.Add(2 * time.Minute)
	pp.recordHome(isvc.UID, "a", oldDerived)
	agg = aggregateAt()
	require.NotNil(t, agg)
	assert.Equal(t, corev1.ConditionFalse, agg.Status)
	assert.Equal(t, v1beta1.AutoscalerPolicyAggregateReasonResolveTimeout, agg.Reason)

	// A recreated derived restarts the clock: creation later than the anchor
	// moves it forward.
	recreated := oldDerived
	recreated.placedSince = base.Add(90 * time.Second)
	pp.recordHome(isvc.UID, "a", recreated)
	agg = aggregateAt()
	require.NotNil(t, agg)
	assert.Equal(t, corev1.ConditionTrue, agg.Status)
}
