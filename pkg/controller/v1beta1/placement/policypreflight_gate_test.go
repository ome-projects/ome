package placement

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"knative.dev/pkg/apis"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
)

// failingRolloutPolicyReader fails every control-plane RolloutPolicy read
// with a plain (non-NotFound) error and serves every other read.
type failingRolloutPolicyReader struct {
	client.Reader
}

func (f failingRolloutPolicyReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.RolloutPolicy); ok {
		return errors.New("etcdserver: request timed out")
	}
	return f.Reader.Get(ctx, key, obj, opts...)
}

// TestPreflightGateBytes records, for every ending the two policy preflights
// share (an unreadable control-plane policy, a candidate without the
// capability label, no candidate left, the serving cluster itself ineligible),
// the outcome and the staged condition byte for byte.
func TestPreflightGateBytes(t *testing.T) {
	s := testScheme(t)
	always := true
	autoscalerLabel := constants.WorkloadClusterAutoscalerPolicyLabel + "=" + WorkloadClusterAutoscalerPolicyCapability
	rolloutLabel := constants.WorkloadClusterRolloutPolicyLabel + "=" + WorkloadClusterRolloutPolicyCapability
	anchorAbsent := apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "autoscalerpolicies"}, testPolicyName).Error()
	rolloutAbsent := apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "rolloutpolicies"}, testRolloutPolicyName).Error()
	selectorOnly := map[string]string{"gpu": "gb300"}

	type row struct {
		name string
		// rollout picks the rollout preflight; the autoscaler preflight otherwise.
		rollout bool
		// placed marks the source as Placed on cluster "a".
		placed bool
		// readFails makes every control-plane read of the policy kind fail.
		readFails bool
		// withPolicy seeds the control-plane policy copy.
		withPolicy bool
		// candidates and their WorkloadCluster labels, in candidate order.
		candidates []string
		labels     map[string]map[string]string
		// members are the connected member clients (autoscaler preflight only).
		members     map[string]workloadcluster.SelectivelyCachingClient
		wantOutcome policyPreflightOutcome
		// wantStaged is nil when the pass stages no condition.
		wantStaged *apis.Condition
	}
	rows := []row{
		{
			name: "autoscaler anchor absent", withPolicy: false,
			candidates: []string{"a"}, labels: map[string]map[string]string{"a": capabilityLabels()},
			wantOutcome: policyPreflightOutcome{hold: true},
			wantStaged: &apis.Condition{Status: corev1.ConditionFalse, Reason: v1beta1.PlacementPolicyPreflightReasonPolicyMissing,
				Message: "AutoscalerPolicy prod/" + testPolicyName + " has no readable anchor copy on control plane cp-east: " + anchorAbsent + "; holding placement"},
		},
		{
			name: "autoscaler anchor unreadable while placed", placed: true, readFails: true, withPolicy: true,
			candidates: []string{"a"}, labels: map[string]map[string]string{"a": capabilityLabels()},
			wantOutcome: policyPreflightOutcome{holdAsIs: true, transient: true},
		},
		{
			name: "autoscaler anchor unreadable before placement", readFails: true, withPolicy: true,
			candidates: []string{"a"}, labels: map[string]map[string]string{"a": capabilityLabels()},
			wantOutcome: policyPreflightOutcome{hold: true, transient: true},
			wantStaged: &apis.Condition{Status: corev1.ConditionFalse, Reason: v1beta1.PlacementPolicyPreflightReasonPolicyMissing,
				Message: "AutoscalerPolicy prod/" + testPolicyName + " has no readable anchor copy on control plane cp-east: etcdserver: request timed out; holding placement"},
		},
		{
			name: "autoscaler candidate without the label", withPolicy: true,
			candidates: []string{"a", "b"}, labels: map[string]map[string]string{"a": capabilityLabels(), "b": selectorOnly},
			members: map[string]workloadcluster.SelectivelyCachingClient{
				"a": memberWith(t, s, hpaPolicy(testPolicyName)), "b": memberWith(t, s, hpaPolicy(testPolicyName)),
			},
			wantOutcome: policyPreflightOutcome{eligible: []string{"a"}},
			wantStaged: &apis.Condition{Status: corev1.ConditionTrue, Reason: v1beta1.PlacementPolicyPreflightReasonPassed,
				Message: "1 of 2 candidates eligible; skipped: b (" + v1beta1.PlacementPolicyPreflightReasonCapabilityMissing + ": cluster does not carry " + autoscalerLabel + ")"},
		},
		{
			name: "autoscaler member copy missing", withPolicy: true,
			candidates: []string{"a", "b"}, labels: map[string]map[string]string{"a": capabilityLabels(), "b": capabilityLabels()},
			members: map[string]workloadcluster.SelectivelyCachingClient{
				"a": memberWith(t, s, hpaPolicy(testPolicyName)), "b": memberWith(t, s),
			},
			wantOutcome: policyPreflightOutcome{eligible: []string{"a"}},
			wantStaged: &apis.Condition{Status: corev1.ConditionTrue, Reason: v1beta1.PlacementPolicyPreflightReasonPassed,
				Message: "1 of 2 candidates eligible; skipped: b (" + v1beta1.PlacementPolicyPreflightReasonPolicyMissing + ": policy \"" + testPolicyName + "\" not found on member)"},
		},
		{
			name: "autoscaler no candidate left", withPolicy: true,
			candidates: []string{"b"}, labels: map[string]map[string]string{"b": selectorOnly},
			wantOutcome: policyPreflightOutcome{hold: true},
			wantStaged: &apis.Condition{Status: corev1.ConditionFalse, Reason: v1beta1.PlacementPolicyPreflightReasonCapabilityMissing,
				Message: "no eligible candidate remains; skipped: b (" + v1beta1.PlacementPolicyPreflightReasonCapabilityMissing + ": cluster does not carry " + autoscalerLabel + ")"},
		},
		{
			name: "autoscaler serving cluster without the label", placed: true, withPolicy: true,
			candidates: []string{"a", "b"}, labels: map[string]map[string]string{"a": selectorOnly, "b": capabilityLabels()},
			members:     map[string]workloadcluster.SelectivelyCachingClient{"b": memberWith(t, s, hpaPolicy(testPolicyName))},
			wantOutcome: policyPreflightOutcome{holdAsIs: true},
			wantStaged: &apis.Condition{Status: corev1.ConditionFalse, Reason: v1beta1.PlacementPolicyPreflightReasonCapabilityMissing,
				Message: "standing winner is ineligible; skipped: a (" + v1beta1.PlacementPolicyPreflightReasonCapabilityMissing + ": cluster does not carry " + autoscalerLabel + ")"},
		},
		{
			name: "autoscaler serving cluster behind a transient member read", placed: true, withPolicy: true,
			candidates: []string{"a"}, labels: map[string]map[string]string{"a": capabilityLabels()},
			members: map[string]workloadcluster.SelectivelyCachingClient{
				"a": flakyPolicyGetter{SelectivelyCachingClient: memberWith(t, s, hpaPolicy(testPolicyName)), fail: &always},
			},
			wantOutcome: policyPreflightOutcome{holdAsIs: true, transient: true},
		},
		{
			name: "rollout policy absent", rollout: true,
			candidates: []string{"a"}, labels: map[string]map[string]string{"a": rolloutCapabilityLabels()},
			wantOutcome: policyPreflightOutcome{hold: true},
			wantStaged: &apis.Condition{Status: corev1.ConditionFalse, Reason: v1beta1.RolloutPlanReasonPolicyNotFound,
				Message: "RolloutPolicy prod/" + testRolloutPolicyName + " has no readable copy on control plane cp-east: " + rolloutAbsent + "; a ref-only rollout group must never fall back to the default progression, holding placement"},
		},
		{
			name: "rollout policy unreadable while placed", rollout: true, placed: true, readFails: true, withPolicy: true,
			candidates: []string{"a"}, labels: map[string]map[string]string{"a": rolloutCapabilityLabels()},
			wantOutcome: policyPreflightOutcome{holdAsIs: true, transient: true},
		},
		{
			name: "rollout candidate without the label", rollout: true, withPolicy: true,
			candidates: []string{"a", "b"}, labels: map[string]map[string]string{"a": rolloutCapabilityLabels(), "b": selectorOnly},
			wantOutcome: policyPreflightOutcome{eligible: []string{"a"}},
			wantStaged: &apis.Condition{Status: corev1.ConditionTrue, Reason: v1beta1.PlacementPolicyPreflightReasonPassed,
				Message: "rollout policy: 1 of 2 candidates eligible; skipped: b (" + v1beta1.PlacementPolicyPreflightReasonCapabilityMissing + ": cluster does not carry " + rolloutLabel + "; its InferenceService schema would prune the inflated plan's gate fields)"},
		},
		{
			name: "rollout no candidate left", rollout: true, withPolicy: true,
			candidates: []string{"b"}, labels: map[string]map[string]string{"b": selectorOnly},
			wantOutcome: policyPreflightOutcome{hold: true},
			wantStaged: &apis.Condition{Status: corev1.ConditionFalse, Reason: v1beta1.PlacementPolicyPreflightReasonCapabilityMissing,
				Message: "rollout policy: no eligible candidate remains; skipped: b (" + v1beta1.PlacementPolicyPreflightReasonCapabilityMissing + ": cluster does not carry " + rolloutLabel + "; its InferenceService schema would prune the inflated plan's gate fields)"},
		},
		{
			name: "rollout serving cluster without the label", rollout: true, placed: true, withPolicy: true,
			candidates: []string{"a", "b"}, labels: map[string]map[string]string{"a": selectorOnly, "b": rolloutCapabilityLabels()},
			wantOutcome: policyPreflightOutcome{holdAsIs: true},
			wantStaged: &apis.Condition{Status: corev1.ConditionFalse, Reason: v1beta1.PlacementPolicyPreflightReasonCapabilityMissing,
				Message: "rollout policy: standing winner a lacks " + rolloutLabel + "; holding placement"},
		},
	}
	for _, tc := range rows {
		t.Run(tc.name, func(t *testing.T) {
			var isvc *v1beta1.InferenceService
			var objs []client.Object
			if tc.rollout {
				isvc = srcISVCWithRolloutRef("gpu=gb300")
				if tc.withPolicy {
					objs = append(objs, canaryRolloutPolicy(testRolloutPolicyName))
				}
			} else {
				isvc = srcISVCWithRef("gpu=gb300")
				if tc.withPolicy {
					objs = append(objs, hpaPolicy(testPolicyName))
				}
			}
			if tc.placed {
				isvc.Status.Placement = &v1beta1.PlacementStatus{Cluster: "a", Phase: v1beta1.PlacementPhasePlaced}
			}
			members := tc.members
			if members == nil {
				members = map[string]workloadcluster.SelectivelyCachingClient{}
			}
			r, cp := newPlacer(s, fakeClusters{m: members}, append(objs, isvc)...)
			r.ControlPlaneID = "cp-east"
			if tc.readFails {
				if tc.rollout {
					r.APIReader = failingRolloutPolicyReader{Reader: cp}
				} else {
					r.APIReader = flakyAnchorReader{Reader: cp, fail: &always}
				}
			}
			var wcs []v1beta1.WorkloadCluster
			for _, c := range tc.candidates {
				wcs = append(wcs, *readyWC(c, tc.labels[c]))
			}

			var out *policyPreflightOutcome
			var staged policyCondition
			var ok bool
			if tc.rollout {
				out = r.preflightRolloutPolicies(context.Background(), isvc, tc.candidates, wcs)
				staged, ok = r.rolloutState().preflightFor(isvc.UID)
			} else {
				out = r.preflightPolicies(context.Background(), isvc, tc.candidates, wcs)
				staged, ok = r.policyState().preflightFor(isvc.UID)
			}
			require.NotNil(t, out)
			if diff := cmp.Diff(tc.wantOutcome, *out, cmp.AllowUnexported(policyPreflightOutcome{})); diff != "" {
				t.Fatalf("outcome (-want +got):\n%s", diff)
			}
			if tc.wantStaged == nil {
				require.False(t, ok, fmt.Sprintf("no condition expected, staged %+v", staged.cond))
				return
			}
			require.True(t, ok, "a condition must be staged")
			require.Equal(t, apis.ConditionType(v1beta1.PlacementPolicyPreflightCondition), staged.condType)
			if diff := cmp.Diff(*tc.wantStaged, staged.cond); diff != "" {
				t.Fatalf("staged condition (-want +got):\n%s", diff)
			}
		})
	}
}
