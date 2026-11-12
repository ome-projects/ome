package placement

import (
	"maps"
	"testing"

	"github.com/google/go-cmp/cmp"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// A routing publisher option annotation configures how the control plane's
// TrafficMap publisher publishes the service. It never makes a new placement
// plan and never reaches a member.

var routingPublisherOptionKey = constants.RoutingPublisherOptionAnnotationPrefix + "routingClass"

func withRoutingPublisherOption(s *v1beta1.InferenceService) *v1beta1.InferenceService {
	out := s.DeepCopy()
	if out.Annotations == nil {
		out.Annotations = map[string]string{}
	}
	out.Annotations[routingPublisherOptionKey] = "premium"
	return out
}

func TestPlanIntentAnnotations_DropsCleanupRequestAndRoutingPublisherOptions(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   map[string]string
		want map[string]string
	}{
		{name: "none", in: nil, want: nil},
		{name: "nothing to drop", in: map[string]string{constants.NetworkVisibility: "cluster-local"},
			want: map[string]string{constants.NetworkVisibility: "cluster-local"}},
		{name: "a routing publisher option", in: map[string]string{constants.NetworkVisibility: "cluster-local", routingPublisherOptionKey: "premium"},
			want: map[string]string{constants.NetworkVisibility: "cluster-local"}},
		{name: "only a routing publisher option", in: map[string]string{routingPublisherOptionKey: "premium"}, want: nil},
		{name: "both kinds", in: map[string]string{
			constants.PlacementReleaseCleanupWaitAnnotation: "7", routingPublisherOptionKey: "premium", constants.NetworkVisibility: "cluster-local",
		}, want: map[string]string{constants.NetworkVisibility: "cluster-local"}},
		{name: "a lookalike key stays", in: map[string]string{constants.OMEAPIGroupName + "/routing-publisher-routingClass": "premium"},
			want: map[string]string{constants.OMEAPIGroupName + "/routing-publisher-routingClass": "premium"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := maps.Clone(tt.in)
			if diff := cmp.Diff(tt.want, planIntentAnnotations(tt.in)); diff != "" {
				t.Fatalf("planIntentAnnotations (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, tt.in); diff != "" {
				t.Fatalf("planIntentAnnotations changed its input (-before +after):\n%s", diff)
			}
		})
	}
}

// TestPlanDigest_RoutingPublisherOption_InputDigestUnchanged builds the Split,
// All and Single proposals with and without a routing publisher option. Each
// keeps its input digest, so setting or removing the option makes no new plan
// revision.
func TestPlanDigest_RoutingPublisherOption_InputDigestUnchanged(t *testing.T) {
	splitDigest := func(t *testing.T) func(*v1beta1.InferenceService) string {
		return func(s *v1beta1.InferenceService) string {
			proposal, err := desiredSplitPlan(s, twoMembers(), nil)
			if err != nil {
				t.Fatal(err)
			}
			return proposal.InputDigest
		}
	}
	for _, tt := range []struct {
		name        string
		annotations map[string]string
	}{
		{name: "Split beside another annotation", annotations: map[string]string{constants.NetworkVisibility: "cluster-local"}},
		{name: "Split as the only annotation", annotations: nil},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := srcISVCMode(v1beta1.PlacementModeSplit, "")
			source.Spec.Engine.MinReplicas = ptr.To(6)
			source.Annotations = maps.Clone(tt.annotations)
			digest := splitDigest(t)
			if before, after := digest(source), digest(withRoutingPublisherOption(source)); before != after {
				t.Fatalf("the option changed the input digest from %s to %s", before, after)
			}
		})
	}
	t.Run("All", func(t *testing.T) {
		f := newBackendFixture(t, v1beta1.PlacementModeAll)
		source := f.reconcile(t)
		digest := func(s *v1beta1.InferenceService) string {
			proposal, _, err := f.reconciler.allProposal(t.Context(), s, f.clusters, []string{"member-a", "member-b"})
			if err != nil {
				t.Fatal(err)
			}
			return proposal.InputDigest
		}
		if before, after := digest(source), digest(withRoutingPublisherOption(source)); before != after {
			t.Fatalf("the option changed the input digest from %s to %s", before, after)
		}
	})
	t.Run("Single", func(t *testing.T) {
		f := singleFixture(t, false)
		source := f.reconcile(t)
		digest := func(s *v1beta1.InferenceService) string {
			proposal, err := f.reconciler.singleProposal(t.Context(), s, f.clusters, "", []string{"member-a", "member-b"})
			if err != nil {
				t.Fatal(err)
			}
			return proposal.InputDigest
		}
		if before, after := digest(source), digest(withRoutingPublisherOption(source)); before != after {
			t.Fatalf("the option changed the input digest from %s to %s", before, after)
		}
	})
}
