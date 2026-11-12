package rollout

import (
	"testing"

	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// recordedPairAndRouter is a service with an open run that recorded the two
// groups the spec declares: a rolling engine-and-decoder pair under a ratio
// rule at tolerance 5, and a router canary.
func recordedPairAndRouter() *v1beta1.InferenceService {
	pair := v1beta1.RolloutGroup{
		Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent},
		RollingUpdate: &v1beta1.GroupRollingUpdate{},
		MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(5))},
	}
	router := v1beta1.RolloutGroup{
		Components: []v1beta1.ComponentType{v1beta1.RouterComponent},
		Canary:     &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Traffic: 100}}},
	}
	isvc := &v1beta1.InferenceService{}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{*pair.DeepCopy(), *router.DeepCopy()}}
	isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{
		{Source: v1beta1.RolloutPlanSourceInline, Group: *pair.DeepCopy()},
		{Source: v1beta1.RolloutPlanSourceInline, Group: *router.DeepCopy()},
	}}}}
	return isvc
}

// coordinationViewCase is one edit of recordedPairAndRouter and what the
// coordination view must read after it.
type coordinationViewCase struct {
	name  string
	edit  func(isvc *v1beta1.InferenceService)
	check func(t *testing.T, view *v1beta1.RolloutSpec, isvc *v1beta1.InferenceService)
}

var coordinationViewCases = []coordinationViewCase{
	{name: "no run open: the live view",
		edit: func(isvc *v1beta1.InferenceService) { isvc.Status.Rollout = nil },
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if len(view.Groups) != 2 || view.Groups[0].MaintainRatio == nil || *view.Groups[0].MaintainRatio.Tolerance != 5 {
				t.Fatalf("view = %+v, want the spec as declared", view.Groups)
			}
		}},
	{name: "the tolerance raised",
		edit: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].MaintainRatio.Tolerance = ptr.To(int32(20))
		},
		check: func(t *testing.T, view *v1beta1.RolloutSpec, isvc *v1beta1.InferenceService) {
			if got := view.Groups[0].MaintainRatio.Tolerance; got == nil || *got != 20 {
				t.Fatalf("the view reads tolerance %v, want 20 from the spec", got)
			}
			if got := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.MaintainRatio.Tolerance; *got != 5 {
				t.Fatalf("the recorded plan moved to %d", *got)
			}
			if plan := Effective(isvc, Policies{}); *plan.Groups[0].MaintainRatio.Tolerance != 5 {
				t.Fatal("the executors' view must keep the recorded tolerance")
			}
		}},
	{name: "the group's budget lowered",
		edit: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].RollingUpdate.MaxUnavailable = ptr.To(intstr.FromString("5%"))
		},
		check: func(t *testing.T, view *v1beta1.RolloutSpec, isvc *v1beta1.InferenceService) {
			if got := view.Groups[0].RollingUpdate.MaxUnavailable; got == nil || got.String() != "5%" {
				t.Fatalf("the view reads maxUnavailable %v, want 5%% from the spec", got)
			}
			if got := isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.RollingUpdate.MaxUnavailable; got != nil {
				t.Fatalf("the recorded plan moved to %v", got)
			}
		}},
	{name: "the progression changed to blue-green",
		edit: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].RollingUpdate = nil
			isvc.Spec.Rollout.Groups[0].BlueGreen = &v1beta1.GroupBlueGreen{}
		},
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if view.Groups[0].RollingUpdate == nil || view.Groups[0].BlueGreen != nil {
				t.Fatalf("a progression change waits for the next run; the view reads %+v", view.Groups[0])
			}
		}},
	{name: "the rule removed",
		edit: func(isvc *v1beta1.InferenceService) { isvc.Spec.Rollout.Groups[0].MaintainRatio = nil },
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if view.Groups[0].MaintainRatio != nil {
				t.Fatalf("the view keeps a rule the spec no longer names: %+v", view.Groups[0].MaintainRatio)
			}
		}},
	{name: "the members reordered",
		edit: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.EngineComponent}
			isvc.Spec.Rollout.Groups[0].MaintainRatio.Tolerance = ptr.To(int32(20))
		},
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if got := view.Groups[0].MaintainRatio.Tolerance; got == nil || *got != 20 {
				t.Fatalf("a group is found by its members in any order; the view reads %v", got)
			}
		}},
	{name: "the decoder removed from the group",
		edit: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent}
			isvc.Spec.Rollout.Groups[0].MaintainRatio.Tolerance = ptr.To(int32(20))
		},
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if got := view.Groups[0].Components; len(got) != 2 || got[0] != v1beta1.EngineComponent || got[1] != v1beta1.DecoderComponent {
				t.Fatalf("the recorded group keeps the members it recorded; the view reads %v", got)
			}
			if got := view.Groups[0].MaintainRatio.Tolerance; got == nil || *got != 20 {
				t.Fatalf("the rule is read from the spec group that still lists a recorded member; the view reads %v", got)
			}
		}},
	{name: "the router moved into the pair's group",
		edit: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups = isvc.Spec.Rollout.Groups[:1]
			isvc.Spec.Rollout.Groups[0].Components = []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent}
		},
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if len(view.Groups) != 2 || len(view.Groups[0].Components) != 2 || view.Groups[1].Canary == nil {
				t.Fatalf("the recorded pair keeps its two members and the recorded canary group stays; the view reads %+v", view.Groups)
			}
		}},
	{name: "the pair's group replaced by one over the engine and the router",
		edit: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Rollout.Groups = []v1beta1.RolloutGroup{{
				Components:    []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.RouterComponent},
				RollingUpdate: &v1beta1.GroupRollingUpdate{},
				MaintainRatio: &v1beta1.MaintainRatio{Tolerance: ptr.To(int32(20))},
			}}
		},
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if got := view.Groups[0].Components; len(got) != 2 || got[0] != v1beta1.EngineComponent || got[1] != v1beta1.DecoderComponent {
				t.Fatalf("a spec group sharing one recorded member is the same group; the view keeps the recorded members, got %v", got)
			}
			if got := view.Groups[0].MaintainRatio.Tolerance; got == nil || *got != 20 {
				t.Fatalf("that group's rule is read from the spec; the view reads %v", got)
			}
		}},
	{name: "the pair's group removed",
		edit: func(isvc *v1beta1.InferenceService) { isvc.Spec.Rollout.Groups = isvc.Spec.Rollout.Groups[1:] },
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if len(view.Groups) != 2 {
				t.Fatalf("the removed group keeps its place so the groups after it keep their names; got %d groups", len(view.Groups))
			}
			if len(view.Groups[0].Components) != 0 {
				t.Fatalf("the removed group must be handed over with no members; got %v", view.Groups[0].Components)
			}
			if view.Groups[1].Canary == nil || len(view.Groups[1].Components) != 1 {
				t.Fatalf("the canary group stays as recorded; got %+v", view.Groups[1])
			}
		}},
	{name: "the canary group removed",
		edit: func(isvc *v1beta1.InferenceService) { isvc.Spec.Rollout.Groups = isvc.Spec.Rollout.Groups[:1] },
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if len(view.Groups) != 2 || view.Groups[1].Canary == nil || len(view.Groups[1].Components) != 1 {
				t.Fatalf("a canary group stays as recorded until the run closes; got %+v", view.Groups)
			}
		}},
	{name: "the whole rollout block removed",
		edit: func(isvc *v1beta1.InferenceService) { isvc.Spec.Rollout = nil },
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if len(view.Groups) != 2 || len(view.Groups[0].Components) != 0 || view.Groups[1].Canary == nil {
				t.Fatalf("with no rollout block the pair's group is handed over empty and the canary stays; got %+v", view.Groups)
			}
		}},
	{name: "a group added beside the recorded ones waits for the next run",
		edit: func(isvc *v1beta1.InferenceService) {
			isvc.Status.Rollout.ActiveRun.Plan.Groups = isvc.Status.Rollout.ActiveRun.Plan.Groups[1:]
		},
		check: func(t *testing.T, view *v1beta1.RolloutSpec, _ *v1beta1.InferenceService) {
			if len(view.Groups) != 1 || view.Groups[0].Canary == nil {
				t.Fatalf("a group the run did not record is not read until the next run; got %+v", view.Groups)
			}
		}},
}

// While a run is open the coordination view is the recorded plan with the
// ratio rule and the group's presence read from the spec as it is now: a
// raised tolerance and a removed rule reach the view at once, a group the
// spec no longer declares is handed over with no members and keeps its
// place, and a canary group stays as recorded whatever the spec says.
func TestEffectiveCoordinationReadsTheRuleAndTheGroupFromTheSpec(t *testing.T) {
	for _, tc := range coordinationViewCases {
		t.Run(tc.name, func(t *testing.T) {
			isvc := recordedPairAndRouter()
			tc.edit(isvc)
			tc.check(t, EffectiveCoordination(isvc, Policies{}), isvc)
		})
	}
}
