package isvc

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

const forceOpenRun = "svc-a-4f1c2b9e"

// forceService is a service whose open run, when runID is set, records a
// canary group on the engine and moves the engine from version 1 to 2.
func forceService(annotations map[string]string, runID string) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "svc-a", Namespace: "team-a", Annotations: annotations}}
	if runID != "" {
		isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
			RunID: runID,
			Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{
				Source: v1beta1.RolloutPlanSourceInline,
				Group: v1beta1.RolloutGroup{
					Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
					Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
						{Capacity: intstr.FromString("50%"), Traffic: 50},
					}},
				},
			}}},
			TargetRevisions: []v1beta1.RolloutRunTarget{
				{Component: v1beta1.EngineComponent, Revision: "v2", StableRevision: "v1"},
			},
		}}
	}
	return isvc
}

// rollingOnly makes the open run's recorded plan a rolling update with no
// canary group.
func rollingOnly(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	isvc.Status.Rollout.ActiveRun.Plan.Groups[0].Group.Canary = nil
	return isvc
}

// firstRollout makes the open run the engine's first rollout: it records no
// earlier version.
func firstRollout(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "v1"},
	}
	return isvc
}

// runningWithoutRecordedStable gives the engine a live canary record at its
// first step.
func runningWithoutRecordedStable(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	isvc.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.EngineComponent: {Canary: &v1beta1.CanaryStatus{CanaryRevisionHash: "v1", StableRevisionHash: "v0"}},
	}
	return isvc
}

// canaryUnchanged makes the open run move only the router, a member of no
// canary group, and leave the engine's canary group on version 1.
func canaryUnchanged(isvc *v1beta1.InferenceService) *v1beta1.InferenceService {
	isvc.Status.Rollout.ActiveRun.TargetRevisions = []v1beta1.RolloutRunTarget{
		{Component: v1beta1.EngineComponent, Revision: "v1", StableRevision: "v1"},
		{Component: v1beta1.RouterComponent, Revision: "v2", StableRevision: "v1"},
	}
	return isvc
}

func forceAnnotations(run string) map[string]string {
	return map[string]string{
		constants.RolloutForceModeAnnotation:   constants.RolloutForceModeDisruptive,
		constants.RolloutForceRunAnnotation:    run,
		constants.RolloutForceReasonAnnotation: "version 1 fails every request",
	}
}

// withMode is a with its force mode set to mode.
func withMode(a map[string]string, mode string) map[string]string {
	a[constants.RolloutForceModeAnnotation] = mode
	return a
}

// forceRequestCase is one admission check of a force request.
type forceRequestCase struct {
	name         string
	configured   bool
	old, new     *v1beta1.InferenceService
	wantContains string // empty: admitted
	wantWarning  bool
}

// admittedForceRequests are the force requests admission lets through.
func admittedForceRequests() []forceRequestCase {
	return []forceRequestCase{
		{
			name:       "a request without a force is untouched",
			configured: false,
			old:        forceService(nil, forceOpenRun), new: forceService(map[string]string{"team": "a"}, forceOpenRun),
		},
		{
			// Removing a force stops it; any responder may do that, configured or not.
			name:       "removing a force is admitted",
			configured: false,
			old:        forceService(forceAnnotations(forceOpenRun), forceOpenRun), new: forceService(nil, forceOpenRun),
		},
		{
			// The executor ends the force on the missing mode and removes
			// the run key it leaves.
			name:       "removing only the mode key is admitted",
			configured: false,
			old:        forceService(forceAnnotations(forceOpenRun), forceOpenRun),
			new: forceService(map[string]string{
				constants.RolloutForceRunAnnotation:    forceOpenRun,
				constants.RolloutForceReasonAnnotation: "version 1 fails every request",
			}, forceOpenRun),
		},
		{
			name:       "an unchanged force is admitted",
			configured: false,
			old:        forceService(forceAnnotations(forceOpenRun), forceOpenRun), new: forceService(forceAnnotations(forceOpenRun), forceOpenRun),
		},
		{
			// The canary started from a stable revision read off the replica,
			// so the run records none; the live canary record is enough.
			name:        "a force on a canary started without a recorded stable revision is admitted",
			configured:  true,
			old:         runningWithoutRecordedStable(firstRollout(forceService(nil, forceOpenRun))),
			new:         runningWithoutRecordedStable(firstRollout(forceService(forceAnnotations(forceOpenRun), forceOpenRun))),
			wantWarning: true,
		},
		{
			name:       "a force for the open run is admitted with the outage warning",
			configured: true,
			old:        forceService(nil, forceOpenRun), new: forceService(forceAnnotations(forceOpenRun), forceOpenRun),
			wantWarning: true,
		},
	}
}

// nothingToForceRequests are the force requests admission refuses because
// the open run starts no canary.
func nothingToForceRequests() []forceRequestCase {
	return []forceRequestCase{
		{
			name:       "a force on a service whose rollout has no canary is refused",
			configured: true,
			old:        rollingOnly(forceService(nil, forceOpenRun)), new: rollingOnly(forceService(forceAnnotations(forceOpenRun), forceOpenRun)),
			wantContains: "RolloutForceNoCanary",
		},
		{
			// A first rollout has no earlier version to replace.
			name:       "a force on a first rollout is refused",
			configured: true,
			old:        firstRollout(forceService(nil, forceOpenRun)), new: firstRollout(forceService(forceAnnotations(forceOpenRun), forceOpenRun)),
			wantContains: "RolloutForceNoCanary",
		},
		{
			name:       "a force on a run that changes no canary group is refused",
			configured: true,
			old:        canaryUnchanged(forceService(nil, forceOpenRun)), new: canaryUnchanged(forceService(forceAnnotations(forceOpenRun), forceOpenRun)),
			wantContains: "RolloutForceNoCanary",
		},
		{
			// An earlier run left the engine's record short of its last step;
			// the open run leaves the engine on version 1, so nothing acts on it.
			name:         "a force on a run that leaves a group with an old record alone is refused",
			configured:   true,
			old:          runningWithoutRecordedStable(canaryUnchanged(forceService(nil, forceOpenRun))),
			new:          runningWithoutRecordedStable(canaryUnchanged(forceService(forceAnnotations(forceOpenRun), forceOpenRun))),
			wantContains: "RolloutForceNoCanary",
		},
	}
}

// refusedForceRequests are the force requests admission refuses.
func refusedForceRequests() []forceRequestCase {
	return []forceRequestCase{
		{
			name:         "a run key added without a mode is refused",
			configured:   true,
			old:          forceService(nil, forceOpenRun),
			new:          forceService(map[string]string{constants.RolloutForceRunAnnotation: forceOpenRun}, forceOpenRun),
			wantContains: "RolloutForceRunWithoutMode",
		},
		{
			name:       "a cluster without the forceComplete block refuses every force",
			configured: false,
			old:        forceService(nil, forceOpenRun), new: forceService(forceAnnotations(forceOpenRun), forceOpenRun),
			wantContains: "RolloutForceNotConfigured",
		},
		{
			name:       "a blue-green force is refused",
			configured: true,
			old:        forceService(nil, forceOpenRun), new: forceService(withMode(forceAnnotations(forceOpenRun), constants.RolloutForceModeBlueGreen), forceOpenRun),
			wantContains: "RolloutForceModeUnavailable",
		},
		{
			// A changed mode is a new request, checked like one.
			name:       "changing an admitted force to another mode is checked again",
			configured: true,
			old:        forceService(forceAnnotations(forceOpenRun), forceOpenRun), new: forceService(withMode(forceAnnotations(forceOpenRun), constants.RolloutForceModeBlueGreen), forceOpenRun),
			wantContains: "RolloutForceModeUnavailable",
		},
		{
			// A force left from an earlier run names that run; moving it to
			// the open run is a new request.
			name:       "moving a force to another run is checked again",
			configured: true,
			old:        forceService(forceAnnotations("svc-a-00000000"), forceOpenRun), new: forceService(forceAnnotations("svc-a-11111111"), forceOpenRun),
			wantContains: "RolloutForceRunMismatch",
		},
		{
			name:       "a force naming another run is refused",
			configured: true,
			old:        forceService(nil, forceOpenRun), new: forceService(forceAnnotations("svc-a-00000000"), forceOpenRun),
			wantContains: "RolloutForceRunMismatch",
		},
		{
			name:       "a force on a service with no open run is refused",
			configured: true,
			old:        forceService(nil, ""), new: forceService(forceAnnotations(forceOpenRun), ""),
			wantContains: "RolloutForceNoOpenRun",
		},
		{
			name:       "a force on a paused rollout is refused",
			configured: true,
			old:        forceService(map[string]string{constants.PausedRolloutAnnotation: "true"}, forceOpenRun),
			new: forceService(map[string]string{constants.PausedRolloutAnnotation: "true",
				constants.RolloutForceModeAnnotation: constants.RolloutForceModeDisruptive,
				constants.RolloutForceRunAnnotation:  forceOpenRun, constants.RolloutForceReasonAnnotation: "r"}, forceOpenRun),
			wantContains: "RolloutForcePaused",
		},
		{
			// The open run is read from the stored object; a new one has none.
			name:       "a force on create is refused",
			configured: true,
			old:        nil, new: forceService(forceAnnotations(forceOpenRun), forceOpenRun),
			wantContains: "RolloutForceNoOpenRun",
		},
	}
}

func TestValidateForceRequest(t *testing.T) {
	cases := append(admittedForceRequests(), refusedForceRequests()...)
	for _, tc := range append(cases, nothingToForceRequests()...) {
		t.Run(tc.name, func(t *testing.T) {
			v := &InferenceServiceValidator{ForceCompleteConfigured: tc.configured}
			warnings, err := v.validateForceRequest(tc.old, tc.new)
			if tc.wantContains == "" && err != nil {
				t.Fatalf("expected admitted, got: %v", err)
			}
			if tc.wantContains != "" && (err == nil || !strings.Contains(err.Error(), tc.wantContains)) {
				t.Fatalf("expected an error containing %q, got: %v", tc.wantContains, err)
			}
			if got := len(warnings) == 1 && strings.Contains(warnings[0], "serves nothing until"); got != tc.wantWarning {
				t.Fatalf("outage warning: got %v (%v), want %v", got, warnings, tc.wantWarning)
			}
		})
	}
}
