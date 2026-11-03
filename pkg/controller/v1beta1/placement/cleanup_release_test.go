package placement

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/placement/allocation"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// TestAdvanceSplitPlan_CleanupRelease_SkipsOnlyCleanupWait completes paused
// moves whose members hold more replicas than the plan at revision 7. A request
// for revision 7 ends that cleanup wait in Split, All and a Single move. A
// request for another revision, or a value that is not a revision, keeps it.
// The request never ends another wait: a pause members have not acknowledged,
// a target a member has not applied, or a leaving member that still holds
// replicas.
func TestAdvanceSplitPlan_CleanupRelease_SkipsOnlyCleanupWait(t *testing.T) {
	type edit func(*v1beta1.InferenceService, map[string]plannedMemberObservation)
	request := func(value string) edit {
		return func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
			s.Annotations = map[string]string{constants.PlacementReleaseCleanupWaitAnnotation: value}
		}
	}
	chain := func(edits ...edit) edit {
		return func(s *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			for _, apply := range edits {
				apply(s, o)
			}
		}
	}
	observed := func(name string, change func(*plannedMemberObservation)) edit {
		return func(_ *v1beta1.InferenceService, o map[string]plannedMemberObservation) {
			observation := o[name]
			change(&observation)
			o[name] = observation
		}
	}
	splitTargets := map[string]int32{"a": 3, "b": 3, "c": 0}
	singleTargets := map[string]int32{"member-a": 0, "member-b": 3}
	for _, tt := range []struct {
		name string
		base func() (*v1beta1.InferenceService, map[string]plannedMemberObservation)
		edit edit
		want allocation.Step
	}{
		{name: "Split without a request", base: splitCleanupWait,
			want: allocation.Step{Targets: splitTargets, Reason: "AwaitingMemberCleanup"}},
		{name: "Split request for the current revision", base: splitCleanupWait, edit: request("7"),
			want: allocation.Step{Targets: splitTargets, Complete: true, CleanupReleased: true}},
		{name: "Split request for an earlier revision", base: splitCleanupWait, edit: request("6"),
			want: allocation.Step{Targets: splitTargets, Reason: "AwaitingMemberCleanup"}},
		{name: "Split request for a later revision", base: splitCleanupWait, edit: request("8"),
			want: allocation.Step{Targets: splitTargets, Reason: "AwaitingMemberCleanup"}},
		{name: "Split request with a sign", base: splitCleanupWait, edit: request("+7"),
			want: allocation.Step{Targets: splitTargets, Reason: "AwaitingMemberCleanup"}},
		{name: "Split request with a leading zero", base: splitCleanupWait, edit: request("07"),
			want: allocation.Step{Targets: splitTargets, Reason: "AwaitingMemberCleanup"}},
		{name: "Split request that is not a number", base: splitCleanupWait, edit: request("seven"),
			want: allocation.Step{Targets: splitTargets, Reason: "AwaitingMemberCleanup"}},
		{name: "pause members have not acknowledged", base: splitCleanupWait,
			edit: chain(request("7"), observed("a", func(o *plannedMemberObservation) { o.PauseAcknowledged = false })),
			want: allocation.Step{Targets: splitTargets, Reason: "AwaitingSurgePause"}},
		{name: "target a member has not applied", base: splitCleanupWait,
			edit: chain(request("7"), observed("a", func(o *plannedMemberObservation) { o.MemberState.Applied = false })),
			want: allocation.Step{Targets: splitTargets, Reason: "AwaitingMemberConvergence"}},
		{name: "leaving member that still holds replicas", base: splitCleanupWait,
			edit: chain(request("7"), observed("c", func(o *plannedMemberObservation) {
				o.MemberState, o.PauseAcknowledged = allocation.MemberState{Known: true, Drained: true, Occupied: 2}, true
			})),
			want: allocation.Step{Targets: splitTargets, Reason: "AwaitingMemberConvergence"}},
		{name: "plan that does not pause", base: splitCleanupWait,
			edit: chain(request("7"), func(s *v1beta1.InferenceService, _ map[string]plannedMemberObservation) {
				s.Status.Placement.Plan.PauseSurge = false
			}),
			want: allocation.Step{Targets: splitTargets, Complete: true}},
		{name: "All without a request", base: allCleanupWait,
			want: allocation.Step{Targets: map[string]int32{"a": 3, "b": 3}, Reason: "AwaitingMemberCleanup"}},
		{name: "All request for the current revision", base: allCleanupWait, edit: request("7"),
			want: allocation.Step{Targets: map[string]int32{"a": 3, "b": 3}, Complete: true, CleanupReleased: true}},
		{name: "Single move without a request", base: singleMoveCleanupWait,
			want: allocation.Step{Targets: singleTargets, Reason: "AwaitingMemberCleanup"}},
		{name: "Single move request for the current revision", base: singleMoveCleanupWait, edit: request("7"),
			want: allocation.Step{Targets: singleTargets, Complete: true, CleanupReleased: true}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s, o := tt.base()
			if tt.edit != nil {
				tt.edit(s, o)
			}
			got, err := advanceSplitPlan(s, o)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.want, got); diff != "" {
				t.Fatalf("step (-want +got):\n%s", diff)
			}
		})
	}
}

// splitCleanupWait is a finished Split move at plan revision 7: member c left
// and is gone, and members a and b keep their shares of 3 while their
// autoscalers run 6 and 5 replicas, more than the plan's 6.
func splitCleanupWait() (*v1beta1.InferenceService, map[string]plannedMemberObservation) {
	s := srcISVCMode(v1beta1.PlacementModeSplit, "")
	s.Spec.Engine.MinReplicas, s.Spec.Placement.Split = ptr.To(6), &v1beta1.SplitSpec{MaxPerCluster: engineCap(18)}
	s.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{Revision: 7, InputDigest: "intent", AssignedReplicas: 6, PauseSurge: true}}
	o := map[string]plannedMemberObservation{}
	for _, m := range []struct {
		name     string
		occupied int32
	}{{"a", 6}, {"b", 5}} {
		s.Status.Placement.Candidates = append(s.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: m.name, Allocation: &v1beta1.CandidateAllocationStatus{
			ClusterUID: types.UID(m.name + "-uid"), OriginalReplicas: 3, CurrentReplicas: 3, DesiredReplicas: 3,
		}})
		member := s.DeepCopy()
		member.Spec.Engine.MinReplicas = ptr.To(3)
		o[m.name] = plannedMemberObservation{
			Member: member, ComponentReady: map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 3},
			MemberState: allocation.MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: m.occupied}, PauseAcknowledged: true,
		}
	}
	s.Status.Placement.Candidates = append(s.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: "c", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "c-uid", OriginalReplicas: 3}})
	o["c"] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Absent: true}}
	return s, o
}

// allCleanupWait is a finished All move at plan revision 7 that raised member
// a's engine minimum from 2 to 3. Each member's autoscaler keeps more engines
// than its minimum.
func allCleanupWait() (*v1beta1.InferenceService, map[string]plannedMemberObservation) {
	policy := &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.DecoderComponent, Replicas: 2}, {Component: v1beta1.EngineComponent, Replicas: 3}}}
	s := srcISVCMode(v1beta1.PlacementModeAll, "")
	s.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{Mode: v1beta1.PlacementModeAll, Revision: 7, InputDigest: "intent", AssignedReplicas: 6, PauseSurge: true}}
	o := map[string]plannedMemberObservation{}
	for _, m := range []struct {
		name               string
		original, occupied int32
	}{{"a", 2, 6}, {"b", 3, 5}} {
		s.Status.Placement.Candidates = append(s.Status.Placement.Candidates, v1beta1.CandidatePlacement{Cluster: m.name, Allocation: &v1beta1.CandidateAllocationStatus{
			ClusterUID: types.UID(m.name + "-uid"), Matched: true, OriginalReplicas: m.original, CurrentHome: policy.DeepCopy(), CurrentReplicas: 3, DesiredHome: policy.DeepCopy(), DesiredReplicas: 3,
		}})
		o[m.name] = plannedMemberObservation{MemberState: allocation.MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: m.occupied}, PauseAcknowledged: true}
	}
	return s, o
}

// singleMoveCleanupWait is a Single move at plan revision 7 that selected
// member-b to replace member-a. member-a is gone and member-b serves its 3
// replicas while its autoscaler keeps 5. The plan records the selection, the
// part of a move that advanceSplitPlan reads.
func singleMoveCleanupWait() (*v1beta1.InferenceService, map[string]plannedMemberObservation) {
	policy := &v1beta1.PlacementHomePolicy{InputDigest: "intent", ReplicaFloors: []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 3}}}
	s := srcISVCMode(v1beta1.PlacementModeSingle, "")
	s.Status.Placement = &v1beta1.PlacementStatus{Plan: &v1beta1.PlacementPlanStatus{
		Mode: v1beta1.PlacementModeSingle, SingleMove: &v1beta1.PlacementSingleMoveStatus{Selected: "member-b"},
		Revision: 7, InputDigest: "intent", AssignedReplicas: 3, PauseSurge: true,
	}, Candidates: []v1beta1.CandidatePlacement{
		{Cluster: "member-a", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "member-a-uid", OriginalReplicas: 3, DrainRequested: true}},
		{Cluster: "member-b", Allocation: &v1beta1.CandidateAllocationStatus{ClusterUID: "member-b-uid", CurrentHome: policy.DeepCopy(), CurrentReplicas: 3, DesiredHome: policy.DeepCopy(), DesiredReplicas: 3}},
	}}
	o := map[string]plannedMemberObservation{
		"member-a": {MemberState: allocation.MemberState{Known: true, Absent: true}},
		"member-b": {MemberState: allocation.MemberState{Known: true, Eligible: true, Applied: true, Routable: true, Ready: 3, Occupied: 5}, PauseAcknowledged: true, FullPolicyReady: true},
	}
	return s, o
}

// TestCleanupRelease_CurrentRevision_EndsPause brings an autoscaled Split and
// an autoscaled All service to a finished move that waits for cleanup, and
// shows the wait holds. The operator sets the annotation to the plan revision.
// The next pass ends the pause with a new revision, says so in the condition
// and an event, and removes the annotation. Growth then stays unpaused.
func TestCleanupRelease_CurrentRevision_EndsPause(t *testing.T) {
	for _, shape := range cleanupWaitShapes {
		t.Run(shape.name, func(t *testing.T) {
			f, pass := shape.hold(t)
			recorder := record.NewFakeRecorder(256)
			f.reconciler.Recorder = recorder
			held := waitForCleanup(t, f, pass)
			revision := held.Status.Placement.Plan.Revision
			requestCleanupRelease(t, f, strconv.FormatInt(revision, 10))
			got := pass()
			if accepted := got.Status.Placement.Plan; accepted.PauseSurge || accepted.Revision != revision+1 {
				t.Fatalf("the request for revision %d did not end the pause: revision=%d %s", revision, accepted.Revision, describeAutoscaledAll(t, f, got))
			}
			if value, present := got.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; present {
				t.Fatalf("the answered request %q stayed on the source", value)
			}
			note := fmt.Sprintf("The operator ended the AwaitingMemberCleanup wait of placement plan revision %d with %s. Members held ", revision, constants.PlacementReleaseCleanupWaitAnnotation)
			if condition := got.Status.GetCondition(v1beta1.PlacementConverged); condition == nil || !strings.Contains(condition.Message, note) {
				t.Fatalf("PlacementConverged does not name the release: %+v", condition)
			}
			assigned := held.Status.Placement.Plan.AssignedReplicas
			released := regexp.MustCompile(fmt.Sprintf(`^Normal %s %s(\d+) replicas, counting replicas reserved for rolling updates, and the plan assigns %d\. Member autoscalers and rollouts may add replicas again$`,
				cleanupWaitReleasedReason, regexp.QuoteMeta(note), assigned))
			events := recordedEvents(recorder)
			var match []string
			if len(events) == 1 {
				match = released.FindStringSubmatch(events[0])
			}
			if match == nil {
				t.Fatalf("events = %q, want one matching %s", events, released)
			}
			if count, err := strconv.ParseInt(match[1], 10, 64); err != nil || count <= assigned {
				t.Fatalf("the event reports %s held replicas, want more than the plan's %d", match[1], assigned)
			}
			for range 3 {
				got = pass()
			}
			if accepted := got.Status.Placement.Plan; accepted.PauseSurge || accepted.Revision != revision+1 || !autoscaledAllConverged(got) {
				t.Fatalf("growth did not stay unpaused at revision %d: revision=%d %s", revision+1, accepted.Revision, describeAutoscaledAll(t, f, got))
			}
			for _, candidate := range got.Status.Placement.Candidates {
				member, _ := allMemberOn(t, f, candidate.Cluster)
				if policy, err := protocol.FromDerived(member); err != nil || policy == nil || policy.PauseSurge {
					t.Fatalf("%s still defers autoscaler growth: %+v (%v)", candidate.Cluster, policy, err)
				}
			}
			if events := recordedEvents(recorder); len(events) > 0 {
				t.Fatalf("unexpected events after the release: %q", events)
			}
		})
	}
}

// TestCleanupRelease_NotCurrentRevision_RemovedWithoutEffect sets values that
// do not name the waiting plan revision. Each is removed on the next pass with
// an event that says why, and the cleanup wait goes on at the same revision. An
// earlier revision therefore never ends a later pause.
func TestCleanupRelease_NotCurrentRevision_RemovedWithoutEffect(t *testing.T) {
	for _, tt := range []struct {
		name  string
		value func(revision int64) string
		event func(revision int64) string
	}{
		{name: "earlier revision", value: func(r int64) string { return strconv.FormatInt(r-1, 10) }, event: func(r int64) string {
			return otherRevisionRemoved(r-1, r)
		}},
		{name: "later revision", value: func(r int64) string { return strconv.FormatInt(r+1, 10) }, event: func(r int64) string {
			return otherRevisionRemoved(r+1, r)
		}},
		{name: "value with a sign", value: func(r int64) string { return "+" + strconv.FormatInt(r, 10) }, event: func(r int64) string {
			return fmt.Sprintf("Warning %s Removed %s=\"+%d\": the value must be a placement plan revision", cleanupReleaseRejectedReason, constants.PlacementReleaseCleanupWaitAnnotation, r)
		}},
		{name: "value that is not a number", value: func(int64) string { return "latest" }, event: func(int64) string {
			return fmt.Sprintf("Warning %s Removed %s=\"latest\": the value must be a placement plan revision", cleanupReleaseRejectedReason, constants.PlacementReleaseCleanupWaitAnnotation)
		}},
		{name: "empty value", value: func(int64) string { return "" }, event: func(int64) string {
			return fmt.Sprintf("Warning %s Removed %s=\"\": the value must be a placement plan revision", cleanupReleaseRejectedReason, constants.PlacementReleaseCleanupWaitAnnotation)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, pass := holdSplitCleanupWait(t)
			recorder := record.NewFakeRecorder(256)
			f.reconciler.Recorder = recorder
			held := waitForCleanup(t, f, pass)
			revision := held.Status.Placement.Plan.Revision
			requestCleanupRelease(t, f, tt.value(revision))
			got := pass()
			if value, present := got.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; present {
				t.Fatalf("the request %q stayed on the source", value)
			}
			if accepted := got.Status.Placement.Plan; !accepted.PauseSurge || accepted.Revision != revision {
				t.Fatalf("the request changed the waiting plan: revision=%d %s", accepted.Revision, describeAutoscaledAll(t, f, got))
			}
			events := recordedEvents(recorder)
			if len(events) != 1 || !strings.HasPrefix(events[0], tt.event(revision)) {
				t.Fatalf("events = %q, want one starting %q", events, tt.event(revision))
			}
			got = pass()
			if condition := got.Status.GetCondition(v1beta1.PlacementConverged); !got.Status.Placement.Plan.PauseSurge || got.Status.Placement.Plan.Revision != revision || condition == nil || condition.Reason != "AwaitingMemberCleanup" {
				t.Fatalf("the cleanup wait did not go on: %s", describeAutoscaledAll(t, f, got))
			}
		})
	}
}

// otherRevisionRemoved is the event for a request that names revision while
// current is the plan revision.
func otherRevisionRemoved(revision, current int64) string {
	return fmt.Sprintf("Normal %s Removed %s=%d: placement plan revision %d is current, and a request acts only on the revision it names",
		cleanupReleaseSkippedReason, constants.PlacementReleaseCleanupWaitAnnotation, revision, current)
}

// TestCleanupRelease_MoveNotFinished_WaitsForMove raises the engine minimum of
// the autoscaled Split service from 12 to 16, and sets the annotation once the
// plan has given each member a share of 8 but before the members apply it. The
// request stays while the move runs; that pass changes no plan revision and no
// member. When the members apply their shares and hold more replicas than the
// plan, the request ends the pause. When they run exactly their minimums, the
// pause ends without it, and the request is removed with an event that says so.
func TestCleanupRelease_MoveNotFinished_WaitsForMove(t *testing.T) {
	for _, tt := range []struct {
		name  string
		run   func(*testing.T, *backendFixture)
		event func(revision int64) string
	}{
		{name: "members hold more replicas than the plan", run: runSplitAboveShares, event: func(r int64) string {
			return fmt.Sprintf("Normal %s The operator ended the AwaitingMemberCleanup wait of placement plan revision %d", cleanupWaitReleasedReason, r)
		}},
		{name: "members run their minimums", run: runSplitAtMinimums, event: func(r int64) string {
			return fmt.Sprintf("Normal %s Removed %s=%d: the pause of placement plan revision %d ended without the request", cleanupReleaseSkippedReason, constants.PlacementReleaseCleanupWaitAnnotation, r, r)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f := autoscaledSplitFixture(t)
			recorder := record.NewFakeRecorder(256)
			f.reconciler.Recorder = recorder
			setAutoscaledAll(t, f, 16, 8, "")
			pass := splitPass(t, f, tt.run)
			var moving *v1beta1.InferenceService
			for range 12 {
				if moving = pass(); sharesSetNotApplied(moving, 8) {
					break
				}
			}
			accepted := moving.Status.Placement.Plan.DeepCopy()
			if !accepted.PauseSurge || !sharesSetNotApplied(moving, 8) {
				t.Fatalf("the raise did not pause growth and set shares of 8: %s", describeAutoscaledAll(t, f, moving))
			}
			requestCleanupRelease(t, f, strconv.FormatInt(accepted.Revision, 10))
			members := memberVersions(t, f)
			pending := f.reconcile(t)
			if value := pending.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; value != strconv.FormatInt(accepted.Revision, 10) {
				t.Fatalf("the request for the running move was removed: %q", value)
			}
			if diff := cmp.Diff(accepted, pending.Status.Placement.Plan); diff != "" {
				t.Fatalf("the request changed the plan (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(members, memberVersions(t, f)); diff != "" {
				t.Fatalf("the request changed members (-want +got):\n%s", diff)
			}
			if events := recordedEvents(recorder); len(events) > 0 {
				t.Fatalf("unexpected events while the move runs: %q", events)
			}
			var got *v1beta1.InferenceService
			for range 12 {
				if got = pass(); !got.Status.Placement.Plan.PauseSurge {
					break
				}
			}
			if got.Status.Placement.Plan.PauseSurge || got.Status.Placement.Plan.Revision != accepted.Revision+1 {
				t.Fatalf("the pause of revision %d did not end after the move: revision=%d %s", accepted.Revision, got.Status.Placement.Plan.Revision, describeAutoscaledAll(t, f, got))
			}
			if value, present := got.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; present {
				t.Fatalf("the answered request %q stayed on the source", value)
			}
			events := recordedEvents(recorder)
			if len(events) != 1 || !strings.HasPrefix(events[0], tt.event(accepted.Revision)) {
				t.Fatalf("events = %q, want one starting %q", events, tt.event(accepted.Revision))
			}
		})
	}
}

// TestCleanupRelease_RemovalFails_NextPassRemoves ends the Split cleanup wait
// with a request for the current revision while every removal of the request
// fails. The pause still ends with a CleanupWaitReleased event, a
// CleanupReleaseNotRemoved warning follows, and the request stays. Once
// removals work again, the next pass removes it with an event that stays true
// after the release, and the plan does not change.
func TestCleanupRelease_RemovalFails_NextPassRemoves(t *testing.T) {
	f, pass := holdSplitCleanupWait(t)
	recorder := record.NewFakeRecorder(256)
	f.reconciler.Recorder = recorder
	held := waitForCleanup(t, f, pass)
	revision := held.Status.Placement.Plan.Revision
	value := strconv.FormatInt(revision, 10)
	requestCleanupRelease(t, f, value)
	failing := true
	unavailable := apierrors.NewServiceUnavailable("the API server is overloaded")
	failCleanupReleaseRemoval(f, &failing, unavailable)
	got := pass()
	if accepted := got.Status.Placement.Plan; accepted.PauseSurge || accepted.Revision != revision+1 {
		t.Fatalf("the request for revision %d did not end the pause: revision=%d %s", revision, accepted.Revision, describeAutoscaledAll(t, f, got))
	}
	if stayed := got.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; stayed != value {
		t.Fatalf("the request is %q after a failed removal, want %q", stayed, value)
	}
	warning := fmt.Sprintf("Warning %s Could not remove %s=%q: %v. The request cannot act on this or any later plan revision, so placement goes on",
		cleanupReleaseNotRemovedReason, constants.PlacementReleaseCleanupWaitAnnotation, value, unavailable)
	if events := recordedEvents(recorder); len(events) != 2 || !strings.HasPrefix(events[0], "Normal "+cleanupWaitReleasedReason+" ") || !strings.HasPrefix(events[1], warning) {
		t.Fatalf("events = %q, want a %s event and then one starting %q", events, cleanupWaitReleasedReason, warning)
	}
	failing = false
	got = pass()
	if stayed, present := got.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; present {
		t.Fatalf("the request %q stayed once removals worked", stayed)
	}
	if accepted := got.Status.Placement.Plan; accepted.PauseSurge || accepted.Revision != revision+1 {
		t.Fatalf("removing the request changed the plan: revision=%d %s", accepted.Revision, describeAutoscaledAll(t, f, got))
	}
	if events, want := recordedEvents(recorder), otherRevisionRemoved(revision, revision+1); len(events) != 1 || events[0] != want {
		t.Fatalf("events = %q, want %q", events, want)
	}
}

// TestCleanupRelease_OutdatedRemovalFails_PlacementGoesOn holds the Split
// cleanup wait, sets a request that can never act, makes every removal of it
// forbidden, and lowers the engine minimum back to 12. Placement goes on to a
// new plan revision with a CleanupReleaseNotRemoved warning, and the request
// stays without effect. Once removals work, the next pass removes it.
func TestCleanupRelease_OutdatedRemovalFails_PlacementGoesOn(t *testing.T) {
	key := constants.PlacementReleaseCleanupWaitAnnotation
	for _, tt := range []struct {
		name    string
		value   func(revision int64) string
		removed func(value string) string
	}{
		{name: "value that is not a revision", value: func(int64) string { return "latest" }, removed: func(v string) string {
			return fmt.Sprintf("Warning %s Removed %s=%q: the value must be a placement plan revision", cleanupReleaseRejectedReason, key, v)
		}},
		{name: "earlier revision", value: func(r int64) string { return strconv.FormatInt(r-1, 10) }, removed: func(v string) string {
			return fmt.Sprintf("Normal %s Removed %s=%s: placement plan revision ", cleanupReleaseSkippedReason, key, v)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, pass := holdSplitCleanupWait(t)
			recorder := record.NewFakeRecorder(256)
			f.reconciler.Recorder = recorder
			held := waitForCleanup(t, f, pass)
			revision := held.Status.Placement.Plan.Revision
			value := tt.value(revision)
			requestCleanupRelease(t, f, value)
			forbidden := apierrors.NewForbidden(v1beta1.Resource("inferenceservices"), f.source.Name, errors.New("an admission policy refuses the change"))
			failing := true
			failCleanupReleaseRemoval(f, &failing, forbidden)
			changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) { s.Spec.Engine.MinReplicas = ptr.To(12) })
			got := pass()
			if got.Status.Placement.Plan.Revision <= revision {
				t.Fatalf("placement stopped at revision %d while the request %q stayed: %s", revision, value, describeAutoscaledAll(t, f, got))
			}
			if stayed := got.Annotations[key]; stayed != value {
				t.Fatalf("the request is %q after a failed removal, want %q", stayed, value)
			}
			warning := fmt.Sprintf("Warning %s Could not remove %s=%q: %v. The request cannot act on this or any later plan revision, so placement goes on",
				cleanupReleaseNotRemovedReason, key, value, forbidden)
			if events := recordedEvents(recorder); len(events) != 1 || !strings.HasPrefix(events[0], warning) {
				t.Fatalf("events = %q, want one starting %q", events, warning)
			}
			failing = false
			got = pass()
			if stayed, present := got.Annotations[key]; present {
				t.Fatalf("the request %q stayed once removals worked", stayed)
			}
			if events := recordedEvents(recorder); len(events) != 1 || !strings.HasPrefix(events[0], tt.removed(value)) {
				t.Fatalf("events = %q, want one starting %q", events, tt.removed(value))
			}
		})
	}
}

// TestCleanupRelease_LaterRevisionRemovalFails_PlacementWaits holds the Split
// cleanup wait, sets a request for the next plan revision, makes every removal
// of it forbidden, and lowers the engine minimum back to 12. Each pass returns
// the error with a CleanupReleaseNotRemoved warning and the plan keeps its
// revision, so the request can never end the next revision's pause. Once
// removals work, one pass removes the request and the next one plans.
func TestCleanupRelease_LaterRevisionRemovalFails_PlacementWaits(t *testing.T) {
	key := constants.PlacementReleaseCleanupWaitAnnotation
	f, pass := holdSplitCleanupWait(t)
	recorder := record.NewFakeRecorder(256)
	f.reconciler.Recorder = recorder
	held := waitForCleanup(t, f, pass)
	revision := held.Status.Placement.Plan.Revision
	later := strconv.FormatInt(revision+1, 10)
	requestCleanupRelease(t, f, later)
	forbidden := apierrors.NewForbidden(v1beta1.Resource("inferenceservices"), f.source.Name, errors.New("an admission policy refuses the change"))
	failing := true
	failCleanupReleaseRemoval(f, &failing, forbidden)
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) { s.Spec.Engine.MinReplicas = ptr.To(12) })
	request := ctrl.Request{NamespacedName: client.ObjectKeyFromObject(f.source)}
	for range 3 {
		runSplitAboveShares(t, f)
		routeAutoscaledAll(t, f)
		if _, err := f.reconciler.Reconcile(t.Context(), request); !apierrors.IsForbidden(err) {
			t.Fatalf("the pass returned %v, want the forbidden removal", err)
		}
	}
	got := &v1beta1.InferenceService{}
	if err := f.reconciler.Get(t.Context(), request.NamespacedName, got); err != nil {
		t.Fatal(err)
	}
	if got.Status.Placement.Plan.Revision != revision {
		t.Fatalf("the plan reached revision %d while the request for revision %s stayed", got.Status.Placement.Plan.Revision, later)
	}
	warning := fmt.Sprintf("Warning %s Could not remove %s=%s: %v. Placement plan revision %s does not exist yet. Placement waits until the annotation is gone",
		cleanupReleaseNotRemovedReason, key, later, forbidden, later)
	events := recordedEvents(recorder)
	if len(events) != 3 || slices.ContainsFunc(events, func(event string) bool { return !strings.HasPrefix(event, warning) }) {
		t.Fatalf("events = %q, want three starting %q", events, warning)
	}
	failing = false
	if got = pass(); got.Status.Placement.Plan.Revision != revision {
		t.Fatalf("the pass that removed the request also planned: %s", describeAutoscaledAll(t, f, got))
	}
	if stayed, present := got.Annotations[key]; present {
		t.Fatalf("the request %q stayed once removals worked", stayed)
	}
	if events, want := recordedEvents(recorder), otherRevisionRemoved(revision+1, revision); len(events) != 1 || events[0] != want {
		t.Fatalf("events = %q, want %q", events, want)
	}
	if got = pass(); got.Status.Placement.Plan.Revision != revision+1 {
		t.Fatalf("placement did not go on once the request was gone: revision=%d %s", got.Status.Placement.Plan.Revision, describeAutoscaledAll(t, f, got))
	}
	if events := recordedEvents(recorder); len(events) > 0 {
		t.Fatalf("unexpected events once the request was gone: %q", events)
	}
}

// TestRemoveCleanupRelease_Conflict_RetriedOnFreshRead sets a request for an
// earlier revision, and another writer labels the source just before OME
// removes the request, so the first removal meets a conflict. OME reads the
// source again and removes the request: the label survives, the plan does not
// change, and one event is recorded.
func TestRemoveCleanupRelease_Conflict_RetriedOnFreshRead(t *testing.T) {
	f, pass := holdSplitCleanupWait(t)
	recorder := record.NewFakeRecorder(256)
	f.reconciler.Recorder = recorder
	held := waitForCleanup(t, f, pass)
	revision := held.Status.Placement.Plan.Revision
	requestCleanupRelease(t, f, strconv.FormatInt(revision-1, 10))
	var removals []error
	base := f.reconciler.Client.(client.WithWatch)
	f.reconciler.Client = interceptor.NewClient(base, interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if !removesCleanupRelease(f, obj) {
			return c.Update(ctx, obj, opts...)
		}
		if len(removals) == 0 {
			// Another writer lands first, so this removal carries an old version.
			other := &v1beta1.InferenceService{}
			if err := c.Get(ctx, client.ObjectKeyFromObject(obj), other); err != nil {
				return err
			}
			if other.Labels == nil {
				other.Labels = map[string]string{}
			}
			other.Labels["team"] = "team-b"
			if err := c.Update(ctx, other); err != nil {
				return err
			}
		}
		err := c.Update(ctx, obj, opts...)
		removals = append(removals, err)
		return err
	}})
	got := pass()
	if len(removals) != 2 || !apierrors.IsConflict(removals[0]) || removals[1] != nil {
		t.Fatalf("removals = %v, want a conflict and then a success", removals)
	}
	if stayed, present := got.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; present {
		t.Fatalf("the request %q stayed after the retry", stayed)
	}
	if got.Labels["team"] != "team-b" {
		t.Fatalf("the removal dropped the other writer's label: %v", got.Labels)
	}
	if accepted := got.Status.Placement.Plan; !accepted.PauseSurge || accepted.Revision != revision {
		t.Fatalf("removing the request changed the plan: revision=%d %s", accepted.Revision, describeAutoscaledAll(t, f, got))
	}
	if events, want := recordedEvents(recorder), otherRevisionRemoved(revision-1, revision); len(events) != 1 || events[0] != want {
		t.Fatalf("events = %q, want %q", events, want)
	}
}

// TestRemoveCleanupRelease_ValueChanged_KeptForNextPass sets "latest", and the
// operator replaces it with the current revision after the pass reads the
// source but before OME removes the old value. The new value is a new request:
// it stays with no event, and the next pass ends the pause with one
// CleanupWaitReleased event.
func TestRemoveCleanupRelease_ValueChanged_KeptForNextPass(t *testing.T) {
	f, pass := holdSplitCleanupWait(t)
	recorder := record.NewFakeRecorder(256)
	f.reconciler.Recorder = recorder
	held := waitForCleanup(t, f, pass)
	revision := held.Status.Placement.Plan.Revision
	current := strconv.FormatInt(revision, 10)
	requestCleanupRelease(t, f, "latest")
	reader, replaced := f.reconciler.APIReader, false
	f.reconciler.APIReader = interceptor.NewClient(reader.(client.WithWatch), interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
		err := c.Get(ctx, key, obj, opts...)
		if _, source := obj.(*v1beta1.InferenceService); err == nil && source && !replaced && key == client.ObjectKeyFromObject(f.source) {
			// The pass has read "latest"; the operator replaces it before the removal.
			replaced = true
			requestCleanupRelease(t, f, current)
		}
		return err
	}})
	got := pass()
	f.reconciler.APIReader = reader
	if !replaced {
		t.Fatal("the pass never read the source directly")
	}
	if stayed := got.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; stayed != current {
		t.Fatalf("the request is %q, want the replacement %q", stayed, current)
	}
	if accepted := got.Status.Placement.Plan; !accepted.PauseSurge || accepted.Revision != revision {
		t.Fatalf("the pass changed the waiting plan: revision=%d %s", accepted.Revision, describeAutoscaledAll(t, f, got))
	}
	if events := recordedEvents(recorder); len(events) > 0 {
		t.Fatalf("unexpected events for the replaced value: %q", events)
	}
	got = pass()
	if accepted := got.Status.Placement.Plan; accepted.PauseSurge || accepted.Revision != revision+1 {
		t.Fatalf("the replacement did not end the pause: revision=%d %s", accepted.Revision, describeAutoscaledAll(t, f, got))
	}
	if stayed, present := got.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; present {
		t.Fatalf("the answered request %q stayed on the source", stayed)
	}
	if events := recordedEvents(recorder); len(events) != 1 || !strings.HasPrefix(events[0], "Normal "+cleanupWaitReleasedReason+" ") {
		t.Fatalf("events = %q, want one %s event", events, cleanupWaitReleasedReason)
	}
}

// TestPlanDigest_CleanupRelease_InputDigestUnchanged builds the Split, All,
// Single and Single move proposals with and without the request. Each keeps
// its input digest, so setting or removing the request makes no new plan
// revision. A request that is the source's only annotation digests like no
// annotations at all, which is how the API server stores an empty map.
func TestPlanDigest_CleanupRelease_InputDigestUnchanged(t *testing.T) {
	requested := func(s *v1beta1.InferenceService) *v1beta1.InferenceService {
		out := s.DeepCopy()
		if out.Annotations == nil {
			out.Annotations = map[string]string{}
		}
		out.Annotations[constants.PlacementReleaseCleanupWaitAnnotation] = "7"
		return out
	}
	for _, tt := range []struct {
		name   string
		digest func(*testing.T) func(*v1beta1.InferenceService) string
		source func(*testing.T) *v1beta1.InferenceService
	}{
		{name: "Split", source: func(*testing.T) *v1beta1.InferenceService {
			s := srcISVCMode(v1beta1.PlacementModeSplit, "")
			s.Spec.Engine.MinReplicas, s.Spec.Placement.Split = ptr.To(6), &v1beta1.SplitSpec{MaxPerCluster: engineCap(18)}
			s.Annotations = map[string]string{constants.NetworkVisibility: "cluster-local"}
			return s
		}, digest: func(t *testing.T) func(*v1beta1.InferenceService) string {
			return func(s *v1beta1.InferenceService) string {
				proposal, err := desiredSplitPlan(s, twoMembers(), nil)
				if err != nil {
					t.Fatal(err)
				}
				return proposal.InputDigest
			}
		}},
		{name: "Split with no other annotation", source: func(*testing.T) *v1beta1.InferenceService {
			s := srcISVCMode(v1beta1.PlacementModeSplit, "")
			s.Spec.Engine.MinReplicas, s.Annotations = ptr.To(6), nil
			return s
		}, digest: func(t *testing.T) func(*v1beta1.InferenceService) string {
			return func(s *v1beta1.InferenceService) string {
				proposal, err := desiredSplitPlan(s, twoMembers(), nil)
				if err != nil {
					t.Fatal(err)
				}
				return proposal.InputDigest
			}
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			source := tt.source(t)
			digest := tt.digest(t)
			if before, after := digest(source), digest(requested(source)); before != after {
				t.Fatalf("the request changed the input digest from %s to %s", before, after)
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
		if before, after := digest(source), digest(requested(source)); before != after {
			t.Fatalf("the request changed the input digest from %s to %s", before, after)
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
		if before, after := digest(source), digest(requested(source)); before != after {
			t.Fatalf("the request changed the input digest from %s to %s", before, after)
		}
	})
	t.Run("Single move", func(t *testing.T) {
		f := singleFixture(t, false)
		f.reconcile(t)
		seedSingleAdmission(t, f, "member-a", v1beta1.EngineComponent)
		source := f.reconcile(t)
		if source.Status.Placement.Cluster != "member-a" {
			t.Fatalf("member-a was not selected: %+v", source.Status.Placement)
		}
		digest := func(s *v1beta1.InferenceService) string {
			standing := &placementObservations{matches: map[string]bool{"member-b": true}}
			proposal, err := f.reconciler.singleMoveProposal(t.Context(), s, f.clusters, []string{"member-b"}, standing)
			if err != nil {
				t.Fatal(err)
			}
			return proposal.InputDigest
		}
		if before, after := digest(source), digest(requested(source)); before != after {
			t.Fatalf("the request changed the input digest from %s to %s", before, after)
		}
	})
}

// TestDeriveISVC_CleanupRelease_MemberUnchanged derives a member from a source
// with and without the request. The request stays on the source: both members
// are identical, so the request changes no member object, no pod template and
// no revision hash on a member cluster.
func TestDeriveISVC_CleanupRelease_MemberUnchanged(t *testing.T) {
	source := srcISVCMode(v1beta1.PlacementModeSplit, "")
	source.Annotations = map[string]string{constants.NetworkVisibility: "cluster-local"}
	requested := source.DeepCopy()
	requested.Annotations[constants.PlacementReleaseCleanupWaitAnnotation] = "7"
	member := DeriveISVC(requested, "cp-east", "serving-lq")
	if value, present := member.Annotations[constants.PlacementReleaseCleanupWaitAnnotation]; present {
		t.Fatalf("the request %q reached the member", value)
	}
	if diff := cmp.Diff(DeriveISVC(source, "cp-east", "serving-lq"), member); diff != "" {
		t.Fatalf("the request changed the member (-without +with):\n%s", diff)
	}
	if requested.Annotations[constants.PlacementReleaseCleanupWaitAnnotation] != "7" {
		t.Fatal("deriving a member removed the request from the source")
	}
}

// cleanupWaitShapes are autoscaled services whose finished move waits for
// cleanup. Each hold returns the fixture and one pass: the members run, routing
// follows the plan, and placement reconciles.
var cleanupWaitShapes = []struct {
	name string
	hold func(*testing.T) (*backendFixture, func() *v1beta1.InferenceService)
}{
	{name: "Split", hold: holdSplitCleanupWait},
	{name: "All", hold: holdAllCleanupWait},
}

// holdSplitCleanupWait raises the engine minimum of the autoscaled Split service
// from 12 to 16, so each share grows from 6 to 8, and runs the members as
// runSplitAboveShares does. Once the move finishes, the members hold more
// replicas than the plan's 16.
func holdSplitCleanupWait(t *testing.T) (*backendFixture, func() *v1beta1.InferenceService) {
	t.Helper()
	f := autoscaledSplitFixture(t)
	setAutoscaledAll(t, f, 16, 8, "")
	return f, splitPass(t, f, runSplitAboveShares)
}

// runSplitAboveShares runs the Split members while member-a's autoscaler keeps
// 9 engines and 6 decoders, and member-b runs 6 engines whatever its share.
func runSplitAboveShares(t *testing.T, f *backendFixture) {
	t.Helper()
	runAutoscaledSplitMember(t, f, "member-a", autoscaledAllBusy, autoscaledAllBusy)
	member, _ := allMemberOn(t, f, "member-b")
	runAutoscaledSplitMember(t, f, "member-b", autoscaledAllRunning, memberMinimums(member))
}

// runSplitAtMinimums runs each Split member at exactly the minimums in its spec.
func runSplitAtMinimums(t *testing.T, f *backendFixture) {
	t.Helper()
	for _, name := range slices.Sorted(maps.Keys(f.workers)) {
		member, _ := allMemberOn(t, f, name)
		minimums := memberMinimums(member)
		runAutoscaledSplitMember(t, f, name, minimums, minimums)
	}
}

// splitPass returns one pass of the Split service: run runs the members,
// routing follows the plan, and placement reconciles.
func splitPass(t *testing.T, f *backendFixture, run func(*testing.T, *backendFixture)) func() *v1beta1.InferenceService {
	t.Helper()
	return func() *v1beta1.InferenceService {
		run(t, f)
		routeAutoscaledAll(t, f)
		return f.reconcile(t)
	}
}

// sharesSetNotApplied reports a move whose plan gives every member the share
// but whose members have not applied it yet.
func sharesSetNotApplied(got *v1beta1.InferenceService, share int32) bool {
	condition := got.Status.GetCondition(v1beta1.PlacementConverged)
	if condition == nil || condition.Reason != "AwaitingMemberConvergence" {
		return false
	}
	for _, candidate := range got.Status.Placement.Candidates {
		if a := candidate.Allocation; a.CurrentReplicas != share || a.DesiredReplicas != share {
			return false
		}
	}
	return true
}

// holdAllCleanupWait raises the engine minimum of the autoscaled All service
// from 6 to 8 while each member's autoscaler keeps 10 engines. Once the move
// finishes, the members hold more replicas than the plan's 16.
func holdAllCleanupWait(t *testing.T) (*backendFixture, func() *v1beta1.InferenceService) {
	t.Helper()
	f := autoscaledAllFixture(t)
	above := map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 10, v1beta1.DecoderComponent: 4, v1beta1.RouterComponent: 2}
	if got := serveAutoscaledAll(t, f, above, above, autoscaledAllConverged); !autoscaledAllConverged(got) {
		t.Fatalf("the service did not stay converged while its autoscalers added engines: %s", describeAutoscaledAll(t, f, got))
	}
	setAutoscaledAll(t, f, 8, 4, "")
	return f, func() *v1beta1.InferenceService {
		for _, name := range slices.Sorted(maps.Keys(f.workers)) {
			runAutoscaledAllMember(t, f, name, above, above)
		}
		routeAutoscaledAll(t, f)
		return f.reconcile(t)
	}
}

// waitForCleanup runs passes until the finished move waits for cleanup, then
// two more to show that, without a request, the wait holds at the same plan
// revision.
func waitForCleanup(t *testing.T, f *backendFixture, pass func() *v1beta1.InferenceService) *v1beta1.InferenceService {
	t.Helper()
	waiting := func(got *v1beta1.InferenceService) bool {
		condition := got.Status.GetCondition(v1beta1.PlacementConverged)
		return got.Status.Placement.Plan.PauseSurge && condition != nil && condition.Reason == "AwaitingMemberCleanup"
	}
	var got *v1beta1.InferenceService
	for range 12 {
		if got = pass(); waiting(got) {
			break
		}
	}
	if !waiting(got) {
		t.Fatalf("the move did not finish and wait for cleanup: %s", describeAutoscaledAll(t, f, got))
	}
	revision := got.Status.Placement.Plan.Revision
	for range 2 {
		if got = pass(); !waiting(got) || got.Status.Placement.Plan.Revision != revision {
			t.Fatalf("the cleanup wait at revision %d did not hold: revision=%d %s", revision, got.Status.Placement.Plan.Revision, describeAutoscaledAll(t, f, got))
		}
	}
	return got
}

// requestCleanupRelease sets the cleanup release request on the stored source,
// as kubectl annotate does.
func requestCleanupRelease(t *testing.T, f *backendFixture, value string) {
	t.Helper()
	changeAutoscaledSplit(t, f, func(s *v1beta1.InferenceService) {
		if s.Annotations == nil {
			s.Annotations = map[string]string{}
		}
		s.Annotations[constants.PlacementReleaseCleanupWaitAnnotation] = value
	})
}

// failCleanupReleaseRemoval makes every update that removes the cleanup release
// request from the source fail with err while *failing is true.
func failCleanupReleaseRemoval(f *backendFixture, failing *bool, err error) {
	base := f.reconciler.Client.(client.WithWatch)
	f.reconciler.Client = interceptor.NewClient(base, interceptor.Funcs{Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
		if *failing && removesCleanupRelease(f, obj) {
			return err
		}
		return c.Update(ctx, obj, opts...)
	}})
}

// removesCleanupRelease reports an update of the source that carries no cleanup
// release request. The tests that use it set the request before any such update.
func removesCleanupRelease(f *backendFixture, obj client.Object) bool {
	_, source := obj.(*v1beta1.InferenceService)
	_, present := obj.GetAnnotations()[constants.PlacementReleaseCleanupWaitAnnotation]
	return source && !present && client.ObjectKeyFromObject(obj) == client.ObjectKeyFromObject(f.source)
}

// memberVersions records the resource version of every member service and
// InferenceReplica on the member clusters.
func memberVersions(t *testing.T, f *backendFixture) map[string]string {
	t.Helper()
	out := map[string]string{}
	for name, worker := range f.workers {
		members := &v1beta1.InferenceServiceList{}
		replicas := &v1beta1.InferenceReplicaList{}
		for _, list := range []client.ObjectList{members, replicas} {
			if err := worker.List(t.Context(), list, client.InNamespace(f.source.Namespace)); err != nil {
				t.Fatal(err)
			}
		}
		for _, member := range members.Items {
			out[name+"/isvc/"+member.Name] = member.ResourceVersion
		}
		for _, replica := range replicas.Items {
			out[name+"/ir/"+replica.Name] = replica.ResourceVersion
		}
	}
	return out
}

// recordedEvents drains the events the recorder holds.
func recordedEvents(recorder *record.FakeRecorder) []string {
	var out []string
	for {
		select {
		case event := <-recorder.Events:
			out = append(out, event)
		default:
			return out
		}
	}
}
