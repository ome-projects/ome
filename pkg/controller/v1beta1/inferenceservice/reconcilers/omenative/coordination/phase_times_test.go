package coordination

import (
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"knative.dev/pkg/apis"
	duckv1 "knative.dev/pkg/apis/duck/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

var (
	phaseTimesEarlier = metav1.NewTime(time.Date(2024, 1, 1, 0, 1, 0, 0, time.UTC))
	phaseTimesLater   = metav1.NewTime(time.Date(2024, 1, 1, 0, 2, 0, 0, time.UTC))
	phaseTimesReady   = apis.ConditionType(v1beta1.RolloutCoordinationReady)
)

// phaseTimesCase is one pair of statuses: the live one and the one a pass
// carries to its flush, with the instants the flush must leave on the pass's.
type phaseTimesCase struct {
	name         string
	live, pass   *v1beta1.InferenceServiceStatus
	wantAt       metav1.Time
	wantFinished *metav1.Time
	wantReadyAt  *metav1.Time
}

// The status flush keeps, for each group entry the live object already
// holds, the instants a pass that read a trailing copy would move: the
// transition time of the same phase, the earlier of two finish records for
// the same member, and the readiness condition's time while the condition is
// the same apart from it. Everything else the pass computed stands.
func TestKeepPhaseTimes(t *testing.T) {
	for _, tc := range phaseTimesCases() {
		t.Run(tc.name, func(t *testing.T) {
			SnapshotLiveGroupEntries(tc.live).KeepPhaseTimes(tc.pass)
			got := tc.pass.RolloutCoordination.Groups[0]
			if !got.LastTransitionTime.Time.Equal(tc.wantAt.Time) {
				t.Fatalf("transition time = %v, want %v", got.LastTransitionTime.Time, tc.wantAt.Time)
			}
			if tc.wantFinished != nil && !got.MemberFinished.At.Time.Equal(tc.wantFinished.Time) {
				t.Fatalf("finish record at %v, want %v", got.MemberFinished.At.Time, tc.wantFinished.Time)
			}
			if tc.wantReadyAt != nil {
				cond := tc.pass.GetCondition(phaseTimesReady)
				if cond == nil || !cond.LastTransitionTime.Inner.Time.Equal(tc.wantReadyAt.Time) {
					t.Fatalf("readiness condition = %+v, want its time at %v", cond, tc.wantReadyAt.Time)
				}
			}
		})
	}
	t.Run("a nil status and a status with no entries are left alone", func(t *testing.T) {
		SnapshotLiveGroupEntries(nil).KeepPhaseTimes(nil)
		empty := &v1beta1.InferenceServiceStatus{}
		live := phaseTimesStatus(phaseTimesGroup(v1beta1.CoordinationPhaseIdle, phaseTimesEarlier, nil), phaseTimesCondition(corev1.ConditionTrue, "Idle", phaseTimesEarlier))
		SnapshotLiveGroupEntries(live).KeepPhaseTimes(empty)
		if empty.RolloutCoordination != nil || len(empty.Conditions) != 0 {
			t.Fatalf("a status with no entries gained some: %+v", empty)
		}
	})
}

func phaseTimesCases() []phaseTimesCase {
	idle, surging := v1beta1.CoordinationPhaseIdle, v1beta1.CoordinationPhaseSurging
	earlier, later := phaseTimesEarlier, phaseTimesLater
	return []phaseTimesCase{
		{
			name:   "the same phase keeps the live transition time",
			live:   phaseTimesStatus(phaseTimesGroup(idle, earlier, nil), nil),
			pass:   phaseTimesStatus(phaseTimesGroup(idle, later, nil), nil),
			wantAt: earlier,
		},
		{
			name:   "another phase takes the pass's time",
			live:   phaseTimesStatus(phaseTimesGroup(idle, earlier, nil), nil),
			pass:   phaseTimesStatus(phaseTimesGroup(surging, later, nil), nil),
			wantAt: later,
		},
		{
			name:   "a group the live object does not know takes the pass's time",
			live:   &v1beta1.InferenceServiceStatus{},
			pass:   phaseTimesStatus(phaseTimesGroup(idle, later, nil), nil),
			wantAt: later,
		},
		{
			name:         "a finish record for the same member keeps the earlier instant",
			live:         phaseTimesStatus(phaseTimesGroup(idle, earlier, &earlier), nil),
			pass:         phaseTimesStatus(phaseTimesGroup(idle, later, &later), nil),
			wantAt:       earlier,
			wantFinished: &earlier,
		},
		{
			name:         "a finish record earlier than the live one stands",
			live:         phaseTimesStatus(phaseTimesGroup(surging, later, &later), nil),
			pass:         phaseTimesStatus(phaseTimesGroup(idle, later, &earlier), nil),
			wantAt:       later,
			wantFinished: &earlier,
		},
		{
			name:        "the same readiness condition keeps the live time",
			live:        phaseTimesStatus(phaseTimesGroup(idle, earlier, nil), phaseTimesCondition(corev1.ConditionTrue, "Idle", earlier)),
			pass:        phaseTimesStatus(phaseTimesGroup(idle, later, nil), phaseTimesCondition(corev1.ConditionTrue, "Idle", later)),
			wantAt:      earlier,
			wantReadyAt: &earlier,
		},
		{
			name:        "a readiness condition with another reason takes the pass's time",
			live:        phaseTimesStatus(phaseTimesGroup(idle, earlier, nil), phaseTimesCondition(corev1.ConditionTrue, "Idle", earlier)),
			pass:        phaseTimesStatus(phaseTimesGroup(surging, later, nil), phaseTimesCondition(corev1.ConditionFalse, "InProgress", later)),
			wantAt:      later,
			wantReadyAt: &later,
		},
	}
}

// phaseTimesGroup is one group entry at a phase and a transition time, with
// a finish record of the decoder when finishedAt is given.
func phaseTimesGroup(phase v1beta1.CoordinationPhase, at metav1.Time, finishedAt *metav1.Time) v1beta1.RolloutCoordinationGroupStatus {
	g := v1beta1.RolloutCoordinationGroupStatus{Name: "0", Phase: phase, CompositePhase: string(phase), LastTransitionTime: at.DeepCopy()}
	if finishedAt != nil {
		g.MemberFinished = &v1beta1.RolloutCoordinationMemberFinish{Component: v1beta1.DecoderComponent, At: *finishedAt}
	}
	return g
}

// phaseTimesStatus is a status carrying one group entry and, when given, the
// readiness condition.
func phaseTimesStatus(group v1beta1.RolloutCoordinationGroupStatus, ready *apis.Condition) *v1beta1.InferenceServiceStatus {
	out := &v1beta1.InferenceServiceStatus{RolloutCoordination: &v1beta1.RolloutCoordinationStatus{Groups: []v1beta1.RolloutCoordinationGroupStatus{group}}}
	if ready != nil {
		out.Conditions = duckv1.Conditions{*ready}
	}
	return out
}

func phaseTimesCondition(status corev1.ConditionStatus, reason string, at metav1.Time) *apis.Condition {
	return &apis.Condition{Type: phaseTimesReady, Status: status, Reason: reason, LastTransitionTime: apis.VolatileTime{Inner: at}}
}
