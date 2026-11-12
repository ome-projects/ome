package routing

import (
	"fmt"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// recordLocked folds one poll's outcome into a cluster's capacity state. A
// report without a timestamp is taken as observed now; one already past the
// maximum age is stale and counts as a failed poll; a fresh one drops the
// samples past their age, joins the window and is applied once the window
// holds quorum, which clears any reason the poll gave. A failed poll in
// Reported mode keeps the estimate of the fresh samples left; in Ceiling
// mode it releases the ceiling and the window. The poll's reason stands
// where it gave one, else the state says why nothing is applied. The
// answer is whether the applied value or the reason moved. One row per
// rule: the state before, the polls, and the state after.

// capacityRecordCall is one poll's outcome as the poller records it.
type capacityRecordCall struct {
	reported   *float64
	observedAt time.Time
	reason     string
	now        time.Time
}

// capacityRecordState is what the row reads back: the last call's answer,
// the applied value, the reason, and the samples kept, oldest first.
type capacityRecordState struct {
	changed  bool
	reported *float64
	reason   string
	window   []float64
	expiries []time.Time
}

type capacityRecordRow struct {
	name string
	mode v1beta1.RoutingCapacityMode
	// before is the state the calls start from.
	before capacityRecordState
	calls  []capacityRecordCall
	want   capacityRecordState
}

var (
	recordNow     = capacityTestNow
	recordFresh   = recordNow.Add(20 * time.Second)
	recordExpired = recordNow.Add(-time.Second)
	recordMaxAge  = testCapacityConfig().MaxAge
)

func reportOf(v float64) *float64 { return &v }

// awaitingQuorum is the reason of a window below quorum.
func awaitingQuorum(accepted int) string {
	return fmt.Sprintf("capacity report awaiting quorum (%d/%d accepted samples)", accepted, testCapacityConfig().Quorum)
}

// stateOf reads the state back as the row compares it.
func stateOf(changed bool, st *capacityState) capacityRecordState {
	out := capacityRecordState{changed: changed, reported: st.reported, reason: st.reason}
	for _, sample := range st.window {
		out.window = append(out.window, sample.servable)
		out.expiries = append(out.expiries, sample.expires)
	}
	return out
}

// stateFrom builds the poller's state from a row's picture.
func stateFrom(before capacityRecordState) *capacityState {
	st := &capacityState{reported: before.reported, reason: before.reason}
	for i, v := range before.window {
		st.window = append(st.window, capacitySample{servable: v, expires: before.expiries[i]})
	}
	return st
}

// poll is a report observed at observedAt, recorded now.
func poll(v float64, observedAt time.Time) capacityRecordCall {
	return capacityRecordCall{reported: reportOf(v), observedAt: observedAt, now: recordNow}
}

// failedPoll is a poll that brought no report, with the reason it gave.
func failedPoll(reason string) capacityRecordCall {
	return capacityRecordCall{reason: reason, now: recordNow}
}

var capacityRecordRows = []capacityRecordRow{
	{
		name:  "the first report in Ceiling mode waits for quorum",
		mode:  v1beta1.RoutingCapacityModeCeiling,
		calls: []capacityRecordCall{poll(5, recordNow)},
		want:  capacityRecordState{changed: true, reason: awaitingQuorum(1), window: []float64{5}, expiries: []time.Time{recordNow.Add(recordMaxAge)}},
	},
	{
		name:   "a second report reaching quorum is applied as the quorum-th smallest",
		mode:   v1beta1.RoutingCapacityModeCeiling,
		before: capacityRecordState{reason: awaitingQuorum(1), window: []float64{5}, expiries: []time.Time{recordFresh}},
		calls:  []capacityRecordCall{poll(7, recordNow)},
		want:   capacityRecordState{changed: true, reported: reportOf(7), window: []float64{5, 7}, expiries: []time.Time{recordFresh, recordNow.Add(recordMaxAge)}},
	},
	{
		name:   "a report that moves neither the value nor the reason is no change",
		mode:   v1beta1.RoutingCapacityModeCeiling,
		before: capacityRecordState{reported: reportOf(5), window: []float64{5, 5}, expiries: []time.Time{recordFresh, recordFresh}},
		calls:  []capacityRecordCall{poll(5, recordNow)},
		want:   capacityRecordState{reported: reportOf(5), window: []float64{5, 5, 5}, expiries: []time.Time{recordFresh, recordFresh, recordNow.Add(recordMaxAge)}},
	},
	{
		name:   "the applied value moving alone is a change",
		mode:   v1beta1.RoutingCapacityModeCeiling,
		before: capacityRecordState{reported: reportOf(7), window: []float64{5, 7}, expiries: []time.Time{recordFresh, recordFresh}},
		calls:  []capacityRecordCall{poll(6, recordNow)},
		want:   capacityRecordState{changed: true, reported: reportOf(6), window: []float64{5, 7, 6}, expiries: []time.Time{recordFresh, recordFresh, recordNow.Add(recordMaxAge)}},
	},
	{
		name:  "a report without a timestamp is taken as observed now",
		mode:  v1beta1.RoutingCapacityModeCeiling,
		calls: []capacityRecordCall{{reported: reportOf(5), now: recordNow}},
		want:  capacityRecordState{changed: true, reason: awaitingQuorum(1), window: []float64{5}, expiries: []time.Time{recordNow.Add(recordMaxAge)}},
	},
	{
		name:   "a report already older than the maximum age is stale and counts as a failed poll",
		mode:   v1beta1.RoutingCapacityModeCeiling,
		before: capacityRecordState{reason: awaitingQuorum(1), window: []float64{5}, expiries: []time.Time{recordFresh}},
		calls:  []capacityRecordCall{poll(6, recordNow.Add(-recordMaxAge-time.Second))},
		want:   capacityRecordState{changed: true, reason: capacityStaleReason(recordMaxAge)},
	},
	{
		name:   "a fresh report drops the samples past their age before it is counted",
		mode:   v1beta1.RoutingCapacityModeCeiling,
		before: capacityRecordState{reason: awaitingQuorum(1), window: []float64{4}, expiries: []time.Time{recordExpired}},
		calls:  []capacityRecordCall{poll(6, recordNow)},
		want:   capacityRecordState{reason: awaitingQuorum(1), window: []float64{6}, expiries: []time.Time{recordNow.Add(recordMaxAge)}},
	},
	{
		name:  "a report below quorum keeps the reason the poll gave",
		mode:  v1beta1.RoutingCapacityModeCeiling,
		calls: []capacityRecordCall{{reported: reportOf(5), observedAt: recordNow, reason: "report decoded with a warning", now: recordNow}},
		want:  capacityRecordState{changed: true, reason: "report decoded with a warning", window: []float64{5}, expiries: []time.Time{recordNow.Add(recordMaxAge)}},
	},
	{
		name:   "a report reaching quorum clears the reason the poll gave",
		mode:   v1beta1.RoutingCapacityModeCeiling,
		before: capacityRecordState{reason: awaitingQuorum(1), window: []float64{5}, expiries: []time.Time{recordFresh}},
		calls:  []capacityRecordCall{{reported: reportOf(7), observedAt: recordNow, reason: "report decoded with a warning", now: recordNow}},
		want:   capacityRecordState{changed: true, reported: reportOf(7), window: []float64{5, 7}, expiries: []time.Time{recordFresh, recordNow.Add(recordMaxAge)}},
	},
	{
		name:   "a failed poll in Reported mode keeps the fresh samples' lower median",
		mode:   v1beta1.RoutingCapacityModeReported,
		before: capacityRecordState{reported: reportOf(4), window: []float64{4, 6}, expiries: []time.Time{recordFresh, recordFresh}},
		calls:  []capacityRecordCall{failedPoll("capacity request timed out")},
		want:   capacityRecordState{changed: true, reported: reportOf(4), reason: "capacity request timed out", window: []float64{4, 6}, expiries: []time.Time{recordFresh, recordFresh}},
	},
	{
		name:   "a failed poll in Reported mode without a reason says the report is unavailable",
		mode:   v1beta1.RoutingCapacityModeReported,
		before: capacityRecordState{reported: reportOf(4), window: []float64{4, 6}, expiries: []time.Time{recordFresh, recordFresh}},
		calls:  []capacityRecordCall{failedPoll("")},
		want:   capacityRecordState{changed: true, reported: reportOf(4), reason: "capacity report unavailable", window: []float64{4, 6}, expiries: []time.Time{recordFresh, recordFresh}},
	},
	{
		name:   "a failed poll in Reported mode drops the samples past their age first",
		mode:   v1beta1.RoutingCapacityModeReported,
		before: capacityRecordState{reported: reportOf(4), window: []float64{4, 6}, expiries: []time.Time{recordExpired, recordFresh}},
		calls:  []capacityRecordCall{failedPoll("capacity request timed out")},
		want:   capacityRecordState{changed: true, reason: "capacity request timed out", window: []float64{6}, expiries: []time.Time{recordFresh}},
	},
	{
		name:   "a failed poll in Ceiling mode releases the ceiling and the window",
		mode:   v1beta1.RoutingCapacityModeCeiling,
		before: capacityRecordState{reported: reportOf(7), window: []float64{5, 7}, expiries: []time.Time{recordFresh, recordFresh}},
		calls:  []capacityRecordCall{failedPoll("connection refused")},
		want:   capacityRecordState{changed: true, reason: "connection refused"},
	},
	{
		name:   "a failed poll in Ceiling mode without a reason says the report is unavailable",
		mode:   v1beta1.RoutingCapacityModeCeiling,
		before: capacityRecordState{reported: reportOf(7), window: []float64{5, 7}, expiries: []time.Time{recordFresh, recordFresh}},
		calls:  []capacityRecordCall{failedPoll("")},
		want:   capacityRecordState{changed: true, reason: "capacity report unavailable"},
	},
	{
		name:   "a repeated failed poll is no change",
		mode:   v1beta1.RoutingCapacityModeCeiling,
		before: capacityRecordState{reason: "capacity report unavailable"},
		calls:  []capacityRecordCall{failedPoll("")},
		want:   capacityRecordState{reason: "capacity report unavailable"},
	},
	{
		name:   "a value moving under the same reason is a change in Reported mode",
		mode:   v1beta1.RoutingCapacityModeReported,
		before: capacityRecordState{reported: reportOf(4), reason: "capacity request timed out", window: []float64{4, 6}, expiries: []time.Time{recordFresh, recordExpired}},
		calls:  []capacityRecordCall{failedPoll("capacity request timed out")},
		want:   capacityRecordState{changed: true, reason: "capacity request timed out", window: []float64{4}, expiries: []time.Time{recordFresh}},
	},
	{
		name: "a sample expires by the time it was observed, not by its arrival",
		mode: v1beta1.RoutingCapacityModeReported,
		calls: []capacityRecordCall{
			poll(5, recordNow.Add(-20*time.Second)),
			{reason: "capacity request timed out", now: recordNow.Add(15 * time.Second)},
		},
		want: capacityRecordState{changed: true, reason: "capacity request timed out"},
	},
}

func TestCapacityRecord(t *testing.T) {
	for _, row := range capacityRecordRows {
		t.Run(row.name, func(t *testing.T) {
			config := testCapacityConfig()
			config.Mode = row.mode
			st := stateFrom(row.before)
			var changed bool
			for _, call := range row.calls {
				changed = (&CapacityPoller{}).recordLocked(st, config, call.reported, call.observedAt, call.reason, call.now)
			}
			if diff := cmp.Diff(row.want, stateOf(changed, st), cmp.AllowUnexported(capacityRecordState{})); diff != "" {
				t.Errorf("state after the polls (-want +got):\n%s", diff)
			}
		})
	}
}
