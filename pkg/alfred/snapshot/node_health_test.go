package snapshot

import (
	"reflect"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestObserveNodeHealth(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC)
	window := 30 * time.Minute
	recent := now.Add(-10 * time.Minute)
	older := now.Add(-20 * time.Minute)
	expired := now.Add(-window)
	longExpired := expired.Add(-time.Nanosecond)
	future := now.Add(5 * time.Minute)
	zero := time.Time{}

	condition := func(conditionType string, status corev1.ConditionStatus, transitioned time.Time) corev1.NodeCondition {
		return corev1.NodeCondition{
			Type:               corev1.NodeConditionType(conditionType),
			Status:             status,
			LastTransitionTime: metav1.NewTime(transitioned),
		}
	}
	observation := func(conditionType string, status corev1.ConditionStatus, transitioned time.Time) NodeConditionObservation {
		return NodeConditionObservation{
			Type:               corev1.NodeConditionType(conditionType),
			Status:             status,
			LastTransitionTime: transitioned,
		}
	}
	timePtr := func(value time.Time) *time.Time { return &value }

	tests := []struct {
		name       string
		conditions []corev1.NodeCondition
		triggers   []string
		wantState  NodeHealthState
		wantUntil  *time.Time
		wantRows   []NodeConditionObservation
	}{
		{
			name: "no configured condition is clear",
			conditions: []corev1.NodeCondition{
				condition("Unrelated", corev1.ConditionTrue, recent),
			},
			triggers:  []string{"GpuUnhealthy"},
			wantState: NodeHealthClear,
		},
		{
			name: "true is unhealthy",
			conditions: []corev1.NodeCondition{
				condition("GpuUnhealthy", corev1.ConditionTrue, recent),
			},
			triggers:  []string{"GpuUnhealthy"},
			wantState: NodeHealthUnhealthy,
			wantRows: []NodeConditionObservation{
				observation("GpuUnhealthy", corev1.ConditionTrue, recent),
			},
		},
		{
			name: "unknown is quarantined without becoming unhealthy",
			conditions: []corev1.NodeCondition{
				condition("GpuUnhealthy", corev1.ConditionUnknown, recent),
			},
			triggers:  []string{"GpuUnhealthy"},
			wantState: NodeHealthUnknown,
			wantRows: []NodeConditionObservation{
				observation("GpuUnhealthy", corev1.ConditionUnknown, recent),
			},
		},
		{
			name: "unrecognized status fails closed as unknown",
			conditions: []corev1.NodeCondition{
				condition("GpuUnhealthy", corev1.ConditionStatus("Invalid"), recent),
			},
			triggers:  []string{"GpuUnhealthy"},
			wantState: NodeHealthUnknown,
			wantRows: []NodeConditionObservation{
				observation("GpuUnhealthy", corev1.ConditionStatus("Invalid"), recent),
			},
		},
		{
			name: "recent false is suspect",
			conditions: []corev1.NodeCondition{
				condition("GpuUnhealthy", corev1.ConditionFalse, recent),
			},
			triggers:  []string{"GpuUnhealthy"},
			wantState: NodeHealthSuspect,
			wantUntil: timePtr(recent.Add(window)),
			wantRows: []NodeConditionObservation{
				observation("GpuUnhealthy", corev1.ConditionFalse, recent),
			},
		},
		{
			name: "exact expiry is clear",
			conditions: []corev1.NodeCondition{
				condition("GpuUnhealthy", corev1.ConditionFalse, expired),
			},
			triggers:  []string{"GpuUnhealthy"},
			wantState: NodeHealthClear,
			wantRows: []NodeConditionObservation{
				observation("GpuUnhealthy", corev1.ConditionFalse, expired),
			},
		},
		{
			name: "older false is clear",
			conditions: []corev1.NodeCondition{
				condition("GpuUnhealthy", corev1.ConditionFalse, longExpired),
			},
			triggers:  []string{"GpuUnhealthy"},
			wantState: NodeHealthClear,
			wantRows: []NodeConditionObservation{
				observation("GpuUnhealthy", corev1.ConditionFalse, longExpired),
			},
		},
		{
			name: "zero transition fails closed as unknown",
			conditions: []corev1.NodeCondition{
				condition("GpuUnhealthy", corev1.ConditionFalse, zero),
			},
			triggers:  []string{"GpuUnhealthy"},
			wantState: NodeHealthUnknown,
			wantRows: []NodeConditionObservation{
				observation("GpuUnhealthy", corev1.ConditionFalse, zero),
			},
		},
		{
			name: "future false remains quarantined",
			conditions: []corev1.NodeCondition{
				condition("GpuUnhealthy", corev1.ConditionFalse, future),
			},
			triggers:  []string{"GpuUnhealthy"},
			wantState: NodeHealthSuspect,
			wantUntil: timePtr(future.Add(window)),
			wantRows: []NodeConditionObservation{
				observation("GpuUnhealthy", corev1.ConditionFalse, future),
			},
		},
		{
			name: "true wins and observations are sorted",
			conditions: []corev1.NodeCondition{
				condition("ZFailure", corev1.ConditionFalse, recent),
				condition("BFailure", corev1.ConditionTrue, older),
				condition("AFailure", corev1.ConditionUnknown, recent),
			},
			triggers:  []string{"ZFailure", "BFailure", "AFailure"},
			wantState: NodeHealthUnhealthy,
			wantRows: []NodeConditionObservation{
				observation("AFailure", corev1.ConditionUnknown, recent),
				observation("BFailure", corev1.ConditionTrue, older),
				observation("ZFailure", corev1.ConditionFalse, recent),
			},
		},
		{
			name: "unknown wins over recent false",
			conditions: []corev1.NodeCondition{
				condition("GpuUnhealthy", corev1.ConditionFalse, recent),
				condition("GpuReady", corev1.ConditionUnknown, older),
			},
			triggers:  []string{"GpuUnhealthy", "GpuReady"},
			wantState: NodeHealthUnknown,
			wantRows: []NodeConditionObservation{
				observation("GpuReady", corev1.ConditionUnknown, older),
				observation("GpuUnhealthy", corev1.ConditionFalse, recent),
			},
		},
		{
			name: "latest false deadline wins",
			conditions: []corev1.NodeCondition{
				condition("ZFailure", corev1.ConditionFalse, recent),
				condition("AFailure", corev1.ConditionFalse, older),
			},
			triggers:  []string{"ZFailure", "AFailure"},
			wantState: NodeHealthSuspect,
			wantUntil: timePtr(recent.Add(window)),
			wantRows: []NodeConditionObservation{
				observation("AFailure", corev1.ConditionFalse, older),
				observation("ZFailure", corev1.ConditionFalse, recent),
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			got := observeNodeHealth(test.conditions, test.triggers, now, window)
			if got.State != test.wantState {
				t.Errorf("State = %q, want %q", got.State, test.wantState)
			}
			if !reflect.DeepEqual(got.Conditions, test.wantRows) {
				t.Errorf("Conditions = %+v, want %+v", got.Conditions, test.wantRows)
			}
			if !reflect.DeepEqual(got.SuspectUntil, test.wantUntil) {
				t.Errorf("SuspectUntil = %v, want %v", got.SuspectUntil, test.wantUntil)
			}
		})
	}
}

func TestNodeHealthObservationQuarantined(t *testing.T) {
	t.Parallel()

	tests := []struct {
		state NodeHealthState
		want  bool
	}{
		{state: "", want: false},
		{state: NodeHealthClear, want: false},
		{state: NodeHealthSuspect, want: true},
		{state: NodeHealthUnknown, want: true},
		{state: NodeHealthUnhealthy, want: true},
		{state: NodeHealthState("FutureState"), want: true},
	}

	for _, test := range tests {
		if got := (NodeHealthObservation{State: test.state}).Quarantined(); got != test.want {
			t.Errorf("state %q Quarantined() = %t, want %t", test.state, got, test.want)
		}
	}
}
