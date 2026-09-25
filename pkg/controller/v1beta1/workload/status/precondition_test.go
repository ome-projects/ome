package status

import (
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestTerminalInstanceIdentity_IgnoresWireTimeAndDerivedStatus(t *testing.T) {
	expected := terminalStatusFixture(3)
	current := cloneTerminalStatus(expected)
	current.Operation.StartedAt = metav1.NewTime(current.Operation.StartedAt.Truncate(time.Second))
	current.Operation.LastProgressAt = metav1.NewTime(current.Operation.LastProgressAt.Truncate(time.Second))
	current.Operation.Deadline = metav1.NewTime(current.Operation.Deadline.Truncate(time.Second))
	current.PodCount = 3
	current.ReadyPodCount = 2
	current.ServingPodCount = 1
	current.AvailablePodCount = 1
	current.ScheduledPodCount = 3
	current.NodesOccupied = []string{"node-b"}
	current.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse}}

	if !Capture(&expected).Matches(current) {
		t.Fatal("wire-normalized timestamps and derived observations changed terminal ownership")
	}
}

func TestTerminalInstanceIdentity_RejectsLifecycleDrift(t *testing.T) {
	expected := terminalStatusFixture(3)
	identity := Capture(&expected)
	tests := []struct {
		name   string
		mutate func(*types.InstanceStatus)
	}{
		{name: "index", mutate: func(s *types.InstanceStatus) { s.Index++ }},
		{name: "incarnation", mutate: func(s *types.InstanceStatus) { s.Incarnation++ }},
		{name: "phase", mutate: func(s *types.InstanceStatus) { s.Phase = types.InstancePhaseFailed }},
		{name: "running revision", mutate: func(s *types.InstanceStatus) { s.RunningRevision = "other" }},
		{name: "target revision", mutate: func(s *types.InstanceStatus) { s.TargetRevision = "other" }},
		{name: "active ordinal", mutate: func(s *types.InstanceStatus) { s.ActiveOrdinal = 0 }},
		{name: "operation missing", mutate: func(s *types.InstanceStatus) { s.Operation = nil }},
		{name: "operation ID", mutate: func(s *types.InstanceStatus) { s.Operation.ID = "other" }},
		{name: "operation type", mutate: func(s *types.InstanceStatus) { s.Operation.Type = types.InstanceOperationMigrate }},
		{name: "operation step", mutate: func(s *types.InstanceStatus) { s.Operation.Step = types.UpdateStepSurgeDrain }},
		{name: "request UUID", mutate: func(s *types.InstanceStatus) { s.Operation.RequestUUID = "other" }},
		{name: "operation target", mutate: func(s *types.InstanceStatus) { s.Operation.TargetRevision = "other" }},
		{name: "retry count", mutate: func(s *types.InstanceStatus) { s.Operation.RetryCount++ }},
		{name: "operation reason", mutate: func(s *types.InstanceStatus) { s.Operation.Reason = "other" }},
		{name: "source node", mutate: func(s *types.InstanceStatus) { s.Operation.FromNode = "other" }},
		{name: "hint target nodes", mutate: func(s *types.InstanceStatus) { s.Operation.HintTargetNodes[0] = "other" }},
		{name: "surge index", mutate: func(s *types.InstanceStatus) { *s.Operation.SurgeIndex = *s.Operation.SurgeIndex + 1 }},
		{name: "surge index nil", mutate: func(s *types.InstanceStatus) { s.Operation.SurgeIndex = nil }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			current := cloneTerminalStatus(expected)
			test.mutate(&current)
			if identity.Matches(current) {
				t.Fatal("lifecycle drift retained terminal ownership")
			}
		})
	}
}
