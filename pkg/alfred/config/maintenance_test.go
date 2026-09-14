package config

import (
	"strings"
	"testing"
)

func TestMaintenanceConfigValidation(t *testing.T) {
	tests := []struct {
		name, triggers string
		valid          bool
	}{
		{"disabled", "[]", true},
		{"condition", `[{name: patch, condition: {type: Patching, status: "True"}}]`, true},
		{"condition false", `[{name: patch, condition: {type: Patching, status: "False"}}]`, true},
		{"condition unknown", `[{name: patch, condition: {type: Patching, status: Unknown}}]`, true},
		{"presence label", `[{name: patch, label: {key: ops.example/patch}}]`, true},
		{"empty label value", `[{name: patch, label: {key: ops.example/patch, value: ""}}]`, true},
		{"taint", `[{name: patch, taint: {key: ops.example/patch, value: planned, effect: NoSchedule}}]`, true},
		{"taint presence", `[{name: patch, taint: {key: ops.example/patch}}]`, true},
		{"empty trigger", `[{}]`, false},
		{"missing condition", `[{name: patch}]`, false},
		{"missing name", `[{label: {key: patch}}]`, false},
		{"blank name", `[{name: "  ", label: {key: patch}}]`, false},
		{"duplicate name", `[{name: patch, label: {key: patch}}, {name: patch, taint: {key: patch}}]`, false},
		{"multiple matchers", `[{name: patch, label: {key: patch}, taint: {key: patch}}]`, false},
		{"empty condition", `[{name: patch, condition: {}}]`, false},
		{"bad condition", `[{name: patch, condition: {type: "bad name", status: "True"}}]`, false},
		{"missing status", `[{name: patch, condition: {type: Patching}}]`, false},
		{"bad status", `[{name: patch, condition: {type: Patching, status: yes}}]`, false},
		{"empty label", `[{name: patch, label: {}}]`, false},
		{"bad label key", `[{name: patch, label: {key: "bad key"}}]`, false},
		{"bad label value", `[{name: patch, label: {key: patch, value: "bad value"}}]`, false},
		{"empty taint", `[{name: patch, taint: {}}]`, false},
		{"bad taint key", `[{name: patch, taint: {key: "bad key"}}]`, false},
		{"bad taint value", `[{name: patch, taint: {key: patch, value: "bad value"}}]`, false},
		{"bad effect", `[{name: patch, taint: {key: patch, effect: Bad}}]`, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load([]byte("schemaVersion: 1\npolicies:\n  nodeHealth:\n    maintenance:\n      triggers: " + tt.triggers + "\n"))
			if (err == nil) != tt.valid {
				t.Fatalf("valid=%t, err=%v", tt.valid, err)
			}
		})
	}
}

func TestFailureTriggersRejectEmptyAndDuplicateNames(t *testing.T) {
	for _, triggers := range []string{`[""]`, `["  "]`, `[GpuUnhealthy, GpuUnhealthy]`} {
		_, err := Load([]byte("schemaVersion: 1\npolicies:\n  nodeHealth:\n    triggerConditions: " + triggers))
		if err == nil || !strings.Contains(err.Error(), "triggerConditions") {
			t.Fatalf("triggers %s: %v", triggers, err)
		}
	}
}

func TestMaintenanceReloadPreservesOptionalValuesAndLastGoodConfig(t *testing.T) {
	store := NewStore()
	if len(store.Get().Policies.NodeHealth.Maintenance.Triggers) != 0 {
		t.Fatal("maintenance must be disabled without configured triggers")
	}
	raw := []byte(`schemaVersion: 1
policies:
  nodeHealth:
    maintenance:
      triggers:
      - name: label-presence
        label: {key: patch}
      - name: label-empty
        label: {key: patch, value: ""}
      - name: taint-presence
        taint: {key: patch}
      - name: taint-empty
        taint: {key: patch, value: ""}
`)
	if outcome, err := store.Update(raw); err != nil || outcome != OutcomeSuccess {
		t.Fatalf("reload=%s, %v", outcome, err)
	}
	loaded := store.Get()
	rules := loaded.Policies.NodeHealth.Maintenance.Triggers
	if len(rules) != 4 || rules[0].Label.Value != nil || rules[1].Label.Value == nil || *rules[1].Label.Value != "" || rules[2].Taint.Value != nil || rules[3].Taint.Value == nil || *rules[3].Taint.Value != "" {
		t.Fatalf("optional matching values lost in config: %+v", rules)
	}
	invalid := strings.Replace(string(raw), "name: label-empty", "name: label-presence", 1)
	if outcome, err := store.Update([]byte(invalid)); err == nil || outcome != OutcomeFailure || store.Get() != loaded {
		t.Fatalf("invalid reload replaced last good config: outcome=%s err=%v", outcome, err)
	}
	if _, err := store.Update([]byte("schemaVersion: 1\npolicies:\n  nodeHealth:\n    maintenance: {triggers: []}")); err != nil {
		t.Fatal(err)
	}
	if len(store.Get().Policies.NodeHealth.Maintenance.Triggers) != 0 {
		t.Fatal("reload did not remove maintenance rules")
	}
}
