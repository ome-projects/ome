package alfredrecommendations

import (
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"k8s.io/utils/ptr"
)

// TestEveryDispatchReasonProjects checks the projector accepts every dispatch
// reason Alfred's dispatcher can write. A reason missing from the whitelist
// would drop its whole row from the report.
func TestEveryDispatchReasonProjects(t *testing.T) {
	files, err := filepath.Glob("../../alfred/engine/dispatch*.go")
	if err != nil {
		t.Fatal(err)
	}
	written := regexp.MustCompile(`(?:fmt\.Errorf\(|return empty, |withholdDecisions\([a-z]+, |DispatchReason = |e\.Reason = )"([A-Z][A-Za-z]+)"`)
	reasons := map[string]bool{}
	for _, file := range files {
		if matched, _ := filepath.Match("*_test.go", filepath.Base(file)); matched {
			continue
		}
		source, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range written.FindAllSubmatch(source, -1) {
			reasons[string(m[1])] = true
		}
	}
	if len(reasons) < 20 {
		t.Fatalf("found only %d dispatch reasons; the scan no longer matches the dispatcher: %v", len(reasons), reasons)
	}
	for reason := range reasons {
		if !validDispatchReason(reason) {
			t.Errorf("dispatch reason %q is not projected", reason)
		}
	}
}

// TestEveryAdvisoryReasonProjects checks the projector accepts every advisory
// reason a policy can emit, and every policy name and cause. A value missing
// from the whitelist would drop its whole row from the report.
func TestEveryAdvisoryReasonProjects(t *testing.T) {
	source, err := os.ReadFile("../../alfred/policy/api.go")
	if err != nil {
		t.Fatal(err)
	}
	declared := regexp.MustCompile(`(?m)^\s+(Advisory|Reason)[A-Za-z]+\s*=\s*"([A-Za-z]+)"`)
	found := 0
	for _, m := range declared.FindAllSubmatch(source, -1) {
		found++
		w := recommendationWire{Workload: "prod/svc", Instance: ptr.To(int32(0)), Component: "engine", Policy: "nodehealth", Reason: "NodeMaintenance", Outcome: "advisory"}
		if string(m[1]) == "Advisory" {
			w.AdvisoryReason = string(m[2])
		} else {
			w.Reason = string(m[2])
		}
		if _, ok := projectRow(w); !ok {
			t.Errorf("%s %q is not projected", m[1], m[2])
		}
	}
	if found < 15 {
		t.Fatalf("found only %d reasons in policy/api.go; the scan no longer matches it", found)
	}
	for _, policy := range []string{"defragmentation", "nodehealth", "tpudefragmentation", "migration-dispatch"} {
		w := recommendationWire{Workload: "prod/svc", Instance: ptr.To(int32(0)), Component: "engine", Policy: policy, Reason: "Fragmentation", Outcome: "advisory"}
		if _, ok := projectRow(w); !ok {
			t.Errorf("policy %q is not projected", policy)
		}
	}
}
