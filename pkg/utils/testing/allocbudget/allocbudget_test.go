package allocbudget

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

// sink keeps the measured allocation from being optimized away.
var sink []byte

// TestGolden_RecordingNamesTheDayAndTheMachine: a re-recorded golden names
// the day and the kind of machine its counts came from and nothing else:
// no commit, and the Go release without a build suffix. A golden recorded
// with an older header still opens, and the comparison never reads the
// header.
func TestGolden_RecordingNamesTheDayAndTheMachine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet_allocations.json")
	seed := `{"recordedOn":{"commit":"0123456789","date":"2026-01-01","machine":"elsewhere"},"reason":"seed","tolerancePercent":100,"budgets":{"one":1}}`
	if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
		t.Fatal(err)
	}
	one := func() { sink = make([]byte, 64) }

	compare := Open(t, path, false)
	compare.Check("one", one)
	compare.Close()

	update := Open(t, path, true)
	update.Check("one", one)
	update.Close()

	var written struct {
		RecordedOn map[string]string `json:"recordedOn"`
		Reason     string            `json:"reason"`
		Budgets    map[string]int64  `json:"budgets"`
	}
	data, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(data, &written)
	}
	if err != nil {
		t.Fatal(err)
	}
	if _, commit := written.RecordedOn["commit"]; commit || len(written.RecordedOn) != 2 {
		t.Errorf("recordedOn = %v, want a date and a machine only", written.RecordedOn)
	}
	if _, err := time.Parse("2006-01-02", written.RecordedOn["date"]); err != nil {
		t.Errorf("recordedOn.date = %q: %v", written.RecordedOn["date"], err)
	}
	want := fmt.Sprintf("%s/%s, %d cores, %s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), goRelease(runtime.Version()))
	if written.RecordedOn["machine"] != want {
		t.Errorf("recordedOn.machine = %q, want %q", written.RecordedOn["machine"], want)
	}
	if written.Reason != "seed" {
		t.Errorf("reason = %q, want the hand-written reason kept", written.Reason)
	}
	if _, ok := written.Budgets["one"]; !ok || len(written.Budgets) != 1 {
		t.Errorf("budgets = %v, want the one measured count", written.Budgets)
	}
}

func TestGoRelease_IsTheFirstWordOfTheVersion(t *testing.T) {
	for version, want := range map[string]string{
		"go1.26.7":                   "go1.26.7",
		"go1.26.7 (vendor 1.26.7-1)": "go1.26.7",
		"go1.27rc1 X:experiment":     "go1.27rc1",
	} {
		if got := goRelease(version); got != want {
			t.Errorf("goRelease(%q) = %q, want %q", version, got, want)
		}
	}
}
