// Package allocbudget holds the allocation count of a hot path at fleet size
// to a recorded golden: a count with a tolerance. A test that measures a
// count outside the tolerance fails, in either direction, so a change to the
// cost of a pass is made on purpose and carries its reason. Allocation
// counts, unlike durations, are the same on every machine.
package allocbudget

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Samples is the number of measured runs behind one count. The smallest run
// is the count: another goroutine in the process can add allocations to a
// run, never remove them.
const Samples = 5

// Golden is the file's shape.
type Golden struct {
	// RecordedOn names the day and the machine the counts came from.
	RecordedOn Recording `json:"recordedOn"`
	// Reason is written by hand with every re-recording: what moved the
	// counts and why that is intended.
	Reason string `json:"reason"`
	// TolerancePercent is how far a count may sit from its budget, either
	// way, before the test fails.
	TolerancePercent float64 `json:"tolerancePercent"`
	// Budgets maps one measured path to its allocations per operation.
	Budgets map[string]int64 `json:"budgets"`
}

// Recording says when and on what kind of machine a golden's counts were
// measured. The comparison never reads it.
type Recording struct {
	Date    string `json:"date"`
	Machine string `json:"machine"`
}

// File is one golden file and the counts measured against it in one test.
type File struct {
	t        testing.TB
	path     string
	update   bool
	golden   Golden
	measured map[string]int64
}

// Open reads the golden at path. With update set, Close rewrites the file
// from the counts Check measured instead of comparing them.
func Open(t testing.TB, path string, update bool) *File {
	t.Helper()
	if raceDetector {
		t.Skip("allocation counts are not stable under the race detector: sync.Pool drops entries at random")
	}
	f := &File{t: t, path: path, update: update, measured: map[string]int64{}}
	data, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(data, &f.golden)
	}
	if err != nil {
		t.Fatalf("reading the allocation golden: %v", err)
	}
	if f.golden.TolerancePercent <= 0 {
		t.Fatalf("%s: tolerancePercent must be above zero", path)
	}
	return f
}

// Measure returns the allocations of one call of op: the smallest of Samples
// measured calls, each after a warm-up call.
func Measure(op func()) int64 {
	best := int64(-1)
	for i := 0; i < Samples; i++ {
		if n := int64(testing.AllocsPerRun(1, op)); best < 0 || n < best {
			best = n
		}
	}
	return best
}

// Check measures op and holds its count to the budget named name.
func (f *File) Check(name string, op func()) {
	f.t.Helper()
	count := Measure(op)
	f.measured[name] = count
	if f.update {
		return
	}
	budget, ok := f.golden.Budgets[name]
	if !ok {
		f.t.Errorf("%s has no budget named %s (measured %d allocations per operation); rerun with -update to record it and write the reason in the file", f.path, name, count)
		return
	}
	slack := float64(budget) * f.golden.TolerancePercent / 100
	low, high := int64(math.Ceil(float64(budget)-slack)), int64(math.Floor(float64(budget)+slack))
	if count < low || count > high {
		f.t.Errorf("%s: %d allocations per operation, outside the budget of %d recorded on %s (%g percent tolerance: %d to %d). If the new cost is intended, rerun with -update to re-record %s and write the reason in its reason field.",
			name, count, budget, f.golden.RecordedOn.Date, f.golden.TolerancePercent, low, high, f.path)
	}
}

// Close ends the check. On update it writes the file from the measured
// counts; otherwise it fails on a budget nothing measured.
func (f *File) Close() {
	f.t.Helper()
	if !f.update {
		for name := range f.golden.Budgets {
			if _, ok := f.measured[name]; !ok {
				f.t.Errorf("%s has a budget named %s that nothing measured; remove it or rerun with -update", f.path, name)
			}
		}
		return
	}
	if f.t.Failed() {
		return
	}
	f.golden.Budgets = f.measured
	f.golden.RecordedOn = recordingNow()
	data, err := json.MarshalIndent(f.golden, "", "  ")
	if err == nil {
		err = os.WriteFile(f.path, append(data, '\n'), 0o644)
	}
	if err != nil {
		f.t.Fatalf("writing the allocation golden: %v", err)
	}
	f.t.Logf("re-recorded %s; write the reason in its reason field", f.path)
}

// recordingNow names today and this kind of machine: the platform, the
// core count and the Go release.
func recordingNow() Recording {
	machine := fmt.Sprintf("%s/%s, %d cores, %s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), goRelease(runtime.Version()))
	return Recording{Date: time.Now().UTC().Format("2006-01-02"), Machine: machine}
}

// goRelease is the toolchain's release, the first word of a runtime.Version
// string; a build suffix after it is not part of the release.
func goRelease(version string) string {
	release, _, _ := strings.Cut(version, " ")
	return release
}
