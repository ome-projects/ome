// Package allocbudget holds the allocation count of a hot path at fleet size
// to a recorded golden: a count with a tolerance. A test that measures a
// count outside the tolerance fails, in either direction, so a change to the
// cost of a pass is made on purpose and carries its reason. Allocation
// counts, unlike durations, are the same on every machine, and so are the
// bytes a path allocates (CheckBytes); CheckCount holds any other reading
// with that property, such as the bytes of an encoding, to the same kind of
// golden.
//
// A count is the same on every machine that runs the Go release line
// (major.minor) the golden was recorded under; another line inlines and
// escapes differently. Under another line the counts are measured and
// printed, not held, and -update re-records them under the running release.
//
// The scheduler is a separate Go module that cannot import this package and
// keeps a copy of it, byte for byte, under scheduler/pkg/testing/allocbudget.
// A change here is made in both; TestCopies_AreTheSame holds them together.
package allocbudget

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"regexp"
	"runtime"
	"strings"
	"testing"
	"time"
)

// Samples is the number of measured runs behind one count. The smallest
// run is the count: another goroutine in the process, or a map whose random
// hash seed packs one of its tables past its share and splits it, can add
// allocations to a run, never remove them. Where a map splits in one call
// of three, twenty runs leave a chance below one in a billion that every
// run carries a split.
const Samples = 20

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
	// Budgets maps one measured path to its reading: allocations per
	// operation, or the count CheckCount was given.
	Budgets map[string]int64 `json:"budgets"`
}

// Recording says when and on what kind of machine a golden's counts were
// measured: the platform, the core count and the Go release, in one string.
// The release is the one part the comparison reads.
type Recording struct {
	Date    string `json:"date"`
	Machine string `json:"machine"`
}

// releaseLinePattern is the major.minor prefix of a Go release string:
// "go1.26" in "go1.26.7" and in "go1.26rc1".
var releaseLinePattern = regexp.MustCompile(`^go\d+\.\d+`)

// GoRelease is the release the counts were measured under, "go1.26.7", the
// field of Machine that names one; "" for a golden recorded without it.
func (r Recording) GoRelease() string {
	for _, field := range strings.Split(r.Machine, ",") {
		if field = strings.TrimSpace(field); releaseLinePattern.MatchString(field) {
			return goRelease(field)
		}
	}
	return ""
}

// File is one golden file and the counts measured against it in one test.
type File struct {
	t        testing.TB
	path     string
	update   bool
	golden   Golden
	measured map[string]int64
	// held is whether the counts are compared with the budgets: they are
	// under the Go release line the golden was recorded under, and under
	// any line when the golden names none.
	held bool
}

// Open reads the golden at path. With update set, Close rewrites the file
// from the counts Check measured instead of comparing them.
func Open(t testing.TB, path string, update bool) *File {
	t.Helper()
	if raceDetector {
		t.Skip("allocation counts are not stable under the race detector: it changes what the compiler inlines, and sync.Pool drops entries at random")
	}
	f := &File{t: t, path: path, update: update, measured: map[string]int64{}, held: true}
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
	recorded, running := f.golden.RecordedOn.GoRelease(), runningGoRelease()
	if !update && recorded != "" && releaseLine(recorded) != releaseLine(running) {
		f.held = false
		t.Logf("%s was recorded under %s and this run is under %s: its counts are measured and printed, not held. GOTOOLCHAIN=%s holds them; -update re-records them under %s.",
			path, recorded, running, recorded, running)
	}
	return f
}

// Measure returns the allocations of one call of op: the smallest of
// Samples measured calls after one warm-up call.
func Measure(op func()) int64 {
	allocations, _ := Smallest(op)
	return allocations
}

// Bytes returns the bytes one call of op allocates: the smallest of Samples
// measured calls after one warm-up call, as Measure counts allocations. A
// pooled buffer the collector dropped between two calls makes a call
// allocate more, never less, so the smallest call is the path's own cost.
func Bytes(op func()) int64 {
	_, bytes := Smallest(op)
	return bytes
}

// Smallest reads Samples calls of op after one warm-up call and returns the
// smallest allocations and the smallest bytes of one call, each over its
// own series. The calls run under one processor, as testing.AllocsPerRun
// runs them, so no other goroutine allocates between the two readings of a
// call.
func Smallest(op func()) (allocations, bytes int64) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	op()
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	mallocs, total := stats.Mallocs, stats.TotalAlloc
	allocations, bytes = -1, -1
	for i := 0; i < Samples; i++ {
		op()
		runtime.ReadMemStats(&stats)
		if n := int64(stats.Mallocs - mallocs); allocations < 0 || n < allocations {
			allocations = n
		}
		if n := int64(stats.TotalAlloc - total); bytes < 0 || n < bytes {
			bytes = n
		}
		mallocs, total = stats.Mallocs, stats.TotalAlloc
	}
	return allocations, bytes
}

// Check measures the allocations of op and holds the count to the budget
// named name.
func (f *File) Check(name string, op func()) {
	f.t.Helper()
	f.CheckCount(name, Measure(op))
}

// CheckBytes measures the bytes op allocates and holds them to the budget
// named name.
func (f *File) CheckBytes(name string, op func()) {
	f.t.Helper()
	f.CheckCount(name, Bytes(op))
}

// CheckCount holds count, a reading the caller took, to the budget named
// name. The reading must be the same on every machine: an allocation count
// or the bytes of an encoding, never a duration.
func (f *File) CheckCount(name string, count int64) {
	f.t.Helper()
	f.measured[name] = count
	if f.update {
		return
	}
	budget, ok := f.golden.Budgets[name]
	if !ok {
		f.t.Errorf("%s has no budget named %s (measured %d); rerun with -update to record it and write the reason in the file", f.path, name, count)
		return
	}
	if !f.held {
		f.t.Logf("%s: measured %d against a budget of %d recorded under %s, not held under %s", name, count, budget, f.golden.RecordedOn.GoRelease(), runningGoRelease())
		return
	}
	slack := float64(budget) * f.golden.TolerancePercent / 100
	low, high := int64(math.Ceil(float64(budget)-slack)), int64(math.Floor(float64(budget)+slack))
	if count < low || count > high {
		f.t.Errorf("%s: measured %d, outside the budget of %d recorded on %s (%g percent tolerance: %d to %d). If the new cost is intended, rerun with -update to re-record %s and write the reason in its reason field.",
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
	f.t.Logf("re-recorded %s under %s; write the reason in its reason field", f.path, runningGoRelease())
}

// recordingNow names today and this kind of machine: the platform, the
// core count and the Go release.
func recordingNow() Recording {
	machine := fmt.Sprintf("%s/%s, %d cores, %s", runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runningGoRelease())
	return Recording{Date: time.Now().UTC().Format("2006-01-02"), Machine: machine}
}

// runningGoRelease is the release of the toolchain running the test.
func runningGoRelease() string {
	return goRelease(runtime.Version())
}

// goRelease is the toolchain's release, the first word of a runtime.Version
// string; a build suffix after it is not part of the release.
func goRelease(version string) string {
	release, _, _ := strings.Cut(version, " ")
	return release
}

// releaseLine is the major.minor of a release, "go1.26" for "go1.26.7" and
// for "go1.26rc1"; a string that names no such release is its own line.
func releaseLine(release string) string {
	if line := releaseLinePattern.FindString(release); line != "" {
		return line
	}
	return release
}
