package allocbudget

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
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

// TestCheckCount_HoldsAReadingTheCallerTook: a count inside the tolerance
// passes against the recorded budget, and an update records the count as
// it was given.
func TestCheckCount_HoldsAReadingTheCallerTook(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet_status_bytes.json")
	seed := `{"recordedOn":{"date":"2026-01-01","machine":"elsewhere"},"reason":"seed","tolerancePercent":10,"budgets":{"bytes":1000}}`
	if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
		t.Fatal(err)
	}

	compare := Open(t, path, false)
	compare.CheckCount("bytes", 1050)
	compare.Close()

	update := Open(t, path, true)
	update.CheckCount("bytes", 2000)
	update.Close()

	var written struct {
		Budgets map[string]int64 `json:"budgets"`
	}
	data, err := os.ReadFile(path)
	if err == nil {
		err = json.Unmarshal(data, &written)
	}
	if err != nil {
		t.Fatal(err)
	}
	if written.Budgets["bytes"] != 2000 || len(written.Budgets) != 1 {
		t.Errorf("budgets = %v, want the one count as given", written.Budgets)
	}
}

// recordingTB is a testing.TB that keeps the errors and the log lines a
// check reports instead of failing the test that drives it.
type recordingTB struct {
	testing.TB
	errors []string
	logs   []string
}

func (r *recordingTB) Errorf(format string, args ...any) {
	r.errors = append(r.errors, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Logf(format string, args ...any) {
	r.logs = append(r.logs, fmt.Sprintf(format, args...))
}

func (r *recordingTB) Helper() {}

// writeGolden seeds one golden with one budget of 1,000 at 10 percent,
// recorded on a machine whose Go release is release ("" for none).
func writeGolden(t *testing.T, release string) string {
	t.Helper()
	machine := "linux/amd64, 8 cores"
	if release != "" {
		machine += ", " + release
	}
	path := filepath.Join(t.TempDir(), "fleet_allocations.json")
	seed := fmt.Sprintf(`{"recordedOn":{"date":"2026-01-01","machine":%q},"reason":"seed","tolerancePercent":10,"budgets":{"one":1000}}`, machine)
	if err := os.WriteFile(path, []byte(seed), 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

// TestCheckCount_HoldsOnlyUnderTheRecordedGoReleaseLine: a count is held to
// its budget under the Go release line (major.minor) the golden was recorded
// under, whatever the patch release, and under any line when the golden
// names none. Under another line the count is printed with both releases
// and not held; a budget nothing recorded still fails there.
func TestCheckCount_HoldsOnlyUnderTheRecordedGoReleaseLine(t *testing.T) {
	running := goRelease(runtime.Version())
	for name, tc := range map[string]struct {
		recorded string
		held     bool
	}{
		"the running release":                  {recorded: running, held: true},
		"another patch release of the line":    {recorded: releaseLine(running) + ".99", held: true},
		"the release with a build suffix":      {recorded: running + " (vendor 1-1.el9)", held: true},
		"no release recorded":                  {recorded: "", held: true},
		"another line":                         {recorded: "go1.0.1", held: false},
		"another line, release candidate form": {recorded: "go1.0rc1", held: false},
	} {
		t.Run(name, func(t *testing.T) {
			tb := &recordingTB{TB: t}
			file := Open(tb, writeGolden(t, tc.recorded), false)
			file.CheckCount("one", 5000)
			file.Close()
			if got := len(tb.errors); (got == 1) != tc.held {
				t.Fatalf("5,000 against a budget of 1,000 recorded under %q: %d failures (%v), want held=%v", tc.recorded, got, tb.errors, tc.held)
			}
			if tc.held {
				return
			}
			printed := strings.Join(tb.logs, "\n")
			for _, want := range []string{"measured 5000", "budget of 1000", tc.recorded, running} {
				if !strings.Contains(printed, want) {
					t.Errorf("the log %q does not name %q", printed, want)
				}
			}
			file.CheckCount("two", 1)
			if len(tb.errors) != 1 || !strings.Contains(tb.errors[0], "no budget named two") {
				t.Errorf("a count with no budget under another line: errors %v, want the missing budget", tb.errors)
			}
		})
	}
}

// TestClose_UpdateRecordsTheRunningGoRelease: -update under another line
// re-records the counts under the running release, which then holds them.
func TestClose_UpdateRecordsTheRunningGoRelease(t *testing.T) {
	path := writeGolden(t, "go1.0.1")
	update := Open(t, path, true)
	update.CheckCount("one", 5000)
	update.Close()

	tb := &recordingTB{TB: t}
	file := Open(tb, path, false)
	if got, want := file.golden.RecordedOn.GoRelease(), goRelease(runtime.Version()); got != want {
		t.Errorf("recorded release after -update = %q, want %q", got, want)
	}
	file.CheckCount("one", 1000)
	file.Close()
	if len(tb.errors) != 1 {
		t.Errorf("1,000 against the re-recorded budget of 5,000: errors %v, want one", tb.errors)
	}
}

func TestRecording_GoReleaseIsTheMachinesReleaseField(t *testing.T) {
	for machine, want := range map[string]string{
		"linux/amd64, 192 cores, go1.26.7":                       "go1.26.7",
		"linux/amd64, 30 cores, go1.26.7 (Red Hat 1.26.7-1.el9)": "go1.26.7",
		"darwin/arm64, 12 cores, go1.27rc1, built from source":   "go1.27rc1",
		"elsewhere":                           "",
		"":                                    "",
		"linux/amd64, 8 cores, good hardware": "",
	} {
		if got := (Recording{Machine: machine}).GoRelease(); got != want {
			t.Errorf("GoRelease of %q = %q, want %q", machine, got, want)
		}
	}
}

func TestReleaseLine_IsTheMajorAndMinor(t *testing.T) {
	for release, want := range map[string]string{
		"go1.26.7":  "go1.26",
		"go1.26":    "go1.26",
		"go1.27rc1": "go1.27",
		"go1.27.0":  "go1.27",
		"devel":     "devel",
	} {
		if got := releaseLine(release); got != want {
			t.Errorf("releaseLine(%q) = %q, want %q", release, got, want)
		}
	}
}

// TestBytes_IsTheBytesOfOneCall: Bytes reads what one call of the path
// allocates, the smallest of its samples, so a 64 KiB slice reads as 64 KiB
// and not as the warm-up's or the samples' sum.
func TestBytes_IsTheBytesOfOneCall(t *testing.T) {
	const size = 64 << 10
	got := Bytes(func() { sink = make([]byte, size) })
	if got < size || got >= 2*size {
		t.Errorf("Bytes() = %d, want the one slice of %d bytes", got, size)
	}
	if Measure(func() { sink = make([]byte, size) }) != 1 {
		t.Error("Measure() of one slice is not 1 allocation")
	}
}

// TestSmallest_ReadsSamplesCallsAfterOneWarmUp: a count is the smallest of
// Samples measured calls, taken after one call that is not read.
func TestSmallest_ReadsSamplesCallsAfterOneWarmUp(t *testing.T) {
	calls := 0
	Measure(func() { calls++ })
	if calls != Samples+1 {
		t.Errorf("Measure() made %d calls, want one warm-up call and %d measured calls", calls, Samples)
	}
}

// TestSmallest_IsTheCheapestCall: a call that allocates more now and then,
// as one does when a map splits a table or another goroutine allocates, is
// read at the cost of its cheapest call, in allocations and in bytes.
func TestSmallest_IsTheCheapestCall(t *testing.T) {
	const size = 64 << 10
	calls := 0
	allocations, bytes := Smallest(func() {
		calls++
		sink = make([]byte, size)
		if calls%2 == 0 {
			sink = append(sink, make([]byte, size)...)
		}
	})
	if allocations != 1 {
		t.Errorf("Smallest() = %d allocations, want the one slice of the cheaper call", allocations)
	}
	if bytes < size || bytes >= 2*size {
		t.Errorf("Smallest() = %d bytes, want the one slice of %d bytes", bytes, size)
	}
}

// TestCopies_AreTheSame: the scheduler module keeps a byte-for-byte copy of
// this package, because it cannot import the manager's; the two copies hold
// the same files with the same bytes.
func TestCopies_AreTheSame(t *testing.T) {
	here, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	root := here
	for {
		if _, err := os.Stat(filepath.Join(root, "scheduler", "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatalf("no repository root with scheduler/go.mod above %s", here)
		}
		root = parent
	}
	copies := []string{
		filepath.Join(root, "pkg", "utils", "testing", "allocbudget"),
		filepath.Join(root, "scheduler", "pkg", "testing", "allocbudget"),
	}
	files := map[string][]string{}
	for _, dir := range copies {
		names, err := filepath.Glob(filepath.Join(dir, "*.go"))
		if err != nil || len(names) == 0 {
			t.Fatalf("listing %s: %v (%d files)", dir, err, len(names))
		}
		for _, name := range names {
			files[filepath.Base(name)] = append(files[filepath.Base(name)], name)
		}
	}
	for name, paths := range files {
		if len(paths) != 2 {
			t.Errorf("%s exists in one copy only: %v", name, paths)
			continue
		}
		first, err := os.ReadFile(paths[0])
		if err != nil {
			t.Fatal(err)
		}
		second, err := os.ReadFile(paths[1])
		if err != nil {
			t.Fatal(err)
		}
		if string(first) != string(second) {
			t.Errorf("%s differs between %s and %s; copy the changed one over the other", name, copies[0], copies[1])
		}
	}
}
