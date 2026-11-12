package ops

import (
	"context"
	"flag"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateCreatePodSetGoldens = flag.Bool("update-create-pod-set", false, "re-record testdata/create_pod_set_allocations.json from this run")

// readPodSet is the Create pass's reading of one planned Instance's pod
// set, as decide makes it before any decision: the revision the attempt
// renders, the runners at that revision, the names the set asks for and
// the names no live pod holds.
func (f *createFinishFixture) readPodSet(tb testing.TB, pass *createPass, planned workload.InstancePlan, existing []*corev1.Pod) createCandidate {
	tb.Helper()
	c := createCandidate{
		pass:     pass,
		planned:  planned,
		index:    planned.Index,
		observed: pass.input.ObservedState.Instance(planned.Index),
		existing: existing,
	}
	if next, err := c.readPodSet(context.Background()); !next || err != nil {
		tb.Fatalf("readPodSet of Instance %d: next=%t err=%v", planned.Index, next, err)
	}
	return c
}

// readPodSets reads every planned Instance's pod set against its own pods.
func (f *createFinishFixture) readPodSets(tb testing.TB, pass *createPass) {
	tb.Helper()
	for _, planned := range f.plan.Instances {
		f.readPodSet(tb, pass, planned, pass.byInstance[planned.Index])
	}
}

// pass is the pass the readings run on, built once as createFilteredBatched
// builds it before deciding anything.
func (f *createFinishFixture) pass(tb testing.TB) *createPass {
	tb.Helper()
	pass, err := newCreatePass(context.Background(), f.deps, f.input, f.plan, f.target, keepEveryIndex, f.byInstance)
	if err != nil {
		tb.Fatalf("newCreatePass: %v", err)
	}
	return pass
}

// BenchmarkCreatePodSetReadingAtRest is the cost of reading every planned
// Instance's pod set over a Component at rest, as every Create pass does.
func BenchmarkCreatePodSetReadingAtRest(b *testing.B) {
	for _, shape := range createFinishShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newCreateFinishFixture(shape)
			pass := f.pass(b)
			b.ReportAllocs()
			for b.Loop() {
				f.readPodSets(b, pass)
			}
		})
	}
}

// TestCreatePodSetReadingAtRestStaysWithinBudget holds, per shape, the
// allocations and the bytes of reading every planned Instance's pod set
// over a Component at rest, to the recorded golden. Beside the golden:
// every Instance renders the spec revision with the plan's runners and is
// short of no pod, and an Instance read against no pod at all is short of
// every pod of its set, named in runner and ordinal order.
func TestCreatePodSetReadingAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "create_pod_set_allocations.json"), *updateCreatePodSetGoldens)
	defer golden.Close()
	for _, shape := range createFinishShapes() {
		f := newCreateFinishFixture(shape)
		pass := f.pass(t)
		f.holdPodSetsAtRest(t, shape, pass)
		golden.Check(shape.name+"/pod-set/allocations", func() { f.readPodSets(t, pass) })
		golden.CheckBytes(shape.name+"/pod-set/bytes", func() { f.readPodSets(t, pass) })
	}
}

// holdPodSetsAtRest holds the reading of every Instance at rest to the spec
// revision, the plan's runners and no missing pod, and the reading of the
// first and the last Instance against no pod to every name of the set.
func (f *createFinishFixture) holdPodSetsAtRest(t *testing.T, shape createNamesShape, pass *createPass) {
	t.Helper()
	for _, planned := range f.plan.Instances {
		c := f.readPodSet(t, pass, planned, pass.byInstance[planned.Index])
		if c.revision != createFinishRevision || !reflect.DeepEqual(c.inst.Runners, shape.runners) || len(c.missing) != 0 {
			t.Errorf("%s: Instance %d reads revision %q, runners %v and %d missing pods; want %q, %v and none",
				shape.name, planned.Index, c.revision, c.inst.Runners, len(c.missing), createFinishRevision, shape.runners)
		}
	}
	for _, planned := range []workload.InstancePlan{f.plan.Instances[0], f.plan.Instances[len(f.plan.Instances)-1]} {
		c := f.readPodSet(t, pass, planned, nil)
		if got, want := targetNames(c.missing), setNames(planned); !reflect.DeepEqual(got, want) {
			t.Errorf("%s: Instance %d read against no pod is short of %v, want %v", shape.name, planned.Index, got, want)
		}
	}
}

// targetNames spells each target as runner/ordinal=name.
func targetNames(targets []podTarget) []string {
	names := make([]string, 0, len(targets))
	for _, target := range targets {
		names = append(names, targetName(target.Runner.Name, target.Ordinal, target.Name))
	}
	return names
}

func targetName(runner string, ordinal int32, name string) string {
	return runner + "/" + strconv.Itoa(int(ordinal)) + "=" + name
}

// setNames is every name of the Instance's pod set in runner and ordinal
// order, the single-pod runners at ordinal 0.
func setNames(planned workload.InstancePlan) []string {
	var names []string
	for _, runner := range planned.Runners {
		for ordinal := int32(0); ordinal < runner.Size; ordinal++ {
			names = append(names, targetName(runner.Name, ordinal, query.PodName(createNamesOwner, workload.ComponentEngine, planned.Index, runner.Name, ordinal)))
		}
	}
	return names
}
