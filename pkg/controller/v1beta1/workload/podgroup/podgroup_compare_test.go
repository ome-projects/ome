package podgroup

import (
	"flag"
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateOwnedFieldsCompareGoldens = flag.Bool("update-owned-fields-compare", false, "re-record testdata/owned_fields_compare_allocations.json from this run")

// ownedFieldsWithEverySet is a reading with a value in every owned field:
// the four labels, one owner reference with both of its flags set, a
// topology key, a size and a timeout, so a walk over it reaches every leaf
// the struct has.
func ownedFieldsWithEverySet() ownedFields {
	controller, blockOwnerDeletion := true, true
	timeout := int32(300)
	return ownedFields{
		Labels: map[string]string{
			constants.InferenceServicePodLabelKey: "fleet",
			constants.OMEComponentLabel:           "engine",
			query.LabelManagedBy:                  query.ManagedByOMENative,
			query.LabelInstanceIdx:                "3",
		},
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "fleet", UID: "fleet-uid",
			Controller: &controller, BlockOwnerDeletion: &blockOwnerDeletion,
		}},
		TopologyKey:            "topology.example.com/rack",
		TopologyKeySet:         true,
		MinMember:              8,
		ScheduleTimeoutSeconds: &timeout,
	}
}

// ownedFieldsWithFlagsAndTimeoutUnset is the reading with every field set,
// except that the owner reference's two flags and the timeout are nil. The
// walk descends into an owner reference only when the reading holds one,
// so this is the reading on which it fills a nil flag.
func ownedFieldsWithFlagsAndTimeoutUnset() ownedFields {
	fields := ownedFieldsWithEverySet()
	fields.OwnerReferences[0].Controller = nil
	fields.OwnerReferences[0].BlockOwnerDeletion = nil
	fields.ScheduleTimeoutSeconds = nil
	return fields
}

// cloneOwnedFields copies a reading so a change to the copy leaves the
// original as it was: the label map, the owner references with their flags
// and the timeout each get their own memory.
func cloneOwnedFields(fields ownedFields) ownedFields {
	clone := fields
	clone.Labels = maps.Clone(fields.Labels)
	if fields.OwnerReferences != nil {
		clone.OwnerReferences = make([]metav1.OwnerReference, len(fields.OwnerReferences))
		for i := range fields.OwnerReferences {
			fields.OwnerReferences[i].DeepCopyInto(&clone.OwnerReferences[i])
		}
	}
	if fields.ScheduleTimeoutSeconds != nil {
		timeout := *fields.ScheduleTimeoutSeconds
		clone.ScheduleTimeoutSeconds = &timeout
	}
	return clone
}

// ownedLeaf reads the value at one path of a reading.
type ownedLeaf func(*ownedFields) reflect.Value

// leafChange is the change the walk makes to a leaf of a given kind: a flag
// flipped, a count moved, a string lengthened. Nil for a kind the walk
// descends into instead.
func leafChange(value reflect.Value) func(reflect.Value) {
	switch value.Kind() {
	case reflect.Bool:
		return func(v reflect.Value) { v.SetBool(!v.Bool()) }
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return func(v reflect.Value) { v.SetInt(v.Int() + 1) }
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return func(v reflect.Value) { v.SetUint(v.Uint() + 1) }
	case reflect.String:
		return func(v reflect.Value) { v.SetString(v.String() + "x") }
	default:
		return nil
	}
}

// forEachOwnedFieldChange calls visit once per leaf of base with a copy of
// base that differs from it in that leaf alone: a scalar moved, a nil
// pointer or an empty map or slice filled, a set pointer cleared, a slice
// emptied or grown by one, a map emptied, grown by one key, changed in one
// value or shrunk by one key. Every such copy reads as different under the
// semantic compare, and a field of a kind the walk does not know fails the
// test, so a field added to ownedFields is walked or named here.
func forEachOwnedFieldChange(t *testing.T, base ownedFields, visit func(path string, changed ownedFields)) {
	t.Helper()
	var walk func(path string, at ownedLeaf)
	walk = func(path string, at ownedLeaf) {
		change := func(suffix string, mutate func(reflect.Value)) {
			changed := cloneOwnedFields(base)
			mutate(at(&changed))
			visit(path+suffix, changed)
		}
		value := at(&base)
		if mutate := leafChange(value); mutate != nil {
			change("", mutate)
			return
		}
		switch value.Kind() {
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				walk(path+"."+value.Type().Field(i).Name, func(f *ownedFields) reflect.Value { return at(f).Field(i) })
			}
		case reflect.Pointer:
			if value.IsNil() {
				change("", func(v reflect.Value) { v.Set(reflect.New(v.Type().Elem())) })
				return
			}
			change("=nil", func(v reflect.Value) { v.Set(reflect.Zero(v.Type())) })
			walk(path, func(f *ownedFields) reflect.Value { return at(f).Elem() })
		case reflect.Slice:
			if value.Len() == 0 {
				change("", func(v reflect.Value) { v.Set(reflect.MakeSlice(v.Type(), 1, 1)) })
				return
			}
			change("=nil", func(v reflect.Value) { v.Set(reflect.Zero(v.Type())) })
			change("+1", func(v reflect.Value) { v.Set(reflect.Append(v, reflect.Zero(v.Type().Elem()))) })
			walk(path+"[0]", func(f *ownedFields) reflect.Value { return at(f).Index(0) })
		case reflect.Map:
			stringMapChanges(t, path, value, change)
		default:
			t.Fatalf("%s: a field of kind %s has no change in this walk; add one", path, value.Kind())
		}
	}
	walk("owned", func(f *ownedFields) reflect.Value { return reflect.ValueOf(f).Elem() })
}

// stringMapChanges makes the walk's changes to a map of strings: an empty
// one is filled with one entry; a filled one is cleared, grown by one key,
// changed in the value of its first key, and shrunk by that key.
func stringMapChanges(t *testing.T, path string, value reflect.Value, change func(suffix string, mutate func(reflect.Value))) {
	t.Helper()
	if value.Type().Key().Kind() != reflect.String || value.Type().Elem().Kind() != reflect.String {
		t.Fatalf("%s: a map of type %s has no change in this walk; add one", path, value.Type())
	}
	if value.Len() == 0 {
		change("", func(v reflect.Value) {
			filled := reflect.MakeMap(v.Type())
			filled.SetMapIndex(reflect.ValueOf("key"), reflect.ValueOf("value"))
			v.Set(filled)
		})
		return
	}
	keys := make([]string, 0, value.Len())
	for _, key := range value.MapKeys() {
		keys = append(keys, key.String())
	}
	slices.Sort(keys)
	first := reflect.ValueOf(keys[0])
	change("=nil", func(v reflect.Value) { v.Set(reflect.Zero(v.Type())) })
	change("+1", func(v reflect.Value) { v.SetMapIndex(reflect.ValueOf("added"), reflect.ValueOf("")) })
	change("["+keys[0]+"]", func(v reflect.Value) { v.SetMapIndex(first, reflect.ValueOf(v.MapIndex(first).String()+"x")) })
	change("-"+keys[0], func(v reflect.Value) { v.SetMapIndex(first, reflect.Value{}) })
}

// The compare answers as the semantic compare answers on every leaf of the
// owned fields: a reading that differs in one leaf reads as different, and
// a reading reads as equal to its own copy. Three readings are walked: one
// with every field set, one with every field empty, and one with every
// field set but the owner reference's two flags and the timeout, which are
// nil. The walk reaches a flag only inside an owner reference the reading
// holds, so the third is where a nil flag is filled. Between the three,
// every pointer is filled from nil and cleared, and every map and slice is
// filled and emptied.
func TestOwnedFieldsEqual_AgreesWithTheSemanticCompareOnEveryOwnedField(t *testing.T) {
	for _, base := range []struct {
		name   string
		fields ownedFields
	}{
		{name: "every field set", fields: ownedFieldsWithEverySet()},
		{name: "every field empty", fields: ownedFields{}},
		{name: "the flags and the timeout unset", fields: ownedFieldsWithFlagsAndTimeoutUnset()},
	} {
		t.Run(base.name, func(t *testing.T) {
			leaves := 0
			forEachOwnedFieldChange(t, base.fields, func(path string, changed ownedFields) {
				leaves++
				want := equality.Semantic.DeepEqual(base.fields, changed)
				if got := base.fields.equal(changed); got != want {
					t.Errorf("%s changed: equal() = %v, the semantic compare says %v", path, got, want)
				}
				if want {
					t.Errorf("%s changed: the semantic compare reads the changed reading as equal; the walk's change is not one", path)
				}
				if !changed.equal(cloneOwnedFields(changed)) {
					t.Errorf("%s changed: the changed reading and its copy read as different", path)
				}
			})
			if fields := reflect.TypeOf(base.fields).NumField(); leaves < fields {
				t.Errorf("the walk changed %d leaves of a reading with %d fields; every field has at least one leaf", leaves, fields)
			}
			t.Logf("%d leaves changed, one at a time, over %d owned fields", leaves, reflect.TypeOf(base.fields).NumField())
		})
	}
}

// The semantic compare reads a nil label map and an empty one as the same,
// and a nil owner reference list and an empty one; a timeout, a flag or
// the topology key set on one side alone is a difference. The compare
// agrees on each.
func TestOwnedFieldsEqual_ReadsNilAndEmptyAsEqual(t *testing.T) {
	left, right := ownedFieldsWithEverySet(), ownedFieldsWithEverySet()
	left.Labels, right.Labels = nil, map[string]string{}
	left.OwnerReferences, right.OwnerReferences = []metav1.OwnerReference{}, nil
	if !equality.Semantic.DeepEqual(left, right) {
		t.Fatalf("the semantic compare reads the two readings as different; the fixture is wrong")
	}
	if !left.equal(right) {
		t.Errorf("equal() = false over nil against empty labels and owner references; the semantic compare says true")
	}
	for name, change := range map[string]func(*ownedFields){
		"the timeout unset on one side":         func(f *ownedFields) { f.ScheduleTimeoutSeconds = nil },
		"the controller flag unset on one side": func(f *ownedFields) { f.OwnerReferences[0].Controller = nil },
		"the topology key recorded as unset":    func(f *ownedFields) { f.TopologyKeySet = false },
		"the owner reference list emptied":      func(f *ownedFields) { f.OwnerReferences = nil },
		"the label map holding one entry fewer": func(f *ownedFields) { delete(f.Labels, query.LabelInstanceIdx) },
	} {
		changed := cloneOwnedFields(ownedFieldsWithEverySet())
		change(&changed)
		if ownedFieldsWithEverySet().equal(changed) {
			t.Errorf("%s: equal() = true", name)
		}
	}
}

// groupThePassWouldWrite is one gang's PodGroup as the pass builds it for a
// Component with a topology key and a placement grace: every owned field
// set, as every group of a Component at rest carries them.
func groupThePassWouldWrite(tb testing.TB) *schedulingv1alpha1.PodGroup {
	tb.Helper()
	owner := newPodGroupISVC("fleet", "fleet")
	plan := workload.ComponentPlan{Component: workload.ComponentEngine, TopologyKey: "topology.example.com/rack"}
	inst := workload.InstancePlan{Index: 3, Runners: []workload.RunnerPlan{{Name: "leader", Size: 1}, {Name: "worker", Size: 7}}}
	group, err := BuildPodGroup(owner, testPodGroupOwnerGVK, owner.GetName(), plan, 5*time.Minute, inst)
	if err != nil {
		tb.Fatalf("BuildPodGroup: %v", err)
	}
	return group
}

// podGroupChange changes the group the pass would write (desired) or the
// one found at its name (existing), or both where a difference needs two
// sides.
type podGroupChange func(desired, existing *schedulingv1alpha1.PodGroup)

// WriteNeeded reads the owned fields off the object at the name: a change
// to any of them calls for a write, a change outside them (another
// annotation, the status, the resource version) does not, and the group as
// the pass would write it reads as unchanged. Every row starts from two
// copies of that group. The semantic compare of the two readings agrees on
// every row.
func TestWriteNeeded_EveryOwnedFieldOfTheObjectDecidesIt(t *testing.T) {
	written := groupThePassWouldWrite(t)
	for _, row := range []struct {
		name   string
		change podGroupChange
		want   bool
	}{
		{name: "the object as the pass would write it", change: func(_, _ *schedulingv1alpha1.PodGroup) {}, want: false},
		{name: "another annotation", change: func(_, pg *schedulingv1alpha1.PodGroup) { pg.Annotations["example.com/note"] = "kept" }, want: false},
		{name: "the status", change: func(_, pg *schedulingv1alpha1.PodGroup) { pg.Status.Phase = schedulingv1alpha1.PodGroupRunning }, want: false},
		{name: "the resource version", change: func(_, pg *schedulingv1alpha1.PodGroup) { pg.ResourceVersion = "9" }, want: false},
		{name: "a label changed", change: func(_, pg *schedulingv1alpha1.PodGroup) { pg.Labels[constants.OMEComponentLabel] = "decoder" }, want: true},
		{name: "a label dropped", change: func(_, pg *schedulingv1alpha1.PodGroup) { delete(pg.Labels, query.LabelManagedBy) }, want: true},
		{name: "a label added", change: func(_, pg *schedulingv1alpha1.PodGroup) { pg.Labels["example.com/extra"] = "yes" }, want: true},
		{name: "the owner reference's UID", change: func(_, pg *schedulingv1alpha1.PodGroup) { pg.OwnerReferences[0].UID = "other" }, want: true},
		{name: "the owner reference's controller flag", change: func(_, pg *schedulingv1alpha1.PodGroup) { pg.OwnerReferences[0].Controller = nil }, want: true},
		{name: "a second owner reference", change: func(_, pg *schedulingv1alpha1.PodGroup) {
			pg.OwnerReferences = append(pg.OwnerReferences, pg.OwnerReferences[0])
		}, want: true},
		{name: "two owner references in the other order", change: func(desired, existing *schedulingv1alpha1.PodGroup) {
			second := metav1.OwnerReference{
				APIVersion: testPodGroupOwnerGVK.GroupVersion().String(), Kind: "InferenceReplica",
				Name: "fleet-engine", UID: "fleet-engine-uid",
			}
			desired.OwnerReferences = append(desired.OwnerReferences, second)
			existing.OwnerReferences = []metav1.OwnerReference{second, existing.OwnerReferences[0]}
		}, want: true},
		{name: "the topology key", change: func(_, pg *schedulingv1alpha1.PodGroup) {
			pg.Annotations[query.AnnotationTopologyKey] = "topology.example.com/zone"
		}, want: true},
		{name: "the topology key removed", change: func(_, pg *schedulingv1alpha1.PodGroup) { delete(pg.Annotations, query.AnnotationTopologyKey) }, want: true},
		{name: "the size", change: func(_, pg *schedulingv1alpha1.PodGroup) { pg.Spec.MinMember = 9 }, want: true},
		{name: "the timeout", change: func(_, pg *schedulingv1alpha1.PodGroup) { *pg.Spec.ScheduleTimeoutSeconds = 299 }, want: true},
		{name: "the timeout unset", change: func(_, pg *schedulingv1alpha1.PodGroup) { pg.Spec.ScheduleTimeoutSeconds = nil }, want: true},
	} {
		t.Run(row.name, func(t *testing.T) {
			desired, existing := written.DeepCopy(), written.DeepCopy()
			row.change(desired, existing)
			if got := WriteNeeded(desired, existing, true); got != row.want {
				t.Errorf("WriteNeeded() = %v, want %v", got, row.want)
			}
			if want := !equality.Semantic.DeepEqual(ownedFieldsOf(desired), ownedFieldsOf(existing)); want != row.want {
				t.Errorf("the semantic compare of the two readings says %v; the row is wrong", want)
			}
		})
	}
	if !WriteNeeded(written, nil, false) {
		t.Errorf("WriteNeeded() = false for a free name")
	}
}

// compareSink keeps the compare's answer, so the measured call is made.
var compareSink bool

// TestOwnedFieldsCompareStaysWithinBudget holds the allocations and the
// bytes of one compare of a gang's group with the group the pass would
// write to the recorded golden. The pass makes that compare for every gang
// on every pass over a Component at rest, and there its answer is always
// unchanged.
func TestOwnedFieldsCompareStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "owned_fields_compare_allocations.json"), *updateOwnedFieldsCompareGoldens)
	defer golden.Close()
	desired := groupThePassWouldWrite(t)
	existing := desired.DeepCopy()
	if drifted(desired, existing) {
		t.Errorf("the group the pass would write reads as drifted from its own copy")
	}
	compare := func() { compareSink = drifted(desired, existing) }
	golden.Check("one-gang/allocations", compare)
	golden.CheckBytes("one-gang/bytes", compare)
}

// BenchmarkOwnedFieldsCompare is the wall time and the allocations of one
// compare of a gang's group with the group the pass would write.
func BenchmarkOwnedFieldsCompare(b *testing.B) {
	desired := groupThePassWouldWrite(b)
	existing := desired.DeepCopy()
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		compareSink = drifted(desired, existing)
	}
}
