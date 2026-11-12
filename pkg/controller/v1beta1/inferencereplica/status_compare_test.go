package inferencereplica

import (
	"context"
	"flag"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

var updateStatusCompareGoldens = flag.Bool("update-status-compare", false, "re-record testdata/status_compare_allocations.json from this run")

// metaTimeType is the one struct of a row the walk below treats as a leaf:
// its fields are unexported, and the semantic compare reads it by instant.
var metaTimeType = reflect.TypeOf(metav1.Time{})

// rowWithEveryFieldSet is a row with a value in every field, every pointer
// set and every slice holding one element, so a walk over it reaches every
// leaf the row type has.
func rowWithEveryFieldSet() v1beta1.OMENativeInstanceStatus {
	at := metav1.NewTime(time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC))
	exitCode, surgeIndex := int32(137), int32(7)
	return v1beta1.OMENativeInstanceStatus{
		Index: 3, Incarnation: 2, Phase: v1beta1.OMENativeInstanceReady,
		RunningRevision: "engine-aaaa", TargetRevision: "engine-bbbb",
		PodCount: 8, ReadyPodCount: 8, ServingPodCount: 8, AvailablePodCount: 8, ScheduledPodCount: 8,
		Admitted:      true,
		NodesOccupied: []string{"node-1"},
		Conditions: []metav1.Condition{{
			Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 4,
			LastTransitionTime: at, Reason: "Serving", Message: "every pod serves",
		}},
		ReadySince: &at,
		Operation: &v1beta1.InstanceOperation{
			ID: "op-1", Type: v1beta1.InstanceOperationUpdate, Step: "Surge",
			StartedAt: at, LastProgressAt: at, Deadline: at, RetryCount: 1,
			TargetRevision: "engine-bbbb", Reason: "push", Waiting: "capacity",
			CapacityRefusedAt: &at, PlacementWaitSince: &at, Strategy: "SurgeThenDrain",
			SurgeIndex: &surgeIndex, FromNode: "node-1", HintTargetNodes: []string{"node-2"},
			RequestUUID: "request-1",
		},
		ActiveOrdinal: 1,
		LastFailure: &v1beta1.InstanceTermination{
			PodName: "engine-3-0", ContainerName: "engine", Reason: "OOMKilled",
			ExitCode: &exitCode, Message: "out of memory", Time: at,
		},
		Announced: []string{"engine-3"},
	}
}

// statusOfRows is a status holding rows and nothing else.
func statusOfRows(rows ...v1beta1.OMENativeInstanceStatus) *v1beta1.InferenceReplicaStatus {
	return &v1beta1.InferenceReplicaStatus{InstanceStatuses: rows}
}

// rowLeaf reads the value at one path of a row.
type rowLeaf func(*v1beta1.OMENativeInstanceStatus) reflect.Value

// leafChange is the change the walk makes to a leaf of a given kind: a
// scalar moved, a string lengthened, a time moved by a second. Nil for a
// kind the walk descends into instead.
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
	case reflect.Struct:
		if value.Type() != metaTimeType {
			return nil
		}
		return func(v reflect.Value) {
			moved := metav1.NewTime(v.Interface().(metav1.Time).Add(time.Second))
			v.Set(reflect.ValueOf(moved))
		}
	default:
		return nil
	}
}

// forEachLeafChange calls visit once per leaf of row with a copy of row
// that differs from it in that leaf alone: a scalar moved, a time moved by
// a second, a nil pointer or an empty slice filled, a set pointer cleared,
// a slice emptied or grown by one. Every such copy reads as different under
// the semantic compare, and a field of a kind the walk does not know fails
// the test, so a field added to a row is walked or named here.
func forEachLeafChange(t *testing.T, row v1beta1.OMENativeInstanceStatus, visit func(path string, changed v1beta1.OMENativeInstanceStatus)) {
	t.Helper()
	var walk func(path string, at rowLeaf)
	walk = func(path string, at rowLeaf) {
		change := func(suffix string, mutate func(reflect.Value)) {
			changed := *row.DeepCopy()
			mutate(at(&changed))
			visit(path+suffix, changed)
		}
		value := at(&row)
		if mutate := leafChange(value); mutate != nil {
			change("", mutate)
			return
		}
		switch value.Kind() {
		case reflect.Struct:
			for i := 0; i < value.NumField(); i++ {
				walk(path+"."+value.Type().Field(i).Name, func(r *v1beta1.OMENativeInstanceStatus) reflect.Value { return at(r).Field(i) })
			}
		case reflect.Pointer:
			if value.IsNil() {
				change("", func(v reflect.Value) { v.Set(reflect.New(v.Type().Elem())) })
				return
			}
			change("=nil", func(v reflect.Value) { v.Set(reflect.Zero(v.Type())) })
			walk(path, func(r *v1beta1.OMENativeInstanceStatus) reflect.Value { return at(r).Elem() })
		case reflect.Slice:
			if value.Len() == 0 {
				change("", func(v reflect.Value) { v.Set(reflect.MakeSlice(v.Type(), 1, 1)) })
				return
			}
			change("=nil", func(v reflect.Value) { v.Set(reflect.Zero(v.Type())) })
			change("+1", func(v reflect.Value) { v.Set(reflect.Append(v, reflect.Zero(v.Type().Elem()))) })
			walk(path+"[0]", func(r *v1beta1.OMENativeInstanceStatus) reflect.Value { return at(r).Index(0) })
		default:
			t.Fatalf("%s: a field of kind %s has no change in this walk; add one", path, value.Kind())
		}
	}
	walk("row", func(r *v1beta1.OMENativeInstanceStatus) reflect.Value { return reflect.ValueOf(r).Elem() })
}

// The compare answers as the semantic compare answers on every leaf of a
// row: a copy that differs in one leaf reads as different, and a row reads
// as equal to its own copy. Both a row with every field set and an empty
// one are walked, so every fill and every clearing is read.
func TestPublishedStatusEqual_AgreesWithTheSemanticCompareOnEveryRowField(t *testing.T) {
	for _, base := range []struct {
		name string
		row  v1beta1.OMENativeInstanceStatus
	}{
		{name: "every field set", row: rowWithEveryFieldSet()},
		{name: "every field empty", row: v1beta1.OMENativeInstanceStatus{}},
	} {
		t.Run(base.name, func(t *testing.T) {
			leaves := 0
			forEachLeafChange(t, base.row, func(path string, changed v1beta1.OMENativeInstanceStatus) {
				leaves++
				want := equality.Semantic.DeepEqual(base.row, changed)
				if got := publishedStatusEqual(statusOfRows(base.row), statusOfRows(changed)); got != want {
					t.Errorf("%s changed: publishedStatusEqual() = %v, the semantic compare says %v", path, got, want)
				}
				if want {
					t.Errorf("%s changed: the semantic compare reads the changed row as equal; the walk's change is not one", path)
				}
				if !publishedStatusEqual(statusOfRows(changed), statusOfRows(*changed.DeepCopy())) {
					t.Errorf("%s changed: the changed row and its copy read as different", path)
				}
			})
			if fields := reflect.TypeOf(base.row).NumField(); leaves < fields {
				t.Errorf("the walk changed %d leaves of a row with %d fields; every field has at least one leaf", leaves, fields)
			}
			t.Logf("%d leaves changed, one at a time, over a row of %d fields", leaves, reflect.TypeOf(base.row).NumField())
		})
	}
}

// The semantic compare reads a nil slice and an empty one as the same, and
// two times at one instant as the same whatever their location or
// monotonic reading; the compare agrees.
func TestPublishedStatusEqual_ReadsNilAndEmptyAndOneInstantAsEqual(t *testing.T) {
	instant := time.Date(2026, 11, 5, 12, 0, 0, 0, time.UTC)
	elsewhere := instant.In(time.FixedZone("east", 3600))
	withMonotonic := time.Now()
	a, b := rowWithEveryFieldSet(), rowWithEveryFieldSet()
	a.NodesOccupied, b.NodesOccupied = nil, []string{}
	a.Announced, b.Announced = []string{}, nil
	a.Conditions, b.Conditions = nil, []metav1.Condition{}
	a.Operation.HintTargetNodes, b.Operation.HintTargetNodes = nil, []string{}
	a.ReadySince, b.ReadySince = &metav1.Time{Time: instant}, &metav1.Time{Time: elsewhere}
	a.Operation.StartedAt, b.Operation.StartedAt = metav1.Time{Time: withMonotonic}, metav1.Time{Time: withMonotonic.Round(0)}
	a.LastFailure.Time, b.LastFailure.Time = metav1.Time{Time: elsewhere}, metav1.Time{Time: instant}
	left, right := statusOfRows(a), statusOfRows(b)
	left.RetryBlocks, right.RetryBlocks = nil, []v1beta1.RetryBlock{}
	if !equality.Semantic.DeepEqual(*left, *right) {
		t.Fatalf("the semantic compare reads the two statuses as different; the fixture is wrong")
	}
	if !publishedStatusEqual(left, right) {
		t.Errorf("publishedStatusEqual() = false over nil against empty slices and times at one instant; the semantic compare says true")
	}
	if !publishedStatusEqual(statusOfRows(), &v1beta1.InferenceReplicaStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{}}) {
		t.Errorf("publishedStatusEqual() = false over no rows against an empty row slice; the semantic compare says true")
	}
}

// The fields of a status beside its rows decide the compare as well: a
// counter, a condition or the hold that differs reads as different, and
// the row count does.
func TestPublishedStatusEqual_TheStatusBesideTheRowsDecidesToo(t *testing.T) {
	base := func() *v1beta1.InferenceReplicaStatus {
		s := statusOfRows(rowWithEveryFieldSet())
		s.Replicas, s.ReadyReplicas = 1, 1
		s.Conditions = []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "EveryRowReady"}}
		s.RolloutHold = &v1beta1.RolloutHold{Reason: "none"}
		return s
	}
	for name, change := range map[string]func(*v1beta1.InferenceReplicaStatus){
		"a counter":        func(s *v1beta1.InferenceReplicaStatus) { s.ReadyReplicas = 0 },
		"a condition":      func(s *v1beta1.InferenceReplicaStatus) { s.Conditions[0].Status = metav1.ConditionFalse },
		"the hold":         func(s *v1beta1.InferenceReplicaStatus) { s.RolloutHold.Reason = "paused" },
		"the hold cleared": func(s *v1beta1.InferenceReplicaStatus) { s.RolloutHold = nil },
		"a row removed":    func(s *v1beta1.InferenceReplicaStatus) { s.InstanceStatuses = nil },
		"a row added": func(s *v1beta1.InferenceReplicaStatus) {
			s.InstanceStatuses = append(s.InstanceStatuses, rowWithEveryFieldSet())
		},
	} {
		changed := base()
		change(changed)
		if publishedStatusEqual(base(), changed) {
			t.Errorf("%s changed: publishedStatusEqual() = true", name)
		}
	}
	if !publishedStatusEqual(base(), base()) {
		t.Errorf("two statuses built alike read as different")
	}
}

// statusCompareFixture is the pass's copy of a replica at rest and the
// status the write recomputes from it, built as the write builds them: the
// two statuses the write compares before it decides that the live object
// needs no write.
type statusCompareFixture struct {
	passCopy *v1beta1.InferenceReplicaStatus
	trial    *v1beta1.InferenceReplicaStatus
}

// publicationInput is the status write's input over the fixture's pods and
// plan, as aggregateAndWriteStatus builds it.
func (f *statusPublicationFixture) publicationInput(tb testing.TB) statusPublication {
	tb.Helper()
	ctx := context.Background()
	component := v1beta1convert.ComponentTypeToWorkload(f.passCopy.Spec.Component)
	pods, _, err := query.SplitPodsOnMissingNodes(ctx, everyNodePresent{}, f.pods)
	if err != nil {
		tb.Fatalf("read the nodes under the pods: %v", err)
	}
	availableByPod, err := workloadstatus.AvailablePodSet(ctx, f.reconciler.Client, f.passCopy.Namespace, query.HeadlessServiceName(f.passCopy.NamePrefix(), component))
	if err != nil {
		tb.Fatalf("compute availability: %v", err)
	}
	return statusPublication{
		plan:             f.plan,
		target:           f.target,
		desiredByIdx:     workloadstatus.DesiredPodCountByInstance(f.plan),
		pods:             workload.NewCachedSelectorPodObservation(pods, query.BucketPodsByInstanceIdx(pods)),
		availableByPod:   availableByPod,
		templateHash:     templateHashOf(f.passCopy),
		instancesAtStart: f.instancesAtStart,
	}
}

// newStatusCompareFixture recomputes the published fields over the resting
// replica's copy, as the status write does on its copy path, and keeps the
// copy and the recomputed status; the two are equal, since the replica is
// at rest.
func newStatusCompareFixture(tb testing.TB, shape statusPublicationShape) statusCompareFixture {
	tb.Helper()
	f := newStatusPublicationFixture(tb, shape)
	publication := f.publicationInput(tb)
	irstatus.ClearPodDerivedObservations(f.passCopy.Status.InstanceStatuses)
	trial := *f.passCopy
	trial.Status = *f.passCopy.Status.DeepCopy()
	if _, _, err := f.reconciler.publish(&trial, publication); err != nil {
		tb.Fatalf("%s: publish: %v", shape.name, err)
	}
	if !equality.Semantic.DeepEqual(f.passCopy.Status, trial.Status) {
		tb.Fatalf("%s: the recomputed status differs from the copy; the replica is not at rest", shape.name)
	}
	return statusCompareFixture{passCopy: &f.passCopy.Status, trial: &trial.Status}
}

// compareSink keeps the compare's answer, so the measured call is made.
var compareSink bool

func (f statusCompareFixture) compare() { compareSink = publishedStatusEqual(f.passCopy, f.trial) }

// BenchmarkStatusCompareAtRest is the cost of the compare the status write
// makes at the end of one pass over a replica at rest, per shape.
func BenchmarkStatusCompareAtRest(b *testing.B) {
	for _, shape := range statusPublicationShapes() {
		b.Run(shape.name, func(b *testing.B) {
			f := newStatusCompareFixture(b, shape)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				f.compare()
			}
		})
	}
}

// TestStatusCompareAtRestStaysWithinBudget holds the allocations and the
// bytes of the compare the status write makes over a replica at rest, at
// 1,000 and 4,000 single-pod Instances and at 160 gangs of eight, to the
// recorded golden. The compare answers equal at every shape: the replica
// is at rest.
func TestStatusCompareAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "status_compare_allocations.json"), *updateStatusCompareGoldens)
	defer golden.Close()
	for _, shape := range statusPublicationShapes() {
		f := newStatusCompareFixture(t, shape)
		if !publishedStatusEqual(f.passCopy, f.trial) {
			t.Errorf("%s: the compare reads the copy and the recomputed status as different", shape.name)
		}
		golden.Check(shape.name+"/allocations", f.compare)
		golden.CheckBytes(shape.name+"/bytes", f.compare)
	}
}
