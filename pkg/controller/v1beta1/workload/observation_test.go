package workload_test

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestPublicationObservationReusesOwnedRowsWithoutMutatingSource(t *testing.T) {
	operation := &types.InstanceOperation{ID: "operation-4", Type: types.InstanceOperationUpdate, Step: "Drain"}
	persisted := []types.InstanceStatus{
		{
			Index: 4, Incarnation: 3, Phase: types.InstancePhaseUpdating,
			RunningRevision: "revision-a", TargetRevision: "revision-b",
			PodCount: 8, ReadyPodCount: 7, ServingPodCount: 6, AvailablePodCount: 5,
			ScheduledPodCount: 8, Admitted: true, NodesOccupied: []string{"old-node"},
			Operation: operation,
		},
		{
			Index: 9, Incarnation: 2, Phase: types.InstancePhaseRestarting,
			PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, AvailablePodCount: 1,
			ScheduledPodCount: 1, Admitted: true, NodesOccupied: []string{"stale-node"},
		},
		{
			Index: 4, Incarnation: 4, Phase: types.InstancePhaseMigrating,
			RunningRevision: "revision-c", NodesOccupied: []string{"other-old-node"},
		},
	}
	apiPersisted := v1beta1convert.InstanceStatusSliceFromWorkload(persisted)
	wantAPI := v1beta1convert.InstanceStatusSliceFromWorkload(
		v1beta1convert.InstanceStatusSliceToWorkload(apiPersisted),
	)
	owned := v1beta1convert.InstanceStatusSliceToWorkload(apiPersisted)
	ownedBacking := &owned[0]

	podA := observationPod("pod-a", "node-b", true, true, false)
	podB := observationPod("pod-b", "node-a", true, false, true)
	podC := observationPod("pod-c", "", false, false, false)
	byInstance := map[int32][]*corev1.Pod{
		4:  {podA, podB, podC},
		99: {observationPod("orphan-row-pod", "node-z", true, true, false)},
	}
	observation, err := workload.NewOwnedPublicationObservation(
		owned,
		workload.NewCachedSelectorPodObservation(nil, byInstance),
		map[string]struct{}{podA.Name: {}}, status.AvailabilityWindow{},
	)
	if err != nil {
		t.Fatalf("NewOwnedPublicationObservation: %v", err)
	}
	if !reflect.DeepEqual(observation.PersistedStatuses(), persisted) {
		t.Fatal("publication observation did not retain the durable rows")
	}

	got, err := observation.TakeInlineV1Statuses()
	if err != nil {
		t.Fatalf("TakeInlineV1Statuses: %v", err)
	}
	if &got[0] != ownedBacking {
		t.Fatal("publication materialization allocated another status row slice")
	}
	if len(got) != 3 {
		t.Fatalf("status rows = %d, want 3", len(got))
	}
	if got[0].Index != 4 || got[1].Index != 9 || got[2].Index != 4 {
		t.Fatalf("row order = [%d %d %d], want [4 9 4]", got[0].Index, got[1].Index, got[2].Index)
	}

	for _, row := range []types.InstanceStatus{got[0], got[2]} {
		if row.PodCount != 3 || row.ReadyPodCount != 2 || row.ServingPodCount != 1 ||
			row.AvailablePodCount != 1 || row.ScheduledPodCount != 2 || row.Admitted {
			t.Fatalf("index 4 current fields = %+v", row)
		}
		if !reflect.DeepEqual(row.NodesOccupied, []string{"node-a", "node-b"}) {
			t.Fatalf("index 4 nodes = %v, want [node-a node-b]", row.NodesOccupied)
		}
	}
	if got[0].Operation == nil || got[0].Operation.ID != operation.ID || got[0].Phase != types.InstancePhaseUpdating {
		t.Fatalf("lifecycle fields were not preserved: %+v", got[0])
	}
	if got[1].PodCount != 0 || got[1].ReadyPodCount != 0 || got[1].ScheduledPodCount != 0 ||
		got[1].Admitted || got[1].NodesOccupied != nil {
		t.Fatalf("empty index current fields = %+v", got[1])
	}
	if !reflect.DeepEqual(apiPersisted, wantAPI) {
		t.Fatalf("source CRD rows mutated\n got: %#v\nwant: %#v", apiPersisted, wantAPI)
	}
	current, availabilityObserved := observation.CurrentCounters(4)
	if !availabilityObserved || current.AvailablePodCount != 1 {
		t.Fatalf("publication availability = current %+v observed %t", current, availabilityObserved)
	}
	extra, _ := observation.CurrentCounters(99)
	if extra.PodCount != 1 || len(got) != len(persisted) {
		t.Fatalf("Pod-only index changed row membership: current %+v rows %d", extra, len(got))
	}
	if _, err := observation.TakeInlineV1Statuses(); err == nil {
		t.Fatal("publication observation was consumed twice")
	}
	if _, _, err := observation.TakeInlineV1Publication(nil, ""); err == nil {
		t.Fatal("publication counters accepted a consumed observation")
	}
}

func TestComponentObservationProvenanceAndEpochGuard(t *testing.T) {
	persisted := []types.InstanceStatus{{Index: 0, AvailablePodCount: 7}}
	observation, err := workload.NewDecisionObservation(
		persisted,
		workload.NewAPIReaderSelectorPodObservation(nil, map[int32][]*corev1.Pod{}),
	)
	if err != nil {
		t.Fatalf("NewDecisionObservation: %v", err)
	}

	if observation.Epoch() != workload.ObservationEpochDecision ||
		observation.PodSource() != workload.PodObservationSourceAPIReader ||
		observation.PodScope() != workload.PodObservationScopeSelector {
		t.Fatalf("unexpected provenance: epoch=%v source=%v scope=%v",
			observation.Epoch(), observation.PodSource(), observation.PodScope())
	}
	current, availabilityObserved := observation.CurrentCounters(0)
	if availabilityObserved {
		t.Fatal("decision observation must not claim EndpointSlice availability")
	}
	if current.AvailablePodCount != 0 || observation.PersistedStatuses()[0].AvailablePodCount != 7 {
		t.Fatalf("persisted and current availability were conflated: current %+v persisted %+v",
			current, observation.PersistedStatuses()[0])
	}
	if _, err := observation.InlineV1Statuses(); err == nil {
		t.Fatal("decision observation materialized publication rows")
	}
	if _, _, err := observation.TakeInlineV1Publication(nil, ""); err == nil {
		t.Fatal("decision observation produced publication counters")
	}
	if !reflect.DeepEqual(observation.PersistedStatuses(), persisted) {
		t.Fatalf("persisted status projection changed")
	}
	if _, err := workload.NewDecisionObservation(persisted, workload.PodObservation{}); err == nil {
		t.Fatal("decision observation accepted unknown Pod provenance")
	}
}

func TestPublicationObservationRejectsUnsupportedProvenance(t *testing.T) {
	tests := []struct {
		name string
		pods workload.PodObservation
	}{
		{name: "unknown", pods: workload.PodObservation{}},
		{name: "API reader selector", pods: workload.NewAPIReaderSelectorPodObservation(nil, nil)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := workload.NewOwnedPublicationObservation(nil, test.pods, nil, status.AvailabilityWindow{}); err == nil {
				t.Fatal("publication observation accepted unsupported provenance")
			}
		})
	}
}

func TestPublicationObservationCopiesNestedDurableState(t *testing.T) {
	surgeIndex := int32(9)
	exitCode := int32(143)
	owned := []types.InstanceStatus{{
		Index:         2,
		NodesOccupied: []string{"node-a"},
		Conditions:    []metav1.Condition{{Type: "Ready", Message: "original"}},
		Operation: &types.InstanceOperation{
			ID: "operation-2", SurgeIndex: &surgeIndex, HintTargetNodes: []string{"node-b"},
		},
		LastFailure: &types.InstanceTermination{PodName: "pod-2", ExitCode: &exitCode},
	}}
	observation, err := workload.NewOwnedPublicationObservation(
		owned,
		workload.NewCachedSelectorPodObservation(nil, nil),
		nil, status.AvailabilityWindow{},
	)
	if err != nil {
		t.Fatalf("NewOwnedPublicationObservation: %v", err)
	}

	first, err := observation.InlineV1Statuses()
	if err != nil {
		t.Fatalf("InlineV1Statuses: %v", err)
	}
	first[0].NodesOccupied = []string{"mutated"}
	first[0].Conditions[0].Message = "mutated"
	first[0].Operation.HintTargetNodes[0] = "mutated"
	*first[0].Operation.SurgeIndex = 99
	*first[0].LastFailure.ExitCode = 1

	second, err := observation.InlineV1Statuses()
	if err != nil {
		t.Fatalf("InlineV1Statuses after output mutation: %v", err)
	}
	if second[0].Conditions[0].Message != "original" ||
		second[0].Operation.HintTargetNodes[0] != "node-b" ||
		*second[0].Operation.SurgeIndex != 9 ||
		*second[0].LastFailure.ExitCode != 143 {
		t.Fatalf("materialized rows alias durable state: %+v", second[0])
	}
	persisted := observation.PersistedStatuses()
	persisted[0].NodesOccupied[0] = "mutated-again"
	persisted[0].Conditions[0].Message = "mutated-again"
	again := observation.PersistedStatuses()[0]
	if again.NodesOccupied[0] != "node-a" || again.Conditions[0].Message != "original" {
		t.Fatal("PersistedStatuses returned aliased nested state")
	}
}

func TestPublicationObservationPreservesNilStatusSlice(t *testing.T) {
	observation, err := workload.NewOwnedPublicationObservation(
		nil,
		workload.NewCachedSelectorPodObservation(nil, nil),
		nil, status.AvailabilityWindow{},
	)
	if err != nil {
		t.Fatalf("NewOwnedPublicationObservation: %v", err)
	}
	got, err := observation.TakeInlineV1Statuses()
	if err != nil {
		t.Fatalf("InlineV1Statuses: %v", err)
	}
	if got != nil {
		t.Fatalf("nil persisted rows materialized as %#v", got)
	}
}

var benchmarkObservationRows []types.InstanceStatus
var benchmarkComponentCounters workload.ComponentCounters

func BenchmarkPublicationObservationMaterialization(b *testing.B) {
	for _, podsPerInstance := range []int{1, 8} {
		persisted := make([]types.InstanceStatus, 2000)
		byInstance := make(map[int32][]*corev1.Pod, len(persisted))
		available := make(map[string]struct{}, len(persisted)*podsPerInstance)
		desired := make(map[int32]int32, len(persisted))
		for index := range persisted {
			persisted[index] = types.InstanceStatus{
				Index: int32(index), Incarnation: 1, Phase: types.InstancePhaseReady,
				RunningRevision: "revision-a",
			}
			desired[int32(index)] = int32(podsPerInstance)
			for ordinal := 0; ordinal < podsPerInstance; ordinal++ {
				name := fmt.Sprintf("pod-%04d-%02d", index, ordinal)
				pod := observationPod(name, fmt.Sprintf("node-%02d", ordinal), true, true, false)
				byInstance[int32(index)] = append(byInstance[int32(index)], pod)
				available[name] = struct{}{}
			}
		}
		pods := workload.NewCachedSelectorPodObservation(nil, byInstance)

		b.Run(fmt.Sprintf("2000x%d/owned", podsPerInstance), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				owned := append([]types.InstanceStatus(nil), persisted...)
				observation, err := workload.NewOwnedPublicationObservation(owned, pods, available, status.AvailabilityWindow{})
				if err != nil {
					b.Fatal(err)
				}
				rows, err := observation.TakeInlineV1Statuses()
				if err != nil {
					b.Fatal(err)
				}
				benchmarkObservationRows = rows
			}
		})

		b.Run(fmt.Sprintf("2000x%d/owned-with-counters", podsPerInstance), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				owned := append([]types.InstanceStatus(nil), persisted...)
				observation, err := workload.NewOwnedPublicationObservation(owned, pods, available, status.AvailabilityWindow{})
				if err != nil {
					b.Fatal(err)
				}
				rows, counters, err := observation.TakeInlineV1Publication(desired, "revision-a")
				if err != nil {
					b.Fatal(err)
				}
				benchmarkObservationRows = rows
				benchmarkComponentCounters = counters
			}
		})

		b.Run(fmt.Sprintf("2000x%d/legacy-owned-with-counters", podsPerInstance), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				owned := append([]types.InstanceStatus(nil), persisted...)
				observation, err := workload.NewOwnedPublicationObservation(owned, pods, available, status.AvailabilityWindow{})
				if err != nil {
					b.Fatal(err)
				}
				rows, err := observation.TakeInlineV1Statuses()
				if err != nil {
					b.Fatal(err)
				}
				benchmarkObservationRows = rows
				benchmarkComponentCounters = legacyObservationComponentCounters(rows, desired, "revision-a")
			}
		})

		b.Run(fmt.Sprintf("2000x%d/copy", podsPerInstance), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				owned := append([]types.InstanceStatus(nil), persisted...)
				observation, err := workload.NewOwnedPublicationObservation(owned, pods, available, status.AvailabilityWindow{})
				if err != nil {
					b.Fatal(err)
				}
				rows, err := observation.InlineV1Statuses()
				if err != nil {
					b.Fatal(err)
				}
				benchmarkObservationRows = rows
			}
		})
	}
}

func legacyObservationComponentCounters(instances []types.InstanceStatus, desiredByIdx map[int32]int32, targetRevision string) workload.ComponentCounters {
	counters := workload.ComponentCounters{Replicas: int32(len(instances))}
	target := query.RevisionFromName(targetRevision)
	for _, instance := range instances {
		desired := status.DesiredFor(desiredByIdx, instance.Index, instance.PodCount)
		if status.InstanceMeetsThreshold(instance.PodCount, instance.ReadyPodCount, desired) {
			counters.ReadyReplicas++
		}
		if status.InstanceMeetsThreshold(instance.PodCount, instance.ServingPodCount, desired) {
			counters.ServingReplicas++
		}
		if status.InstanceMeetsThreshold(instance.PodCount, instance.AvailablePodCount, desired) {
			counters.AvailableReplicas++
		}
		if targetRevision != "" && query.RevisionFromName(instance.RunningRevision).Same(target) {
			counters.UpdatedReplicas++
			if status.InstanceMeetsThreshold(instance.PodCount, instance.ReadyPodCount, desired) {
				counters.UpdatedReadyReplicas++
			}
		}
	}
	return counters
}

func observationPod(name, node string, ready, serving, gated bool) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec:       corev1.PodSpec{NodeName: node},
	}
	if ready {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.ContainersReady, Status: corev1.ConditionTrue,
		})
	}
	if serving {
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: podreadiness.ConditionType, Status: corev1.ConditionTrue,
		})
	}
	if gated {
		pod.Spec.SchedulingGates = []corev1.PodSchedulingGate{{Name: "example-gate"}}
	}
	return pod
}

type removableFieldUse struct {
	file     string
	function string
	field    string
}

type removableFieldAccessCounts struct {
	reads      int
	writes     int
	readWrites int
}

type approvedRemovableFieldUse struct {
	counts removableFieldAccessCounts
	reason string
}

func TestRemovableObservationFieldsHaveNoUnauditedDirectProductionAccess(t *testing.T) {
	approved := map[removableFieldUse]approvedRemovableFieldUse{}
	approve := func(file, function string, counts removableFieldAccessCounts, reason string, fields ...string) {
		for _, field := range fields {
			approved[removableFieldUse{file: file, function: function, field: field}] = approvedRemovableFieldUse{counts: counts, reason: reason}
		}
	}
	read := removableFieldAccessCounts{reads: 1}
	write := removableFieldAccessCounts{writes: 1}
	readWrite := removableFieldAccessCounts{reads: 1, writes: 1}
	allFields := []string{"ReadyPodCount", "ScheduledPodCount", "NodesOccupied"}

	approve("pkg/alfred/engine/dispatch_preflight.go", "dispatchSourceFingerprint", write, "clear non-persisted Pod observations on an independent logical row before fingerprinting", allFields...)
	approve("pkg/controller/v1beta1/workload/observation.go", "overlayInlineV1", readWrite, "publication materialization", allFields...)
	approve("pkg/controller/v1beta1/workload/observation.go", "cloneInstanceStatus", removableFieldAccessCounts{reads: 2, writes: 1}, "isolated status copy", "NodesOccupied")
	approve("pkg/controller/v1beta1/workload/observation.go", "TakeInlineV1Publication", removableFieldAccessCounts{reads: 2}, "current Pod observation", "ReadyPodCount")
	approve("pkg/controller/v1beta1/workload/status/aggregate.go", "String", read, "current counter diagnostics", allFields...)
	for _, function := range []string{"InstanceStatusToWorkload", "InstanceStatusFromWorkload"} {
		approve("pkg/controller/v1beta1/v1beta1convert/convert.go", function, read, "wire conversion", "ReadyPodCount", "ScheduledPodCount")
		approve("pkg/controller/v1beta1/v1beta1convert/convert.go", function, removableFieldAccessCounts{reads: 2, writes: 1}, "wire conversion", "NodesOccupied")
	}
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "aggregateAndWriteStatus", readWrite, "transient publication materialization", allFields...)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "mirrorInstanceCounters", readWrite, "transient same-pass mirror", "ReadyPodCount", "ScheduledPodCount")
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "mirrorInstanceCounters", removableFieldAccessCounts{reads: 2, writes: 2}, "transient same-pass mirror", "NodesOccupied")
	approve("pkg/controller/v1beta1/irstatus/encoding.go", "ClearPodDerivedObservations", write, "status persistence boundary", allFields...)
	approve("pkg/controller/v1beta1/workload/status/precondition.go", "Clone", readWrite, "isolated status copy", "NodesOccupied")
	approve("pkg/controller/v1beta1/workload/status/create.go", "sameCreateTransitionOwnerState", removableFieldAccessCounts{writes: 2}, "rollback comparison normalization", allFields...)
	approve("pkg/controller/v1beta1/workload/status/create.go", "restoreCreateTransitionState", readWrite, "transient rollback observation preservation", "ReadyPodCount", "ScheduledPodCount")
	approve("pkg/controller/v1beta1/workload/status/create.go", "restoreCreateTransitionState", removableFieldAccessCounts{reads: 2, writes: 2}, "transient rollback observation preservation", "NodesOccupied")

	repoRoot := removableFieldAuditRepositoryRoot(t)
	fields := map[string]struct{}{
		"ReadyPodCount":     {},
		"ScheduledPodCount": {},
		"NodesOccupied":     {},
	}
	actual := make(map[removableFieldUse]removableFieldAccessCounts, len(approved))
	fset := token.NewFileSet()
	for _, productionRoot := range []string{"pkg", "cmd", "internal", "scheduler"} {
		root := filepath.Join(repoRoot, productionRoot)
		if _, statErr := os.Stat(root); os.IsNotExist(statErr) {
			continue
		}
		err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if entry.IsDir() || !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") || strings.HasPrefix(entry.Name(), "zz_generated.") {
				return nil
			}
			parsed, parseErr := parser.ParseFile(fset, path, nil, 0)
			if parseErr != nil {
				return parseErr
			}
			relative, relErr := filepath.Rel(repoRoot, path)
			if relErr != nil {
				return relErr
			}
			collectRemovableFieldAccesses(parsed, filepath.ToSlash(relative), fields, actual)
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	for use, got := range actual {
		want, ok := approved[use]
		if !ok {
			t.Errorf("unaudited direct access to %s in %s:%s: %+v", use.field, use.file, use.function, got)
			continue
		}
		if got != want.counts {
			t.Errorf("direct-access count changed for %s in %s:%s: got %+v, want %+v (%s)", use.field, use.file, use.function, got, want.counts, want.reason)
		}
	}
	for use, want := range approved {
		if _, ok := actual[use]; !ok {
			t.Errorf("stale direct-access approval for %s in %s:%s (%s)", use.field, use.file, use.function, want.reason)
		}
	}
}

func removableFieldAuditRepositoryRoot(t *testing.T) string {
	t.Helper()
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve audit test source path")
	}
	var starts []string
	if filepath.IsAbs(source) {
		starts = append(starts, filepath.Dir(source))
	}
	if workingDir, err := os.Getwd(); err == nil {
		starts = append(starts, workingDir)
	}
	for _, start := range starts {
		for directory := filepath.Clean(start); ; directory = filepath.Dir(directory) {
			if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
				return directory
			}
			parent := filepath.Dir(directory)
			if parent == directory {
				break
			}
		}
	}
	t.Fatalf("resolve repository root from source %s", source)
	return ""
}

func collectRemovableFieldAccesses(parsed *ast.File, file string, fields map[string]struct{}, actual map[removableFieldUse]removableFieldAccessCounts) {
	parents := make(map[ast.Node]ast.Node)
	stack := make([]ast.Node, 0, 16)
	ast.Inspect(parsed, func(node ast.Node) bool {
		if node == nil {
			stack = stack[:len(stack)-1]
			return false
		}
		if len(stack) > 0 {
			parents[node] = stack[len(stack)-1]
		}
		stack = append(stack, node)
		return true
	})

	var functions []*ast.FuncDecl
	for _, declaration := range parsed.Decls {
		if function, ok := declaration.(*ast.FuncDecl); ok {
			functions = append(functions, function)
		}
	}
	ast.Inspect(parsed, func(node ast.Node) bool {
		selector, ok := node.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		if _, tracked := fields[selector.Sel.Name]; !tracked {
			return true
		}
		functionName := "<package>"
		for _, function := range functions {
			if function.Pos() <= selector.Pos() && selector.End() <= function.End() {
				functionName = function.Name.Name
				break
			}
		}
		use := removableFieldUse{file: file, function: functionName, field: selector.Sel.Name}
		counts := actual[use]
		switch removableFieldSelectorAccess(selector, parents) {
		case "write":
			counts.writes++
		case "read-write":
			counts.readWrites++
		default:
			counts.reads++
		}
		actual[use] = counts
		return true
	})
}

func removableFieldSelectorAccess(selector *ast.SelectorExpr, parents map[ast.Node]ast.Node) string {
	original := ast.Node(selector)
	child := original
	for parent := parents[child]; parent != nil; child, parent = parent, parents[parent] {
		switch node := parent.(type) {
		case *ast.AssignStmt:
			for _, left := range node.Lhs {
				if left != child {
					continue
				}
				if child != original || (node.Tok != token.ASSIGN && node.Tok != token.DEFINE) {
					return "read-write"
				}
				return "write"
			}
			return "read"
		case *ast.IncDecStmt:
			return "read-write"
		case *ast.RangeStmt:
			if node.Key == child || node.Value == child {
				return "write"
			}
		case *ast.UnaryExpr:
			if node.Op == token.AND && node.X == child {
				return "read-write"
			}
		case *ast.FuncDecl, *ast.FuncLit:
			return "read"
		}
	}
	return "read"
}

func TestTakeInlineV1PublicationCountersIgnorePersistedObservationFields(t *testing.T) {
	persisted := []types.InstanceStatus{
		{Index: 0, PodCount: 1, ReadyPodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, RunningRevision: "rev-a"},
		{Index: 1, PodCount: 1, ReadyPodCount: 0, ServingPodCount: 0, AvailablePodCount: 0, RunningRevision: "rev-b"},
	}
	desired := map[int32]int32{0: 1, 1: 1}

	tests := []struct {
		name   string
		target string
	}{
		{name: "without target revision"},
		{name: "with target revision", target: "rev-a"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation, err := workload.NewOwnedPublicationObservation(
				append([]types.InstanceStatus(nil), persisted...),
				workload.NewCachedSelectorPodObservation(nil, nil),
				nil,
				status.AvailabilityWindow{},
			)
			if err != nil {
				t.Fatalf("NewOwnedPublicationObservation: %v", err)
			}
			statuses, got, err := observation.TakeInlineV1Publication(desired, test.target)
			if err != nil {
				t.Fatalf("TakeInlineV1Publication: %v", err)
			}
			if got.Replicas != 2 || got.ReadyReplicas != 0 || got.ServingReplicas != 0 || got.AvailableReplicas != 0 {
				t.Errorf("current counters must come from the empty Pod observation: %+v", got)
			}
			if test.target == "" && (got.UpdatedReplicas != 0 || got.UpdatedReadyReplicas != 0) {
				t.Errorf("Updated* should be zero with empty target: %+v", got)
			}
			if test.target != "" && (got.UpdatedReplicas != 1 || got.UpdatedReadyReplicas != 0) {
				t.Errorf("rev-a Updated*: got (%d,%d) want (1,0)", got.UpdatedReplicas, got.UpdatedReadyReplicas)
			}
			for _, status := range statuses {
				if status.PodCount != 0 || status.ReadyPodCount != 0 || status.ServingPodCount != 0 || status.AvailablePodCount != 0 {
					t.Errorf("status %d retained stale observation fields: %+v", status.Index, status)
				}
			}
		})
	}
}

// A row whose in-flight Update pins a revision the Component no longer
// targets still owes a roll, so it is not updated even when it happens to
// run the target: the surge under way promotes onto the pinned revision
// first. Publishing it as updated would claim a convergence the Component
// has not reached and idle a rollout that is still moving.
func TestTakeInlineV1PublicationUpdatedExcludesRowsPinnedOffTarget(t *testing.T) {
	surging := &types.InstanceOperation{Type: types.InstanceOperationUpdate, TargetRevision: "rev-b"}
	settling := &types.InstanceOperation{Type: types.InstanceOperationUpdate, TargetRevision: "rev-a"}
	persisted := []types.InstanceStatus{
		{Index: 0, RunningRevision: "rev-a"},
		{Index: 1, RunningRevision: "rev-a", Operation: surging},
		{Index: 2, RunningRevision: "rev-a", Operation: settling},
	}

	tests := []struct {
		name   string
		target string
		want   int32
	}{
		// The revert: the Component is back on rev-a, index 1 is
		// mid-surge toward the abandoned rev-b and index 2 is
		// finishing its roll onto rev-a.
		{name: "target is the running revision", target: "rev-a", want: 2},
		// index 1 runs rev-a, not the rev-b it is surging toward,
		// so the pin does not make it updated either.
		{name: "target is the pinned revision", target: "rev-b", want: 0},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			observation, err := workload.NewOwnedPublicationObservation(
				append([]types.InstanceStatus(nil), persisted...),
				workload.NewCachedSelectorPodObservation(nil, nil),
				nil,
				status.AvailabilityWindow{},
			)
			if err != nil {
				t.Fatalf("NewOwnedPublicationObservation: %v", err)
			}
			_, got, err := observation.TakeInlineV1Publication(map[int32]int32{0: 1, 1: 1, 2: 1}, test.target)
			if err != nil {
				t.Fatalf("TakeInlineV1Publication: %v", err)
			}
			if got.UpdatedReplicas != test.want {
				t.Errorf("UpdatedReplicas: got %d want %d", got.UpdatedReplicas, test.want)
			}
			if got.Replicas != 3 {
				t.Errorf("Replicas: got %d want 3", got.Replicas)
			}
		})
	}
}
