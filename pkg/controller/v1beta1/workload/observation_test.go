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
	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/irstatustest"
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
			Operation: &types.InstanceOperation{ID: "restart-9", Type: types.InstanceOperationRestart, Step: types.RestartStepDrain},
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
	if ready && serving {
		// The kubelet folds a satisfied gate into the pod's Ready condition.
		pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
			Type: corev1.PodReady, Status: corev1.ConditionTrue,
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
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "publishedRowsSettled", removableFieldAccessCounts{reads: 1}, "the status write's decision on the pass's copy reads the three fields as cleared, the way the store holds them", allFields...)
	approve("pkg/controller/v1beta1/inferencereplica/status_compare.go", "instanceStatusEqual", removableFieldAccessCounts{reads: 2}, "the status write's compare of the pass's copy with the recomputed status reads every row field", allFields...)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "mirrorInstanceCounters", readWrite, "transient same-pass mirror", "ReadyPodCount", "ScheduledPodCount")
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "mirrorInstanceCounters", readWrite, "transient same-pass mirror", "NodesOccupied")
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "indexPublication", read, "transient same-pass mirror: the publication's node names counted once to size the rows' lists", "NodesOccupied")
	approve("pkg/controller/v1beta1/irstatus/encoding.go", "ClearPodDerivedObservations", write, "status persistence boundary", allFields...)
	approve("pkg/controller/v1beta1/irstatus/densesize.go", "rowSize", removableFieldAccessCounts{reads: 2}, "status size counted over every row field", allFields...)
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

// A settled Ready row publishes against the Pod observation of its own
// pass. With no pods it publishes Pending, the phase the truth pass moves
// such a row to, beside the zero pod count, under every restart policy;
// the running revision stays on the row for the pass that rebuilds it. An
// observation that finds a Ready pod for it publishes Ready again. A pod
// that is present but serves nothing is the other reading, Restarting
// (TestTakeInlineV1PublicationReadsRestartingOverAPodSetNotServing).
func TestTakeInlineV1PublicationDemotesReadyRowWithoutPods(t *testing.T) {
	readySince := metav1.Unix(1700000000, 0)
	rows := func() []types.InstanceStatus {
		return []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince},
			{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince},
			{Index: 2, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-a", TargetRevision: "rev-b",
				Operation: &types.InstanceOperation{ID: "update-2", Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, TargetRevision: "rev-b"}},
		}
	}
	publish := func(t *testing.T, byInstance map[int32][]*corev1.Pod) ([]types.InstanceStatus, workload.ComponentCounters) {
		t.Helper()
		observation, err := workload.NewOwnedPublicationObservation(
			rows(), workload.NewCachedSelectorPodObservation(nil, byInstance), nil, status.AvailabilityWindow{})
		if err != nil {
			t.Fatalf("NewOwnedPublicationObservation: %v", err)
		}
		statuses, counters, err := observation.TakeInlineV1Publication(map[int32]int32{0: 1, 1: 1, 2: 1}, "rev-a")
		if err != nil {
			t.Fatalf("TakeInlineV1Publication: %v", err)
		}
		return statuses, counters
	}
	backed := observationPod("pod-0", "node-a", true, true, false)

	t.Run("no pods publishes Pending", func(t *testing.T) {
		statuses, counters := publish(t, map[int32][]*corev1.Pod{0: {backed}})
		if statuses[0].Phase != types.InstancePhaseReady {
			t.Errorf("backed row phase = %s, want Ready", statuses[0].Phase)
		}
		if statuses[1].Phase != types.InstancePhasePending || statuses[1].PodCount != 0 {
			t.Errorf("unbacked row = phase %s with %d pod(s), want Pending with none", statuses[1].Phase, statuses[1].PodCount)
		}
		if statuses[1].RunningRevision != "rev-a" {
			t.Errorf("unbacked row RunningRevision = %q, want rev-a kept for the pass that rebuilds it", statuses[1].RunningRevision)
		}
		if statuses[1].ReadySince == nil || !statuses[1].ReadySince.Equal(&readySince) {
			t.Errorf("unbacked row ReadySince = %v, want the anchor preserved", statuses[1].ReadySince)
		}
		if statuses[2].Phase != types.InstancePhaseUpdating {
			t.Errorf("operation-owned row phase = %s, want Updating", statuses[2].Phase)
		}
		if counters.Replicas != 3 || counters.ReadyReplicas != 1 {
			t.Errorf("counters = %+v, want 3 replicas with 1 ready", counters)
		}
	})

	t.Run("a pod that is not serving reads Restarting beside its count", func(t *testing.T) {
		starting := observationPod("pod-1", "", false, false, false)
		statuses, counters := publish(t, map[int32][]*corev1.Pod{0: {backed}, 1: {starting}})
		if statuses[1].Phase != types.InstancePhaseRestarting || statuses[1].PodCount != 1 {
			t.Errorf("row with a pod that serves nothing = phase %s with %d pod(s), want Restarting with one", statuses[1].Phase, statuses[1].PodCount)
		}
		if counters.ReadyReplicas != 1 {
			t.Errorf("ReadyReplicas = %d, want 1: readiness stays with the counters", counters.ReadyReplicas)
		}
	})

	t.Run("a terminal pod backs nothing and publishes Pending beside its count", func(t *testing.T) {
		refused := observationPod("pod-1", "node-b", false, false, false)
		refused.Status.Phase = corev1.PodFailed
		refused.Status.Reason = "OutOfcpu"
		statuses, counters := publish(t, map[int32][]*corev1.Pod{0: {backed}, 1: {refused}})
		if statuses[1].Phase != types.InstancePhasePending || statuses[1].PodCount != 1 {
			t.Errorf("row whose only pod is terminal = phase %s with %d pod(s), want Pending with the pod still counted", statuses[1].Phase, statuses[1].PodCount)
		}
		if statuses[1].RunningRevision != "rev-a" {
			t.Errorf("row RunningRevision = %q, want rev-a kept for the pass that rebuilds it", statuses[1].RunningRevision)
		}
		if counters.ReadyReplicas != 1 {
			t.Errorf("ReadyReplicas = %d, want 1", counters.ReadyReplicas)
		}
	})

	t.Run("a Ready pod publishes Ready again", func(t *testing.T) {
		replacement := observationPod("pod-1", "node-b", true, true, false)
		statuses, counters := publish(t, map[int32][]*corev1.Pod{0: {backed}, 1: {replacement}})
		if statuses[1].Phase != types.InstancePhaseReady || statuses[1].PodCount != 1 {
			t.Errorf("rebacked row = phase %s with %d pod(s), want Ready with one", statuses[1].Phase, statuses[1].PodCount)
		}
		if counters.ReadyReplicas != 2 {
			t.Errorf("ReadyReplicas = %d, want 2", counters.ReadyReplicas)
		}
	})
}

// A settled Ready row whose pods are present but serve nothing publishes
// Restarting beside its counters: the only pod of a promoted Instance
// crashed and the kubelet is restarting it, so no pod of the set is
// ContainersReady with the serving gate, the bar the promote path read.
// The row keeps its running revision and its ReadySince anchor, so the
// engine's own reading of the row does not move. It reads Ready again on
// its own once the set serves again. A pod serving on its containers
// before the kubelet folds the gate into PodReady is serving. A gang
// publishes against its whole set, as it was promoted. A row an
// operation owns keeps the operation's phase.
func TestTakeInlineV1PublicationReadsRestartingOverAPodSetNotServing(t *testing.T) {
	readySince := metav1.Unix(1700000000, 0)
	rows := func() []types.InstanceStatus {
		return []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince},
			{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince},
			{Index: 2, Incarnation: 1, Phase: types.InstancePhaseUpdating, RunningRevision: "rev-a", TargetRevision: "rev-b",
				Operation: &types.InstanceOperation{ID: "update-2", Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurge, TargetRevision: "rev-b"}},
		}
	}
	publish := func(t *testing.T, desired map[int32]int32, byInstance map[int32][]*corev1.Pod) []types.InstanceStatus {
		t.Helper()
		observation, err := workload.NewOwnedPublicationObservation(
			rows(), workload.NewCachedSelectorPodObservation(nil, byInstance), nil, status.AvailabilityWindow{})
		if err != nil {
			t.Fatalf("NewOwnedPublicationObservation: %v", err)
		}
		statuses, _, err := observation.TakeInlineV1Publication(desired, "rev-a")
		if err != nil {
			t.Fatalf("TakeInlineV1Publication: %v", err)
		}
		return statuses
	}
	single := map[int32]int32{0: 1, 1: 1, 2: 1}
	serving := observationPod("pod-0", "node-a", true, true, false)
	// The runner crashed: ContainersReady is gone while the serving gate
	// the controller wrote stays on the pod.
	crashed := observationPod("pod-1", "node-b", false, true, false)

	t.Run("a crashed only pod reads Restarting", func(t *testing.T) {
		statuses := publish(t, single, map[int32][]*corev1.Pod{0: {serving}, 1: {crashed}})
		row := statuses[1]
		if row.Phase != types.InstancePhaseRestarting || row.PodCount != 1 || row.ServingPodCount != 0 {
			t.Errorf("row = phase %s, %d pod(s), %d serving; want Restarting with one pod and none serving", row.Phase, row.PodCount, row.ServingPodCount)
		}
		if row.RunningRevision != "rev-a" || row.Operation != nil {
			t.Errorf("row = revision %q operation %+v, want rev-a kept and no operation", row.RunningRevision, row.Operation)
		}
		if row.ReadySince == nil || !row.ReadySince.Equal(&readySince) {
			t.Errorf("ReadySince = %v, want the anchor preserved", row.ReadySince)
		}
		if statuses[0].Phase != types.InstancePhaseReady {
			t.Errorf("serving row phase = %s, want Ready", statuses[0].Phase)
		}
	})

	t.Run("the set serving again reads Ready", func(t *testing.T) {
		statuses := publish(t, single, map[int32][]*corev1.Pod{0: {serving}, 1: {observationPod("pod-1", "node-b", true, true, false)}})
		if statuses[1].Phase != types.InstancePhaseReady || statuses[1].ServingPodCount != 1 {
			t.Errorf("row = phase %s with %d serving, want Ready with one", statuses[1].Phase, statuses[1].ServingPodCount)
		}
	})

	t.Run("a pod serving on its containers before PodReady folds reads Ready", func(t *testing.T) {
		gateNotFolded := observationPod("pod-1", "node-b", true, true, false)
		kept := gateNotFolded.Status.Conditions[:0]
		for _, cond := range gateNotFolded.Status.Conditions {
			if cond.Type != corev1.PodReady {
				kept = append(kept, cond)
			}
		}
		gateNotFolded.Status.Conditions = kept
		statuses := publish(t, single, map[int32][]*corev1.Pod{0: {serving}, 1: {gateNotFolded}})
		if statuses[1].Phase != types.InstancePhaseReady || statuses[1].ServingPodCount != 0 {
			t.Errorf("row = phase %s with %d serving, want Ready while the kubelet folds the gate", statuses[1].Phase, statuses[1].ServingPodCount)
		}
	})

	t.Run("a pod on its way out serves nothing", func(t *testing.T) {
		leaving := observationPod("pod-1", "node-b", true, true, false)
		now := metav1.Now()
		leaving.DeletionTimestamp = &now
		statuses := publish(t, single, map[int32][]*corev1.Pod{0: {serving}, 1: {leaving}})
		if statuses[1].Phase != types.InstancePhaseRestarting || statuses[1].PodCount != 1 {
			t.Errorf("row = phase %s with %d pod(s), want Restarting with one", statuses[1].Phase, statuses[1].PodCount)
		}
	})

	t.Run("a gang reads against its whole set", func(t *testing.T) {
		gang := map[int32]int32{0: 1, 1: 2, 2: 1}
		leader := observationPod("pod-1-leader", "node-b", true, true, false)
		statuses := publish(t, gang, map[int32][]*corev1.Pod{0: {serving}, 1: {leader, crashed}})
		if statuses[1].Phase != types.InstancePhaseRestarting || statuses[1].ServingPodCount != 1 {
			t.Errorf("gang with a crashed member = phase %s with %d serving, want Restarting with one", statuses[1].Phase, statuses[1].ServingPodCount)
		}
		worker := observationPod("pod-1-worker", "node-c", true, true, false)
		statuses = publish(t, gang, map[int32][]*corev1.Pod{0: {serving}, 1: {leader, worker}})
		if statuses[1].Phase != types.InstancePhaseReady {
			t.Errorf("whole gang serving = phase %s, want Ready", statuses[1].Phase)
		}
	})

	t.Run("a row an operation owns keeps its phase", func(t *testing.T) {
		statuses := publish(t, single, map[int32][]*corev1.Pod{0: {serving}, 1: {serving}, 2: {observationPod("pod-2", "node-c", false, true, false)}})
		if statuses[2].Phase != types.InstancePhaseUpdating {
			t.Errorf("operation-owned row phase = %s, want Updating", statuses[2].Phase)
		}
	})
}

// A Failed row with no operation that still runs a revision and records a
// spent attempt at another publishes Ready while the pods on the revision
// it runs serve at the desired count: the replacement failed, the Instance
// did not. The failed attempt stays on the row, its record, the revision it
// was for and the row's ReadySince anchor, and the engine keeps reading the
// row as Failed (v1beta1convert.InstanceStatusToWorkload). Only the pods on
// the running revision are read: a replacement crash-looping, or serving
// between two crashes, beside the source changes nothing. A source that
// stops serving reads Failed again. A Failed row that never ran a revision,
// one whose attempt was at the revision it runs, and one whose surge is
// past its drain hand-over keep Failed; a row still at its surge step is
// read by TestTakeInlineV1PublicationReadsReadyOverASurgeSourceWhoseReplacementCrashes.
func TestTakeInlineV1PublicationReadsReadyOverAFailedAttemptWhileTheSourceServes(t *testing.T) {
	const runningRev, targetRev = "svc-engine-aaaaaaaa", "svc-engine-bbbbbbbb"
	readySince := metav1.Unix(1700000000, 0)
	failedAt := metav1.Unix(1700000600, 0)
	record := func(pod string) *types.InstanceTermination {
		return &types.InstanceTermination{PodName: pod, Reason: "CrashLoopBackOff", Time: failedAt}
	}
	rows := func() []types.InstanceStatus {
		return []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: runningRev, TargetRevision: targetRev,
				ReadySince: &readySince, LastFailure: record("pod-0-1")},
			{Index: 1, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: runningRev, TargetRevision: targetRev,
				ReadySince: &readySince, LastFailure: record("pod-1-1-leader")},
			{Index: 2, Incarnation: 1, Phase: types.InstancePhaseFailed, TargetRevision: targetRev, LastFailure: record("pod-2-0")},
			{Index: 3, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: runningRev, TargetRevision: runningRev,
				ReadySince: &readySince, LastFailure: record("pod-3-0")},
			{Index: 4, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: runningRev, TargetRevision: targetRev,
				ReadySince: &readySince, LastFailure: record(""),
				Operation: &types.InstanceOperation{ID: "update-4", Type: types.InstanceOperationUpdate, Step: types.UpdateStepSurgeDrain, TargetRevision: targetRev}},
		}
	}
	desired := map[int32]int32{0: 1, 1: 2, 2: 1, 3: 1, 4: 2}
	publish := func(t *testing.T, byInstance map[int32][]*corev1.Pod) []types.InstanceStatus {
		t.Helper()
		observation, err := workload.NewOwnedPublicationObservation(
			rows(), workload.NewCachedSelectorPodObservation(nil, byInstance), nil, status.AvailabilityWindow{})
		if err != nil {
			t.Fatalf("NewOwnedPublicationObservation: %v", err)
		}
		statuses, _, err := observation.TakeInlineV1Publication(desired, targetRev)
		if err != nil {
			t.Fatalf("TakeInlineV1Publication: %v", err)
		}
		return statuses
	}
	pod := func(name, rev string, ready, serving bool) *corev1.Pod {
		p := observationPod(name, "node-"+name, ready, serving, false)
		p.Labels = map[string]string{query.LabelRevisionHash: query.RevisionHashFromControllerRevisionName(rev)}
		return p
	}
	source := pod("pod-0-0", runningRev, true, true)
	crashing := pod("pod-0-1", targetRev, false, true)

	t.Run("the serving source publishes Ready beside the crash-looping replacement", func(t *testing.T) {
		row := publish(t, map[int32][]*corev1.Pod{0: {source, crashing}})[0]
		if row.Phase != types.InstancePhaseReady || row.PodCount != 2 {
			t.Fatalf("row = phase %s with %d pod(s), want Ready with two", row.Phase, row.PodCount)
		}
		if row.RunningRevision != runningRev || row.TargetRevision != targetRev || row.Operation != nil {
			t.Errorf("row = running %q target %q operation %+v, want the revisions kept and no operation", row.RunningRevision, row.TargetRevision, row.Operation)
		}
		if row.LastFailure == nil || row.LastFailure.PodName != "pod-0-1" || !row.LastFailure.Time.Equal(&failedAt) {
			t.Errorf("LastFailure = %+v, want the replacement's failure kept", row.LastFailure)
		}
		if row.ReadySince == nil || !row.ReadySince.Equal(&readySince) {
			t.Errorf("ReadySince = %v, want the anchor preserved", row.ReadySince)
		}
	})

	t.Run("a replacement serving between two crashes changes nothing", func(t *testing.T) {
		between := pod("pod-0-1", targetRev, true, true)
		if row := publish(t, map[int32][]*corev1.Pod{0: {source, between}})[0]; row.Phase != types.InstancePhaseReady {
			t.Errorf("source serving beside a serving replacement = phase %s, want Ready", row.Phase)
		}
		down := pod("pod-0-0", runningRev, false, true)
		if row := publish(t, map[int32][]*corev1.Pod{0: {down, between}})[0]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("source down beside a serving replacement = phase %s, want Failed: the replacement is not the source", row.Phase)
		}
	})

	t.Run("a source that stops serving reads Failed again", func(t *testing.T) {
		down := pod("pod-0-0", runningRev, false, true)
		if row := publish(t, map[int32][]*corev1.Pod{0: {down, crashing}})[0]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("source not ready = phase %s, want Failed", row.Phase)
		}
		leaving := pod("pod-0-0", runningRev, true, true)
		now := metav1.Now()
		leaving.DeletionTimestamp = &now
		if row := publish(t, map[int32][]*corev1.Pod{0: {leaving}})[0]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("source on its way out = phase %s, want Failed", row.Phase)
		}
		if row := publish(t, map[int32][]*corev1.Pod{})[0]; row.Phase != types.InstancePhaseFailed || row.PodCount != 0 {
			t.Errorf("no pods = phase %s with %d pod(s), want Failed with none", row.Phase, row.PodCount)
		}
	})

	t.Run("a gang reads against its whole source set", func(t *testing.T) {
		leader := pod("pod-1-0-leader", runningRev, true, true)
		worker := pod("pod-1-0-worker", runningRev, true, true)
		failedLeader := pod("pod-1-1-leader", targetRev, false, true)
		if row := publish(t, map[int32][]*corev1.Pod{1: {leader, worker, failedLeader}})[1]; row.Phase != types.InstancePhaseReady {
			t.Errorf("whole source gang serving = phase %s, want Ready", row.Phase)
		}
		workerDown := pod("pod-1-0-worker", runningRev, false, true)
		if row := publish(t, map[int32][]*corev1.Pod{1: {leader, workerDown, failedLeader}})[1]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("source gang short of a serving member = phase %s, want Failed", row.Phase)
		}
	})

	t.Run("a row that never ran a revision keeps Failed", func(t *testing.T) {
		if row := publish(t, map[int32][]*corev1.Pod{2: {pod("pod-2-0", targetRev, true, true)}})[2]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("never promoted = phase %s, want Failed: a first attempt has no source", row.Phase)
		}
	})

	t.Run("a failure at the revision the row runs keeps Failed", func(t *testing.T) {
		if row := publish(t, map[int32][]*corev1.Pod{3: {pod("pod-3-0", runningRev, true, true)}})[3]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("failed at its running revision = phase %s, want Failed: the restart pass owns that return", row.Phase)
		}
	})

	t.Run("a row whose surge is past its drain hand-over keeps Failed", func(t *testing.T) {
		leader := pod("pod-4-0-leader", runningRev, true, true)
		worker := pod("pod-4-0-worker", runningRev, true, true)
		if row := publish(t, map[int32][]*corev1.Pod{4: {leader, worker}})[4]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("gang surge source at its drain step = phase %s, want Failed: the replacement had taken over", row.Phase)
		}
	})
}

// A Failed row that still keeps the surge attempt it failed at publishes
// Ready while the pods on the revision it runs serve at the desired count:
// the replacement crashed, the Instance did not. The escalation leaves the
// source of a gang surge in this shape until the replacement gang is
// retired; a single-pod source in the same shape reads the same. The
// attempt stays on the row with its record, the revision it was for and the
// ReadySince anchor, and the engine keeps reading the row as Failed
// (v1beta1convert.InstanceStatusToWorkload). The replacement's own row,
// which never ran a revision, keeps Failed. A source short of a serving
// member or on its way out reads Failed, and so does a source whose
// replacement took over and was then lost at the drain step.
func TestTakeInlineV1PublicationReadsReadyOverASurgeSourceWhoseReplacementCrashes(t *testing.T) {
	const runningRev, targetRev = "svc-engine-aaaaaaaa", "svc-engine-bbbbbbbb"
	readySince := metav1.Unix(1700000000, 0)
	failedAt := metav1.Unix(1700000600, 0)
	gangSlot, drainSlot := int32(3), int32(4)
	record := func(pod string) *types.InstanceTermination {
		return &types.InstanceTermination{PodName: pod, Reason: "CrashLoopBackOff", Time: failedAt}
	}
	attempt := func(id, step string, surgeIdx *int32) *types.InstanceOperation {
		return &types.InstanceOperation{ID: id, Type: types.InstanceOperationUpdate, Step: step, TargetRevision: targetRev, SurgeIndex: surgeIdx}
	}
	rows := func() []types.InstanceStatus {
		return []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: runningRev, TargetRevision: targetRev,
				ReadySince: &readySince, LastFailure: record("pod-0-1"), Operation: attempt("update-0", types.UpdateStepSurge, nil)},
			{Index: 1, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: runningRev, TargetRevision: targetRev,
				ReadySince: &readySince, LastFailure: record("pod-3-0-leader"), Operation: attempt("gangsurge-1", types.UpdateStepSurge, &gangSlot)},
			{Index: 2, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: runningRev, TargetRevision: targetRev,
				ReadySince: &readySince, LastFailure: record(""), Operation: attempt("gangsurge-2", types.UpdateStepSurgeDrain, &drainSlot)},
			{Index: 3, Incarnation: 1, Phase: types.InstancePhaseFailed, TargetRevision: targetRev,
				LastFailure: record("pod-3-0-leader"), Operation: attempt("gangsurgetarget-3", types.UpdateStepGangSurgeTarget, nil)},
		}
	}
	desired := map[int32]int32{0: 1, 1: 2, 2: 2, 3: 2}
	publish := func(t *testing.T, byInstance map[int32][]*corev1.Pod) []types.InstanceStatus {
		t.Helper()
		observation, err := workload.NewOwnedPublicationObservation(
			rows(), workload.NewCachedSelectorPodObservation(nil, byInstance), nil, status.AvailabilityWindow{})
		if err != nil {
			t.Fatalf("NewOwnedPublicationObservation: %v", err)
		}
		statuses, _, err := observation.TakeInlineV1Publication(desired, targetRev)
		if err != nil {
			t.Fatalf("TakeInlineV1Publication: %v", err)
		}
		return statuses
	}
	pod := func(name, rev string, ready, serving bool) *corev1.Pod {
		p := observationPod(name, "node-"+name, ready, serving, false)
		p.Labels = map[string]string{query.LabelRevisionHash: query.RevisionHashFromControllerRevisionName(rev)}
		return p
	}
	leader := pod("pod-1-0-leader", runningRev, true, true)
	worker := pod("pod-1-0-worker", runningRev, true, true)
	crashingLeader := pod("pod-3-0-leader", targetRev, false, false)

	t.Run("a single-pod source serving beside its crash-looping replacement publishes Ready", func(t *testing.T) {
		source := pod("pod-0-0", runningRev, true, true)
		crashing := pod("pod-0-1", targetRev, false, false)
		row := publish(t, map[int32][]*corev1.Pod{0: {source, crashing}})[0]
		if row.Phase != types.InstancePhaseReady || row.PodCount != 2 {
			t.Fatalf("row = phase %s with %d pod(s), want Ready with two", row.Phase, row.PodCount)
		}
		if row.Operation == nil || row.Operation.Step != types.UpdateStepSurge || row.RunningRevision != runningRev || row.TargetRevision != targetRev {
			t.Errorf("row = running %q target %q operation %+v, want the attempt and both revisions kept", row.RunningRevision, row.TargetRevision, row.Operation)
		}
		if row.LastFailure == nil || row.LastFailure.PodName != "pod-0-1" || !row.LastFailure.Time.Equal(&failedAt) {
			t.Errorf("LastFailure = %+v, want the replacement's failure kept", row.LastFailure)
		}
		if row.ReadySince == nil || !row.ReadySince.Equal(&readySince) {
			t.Errorf("ReadySince = %v, want the anchor preserved", row.ReadySince)
		}
	})

	t.Run("a gang source serving beside its crash-looping replacement gang publishes Ready", func(t *testing.T) {
		statuses := publish(t, map[int32][]*corev1.Pod{1: {leader, worker}, 3: {crashingLeader}})
		if row := statuses[1]; row.Phase != types.InstancePhaseReady || row.PodCount != 2 {
			t.Fatalf("source = phase %s with %d pod(s), want Ready with its two members", row.Phase, row.PodCount)
		}
		if row := statuses[1]; row.Operation == nil || row.Operation.SurgeIndex == nil || *row.Operation.SurgeIndex != gangSlot ||
			row.LastFailure == nil || row.LastFailure.PodName != "pod-3-0-leader" {
			t.Errorf("source = operation %+v failure %+v, want the surge attempt and the replacement's failure kept", row.Operation, row.LastFailure)
		}
		if row := statuses[3]; row.Phase != types.InstancePhaseFailed || row.PodCount != 1 {
			t.Errorf("replacement's own row = phase %s with %d pod(s), want Failed: it never ran a revision", row.Phase, row.PodCount)
		}
	})

	t.Run("a replacement serving between two crashes changes nothing", func(t *testing.T) {
		between := pod("pod-3-0-leader", targetRev, true, true)
		if row := publish(t, map[int32][]*corev1.Pod{1: {leader, worker}, 3: {between}})[1]; row.Phase != types.InstancePhaseReady {
			t.Errorf("source serving beside a serving replacement = phase %s, want Ready", row.Phase)
		}
		workerDown := pod("pod-1-0-worker", runningRev, false, true)
		if row := publish(t, map[int32][]*corev1.Pod{1: {leader, workerDown}, 3: {between}})[1]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("source down beside a serving replacement = phase %s, want Failed: the replacement is not the source", row.Phase)
		}
	})

	t.Run("a source gang short of a serving member or on its way out reads Failed", func(t *testing.T) {
		workerDown := pod("pod-1-0-worker", runningRev, false, true)
		if row := publish(t, map[int32][]*corev1.Pod{1: {leader, workerDown}, 3: {crashingLeader}})[1]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("source gang short of a serving member = phase %s, want Failed", row.Phase)
		}
		leaving := pod("pod-1-0-leader", runningRev, true, true)
		now := metav1.Now()
		leaving.DeletionTimestamp = &now
		if row := publish(t, map[int32][]*corev1.Pod{1: {leaving, worker}, 3: {crashingLeader}})[1]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("source member on its way out = phase %s, want Failed", row.Phase)
		}
		if row := publish(t, map[int32][]*corev1.Pod{3: {crashingLeader}})[1]; row.Phase != types.InstancePhaseFailed || row.PodCount != 0 {
			t.Errorf("no source pods = phase %s with %d pod(s), want Failed with none", row.Phase, row.PodCount)
		}
	})

	t.Run("a source whose replacement took over and was then lost keeps Failed", func(t *testing.T) {
		drainingLeader := pod("pod-2-0-leader", runningRev, true, true)
		drainingWorker := pod("pod-2-0-worker", runningRev, true, true)
		if row := publish(t, map[int32][]*corev1.Pod{2: {drainingLeader, drainingWorker}})[2]; row.Phase != types.InstancePhaseFailed {
			t.Errorf("source at the drain step = phase %s, want Failed: the replacement had taken over", row.Phase)
		}
	})
}

// An Instance serves through its routable pod, and a Service never
// publishes a gang's worker. A Failed gang whose leader is gone therefore
// publishes no serving pod however many workers keep the gate, and the
// unserving-row rule (types.RecreateOfUnservingRow) reads it as serving
// nothing: rebuilding it takes nothing further offline. A Failed gang whose
// leader serves beside a worker held in a waiting state publishes the
// members that serve, short of its desired count, and is not unserving. A
// whole gang publishes every member and is the one serving Instance.
func TestTakeInlineV1PublicationReadsAGangWithoutItsLeaderAsUnserving(t *testing.T) {
	member := func(index int32, runner string, ordinal int, serving bool) *corev1.Pod {
		pod := observationPod(fmt.Sprintf("engine-%d-%s-%d", index, runner, ordinal), fmt.Sprintf("node-%d-%d", index, ordinal), serving, serving, false)
		pod.Labels = map[string]string{query.LabelRunner: runner}
		return pod
	}
	gang := func(index int32, withLeader bool, servingWorkers, wedgedWorkers int) []*corev1.Pod {
		var pods []*corev1.Pod
		if withLeader {
			pods = append(pods, member(index, types.RunnerLeader, 0, true))
		}
		for ordinal := 0; ordinal < servingWorkers+wedgedWorkers; ordinal++ {
			pods = append(pods, member(index, types.RunnerWorker, ordinal, ordinal < servingWorkers))
		}
		return pods
	}
	rows := []types.InstanceStatus{
		{Index: 0, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a"},
		{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a"},
		{Index: 2, Incarnation: 1, Phase: types.InstancePhaseFailed, RunningRevision: "rev-a"},
	}
	byInstance := map[int32][]*corev1.Pod{
		0: gang(0, false, 7, 0),
		1: gang(1, true, 7, 0),
		2: gang(2, true, 6, 1),
	}
	observation, err := workload.NewOwnedPublicationObservation(rows,
		workload.NewCachedSelectorPodObservation(nil, byInstance), nil, status.AvailabilityWindow{})
	if err != nil {
		t.Fatalf("NewOwnedPublicationObservation: %v", err)
	}
	statuses, counters, err := observation.TakeInlineV1Publication(map[int32]int32{0: 8, 1: 8, 2: 8}, "rev-a")
	if err != nil {
		t.Fatalf("TakeInlineV1Publication: %v", err)
	}

	leaderGone := statuses[0]
	if leaderGone.PodCount != 7 || leaderGone.ServingPodCount != 0 {
		t.Errorf("gang without its leader publishes %d pod(s), %d serving; want 7 pods and none serving", leaderGone.PodCount, leaderGone.ServingPodCount)
	}
	if leaderGone.Phase != types.InstancePhaseFailed {
		t.Errorf("gang without its leader publishes phase %s, want Failed", leaderGone.Phase)
	}
	if !types.RecreateOfUnservingRow(&leaderGone, types.UpdateStrategyRecreatePod) {
		t.Errorf("a Failed gang without its leader must read as dark: %d serving pod(s) published", leaderGone.ServingPodCount)
	}

	whole := statuses[1]
	if whole.PodCount != 8 || whole.ServingPodCount != 8 {
		t.Errorf("whole gang publishes %d pod(s), %d serving; want 8 pods, all serving", whole.PodCount, whole.ServingPodCount)
	}
	if whole.Phase != types.InstancePhaseReady {
		t.Errorf("whole gang publishes phase %s, want Ready", whole.Phase)
	}

	wedgedWorker := statuses[2]
	if wedgedWorker.PodCount != 8 || wedgedWorker.ServingPodCount != 7 {
		t.Errorf("gang with a wedged worker publishes %d pod(s), %d serving; want 8 pods and the 7 that serve", wedgedWorker.PodCount, wedgedWorker.ServingPodCount)
	}
	if wedgedWorker.Phase != types.InstancePhaseFailed {
		t.Errorf("gang with a wedged worker publishes phase %s, want Failed", wedgedWorker.Phase)
	}
	if types.RecreateOfUnservingRow(&wedgedWorker, types.UpdateStrategyRecreatePod) {
		t.Errorf("a Failed gang whose leader serves beside a wedged worker is not dark: %d serving pod(s) published", wedgedWorker.ServingPodCount)
	}

	if counters.ServingReplicas != 1 {
		t.Errorf("ServingReplicas = %d, want 1: the whole gang serves, the other two are short of their desired count", counters.ServingReplicas)
	}
}

// A settled Ready row whose node stopped reporting publishes Restarting
// beside a serving count of zero. The node lifecycle controller revokes
// the pods' Ready condition with its own reason once the node's grace has
// passed; the kubelet is gone, so ContainersReady and the serving gate
// keep their last values. No pod of the set is in any Service's endpoints,
// so the row reads not serving the moment the revocation lands, not when
// the rebuild replaces the set. A pod whose Ready is merely not folded yet
// still reads as serving (the sub-case in
// TestTakeInlineV1PublicationReadsRestartingOverAPodSetNotServing).
func TestTakeInlineV1PublicationReadsRestartingWhenTheNodeRevokedReady(t *testing.T) {
	readySince := metav1.Unix(1700000000, 0)
	rows := func() []types.InstanceStatus {
		return []types.InstanceStatus{
			{Index: 0, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince},
			{Index: 1, Incarnation: 1, Phase: types.InstancePhaseReady, RunningRevision: "rev-a", ReadySince: &readySince},
		}
	}
	publish := func(t *testing.T, desired map[int32]int32, byInstance map[int32][]*corev1.Pod) []types.InstanceStatus {
		t.Helper()
		observation, err := workload.NewOwnedPublicationObservation(
			rows(), workload.NewCachedSelectorPodObservation(nil, byInstance), nil, status.AvailabilityWindow{})
		if err != nil {
			t.Fatalf("NewOwnedPublicationObservation: %v", err)
		}
		statuses, _, err := observation.TakeInlineV1Publication(desired, "rev-a")
		if err != nil {
			t.Fatalf("TakeInlineV1Publication: %v", err)
		}
		return statuses
	}
	revokedByNode := func(name, node string) *corev1.Pod {
		pod := observationPod(name, node, true, true, false)
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == corev1.PodReady {
				pod.Status.Conditions[i].Status = corev1.ConditionFalse
				pod.Status.Conditions[i].Reason = podreadiness.NodeNotReadyReason
			}
		}
		return pod
	}
	serving := observationPod("pod-0", "node-a", true, true, false)

	t.Run("a single pod whose node stopped reporting", func(t *testing.T) {
		statuses := publish(t, map[int32]int32{0: 1, 1: 1}, map[int32][]*corev1.Pod{0: {serving}, 1: {revokedByNode("pod-1", "node-b")}})
		row := statuses[1]
		if row.Phase != types.InstancePhaseRestarting || row.PodCount != 1 || row.ServingPodCount != 0 {
			t.Errorf("row = phase %s, %d pod(s), %d serving; want Restarting with one pod and none serving", row.Phase, row.PodCount, row.ServingPodCount)
		}
		if row.RunningRevision != "rev-a" || row.Operation != nil || row.ReadySince == nil {
			t.Errorf("row = revision %q operation %+v readySince %v; want rev-a kept, no operation and the anchor preserved", row.RunningRevision, row.Operation, row.ReadySince)
		}
		if statuses[0].Phase != types.InstancePhaseReady {
			t.Errorf("serving row phase = %s, want Ready", statuses[0].Phase)
		}
	})

	t.Run("a gang whose node stopped reporting", func(t *testing.T) {
		leader := revokedByNode("pod-1-leader", "node-b")
		worker := revokedByNode("pod-1-worker", "node-b")
		statuses := publish(t, map[int32]int32{0: 1, 1: 2}, map[int32][]*corev1.Pod{0: {serving}, 1: {leader, worker}})
		row := statuses[1]
		if row.Phase != types.InstancePhaseRestarting || row.PodCount != 2 || row.ServingPodCount != 0 {
			t.Errorf("gang = phase %s, %d pod(s), %d serving; want Restarting with two pods and none serving", row.Phase, row.PodCount, row.ServingPodCount)
		}
	})
}

// fleetPublicationCase is one pass's publication over a fleet: the name the
// benchmark and the allocation golden share, and the pass.
type fleetPublicationCase struct {
	name string
	run  func(testing.TB)
}

// fleetPublicationCases run the publication, the pass that converts the
// decoded rows, reads each against its pods and writes the published phase
// and the counters, at the benchmark's largest shapes, 4,000 single-pod
// Instances and 160 gangs of one leader and seven workers, and at a quarter
// of the largest, 1,000 single-pod Instances, every pod serving and in
// rotation. Each shape runs over rows with no readySince (uniform) and over
// rows each carrying its own (readySince), as a replica at rest stores them.
func fleetPublicationCases() []fleetPublicationCase {
	shapes := []struct {
		name            string
		instances, pods int
	}{
		{name: "1000x1", instances: 1000, pods: 1},
		{name: "4000x1", instances: 4000, pods: 1},
		{name: "160x8", instances: 160, pods: 8},
	}
	rowSets := []struct {
		name string
		rows func(int) []v1beta1.OMENativeInstanceStatus
	}{
		{name: "uniform", rows: irstatustest.UniformRows},
		{name: "readySince", rows: irstatustest.ReadySinceRows},
	}
	var cases []fleetPublicationCase
	for _, s := range shapes {
		byIdx := fleetPods(s.instances, s.pods)
		pods := workload.NewCachedSelectorPodObservation(nil, byIdx)
		available := make(map[string]struct{}, s.instances*s.pods)
		desired := make(map[int32]int32, s.instances)
		for index, set := range byIdx {
			desired[index] = int32(s.pods)
			for _, pod := range set {
				available[pod.Name] = struct{}{}
			}
		}
		for _, rowSet := range rowSets {
			stored := rowSet.rows(s.instances)
			for i := range stored {
				stored[i].PodCount, stored[i].ServingPodCount, stored[i].AvailablePodCount = int32(s.pods), int32(s.pods), int32(s.pods)
			}
			cases = append(cases, fleetPublicationCase{name: s.name + "/" + rowSet.name + "/publication", run: func(tb testing.TB) {
				observation, err := workload.NewOwnedPublicationObservation(
					v1beta1convert.InstanceStatusSliceToWorkload(stored), pods, available, status.AvailabilityWindow{})
				if err != nil {
					tb.Fatal(err)
				}
				published, counters, err := observation.TakeInlineV1Publication(desired, irstatustest.Revision)
				if err != nil {
					tb.Fatal(err)
				}
				if int(counters.ServingReplicas) != s.instances || int(counters.UpdatedReadyReplicas) != s.instances {
					tb.Fatalf("%s: %d serving and %d updated ready of %d Instances", s.name, counters.ServingReplicas, counters.UpdatedReadyReplicas, s.instances)
				}
				benchmarkObservationRows = published
			}})
		}
	}
	return cases
}

// BenchmarkPublicationFleet is the cost of one pass's publication per shape
// and row set.
func BenchmarkPublicationFleet(b *testing.B) {
	for _, c := range fleetPublicationCases() {
		b.Run(c.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				c.run(b)
			}
		})
	}
}
