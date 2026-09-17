package workload_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

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

	approve("pkg/controller/v1beta1/workload/observation.go", "overlayInlineV1", readWrite, "publication materialization", allFields...)
	approve("pkg/controller/v1beta1/workload/observation.go", "cloneInstanceStatus", removableFieldAccessCounts{reads: 2, writes: 1}, "isolated status copy", "NodesOccupied")
	approve("pkg/controller/v1beta1/workload/status_aggregate.go", "TakeInlineV1Publication", removableFieldAccessCounts{reads: 2}, "current Pod observation", "ReadyPodCount")
	approve("pkg/controller/v1beta1/workload/status_aggregate.go", "String", read, "current counter diagnostics", allFields...)
	for _, function := range []string{"InstanceStatusToWorkload", "InstanceStatusFromWorkload"} {
		approve("pkg/controller/v1beta1/v1beta1convert/convert.go", function, read, "wire conversion", "ReadyPodCount", "ScheduledPodCount")
		approve("pkg/controller/v1beta1/v1beta1convert/convert.go", function, removableFieldAccessCounts{reads: 2, writes: 1}, "wire conversion", "NodesOccupied")
	}
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "aggregateAndWriteStatus", readWrite, "transient publication materialization", allFields...)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "mirrorInstanceCounters", readWrite, "transient same-pass mirror", "ReadyPodCount", "ScheduledPodCount")
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "mirrorInstanceCounters", removableFieldAccessCounts{reads: 2, writes: 2}, "transient same-pass mirror", "NodesOccupied")
	approve("pkg/controller/v1beta1/irstatus/encoding.go", "ClearPodDerivedObservations", write, "status persistence boundary", allFields...)
	approve("pkg/controller/v1beta1/workload/ops/update_status.go", "cloneTerminalStatusValue", readWrite, "isolated status copy", "NodesOccupied")
	approve("pkg/controller/v1beta1/workload/ops/create.go", "sameCreateTransitionOwnerState", removableFieldAccessCounts{writes: 2}, "rollback comparison normalization", allFields...)
	approve("pkg/controller/v1beta1/workload/ops/create.go", "restoreCreateTransitionState", readWrite, "transient rollback observation preservation", "ReadyPodCount", "ScheduledPodCount")
	approve("pkg/controller/v1beta1/workload/ops/create.go", "restoreCreateTransitionState", removableFieldAccessCounts{reads: 2, writes: 2}, "transient rollback observation preservation", "NodesOccupied")

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
