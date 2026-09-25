package workload_test

// Single-transition-writer architecture test, beside the package doc
// that states the boundary. The ownership boundary of the observe →
// plan → execute reconcile, encoded as a source scan so a
// regression fails a unit test instead of surfacing as a coordination
// bug in production:
//
//   - Per-instance transition fields on the workload InstanceStatus —
//     Phase / Operation / RunningRevision — are rollTarget-scoped and
//     written ONLY by the workload package (ops state machines,
//     escalation, disposition, deadline parking, migrate).
//   - The component-level revision pair — CurrentRevision /
//     UpdateRevision — is specTarget-scoped and written ONLY by the IR
//     controller: CurrentRevision in buildPromoteCurrentRevision,
//     UpdateRevision in aggregateAndWriteStatus, plus their in-memory
//     mirror sites. The pair is semantically inseparable (coordination
//     reads RolloutInFlight from their skew; canary rollback load-bears
//     on CurrentRevision naming the last revision fully rolled forward
//     onto), so the workload package must never touch either half.
//
// The scan is deliberately approximate: it flags every assignment
// statement whose left-hand side selects one of the owned field names,
// attributed to the enclosing top-level function, without resolving the
// receiver's type. False positives (a same-named field on another type)
// surface for review and an explicit allowlist entry — preferable to a
// type-resolving scan that could silently miss a write site.

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
)

// perInstanceFields are the workload-owned per-instance transition
// fields: any assignment under workload/ is implicitly allowed, any
// assignment elsewhere must be allowlisted.
var perInstanceFields = map[string]bool{
	"Phase":           true,
	"Operation":       true,
	"RunningRevision": true,
}

// componentPairFields are the IR-controller-owned component-level
// revision fields: EVERY assignment in the scanned trees — including
// workload/ — must be allowlisted.
var componentPairFields = map[string]bool{
	"CurrentRevision": true,
	"UpdateRevision":  true,
}

// scannedRoots are the package trees the boundary spans, relative to
// pkg/controller/v1beta1.
var scannedRoots = []string{
	"workload",
	"inferencereplica",
	"inferenceservice",
	"v1beta1convert",
}

// allowedTransitionWrite pins one deliberately-reviewed write site:
// exact file (relative to pkg/controller/v1beta1), enclosing top-level
// function, field, and assignment count. An exact count means a NEW
// assignment inside an already-allowlisted function still fails until
// reviewed.
type allowedTransitionWrite struct {
	file  string
	fn    string
	field string
	count int
	why   string
}

// transitionWriteAllowlist is the complete audited set of transition-
// field write sites outside the implicit workload/ per-instance
// ownership. Every entry says WHY the site is legitimate — a mirror or
// an owned decision, never a second decision-maker.
var transitionWriteAllowlist = []allowedTransitionWrite{
	{
		file: "workload/escalation/escalation.go", fn: "Run", field: "UpdateRevision", count: 1,
		why: "defaults the pass's LOCAL ReconcileInput copy (pass-by-value) so the first-rollout " +
			"disposition can resolve its RetryBlock target before the aggregator has stamped " +
			"UpdateRevision; never persisted — not a component-pair decision",
	},
	{
		file: "inferencereplica/convert.go", fn: "buildPromoteCurrentRevision", field: "CurrentRevision", count: 2,
		why: "the ONE promotion decision site: stamps CurrentRevision = spec target iff " +
			"RolloutComplete (persisted write), then mirrors the committed value onto the " +
			"caller's in-memory IR (second assignment) so the deferred aggregator observes it",
	},
	{
		file: "inferencereplica/status.go", fn: "aggregateAndWriteStatus", field: "UpdateRevision", count: 1,
		why: "the ONE UpdateRevision stamp: the aggregator names the spec/canary target on the " +
			"component pair; per-instance transition fields stay untouched",
	},
	{
		file: "inferencereplica/status.go", fn: "mirrorBack", field: "UpdateRevision", count: 1,
		why: "in-memory mirror of the just-committed status onto the caller's IR (fresh → ir " +
			"field copy), not a decision",
	},
	{
		file: "inferencereplica/status.go", fn: "mirrorBack", field: "CurrentRevision", count: 1,
		why: "in-memory mirror of the just-committed status onto the caller's IR (fresh → ir " +
			"field copy), not a decision",
	},
}

// transitionWriteSite is one observed assignment.
type transitionWriteSite struct {
	file  string // relative to pkg/controller/v1beta1
	fn    string // enclosing top-level function ("" at package scope)
	field string
	line  int
}

// TestSingleTransitionWriter scans the non-test sources of the scanned
// roots and fails on any transition-field assignment that is neither
// covered by the workload package's implicit per-instance ownership nor
// pinned in transitionWriteAllowlist with a matching count.
func TestSingleTransitionWriter(t *testing.T) {
	sites, filesScanned := scanTransitionWrites(t)

	// Scan sanity: a broken root path or field set must fail loudly, not
	// pass vacuously.
	if filesScanned < 50 {
		t.Fatalf("scanned only %d files — the scan roots look wrong (cwd must be the workload package dir)", filesScanned)
	}
	implicitWorkload := 0

	counts := make(map[string]int)
	for _, s := range sites {
		if perInstanceFields[s.field] && strings.HasPrefix(s.file, "workload/") {
			implicitWorkload++
			continue
		}
		counts[s.file+"|"+s.fn+"|"+s.field]++
	}
	if implicitWorkload == 0 {
		t.Fatalf("found no per-instance transition writes under workload/ — the scanner is broken (the ops state machines contain many)")
	}

	allowed := make(map[string]int, len(transitionWriteAllowlist))
	for _, a := range transitionWriteAllowlist {
		allowed[a.file+"|"+a.fn+"|"+a.field] = a.count
	}

	// Unallowlisted or over-count sites.
	for _, s := range sites {
		if perInstanceFields[s.field] && strings.HasPrefix(s.file, "workload/") {
			continue
		}
		key := s.file + "|" + s.fn + "|" + s.field
		if want, ok := allowed[key]; !ok || counts[key] != want {
			t.Errorf("new transition-field write site: %s:%d (%s) assigns .%s — review against the ownership boundary in the design doc "+
				"(workload owns Phase/Operation/RunningRevision; the IR controller owns the CurrentRevision/UpdateRevision pair), "+
				"then extend transitionWriteAllowlist deliberately", s.file, s.line, s.fn, s.field)
		}
	}

	// Stale allowlist entries: a vanished or moved site must force a
	// review too, so the allowlist never outlives the code it describes.
	for _, a := range transitionWriteAllowlist {
		key := a.file + "|" + a.fn + "|" + a.field
		if got := counts[key]; got != a.count {
			t.Errorf("allowlist entry %s (%s, .%s) expects %d assignment(s), found %d — the write site moved or changed; re-audit and update the entry",
				a.file, a.fn, a.field, a.count, got)
		}
	}
}

// scanTransitionWrites parses every non-test .go file under the scanned
// roots and returns each assignment whose LHS selects a transition
// field, plus the number of files scanned. Test cwd is the workload
// package dir, so the roots resolve through "..".
func scanTransitionWrites(t *testing.T) ([]transitionWriteSite, int) {
	t.Helper()
	fieldSet := make(map[string]bool, len(perInstanceFields)+len(componentPairFields))
	for f := range perInstanceFields {
		fieldSet[f] = true
	}
	for f := range componentPairFields {
		fieldSet[f] = true
	}

	var sites []transitionWriteSite
	filesScanned := 0
	fset := token.NewFileSet()
	for _, root := range scannedRoots {
		files, err := goSourceFiles(filepath.Join("..", root))
		if err != nil {
			t.Fatalf("walk %s: %v", root, err)
		}
		for _, path := range files {
			f, perr := parser.ParseFile(fset, path, nil, parser.SkipObjectResolution)
			if perr != nil {
				t.Fatalf("parse %s: %v", path, perr)
			}
			filesScanned++
			rel, rerr := filepath.Rel("..", path)
			if rerr != nil {
				t.Fatalf("rel %s: %v", path, rerr)
			}
			rel = filepath.ToSlash(rel)
			for _, decl := range f.Decls {
				fn := ""
				if fd, ok := decl.(*ast.FuncDecl); ok {
					fn = fd.Name.Name
				}
				ast.Inspect(decl, func(n ast.Node) bool {
					assign, ok := n.(*ast.AssignStmt)
					if !ok {
						return true
					}
					for _, lhs := range assign.Lhs {
						sel, ok := lhs.(*ast.SelectorExpr)
						if !ok || !fieldSet[sel.Sel.Name] {
							continue
						}
						sites = append(sites, transitionWriteSite{
							file:  rel,
							fn:    fn,
							field: sel.Sel.Name,
							line:  fset.Position(sel.Pos()).Line,
						})
					}
					return true
				})
			}
		}
	}
	sort.Slice(sites, func(i, j int) bool {
		if sites[i].file != sites[j].file {
			return sites[i].file < sites[j].file
		}
		return sites[i].line < sites[j].line
	})
	return sites, filesScanned
}

// goSourceFiles returns every non-test .go file under root, recursively.
func goSourceFiles(root string) ([]string, error) {
	var out []string
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			if d.Name() == "testdata" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		out = append(out, path)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("walk %s: %w", root, err)
	}
	return out, nil
}
