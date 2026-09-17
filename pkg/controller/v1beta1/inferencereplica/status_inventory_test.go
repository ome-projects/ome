package inferencereplica

import (
	"fmt"
	"go/ast"
	"go/token"
	"go/types"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"golang.org/x/tools/go/packages"
)

// The executable consumer inventory for InferenceReplica per-Instance status.
//
// Three type-aware sweeps over every production package (pkg, cmd, internal,
// scheduler; generated files and tests excluded) pin the per-Instance status
// reader and writer boundaries:
//
//   - field reads: every use of InferenceReplicaStatus.InstanceStatuses,
//     InstanceStatusColumns, and InstanceStatusEncoding outside the codec
//     package must be listed below with the classification that makes it
//     safe;
//   - fetch sites: every Get or List of an InferenceReplica must be the
//     decoded accessor or a listed pass-through that inspects no rows;
//   - status writes: every InferenceReplica status write must be the single
//     writer.
//
// The field-read sweep also covers every file under tests/, test files
// included: integration specs read rows only through the shared helper that
// decodes either stored representation, so a spec cannot silently observe an
// empty dense list on a ColumnarV2 object. The qualification suites that
// inspect the stored representation on purpose are classified as such.
//
// A new site anywhere fails until it is classified here; a stale entry fails
// so the table never drifts from the code. An import-graph check keeps the
// shared test-fixture package out of every production package, which is what
// makes its classification below true.

const (
	omeAPIPackagePath  = "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	codecPackagePath   = "sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	fixturePackagePath = codecPackagePath + "/irstatustest"
	decodedAccessor    = "GetDecoded"
	singleWriter       = "persistInferenceReplicaStatus"

	projectorPackagePath               = "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	controllerRuntimeClientPackagePath = "sigs.k8s.io/controller-runtime/pkg/client"
)

var representationFields = map[string]struct{}{
	"InstanceStatuses":       {},
	"InstanceStatusColumns":  {},
	"InstanceStatusEncoding": {},
}

// Classifications name why a site outside the boundary is allowed.
const (
	// decodedObjectRows: the function consumes rows of an object that the
	// decoded accessor rewrote into the dense logical shape before the
	// function ran, or that the single writer decoded after its commit.
	decodedObjectRows = "logical rows of an object decoded at the boundary"
	// inMemoryMirror: the function copies committed rows onto the decoded
	// in-memory object so later work in the same pass observes them.
	inMemoryMirror = "in-memory mirror of committed rows onto the decoded object"
	// passThroughSpecMetadata: the fetch serves a spec- or metadata-only
	// path and never inspects the per-Instance representation.
	passThroughSpecMetadata = "pass-through: spec/metadata only"
	// passThroughTopLevelStatus: the fetch serves a reader of top-level
	// status fields only (revisions, counters, conditions, traffic).
	passThroughTopLevelStatus = "pass-through: top-level status only"
	// decodedBoundary: the fetch is the decoded accessor itself.
	decodedBoundary = "decoded-accessor boundary"
	// administrativeCensus: the operator preflight's paginated list; every
	// object is classified through the codec (ObservedEncoding, DecodeStatus)
	// and no row is consumed by a decision.
	administrativeCensus = "administrative paginated census classified through the codec"
	// testFixtureBuilder: the function builds logical statuses for the codec
	// and qualification suites to measure; its package is test support that
	// no production package imports (TestIRStatusFixturePackageImportInventory).
	testFixtureBuilder = "test-fixture builder constructing logical statuses; imported only by tests"
	// breakGlassRepair: the operator-side repair installs an independently
	// validated replacement on a copy of the raw live object and writes it
	// through the single writer; the stored payload is never decoded.
	breakGlassRepair = "break-glass repair through the single writer"
	// breakGlassRepairRead: the repair's raw live read supplies the
	// resourceVersion precondition and the status outside the per-Instance
	// representation; the stored payload is replaced, not consumed.
	breakGlassRepairRead = "pass-through: break-glass repair live read (resourceVersion and unrelated status)"
	// rawReaderOutsideManager: a reader outside the manager (the kubectl-ome
	// CLI) fetches the object raw and consumes the stored dense rows
	// directly, so a ColumnarV2 object presents no rows to it until it reads
	// through the decoded accessor.
	rawReaderOutsideManager = "raw reader outside the manager: stored dense rows only; a ColumnarV2 object presents no rows"
)

type accessCounts struct {
	reads      int
	writes     int
	readWrites int
}

type fieldUse struct {
	file     string
	function string
	field    string
}

type approvedFieldUse struct {
	counts accessCounts
	reason string
}

type fetchSite struct {
	file     string
	function string
	method   string
}

type approvedFetch struct {
	count  int
	reason string
}

func TestInferenceReplicaStatusReadInventory(t *testing.T) {
	approved := map[fieldUse]approvedFieldUse{}
	approve := func(file, function string, counts accessCounts, reason string, fields ...string) {
		for _, field := range fields {
			approved[fieldUse{file: file, function: function, field: field}] = approvedFieldUse{counts: counts, reason: reason}
		}
	}
	read := func(n int) accessCounts { return accessCounts{reads: n} }
	rows := "InstanceStatuses"

	// InferenceReplica reconciler: every function below runs on an object
	// fetched through irstatus.GetDecoded (see the fetch inventory). Counts
	// are reads / writes / read-writes of the field selector.
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "observedFromIR", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "buildMutateInstance", accessCounts{reads: 4, writes: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "buildApplyInstanceMutationsWithRetryBlockFromReader", accessCounts{reads: 7, writes: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "instanceMutationPostconditionsHold", read(3), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "replaceInstanceStatuses", accessCounts{writes: 1}, inMemoryMirror, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "mirrorInstanceStatuses", accessCounts{reads: 4, writes: 1, readWrites: 2}, inMemoryMirror, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "buildPromoteCurrentRevision", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/convert.go", "buildRemoveInstance", accessCounts{reads: 1, writes: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "Reconciler.aggregateAndWriteStatus", accessCounts{reads: 3, readWrites: 7}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "Reconciler.reconcileHeldDeadlines", accessCounts{reads: 3, readWrites: 2}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "mirrorInstanceCounters", accessCounts{reads: 1, readWrites: 1}, inMemoryMirror, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "stagedAtPartition", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "computeRolloutStalledCondition", read(2), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status.go", "computeReadyCondition", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/reconciler.go", "Reconciler.Reconcile", read(2), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/reconciler.go", "Reconciler.reconcileRelocationDirectives", accessCounts{reads: 3, readWrites: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/reconciler.go", "Reconciler.stampAutoRelocationSuccess", accessCounts{reads: 1, readWrites: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/reset_instances.go", "Reconciler.resetInstances", accessCounts{reads: 2, readWrites: 2}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/retention.go", "Reconciler.sweepRevisions", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferencereplica/status_transition.go", "Reconciler.convertStoredRepresentation", read(1), decodedObjectRows, rows)

	// Break-glass repair: the only writer entry point registered with no
	// reconciler. It installs validated replacement rows on a deep copy of
	// the raw live object and clears the marker and columns on that copy.
	approve("pkg/controller/v1beta1/inferencereplica/status_repair.go", "RepairInstanceStatus", accessCounts{writes: 1}, breakGlassRepair, rows, "InstanceStatusEncoding", "InstanceStatusColumns")
	approve("pkg/controller/v1beta1/inferencereplica/status_repair.go", "validateRepairReplacement", accessCounts{writes: 1}, breakGlassRepair, rows)

	// Remote readers: coordination and placement consume the object that
	// irprojector.DecodedComponentIR(Status) returned.
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/pairing.go", "GateContext.CheckPairing", accessCounts{reads: 1, readWrites: 1}, decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/ratio.go", "GateContext.CheckRatio", read(3), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/ratio.go", "GateContext.CheckSurge", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/sequential_gate.go", "observeSequentialComponentsForGate", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination/reconciler.go", "buildComponentObservation", read(2), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/placement/admission.go", "admittedReplicaCount", read(2), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/placement/admission.go", "componentHasAdmittedInstance", read(1), decodedObjectRows, rows)
	approve("pkg/controller/v1beta1/placement/failed.go", "IsTerminallyFailed", read(1), decodedObjectRows, rows)

	// The kubectl-ome CLI still reads stored dense rows directly. Alfred has
	// no direct representation-field readers: its bounded adapter calls the
	// shared codec and returns independent logical rows without changing the
	// raw captured object.
	approve("pkg/cli/instancecollection/collect.go", "CollectRelated", read(2), rawReaderOutsideManager, rows)
	approve("pkg/cli/instancecollection/collect.go", "boundedReplicaCopy", accessCounts{reads: 2, writes: 1, readWrites: 2}, rawReaderOutsideManager, rows)
	approve("pkg/cli/instanceprojection/project.go", "Project", read(8), rawReaderOutsideManager, rows)
	approve("pkg/cli/instanceprojection/project.go", "validAggregateStatus", read(2), rawReaderOutsideManager, rows)
	approve("pkg/cli/instancestatusprojection/project.go", "EventTargets", accessCounts{reads: 2, readWrites: 1}, rawReaderOutsideManager, rows)
	approve("pkg/cli/instancestatusprojection/project.go", "findRawRow", accessCounts{reads: 2, readWrites: 1}, rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/held_release_source.go", "validateHeldReplicaIdentity", read(1), rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/migration_evidence.go", "CollectMigrationEvidence", accessCounts{reads: 2, readWrites: 1}, rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/replicas.go", "inspectReplica", read(2), rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/replicas.go", "replicaPayloadBounded", read(1), rawReaderOutsideManager, rows)
	approve("pkg/cli/mutate/scale_evidence.go", "scaleLifecycleWork", read(1), rawReaderOutsideManager, rows)

	// Shared test fixtures: the builder writes dense rows into a status it
	// constructs; nothing outside tests links it.
	approve("pkg/controller/v1beta1/irstatus/irstatustest/fixtures.go", "LogicalStatus", accessCounts{writes: 1}, testFixtureBuilder, rows)

	inv := loadStatusInventory(t)
	actual := map[fieldUse]accessCounts{}
	inv.eachProductionFile(func(pkg *packages.Package, file *ast.File, relative string) {
		if pkg.PkgPath == codecPackagePath {
			return
		}
		collectRepresentationFieldUses(pkg, file, relative, actual)
	})
	loadTestInventory(t).eachTestFile(func(pkg *packages.Package, file *ast.File, relative string) {
		collectRepresentationFieldUses(pkg, file, relative, actual)
	})

	for use, got := range actual {
		want, ok := approved[use]
		if !ok {
			t.Errorf("unclassified read of %s in %s:%s: %+v", use.field, use.file, use.function, got)
			continue
		}
		if got != want.counts {
			t.Errorf("access count changed for %s in %s:%s: got %+v, want %+v (%s)", use.field, use.file, use.function, got, want.counts, want.reason)
		}
	}
	for use, want := range approved {
		if _, ok := actual[use]; !ok {
			t.Errorf("stale approval for %s in %s:%s (%s)", use.field, use.file, use.function, want.reason)
		}
	}
}

func TestInferenceReplicaFetchInventory(t *testing.T) {
	approved := map[fetchSite]approvedFetch{}
	approve := func(file, function, method string, count int, reason string) {
		approved[fetchSite{file: file, function: function, method: method}] = approvedFetch{count: count, reason: reason}
	}

	approve("pkg/controller/v1beta1/irstatus/reader.go", decodedAccessor, "Get", 1, decodedBoundary)
	approve("pkg/controller/v1beta1/irstatus/transitionpreflight/preflight.go", "checkReplicas", "List", 1, administrativeCensus)
	approve("pkg/controller/v1beta1/irstatus/statusrepair/repair.go", "Run", "Get", 1, breakGlassRepairRead)

	// InferenceReplica reconciler pass-through reads.
	approve("pkg/controller/v1beta1/inferencereplica/reconciler.go", "Reconciler.Reconcile", "Get", 1, passThroughSpecMetadata+" (finalizer add rejected: re-read DeletionTimestamp)")
	approve("pkg/controller/v1beta1/inferencereplica/teardown.go", "Reconciler.removeTeardownFinalizer", "Get", 1, passThroughSpecMetadata+" (finalizer removal)")
	approve("pkg/controller/v1beta1/inferencereplica/release_held.go", "Reconciler.consumeReleaseHeldRequest", "Get", 1, passThroughSpecMetadata+" (annotation consumption)")
	approve("pkg/controller/v1beta1/inferencereplica/reset_instances.go", "Reconciler.consumeResetInstancesRequest", "Get", 1, passThroughSpecMetadata+" (annotation consumption)")

	// ISVC-side raw accessors and their callers.
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector/irstatus_read.go", "ComponentIR", "Get", 1, passThroughTopLevelStatus+" (raw accessor for callers that inspect no rows)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector/status.go", "aggregateOneComponent", "Get", 1, passThroughTopLevelStatus+" (counters, revisions, RolloutHold, Conditions mirrored onto the ISVC)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector/projector.go", "EnsureInferenceReplica", "Get", 1, passThroughSpecMetadata+" (spec projection)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/dispatch.go", "observeCanaryRevisions", "Get", 1, passThroughTopLevelStatus+" (revision pointers)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary/dispatch.go", "reconcileRollbackSignal", "Get", 1, passThroughTopLevelStatus+" (revision pointers and observedGeneration)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/rolloutrun/observe.go", "observeGroupTargets", "Get", 1, passThroughTopLevelStatus+" (revision pointers and replica counters)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/pdb/cutover.go", "OMENativeCutoverReady", "Get", 1, passThroughTopLevelStatus+" (ready and available counters)")
	approve("pkg/controller/v1beta1/inferenceservice/reconcilers/autoscaler/dispatch.go", "controlledByVerifiedModeBridge", "Get", 1, passThroughSpecMetadata+" (ownership: UID, labels, parentRef)")

	// Generated client-go informer: a raw list/watch cache that consumes no
	// rows; row-consuming code reads through the decoded accessor, never
	// through this cache.
	approve("pkg/client/informers/externalversions/ome/v1beta1/inferencereplica.go", "NewFilteredInferenceReplicaInformer", "List", 2, passThroughSpecMetadata+" (generated informer list/watch)")

	// Alfred preserves the raw object; logical-row consumers use its bounded
	// codec adapter. Dispatch reconciliation reads only migration records.
	approve("pkg/alfred/engine/dispatch_reconcile.go", "Dispatcher.reconcileDispatch", "Get", 1, passThroughTopLevelStatus+" (migration records)")
	approve("pkg/alfred/snapshot/builder.go", "Build", "List", 1, "raw capture; per-instance rows decoded through Alfred's bounded codec adapter")
	// CLI readers below still consume stored dense rows directly.
	approve("pkg/cli/cmd/get/registry.go", "<package>", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/cmd/get/registry.go", "<package>", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/cmd/scale/collect.go", "collect", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/instancecollection/collect.go", "CollectRelated", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/migrationcollection/collect.go", "Collect", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/migrationhistorycollection/collect.go", "collectReplicas", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/held_release_source.go", "CollectHeldReleaseEvidence", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/held_release_source.go", "CollectHeldReleaseEvidence", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/migration_evidence.go", "RecheckMigration", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/replicas.go", "collectReplicaEvidence", "List", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/scale_pinned.go", "CollectScalePinnedTargets", "Get", 1, rawReaderOutsideManager)
	approve("pkg/cli/mutate/scale_pinned.go", "ScaleEvidence.Revalidate", "Get", 1, rawReaderOutsideManager)

	inv := loadStatusInventory(t)
	actual := map[fetchSite]int{}
	inv.eachProductionFile(func(pkg *packages.Package, file *ast.File, relative string) {
		collectInferenceReplicaFetches(pkg, file, relative, actual)
	})

	for site, got := range actual {
		want, ok := approved[site]
		if !ok {
			t.Errorf("unclassified InferenceReplica %s in %s:%s (%d): route it through irstatus.GetDecoded or classify it as pass-through", site.method, site.file, site.function, got)
			continue
		}
		if got != want.count {
			t.Errorf("fetch count changed for %s in %s:%s: got %d, want %d (%s)", site.method, site.file, site.function, got, want.count, want.reason)
		}
	}
	for site, want := range approved {
		if _, ok := actual[site]; !ok {
			t.Errorf("stale fetch approval for %s in %s:%s (%s)", site.method, site.file, site.function, want.reason)
		}
	}
}

// A decoded read carries its row decoder on the reader it receives. A bare
// client.Client carries the zero Decoder and fails closed on every ColumnarV2
// object, so a decoded accessor may receive only the codec's Reader or a
// client.Reader parameter that a caller filled under this same rule.
func TestInferenceReplicaDecodedReadsCarryADecoder(t *testing.T) {
	decodedAccessors := map[string]map[string]bool{
		codecPackagePath:     {decodedAccessor: true},
		projectorPackagePath: {"DecodedComponentIR": true, "DecodedComponentIRStatus": true},
	}
	inv := loadStatusInventory(t)
	calls := 0
	inv.eachProductionFile(func(pkg *packages.Package, file *ast.File, relative string) {
		ast.Inspect(file, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok {
				return true
			}
			fn, ok := pkg.TypesInfo.Uses[selector.Sel].(*types.Func)
			if !ok || fn.Pkg() == nil || !decodedAccessors[fn.Pkg().Path()][fn.Name()] || len(call.Args) < 2 {
				return true
			}
			calls++
			typ := pkg.TypesInfo.TypeOf(call.Args[1])
			if typ == nil || readerCarriesDecoder(typ) {
				return true
			}
			t.Errorf("%s:%s passes a %s to %s: wrap it with irstatus.NewReader so the read carries the row decoder",
				relative, enclosingFunction(file, call.Pos()), types.TypeString(typ, nil), fn.Name())
			return true
		})
	})
	if calls == 0 {
		t.Fatal("no decoded-accessor call found; the rule would pass vacuously")
	}
}

// readerCarriesDecoder accepts the codec's Reader, which carries a Decoder by
// construction, and the client.Reader interface, which reaches a decoded
// accessor only as a parameter whose caller is checked by the same rule.
func readerCarriesDecoder(typ types.Type) bool {
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	switch named.Obj().Pkg().Path() {
	case codecPackagePath:
		return named.Obj().Name() == "Reader"
	case controllerRuntimeClientPackagePath:
		return named.Obj().Name() == "Reader"
	}
	return false
}

// The shared fixture package writes the dense representation directly, so its
// read-inventory classification holds only while no production package links
// it. The inventory loads non-test files only, so any importer found here is
// production code; the fixture package itself must be among the loaded
// packages so a rename cannot make the check pass vacuously.
func TestIRStatusFixturePackageImportInventory(t *testing.T) {
	inv := loadStatusInventory(t)
	loaded := false
	var importers []string
	for _, pkg := range inv.pkgs {
		if pkg.PkgPath == fixturePackagePath {
			loaded = true
			continue
		}
		if _, ok := pkg.Imports[fixturePackagePath]; ok {
			importers = append(importers, pkg.PkgPath)
		}
	}
	if !loaded {
		t.Fatalf("fixture package %s is not among the loaded production packages; update fixturePackagePath", fixturePackagePath)
	}
	sort.Strings(importers)
	if len(importers) != 0 {
		t.Fatalf("production packages import the test-only fixture package %s: %v", fixturePackagePath, importers)
	}
}

// assertInferenceReplicaStatusWritesUseSingleWriter is the repo-wide,
// type-aware half of the single-writer contract: every status-subresource
// write of an InferenceReplica, through any client shape, must be the single
// writer.
func assertInferenceReplicaStatusWritesUseSingleWriter(t *testing.T) {
	t.Helper()
	inv := loadStatusInventory(t)
	var sites []string
	inv.eachProductionFile(func(pkg *packages.Package, file *ast.File, relative string) {
		for _, site := range collectInferenceReplicaStatusWrites(pkg, file, relative) {
			if site.function == singleWriter && strings.HasSuffix(site.file, "inferencereplica/status_writer.go") {
				continue
			}
			sites = append(sites, fmt.Sprintf("%s:%s (%s)", site.file, site.function, site.method))
		}
	})
	sort.Strings(sites)
	if len(sites) != 0 {
		t.Fatalf("InferenceReplica status writes outside %s: %v", singleWriter, sites)
	}
}

// --- inventory mechanics ---

type statusInventory struct {
	repoRoot string
	pkgs     []*packages.Package
}

var (
	inventoryOnce sync.Once
	inventoryPkgs []*packages.Package
	inventoryRoot string
	inventoryErr  error
)

func loadStatusInventory(t *testing.T) *statusInventory {
	t.Helper()
	inventoryOnce.Do(func() {
		inventoryRoot = inventoryRepositoryRoot()
		if inventoryRoot == "" {
			inventoryErr = fmt.Errorf("resolve repository root")
			return
		}
		// Every production root of the main module; a root that is its own
		// module (scheduler) cannot be loaded from here and cannot import
		// the main module's API types.
		var patterns []string
		for _, root := range []string{"pkg", "cmd", "internal", "scheduler"} {
			if _, err := os.Stat(filepath.Join(inventoryRoot, root)); err != nil {
				continue
			}
			if _, err := os.Stat(filepath.Join(inventoryRoot, root, "go.mod")); err == nil {
				continue
			}
			patterns = append(patterns, "./"+root+"/...")
		}
		cfg := &packages.Config{
			Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
			Dir:   inventoryRoot,
			Tests: false,
		}
		inventoryPkgs, inventoryErr = packages.Load(cfg, patterns...)
	})
	if inventoryErr != nil {
		t.Fatalf("load production packages: %v", inventoryErr)
	}
	for _, pkg := range inventoryPkgs {
		if len(pkg.Errors) == 0 {
			continue
		}
		if _, usesAPI := pkg.Imports[omeAPIPackagePath]; usesAPI {
			t.Fatalf("package %s did not type-check: %v", pkg.PkgPath, pkg.Errors[0])
		}
		t.Logf("skipping %s (does not import the OME API and did not load: %v)", pkg.PkgPath, pkg.Errors[0])
	}
	return &statusInventory{repoRoot: inventoryRoot, pkgs: inventoryPkgs}
}

func inventoryRepositoryRoot() string {
	_, source, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	for directory := filepath.Dir(source); ; directory = filepath.Dir(directory) {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		if parent := filepath.Dir(directory); parent == directory {
			return ""
		}
	}
}

func (inv *statusInventory) eachProductionFile(visit func(pkg *packages.Package, file *ast.File, relative string)) {
	for _, pkg := range inv.pkgs {
		if len(pkg.Errors) > 0 || pkg.TypesInfo == nil || pkg.Fset == nil {
			continue
		}
		for _, file := range pkg.Syntax {
			path := pkg.Fset.Position(file.Pos()).Filename
			base := filepath.Base(path)
			if strings.HasPrefix(base, "zz_generated.") || base == "openapi_generated.go" || strings.HasSuffix(base, "_test.go") {
				continue
			}
			relative, err := filepath.Rel(inv.repoRoot, path)
			if err != nil || strings.HasPrefix(relative, "..") {
				continue
			}
			visit(pkg, file, filepath.ToSlash(relative))
		}
	}
}

var (
	testInventoryOnce sync.Once
	testInventoryPkgs []*packages.Package
	testInventoryRoot string
	testInventoryErr  error
)

// loadTestInventory loads every package under tests/ together with its test
// files, which is where the integration specs live.
func loadTestInventory(t *testing.T) *statusInventory {
	t.Helper()
	testInventoryOnce.Do(func() {
		testInventoryRoot = inventoryRepositoryRoot()
		if testInventoryRoot == "" {
			testInventoryErr = fmt.Errorf("resolve repository root")
			return
		}
		if _, err := os.Stat(filepath.Join(testInventoryRoot, "tests")); err != nil {
			testInventoryErr = fmt.Errorf("locate tests root: %w", err)
			return
		}
		cfg := &packages.Config{
			Mode:  packages.NeedName | packages.NeedFiles | packages.NeedSyntax | packages.NeedTypes | packages.NeedTypesInfo | packages.NeedImports,
			Dir:   testInventoryRoot,
			Tests: true,
		}
		testInventoryPkgs, testInventoryErr = packages.Load(cfg, "./tests/...")
	})
	if testInventoryErr != nil {
		t.Fatalf("load test packages: %v", testInventoryErr)
	}
	for _, pkg := range testInventoryPkgs {
		if len(pkg.Errors) == 0 {
			continue
		}
		if _, usesAPI := pkg.Imports[omeAPIPackagePath]; usesAPI {
			t.Fatalf("package %s did not type-check: %v", pkg.PkgPath, pkg.Errors[0])
		}
		t.Logf("skipping %s (does not import the OME API and did not load: %v)", pkg.PkgPath, pkg.Errors[0])
	}
	return &statusInventory{repoRoot: testInventoryRoot, pkgs: testInventoryPkgs}
}

// eachTestFile visits every repository file of the loaded test packages once,
// test files included. A package's test variants share its non-test files, so
// files are deduplicated by path.
func (inv *statusInventory) eachTestFile(visit func(pkg *packages.Package, file *ast.File, relative string)) {
	seen := map[string]struct{}{}
	for _, pkg := range inv.pkgs {
		if len(pkg.Errors) > 0 || pkg.TypesInfo == nil || pkg.Fset == nil {
			continue
		}
		for _, file := range pkg.Syntax {
			path := pkg.Fset.Position(file.Pos()).Filename
			if _, visited := seen[path]; visited {
				continue
			}
			seen[path] = struct{}{}
			relative, err := filepath.Rel(inv.repoRoot, path)
			if err != nil || strings.HasPrefix(relative, "..") {
				continue
			}
			visit(pkg, file, filepath.ToSlash(relative))
		}
	}
}

// isOMEAPIType reports whether typ (after pointer dereference) is the named
// type name from the OME API package.
func isOMEAPIType(typ types.Type, name string) bool {
	for {
		pointer, ok := typ.(*types.Pointer)
		if !ok {
			break
		}
		typ = pointer.Elem()
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == omeAPIPackagePath && named.Obj().Name() == name
}

func enclosingFunction(file *ast.File, pos token.Pos) string {
	for _, declaration := range file.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok || pos < function.Pos() || pos > function.End() {
			continue
		}
		if function.Recv == nil || len(function.Recv.List) == 0 {
			return function.Name.Name
		}
		receiver := function.Recv.List[0].Type
		if star, ok := receiver.(*ast.StarExpr); ok {
			receiver = star.X
		}
		if ident, ok := receiver.(*ast.Ident); ok {
			return ident.Name + "." + function.Name.Name
		}
		return function.Name.Name
	}
	return "<package>"
}

func parentMap(file *ast.File) map[ast.Node]ast.Node {
	parents := make(map[ast.Node]ast.Node)
	stack := make([]ast.Node, 0, 16)
	ast.Inspect(file, func(node ast.Node) bool {
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
	return parents
}

func selectorAccessKind(selector ast.Node, parents map[ast.Node]ast.Node) string {
	child := selector
	for parent := parents[child]; parent != nil; child, parent = parent, parents[parent] {
		switch node := parent.(type) {
		case *ast.AssignStmt:
			for _, left := range node.Lhs {
				if left != child {
					continue
				}
				if child != selector || (node.Tok != token.ASSIGN && node.Tok != token.DEFINE) {
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

func collectRepresentationFieldUses(pkg *packages.Package, file *ast.File, relative string, actual map[fieldUse]accessCounts) {
	parents := parentMap(file)
	record := func(pos token.Pos, field, kind string) {
		use := fieldUse{file: relative, function: enclosingFunction(file, pos), field: field}
		counts := actual[use]
		switch kind {
		case "write":
			counts.writes++
		case "read-write":
			counts.readWrites++
		default:
			counts.reads++
		}
		actual[use] = counts
	}
	ast.Inspect(file, func(node ast.Node) bool {
		switch n := node.(type) {
		case *ast.SelectorExpr:
			if _, tracked := representationFields[n.Sel.Name]; !tracked {
				return true
			}
			selection, ok := pkg.TypesInfo.Selections[n]
			if !ok || selection.Kind() != types.FieldVal || !isOMEAPIType(selection.Recv(), "InferenceReplicaStatus") {
				return true
			}
			record(n.Pos(), n.Sel.Name, selectorAccessKind(n, parents))
		case *ast.CompositeLit:
			if !isOMEAPIType(pkg.TypesInfo.TypeOf(n), "InferenceReplicaStatus") {
				return true
			}
			for _, element := range n.Elts {
				keyed, ok := element.(*ast.KeyValueExpr)
				if !ok {
					continue
				}
				key, ok := keyed.Key.(*ast.Ident)
				if !ok {
					continue
				}
				if _, tracked := representationFields[key.Name]; tracked {
					record(key.Pos(), key.Name, "write")
				}
			}
		}
		return true
	})
}

// inferenceReplicaFetch reports whether call fetches an InferenceReplica:
// a Get or List whose object argument, or whose first result, is the
// InferenceReplica or InferenceReplicaList type.
func inferenceReplicaFetch(pkg *packages.Package, call *ast.CallExpr) (string, bool) {
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || (selector.Sel.Name != "Get" && selector.Sel.Name != "List") {
		return "", false
	}
	for _, argument := range call.Args {
		typ := pkg.TypesInfo.TypeOf(argument)
		if typ != nil && (isOMEAPIType(typ, "InferenceReplica") || isOMEAPIType(typ, "InferenceReplicaList")) {
			return selector.Sel.Name, true
		}
	}
	if signature, ok := pkg.TypesInfo.TypeOf(call.Fun).(*types.Signature); ok && signature.Results().Len() > 0 {
		first := signature.Results().At(0).Type()
		if isOMEAPIType(first, "InferenceReplica") || isOMEAPIType(first, "InferenceReplicaList") {
			return selector.Sel.Name, true
		}
	}
	return "", false
}

func collectInferenceReplicaFetches(pkg *packages.Package, file *ast.File, relative string, actual map[fetchSite]int) {
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		method, fetch := inferenceReplicaFetch(pkg, call)
		if !fetch {
			return true
		}
		actual[fetchSite{file: relative, function: enclosingFunction(file, call.Pos()), method: method}]++
		return true
	})
}

// collectInferenceReplicaStatusWrites finds status-subresource writes of an
// InferenceReplica through every client shape: controller-runtime
// Status()/SubResource("status") writers, generated typed clients, and
// dynamic clients.
func collectInferenceReplicaStatusWrites(pkg *packages.Package, file *ast.File, relative string) []fetchSite {
	var sites []fetchSite
	record := func(pos token.Pos, method string) {
		sites = append(sites, fetchSite{file: relative, function: enclosingFunction(file, pos), method: method})
	}
	ast.Inspect(file, func(node ast.Node) bool {
		call, ok := node.(*ast.CallExpr)
		if !ok {
			return true
		}
		selector, ok := call.Fun.(*ast.SelectorExpr)
		if !ok {
			return true
		}
		method := selector.Sel.Name
		switch method {
		case "Update", "Patch", "Apply", "Create":
			if statusWriterReceiver(selector.X) && callTouchesInferenceReplica(pkg, call) {
				record(call.Pos(), "Status()."+method)
			}
		case "UpdateStatus", "ApplyStatus":
			if callTouchesInferenceReplica(pkg, call) || dynamicResourceReceiver(pkg, selector.X) {
				record(call.Pos(), method)
			}
		}
		return true
	})
	return sites
}

// statusWriterReceiver reports whether expr is a Status() call or a
// SubResource("status") call.
func statusWriterReceiver(expr ast.Expr) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	switch selector.Sel.Name {
	case "Status":
		return len(call.Args) == 0
	case "SubResource":
		if len(call.Args) != 1 {
			return false
		}
		literal, ok := call.Args[0].(*ast.BasicLit)
		return ok && literal.Kind == token.STRING && literal.Value == `"status"`
	}
	return false
}

func callTouchesInferenceReplica(pkg *packages.Package, call *ast.CallExpr) bool {
	for _, argument := range call.Args {
		if typ := pkg.TypesInfo.TypeOf(argument); typ != nil && isOMEAPIType(typ, "InferenceReplica") {
			return true
		}
	}
	if signature, ok := pkg.TypesInfo.TypeOf(call.Fun).(*types.Signature); ok && signature.Results().Len() > 0 {
		return isOMEAPIType(signature.Results().At(0).Type(), "InferenceReplica")
	}
	return false
}

func dynamicResourceReceiver(pkg *packages.Package, expr ast.Expr) bool {
	typ := pkg.TypesInfo.TypeOf(expr)
	if typ == nil {
		return false
	}
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == "k8s.io/client-go/dynamic"
}
