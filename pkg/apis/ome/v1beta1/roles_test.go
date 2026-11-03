package v1beta1

import (
	"bufio"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"testing"
)

func TestRolesListEveryComponentTypeOnce(t *testing.T) {
	seen := map[ComponentType]bool{}
	ranks := map[int]ComponentType{}
	for _, r := range Roles {
		if seen[r.Type] {
			t.Errorf("role %q has two rows", r.Type)
		}
		seen[r.Type] = true
		if other, ok := ranks[r.PrimaryRank]; ok {
			t.Errorf("roles %q and %q share primary rank %d", other, r.Type, r.PrimaryRank)
		}
		ranks[r.PrimaryRank] = r.Type
		if r.ReadyCondition == "" || r.ServiceField == "" || r.RuntimeField == "" {
			t.Errorf("role %q leaves a column empty: %+v", r.Type, r)
		}
		if r.ServiceField != "spec."+string(r.Type) {
			t.Errorf("role %q is declared at %q; a role's block is named for the role", r.Type, r.ServiceField)
		}
	}
	for _, c := range declaredComponentTypes(t) {
		if !seen[c] {
			t.Errorf("ComponentType %q has no row in Roles", c)
		}
	}
}

// Every typed block a service or a runtime declares must be readable through
// the view, or a rule that walks the roles silently skips that role.
func TestViewReadsEveryRoleBlock(t *testing.T) {
	specs := RoleSpecs{}
	value := reflect.ValueOf(&specs).Elem()
	for i := 0; i < value.NumField(); i++ {
		field := value.Field(i)
		field.Set(reflect.New(field.Type().Elem()))
	}
	views := specs.Declared()
	if len(views) != value.NumField() || len(views) != len(Roles) {
		t.Fatalf("a RoleSpecs with every block set yields %d views; it has %d blocks and there are %d roles", len(views), value.NumField(), len(Roles))
	}
	for i, view := range views {
		if view.Type != Roles[i].Type {
			t.Errorf("view %d is role %q, want %q: views follow the order of Roles", i, view.Type, Roles[i].Type)
		}
		if view.PodSpec == nil || view.Extension == nil {
			t.Errorf("role %q: the view does not reach the block's pod spec and extension", view.Type)
		}
		spec := &InferenceServiceSpec{}
		if !DeclareRole(spec, view.Type) || SpecOf(spec, view.Type) == nil {
			t.Errorf("role %q: DeclareRole does not give the service a block SpecOf reads", view.Type)
		}
	}
	if (RoleSpecs{}).View("not-a-role") != nil || DeclareRole(&InferenceServiceSpec{}, "not-a-role") {
		t.Error("a value that names no role reads as a role")
	}
}

func TestViewWritesThroughToTheBlock(t *testing.T) {
	spec := &InferenceServiceSpec{Decoder: &DecoderSpec{}}
	view := SpecOf(spec, DecoderComponent)
	replicas := 4
	view.Extension.MinReplicas = &replicas
	view.PodSpec.SchedulerName = "example-scheduler"
	if spec.Decoder.MinReplicas == nil || *spec.Decoder.MinReplicas != 4 || spec.Decoder.SchedulerName != "example-scheduler" {
		t.Errorf("writing through the view did not reach the decoder block: %+v", spec.Decoder)
	}
}

func TestRolesByPrimaryRankFollowsTheRank(t *testing.T) {
	order := RolesByPrimaryRank()
	for i := 1; i < len(order); i++ {
		prev, _ := RoleOf(order[i-1])
		cur, _ := RoleOf(order[i])
		if prev.PrimaryRank > cur.PrimaryRank {
			t.Errorf("%q (rank %d) comes before %q (rank %d)", prev.Type, prev.PrimaryRank, cur.Type, cur.PrimaryRank)
		}
	}
}

// declaredComponentTypes returns every ComponentType constant this package
// declares.
func declaredComponentTypes(t *testing.T) []ComponentType {
	t.Helper()
	consts := typedConstants(t, ".", "ComponentType")
	if len(consts) == 0 {
		t.Fatal("no ComponentType constant is declared")
	}
	out := make([]ComponentType, 0, len(consts))
	for _, value := range consts {
		out = append(out, ComponentType(value))
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return out
}

// typedConstants parses the non-test files of the package in dir and returns,
// by name, the string value of every constant declared with the named type.
func typedConstants(t *testing.T, dir, typeName string) map[string]string {
	t.Helper()
	pkgs, err := parser.ParseDir(token.NewFileSet(), dir, func(fi os.FileInfo) bool {
		return !strings.HasSuffix(fi.Name(), "_test.go")
	}, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", dir, err)
	}
	out := map[string]string{}
	for _, pkg := range pkgs {
		for _, file := range pkg.Files {
			for _, decl := range file.Decls {
				gen, ok := decl.(*ast.GenDecl)
				if !ok || gen.Tok != token.CONST {
					continue
				}
				for _, s := range gen.Specs {
					vs := s.(*ast.ValueSpec)
					if ident, ok := vs.Type.(*ast.Ident); !ok || ident.Name != typeName {
						continue
					}
					for i, v := range vs.Values {
						lit, ok := v.(*ast.BasicLit)
						if !ok {
							t.Fatalf("%s: a %s constant is not a plain string literal", dir, typeName)
						}
						value, err := strconv.Unquote(lit.Value)
						if err != nil {
							t.Fatalf("unquote %s: %v", lit.Value, err)
						}
						out[vs.Names[i].Name] = value
					}
				}
			}
		}
	}
	return out
}

var updateRoleRecord = flag.Bool("update-role-record", false, "lower testdata/role_enumerations.txt to the tree; never adds an entry")

// roleEnumerationRecord lists, per file and function, the hand-written role
// enumerations the tree still carries.
const roleEnumerationRecord = "testdata/role_enumerations.txt"

// A rule that needs every role walks Roles or reads a role through SpecOf and
// PieceOf, so a new role is one more row in roles.go. A switch or a literal
// that spells out two or more roles by hand is a place a new role is
// forgotten without an error. The ones the tree still carries are recorded;
// an entry may fall or leave, never rise or join.
func TestNoNewHandWrittenRoleEnumerations(t *testing.T) {
	root, err := filepath.Abs(filepath.Join("..", "..", "..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	got := scanRoleEnumerations(t, root)
	want := readRoleRecord(t)
	if *updateRoleRecord {
		lowered := map[string]int{}
		for key, allowed := range want {
			if n := got[key]; n > 0 {
				lowered[key] = min(n, allowed)
			}
		}
		var raised []string
		for key, n := range got {
			if n > want[key] {
				raised = append(raised, fmt.Sprintf("%s: %d hand-written role enumeration(s), the record allows %d", key, n, want[key]))
			}
		}
		if len(raised) > 0 {
			sort.Strings(raised)
			t.Fatalf("the update only lowers the record; walk v1beta1.Roles or read a role through SpecOf or PieceOf instead:\n%s", strings.Join(raised, "\n"))
		}
		writeRoleRecord(t, lowered)
		return
	}
	var problems []string
	for key, n := range got {
		if allowed := want[key]; n > allowed {
			problems = append(problems, fmt.Sprintf("%s: %d hand-written role enumeration(s), the record allows %d; walk v1beta1.Roles or read a role through SpecOf or PieceOf instead", key, n, allowed))
		}
	}
	for key, allowed := range want {
		if n := got[key]; n < allowed {
			problems = append(problems, fmt.Sprintf("%s: the record allows %d but the code has %d; lower the entry in %s (the record only shrinks)", key, allowed, n, roleEnumerationRecord))
		}
	}
	sort.Strings(problems)
	for _, p := range problems {
		t.Error(p)
	}
}

// roleIdentifier is the family an identifier that names a role belongs to,
// and the package name it must be qualified with to count, when its bare name
// is too common to mean a role on its own.
type roleIdentifier struct {
	family    string
	qualifier string
}

// roleIdentifiers returns, by name, every identifier that names a role: the
// API's ComponentType constants, their workload mirrors, the component
// constants in pkg/constants, and the typed role blocks RoleSpecs holds. They
// are read from the code, so a role is counted as soon as it is declared.
func roleIdentifiers(t *testing.T, root string) map[string]roleIdentifier {
	t.Helper()
	ids := map[string]roleIdentifier{}
	families := []struct{ family, dir, typeName, qualifier string }{
		{"api", ".", "ComponentType", ""},
		{"workload", filepath.Join(root, "pkg", "controller", "v1beta1", "workload", "types"), "ComponentType", ""},
		{"constants", filepath.Join(root, "pkg", "constants"), "InferenceServiceComponent", "constants"},
	}
	for _, f := range families {
		consts := typedConstants(t, f.dir, f.typeName)
		if len(consts) == 0 {
			t.Fatalf("no %s constant is declared in %s", f.typeName, f.dir)
		}
		for name := range consts {
			ids[name] = roleIdentifier{family: f.family, qualifier: f.qualifier}
		}
	}
	blocks := reflect.TypeOf(RoleSpecs{})
	for i := 0; i < blocks.NumField(); i++ {
		ids[blocks.Field(i).Type.Elem().Name()] = roleIdentifier{family: "spec"}
	}
	return ids
}

// scanRoleEnumerations counts, per "file function" key, the switch and type
// switch statements whose cases, and the composite literals whose keys,
// values or row elements, name two or more roles of one family, over the
// non-test, non-generated Go files of the core roots. A field read or field
// key (spec.Engine, Engine: x) and an if-chain are not counted. roles.go is
// the table and is not counted.
func scanRoleEnumerations(t *testing.T, root string) map[string]int {
	t.Helper()
	ids := roleIdentifiers(t, root)
	counts := map[string]int{}
	for _, dir := range []string{"pkg", "cmd", "internal", "scheduler"} {
		base := filepath.Join(root, dir)
		if _, err := os.Stat(base); err != nil {
			continue
		}
		err := filepath.WalkDir(base, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}
			rel, _ := filepath.Rel(root, path)
			rel = filepath.ToSlash(rel)
			if d.IsDir() {
				if rel == "pkg/client" || rel == "pkg/openapi" || d.Name() == "testdata" {
					return filepath.SkipDir
				}
				return nil
			}
			if !strings.HasSuffix(rel, ".go") || strings.HasSuffix(rel, "_test.go") ||
				strings.Contains(filepath.Base(rel), "zz_generated") || rel == "pkg/apis/ome/v1beta1/roles.go" {
				return nil
			}
			file, err := parser.ParseFile(token.NewFileSet(), path, nil, parser.SkipObjectResolution)
			if err != nil {
				return fmt.Errorf("parse %s: %w", rel, err)
			}
			for _, decl := range file.Decls {
				name := declName(decl)
				ast.Inspect(decl, func(n ast.Node) bool {
					var exprs []ast.Expr
					switch n := n.(type) {
					case *ast.SwitchStmt:
						for _, s := range n.Body.List {
							exprs = append(exprs, s.(*ast.CaseClause).List...)
						}
					case *ast.TypeSwitchStmt:
						for _, s := range n.Body.List {
							for _, e := range s.(*ast.CaseClause).List {
								if star, ok := e.(*ast.StarExpr); ok {
									e = star.X
								}
								exprs = append(exprs, e)
							}
						}
					case *ast.CompositeLit:
						for _, e := range n.Elts {
							if row, ok := e.(*ast.CompositeLit); ok {
								exprs = append(exprs, literalParts(row)...)
								continue
							}
							exprs = append(exprs, literalParts(&ast.CompositeLit{Elts: []ast.Expr{e}})...)
						}
					default:
						return true
					}
					if namesTwoRoles(ids, exprs) {
						counts[rel+" "+name]++
					}
					return true
				})
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	return counts
}

// literalParts returns the keys and values of a composite literal's elements.
func literalParts(lit *ast.CompositeLit) []ast.Expr {
	var out []ast.Expr
	for _, e := range lit.Elts {
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			out = append(out, kv.Key, kv.Value)
			continue
		}
		out = append(out, e)
	}
	return out
}

// namesTwoRoles reports whether the expressions name two or more distinct
// roles of one family. A conversion such as string(EngineComponent) names the
// role it converts.
func namesTwoRoles(ids map[string]roleIdentifier, exprs []ast.Expr) bool {
	families := map[string]map[string]bool{}
	for _, e := range exprs {
		for {
			if paren, ok := e.(*ast.ParenExpr); ok {
				e = paren.X
				continue
			}
			if call, ok := e.(*ast.CallExpr); ok && len(call.Args) == 1 {
				e = call.Args[0]
				continue
			}
			break
		}
		var name, qualifier string
		switch e := e.(type) {
		case *ast.Ident:
			name = e.Name
		case *ast.SelectorExpr:
			name = e.Sel.Name
			if pkg, ok := e.X.(*ast.Ident); ok {
				qualifier = pkg.Name
			}
		default:
			continue
		}
		id, ok := ids[name]
		if !ok || (id.qualifier != "" && id.qualifier != qualifier) {
			continue
		}
		if families[id.family] == nil {
			families[id.family] = map[string]bool{}
		}
		families[id.family][name] = true
	}
	for _, names := range families {
		if len(names) >= 2 {
			return true
		}
	}
	return false
}

// declName names a top-level declaration: "(Recv).Name" or "Name" for a
// function, "var" or "const" for a declaration block.
func declName(decl ast.Decl) string {
	switch d := decl.(type) {
	case *ast.FuncDecl:
		if d.Recv == nil || len(d.Recv.List) == 0 {
			return d.Name.Name
		}
		recv := d.Recv.List[0].Type
		if star, ok := recv.(*ast.StarExpr); ok {
			recv = star.X
		}
		if index, ok := recv.(*ast.IndexExpr); ok {
			recv = index.X
		}
		if ident, ok := recv.(*ast.Ident); ok {
			return "(" + ident.Name + ")." + d.Name.Name
		}
		return d.Name.Name
	case *ast.GenDecl:
		return d.Tok.String()
	}
	return "?"
}

func readRoleRecord(t *testing.T) map[string]int {
	t.Helper()
	f, err := os.Open(roleEnumerationRecord)
	if err != nil {
		t.Fatalf("read %s: %v", roleEnumerationRecord, err)
	}
	defer f.Close()
	record := map[string]int{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) != 3 {
			t.Fatalf("%s: %q is not \"<count> <file> <function>\"", roleEnumerationRecord, line)
		}
		n, err := strconv.Atoi(fields[0])
		if err != nil || n <= 0 {
			t.Fatalf("%s: %q does not start with a positive count", roleEnumerationRecord, line)
		}
		record[fields[1]+" "+fields[2]] = n
	}
	if err := scanner.Err(); err != nil {
		t.Fatal(err)
	}
	return record
}

func writeRoleRecord(t *testing.T, counts map[string]int) {
	t.Helper()
	keys := make([]string, 0, len(counts))
	for key := range counts {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	var b strings.Builder
	b.WriteString("# Hand-written role enumerations the tree still carries: <count> <file> <function>.\n")
	b.WriteString("# TestNoNewHandWrittenRoleEnumerations fails when a count rises, an entry joins, or the\n")
	b.WriteString("# code falls below its entry. Walk v1beta1.Roles or read a role through SpecOf/PieceOf\n")
	b.WriteString("# instead of adding one. go test ./pkg/apis/ome/v1beta1/ -update-role-record lowers\n")
	b.WriteString("# entries to the code and drops the ones it no longer has; it never adds one.\n")
	for _, key := range keys {
		fmt.Fprintf(&b, "%d %s\n", counts[key], key)
	}
	if err := os.WriteFile(roleEnumerationRecord, []byte(b.String()), 0o644); err != nil {
		t.Fatal(err)
	}
}
