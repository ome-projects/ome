package guard

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/google/cel-go/cel"
	admissionv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/version"
	admissioncel "k8s.io/apiserver/pkg/admission/plugin/cel"
	"k8s.io/apiserver/pkg/cel/environment"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestMissingGuardWithholdsExecution(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := admissionv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	g := Guard{Reader: fake.NewClientBuilder().WithScheme(scheme).Build(), Namespace: "ome", ServiceAccount: "ome-alfred"}
	if err := g.Check(context.Background()); err == nil {
		t.Fatal("missing guard authorized execution")
	}
}

type failingReader struct{ client.Reader }

func (f failingReader) Get(context.Context, client.ObjectKey, client.Object, ...client.GetOption) error {
	return errors.New("API unavailable")
}

func TestGuardWithholdsOnUnavailableBindingAndAPI(t *testing.T) {
	scheme := runtime.NewScheme()
	_ = admissionv1.AddToScheme(scheme)
	p := Policy(PolicyName, "system:serviceaccount:ome:ome-alfred")
	p.Generation = 1
	p.Status.ObservedGeneration = 1
	p.Status.TypeChecking = &admissionv1.TypeChecking{}
	for _, reader := range []client.Reader{fake.NewClientBuilder().WithScheme(scheme).WithObjects(p).Build(), failingReader{}, nil} {
		g := Guard{Reader: reader, Namespace: "ome", ServiceAccount: "ome-alfred"}
		if err := g.Check(context.Background()); err == nil {
			t.Fatal("unavailable guard authorized execution")
		}
	}
}

func TestGuardRejectsDriftAndUncheckedStatus(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*admissionv1.ValidatingAdmissionPolicy, *admissionv1.ValidatingAdmissionPolicyBinding)
		want   bool
	}{
		{"canonical", func(_ *admissionv1.ValidatingAdmissionPolicy, _ *admissionv1.ValidatingAdmissionPolicyBinding) {}, true},
		{"ignore failures", func(p *admissionv1.ValidatingAdmissionPolicy, _ *admissionv1.ValidatingAdmissionPolicyBinding) {
			v := admissionv1.Ignore
			p.Spec.FailurePolicy = &v
		}, false},
		{"no deny", func(_ *admissionv1.ValidatingAdmissionPolicy, b *admissionv1.ValidatingAdmissionPolicyBinding) {
			b.Spec.ValidationActions = []admissionv1.ValidationAction{admissionv1.Audit}
		}, false},
		{"expression", func(p *admissionv1.ValidatingAdmissionPolicy, _ *admissionv1.ValidatingAdmissionPolicyBinding) {
			p.Spec.Validations[0].Expression = "true"
		}, false},
		{"actor", func(p *admissionv1.ValidatingAdmissionPolicy, _ *admissionv1.ValidatingAdmissionPolicyBinding) {
			p.Spec.MatchConditions[0].Expression = "false"
		}, false},
		{"rules", func(p *admissionv1.ValidatingAdmissionPolicy, _ *admissionv1.ValidatingAdmissionPolicyBinding) {
			p.Spec.MatchConstraints.ResourceRules[0].Resources = []string{"pods"}
		}, false},
		{"unchecked", func(p *admissionv1.ValidatingAdmissionPolicy, _ *admissionv1.ValidatingAdmissionPolicyBinding) {
			p.Status.TypeChecking = nil
		}, false},
		{"unobserved", func(p *admissionv1.ValidatingAdmissionPolicy, _ *admissionv1.ValidatingAdmissionPolicyBinding) {
			p.Status.ObservedGeneration = 0
		}, false},
		{"warning", func(p *admissionv1.ValidatingAdmissionPolicy, _ *admissionv1.ValidatingAdmissionPolicyBinding) {
			p.Status.TypeChecking.ExpressionWarnings = []admissionv1.ExpressionWarning{{FieldRef: "spec.validations[0].expression", Warning: "unknown field"}}
		}, false},
		{"binding selector", func(_ *admissionv1.ValidatingAdmissionPolicy, b *admissionv1.ValidatingAdmissionPolicyBinding) {
			b.Spec.MatchResources = &admissionv1.MatchResources{}
		}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, b := Policy(PolicyName, "system:serviceaccount:ome:ome-alfred"), Binding(BindingName, PolicyName)
			p.Generation = 1
			p.Status.ObservedGeneration = 1
			p.Status.TypeChecking = &admissionv1.TypeChecking{}
			// An empty constructor is a missing security boundary, not a usable fixture.
			if len(p.Spec.Validations) == 0 || len(p.Spec.MatchConditions) == 0 {
				t.Fatal("canonical admission restrictions missing")
			}
			tc.mutate(p, b)
			scheme := runtime.NewScheme()
			_ = admissionv1.AddToScheme(scheme)
			g := Guard{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(p, b).Build(), Namespace: "ome", ServiceAccount: "ome-alfred"}
			if err := g.Check(context.Background()); (err == nil) != tc.want {
				t.Fatalf("Check() = %v, allowed want %v", err, tc.want)
			}
		})
	}
}

type expression string

func (e expression) GetExpression() string    { return string(e) }
func (e expression) ReturnTypes() []*cel.Type { return []*cel.Type{cel.BoolType} }

// TestCELBoundary catches weakening of any preservation or additive-only rule.
// Use the Kubernetes admission compiler with the 1.30 compatibility environment.
func TestCELBoundary(t *testing.T) {
	p := Policy(PolicyName, "system:serviceaccount:ome:ome-alfred")
	if len(p.Spec.Validations) == 0 {
		t.Fatal("canonical admission restrictions missing")
	}
	compiler := admissioncel.NewCompiler(environment.MustBaseEnvSet(version.MajorMinor(1, 30)))
	var programs []cel.Program
	for _, v := range p.Spec.Validations {
		result := compiler.CompileCELExpression(expression(v.Expression), admissioncel.OptionalVariableDeclarations{}, environment.NewExpressions)
		if result.Error != nil {
			t.Fatalf("compile %s: %v", v.Expression, result.Error)
		}
		programs = append(programs, result.Program)
	}
	const key = "ome.io/migration-request-v1-12345678-1234-1234-1234-123456789abc"
	base := map[string]any{"apiVersion": "ome.io/v1beta1", "kind": "InferenceService", "metadata": map[string]any{"name": "test", "namespace": "workloads", "uid": "id", "resourceVersion": "1", "annotations": map[string]any{"owner": "keep"}}, "spec": map[string]any{"replicas": int64(1)}, "status": map[string]any{"ready": true}}
	for _, tc := range []struct {
		name   string
		mutate func(map[string]any)
		want   bool
	}{
		{"noop", func(_ map[string]any) {}, true},
		{"add request", func(o map[string]any) { o["metadata"].(map[string]any)["annotations"].(map[string]any)[key] = `{}` }, true},
		{"byte limit", func(o map[string]any) {
			o["metadata"].(map[string]any)["annotations"].(map[string]any)[key] = strings.Repeat("x", 4096)
		}, true},
		{"oversize", func(o map[string]any) {
			o["metadata"].(map[string]any)["annotations"].(map[string]any)[key] = strings.Repeat("x", 4097)
		}, false},
		{"multibyte oversize", func(o map[string]any) {
			o["metadata"].(map[string]any)["annotations"].(map[string]any)[key] = strings.Repeat("界", 1366)
		}, false},
		{"wrong version", func(o map[string]any) {
			o["metadata"].(map[string]any)["annotations"].(map[string]any)[strings.Replace(key, "-v1-", "-v2-", 1)] = `{}`
		}, false},
		{"invalid uuid", func(o map[string]any) {
			o["metadata"].(map[string]any)["annotations"].(map[string]any)["ome.io/migration-request-v1-xxx"] = `{}`
		}, false},
		{"two additions", func(o map[string]any) {
			a := o["metadata"].(map[string]any)["annotations"].(map[string]any)
			a[key] = `{}`
			a[strings.Replace(key, "abc", "def", 1)] = `{}`
		}, false},
		{"unrelated addition", func(o map[string]any) { o["metadata"].(map[string]any)["annotations"].(map[string]any)["other"] = "x" }, false},
		{"replace", func(o map[string]any) { o["metadata"].(map[string]any)["annotations"].(map[string]any)["owner"] = "x" }, false},
		{"remove", func(o map[string]any) {
			delete(o["metadata"].(map[string]any)["annotations"].(map[string]any), "owner")
		}, false},
		{"remove all", func(o map[string]any) { delete(o["metadata"].(map[string]any), "annotations") }, false},
		{"spec", func(o map[string]any) { o["spec"].(map[string]any)["replicas"] = float64(2) }, false},
		{"status", func(o map[string]any) { o["status"].(map[string]any)["ready"] = false }, false},
		{"labels", func(o map[string]any) { o["metadata"].(map[string]any)["labels"] = map[string]any{"x": "y"} }, false},
		{"owners", func(o map[string]any) {
			o["metadata"].(map[string]any)["ownerReferences"] = []any{map[string]any{"name": "x"}}
		}, false},
		{"finalizers", func(o map[string]any) { o["metadata"].(map[string]any)["finalizers"] = []any{"x"} }, false},
		{"generate name", func(o map[string]any) { o["metadata"].(map[string]any)["generateName"] = "x" }, false},
		{"self link", func(o map[string]any) { o["metadata"].(map[string]any)["selfLink"] = "/changed" }, false},
		{"deletion timestamp", func(o map[string]any) { o["metadata"].(map[string]any)["deletionTimestamp"] = "2026-01-01T00:00:00Z" }, false},
		{"server bookkeeping", func(o map[string]any) {
			o["metadata"].(map[string]any)["resourceVersion"] = "2"
			o["metadata"].(map[string]any)["managedFields"] = []any{}
		}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			b, _ := json.Marshal(base)
			var old, obj map[string]any
			_ = json.Unmarshal(b, &old)
			_ = json.Unmarshal(b, &obj)
			tc.mutate(obj)
			allowed := true
			for _, program := range programs {
				out, _, err := program.Eval(map[string]any{"object": obj, "oldObject": old})
				if err != nil || out.Value() != true {
					allowed = false
				}
			}
			if allowed != tc.want {
				t.Fatalf("allowed = %v, want %v", allowed, tc.want)
			}
		})
	}
}
