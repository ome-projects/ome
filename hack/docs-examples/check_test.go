package main

import (
	"context"
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/google/go-cmp/cmp"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// examplesPage has one example of each outcome. Tilde fences keep it a raw
// string.
const examplesPage = `# Examples

~~~yaml title="valid.yaml"
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: llama-3-1-8b
spec:
  modelFormat:
    name: safetensors
  storage:
    storageUri: hf://meta-llama/Llama-3.1-8B-Instruct
---
apiVersion: v1
kind: Namespace
metadata:
  name: llama
---
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: llama
  namespace: llama
spec:
  model:
    name: llama-3-1-8b
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: in-default
data:
  key: value
---
apiVersion: v1
kind: ConfigMap
metadata:
  generateName: generated-
data:
  key: value
~~~

~~~yaml
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: unknown-field
spec:
  predictor:
    name: llama-3-1-8b
---
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: bad-enum
spec:
  deploymentMode: Serverless
---
apiVersion: ome.io/v1beta1
kind: ClusterBaseModel
metadata:
  name: missing-storage-uri
spec:
  storage:
    path: /models
---
apiVersion: v1
kind: Service
metadata:
  name: bad-port
spec:
  ports:
    - port: 700000
---
apiVersion: ome.io/v1beta1
kind: Model
metadata:
  name: unknown-kind
---
apiVersion: ome.io/v1alpha1
kind: InferenceService
metadata:
  name: unserved-version
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: duplicate-key
  name: again
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: bad-namespace
  namespace: Bad_NS
---
apiVersion: v1
kind: ConfigMap
metadata:
  generateName: generated-
unknown: field
---
apiVersion: v1
kind: Config
clusters: []
---
apiVersion: ome.io/v1beta1/extra
kind: InferenceService
metadata:
  name: bad-api-version
~~~

~~~yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: not-installed
~~~

~~~yaml check=skip
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  name: marked-skip
spec:
  deploymentMode: Serverless
~~~

~~~yaml
spec:
  model:
    name: a-fragment
~~~
`

func TestCheck(t *testing.T) {
	requireEnvtest(t)
	log.SetLogger(logr.Discard())
	env := &envtest.Environment{CRDDirectoryPaths: []string{crdDir}, ErrorIfCRDPathMissing: true}
	// Register Stop first: Start can fail after etcd and the API server are
	// running.
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Errorf("stop envtest: %v", err)
		}
	})
	cfg, err := env.Start()
	if err != nil {
		t.Fatalf("start envtest: %v", err)
	}
	c, err := newChecker(cfg)
	if err != nil {
		t.Fatal(err)
	}

	examples, rep := parse(extractDocuments("page.md", examplesPage))
	if err := c.check(context.Background(), examples, rep); err != nil {
		t.Fatalf("check() error: %v", err)
	}

	// The five valid objects and the six invalid ones the server knows.
	if rep.checked != 11 {
		t.Errorf("checked = %d, want 11", rep.checked)
	}
	wantSkipped := map[string]int{"keda.sh/v1alpha1 ScaledObject": 1}
	if diff := cmp.Diff(wantSkipped, rep.skipped); diff != "" {
		t.Errorf("skipped mismatch (-want +got):\n%s", diff)
	}
	want := []struct {
		line            int
		object, message string
	}{
		{44, "InferenceService unknown-field", `unknown field "spec.predictor"`},
		{52, "InferenceService bad-enum", `Unsupported value: "Serverless"`},
		{59, "ClusterBaseModel missing-storage-uri", "spec.storage.storageUri: Required value"},
		{67, "Service bad-port", "spec.ports[0].port: Invalid value: 700000"},
		{75, "Model unknown-kind", "the API server has no kind Model in ome.io/v1beta1"},
		{80, "InferenceService unserved-version", "the API server has no kind InferenceService in ome.io/v1alpha1"},
		{85, "", "invalid YAML: yaml: unmarshal errors:\n  line 89: key \"name\" already set in map"},
		{91, "ConfigMap bad-namespace", `create namespace Bad_NS: Namespace "Bad_NS" is invalid`},
		{97, "ConfigMap generated-", `unknown field "unknown"`},
		{103, "Config", "no metadata.name or metadata.generateName: add metadata.name, or mark the block check=skip"},
		{107, "InferenceService bad-api-version", `invalid apiVersion "ome.io/v1beta1/extra": unexpected GroupVersion string: ome.io/v1beta1/extra`},
	}
	if len(rep.failures) != len(want) {
		t.Fatalf("got %d failures, want %d:\n%s", len(rep.failures), len(want), failureList(rep.failures))
	}
	for i, w := range want {
		got := rep.failures[i]
		if got.path != "page.md" || got.line != w.line || got.object != w.object || !strings.Contains(got.message, w.message) {
			t.Errorf("failure %d = %q, want line %d, object %q and a message containing %q", i, got, w.line, w.object, w.message)
		}
	}
}

func TestParse(t *testing.T) {
	docs := extractDocuments("page.md", `~~~yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: named
---
apiVersion: ome.io/v1beta1
kind: InferenceService
metadata:
  generateName: generated-
---
apiVersion: v1
kind: Config
clusters: []
---
apiVersion: ome.io/v1beta1/extra
kind: InferenceService
metadata:
  name: bad-api-version
---
spec:
  model:
    name: a-fragment
---
a: 1
a: 2
~~~
`)
	examples, rep := parse(docs)

	type object struct {
		line   int
		gvk    schema.GroupVersionKind
		object string
	}
	var got []object
	for _, ex := range examples {
		got = append(got, object{line: ex.doc.line, gvk: ex.gvk, object: describe(ex.obj)})
	}
	want := []object{
		{line: 2, gvk: schema.GroupVersionKind{Version: "v1", Kind: "ConfigMap"}, object: "ConfigMap named"},
		{line: 7, gvk: schema.GroupVersionKind{Group: "ome.io", Version: "v1beta1", Kind: "InferenceService"}, object: "InferenceService generated-"},
	}
	if diff := cmp.Diff(want, got, cmp.AllowUnexported(object{})); diff != "" {
		t.Errorf("parse() objects mismatch (-want +got):\n%s", diff)
	}
	wantFailures := []failure{
		{path: "page.md", line: 12, object: "Config", message: "no metadata.name or metadata.generateName: add metadata.name, or mark the block check=skip"},
		{path: "page.md", line: 16, object: "InferenceService bad-api-version", message: `invalid apiVersion "ome.io/v1beta1/extra": unexpected GroupVersion string: ome.io/v1beta1/extra`},
		{path: "page.md", line: 25, message: "invalid YAML: yaml: unmarshal errors:\n  line 26: key \"a\" already set in map"},
	}
	if diff := cmp.Diff(wantFailures, rep.failures, cmp.AllowUnexported(failure{})); diff != "" {
		t.Errorf("parse() failures mismatch (-want +got):\n%s", diff)
	}
}

func TestPrint(t *testing.T) {
	tests := []struct {
		name string
		rep  report
		want string
	}{
		{
			name: "no problems",
			rep:  report{checked: 1},
			want: "Checked 1 object: 0 problems.\n",
		},
		{
			name: "problems and skipped kinds",
			rep: report{
				checked: 2,
				failures: []failure{
					{path: "a.md", line: 3, message: "invalid YAML: yaml: line 4: could not find expected ':'"},
					{path: "b.md", line: 7, object: "ConfigMap settings", message: "the API server has no kind ConfigMap in v2"},
				},
				skipped: map[string]int{"keda.sh/v1alpha1 ScaledObject": 2, "gateway.networking.k8s.io/v1 HTTPRoute": 1},
			},
			want: `a.md:3: invalid YAML: yaml: line 4: could not find expected ':'
b.md:7: ConfigMap settings: the API server has no kind ConfigMap in v2
Checked 2 objects: 2 problems.
Skipped objects of kinds whose CRDs aren't installed:
  gateway.networking.k8s.io/v1 HTTPRoute: 1
  keda.sh/v1alpha1 ScaledObject: 2
`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var b strings.Builder
			tc.rep.print(&b)
			if diff := cmp.Diff(tc.want, b.String()); diff != "" {
				t.Errorf("print() mismatch (-want +got):\n%s", diff)
			}
		})
	}
}

func failureList(failures []failure) string {
	var b strings.Builder
	for _, f := range failures {
		b.WriteString(f.String() + "\n")
	}
	return b.String()
}
