package statusrepair

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	kubefake "k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/transitionpreflight"
)

const (
	testInventory = `managerImage: registry.example.com/ome/manager@sha256:0000000000000000000000000000000000000000000000000000000000000000
omenativeStatus:
  instanceStatusEncoding: ColumnarV2
  maxDecodedInstances: 100
manager:
  namespace: ome
  deployment: ome-controller-manager
  container: manager
  configMap: inferenceservice-config
pageSize: 50
clusters:
- name: east
  kubeconfig: /nonexistent/east.kubeconfig
  context: east
`
	testReplacement = `instanceStatuses:
- index: 0
  incarnation: 1
  phase: Ready
  runningRevision: example-engine-2f32f6fe
  podCount: 1
  servingPodCount: 1
  availablePodCount: 1
  admitted: true
- index: 1
  incarnation: 1
  phase: Ready
  runningRevision: example-engine-2f32f6fe
  podCount: 1
  servingPodCount: 1
  availablePodCount: 1
  admitted: true
`
	testColumnarReplacement = `instanceStatusEncoding: ColumnarV2
instanceStatusColumns:
  members: "0-1"
  phases:
  - value: Ready
    indexes: "0-1"
`
)

func writeFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write %s: %v", name, err)
	}
	return path
}

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	return scheme
}

func statusConfigMap(block string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: "ome", Name: "inferenceservice-config"},
		Data:       map[string]string{"omenativeStatus": block},
	}
}

func storedIR(t *testing.T) *v1beta1.InferenceReplica {
	t.Helper()
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Namespace: "serving", Name: "example-engine"},
		Spec:       v1beta1.InferenceReplicaSpec{Component: v1beta1.EngineComponent},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas:        2,
			CurrentRevision: "example-engine-2f32f6fe",
			Conditions:      []metav1.Condition{{Type: "Ready", Status: metav1.ConditionFalse, Reason: "Repairing", LastTransitionTime: metav1.Now()}},
		},
	}
	// The stored payload is a ColumnarV2 object the codec refuses.
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatusEncoding = &encoding
	ir.Status.InstanceStatusColumns = &v1beta1.InstanceStatusColumns{Members: "1-0", Phases: []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "1-0"}}}
	return ir
}

type fixture struct {
	opts    Options
	connect Connector
	kube    client.Client
}

func newFixture(t *testing.T, configBlock string) *fixture {
	t.Helper()
	dir := t.TempDir()
	ir := storedIR(t)
	c := fake.NewClientBuilder().WithScheme(testScheme(t)).WithObjects(ir).WithStatusSubresource(&v1beta1.InferenceReplica{}).Build()
	kube := kubefake.NewClientset(statusConfigMap(configBlock))
	return &fixture{
		opts: Options{
			InventoryPath:   writeFile(t, dir, "fleet.yaml", testInventory),
			Cluster:         "east",
			Namespace:       ir.Namespace,
			Name:            ir.Name,
			ReplacementPath: writeFile(t, dir, "rows.yaml", testReplacement),
		},
		connect: func(_ context.Context, cluster transitionpreflight.Cluster) (*Clients, error) {
			if cluster.Name != "east" {
				t.Fatalf("connector received cluster %q", cluster.Name)
			}
			return &Clients{Kube: kube, Client: c}, nil
		},
		kube: c,
	}
}

const matchingConfig = `{"instanceStatusEncoding": "ColumnarV2", "maxDecodedInstances": 100}`

func (f *fixture) stored(t *testing.T) *v1beta1.InferenceReplica {
	t.Helper()
	ir := &v1beta1.InferenceReplica{}
	if err := f.kube.Get(context.Background(), client.ObjectKey{Namespace: f.opts.Namespace, Name: f.opts.Name}, ir); err != nil {
		t.Fatalf("read stored IR: %v", err)
	}
	return ir
}

func TestRunDryRunReportsAndWritesNothing(t *testing.T) {
	f := newFixture(t, matchingConfig)
	before := f.stored(t)

	result, err := Run(context.Background(), f.opts, f.connect)
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if result.Outcome.Applied || !strings.HasPrefix(result.Outcome.StoredEncoding, "undecodable (") || result.Outcome.ReplacementRows != 2 {
		t.Fatalf("unexpected outcome: %+v", result.Outcome)
	}
	if result.Outcome.SelectedEncoding != irstatus.EncodingDenseV1 {
		t.Fatalf("two rows must select DenseV1 under the ColumnarV2 target, got %s", result.Outcome.SelectedEncoding)
	}
	after := f.stored(t)
	if after.ResourceVersion != before.ResourceVersion || !equality.Semantic.DeepEqual(after.Status, before.Status) {
		t.Fatal("a dry run must not write")
	}
	var out bytes.Buffer
	if err := result.WriteText(&out); err != nil {
		t.Fatalf("write report: %v", err)
	}
	for _, want := range []string{"dry run", "undecodable (range_order)", "would write", "DenseV1", "Re-run with --apply", "serving/example-engine", "resourceVersion " + before.ResourceVersion} {
		if !strings.Contains(out.String(), want) {
			t.Fatalf("report lacks %q:\n%s", want, out.String())
		}
	}
}

func TestRunApplyReplacesRowsAndPreservesUnrelatedStatus(t *testing.T) {
	f := newFixture(t, matchingConfig)
	f.opts.Apply = true
	before := f.stored(t)

	result, err := Run(context.Background(), f.opts, f.connect)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if !result.Outcome.Applied || result.Outcome.ResourceVersion == before.ResourceVersion {
		t.Fatalf("apply must write: %+v", result.Outcome)
	}
	after := f.stored(t)
	if after.Status.InstanceStatusEncoding != nil || after.Status.InstanceStatusColumns != nil || len(after.Status.InstanceStatuses) != 2 {
		t.Fatalf("stored object must carry the two dense rows: %+v", after.Status)
	}
	if after.Status.Replicas != before.Status.Replicas || after.Status.CurrentRevision != before.Status.CurrentRevision || !equality.Semantic.DeepEqual(after.Status.Conditions, before.Status.Conditions) {
		t.Fatalf("unrelated status must be preserved: %+v", after.Status)
	}
	var out bytes.Buffer
	if err := result.WriteText(&out); err != nil {
		t.Fatalf("write report: %v", err)
	}
	if !strings.Contains(out.String(), "(applied)") || !strings.Contains(out.String(), "wrote:") {
		t.Fatalf("report must say the write was applied:\n%s", out.String())
	}
}

func TestRunRefusesColumnarReplacement(t *testing.T) {
	f := newFixture(t, matchingConfig)
	f.opts.ReplacementPath = writeFile(t, t.TempDir(), "columns.yaml", testColumnarReplacement)
	f.opts.Apply = true
	before := f.stored(t)

	_, err := Run(context.Background(), f.opts, f.connect)
	if !IsRefusal(err) || !strings.Contains(err.Error(), "DenseV1 row set only") {
		t.Fatalf("a ColumnarV2 replacement must be refused, got %v", err)
	}
	if f.stored(t).ResourceVersion != before.ResourceVersion {
		t.Fatal("a refused repair must not write")
	}
}

func TestRunRefusesUnknownReplacementKeys(t *testing.T) {
	f := newFixture(t, matchingConfig)
	f.opts.ReplacementPath = writeFile(t, t.TempDir(), "bad.yaml", "instanceStatuses: []\nreplicas: 3\nextra: 1\n")
	if _, err := Run(context.Background(), f.opts, f.connect); err == nil || !strings.Contains(err.Error(), "parse replacement") {
		t.Fatalf("unknown keys must fail strict parsing, got %v", err)
	}
}

func TestRunRefusesConfigurationMismatch(t *testing.T) {
	f := newFixture(t, `{"instanceStatusEncoding": "DenseV1"}`)
	f.opts.Apply = true
	before := f.stored(t)
	_, err := Run(context.Background(), f.opts, f.connect)
	if err == nil || !strings.Contains(err.Error(), "inventory expects") {
		t.Fatalf("a cluster whose block differs from the inventory must be refused, got %v", err)
	}
	if f.stored(t).ResourceVersion != before.ResourceVersion {
		t.Fatal("a refused repair must not write")
	}
}

func TestRunRefusesUnknownCluster(t *testing.T) {
	f := newFixture(t, matchingConfig)
	f.opts.Cluster = "west"
	if _, err := Run(context.Background(), f.opts, f.connect); err == nil || !strings.Contains(err.Error(), `cluster "west" is not in the inventory`) {
		t.Fatalf("an unknown cluster must be refused, got %v", err)
	}
}

func TestCommandRequiresEveryFlag(t *testing.T) {
	var out, errOut bytes.Buffer
	cmd := NewCommand(&out, &errOut)
	cmd.SetArgs([]string{"--inventory", "fleet.yaml"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "required flag") {
		t.Fatalf("missing flags must be a usage error, got %v", err)
	}
}
