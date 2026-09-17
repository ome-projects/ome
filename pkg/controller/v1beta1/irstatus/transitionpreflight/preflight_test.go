package transitionpreflight

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/openapi"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

const (
	testImage      = "registry.example.com/ome/manager@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	testOtherImage = "registry.example.com/ome/manager:older"
)

// --- fixtures ---

func denseInventory() *Inventory {
	return &Inventory{
		ManagerImage:    testImage,
		OMENativeStatus: ExpectedStatusConfig{InstanceStatusEncoding: irstatus.EncodingDenseV1, MaxDecodedInstances: ptr.To(uint64(8))},
		Manager:         ManagerLocation{Namespace: "ome", Deployment: "ome-controller-manager", Container: "manager", ConfigMap: "inferenceservice-config"},
		PageSize:        2,
		Clusters:        []Cluster{{Name: "east", Kubeconfig: "/etc/ome/east.kubeconfig", Context: "east"}},
	}
}

func columnarInventory() *Inventory {
	inventory := denseInventory()
	inventory.OMENativeStatus.InstanceStatusEncoding = irstatus.EncodingColumnarV2
	return inventory
}

func managerDeployment(inventory *Inventory, image string) *appsv1.Deployment {
	return &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: inventory.Manager.Deployment, Namespace: inventory.Manager.Namespace},
		Spec: appsv1.DeploymentSpec{
			Replicas: ptr.To(int32(2)),
			Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: []corev1.Container{
				{Name: "kube-rbac-proxy", Image: "registry.example.com/kube-rbac-proxy:v0"},
				{Name: inventory.Manager.Container, Image: image},
			}}},
		},
		Status: appsv1.DeploymentStatus{ReadyReplicas: 2},
	}
}

func configMapFor(inventory *Inventory, expected ExpectedStatusConfig) *corev1.ConfigMap {
	block := fmt.Sprintf(`{"instanceStatusEncoding": %q`, expected.InstanceStatusEncoding)
	if expected.MaxDecodedInstances != nil {
		block += fmt.Sprintf(`, "maxDecodedInstances": %d`, *expected.MaxDecodedInstances)
	}
	block += "}"
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: inventory.Manager.ConfigMap, Namespace: inventory.Manager.Namespace},
		Data:       map[string]string{controllerconfig.OMENativeStatusConfigName: block},
	}
}

func denseRows(n int) []v1beta1.OMENativeInstanceStatus {
	rows := make([]v1beta1.OMENativeInstanceStatus, n)
	for i := range rows {
		rows[i] = v1beta1.OMENativeInstanceStatus{Index: int32(i), Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "chat-engine-0a1b2c3d", PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true}
	}
	return rows
}

func denseIR(namespace, name string, rows int) v1beta1.InferenceReplica {
	return v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Status:     v1beta1.InferenceReplicaStatus{InstanceStatuses: denseRows(rows)},
	}
}

func columnarIR(t *testing.T, namespace, name string, rows int) v1beta1.InferenceReplica {
	t.Helper()
	columns, err := irstatus.EncodeColumns(denseRows(rows), uint64(rows))
	if err != nil {
		t.Fatalf("encode columns: %v", err)
	}
	encoding := v1beta1.InstanceStatusEncodingColumnarV2
	return v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Status:     v1beta1.InferenceReplicaStatus{InstanceStatusEncoding: &encoding, InstanceStatusColumns: columns},
	}
}

// pagedLister serves fixed pages with continue tokens and can fail one page.
type pagedLister struct {
	pages    [][]v1beta1.InferenceReplica
	failPage int
	calls    []metav1.ListOptions
}

func (l *pagedLister) List(_ context.Context, opts metav1.ListOptions) (*v1beta1.InferenceReplicaList, error) {
	l.calls = append(l.calls, opts)
	index := 0
	if opts.Continue != "" {
		if _, err := fmt.Sscanf(opts.Continue, "page-%d", &index); err != nil {
			return nil, fmt.Errorf("bad continue token %q", opts.Continue)
		}
	}
	if index+1 == l.failPage {
		return nil, errors.New("etcdserver: leader changed")
	}
	if index >= len(l.pages) {
		return &v1beta1.InferenceReplicaList{}, nil
	}
	list := &v1beta1.InferenceReplicaList{Items: l.pages[index]}
	if index+1 < len(l.pages) {
		list.Continue = fmt.Sprintf("page-%d", index+1)
	}
	return list, nil
}

// --- OpenAPI discovery fake serving the generated full CRD schema ---

type fakeGroupVersion struct {
	document []byte
	err      error
}

func (g fakeGroupVersion) Schema(string) ([]byte, error) { return g.document, g.err }
func (g fakeGroupVersion) ServerRelativeURL() string     { return "/openapi/v3/apis/ome.io/v1beta1" }

type fakeOpenAPI struct {
	paths map[string]openapi.GroupVersion
	err   error
}

func (c fakeOpenAPI) Paths() (map[string]openapi.GroupVersion, error) { return c.paths, c.err }

func fullSchemaOpenAPI(t *testing.T) openapi.Client {
	t.Helper()
	path := filepath.Join("..", "..", "..", "..", "..", "config", "crd", "full", "ome.io_inferencereplicas.yaml")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read generated CRD %s: %v", path, err)
	}
	var crd apiextensionsv1.CustomResourceDefinition
	if err := yaml.Unmarshal(data, &crd); err != nil {
		t.Fatalf("parse generated CRD: %v", err)
	}
	var root map[string]any
	for _, version := range crd.Spec.Versions {
		if version.Name != v1beta1.SchemeGroupVersion.Version || version.Schema == nil {
			continue
		}
		raw, err := json.Marshal(version.Schema.OpenAPIV3Schema)
		if err != nil {
			t.Fatalf("marshal schema: %v", err)
		}
		if err := json.Unmarshal(raw, &root); err != nil {
			t.Fatalf("unmarshal schema: %v", err)
		}
	}
	if root == nil {
		t.Fatalf("CRD %s has no v1beta1 schema", path)
	}
	root["x-kubernetes-group-version-kind"] = []map[string]string{{"group": v1beta1.SchemeGroupVersion.Group, "version": v1beta1.SchemeGroupVersion.Version, "kind": "InferenceReplica"}}
	document, err := json.Marshal(map[string]any{
		"openapi": "3.0.0",
		"paths": map[string]any{
			"/apis/ome.io/v1beta1/namespaces/{namespace}/inferencereplicas/{name}":        map[string]any{},
			"/apis/ome.io/v1beta1/namespaces/{namespace}/inferencereplicas/{name}/status": map[string]any{},
		},
		"components": map[string]any{"schemas": map[string]any{"io.ome.v1beta1.InferenceReplica": root}},
	})
	if err != nil {
		t.Fatalf("marshal document: %v", err)
	}
	return fakeOpenAPI{paths: map[string]openapi.GroupVersion{"apis/ome.io/v1beta1": fakeGroupVersion{document: document}}}
}

// clusterFixture assembles one cluster's fake clients from its parts.
type clusterFixture struct {
	objects []runtime.Object
	openAPI openapi.Client
	lister  *pagedLister
}

func fixtureFor(t *testing.T, inventory *Inventory) *clusterFixture {
	t.Helper()
	return &clusterFixture{
		objects: []runtime.Object{managerDeployment(inventory, testImage), configMapFor(inventory, inventory.OMENativeStatus)},
		openAPI: fullSchemaOpenAPI(t),
		lister:  &pagedLister{pages: [][]v1beta1.InferenceReplica{{denseIR("team-a", "chat-engine", 3), denseIR("team-a", "chat-decoder", 2)}, {denseIR("team-b", "embed-engine", 1)}}},
	}
}

func (f *clusterFixture) connector() Connector {
	return func(context.Context, Cluster) (*ClusterClients, error) {
		return &ClusterClients{Kube: k8sfake.NewSimpleClientset(f.objects...), OpenAPI: f.openAPI, Replicas: f.lister}, nil
	}
}

func reasonKinds(report ClusterReport) []ReasonKind {
	kinds := make([]ReasonKind, 0, len(report.Reasons))
	for _, reason := range report.Reasons {
		kinds = append(kinds, reason.Kind)
	}
	return kinds
}

// --- tests ---

func TestRunReportsGoForConvergedDenseV1Fleet(t *testing.T) {
	inventory := denseInventory()
	inventory.Clusters = append(inventory.Clusters, Cluster{Name: "west", Kubeconfig: "/etc/ome/west.kubeconfig", Context: "west"})
	fixture := fixtureFor(t, inventory)

	report := Run(context.Background(), inventory, "abc123", fixture.connector())
	if !report.Go || len(report.Clusters) != 2 {
		t.Fatalf("report = %+v, want GO for both clusters", report)
	}
	for _, cluster := range report.Clusters {
		if len(cluster.Reasons) != 0 || !cluster.Reachable {
			t.Fatalf("cluster %s: %+v", cluster.Name, cluster)
		}
		if !cluster.Manager.ImageMatches || cluster.Manager.Replicas != 2 || cluster.Manager.ReadyReplicas != 2 {
			t.Fatalf("cluster %s manager = %+v", cluster.Name, cluster.Manager)
		}
		if !cluster.Config.Matches || !cluster.Schema.OK {
			t.Fatalf("cluster %s config = %+v schema = %+v", cluster.Name, cluster.Config, cluster.Schema)
		}
		if cluster.Replicas.Pages != 2 || cluster.Replicas.Objects != 3 || cluster.Replicas.DenseV1 != 3 || len(cluster.Replicas.ColumnarV2) != 0 {
			t.Fatalf("cluster %s census = %+v", cluster.Name, cluster.Replicas)
		}
	}
	// Both clusters share one lister here, so the calls interleave: every
	// page request carries the inventory page size and the server's token.
	for _, call := range fixture.lister.calls {
		if call.Limit != inventory.PageSize {
			t.Fatalf("list page size = %d, want %d", call.Limit, inventory.PageSize)
		}
	}
	if len(fixture.lister.calls) != 4 || fixture.lister.calls[1].Continue != "page-1" {
		t.Fatalf("list calls = %+v, want two pages per cluster driven by the continue token", fixture.lister.calls)
	}

	var text bytes.Buffer
	if err := report.WriteText(&text); err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"inventory digest: sha256:abc123", "cluster east (context east): GO", "result: GO (2 clusters)", "denseV1=3 columnarV2=0"} {
		if !strings.Contains(text.String(), want) {
			t.Fatalf("text report lacks %q:\n%s", want, text.String())
		}
	}
	var raw bytes.Buffer
	if err := report.WriteJSON(&raw); err != nil {
		t.Fatal(err)
	}
	var decoded Report
	if err := json.Unmarshal(raw.Bytes(), &decoded); err != nil {
		t.Fatalf("JSON report does not round-trip: %v", err)
	}
	if !decoded.Go || decoded.InventoryDigest != "abc123" || decoded.Expected.ManagerImage != testImage || len(decoded.Clusters) != 2 {
		t.Fatalf("JSON report = %+v", decoded)
	}
}

func TestRunReportsEveryNoGoReason(t *testing.T) {
	cases := map[string]struct {
		inventory func() *Inventory
		mutate    func(t *testing.T, inventory *Inventory, fixture *clusterFixture) Connector
		want      []ReasonKind
		detail    string
		reachable bool
	}{
		"columnar present under DenseV1 target": {
			inventory: denseInventory,
			mutate: func(t *testing.T, _ *Inventory, f *clusterFixture) Connector {
				f.lister.pages[1] = append(f.lister.pages[1], columnarIR(t, "team-b", "vision-engine", 4))
				return f.connector()
			},
			want:      []ReasonKind{ReasonColumnarV2Present},
			detail:    "team-b/vision-engine",
			reachable: true,
		},
		"columnar above bound": {
			inventory: columnarInventory,
			mutate: func(t *testing.T, _ *Inventory, f *clusterFixture) Connector {
				f.lister.pages[0] = append(f.lister.pages[0], columnarIR(t, "team-a", "big-engine", 9))
				return f.connector()
			},
			want:      []ReasonKind{ReasonAboveBound},
			detail:    "team-a/big-engine",
			reachable: true,
		},
		"columnar under DenseV1 target without a bound": {
			inventory: func() *Inventory {
				inventory := denseInventory()
				inventory.OMENativeStatus.MaxDecodedInstances = nil
				return inventory
			},
			mutate: func(t *testing.T, _ *Inventory, f *clusterFixture) Connector {
				f.lister.pages[0] = append(f.lister.pages[0], columnarIR(t, "team-a", "small-engine", 1))
				return f.connector()
			},
			want:      []ReasonKind{ReasonAboveBound, ReasonColumnarV2Present},
			detail:    "none configured",
			reachable: true,
		},
		"undecodable columnar payload": {
			inventory: columnarInventory,
			mutate: func(t *testing.T, _ *Inventory, f *clusterFixture) Connector {
				broken := columnarIR(t, "team-a", "broken-engine", 2)
				broken.Status.InstanceStatusColumns.Members = "1-0"
				f.lister.pages[0] = append(f.lister.pages[0], broken)
				return f.connector()
			},
			want:      []ReasonKind{ReasonUndecodable},
			detail:    "team-a/broken-engine (" + string(irstatus.ErrorReasonRangeOrder) + ")",
			reachable: true,
		},
		"failed page": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, _ *Inventory, f *clusterFixture) Connector {
				f.lister.failPage = 2
				return f.connector()
			},
			want:      []ReasonKind{ReasonFailedPage},
			detail:    "page 2 failed",
			reachable: true,
		},
		"unexpected image": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, inventory *Inventory, f *clusterFixture) Connector {
				f.objects[0] = managerDeployment(inventory, testOtherImage)
				return f.connector()
			},
			want:      []ReasonKind{ReasonUnexpectedImage},
			detail:    testOtherImage,
			reachable: true,
		},
		"manager Deployment missing": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, _ *Inventory, f *clusterFixture) Connector {
				f.objects = f.objects[1:]
				return f.connector()
			},
			want:      []ReasonKind{ReasonUnexpectedImage},
			detail:    "not found",
			reachable: true,
		},
		"manager container missing": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, inventory *Inventory, f *clusterFixture) Connector {
				deployment := managerDeployment(inventory, testImage)
				deployment.Spec.Template.Spec.Containers = deployment.Spec.Template.Spec.Containers[:1]
				f.objects[0] = deployment
				return f.connector()
			},
			want:      []ReasonKind{ReasonUnexpectedImage},
			detail:    `no container named "manager"`,
			reachable: true,
		},
		"configuration mismatch": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, inventory *Inventory, f *clusterFixture) Connector {
				f.objects[1] = configMapFor(inventory, ExpectedStatusConfig{InstanceStatusEncoding: irstatus.EncodingDenseV1, MaxDecodedInstances: ptr.To(uint64(16))})
				return f.connector()
			},
			want:      []ReasonKind{ReasonConfigMismatch},
			detail:    "maxDecodedInstances=16, expected instanceStatusEncoding=DenseV1 maxDecodedInstances=8",
			reachable: true,
		},
		"configuration block invalid": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, inventory *Inventory, f *clusterFixture) Connector {
				configMap := configMapFor(inventory, inventory.OMENativeStatus)
				configMap.Data[controllerconfig.OMENativeStatusConfigName] = `{"instanceStatusEncoding": "Compressed"}`
				f.objects[1] = configMap
				return f.connector()
			},
			want:      []ReasonKind{ReasonConfigMismatch},
			detail:    "Compressed",
			reachable: true,
		},
		"configuration ConfigMap missing": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, _ *Inventory, f *clusterFixture) Connector {
				f.objects = f.objects[:1]
				return f.connector()
			},
			want:      []ReasonKind{ReasonConfigMismatch},
			detail:    "not found",
			reachable: true,
		},
		"stale schema": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, _ *Inventory, f *clusterFixture) Connector {
				f.openAPI = fakeOpenAPI{paths: map[string]openapi.GroupVersion{}}
				return f.connector()
			},
			want:      []ReasonKind{ReasonStaleSchema},
			detail:    "not installed",
			reachable: true,
		},
		"discovery unreachable": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, _ *Inventory, f *clusterFixture) Connector {
				f.openAPI = fakeOpenAPI{err: errors.New("dial tcp: connection refused")}
				return f.connector()
			},
			want:      []ReasonKind{ReasonUnreachable},
			detail:    "connection refused",
			reachable: true,
		},
		"cluster unreachable": {
			inventory: denseInventory,
			mutate: func(_ *testing.T, _ *Inventory, _ *clusterFixture) Connector {
				return func(context.Context, Cluster) (*ClusterClients, error) {
					return nil, errors.New("load kubeconfig: no such file")
				}
			},
			want:      []ReasonKind{ReasonUnreachable},
			detail:    "no such file",
			reachable: false,
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			inventory := tc.inventory()
			fixture := fixtureFor(t, inventory)
			connect := tc.mutate(t, inventory, fixture)

			report := Run(context.Background(), inventory, "digest", connect)
			if report.Go || len(report.Clusters) != 1 {
				t.Fatalf("report = %+v, want NO-GO for one cluster", report)
			}
			cluster := report.Clusters[0]
			if cluster.Reachable != tc.reachable {
				t.Fatalf("reachable = %v, want %v", cluster.Reachable, tc.reachable)
			}
			if got := reasonKinds(cluster); fmt.Sprint(got) != fmt.Sprint(tc.want) {
				t.Fatalf("reasons = %v, want %v (%+v)", got, tc.want, cluster.Reasons)
			}
			var found bool
			for _, reason := range cluster.Reasons {
				found = found || strings.Contains(reason.Detail, tc.detail)
			}
			if !found {
				t.Fatalf("no reason names %q: %+v", tc.detail, cluster.Reasons)
			}
			var text bytes.Buffer
			if err := report.WriteText(&text); err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(text.String(), "NO-GO") || !strings.Contains(text.String(), string(tc.want[0])) {
				t.Fatalf("text report must show the verdict and the reason kind:\n%s", text.String())
			}
		})
	}
}

func TestRunRecordsLargestColumnarCardinality(t *testing.T) {
	inventory := columnarInventory()
	fixture := fixtureFor(t, inventory)
	fixture.lister.pages[0] = append(fixture.lister.pages[0], columnarIR(t, "team-a", "wide-engine", 7), columnarIR(t, "team-a", "narrow-engine", 2))

	report := Run(context.Background(), inventory, "digest", fixture.connector())
	if !report.Go {
		t.Fatalf("ColumnarV2 objects within the bound are allowed under a ColumnarV2 target: %+v", report.Clusters[0].Reasons)
	}
	census := report.Clusters[0].Replicas
	if len(census.ColumnarV2) != 2 || census.LargestColumnarV2Rows != 7 || census.DenseV1 != 3 || census.Objects != 5 {
		t.Fatalf("census = %+v", census)
	}
}

func TestParseInventoryValidation(t *testing.T) {
	valid := `
managerImage: ` + testImage + `
omenativeStatus:
  instanceStatusEncoding: DenseV1
  maxDecodedInstances: 5000
manager:
  namespace: ome
  deployment: ome-controller-manager
  container: manager
  configMap: inferenceservice-config
pageSize: 500
clusters:
- name: east
  kubeconfig: /etc/ome/east.kubeconfig
  context: east
- name: west
  kubeconfig: /etc/ome/west.kubeconfig
  context: west
`
	inventory, err := ParseInventory([]byte(valid))
	if err != nil {
		t.Fatalf("valid inventory rejected: %v", err)
	}
	if inventory.OMENativeStatus.Bound() != 5000 || len(inventory.Clusters) != 2 || inventory.PageSize != 500 {
		t.Fatalf("parsed inventory = %+v", inventory)
	}

	rejected := map[string]struct {
		yaml string
		want []string
	}{
		"unknown field": {yaml: valid + "extra: true\n", want: []string{"extra"}},
		"missing everything": {yaml: "clusters: []\n", want: []string{
			"managerImage is required", "instanceStatusEncoding is required", "manager.namespace is required", "manager.deployment is required",
			"manager.container is required", "manager.configMap is required", "pageSize must be a positive integer", "at least one cluster",
		}},
		"unsupported encoding":     {yaml: strings.Replace(valid, "DenseV1", "Compressed", 1), want: []string{`"Compressed" is not supported`}},
		"ColumnarV2 without bound": {yaml: strings.Replace(strings.Replace(valid, "DenseV1", "ColumnarV2", 1), "  maxDecodedInstances: 5000\n", "", 1), want: []string{"maxDecodedInstances is required when instanceStatusEncoding is ColumnarV2"}},
		"zero bound":               {yaml: strings.Replace(valid, "5000", "0", 1), want: []string{"maxDecodedInstances must be a positive integer"}},
		"duplicate cluster":        {yaml: strings.Replace(valid, "name: west", "name: east", 1), want: []string{`"east" is listed twice`}},
		"cluster without context":  {yaml: strings.Replace(valid, "  context: west\n", "", 1), want: []string{"clusters[1].context is required"}},
	}
	for name, tc := range rejected {
		t.Run(name, func(t *testing.T) {
			_, err := ParseInventory([]byte(tc.yaml))
			if err == nil {
				t.Fatal("inventory accepted")
			}
			for _, want := range tc.want {
				if !strings.Contains(err.Error(), want) {
					t.Fatalf("error %q lacks %q", err, want)
				}
			}
		})
	}
}

func TestLoadInventoryDigestsTheFileBytes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "fleet.yaml")
	content := []byte("managerImage: " + testImage + "\nomenativeStatus:\n  instanceStatusEncoding: DenseV1\nmanager:\n  namespace: ome\n  deployment: m\n  container: c\n  configMap: cm\npageSize: 10\nclusters:\n- name: a\n  kubeconfig: /k\n  context: a\n")
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
	inventory, digest, err := LoadInventory(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	sum := sha256.Sum256(content)
	if digest != hex.EncodeToString(sum[:]) || inventory.OMENativeStatus.MaxDecodedInstances != nil {
		t.Fatalf("digest = %s inventory = %+v", digest, inventory)
	}
	if _, _, err := LoadInventory(filepath.Join(t.TempDir(), "missing.yaml")); err == nil {
		t.Fatal("missing inventory accepted")
	}
}

// TestCommandReportsUnreachableClustersWithoutAClusterAccess drives the CLI
// end to end: a kubeconfig that does not exist makes the cluster unreachable,
// the report is still written, and the exit signal is the no-go error.
func TestCommandReportsUnreachableClustersWithoutAClusterAccess(t *testing.T) {
	dir := t.TempDir()
	inventoryPath := filepath.Join(dir, "fleet.yaml")
	content := "managerImage: " + testImage + "\nomenativeStatus:\n  instanceStatusEncoding: DenseV1\nmanager:\n  namespace: ome\n  deployment: m\n  container: c\n  configMap: cm\npageSize: 10\nclusters:\n- name: ghost\n  kubeconfig: " + filepath.Join(dir, "missing.kubeconfig") + "\n  context: ghost\n"
	if err := os.WriteFile(inventoryPath, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, asJSON := range []bool{false, true} {
		var out, errOut bytes.Buffer
		cmd := NewCommand(&out, &errOut)
		args := []string{"--inventory", inventoryPath}
		if asJSON {
			args = append(args, "--json")
		}
		cmd.SetArgs(args)
		err := cmd.ExecuteContext(context.Background())
		if !errors.Is(err, ErrNoGo) {
			t.Fatalf("json=%v: err = %v, want ErrNoGo", asJSON, err)
		}
		if asJSON {
			var report Report
			if jsonErr := json.Unmarshal(out.Bytes(), &report); jsonErr != nil || report.Go || report.Clusters[0].Reachable {
				t.Fatalf("JSON report = %s (%v)", out.String(), jsonErr)
			}
			continue
		}
		if !strings.Contains(out.String(), "cluster ghost (context ghost): NO-GO") || !strings.Contains(out.String(), string(ReasonUnreachable)) {
			t.Fatalf("text report:\n%s", out.String())
		}
	}

	cmd := NewCommand(&bytes.Buffer{}, &bytes.Buffer{})
	cmd.SetArgs([]string{})
	if err := cmd.ExecuteContext(context.Background()); err == nil || errors.Is(err, ErrNoGo) {
		t.Fatalf("a missing --inventory must be a usage error, got %v", err)
	}
}
