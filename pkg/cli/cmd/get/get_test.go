package get

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

var update = flag.Bool("update", false, "rewrite golden files")

func assertGolden(t *testing.T, name string, got []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		require.NoError(t, os.WriteFile(path, got, 0o644))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Equal(t, string(want), string(got))
}

func execute(t *testing.T, f factory.Factory, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &out}
	cmd := NewCmd(f, streams)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func fixtureISVC(name, ns string) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Spec: v1beta1.InferenceServiceSpec{
			Model:   &v1beta1.ModelRef{Name: "llama-3-3-70b"},
			Runtime: &v1beta1.ServingRuntimeRef{Name: "srt-llama"},
		},
	}
}

func getScheme(t *testing.T) *k8sruntime.Scheme {
	t.Helper()
	scheme := k8sruntime.NewScheme()
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	return scheme
}

func fixtureRolloutPolicy(name, ns string) *v1beta1.RolloutPolicy {
	return &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns, Generation: 1, Labels: map[string]string{"tier": "gold"}},
		Spec:       v1beta1.RolloutPolicySpec{Canary: &v1beta1.GroupCanary{}},
		Status: v1beta1.RolloutPolicyStatus{
			ObservedGeneration: 1,
			PortableDigest:     "rp1:1234567890ab",
			AttachedGroups:     2,
			Conditions: []metav1.Condition{
				{Type: v1beta1.RolloutPolicyReadyCondition, Status: metav1.ConditionTrue, Reason: v1beta1.RolloutPolicyReasonBodyValid},
				{Type: v1beta1.RolloutPolicyInUseCondition, Status: metav1.ConditionTrue},
			},
		},
	}
}

type pagingRuntimeClient struct {
	ctrlclient.Client
	pages    [][]v1beta1.RolloutPolicy
	requests []ctrlclient.ListOptions
}

func (c *pagingRuntimeClient) List(_ context.Context, list ctrlclient.ObjectList, options ...ctrlclient.ListOption) error {
	request := ctrlclient.ListOptions{}
	request.ApplyOptions(options)
	c.requests = append(c.requests, request)

	policies := list.(*v1beta1.RolloutPolicyList)
	page := len(c.requests) - 1
	policies.Items = append(policies.Items, c.pages[page]...)
	if page+1 < len(c.pages) {
		policies.Continue = "next-page"
	}
	return nil
}

func TestGetISVCTable(t *testing.T) {
	f := factory.Static{
		OME: omefake.NewSimpleClientset(fixtureISVC("a-isvc", "team-a"), fixtureISVC("b-isvc", "team-a")),
		NS:  "team-a",
	}
	out, err := execute(t, f, "isvc")
	require.NoError(t, err)
	assertGolden(t, "isvc_list.golden", []byte(out))
}

func TestGetRolloutPolicyUsesRuntimeClientAndSelector(t *testing.T) {
	selected := fixtureRolloutPolicy("guarded", "team-a")
	unselected := fixtureRolloutPolicy("other", "team-a")
	unselected.Labels = map[string]string{"tier": "silver"}
	f := factory.Static{
		Runtime: ctrlfake.NewClientBuilder().WithScheme(getScheme(t)).WithObjects(selected, unselected).Build(),
		NS:      "team-a",
	}

	out, err := execute(t, f, "rp", "-l", "tier=gold")
	require.NoError(t, err)
	assert.Contains(t, out, "NAME")
	assert.Contains(t, out, "PROGRESSION")
	assert.Contains(t, out, "guarded")
	assert.NotContains(t, out, "other")

	wide, err := execute(t, f, "rolloutpolicies", "-o", "wide")
	require.NoError(t, err)
	assert.Contains(t, wide, "REASON")
	assert.Contains(t, wide, "IN-USE")
	assert.Contains(t, wide, "STATUS-FRESHNESS")
	assert.Contains(t, wide, v1beta1.RolloutPolicyReasonBodyValid)
}

func TestGetRolloutPolicyForwardsPagingOptions(t *testing.T) {
	client := &pagingRuntimeClient{pages: [][]v1beta1.RolloutPolicy{
		{*fixtureRolloutPolicy("first", "team-a")},
		{*fixtureRolloutPolicy("second", "team-a")},
	}}
	f := factory.Static{Runtime: client, NS: "team-a"}

	out, err := execute(t, f, "rp")
	require.NoError(t, err)
	assert.Contains(t, out, "first")
	assert.Contains(t, out, "second")
	require.Len(t, client.requests, 2)
	assert.Equal(t, int64(paging.ChunkSize), client.requests[0].Limit)
	assert.Empty(t, client.requests[0].Continue)
	assert.Equal(t, int64(paging.ChunkSize), client.requests[1].Limit)
	assert.Equal(t, "next-page", client.requests[1].Continue)
}

func TestGetRolloutPolicyAllNamespacesAndMachineFormats(t *testing.T) {
	teamA := fixtureRolloutPolicy("guarded", "team-a")
	teamB := fixtureRolloutPolicy("guarded", "team-b")
	f := factory.Static{
		Runtime: ctrlfake.NewClientBuilder().WithScheme(getScheme(t)).WithObjects(teamA, teamB).Build(),
		NS:      "team-a",
	}

	out, err := execute(t, f, "rolloutpolicy", "-A")
	require.NoError(t, err)
	assert.Contains(t, out, "NAMESPACE")
	assert.Contains(t, out, "guarded")
	assert.Equal(t, 2, strings.Count(out, "guarded"))
	assert.Contains(t, out, "team-a")
	assert.Contains(t, out, "team-b")

	wideOut, err := execute(t, f, "rolloutpolicy", "-A", "-o", "wide")
	require.NoError(t, err)
	assert.Contains(t, wideOut, "NAMESPACE")
	assert.Contains(t, wideOut, "team-a")
	assert.Contains(t, wideOut, "team-b")

	jsonOut, err := execute(t, f, "rp", "guarded", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, jsonOut, `"name": "guarded"`)

	yamlOut, err := execute(t, f, "rp", "guarded", "-o", "yaml")
	require.NoError(t, err)
	assert.Contains(t, yamlOut, "name: guarded")
}

func TestGetModelsMerged(t *testing.T) {
	arch := "LlamaForCausalLM"
	f := factory.Static{
		OME: omefake.NewSimpleClientset(
			&v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "ns-model", Namespace: "team-a"},
				Spec: v1beta1.BaseModelSpec{ModelArchitecture: &arch}},
			&v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "cluster-model"},
				Spec: v1beta1.BaseModelSpec{ModelArchitecture: &arch}},
		),
		NS: "team-a",
	}
	out, err := execute(t, f, "models")
	require.NoError(t, err)
	assertGolden(t, "models_merged.golden", []byte(out))
}

func TestGetSingleNameJSON(t *testing.T) {
	f := factory.Static{OME: omefake.NewSimpleClientset(fixtureISVC("a-isvc", "team-a")), NS: "team-a"}
	out, err := execute(t, f, "isvc", "a-isvc", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, out, `"name": "a-isvc"`)
}

func TestGetUnknownResourceErr(t *testing.T) {
	_, err := execute(t, factory.Static{NS: "d"}, "banana")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown resource")
}

func TestGetNotFoundFriendly(t *testing.T) {
	f := factory.Static{OME: omefake.NewSimpleClientset(), NS: "team-a"}
	_, err := execute(t, f, "isvc", "missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), `"missing" not found`)
}

// TestGetISVCListJSONEnvelope pins Finding 1: a multi-object -o json listing
// must be one valid document -- a v1 List envelope -- not several bare
// objects printed back-to-back.
func TestGetISVCListJSONEnvelope(t *testing.T) {
	f := factory.Static{
		OME: omefake.NewSimpleClientset(fixtureISVC("a-isvc", "team-a"), fixtureISVC("b-isvc", "team-a")),
		NS:  "team-a",
	}
	out, err := execute(t, f, "isvc", "-o", "json")
	require.NoError(t, err)
	var list struct {
		Kind  string            `json:"kind"`
		Items []json.RawMessage `json:"items"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &list), "must be a single valid JSON document")
	assert.Equal(t, "List", list.Kind)
	assert.Len(t, list.Items, 2)
}

// TestGetModelsListYAMLEnvelope pins Finding 1 for the YAML path: printing
// each object separately produces a malformed multi-document stream (no ---
// separators); the fix wraps the list in a single List envelope instead.
func TestGetModelsListYAMLEnvelope(t *testing.T) {
	arch := "LlamaForCausalLM"
	f := factory.Static{
		OME: omefake.NewSimpleClientset(
			&v1beta1.BaseModel{ObjectMeta: metav1.ObjectMeta{Name: "ns-model", Namespace: "team-a"},
				Spec: v1beta1.BaseModelSpec{ModelArchitecture: &arch}},
			&v1beta1.ClusterBaseModel{ObjectMeta: metav1.ObjectMeta{Name: "cluster-model"},
				Spec: v1beta1.BaseModelSpec{ModelArchitecture: &arch}},
		),
		NS: "team-a",
	}
	out, err := execute(t, f, "models", "-o", "yaml")
	require.NoError(t, err)
	assert.Contains(t, out, "kind: List")
	assert.Contains(t, out, "name: ns-model")
	assert.Contains(t, out, "name: cluster-model")

	var doc struct {
		Kind  string           `json:"kind"`
		Items []map[string]any `json:"items"`
	}
	require.NoError(t, yaml.Unmarshal([]byte(out), &doc), "must be a single valid YAML document")
	assert.Equal(t, "List", doc.Kind)
	assert.Len(t, doc.Items, 2)
}

// TestGetEmptyListJSONEnvelopeHasEmptyItemsArray pins the round-2 fix: an
// empty result set's -o json output must have an empty items ARRAY, never a
// null items field -- naive consumers like `jq '.items[]'` error on null.
func TestGetEmptyListJSONEnvelopeHasEmptyItemsArray(t *testing.T) {
	f := factory.Static{OME: omefake.NewSimpleClientset(), NS: "team-a"}
	out, err := execute(t, f, "workloadclusters", "-o", "json")
	require.NoError(t, err)
	assert.NotContains(t, out, "null", "no field of an empty List envelope should render as null")

	var list struct {
		Kind  string            `json:"kind"`
		Items []json.RawMessage `json:"items"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &list), "must be a single valid JSON document")
	assert.Equal(t, "List", list.Kind)
	assert.NotNil(t, list.Items, "items must unmarshal as an empty array, not null")
	assert.Len(t, list.Items, 0)
}

// TestGetAllNamespacesWarnsOnClusterScoped pins Finding 2a: -A on a
// cluster-scoped resource is accepted (not an error, kubectl parity) but
// warns instead of silently doing nothing.
func TestGetAllNamespacesWarnsOnClusterScoped(t *testing.T) {
	f := factory.Static{OME: omefake.NewSimpleClientset(), NS: "team-a"}
	out, err := execute(t, f, "acceleratorclasses", "-A")
	require.NoError(t, err)
	assert.Contains(t, out, `warning: --all-namespaces is ignored for the cluster-scoped resource "acceleratorclasses"`)
}

// TestGetEmptyClusterScopedMessageHasNoNamespaceClause pins Finding 2b: the
// empty-list message for a cluster-scoped resource must not claim a
// namespace scope that doesn't apply.
func TestGetEmptyClusterScopedMessageHasNoNamespaceClause(t *testing.T) {
	f := factory.Static{OME: omefake.NewSimpleClientset(), NS: "team-a"}
	out, err := execute(t, f, "workloadclusters")
	require.NoError(t, err)
	assert.Contains(t, out, "No workloadclusters found.\n")
	assert.NotContains(t, out, "namespace")
}

func TestGetInvalidOutputFormat(t *testing.T) {
	_, err := execute(t, factory.Static{NS: "team-a"}, "isvc", "-o", "toml")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "supported: wide, json, yaml")
}

func TestGetNameWithAllNamespacesRejected(t *testing.T) {
	_, err := execute(t, factory.Static{NS: "team-a"}, "isvc", "a-isvc", "-A")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be combined with --all-namespaces")
}

func TestGetNameWithSelectorRejected(t *testing.T) {
	_, err := execute(t, factory.Static{NS: "team-a"}, "isvc", "a-isvc", "-l", "foo=bar")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "cannot be combined with --selector")
}

// TestGetSelectorSurvivesPaging pins the pagination helper's contract to
// preserve a caller-supplied --selector: chunking a list through
// Limit/Continue must never silently drop LabelSelector filtering.
func TestGetSelectorSurvivesPaging(t *testing.T) {
	tagged := fixtureISVC("gpu-isvc", "team-a")
	tagged.Labels = map[string]string{"tier": "gpu"}
	untagged := fixtureISVC("cpu-isvc", "team-a")
	f := factory.Static{
		OME: omefake.NewSimpleClientset(tagged, untagged),
		NS:  "team-a",
	}
	out, err := execute(t, f, "isvc", "-l", "tier=gpu")
	require.NoError(t, err)
	assert.Contains(t, out, "gpu-isvc")
	assert.NotContains(t, out, "cpu-isvc")
}

func TestGetAcceleratorQuotaFlattensBudgetRows(t *testing.T) {
	quota := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a"},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role:      v1beta1.AcceleratorQuotaRoleClusterQueue,
			ParentRef: &v1beta1.AcceleratorQuotaParentRef{Name: "root"},
		},
		Status: v1beta1.AcceleratorQuotaStatus{
			Budgets: []v1beta1.AcceleratorBudgetStatus{
				{ResourceName: "nvidia.com/gpu", ResourceFlavor: "h200", Nominal: resource.MustParse("4"), Admitted: resource.MustParse("1")},
				{ResourceName: "nvidia.com/gpu", ResourceFlavor: "h100", Nominal: resource.MustParse("8"), Admitted: resource.MustParse("3")},
			},
			Conditions: []metav1.Condition{{Type: v1beta1.AcceleratorQuotaReady, Status: metav1.ConditionTrue}},
		},
	}
	f := factory.Static{OME: omefake.NewSimpleClientset(quota), NS: "team-a"}

	out, err := execute(t, f, "aq")
	require.NoError(t, err)
	assert.Equal(t, 2, strings.Count(out, "team-a"), out)
	assert.Contains(t, out, "h100")
	assert.Contains(t, out, "h200")
	assert.Equal(t, 2, strings.Count(out, "Reported"), "each status-backed row must label its evidence")
	assert.Less(t, strings.Index(out, "h100"), strings.Index(out, "h200"), "budget rows must be deterministic")
}

func TestGetAcceleratorQuotaRawJSONRemainsUnflattened(t *testing.T) {
	quota := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a"},
		Status: v1beta1.AcceleratorQuotaStatus{Budgets: []v1beta1.AcceleratorBudgetStatus{
			{ResourceName: "nvidia.com/gpu", ResourceFlavor: "h100"},
			{ResourceName: "nvidia.com/gpu", ResourceFlavor: "h200"},
		}},
	}
	f := factory.Static{OME: omefake.NewSimpleClientset(quota), NS: "team-a"}

	out, err := execute(t, f, "acceleratorquotas", "-o", "json")
	require.NoError(t, err)
	var list struct {
		Items []struct {
			Status struct {
				Budgets []json.RawMessage `json:"budgets"`
			} `json:"status"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &list))
	require.Len(t, list.Items, 1)
	assert.Len(t, list.Items[0].Status.Budgets, 2, "machine output must preserve one API object")
}

func TestGetAcceleratorQuotaFallsBackToDeclaredBudget(t *testing.T) {
	quota := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "unreported"},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role: v1beta1.AcceleratorQuotaRoleClusterQueue,
			Budgets: []v1beta1.AcceleratorBudget{{
				ResourceName: "nvidia.com/gpu", ResourceFlavor: "b200", Nominal: resource.MustParse("16"),
			}},
		},
	}
	f := factory.Static{OME: omefake.NewSimpleClientset(quota), NS: "team-a"}

	out, err := execute(t, f, "aq")
	require.NoError(t, err)
	assert.Contains(t, out, "b200")
	assert.Contains(t, out, "16")
	assert.Contains(t, out, "Declared")
	assert.Contains(t, out, "Unknown", "missing status must not be presented as healthy")
}

func TestGetAcceleratorQuotaPreservesStaleAndCurrentBudgets(t *testing.T) {
	quota := &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: "team-a", Generation: 2},
		Spec: v1beta1.AcceleratorQuotaSpec{
			Role: v1beta1.AcceleratorQuotaRoleClusterQueue,
			Budgets: []v1beta1.AcceleratorBudget{
				{ResourceName: "nvidia.com/gpu", ResourceFlavor: "h100", Nominal: resource.MustParse("8")},
				{ResourceName: "nvidia.com/gpu", ResourceFlavor: "h200", Nominal: resource.MustParse("4")},
			},
		},
		Status: v1beta1.AcceleratorQuotaStatus{
			ObservedGeneration: 1,
			Budgets: []v1beta1.AcceleratorBudgetStatus{{
				ResourceName: "nvidia.com/gpu", ResourceFlavor: "h100",
				Nominal: resource.MustParse("2"), Admitted: resource.MustParse("2"),
			}},
			Conditions: []metav1.Condition{{
				Type: v1beta1.AcceleratorQuotaReady, Status: metav1.ConditionTrue,
			}},
		},
	}
	f := factory.Static{OME: omefake.NewSimpleClientset(quota), NS: "team-a"}

	out, err := execute(t, f, "aq")
	require.NoError(t, err)
	assert.Equal(t, 3, strings.Count(out, "team-a"), out)
	assert.Equal(t, 2, strings.Count(out, "Declared"), out)
	assert.Equal(t, 1, strings.Count(out, "Reported"), out)
	assert.Equal(t, 3, strings.Count(out, "Stale"), out)
	assert.Contains(t, out, "h200", "a current declaration absent from stale status must remain visible")
	assert.Less(t, strings.Index(out, "Declared"), strings.Index(out, "Reported"), "current declaration must precede stale reported data for the same budget")
}
