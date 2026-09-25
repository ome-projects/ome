package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
	"sigs.k8s.io/ome/pkg/cli/paging"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

type runtimeListCall struct {
	kind                 string
	namespace            string
	limit                int64
	continueToken        string
	resourceVersion      string
	resourceVersionMatch metav1.ResourceVersionMatch
	hasDeadline          bool
}

type runtimeListPage struct {
	serving              []v1beta1.ServingRuntime
	clusterServing       []v1beta1.ClusterServingRuntime
	continueToken        string
	resourceVersion      string
	emptyResourceVersion bool
	typeMeta             metav1.TypeMeta
	err                  error
	waitForContext       bool
}

// scriptedRuntimeClient is deliberately stateful. A production implementation
// that re-reads a candidate after collection will observe the embedded client's
// different object and make the no-live-GET assertions fail.
type scriptedRuntimeClient struct {
	ctrlclient.Client

	mu    sync.Mutex
	pages map[string]map[string]runtimeListPage
	calls []runtimeListCall
	gets  int
}

func newScriptedRuntimeClient(t *testing.T, objects ...ctrlclient.Object) *scriptedRuntimeClient {
	t.Helper()
	base := ctrlfake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(objects...).Build()
	return &scriptedRuntimeClient{
		Client: base,
		pages: map[string]map[string]runtimeListPage{
			"ServingRuntime":        {},
			"ClusterServingRuntime": {},
		},
	}
}

func (c *scriptedRuntimeClient) List(
	ctx context.Context,
	list ctrlclient.ObjectList,
	opts ...ctrlclient.ListOption,
) error {
	options := (&ctrlclient.ListOptions{}).ApplyOptions(opts)
	kind := ""
	switch list.(type) {
	case *v1beta1.ServingRuntimeList:
		kind = "ServingRuntime"
	case *v1beta1.ClusterServingRuntimeList:
		kind = "ClusterServingRuntime"
	default:
		return fmt.Errorf("unexpected list type %T", list)
	}
	_, hasDeadline := ctx.Deadline()

	c.mu.Lock()
	page, found := c.pages[kind][options.Continue]
	c.calls = append(c.calls, runtimeListCall{
		kind: kind, namespace: options.Namespace, limit: options.Limit,
		continueToken: options.Continue, hasDeadline: hasDeadline,
	})
	if options.Raw != nil {
		c.calls[len(c.calls)-1].resourceVersion = options.Raw.ResourceVersion
		c.calls[len(c.calls)-1].resourceVersionMatch = options.Raw.ResourceVersionMatch
	}
	c.mu.Unlock()
	if !found {
		return apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: kind}, "missing-script-page")
	}
	if page.waitForContext {
		<-ctx.Done()
		return ctx.Err()
	}
	if page.err != nil {
		return page.err
	}
	resourceVersion := page.resourceVersion
	if resourceVersion == "" && !page.emptyResourceVersion {
		resourceVersion = "fixture-rv"
	}

	switch typed := list.(type) {
	case *v1beta1.ServingRuntimeList:
		typed.Items = make([]v1beta1.ServingRuntime, len(page.serving))
		for i := range page.serving {
			typed.Items[i] = *page.serving[i].DeepCopy()
		}
		typed.TypeMeta = page.typeMeta
		typed.ListMeta = metav1.ListMeta{
			Continue: page.continueToken, ResourceVersion: resourceVersion,
		}
	case *v1beta1.ClusterServingRuntimeList:
		typed.Items = make([]v1beta1.ClusterServingRuntime, len(page.clusterServing))
		for i := range page.clusterServing {
			typed.Items[i] = *page.clusterServing[i].DeepCopy()
		}
		typed.TypeMeta = page.typeMeta
		typed.ListMeta = metav1.ListMeta{
			Continue: page.continueToken, ResourceVersion: resourceVersion,
		}
	}
	return nil
}

func (c *scriptedRuntimeClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	object ctrlclient.Object,
	opts ...ctrlclient.GetOption,
) error {
	switch object.(type) {
	case *v1beta1.ServingRuntime, *v1beta1.ClusterServingRuntime:
		c.mu.Lock()
		c.gets++
		c.mu.Unlock()
	}
	return c.Client.Get(ctx, key, object, opts...)
}

func (c *scriptedRuntimeClient) setPage(kind, token string, page runtimeListPage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.pages[kind][token] = page
}

func (c *scriptedRuntimeClient) observations() ([]runtimeListCall, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]runtimeListCall(nil), c.calls...), c.gets
}

func testRuntimeLimits() paging.Limits {
	return paging.Limits{
		PageSize: 2, MaxItems: 8, MaxPages: 4, RequestTimeout: time.Second,
	}
}

func TestExplainHelpScopesRequestTimeoutToSnapshot(t *testing.T) {
	cmd := newExplainCmd(factory.Static{}, genericiooptions.IOStreams{})
	assert.Contains(t, cmd.Long,
		"Every snapshot API request has a 10-second timeout.")
	assert.NotContains(t, cmd.Long,
		"Every API request has a 10-second timeout.")
}

func namespacedRuntime(namespace, name, format string) v1beta1.ServingRuntime {
	return v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{autoSelectFormat(format)},
		},
	}
}

func clusterRuntime(name, format string) v1beta1.ClusterServingRuntime {
	return v1beta1.ClusterServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1beta1.ServingRuntimeSpec{
			SupportedModelFormats: []v1beta1.SupportedModelFormat{autoSelectFormat(format)},
		},
	}
}

func TestCollectRuntimeCandidateSnapshotUsesOneSharedBudget(t *testing.T) {
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		serving: []v1beta1.ServingRuntime{
			namespacedRuntime("team-a", "ns-a", "safetensors"),
			namespacedRuntime("team-a", "ns-b", "safetensors"),
		},
		continueToken: "ns-next", resourceVersion: "101",
	})
	client.setPage("ServingRuntime", "ns-next", runtimeListPage{
		serving: []v1beta1.ServingRuntime{
			namespacedRuntime("team-a", "ns-c", "safetensors"),
		},
		resourceVersion: "101",
	})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{
		clusterServing: []v1beta1.ClusterServingRuntime{
			clusterRuntime("cluster-a", "safetensors"),
			clusterRuntime("cluster-b", "safetensors"),
		},
		continueToken: "cluster-next", resourceVersion: "101",
	})
	client.setPage("ClusterServingRuntime", "cluster-next", runtimeListPage{
		clusterServing: []v1beta1.ClusterServingRuntime{
			clusterRuntime("cluster-c", "safetensors"),
		},
		resourceVersion: "101",
	})

	snapshot, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.NoError(t, err)
	require.Len(t, snapshot.candidates, 6)

	calls, gets := client.observations()
	assert.Equal(t, 0, gets, "candidate capture must not issue per-object GETs")
	require.Len(t, calls, 4)
	assert.Equal(t, []runtimeListCall{
		{kind: "ServingRuntime", namespace: "team-a", limit: 2, hasDeadline: true},
		{kind: "ServingRuntime", namespace: "team-a", limit: 2, continueToken: "ns-next", hasDeadline: true},
		{
			kind: "ClusterServingRuntime", limit: 2, hasDeadline: true,
			resourceVersion: "101", resourceVersionMatch: metav1.ResourceVersionMatchExact,
		},
		{kind: "ClusterServingRuntime", limit: 2, continueToken: "cluster-next", hasDeadline: true},
	}, calls)
	assert.Empty(t, calls[3].resourceVersion,
		"continuation requests must not repeat the exact resourceVersion")
	assert.Empty(t, calls[3].resourceVersionMatch,
		"continuation requests must not repeat ResourceVersionMatch")

	// The real selector may issue repeated internal LISTs. Both must be served
	// from the captured snapshot and never touch the API client again.
	model := &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "safetensors"}}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a"}}
	client.setPage("ServingRuntime", "", runtimeListPage{
		serving: []v1beta1.ServingRuntime{
			namespacedRuntime("team-a", "mutated-after-capture", "onnx"),
		},
	})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{})
	selector := runtimeselector.New(snapshot.client)
	first, err := selector.GetCompatibleRuntimes(context.Background(), model, isvc, "team-a")
	require.NoError(t, err)
	second, err := selector.GetCompatibleRuntimes(context.Background(), model, isvc, "team-a")
	require.NoError(t, err)
	assert.Equal(t, first, second)
	assert.NotContains(t, strings.Join(runtimeMatchSummary(first), ","), "mutated-after-capture")
	after, afterGets := client.observations()
	assert.Equal(t, calls, after)
	assert.Equal(t, gets, afterGets)
}

func TestCollectRuntimeCandidateSnapshotAllowsExactItemBudget(t *testing.T) {
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		serving: []v1beta1.ServingRuntime{
			namespacedRuntime("team-a", "one", "safetensors"),
			namespacedRuntime("team-a", "two", "safetensors"),
		},
	})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{})
	limits := testRuntimeLimits()
	limits.MaxItems = 2

	snapshot, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", limits,
	)
	require.NoError(t, err)
	assert.Len(t, snapshot.candidates, 2)

	calls, _ := client.observations()
	require.Len(t, calls, 2, "both runtime kinds must be proven complete")
	assert.Equal(t, int64(1), calls[1].limit,
		"the zero-budget completeness probe must remain finite")
}

func TestExplainSnapshotTruncationIsAtomicAndPrivate(t *testing.T) {
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "safetensors"}},
	}
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		serving: []v1beta1.ServingRuntime{
			namespacedRuntime("team-a", "hostile-runtime", "safetensors"),
		},
		continueToken: "secret-continuation-token",
	})

	var stdout, stderr bytes.Buffer
	o := &explainOptions{
		IOStreams: genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr},
		Model:     "llama", namespaceOptions: namespace.NewOptions(),
		candidateLimits: paging.Limits{
			PageSize: 1, MaxItems: 1, MaxPages: 1, RequestTimeout: time.Second,
		},
	}
	err := o.Run(context.Background(), factory.Static{
		OME: omefake.NewSimpleClientset(model), Runtime: client, NS: "team-a",
	})
	require.Error(t, err)
	assert.Empty(t, stdout.String())
	assert.Empty(t, stderr.String())
	assert.Contains(t, err.Error(), "runtime candidate snapshot")
	assert.NotContains(t, err.Error(), "secret-continuation-token")
	assert.NotContains(t, err.Error(), "hostile-runtime")
}

func TestExplainSnapshotBoundsEveryHumanOutputLine(t *testing.T) {
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama"},
		Spec: v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{
			Name: "an-intentionally-long-model-format-name-that-must-wrap-safely",
		}},
	}
	runtime := clusterRuntime(
		"an-intentionally-long-runtime-name-that-must-wrap-safely",
		"a-different-intentionally-long-format-name-that-must-wrap-safely",
	)
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{
		clusterServing: []v1beta1.ClusterServingRuntime{runtime},
	})

	var output bytes.Buffer
	o := &explainOptions{
		IOStreams: genericiooptions.IOStreams{Out: &output, ErrOut: &output},
		Model:     "llama", namespaceOptions: namespace.NewOptions(),
		candidateLimits: testRuntimeLimits(),
	}
	err := o.Run(context.Background(), factory.Static{
		OME: omefake.NewSimpleClientset(model), Runtime: client, NS: "team-a",
	})
	require.NoError(t, err)
	for _, line := range strings.Split(strings.TrimSuffix(output.String(), "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 80, "human output line is too wide: %q", line)
	}
}

func TestExplainSnapshotBoundsEmptyResultMessage(t *testing.T) {
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama"},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat: v1beta1.ModelFormat{Name: "safetensors"},
		},
	}
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{})

	var output bytes.Buffer
	o := &explainOptions{
		IOStreams: genericiooptions.IOStreams{Out: &output, ErrOut: &output},
		Model:     "llama", namespaceOptions: namespace.NewOptions(),
		candidateLimits: testRuntimeLimits(),
	}
	namespaceName := strings.Repeat("a", 63)
	err := o.Run(context.Background(), factory.Static{
		OME: omefake.NewSimpleClientset(model), Runtime: client, NS: namespaceName,
	})
	require.NoError(t, err)
	for _, line := range strings.Split(strings.TrimSpace(output.String()), "\n") {
		assert.LessOrEqual(t, len(line), 80,
			"empty-result output line is too wide: %q", line)
	}
}

func TestExplainListAndGetCountsDoNotGrowWithRejectedCandidates(t *testing.T) {
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "safetensors"}},
	}
	items := make([]v1beta1.ClusterServingRuntime, 200)
	for i := range items {
		items[i] = clusterRuntime(fmt.Sprintf("incompatible-%03d", i), "onnx")
	}
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{clusterServing: items})

	var output bytes.Buffer
	o := &explainOptions{
		IOStreams: genericiooptions.IOStreams{Out: &output, ErrOut: &output},
		Model:     "llama", namespaceOptions: namespace.NewOptions(),
		candidateLimits: defaultRuntimeExplainLimits(),
	}
	err := o.Run(context.Background(), factory.Static{
		OME: omefake.NewSimpleClientset(model), Runtime: client, NS: "team-a",
	})
	require.NoError(t, err)
	assert.Contains(t, output.String(), "incompatible-199")

	calls, gets := client.observations()
	assert.Len(t, calls, 2, "candidate count must not add API LIST calls")
	assert.Zero(t, gets, "candidate count must not add API GET calls")
}

func TestExplainStopsBeforeTableWhenPinNoteCannotBeWritten(t *testing.T) {
	writeErr := errors.New("writer sentinel")
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "llama"},
		Spec:       v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: "safetensors"}},
	}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "team-a"},
		Spec: v1beta1.InferenceServiceSpec{
			Model:   &v1beta1.ModelRef{Name: "llama"},
			Runtime: &v1beta1.ServingRuntimeRef{Name: "runtime"},
		},
	}
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{
		clusterServing: []v1beta1.ClusterServingRuntime{
			clusterRuntime("runtime", "safetensors"),
		},
	})

	var stdout bytes.Buffer
	o := &explainOptions{
		IOStreams: genericiooptions.IOStreams{
			Out: &stdout, ErrOut: failingWriter{err: writeErr},
		},
		ISVC: "service", namespaceOptions: namespace.NewOptions(),
		candidateLimits: testRuntimeLimits(),
	}
	err := o.Run(context.Background(), factory.Static{
		OME: omefake.NewSimpleClientset(model, isvc), Runtime: client, NS: "team-a",
	})
	require.ErrorIs(t, err, writeErr)
	assert.Empty(t, stdout.String())
}

func TestCollectRuntimeCandidateSnapshotRejectsInvalidIdentityAndDuplicates(t *testing.T) {
	tests := []struct {
		name  string
		items []v1beta1.ServingRuntime
	}{
		{
			name: "wrong namespace",
			items: []v1beta1.ServingRuntime{
				namespacedRuntime("other-team", "private-name", "safetensors"),
			},
		},
		{
			name: "empty name",
			items: []v1beta1.ServingRuntime{
				namespacedRuntime("team-a", "", "safetensors"),
			},
		},
		{
			name: "invalid name",
			items: []v1beta1.ServingRuntime{
				namespacedRuntime("team-a", "private_name", "safetensors"),
			},
		},
		{
			name: "duplicate",
			items: []v1beta1.ServingRuntime{
				namespacedRuntime("team-a", "private-name", "safetensors"),
				namespacedRuntime("team-a", "private-name", "onnx"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newScriptedRuntimeClient(t)
			client.setPage("ServingRuntime", "", runtimeListPage{serving: test.items})
			client.setPage("ClusterServingRuntime", "", runtimeListPage{})

			_, err := collectRuntimeCandidateSnapshot(
				context.Background(), client, "team-a", testRuntimeLimits(),
			)
			require.Error(t, err)
			assert.NotContains(t, err.Error(), "private-name")
			assert.NotContains(t, err.Error(), "private_name")
			assert.NotContains(t, err.Error(), "other-team")
		})
	}
}

func TestCollectRuntimeCandidateSnapshotRejectsInconsistentPages(t *testing.T) {
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		serving: []v1beta1.ServingRuntime{
			namespacedRuntime("team-a", "first", "safetensors"),
		},
		continueToken: "opaque-private-token", resourceVersion: "101",
	})
	client.setPage("ServingRuntime", "opaque-private-token", runtimeListPage{
		serving: []v1beta1.ServingRuntime{
			namespacedRuntime("team-a", "second", "safetensors"),
		},
		resourceVersion: "changed",
	})

	_, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "inconsistent")
	assert.NotContains(t, err.Error(), "opaque-private-token")
	assert.NotContains(t, err.Error(), "changed")
}

func TestCollectRuntimeCandidateSnapshotRejectsCrossKindRevisionChange(t *testing.T) {
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		resourceVersion: "101",
	})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{
		resourceVersion: "private-newer-revision",
	})

	_, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.ErrorIs(t, err, errRuntimeSnapshotInconsistent)
	assert.NotContains(t, err.Error(), "private-newer-revision")
	calls, _ := client.observations()
	require.Len(t, calls, 2)
	assert.Equal(t, "101", calls[1].resourceVersion)
	assert.Equal(t, metav1.ResourceVersionMatchExact,
		calls[1].resourceVersionMatch)
}

func TestCollectRuntimeCandidateSnapshotRejectsEmptyResourceVersion(t *testing.T) {
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		emptyResourceVersion: true,
	})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{})

	_, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.ErrorIs(t, err, errRuntimeSnapshotInconsistent)
	assert.NotContains(t, err.Error(), "resourceVersion")
}

func TestCollectRuntimeCandidateSnapshotRejectsWrongListGVK(t *testing.T) {
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		typeMeta: metav1.TypeMeta{
			APIVersion: v1beta1.SchemeGroupVersion.String(),
			Kind:       "ClusterServingRuntimeList",
		},
	})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{})

	_, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.ErrorIs(t, err, errRuntimeSnapshotIdentity)
	assert.NotContains(t, err.Error(), "ClusterServingRuntimeList")
}

func TestCollectRuntimeCandidateSnapshotRejectsUnexpectedPaginationPrivately(t *testing.T) {
	t.Run("continue token cycle", func(t *testing.T) {
		client := newScriptedRuntimeClient(t)
		client.setPage("ServingRuntime", "", runtimeListPage{
			continueToken: "private-cycle-token", resourceVersion: "101",
		})
		client.setPage("ServingRuntime", "private-cycle-token", runtimeListPage{
			continueToken: "private-cycle-token", resourceVersion: "101",
		})

		_, err := collectRuntimeCandidateSnapshot(
			context.Background(), client, "team-a", testRuntimeLimits(),
		)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "continue token")
		assert.NotContains(t, err.Error(), "private-cycle-token")
	})

	t.Run("response exceeds request limit", func(t *testing.T) {
		client := newScriptedRuntimeClient(t)
		client.setPage("ServingRuntime", "", runtimeListPage{
			serving: []v1beta1.ServingRuntime{
				namespacedRuntime("team-a", "private-one", "safetensors"),
				namespacedRuntime("team-a", "private-two", "safetensors"),
				namespacedRuntime("team-a", "private-three", "safetensors"),
			},
		})

		_, err := collectRuntimeCandidateSnapshot(
			context.Background(), client, "team-a", testRuntimeLimits(),
		)
		require.ErrorIs(t, err, errRuntimeSnapshotInconsistent)
		assert.NotContains(t, err.Error(), "private-one")
	})
}

func TestCollectRuntimeCandidateSnapshotDoesNotResolveUnusedInheritance(t *testing.T) {
	tests := []struct {
		name  string
		items []v1beta1.ServingRuntime
	}{
		{
			name: "missing parent",
			items: []v1beta1.ServingRuntime{
				withRuntimeParent(namespacedRuntime("team-a", "child", "safetensors"), "private-missing-parent"),
			},
		},
		{
			name: "cycle",
			items: []v1beta1.ServingRuntime{
				withRuntimeParent(namespacedRuntime("team-a", "a", "safetensors"), "b"),
				withRuntimeParent(namespacedRuntime("team-a", "b", "safetensors"), "a"),
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			client := newScriptedRuntimeClient(t)
			client.setPage("ServingRuntime", "", runtimeListPage{serving: test.items})
			client.setPage("ClusterServingRuntime", "", runtimeListPage{})

			snapshot, err := collectRuntimeCandidateSnapshot(
				context.Background(), client, "team-a", testRuntimeLimits(),
			)
			require.NoError(t, err)
			model := &v1beta1.BaseModelSpec{
				ModelFormat: v1beta1.ModelFormat{Name: "safetensors"},
			}
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{Namespace: "team-a"},
			}
			got, err := runtimeselector.New(snapshot.client).GetCompatibleRuntimes(
				context.Background(), model, isvc, "team-a",
			)
			require.NoError(t, err)
			assert.NotEmpty(t, got,
				"explain must preserve the operator selector's declared-spec behavior")
			_, gets := client.observations()
			assert.Zero(t, gets)
		})
	}
}

func withRuntimeParent(runtime v1beta1.ServingRuntime, parent string) v1beta1.ServingRuntime {
	runtime.Annotations = map[string]string{constants.RuntimeInheritFromAnnotationKey: parent}
	return runtime
}

func TestCollectRuntimeCandidateSnapshotPreservesShadowing(t *testing.T) {
	clusterParent := clusterRuntime("base", "onnx")
	namespaceParent := namespacedRuntime("team-a", "base", "safetensors")
	child := withRuntimeParent(v1beta1.ServingRuntime{
		ObjectMeta: metav1.ObjectMeta{Name: "child", Namespace: "team-a"},
	}, "base")
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		serving: []v1beta1.ServingRuntime{namespaceParent, child},
	})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{
		clusterServing: []v1beta1.ClusterServingRuntime{clusterParent},
	})

	snapshot, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.NoError(t, err)
	before, beforeGets := client.observations()
	resolved, _, err := runtimeinheritance.ResolveNamespacedRuntime(
		context.Background(), snapshot.client, "team-a", "child",
	)
	require.NoError(t, err)
	require.NotNil(t, resolved)
	require.Len(t, resolved.SupportedModelFormats, 1)
	assert.Equal(t, "safetensors", resolved.SupportedModelFormats[0].ModelFormat.Name)
	after, afterGets := client.observations()
	assert.Equal(t, before, after)
	assert.Equal(t, beforeGets, afterGets,
		"inheritance reads must remain inside the captured snapshot")
}

func TestCollectRuntimeCandidateSnapshotHonorsCancellationAndTimeout(t *testing.T) {
	t.Run("parent cancellation", func(t *testing.T) {
		client := newScriptedRuntimeClient(t)
		client.setPage("ServingRuntime", "", runtimeListPage{waitForContext: true})
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err := collectRuntimeCandidateSnapshot(ctx, client, "team-a", testRuntimeLimits())
		require.ErrorIs(t, err, context.Canceled)
	})

	t.Run("per request timeout", func(t *testing.T) {
		client := newScriptedRuntimeClient(t)
		client.setPage("ServingRuntime", "", runtimeListPage{waitForContext: true})
		limits := testRuntimeLimits()
		limits.RequestTimeout = 10 * time.Millisecond

		_, err := collectRuntimeCandidateSnapshot(
			context.Background(), client, "team-a", limits,
		)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestRuntimeSnapshotSelectorMatchesOperatorRanking(t *testing.T) {
	format := "safetensors"
	priorityHigh := int32(20)
	priorityLow := int32(10)
	newer := metav1.NewTime(time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC))
	older := metav1.NewTime(newer.Add(-time.Hour))

	nsHigh := namespacedRuntime("team-a", "ns-high", format)
	nsHigh.CreationTimestamp = newer
	nsHigh.Spec.SupportedModelFormats[0].Priority = &priorityHigh
	nsLow := namespacedRuntime("team-a", "ns-low", format)
	nsLow.CreationTimestamp = older
	nsLow.Spec.SupportedModelFormats[0].Priority = &priorityLow
	clusterHigh := clusterRuntime("cluster-high", format)
	clusterHigh.Spec.SupportedModelFormats[0].Priority = &priorityHigh
	clusterWrong := clusterRuntime("cluster-wrong", "onnx")

	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		serving: []v1beta1.ServingRuntime{nsLow, nsHigh},
	})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{
		clusterServing: []v1beta1.ClusterServingRuntime{clusterWrong, clusterHigh},
	})
	snapshot, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.NoError(t, err)

	model := &v1beta1.BaseModelSpec{ModelFormat: v1beta1.ModelFormat{Name: format}}
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "team-a"}}
	got, err := runtimeselector.New(snapshot.client).GetCompatibleRuntimes(
		context.Background(), model, isvc, "team-a",
	)
	require.NoError(t, err)

	operatorClient := ctrlfake.NewClientBuilder().WithScheme(scheme(t)).WithObjects(
		nsLow.DeepCopy(), nsHigh.DeepCopy(), clusterWrong.DeepCopy(), clusterHigh.DeepCopy(),
	).Build()
	want, err := runtimeselector.New(operatorClient).GetCompatibleRuntimes(
		context.Background(), model, isvc, "team-a",
	)
	require.NoError(t, err)

	assert.Equal(t, runtimeMatchSummary(want), runtimeMatchSummary(got))
}

func runtimeMatchSummary(matches []runtimeselector.RuntimeMatch) []string {
	result := make([]string, 0, len(matches))
	for _, match := range matches {
		result = append(result, fmt.Sprintf("%t/%s/%d", match.IsCluster, match.Name, match.Score))
	}
	return result
}

func TestSnapshotClientMissingObjectsRemainTypedNotFound(t *testing.T) {
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{})
	snapshot, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.NoError(t, err)

	err = snapshot.client.Get(
		context.Background(), ctrlclient.ObjectKey{Name: "missing", Namespace: "team-a"},
		&v1beta1.ServingRuntime{},
	)
	require.Error(t, err)
	assert.True(t, apierrors.IsNotFound(err))
	_, gets := client.observations()
	assert.Equal(t, 0, gets)
}

func TestSnapshotClientRejectsUnexpectedRawListOptions(t *testing.T) {
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{})
	snapshot, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.NoError(t, err)

	err = snapshot.client.List(
		context.Background(),
		&v1beta1.ServingRuntimeList{},
		&ctrlclient.ListOptions{
			Namespace: "team-a",
			Raw:       &metav1.ListOptions{LabelSelector: "private=selector"},
		},
	)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unexpected LIST options")
	assert.NotContains(t, err.Error(), "private=selector")
	calls, _ := client.observations()
	assert.Len(t, calls, 2, "unexpected snapshot queries must not reach the API")
}

func TestCollectRuntimeCandidateSnapshotDoesNotReturnPartialOnListFailure(t *testing.T) {
	transportErr := errors.New("private transport details")
	client := newScriptedRuntimeClient(t)
	client.setPage("ServingRuntime", "", runtimeListPage{
		serving: []v1beta1.ServingRuntime{
			namespacedRuntime("team-a", "candidate", "safetensors"),
		},
	})
	client.setPage("ClusterServingRuntime", "", runtimeListPage{
		err: transportErr,
	})

	snapshot, err := collectRuntimeCandidateSnapshot(
		context.Background(), client, "team-a", testRuntimeLimits(),
	)
	require.Error(t, err)
	assert.Nil(t, snapshot)
	assert.ErrorIs(t, err, transportErr, "typed transport cause must remain inspectable")
	for _, rendered := range []string{
		err.Error(),
		fmt.Sprintf("%v", err),
		fmt.Sprintf("%+v", err),
		fmt.Sprintf("%#v", err),
		fmt.Sprintf("%q", err),
		fmt.Sprintf("%d", err),
		fmt.Sprintf("%c", err),
		fmt.Sprintf("%U", err),
		fmt.Sprintf("%#x", err),
		fmt.Sprintf("%20.5v", err),
	} {
		assert.Equal(t, "runtime candidate snapshot API request failed", rendered)
		assert.NotContains(t, rendered, "private transport details")
	}
}
