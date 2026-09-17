package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/namespace"
)

func explainEffectiveFixture(t *testing.T) (*acquisitionFactory, *recordingRuntimeClient) {
	t.Helper()
	f, _ := healthyPinnedFactory(t)
	model := &v1beta1.ClusterBaseModel{
		ObjectMeta: metav1.ObjectMeta{Name: "model"},
		Spec: v1beta1.BaseModelSpec{
			ModelFormat: v1beta1.ModelFormat{Name: "safetensors"},
		},
	}
	_, err := f.ome.OmeV1beta1().ClusterBaseModels().Create(context.Background(), model, metav1.CreateOptions{})
	require.NoError(t, err)
	require.NoError(t, f.runtime.Create(context.Background(), model))
	isvc, err := f.ome.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
	require.NoError(t, err)
	isvc.Spec.Model = &v1beta1.ModelRef{Name: "model"}
	_, err = f.ome.OmeV1beta1().InferenceServices("team-a").Update(context.Background(), isvc, metav1.UpdateOptions{})
	require.NoError(t, err)
	runtimeClient := &recordingRuntimeClient{Client: f.runtime}
	f.runtime = runtimeClient
	return f, runtimeClient
}

func TestExplainWithEffectivePreservesSelectorVerdictAndAddsBoundedContext(t *testing.T) {
	baselineFactory, baselineReads := explainEffectiveFixture(t)
	baseline, err := execute(t, baselineFactory, "explain", "--isvc", "service")
	require.NoError(t, err)
	assert.Equal(t, "Note: InferenceService \"service\" pins spec.runtime=\"cluster-runtime\"; the table below shows what automatic selection would choose, which may differ from what is currently deployed.\n"+
		"RUNTIME           SCOPE     COMPATIBLE   PRIORITY   WEIGHT   REASON\n"+
		"cluster-runtime   Cluster   No           -          -        model format 'mt:safetensors' not in supported formats: no supported formats defined\n", baseline)
	baselineLists := countRuntimeLists(baselineReads.operations)

	contextFactory, contextReads := explainEffectiveFixture(t)
	output, err := execute(t, contextFactory, "explain", "--isvc", "service", "--with-effective", "--ome-namespace", "control-plane")
	require.NoError(t, err)
	assert.Equal(t, row(t, baseline, "cluster-runtime")[2], row(t, output, "cluster-runtime")[2], "effective context must preserve the selector verdict")
	assert.Contains(t, output, "model format")
	assert.Contains(t, strings.Join(strings.Fields(output), " "), "not in supported formats: no supported formats defined")
	assert.Contains(t, output, "Effective context (separate observation; selector verdict unchanged)")
	assert.Contains(t, output, "SELECTION")
	assert.Contains(t, output, "Explicit")
	assert.Contains(t, output, "INHERITANCE")
	assert.Contains(t, output, "Observed")
	assert.Contains(t, output, "ROOT-FIRST")
	assert.Contains(t, output, "CSR/cluster-runtime")
	assert.Equal(t, baselineLists, countRuntimeLists(contextReads.operations), "effective context must not add broad runtime lists for a named runtime")
	for _, line := range strings.Split(strings.TrimSpace(output), "\n") {
		assert.LessOrEqual(t, len(line), 80, "opt-in table line is too wide: %q", line)
	}
	t.Logf("output (synthetic fixture):\n%s", output)
}

func TestExplainWithEffectiveUnavailableLeavesSelectorVerdict(t *testing.T) {
	f, _ := explainEffectiveFixture(t)
	baseline, err := execute(t, f, "explain", "--isvc", "service")
	require.NoError(t, err)
	f.kube = nil
	output, err := execute(t, f, "explain", "--isvc", "service", "--with-effective")
	require.NoError(t, err)
	assert.Equal(t, row(t, baseline, "cluster-runtime")[2], row(t, output, "cluster-runtime")[2])
	assert.Contains(t, output, "EVIDENCE")
	assert.Contains(t, output, "Unavailable")
	assert.NotContains(t, output, "<nil>")
}

type failingExplainKubeFactory struct {
	factory.Factory
	err error
}

func (f failingExplainKubeFactory) KubeClient() (kubernetes.Interface, error) {
	return nil, f.err
}

func TestExplainWithEffectiveRedactsOptionalClientErrors(t *testing.T) {
	f, _ := explainEffectiveFixture(t)
	baseline, err := execute(t, f, "explain", "--isvc", "service")
	require.NoError(t, err)
	secret := "private-registry-token=do-not-print"
	output, err := execute(t, failingExplainKubeFactory{Factory: f, err: errors.New(secret)},
		"explain", "--isvc", "service", "--with-effective")
	require.NoError(t, err)
	assert.Equal(t, row(t, baseline, "cluster-runtime")[2], row(t, output, "cluster-runtime")[2])
	assert.Contains(t, output, "Unavailable")
	assert.NotContains(t, output, secret)
}

func TestExplainWithEffectiveRequiresISVC(t *testing.T) {
	_, err := execute(t, factory.Static{NS: "demo"}, "explain", "--model", "model", "--with-effective")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "--with-effective requires --isvc")
}

func TestExplainEffectiveRejectsMismatchedServiceBeforeOtherReads(t *testing.T) {
	f, _ := healthyPinnedFactory(t)
	isvc, err := f.ome.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
	require.NoError(t, err)
	isvc.Name = "different"
	o := &explainOptions{ISVC: "service", namespaceOptions: namespace.NewOptions()}
	_, err = o.projectEffectiveContext(context.Background(), f, f.runtime, "team-a", isvc)
	require.Error(t, err)
	assert.Zero(t, f.kubeGet, "wrong primary identity must stop before optional client acquisition")
}

func TestExplainEffectivePropagatesCancellation(t *testing.T) {
	for _, tc := range []struct {
		name    string
		context func() context.Context
		want    error
	}{
		{name: "canceled", context: func() context.Context {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx
		}, want: context.Canceled},
		{name: "deadline", context: func() context.Context {
			ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			t.Cleanup(cancel)
			return ctx
		}, want: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, _ := healthyPinnedFactory(t)
			isvc, err := f.ome.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
			require.NoError(t, err)
			var out bytes.Buffer
			o := &explainOptions{
				IOStreams: genericiooptions.IOStreams{Out: &out},
				ISVC:      "service", namespaceOptions: namespace.NewOptions(),
			}
			err = o.writeEffectiveContext(tc.context(), f, f.runtime, "team-a", isvc)
			assert.True(t, errors.Is(err, tc.want), "error = %v, want %v", err, tc.want)
			assert.Empty(t, out.String(), "cancellation must not leave a partial context section")
		})
	}
}

type continuedRuntimeCandidateClient struct {
	ctrlclient.Client
	boundedPages int
}

func (c *continuedRuntimeCandidateClient) List(
	ctx context.Context,
	list ctrlclient.ObjectList,
	opts ...ctrlclient.ListOption,
) error {
	options := &ctrlclient.ListOptions{}
	options.ApplyOptions(opts)
	if candidates, ok := list.(*v1beta1.ServingRuntimeList); ok && options.Limit > 0 {
		c.boundedPages++
		candidates.Items = []v1beta1.ServingRuntime{{
			ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("page-%d", c.boundedPages), Namespace: options.Namespace},
		}}
		candidates.Continue = fmt.Sprintf("next-%d", c.boundedPages)
		return nil
	}
	return c.Client.List(ctx, list, opts...)
}

func TestExplainWithEffectiveTruncatedAutoSelectionKeepsSelectorVerdict(t *testing.T) {
	f, _ := explainEffectiveFixture(t)
	isvc, err := f.ome.OmeV1beta1().InferenceServices("team-a").Get(context.Background(), "service", metav1.GetOptions{})
	require.NoError(t, err)
	isvc.Spec.Runtime = nil
	_, err = f.ome.OmeV1beta1().InferenceServices("team-a").Update(context.Background(), isvc, metav1.UpdateOptions{})
	require.NoError(t, err)
	continued := &continuedRuntimeCandidateClient{Client: f.runtime}
	f.runtime = continued

	baseline, err := execute(t, f, "explain", "--isvc", "service")
	require.NoError(t, err)
	output, err := execute(t, f, "explain", "--isvc", "service", "--with-effective")
	require.NoError(t, err)
	assert.Equal(t, row(t, baseline, "cluster-runtime")[2], row(t, output, "cluster-runtime")[2])
	assert.Equal(t, 2, continued.boundedPages, "auto-selection evidence must stop at its two-page budget")
	assert.Contains(t, output, "Unavailable")
	assert.Contains(t, output, "InheritanceUnavailable")
	assert.Contains(t, output, "LiveRuntimeUnavailable")
	assert.NotContains(t, output, "next-1", "continuation tokens must never be printed")
}

func countRuntimeLists(operations []runtimeClientOperation) int {
	count := 0
	for _, operation := range operations {
		if operation.verb == "list" {
			count++
		}
	}
	return count
}
