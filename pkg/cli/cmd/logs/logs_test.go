package logs

import (
	"bytes"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	kubefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

func pod(name, isvc, component string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
		Name: name, Namespace: "team-a",
		Labels: map[string]string{
			constants.InferenceServiceLabel: isvc,
			constants.OMEComponentLabel:     component,
		},
	}}
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

func executeWithDependencies(t *testing.T, f factory.Factory, deps dependencies, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &out}
	cmd := newCmdWithDependencies(f, streams, deps)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func omenativePod(name, isvc, component, instance, revision string) *corev1.Pod {
	p := pod(name, isvc, component)
	p.Labels[query.LabelManagedBy] = query.ManagedByOMENative
	p.Labels[query.LabelInstanceIdx] = instance
	p.Labels[query.LabelRevisionHash] = revision
	return p
}

func TestLogsPrefixesMultiplePods(t *testing.T) {
	f := factory.Static{
		Kube: kubefake.NewSimpleClientset(
			pod("llama-engine-1", "llama", "engine"),
			pod("llama-decoder-1", "llama", "decoder"),
		),
		NS: "team-a",
	}
	out, err := execute(t, f, "llama")
	require.NoError(t, err)
	assert.Contains(t, out, "[engine/llama-engine-1] fake logs")
	assert.Contains(t, out, "[decoder/llama-decoder-1] fake logs")
}

func TestLogsComponentFilterSinglePodNoPrefix(t *testing.T) {
	f := factory.Static{
		Kube: kubefake.NewSimpleClientset(
			pod("llama-engine-1", "llama", "engine"),
			pod("llama-decoder-1", "llama", "decoder"),
		),
		NS: "team-a",
	}
	out, err := execute(t, f, "llama", "-c", "engine")
	require.NoError(t, err)
	assert.Equal(t, "fake logs\n", out)
}

func TestLogsNoPodsIsError(t *testing.T) {
	f := factory.Static{Kube: kubefake.NewSimpleClientset(), NS: "team-a"}
	_, err := execute(t, f, "llama")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pods found")
}

func TestLogsRejectsBadComponent(t *testing.T) {
	_, err := execute(t, factory.Static{NS: "d"}, "llama", "-c", "gpu")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid component")
}

func TestLogsFiltersOMENativePodsByInstanceAndRevision(t *testing.T) {
	kube := kubefake.NewSimpleClientset(
		omenativePod("llama-engine-2-current", "llama", "engine", "2", "deadbeef"),
		omenativePod("llama-engine-3", "llama", "engine", "3", "deadbeef"),
		omenativePod("llama-engine-2-old", "llama", "engine", "2", "cafebabe"),
		pod("llama-engine-2-legacy", "llama", "engine"),
	)
	f := factory.Static{Kube: kube, NS: "team-a"}

	out, err := execute(t, f, "llama", "--component", "engine", "--instance", "2", "--revision", "deadbeef")
	require.NoError(t, err)
	assert.Equal(t, "fake logs\n", out)

	var listAction k8stesting.ListAction
	for _, action := range kube.Actions() {
		if candidate, ok := action.(k8stesting.ListAction); ok && action.GetResource().Resource == "pods" {
			listAction = candidate
		}
	}
	require.NotNil(t, listAction)
	selector := listAction.GetListRestrictions().Labels
	for key, want := range map[string]string{
		constants.InferenceServiceLabel: "llama",
		constants.OMEComponentLabel:     "engine",
		query.LabelManagedBy:            query.ManagedByOMENative,
		query.LabelInstanceIdx:          "2",
		query.LabelRevisionHash:         "deadbeef",
	} {
		got, found := selector.RequiresExactMatch(key)
		assert.Truef(t, found, "selector missing %s", key)
		assert.Equal(t, want, got)
	}
}

func TestLogsFullRevisionInfersComponent(t *testing.T) {
	f := factory.Static{
		Kube: kubefake.NewSimpleClientset(
			omenativePod("llama-decoder-0", "llama", "decoder", "0", "deadbeef"),
		),
		NS: "team-a",
	}

	out, err := execute(t, f, "llama", "--revision", "llama-decoder-deadbeef")
	require.NoError(t, err)
	assert.Equal(t, "fake logs\n", out)
}

func TestLogsFullRevisionInfersComponentForInstance(t *testing.T) {
	f := factory.Static{
		Kube: kubefake.NewSimpleClientset(
			omenativePod("llama-decoder-2", "llama", "decoder", "2", "deadbeef"),
		),
		NS: "team-a",
	}

	out, err := execute(t, f, "llama", "--instance", "2", "--revision", "llama-decoder-deadbeef")
	require.NoError(t, err)
	assert.Equal(t, "fake logs\n", out)
}

func TestLogsAcceptsMaximumInstanceIndex(t *testing.T) {
	f := factory.Static{
		Kube: kubefake.NewSimpleClientset(
			omenativePod("llama-engine-max", "llama", "engine", "2147483647", "deadbeef"),
		),
		NS: "team-a",
	}

	out, err := execute(t, f, "llama", "--component", "engine", "--instance", "2147483647")
	require.NoError(t, err)
	assert.Equal(t, "fake logs\n", out)
}

func TestLogsHelpDescribesFullRevisionComponentInference(t *testing.T) {
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{})
	flag := cmd.Flags().Lookup("instance")
	require.NotNil(t, flag)
	assert.Contains(t, flag.Usage, "requires --component or a full --revision")
}

func TestLogsRejectsInvalidOMENativeFilters(t *testing.T) {
	tooLarge := fmt.Sprintf("%d", int64(1)<<31)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "instance needs component", args: []string{"llama", "--instance", "0"}, want: "--instance requires --component"},
		{name: "hash needs component", args: []string{"llama", "--revision", "deadbeef"}, want: "hash-only --revision requires --component"},
		{name: "negative instance", args: []string{"llama", "--component", "engine", "--instance", "-2"}, want: "between 0 and 2147483647"},
		{name: "large instance", args: []string{"llama", "--component", "engine", "--instance", tooLarge}, want: "between 0 and 2147483647"},
		{name: "uppercase revision", args: []string{"llama", "--component", "engine", "--revision", "DEADBEEF"}, want: "invalid revision"},
		{name: "wrong service", args: []string{"llama", "--revision", "other-engine-deadbeef"}, want: "does not belong to InferenceService"},
		{name: "component mismatch", args: []string{"llama", "--component", "engine", "--revision", "llama-decoder-deadbeef"}, want: "does not match --component"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := execute(t, factory.Static{NS: "team-a"}, tt.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.want)
		})
	}
}

func TestLogsRejectsMalformedInferenceServiceName(t *testing.T) {
	_, err := execute(t, factory.Static{NS: "team-a"}, "Bad_Name")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid InferenceService name")
}

func TestLogsRefiltersUntrustedListResponses(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &corev1.PodList{Items: []corev1.Pod{
			*omenativePod("foreign", "other", "engine", "2", "deadbeef"),
		}}, nil
	})

	_, err := execute(t, factory.Static{Kube: kube, NS: "team-a"},
		"llama", "--component", "engine", "--instance", "2", "--revision", "deadbeef")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pods found")
}

func TestLogsRejectsMatchingPodReturnedFromAnotherNamespace(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		foreign := omenativePod("llama-engine-2", "llama", "engine", "2", "deadbeef")
		foreign.Namespace = "other-team"
		return true, &corev1.PodList{Items: []corev1.Pod{*foreign}}, nil
	})

	_, err := execute(t, factory.Static{Kube: kube, NS: "team-a"},
		"llama", "--component", "engine", "--instance", "2", "--revision", "deadbeef")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "no pods found")
}

func TestLogsFailsClosedWhenPodDiscoveryIsTruncated(t *testing.T) {
	kube := kubefake.NewSimpleClientset()
	kube.PrependReactor("list", "pods", func(action k8stesting.Action) (bool, runtime.Object, error) {
		_ = action.(k8stesting.ListAction)
		return true, &corev1.PodList{
			ListMeta: metav1.ListMeta{Continue: "more"},
			Items: []corev1.Pod{
				*omenativePod("llama-engine-0", "llama", "engine", "0", "deadbeef"),
			},
		}, nil
	})
	deps := dependencies{podLimits: paging.Limits{
		PageSize:       1,
		MaxItems:       10,
		MaxPages:       1,
		RequestTimeout: time.Second,
	}}

	_, err := executeWithDependencies(t, factory.Static{Kube: kube, NS: "team-a"}, deps,
		"llama", "--component", "engine", "--instance", "0")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pod discovery was truncated")
}
