package logs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	kubefake "k8s.io/client-go/kubernetes/fake"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/client-go/rest"
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

func TestOptionsRunUsesDefaultLogStreamAndClosesResponse(t *testing.T) {
	logBody := newCountingReadCloser(strings.NewReader("direct run logs\n"))
	kube, err := kubernetes.NewForConfig(&rest.Config{
		Host: "https://fixture.invalid",
		Transport: roundTripperFunc(func(request *http.Request) (*http.Response, error) {
			response := &http.Response{
				StatusCode: http.StatusOK,
				Header:     make(http.Header),
				Request:    request,
			}
			switch request.URL.Path {
			case "/api/v1/namespaces/team-a/pods":
				response.Header.Set("Content-Type", "application/json")
				response.Body = io.NopCloser(strings.NewReader(`{
					"apiVersion":"v1",
					"kind":"PodList",
					"metadata":{"resourceVersion":"1"},
					"items":[{
						"apiVersion":"v1",
						"kind":"Pod",
						"metadata":{"name":"chat-engine-0","namespace":"team-a","labels":{"ome.io/inferenceservice":"chat","component":"engine"}},
						"spec":{"containers":[{"name":"main"}]}
					}]
				}`))
			case "/api/v1/namespaces/team-a/pods/chat-engine-0/log":
				response.Header.Set("Content-Type", "text/plain")
				response.Body = logBody
			default:
				response.StatusCode = http.StatusNotFound
				response.Body = io.NopCloser(strings.NewReader("not found"))
			}
			return response, nil
		}),
	})
	require.NoError(t, err)
	var out bytes.Buffer
	options := &Options{
		IOStreams:      genericiooptions.IOStreams{Out: &out},
		Name:           "chat",
		Instance:       -1,
		Tail:           -1,
		MaxLogRequests: defaultMaxLogRequests,
	}

	require.NoError(t, options.Run(context.Background(), factory.Static{Kube: kube, NS: "team-a"}))
	assert.Equal(t, "direct run logs\n", out.String())
	assert.Equal(t, int32(1), logBody.closes.Load())
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

func TestLogsFollowRejectsSixTargetsBeforeOpeningAStream(t *testing.T) {
	kube := kubefake.NewSimpleClientset(sixPods("chat")...)
	var opened atomic.Int32
	deps := defaultDependencies
	deps.openLogStream = func(context.Context, coreclient.PodInterface, logTarget) (io.ReadCloser, error) {
		opened.Add(1)
		return io.NopCloser(strings.NewReader("unexpected\n")), nil
	}

	out, err := executeWithDependencies(t, factory.Static{Kube: kube, NS: "team-a"}, deps, "chat", "--follow")
	require.EqualError(t, err, "you are attempting to follow 6 log streams, but maximum allowed concurrency is 5, use --max-log-requests to increase the limit")
	assert.Empty(t, out)
	assert.Zero(t, opened.Load())
}

func TestLogsFollowHonorsRaisedRequestCap(t *testing.T) {
	kube := kubefake.NewSimpleClientset(sixPods("chat")...)
	var opened atomic.Int32
	deps := defaultDependencies
	deps.openLogStream = func(_ context.Context, _ coreclient.PodInterface, target logTarget) (io.ReadCloser, error) {
		opened.Add(1)
		return io.NopCloser(strings.NewReader(target.podName + "\n")), nil
	}

	out, err := executeWithDependencies(t, factory.Static{Kube: kube, NS: "team-a"}, deps,
		"chat", "--follow", "--max-log-requests=6")
	require.NoError(t, err)
	assert.Equal(t, int32(6), opened.Load())
	for i := range 6 {
		assert.Contains(t, out, fmt.Sprintf("[engine/chat-engine-%d] chat-engine-%d\n", i, i))
	}
}

func TestLogsRejectsInvalidMaxLogRequestsBeforeFactoryAccess(t *testing.T) {
	for _, value := range []string{"0", "-1"} {
		t.Run(value, func(t *testing.T) {
			_, err := execute(t, factory.Static{}, "chat", "--max-log-requests="+value)
			require.EqualError(t, err, "--max-log-requests must be greater than 0")
		})
	}
}

func TestLogsRejectsInvalidPagingOverrideBeforeFactoryAccess(t *testing.T) {
	t.Parallel()

	deps := defaultDependencies
	deps.podLimits.MaxRequests = deps.podLimits.MaxPages*2 + 1

	_, err := executeWithDependencies(t, struct{ factory.Factory }{}, deps, "chat")

	require.ErrorIs(t, err, errInvalidPodPaging)
}

func TestLogsHelpDocumentsFollowRequestLimit(t *testing.T) {
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{})
	flag := cmd.Flags().Lookup("max-log-requests")
	require.NotNil(t, flag)
	assert.Equal(t, "5", flag.DefValue)
	assert.Contains(t, flag.Usage, "concurrent log streams")
}

func TestLogsLimitBytesUsesPerPodLogOption(t *testing.T) {
	kube := kubefake.NewSimpleClientset(
		pod("chat-decoder-0", "chat", "decoder"),
		pod("chat-engine-0", "chat", "engine"),
	)
	out, err := execute(t, factory.Static{Kube: kube, NS: "team-a"},
		"chat", "--limit-bytes=2048", "--tail=7", "--since=30s")
	require.NoError(t, err)
	assert.Contains(t, out, "[decoder/chat-decoder-0] fake logs")
	assert.Contains(t, out, "[engine/chat-engine-0] fake logs")

	var logActions int
	for _, action := range kube.Actions() {
		if action.GetSubresource() != "log" {
			continue
		}
		generic, ok := action.(k8stesting.GenericAction)
		require.True(t, ok)
		options, ok := generic.GetValue().(*corev1.PodLogOptions)
		require.True(t, ok)
		assert.False(t, options.Follow)
		require.NotNil(t, options.LimitBytes)
		assert.Equal(t, int64(2048), *options.LimitBytes)
		require.NotNil(t, options.TailLines)
		assert.Equal(t, int64(7), *options.TailLines)
		require.NotNil(t, options.SinceSeconds)
		assert.Equal(t, int64(30), *options.SinceSeconds)
		logActions++
	}
	assert.Equal(t, 2, logActions)
}

func TestLogsRejectsInvalidLimitBytesBeforeFactoryAccess(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "negative",
			args: []string{"chat", "--limit-bytes=-1"},
			want: "--limit-bytes must be greater than or equal to 0",
		},
		{
			name: "follow",
			args: []string{"chat", "--follow", "--limit-bytes=1"},
			want: "--limit-bytes cannot be used with --follow",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := execute(t, factory.Static{}, tt.args...)
			require.EqualError(t, err, tt.want)
		})
	}
}

func TestLogsHelpDocumentsOneShotByteLimit(t *testing.T) {
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{})
	flag := cmd.Flags().Lookup("limit-bytes")
	require.NotNil(t, flag)
	assert.Equal(t, "0", flag.DefValue)
	assert.Contains(t, flag.Usage, "per pod")
	assert.Contains(t, flag.Usage, "one-shot")
}

func sixPods(isvc string) []runtime.Object {
	objects := make([]runtime.Object, 0, 6)
	for i := range 6 {
		objects = append(objects, pod(fmt.Sprintf("%s-engine-%d", isvc, i), isvc, "engine"))
	}
	return objects
}
