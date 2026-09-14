package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	appstypedv1 "k8s.io/client-go/kubernetes/typed/apps/v1"
	k8stesting "k8s.io/client-go/testing"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

type recordingHistoryKubeClient struct {
	kubernetes.Interface
	listNamespaces []string
	listOptions    []metav1.ListOptions
	listResponses  []*appsv1.ControllerRevisionList
}

func (c *recordingHistoryKubeClient) AppsV1() appstypedv1.AppsV1Interface {
	return &recordingHistoryAppsClient{AppsV1Interface: c.Interface.AppsV1(), parent: c}
}

type recordingHistoryAppsClient struct {
	appstypedv1.AppsV1Interface
	parent *recordingHistoryKubeClient
}

func (c *recordingHistoryAppsClient) ControllerRevisions(namespace string) appstypedv1.ControllerRevisionInterface {
	return &recordingHistoryRevisions{
		ControllerRevisionInterface: c.AppsV1Interface.ControllerRevisions(namespace),
		parent:                      c.parent,
		namespace:                   namespace,
	}
}

type recordingHistoryRevisions struct {
	appstypedv1.ControllerRevisionInterface
	parent    *recordingHistoryKubeClient
	namespace string
}

func (c *recordingHistoryRevisions) List(
	ctx context.Context,
	options metav1.ListOptions,
) (*appsv1.ControllerRevisionList, error) {
	c.parent.listNamespaces = append(c.parent.listNamespaces, c.namespace)
	c.parent.listOptions = append(c.parent.listOptions, options)
	index := len(c.parent.listOptions) - 1
	if index < len(c.parent.listResponses) {
		return c.parent.listResponses[index].DeepCopy(), nil
	}
	return c.ControllerRevisionInterface.List(ctx, options)
}

func executeHistory(
	t *testing.T,
	f factory.Factory,
	out io.Writer,
	args ...string,
) (string, error) {
	t.Helper()
	var errOut bytes.Buffer
	cmd := newHistoryCmd(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: out, ErrOut: &errOut,
	})
	cmd.SetArgs(args)
	return errOut.String(), cmd.Execute()
}

func TestHistoryReadsExactlyOneTargetAndBoundedHistory(t *testing.T) {
	f, _ := healthyPinnedFactory(t)
	omeClient := f.ome.(*omefake.Clientset)
	recordingKube := &recordingHistoryKubeClient{Interface: f.kube}
	f.kube = recordingKube
	var out bytes.Buffer

	errOut, err := executeHistory(t, f, &out, "service", "--ome-namespace", "control-plane")

	require.NoError(t, err)
	require.Len(t, omeClient.Actions(), 1)
	get, ok := omeClient.Actions()[0].(k8stesting.GetAction)
	require.True(t, ok)
	assert.Equal(t, "inferenceservices", get.GetResource().Resource)
	assert.Equal(t, "team-a", get.GetNamespace())
	assert.Equal(t, "service", get.GetName())
	assert.Equal(t, 1, f.namespaceGet)
	assert.Equal(t, 1, f.omeGet)
	assert.Equal(t, 1, f.kubeGet)
	assert.Equal(t, 1, f.runtimeGet)
	require.Len(t, recordingKube.listOptions, 1)
	assert.Equal(t, []string{"control-plane"}, recordingKube.listNamespaces)
	assert.Equal(t, int64(500), recordingKube.listOptions[0].Limit)
	assert.Empty(t, recordingKube.listOptions[0].Continue)
	assert.Equal(t, constants.RuntimeRevisionOfLabelKey+"=cluster-runtime", recordingKube.listOptions[0].LabelSelector)
	assert.Contains(t, out.String(), "C/B/1/1")
	assert.Empty(t, errOut)
}

func TestHistoryStopsAfterTwoPages(t *testing.T) {
	f, _ := healthyPinnedFactory(t)
	recordingKube := &recordingHistoryKubeClient{
		Interface: f.kube,
		listResponses: []*appsv1.ControllerRevisionList{
			{ListMeta: metav1.ListMeta{Continue: "second-page"}},
			{ListMeta: metav1.ListMeta{Continue: "must-not-be-read"}},
		},
	}
	f.kube = recordingKube
	var out bytes.Buffer

	errOut, err := executeHistory(t, f, &out, "service", "--ome-namespace", "control-plane")

	require.NoError(t, err)
	require.Len(t, recordingKube.listOptions, 2)
	assert.Equal(t, int64(500), recordingKube.listOptions[0].Limit)
	assert.Equal(t, int64(500), recordingKube.listOptions[1].Limit)
	assert.Empty(t, recordingKube.listOptions[0].Continue)
	assert.Equal(t, "second-page", recordingKube.listOptions[1].Continue)
	assert.Contains(t, out.String(), "P/I/2/2")
	assert.Contains(t, out.String(), "R0/G1")
	assert.Empty(t, errOut)
}

func TestHistoryHealthyPinnedFormats(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			f, _ := healthyPinnedFactory(t)
			var out bytes.Buffer
			errOut, err := executeHistory(
				t, f, &out, "service", "--ome-namespace", "control-plane", "--output", format,
			)
			require.NoError(t, err)
			assert.Empty(t, errOut)

			if format == "table" {
				assert.Equal(t, []string{
					"WINDOW", "REVISION", "CREATED", "ROLES", "CHECK", "LIVE", "ISSUES",
					"C/B/1/1", "cr...#2130c7ba", "26-08-30T01:02Z", "AQRH", "OK", "MATCH", "R0/G0",
				}, strings.Fields(out.String()))
				return
			}
			if format == "wide" {
				assert.Equal(t, []string{
					"OBSERVATION", "COMPLETENESS", "PAGES", "REVISION", "CREATED", "HASH", "ROLES", "SOURCE",
					"CONSISTENCY", "RELATION", "REVISION-ISSUES", "REPORT-ISSUES",
					"Complete", "RetentionBounded", "1/1", "cr-cluster-runtime-e0e2b0d6", "2026-08-30T01:02:03Z",
					"e0e2b0d6", "Active,Requested,Reported,History", "ClusterServingRuntime/cluster-runtime",
					"Consistent", "MatchesLive", "-", "-",
				}, strings.Fields(out.String()))
				return
			}

			var got reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent]
			if format == "json" {
				require.NoError(t, json.Unmarshal(out.Bytes(), &got))
			} else {
				require.NoError(t, yaml.Unmarshal(out.Bytes(), &got))
			}
			assertHealthyHistoryReport(t, got)
		})
	}
}

func assertHealthyHistoryReport(
	t *testing.T,
	got reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeHistoryContent],
) {
	t.Helper()
	assert.Equal(t, "cli.ome.io/v1alpha1", got.APIVersion)
	assert.Equal(t, "RuntimeHistoryReport", got.Kind)
	assert.Equal(t, reportv1alpha1.Metadata{Namespace: "team-a", Name: "service"}, got.Metadata)
	assert.False(t, got.CollectedAt.IsZero())
	assert.Equal(t, time.UTC, got.CollectedAt.Location())
	require.NotNil(t, got.Content.Runtime)
	assert.Equal(t, reportv1alpha1.RuntimeKindClusterServingRuntime, got.Content.Runtime.Kind)
	assert.Equal(t, "cluster-runtime", got.Content.Runtime.Name)
	assert.Equal(t, "runtime-uid", got.Content.Runtime.UID)
	assert.Equal(t, int64(2), got.Content.Runtime.Generation)
	assert.Equal(t, reportv1alpha1.HistoryObservationStateComplete, got.Content.Observation)
	assert.Equal(t, reportv1alpha1.HistoryCompletenessRetentionBounded, got.Content.Completeness)
	assert.Equal(t, 1, got.Content.RequestedPages)
	assert.Equal(t, 1, got.Content.ObservedPages)
	require.Len(t, got.Content.Revisions, 1)
	revision := got.Content.Revisions[0]
	assert.Equal(t, "control-plane", revision.Revision.Namespace)
	assert.Equal(t, "cr-cluster-runtime-e0e2b0d6", revision.Revision.Name)
	assert.Equal(t, "revision-uid", revision.Revision.UID)
	require.NotNil(t, revision.Revision.CreatedAt)
	assert.Equal(t, time.Date(2026, 8, 30, 1, 2, 3, 0, time.UTC), *revision.Revision.CreatedAt)
	assert.Equal(t, "e0e2b0d6", revision.Hash)
	assert.Equal(t, []reportv1alpha1.RuntimeRevisionRole{
		reportv1alpha1.RuntimeRevisionRoleActive,
		reportv1alpha1.RuntimeRevisionRoleRequested,
		reportv1alpha1.RuntimeRevisionRoleReported,
		reportv1alpha1.RuntimeRevisionRoleHistory,
	}, revision.Roles)
	assert.Equal(t, reportv1alpha1.RevisionConsistencyConsistent, revision.Consistency)
	assert.Equal(t, reportv1alpha1.RevisionRelationMatchesLive, revision.RelationToLive)
	assert.Empty(t, revision.Issues)
	assert.Empty(t, got.Content.Issues)
	assert.Empty(t, got.Warnings)
}

func TestHistoryValidationPrecedesAcquisition(t *testing.T) {
	tests := []struct {
		name               string
		args               []string
		workloadNamespace  string
		wantError          string
		wantNamespaceCalls int
	}{
		{name: "zero arguments", wantError: "accepts 1 arg(s)"},
		{name: "two arguments", args: []string{"one", "two"}, wantError: "accepts 1 arg(s)"},
		{name: "invalid service name", args: []string{"Bad_Name"}, wantError: "InferenceService name"},
		{name: "invalid output", args: []string{"service", "--output", "xml"}, wantError: "unsupported output format"},
		{name: "invalid output before invalid service name", args: []string{"Bad_Name", "--output", "xml"}, wantError: "unsupported output format"},
		{name: "invalid workload namespace", args: []string{"service"}, workloadNamespace: "Bad_NS", wantError: "workload namespace", wantNamespaceCalls: 1},
		{name: "invalid OME namespace", args: []string{"service", "--ome-namespace", "Bad_NS"}, workloadNamespace: "team-a", wantError: "OME namespace", wantNamespaceCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &validationFactory{namespace: test.workloadNamespace}
			var out bytes.Buffer
			errOut, err := executeHistory(t, f, &out, test.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.wantError)
			assert.Equal(t, test.wantNamespaceCalls, f.namespaceCalls)
			assert.Empty(t, out.String())
			assert.Empty(t, errOut)
		})
	}
}

func TestHistoryRejectsUnboundPrimarySnapshots(t *testing.T) {
	valid := func() *v1beta1.InferenceService {
		return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
			Name: "service", Namespace: "team-a", UID: types.UID("isvc-uid"), ResourceVersion: "17",
		}}
	}
	tests := []struct {
		name string
		got  func() *v1beta1.InferenceService
		want error
	}{
		{name: "nil response", got: func() *v1beta1.InferenceService { return nil }, want: errNilInferenceService},
		{name: "wrong name", got: func() *v1beta1.InferenceService { value := valid(); value.Name = "hostile-returned-name"; return value }, want: errInferenceServiceNameMismatch},
		{name: "wrong namespace", got: func() *v1beta1.InferenceService {
			value := valid()
			value.Namespace = "hostile-returned-namespace"
			return value
		}, want: errInferenceServiceNamespaceMismatch},
		{name: "empty UID", got: func() *v1beta1.InferenceService { value := valid(); value.UID = ""; return value }, want: errInferenceServiceUIDEmpty},
		{name: "empty resource version", got: func() *v1beta1.InferenceService { value := valid(); value.ResourceVersion = ""; return value }, want: errInferenceServiceVersionEmpty},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ome := omefake.NewSimpleClientset()
			ome.PrependReactor("get", "inferenceservices", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
				return true, test.got(), nil
			})
			f := &acquisitionFactory{
				ome: ome, kube: k8sfake.NewSimpleClientset(),
				runtime: ctrlfake.NewClientBuilder().WithScheme(scheme(t)).Build(), namespace: "team-a",
			}
			var out bytes.Buffer
			errOut, err := executeHistory(t, f, &out, "service")
			require.Error(t, err)
			assert.ErrorIs(t, err, test.want)
			assert.NotContains(t, err.Error(), "hostile-returned")
			assert.Zero(t, f.kubeGet)
			assert.Zero(t, f.runtimeGet)
			assert.Empty(t, out.String())
			assert.Empty(t, errOut)
		})
	}
}

func TestHistoryPreservesRequiredFailureCauses(t *testing.T) {
	namespaceFailure := errors.New("namespace sentinel")
	omeFailure := errors.New("OME client sentinel")
	kubeFailure := errors.New("Kubernetes client sentinel")
	runtimeFailure := errors.New("runtime client sentinel")
	tests := []struct {
		name      string
		want      error
		configure func(*failingFactory)
	}{
		{name: "namespace", want: namespaceFailure, configure: func(f *failingFactory) { f.nsErr = namespaceFailure }},
		{name: "OME client", want: omeFailure, configure: func(f *failingFactory) { f.omeErr = omeFailure }},
		{name: "Kubernetes client", want: kubeFailure, configure: func(f *failingFactory) { f.kubeErr = kubeFailure }},
		{name: "runtime client", want: runtimeFailure, configure: func(f *failingFactory) { f.runtimeErr = runtimeFailure }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := completeFactory(t)
			test.configure(f)
			var out bytes.Buffer
			errOut, err := executeHistory(t, f, &out, "service")
			require.Error(t, err)
			assert.ErrorIs(t, err, test.want)
			assert.Empty(t, out.String())
			assert.Empty(t, errOut)
		})
	}
}

func TestHistoryOptionalListFailureIsBoundedDiagnostic(t *testing.T) {
	const canary = "secret-history-list-failure"
	f, _ := healthyPinnedFactory(t)
	client := f.kube.(*k8sfake.Clientset)
	client.PrependReactor("list", "controllerrevisions", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, nil, errors.New(canary)
	})
	var out bytes.Buffer

	errOut, err := executeHistory(t, f, &out, "service", "--ome-namespace", "control-plane")

	require.NoError(t, err)
	assert.Contains(t, out.String(), "R0/G1")
	assert.NotContains(t, out.String(), canary)
	assert.NotContains(t, errOut, canary)
	assert.Empty(t, errOut)

	var wide bytes.Buffer
	errOut, err = executeHistory(
		t, f, &wide, "service", "--ome-namespace", "control-plane", "--output", "wide",
	)
	require.NoError(t, err)
	assert.Contains(t, wide.String(), "HistoryUnavailable")
	assert.NotContains(t, wide.String(), canary)
	assert.Empty(t, errOut)
}

func TestHistoryPreservesWriterFailures(t *testing.T) {
	writerFailure := errors.New("writer sentinel")
	tests := []struct {
		name   string
		writer io.Writer
		want   error
	}{
		{name: "failure", writer: failingWriter{err: writerFailure}, want: writerFailure},
		{name: "short write", writer: failingWriter{short: true}, want: io.ErrShortWrite},
	}
	for _, test := range tests {
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			t.Run(test.name+"/"+format, func(t *testing.T) {
				f, _ := healthyPinnedFactory(t)
				errOut, err := executeHistory(
					t, f, test.writer, "service", "--ome-namespace", "control-plane", "--output", format,
				)
				require.Error(t, err)
				assert.ErrorIs(t, err, test.want)
				assert.Contains(t, err.Error(), "write runtime history report")
				assert.Empty(t, errOut)
			})
		}
	}
}
