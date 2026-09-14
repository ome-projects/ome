package runtime

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/effective"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/runtimeprojection"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

type failingFactory struct {
	ome        versioned.Interface
	kube       kubernetes.Interface
	runtime    ctrlclient.Client
	namespace  string
	nsErr      error
	omeErr     error
	kubeErr    error
	runtimeErr error
}

func (*failingFactory) RESTConfig() (*rest.Config, error)           { panic("unexpected RESTConfig call") }
func (f *failingFactory) KubeClient() (kubernetes.Interface, error) { return f.kube, f.kubeErr }
func (f *failingFactory) OMEClient() (versioned.Interface, error)   { return f.ome, f.omeErr }
func (f *failingFactory) RuntimeClient() (ctrlclient.Client, error) { return f.runtime, f.runtimeErr }
func (f *failingFactory) Namespace() (string, bool, error)          { return f.namespace, false, f.nsErr }

func fixedEffectiveDependencies() effectiveCommandDependencies {
	return effectiveCommandDependencies{
		clock: reportv1alpha1.ClockFunc(func() time.Time {
			return time.Date(2026, 8, 31, 12, 34, 56, 0, time.FixedZone("test", -7*60*60))
		}),
		limits: paging.Limits{
			PageSize: paging.ChunkSize, MaxItems: 1000, MaxPages: 2, RequestTimeout: 10 * time.Second,
		},
		projector: runtimeprojection.ProjectEffective,
	}
}

func boundISVC() *v1beta1.InferenceService {
	return &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "service", Namespace: "team-a", UID: types.UID("isvc-uid"), ResourceVersion: "17",
	}}
}

func completeFactory(t *testing.T) *failingFactory {
	t.Helper()
	return &failingFactory{
		ome: omefake.NewSimpleClientset(boundISVC()), kube: k8sfake.NewSimpleClientset(),
		runtime: ctrlfake.NewClientBuilder().WithScheme(scheme(t)).Build(), namespace: "team-a",
	}
}

func executeEffectiveWithDependencies(
	t *testing.T,
	f factory.Factory,
	dependencies effectiveCommandDependencies,
	out io.Writer,
) (string, error) {
	t.Helper()
	var errOut bytes.Buffer
	cmd := newEffectiveCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: out, ErrOut: &errOut,
	}, dependencies)
	cmd.SetArgs([]string{"service"})
	err := cmd.Execute()
	return errOut.String(), err
}

// TestEffectivePreservesRequiredFailureCauses catches required acquisition
// failures being downgraded, stringified without %w, or followed by output.
func TestEffectivePreservesRequiredFailureCauses(t *testing.T) {
	notFound := apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "service")
	forbidden := apierrors.NewForbidden(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, "service", errors.New("denied"))
	namespaceFailure := errors.New("namespace failure")
	omeClientFailure := errors.New("OME client failure")
	kubeClientFailure := errors.New("Kubernetes client failure")
	runtimeClientFailure := errors.New("runtime client failure")
	tests := []struct {
		name      string
		configure func(*failingFactory)
		want      error
	}{
		{name: "namespace", want: namespaceFailure, configure: func(f *failingFactory) { f.nsErr = namespaceFailure }},
		{name: "OME client", want: omeClientFailure, configure: func(f *failingFactory) { f.omeErr = omeClientFailure }},
		{name: "Kubernetes client", want: kubeClientFailure, configure: func(f *failingFactory) { f.kubeErr = kubeClientFailure }},
		{name: "runtime client", want: runtimeClientFailure, configure: func(f *failingFactory) { f.runtimeErr = runtimeClientFailure }},
		{name: "primary not found", want: notFound, configure: func(f *failingFactory) {
			f.ome = primaryErrorClient(notFound)
		}},
		{name: "primary forbidden", want: forbidden, configure: func(f *failingFactory) {
			f.ome = primaryErrorClient(forbidden)
		}},
		{name: "parent canceled", want: context.Canceled, configure: func(f *failingFactory) {
			f.ome = primaryErrorClient(context.Canceled)
		}},
		{name: "parent deadline", want: context.DeadlineExceeded, configure: func(f *failingFactory) {
			f.ome = primaryErrorClient(context.DeadlineExceeded)
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := completeFactory(t)
			test.configure(f)
			var out bytes.Buffer
			_, err := executeEffectiveWithDependencies(t, f, fixedEffectiveDependencies(), &out)
			require.Error(t, err)
			assert.ErrorIs(t, err, test.want)
			assert.Empty(t, out.String())
		})
	}
}

// TestEffectiveFriendlyCRDNotFound catches loss of the actionable
// apierror.Friendly translation while retaining the Kubernetes status cause.
func TestEffectiveFriendlyCRDNotFound(t *testing.T) {
	cause := &apierrors.StatusError{ErrStatus: metav1.Status{
		Status: metav1.StatusFailure, Reason: metav1.StatusReasonNotFound,
		Message: "the server could not find the requested resource",
	}}
	f := completeFactory(t)
	f.ome = primaryErrorClient(cause)
	var out bytes.Buffer
	_, err := executeEffectiveWithDependencies(t, f, fixedEffectiveDependencies(), &out)
	require.Error(t, err)
	assert.ErrorIs(t, err, cause)
	assert.Contains(t, err.Error(), "OME does not appear to be installed on this cluster")
	assert.Empty(t, out.String())
}

// TestEffectivePreservesParentContextFailure catches collection continuing as
// success after the invoking command has already been canceled or expired.
func TestEffectivePreservesParentContextFailure(t *testing.T) {
	tests := []struct {
		name    string
		context func() (context.Context, context.CancelFunc)
		want    error
	}{
		{name: "canceled", want: context.Canceled, context: func() (context.Context, context.CancelFunc) {
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			return ctx, func() {}
		}},
		{name: "deadline", want: context.DeadlineExceeded, context: func() (context.Context, context.CancelFunc) {
			return context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := test.context()
			defer cancel()
			var out, errOut bytes.Buffer
			cmd := newEffectiveCmdWithDependencies(completeFactory(t), genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut,
			}, fixedEffectiveDependencies())
			cmd.SetContext(ctx)
			cmd.SetArgs([]string{"service"})
			err := cmd.Execute()
			require.Error(t, err)
			assert.ErrorIs(t, err, test.want)
			assert.Empty(t, out.String())
		})
	}
}

func primaryErrorClient(cause error) versioned.Interface {
	client := omefake.NewSimpleClientset()
	client.PrependReactor("get", "inferenceservices", func(k8stesting.Action) (bool, k8sruntime.Object, error) {
		return true, nil, cause
	})
	return client
}

// TestEffectivePreservesProjectorFailure catches projection errors being
// swallowed or rendered as a successful empty report.
func TestEffectivePreservesProjectorFailure(t *testing.T) {
	tests := []error{errors.New("projector sentinel"), runtimeprojection.ErrInvalidEvidence}
	for _, sentinel := range tests {
		t.Run(sentinel.Error(), func(t *testing.T) {
			dependencies := fixedEffectiveDependencies()
			dependencies.projector = func(
				*v1beta1.InferenceService,
				*effective.RuntimeState,
				reportv1alpha1.Clock,
			) (reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent], error) {
				return reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent]{}, sentinel
			}
			var out bytes.Buffer
			_, err := executeEffectiveWithDependencies(t, completeFactory(t), dependencies, &out)
			require.Error(t, err)
			assert.ErrorIs(t, err, sentinel)
			assert.Empty(t, out.String())
		})
	}
}

// TestEffectivePreservesResolverConstructionFailure catches invalid fixed
// collection limits being ignored or converted into a successful report.
func TestEffectivePreservesResolverConstructionFailure(t *testing.T) {
	dependencies := fixedEffectiveDependencies()
	dependencies.limits.PageSize = 0
	var out bytes.Buffer
	_, err := executeEffectiveWithDependencies(t, completeFactory(t), dependencies, &out)
	require.Error(t, err)
	assert.EqualError(t, err, "construct runtime evidence resolver: revision paging limits are invalid")
	cause := errors.Unwrap(err)
	require.Error(t, cause, "resolver-construction wrapping must retain a nonnil cause")
	assert.EqualError(t, cause, "revision paging limits are invalid")
	assert.NoError(t, errors.Unwrap(cause), "the exact lower-level constructor error is the terminal cause")
	assert.Empty(t, out.String())
}

// TestEffectiveProjectsExactlyOnce catches duplicate projection or rendering
// directly from internal evidence instead of the typed projected report.
func TestEffectiveProjectsExactlyOnce(t *testing.T) {
	dependencies := fixedEffectiveDependencies()
	calls := 0
	dependencies.projector = func(
		isvc *v1beta1.InferenceService,
		state *effective.RuntimeState,
		clock reportv1alpha1.Clock,
	) (reportv1alpha1.RuntimeEnvelope[reportv1alpha1.RuntimeEffectiveContent], error) {
		calls++
		return runtimeprojection.ProjectEffective(isvc, state, clock)
	}
	var out bytes.Buffer
	_, err := executeEffectiveWithDependencies(t, completeFactory(t), dependencies, &out)
	require.NoError(t, err)
	assert.Equal(t, 1, calls)
	assert.NotEmpty(t, out.String())
}

type failingWriter struct {
	short bool
	err   error
}

func (w failingWriter) Write(p []byte) (int, error) {
	if w.short {
		return len(p) - 1, nil
	}
	return 0, w.err
}

// TestEffectivePreservesWriterFailures catches successful exit on destination
// errors, including io.Writer's short-write contract.
func TestEffectivePreservesWriterFailures(t *testing.T) {
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
				var errOut bytes.Buffer
				cmd := newEffectiveCmdWithDependencies(completeFactory(t), genericiooptions.IOStreams{
					In: &bytes.Buffer{}, Out: test.writer, ErrOut: &errOut,
				}, fixedEffectiveDependencies())
				cmd.SetArgs([]string{"service", "--output", format})
				err := cmd.Execute()
				require.Error(t, err)
				assert.ErrorIs(t, err, test.want)
			})
		}
	}
}

// TestEffectivePreservesMachineFormatMarshalFailures catches JSON/YAML
// serialization failures being swallowed, flattened, or partially written.
func TestEffectivePreservesMachineFormatMarshalFailures(t *testing.T) {
	dependencies := fixedEffectiveDependencies()
	dependencies.clock = reportv1alpha1.ClockFunc(func() time.Time {
		return time.Date(10000, 1, 2, 3, 4, 5, 0, time.UTC)
	})
	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var out, errOut bytes.Buffer
			cmd := newEffectiveCmdWithDependencies(completeFactory(t), genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut,
			}, dependencies)
			cmd.SetArgs([]string{"service", "--output", format})

			err := cmd.Execute()

			require.Error(t, err)
			assert.Contains(t, err.Error(), "marshal report "+format)
			assert.Contains(t, err.Error(), "year outside of range [0,9999]")
			var marshalerError *json.MarshalerError
			assert.ErrorAs(t, err, &marshalerError)
			assert.Empty(t, out.String())
			assert.Empty(t, errOut.String())
		})
	}
}
