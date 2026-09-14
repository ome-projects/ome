package instance

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
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

var commandClock = reportv1alpha1.ClockFunc(func() time.Time {
	return time.Date(2026, 9, 14, 21, 0, 0, 0, time.UTC)
})

func TestListReadsOnlyExactInferenceServiceAndBoundedRelatedReplicas(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	client := omefake.NewSimpleClientset(isvc, ir)
	deps := listDependencies{
		clock: commandClock,
		limits: paging.Limits{
			PageSize: 2, MaxItems: 9, MaxPages: 5, RequestTimeout: time.Second,
		},
		maxInstances: 100,
	}

	out, err := executeList(t, factory.Static{OME: client, NS: "prod"}, deps, "chat")

	require.NoError(t, err)
	assert.Equal(t,
		"COMP     IDX/INC   PHASE   PODS    REVS          AOF   EVIDENCE\n"+
			"engine   0/1       Ready   1/1/1   chat...ne-a   A--   OK\n",
		out,
	)
	require.Len(t, client.Actions(), 2)
	get := client.Actions()[0].(ktesting.GetAction)
	assert.Equal(t, "prod", get.GetNamespace())
	assert.Equal(t, "chat", get.GetName())
	list := client.Actions()[1]
	assert.Equal(t, "list", list.GetVerb())
	assert.Equal(t, "inferencereplicas", list.GetResource().Resource)
	assert.Equal(t, "prod", list.GetNamespace())
	options := list.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
	assert.Equal(t, constants.InferenceServiceLabel+"=chat", options.LabelSelector)
	assert.Equal(t, int64(2), options.Limit)
}

func TestListWritesTypedJSONAndYAMLFromDirectStatusFields(t *testing.T) {
	t.Parallel()

	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			isvc := commandISVC()
			client := omefake.NewSimpleClientset(isvc, commandIR(isvc))

			out, err := executeList(t, factory.Static{OME: client, NS: "prod"}, commandDependencies(),
				"chat", "--output", format)

			require.NoError(t, err)
			assert.Contains(t, out, "InstanceListReport")
			assert.Contains(t, out, "cli.ome.io/v1alpha1")
			assert.Contains(t, out, "ready")
			assert.NotContains(t, out, "DenseV1")
			assert.NotContains(t, out, "ColumnarV2")
			if format == "json" {
				var decoded map[string]any
				require.NoError(t, json.Unmarshal([]byte(out), &decoded))
				assert.Equal(t, "InstanceListReport", decoded["kind"])
			}
		})
	}
}

func TestListValidatesArgumentsBeforeFactoryAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing name", want: "accepts 1 arg(s), received 0"},
		{name: "extra name", args: []string{"chat", "other"}, want: "accepts 1 arg(s), received 2"},
		{name: "unsupported output", args: []string{"chat", "-o", "wide"}, want: `unsupported output format "wide"`},
		{name: "invalid name", args: []string{"Bad_Name"}, want: ErrInvalidInferenceServiceName.Error()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := executeList(t, panicFactory{}, commandDependencies(), test.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

func TestListReturnsPrimaryAcquisitionAndIdentityErrors(t *testing.T) {
	t.Parallel()

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		client := omefake.NewSimpleClientset()
		_, err := executeList(t, factory.Static{OME: client, NS: "prod"}, commandDependencies(), "chat")
		require.Error(t, err)
		assert.True(t, apierrors.IsNotFound(err))
		assert.Contains(t, err.Error(), `get InferenceService "prod/chat"`)
	})

	tests := []struct {
		name   string
		object *omev1beta1.InferenceService
		kind   error
	}{
		{name: "nil", kind: ErrReturnedInferenceServiceNil},
		{name: "name", object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "other", Namespace: "prod", UID: "uid"}}, kind: ErrReturnedInferenceServiceNameMismatch},
		{name: "namespace", object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "other", UID: "uid"}}, kind: ErrReturnedInferenceServiceNamespaceMismatch},
		{name: "uid", object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod"}}, kind: ErrReturnedInferenceServiceUIDMissing},
		{name: "unsafe uid", object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "uid\u202e\nSECRET"}}, kind: ErrReturnedInferenceServiceUIDInvalid},
		{name: "oversized uid", object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID(strings.Repeat("x", 129))}}, kind: ErrReturnedInferenceServiceUIDInvalid},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				if test.object == nil {
					var nilISVC *omev1beta1.InferenceService
					return true, nilISVC, nil
				}
				return true, test.object.DeepCopy(), nil
			})
			out, err := executeList(t, factory.Static{OME: client, NS: "prod"}, commandDependencies(), "chat")
			require.ErrorIs(t, err, test.kind)
			assert.Empty(t, out)
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestListNeverRendersOpaqueResourceVersionsOrInvalidRevisions(t *testing.T) {
	t.Parallel()

	for _, format := range []string{"table", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			isvc := commandISVC()
			isvc.ResourceVersion = "SECRET_ISVC_RV\u202e\n" + strings.Repeat("x", 10_000)
			ir := commandIR(isvc)
			ir.ResourceVersion = "SECRET_IR_RV\x1b[31m" + strings.Repeat("x", 10_000)
			ir.Status.CurrentRevision = "SECRET_REVISION\u202e" + strings.Repeat("x", 254)
			client := omefake.NewSimpleClientset(isvc, ir)

			out, err := executeList(t, factory.Static{OME: client, NS: "prod"}, commandDependencies(),
				"chat", "--output", format)

			require.NoError(t, err)
			for _, hostile := range []string{"SECRET_ISVC_RV", "SECRET_IR_RV", "SECRET_REVISION"} {
				assert.NotContains(t, out, hostile)
			}
			if format == "table" {
				assert.Contains(t, out, "BAD:REVISION")
			}
		})
	}
}

func TestListSurfacesNestedStatusWorkLimitWithoutScanningRows(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	ir.Status.Replicas = 2
	ir.Status.ReadyReplicas = 0
	ir.Status.ServingReplicas = 0
	ir.Status.AvailableReplicas = 0
	ir.Status.InstanceStatuses = []omev1beta1.OMENativeInstanceStatus{
		{Index: 0, Phase: omev1beta1.OMENativeInstancePhase("SECRET-PHASE")},
		{Index: 1, Phase: omev1beta1.OMENativeInstancePhase("SECRET-PHASE")},
	}
	deps := commandDependencies()
	deps.maxInstances = 1
	client := omefake.NewSimpleClientset(isvc, ir)

	out, err := executeList(t, factory.Static{OME: client, NS: "prod"}, deps,
		"chat", "-o", "json")

	require.NoError(t, err)
	assert.Contains(t, out, `"code": "StatusRowsTruncated"`)
	assert.Contains(t, out, `"state": "Unavailable"`)
	assert.NotContains(t, out, "SECRET-PHASE")
}

func TestListStopsAtNamespaceAndClientFailures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		factory factory.Factory
		want    string
	}{
		{
			name: "namespace resolver", factory: namespaceFactory{Factory: panicFactory{}, err: errors.New("resolver failed")},
			want: "resolve namespace: resolver failed",
		},
		{
			name: "empty namespace", factory: namespaceFactory{Factory: panicFactory{}},
			want: ErrInvalidNamespace.Error(),
		},
		{
			name: "invalid namespace", factory: namespaceFactory{Factory: panicFactory{}, namespace: "Bad_Namespace"},
			want: ErrInvalidNamespace.Error(),
		},
		{
			name: "client", factory: factory.Static{NS: "prod"},
			want: "create OME client: static factory: no OME client configured",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			out, err := executeList(t, test.factory, commandDependencies(), "chat")
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
			assert.Empty(t, out)
		})
	}
}

func TestListRendersRelatedReplicaListFailureAsUnavailableEvidence(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	client := omefake.NewSimpleClientset(isvc)
	forbidden := apierrors.NewForbidden(schema.GroupResource{
		Group: "ome.io", Resource: "inferencereplicas",
	}, "", errors.New("denied"))
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, forbidden
	})

	out, err := executeList(t, factory.Static{OME: client, NS: "prod"}, commandDependencies(),
		"chat", "-o", "json")

	require.NoError(t, err)
	assert.Contains(t, out, `"state": "Unavailable"`)
	assert.Contains(t, out, `"code": "CollectionUnavailable"`)
	assert.Contains(t, out, `"unavailableReason": "Forbidden"`)
	assert.NotContains(t, out, "denied")
}

func TestListPreservesEarlierPagesWhenRelatedReplicaCollectionFails(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	client := omefake.NewSimpleClientset(isvc)
	calls := 0
	client.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		calls++
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		if options.Continue == "" {
			return true, &omev1beta1.InferenceReplicaList{
				ListMeta: metav1.ListMeta{Continue: "next"}, Items: []omev1beta1.InferenceReplica{*ir},
			}, nil
		}
		return true, nil, apierrors.NewForbidden(schema.GroupResource{
			Group: "ome.io", Resource: "inferencereplicas",
		}, "", errors.New("denied"))
	})

	out, err := executeList(t, factory.Static{OME: client, NS: "prod"}, commandDependencies(),
		"chat", "-o", "json")

	require.NoError(t, err)
	assert.Equal(t, 2, calls)
	assert.Contains(t, out, `"state": "Partial"`)
	assert.Contains(t, out, `"inferenceReplica": "chat-engine"`)
	assert.Contains(t, out, `"unavailableReason": "Forbidden"`)
}

func TestListDoesNotSwallowCallerCancellation(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	client := omefake.NewSimpleClientset(isvc)
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.Canceled
	})

	_, err := executeList(t, factory.Static{OME: client, NS: "prod"}, commandDependencies(), "chat")

	assert.ErrorIs(t, err, context.Canceled)
}

func TestListHelpDefinesDirectEvidenceColumnsAndReadBoundary(t *testing.T) {
	t.Parallel()

	cmd := newListCmd(panicFactory{}, genericiooptions.IOStreams{}, commandDependencies())
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--help"})
	require.NoError(t, cmd.Execute())

	help := out.String()
	for _, want := range []string{
		"PODS is serving/available/total",
		"AOF is admitted/operation/last-failure presence",
		"does not read Pods",
		"ReadyPodCount",
		"table, json or yaml",
	} {
		assert.Contains(t, help, want)
	}
}

func TestListReturnsWriterErrors(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	want := errors.New("writer failed")
	cmd := newListCmd(
		factory.Static{OME: omefake.NewSimpleClientset(isvc, commandIR(isvc)), NS: "prod"},
		genericiooptions.IOStreams{Out: errorWriter{err: want}}, commandDependencies(),
	)
	cmd.SetArgs([]string{"chat"})

	err := cmd.Execute()

	require.ErrorIs(t, err, want)
	assert.Contains(t, err.Error(), "write instance list")
}

func executeList(t *testing.T, f factory.Factory, deps listDependencies, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := newListCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}}, deps)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func commandDependencies() listDependencies {
	return listDependencies{
		clock: commandClock,
		limits: paging.Limits{
			PageSize: 50, MaxItems: 100, MaxPages: 10, RequestTimeout: time.Second,
		},
		maxInstances: 1000,
	}
}

func commandISVC() *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 4,
		ResourceVersion: "100",
	}}
}

func commandIR(isvc *omev1beta1.InferenceService) *omev1beta1.InferenceReplica {
	controller := true
	return &omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat-engine", Namespace: isvc.Namespace, UID: types.UID("uid-engine"),
			Generation: 2, ResourceVersion: "200",
			Annotations: map[string]string{
				constants.InferenceReplicaParentGenerationAnnotationKey: "4",
			},
			Labels: map[string]string{
				constants.InferenceServiceLabel: isvc.Name,
				constants.OMEComponentLabel:     string(omev1beta1.EngineComponent),
			},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: isvc.Name, UID: isvc.UID, Controller: &controller,
			}},
		},
		Spec: omev1beta1.InferenceReplicaSpec{
			ParentRef: omev1beta1.ParentReference{Name: isvc.Name}, Component: omev1beta1.EngineComponent,
		},
		Status: omev1beta1.InferenceReplicaStatus{
			ObservedGeneration: 2, Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1, AvailableReplicas: 1,
			CurrentRevision: "chat-engine-a", UpdateRevision: "chat-engine-a",
			InstanceStatuses: []omev1beta1.OMENativeInstanceStatus{{
				Index: 0, Incarnation: 1, Phase: omev1beta1.OMENativeInstanceReady,
				RunningRevision: "chat-engine-a", TargetRevision: "chat-engine-a",
				PodCount: 1, ReadyPodCount: 0, ServingPodCount: 1, AvailablePodCount: 1,
				Admitted: true,
			}},
		},
	}
}

type panicFactory struct{}

func (panicFactory) RESTConfig() (*rest.Config, error)         { panic("factory access") }
func (panicFactory) KubeClient() (kubernetes.Interface, error) { panic("factory access") }
func (panicFactory) OMEClient() (versioned.Interface, error)   { panic("factory access") }
func (panicFactory) RuntimeClient() (ctrlclient.Client, error) { panic("factory access") }
func (panicFactory) Namespace() (string, bool, error)          { panic("factory access") }

type namespaceFactory struct {
	factory.Factory
	namespace string
	err       error
}

func (f namespaceFactory) Namespace() (string, bool, error) { return f.namespace, false, f.err }

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }

var _ io.Writer = errorWriter{}
