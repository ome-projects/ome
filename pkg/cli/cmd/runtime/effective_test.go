package runtime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	k8stesting "k8s.io/client-go/testing"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	ctrlfake "sigs.k8s.io/controller-runtime/pkg/client/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	ometypedv1beta1 "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

type primaryGetContextObservation struct {
	deadline    time.Time
	hasDeadline bool
	observedAt  time.Time
}

type deadlineRecordingOMEClient struct {
	versioned.Interface
	primaryGETs []primaryGetContextObservation
}

func (c *deadlineRecordingOMEClient) OmeV1beta1() ometypedv1beta1.OmeV1beta1Interface {
	return &deadlineRecordingOmeV1beta1{
		OmeV1beta1Interface: c.Interface.OmeV1beta1(),
		parent:              c,
	}
}

type deadlineRecordingOmeV1beta1 struct {
	ometypedv1beta1.OmeV1beta1Interface
	parent *deadlineRecordingOMEClient
}

func (c *deadlineRecordingOmeV1beta1) InferenceServices(namespace string) ometypedv1beta1.InferenceServiceInterface {
	return &deadlineRecordingInferenceServices{
		InferenceServiceInterface: c.OmeV1beta1Interface.InferenceServices(namespace),
		parent:                    c.parent,
	}
}

type deadlineRecordingInferenceServices struct {
	ometypedv1beta1.InferenceServiceInterface
	parent *deadlineRecordingOMEClient
}

func (c *deadlineRecordingInferenceServices) Get(
	ctx context.Context,
	name string,
	options metav1.GetOptions,
) (*v1beta1.InferenceService, error) {
	observedAt := time.Now()
	deadline, hasDeadline := ctx.Deadline()
	c.parent.primaryGETs = append(c.parent.primaryGETs, primaryGetContextObservation{
		deadline: deadline, hasDeadline: hasDeadline, observedAt: observedAt,
	})
	return c.InferenceServiceInterface.Get(ctx, name, options)
}

type runtimeClientOperation struct {
	verb       string
	objectType string
	key        ctrlclient.ObjectKey
}

type recordingRuntimeClient struct {
	ctrlclient.Client
	operations []runtimeClientOperation
}

func (c *recordingRuntimeClient) record(verb string, object any, key ctrlclient.ObjectKey) {
	c.operations = append(c.operations, runtimeClientOperation{
		verb: verb, objectType: fmt.Sprintf("%T", object), key: key,
	})
}

func (c *recordingRuntimeClient) Get(
	ctx context.Context,
	key ctrlclient.ObjectKey,
	object ctrlclient.Object,
	options ...ctrlclient.GetOption,
) error {
	c.record("get", object, key)
	return c.Client.Get(ctx, key, object, options...)
}

func (c *recordingRuntimeClient) List(
	ctx context.Context,
	list ctrlclient.ObjectList,
	options ...ctrlclient.ListOption,
) error {
	c.record("list", list, ctrlclient.ObjectKey{})
	return c.Client.List(ctx, list, options...)
}

func (c *recordingRuntimeClient) Apply(
	ctx context.Context,
	configuration k8sruntime.ApplyConfiguration,
	options ...ctrlclient.ApplyOption,
) error {
	c.record("apply", configuration, ctrlclient.ObjectKey{})
	return c.Client.Apply(ctx, configuration, options...)
}

func (c *recordingRuntimeClient) Create(
	ctx context.Context,
	object ctrlclient.Object,
	options ...ctrlclient.CreateOption,
) error {
	c.record("create", object, ctrlclient.ObjectKeyFromObject(object))
	return c.Client.Create(ctx, object, options...)
}

func (c *recordingRuntimeClient) Delete(
	ctx context.Context,
	object ctrlclient.Object,
	options ...ctrlclient.DeleteOption,
) error {
	c.record("delete", object, ctrlclient.ObjectKeyFromObject(object))
	return c.Client.Delete(ctx, object, options...)
}

func (c *recordingRuntimeClient) Update(
	ctx context.Context,
	object ctrlclient.Object,
	options ...ctrlclient.UpdateOption,
) error {
	c.record("update", object, ctrlclient.ObjectKeyFromObject(object))
	return c.Client.Update(ctx, object, options...)
}

func (c *recordingRuntimeClient) Patch(
	ctx context.Context,
	object ctrlclient.Object,
	patch ctrlclient.Patch,
	options ...ctrlclient.PatchOption,
) error {
	c.record("patch", object, ctrlclient.ObjectKeyFromObject(object))
	return c.Client.Patch(ctx, object, patch, options...)
}

func (c *recordingRuntimeClient) DeleteAllOf(
	ctx context.Context,
	object ctrlclient.Object,
	options ...ctrlclient.DeleteAllOfOption,
) error {
	c.record("delete-all-of", object, ctrlclient.ObjectKey{})
	return c.Client.DeleteAllOf(ctx, object, options...)
}

func (c *recordingRuntimeClient) Status() ctrlclient.SubResourceWriter {
	return &recordingSubResourceWriter{
		parent: c, name: "status", delegate: c.Client.Status(),
	}
}

func (c *recordingRuntimeClient) SubResource(name string) ctrlclient.SubResourceClient {
	return &recordingSubResourceClient{
		parent: c, name: name, delegate: c.Client.SubResource(name),
	}
}

type recordingSubResourceWriter struct {
	parent   *recordingRuntimeClient
	name     string
	delegate ctrlclient.SubResourceWriter
}

func (c *recordingSubResourceWriter) Create(
	ctx context.Context,
	object ctrlclient.Object,
	subResource ctrlclient.Object,
	options ...ctrlclient.SubResourceCreateOption,
) error {
	c.parent.record(c.name+"-create", object, ctrlclient.ObjectKeyFromObject(object))
	return c.delegate.Create(ctx, object, subResource, options...)
}

func (c *recordingSubResourceWriter) Update(
	ctx context.Context,
	object ctrlclient.Object,
	options ...ctrlclient.SubResourceUpdateOption,
) error {
	c.parent.record(c.name+"-update", object, ctrlclient.ObjectKeyFromObject(object))
	return c.delegate.Update(ctx, object, options...)
}

func (c *recordingSubResourceWriter) Patch(
	ctx context.Context,
	object ctrlclient.Object,
	patch ctrlclient.Patch,
	options ...ctrlclient.SubResourcePatchOption,
) error {
	c.parent.record(c.name+"-patch", object, ctrlclient.ObjectKeyFromObject(object))
	return c.delegate.Patch(ctx, object, patch, options...)
}

func (c *recordingSubResourceWriter) Apply(context.Context, k8sruntime.ApplyConfiguration, ...ctrlclient.SubResourceApplyOption) error {
	return errors.New("apply is not supported by this test client")
}

type recordingSubResourceClient struct {
	parent   *recordingRuntimeClient
	name     string
	delegate ctrlclient.SubResourceClient
}

func (c *recordingSubResourceClient) Get(
	ctx context.Context,
	object ctrlclient.Object,
	subResource ctrlclient.Object,
	options ...ctrlclient.SubResourceGetOption,
) error {
	c.parent.record(c.name+"-get", object, ctrlclient.ObjectKeyFromObject(object))
	return c.delegate.Get(ctx, object, subResource, options...)
}

func (c *recordingSubResourceClient) Create(
	ctx context.Context,
	object ctrlclient.Object,
	subResource ctrlclient.Object,
	options ...ctrlclient.SubResourceCreateOption,
) error {
	c.parent.record(c.name+"-create", object, ctrlclient.ObjectKeyFromObject(object))
	return c.delegate.Create(ctx, object, subResource, options...)
}

func (c *recordingSubResourceClient) Update(
	ctx context.Context,
	object ctrlclient.Object,
	options ...ctrlclient.SubResourceUpdateOption,
) error {
	c.parent.record(c.name+"-update", object, ctrlclient.ObjectKeyFromObject(object))
	return c.delegate.Update(ctx, object, options...)
}

func (c *recordingSubResourceClient) Patch(
	ctx context.Context,
	object ctrlclient.Object,
	patch ctrlclient.Patch,
	options ...ctrlclient.SubResourcePatchOption,
) error {
	c.parent.record(c.name+"-patch", object, ctrlclient.ObjectKeyFromObject(object))
	return c.delegate.Patch(ctx, object, patch, options...)
}

func (c *recordingSubResourceClient) Apply(context.Context, k8sruntime.ApplyConfiguration, ...ctrlclient.SubResourceApplyOption) error {
	return errors.New("apply is not supported by this test client")
}

// TestEffectiveCommandContract catches removing the effective command, changing
// its operator-facing contract, or accidentally adding unsupported local flags.
func TestEffectiveCommandContract(t *testing.T) {
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}}
	parent := NewCmd(factory.Static{}, streams)

	assert.Equal(t, "Inspect OME runtime selection and configuration", parent.Short)
	cmd, _, err := parent.Find([]string{"effective"})
	require.NoError(t, err)
	require.NotSame(t, parent, cmd)
	assert.Equal(t, "effective INFERENCESERVICE", cmd.Use)
	assert.Equal(t, "Show effective runtime evidence for an InferenceService", cmd.Short)
	assert.Equal(t, `Shows allowlisted runtime selection, inheritance, pin, status, drift,
and live-versus-controller-active evidence for an InferenceService.

Current only means status.observedGeneration == metadata.generation in the
fetched snapshot, never wall-clock freshness or rollout convergence. Raw
runtime specs, ControllerRevision data, status messages, resource versions,
and synchronization tokens are never printed.

The compact table uses SR for ServingRuntime and CSR for ClusterServingRuntime,
and renders components as MODE (SOURCE). Use -o wide for the complete legacy
table.`, cmd.Long)
	assert.Contains(t, cmd.Long, "status.observedGeneration == metadata.generation")
	assert.Contains(t, cmd.Long, "never wall-clock freshness or rollout convergence")
	assert.Contains(t, cmd.Long, "Raw\nruntime specs")
	assert.Contains(t, cmd.Long, "ControllerRevision data")
	assert.Contains(t, cmd.Long, "synchronization tokens are never printed")

	output := cmd.Flags().Lookup("output")
	require.NotNil(t, output)
	assert.Equal(t, "o", output.Shorthand)
	assert.Equal(t, "table", output.DefValue)
	assert.Equal(t, "Output format: table, wide, json or yaml", output.Usage)
	omeNamespace := cmd.Flags().Lookup("ome-namespace")
	require.NotNil(t, omeNamespace)
	assert.Equal(t, "ome", omeNamespace.DefValue)
	assert.Equal(t, "Namespace where the OME control plane is installed", omeNamespace.Usage)
	assert.Nil(t, cmd.Flags().Lookup("namespace"), "namespace must remain inherited from the root")

	wantLocalFlags := map[string]bool{"ome-namespace": true, "output": true}
	cmd.LocalNonPersistentFlags().VisitAll(func(flag *pflag.Flag) {
		assert.Truef(t, wantLocalFlags[flag.Name], "unexpected local flag %q", flag.Name)
		delete(wantLocalFlags, flag.Name)
	})
	assert.Empty(t, wantLocalFlags)
}

// TestEffectiveHelpOutput catches user-visible help drift that field-level
// metadata assertions cannot see, including flag ordering and defaults.
func TestEffectiveHelpOutput(t *testing.T) {
	var out bytes.Buffer
	parent := NewCmd(factory.Static{}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &out, ErrOut: &out,
	})
	parent.SetOut(&out)
	parent.SetErr(&out)
	parent.SetArgs([]string{"effective", "--help"})
	require.NoError(t, parent.Execute())
	assert.Equal(t, `Shows allowlisted runtime selection, inheritance, pin, status, drift,
and live-versus-controller-active evidence for an InferenceService.

Current only means status.observedGeneration == metadata.generation in the
fetched snapshot, never wall-clock freshness or rollout convergence. Raw
runtime specs, ControllerRevision data, status messages, resource versions,
and synchronization tokens are never printed.

The compact table uses SR for ServingRuntime and CSR for ClusterServingRuntime,
and renders components as MODE (SOURCE). Use -o wide for the complete legacy
table.

Usage:
  runtime effective INFERENCESERVICE [flags]

Flags:
  -h, --help                   help for effective
      --ome-namespace string   Namespace where the OME control plane is installed (default "ome")
  -o, --output string          Output format: table, wide, json or yaml (default "table")
`, out.String())
}

// TestEffectiveUsesOnlyBoundedReadRequests catches accidental history LISTs,
// duplicate exact revision GETs, namespace cross-wiring, write verbs, or an
// unbounded/excessive primary request timeout.
func TestEffectiveUsesOnlyBoundedReadRequests(t *testing.T) {
	tests := []struct {
		name             string
		reportedRevision string
		wantRevisionGETs []string
	}{
		{name: "distinct revisions", reportedRevision: "reported-revision", wantRevisionGETs: []string{"requested-revision", "reported-revision"}},
		{name: "deduplicated revision", reportedRevision: "requested-revision", wantRevisionGETs: []string{"requested-revision"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			autoSync := false
			kind := runtimeselector.KindClusterServingRuntime
			requestedRevision := "requested-revision"
			isvc := &v1beta1.InferenceService{
				ObjectMeta: metav1.ObjectMeta{
					Name: "service", Namespace: "team-a", UID: types.UID("isvc-uid"),
					ResourceVersion: "17", Generation: 4,
				},
				Spec: v1beta1.InferenceServiceSpec{
					Runtime: &v1beta1.ServingRuntimeRef{
						Name: "cluster-runtime", Kind: &kind, AutoSync: &autoSync, Revision: &requestedRevision,
					},
					Engine: &v1beta1.EngineSpec{},
				},
			}
			isvc.Status.ObservedGeneration = 4
			isvc.Status.PinnedRevisionName = test.reportedRevision
			runtimeObject := &v1beta1.ClusterServingRuntime{
				ObjectMeta: metav1.ObjectMeta{
					Name: "cluster-runtime", UID: types.UID("runtime-uid"), ResourceVersion: "23", Generation: 2,
				},
				Spec: v1beta1.ServingRuntimeSpec{EngineConfig: &v1beta1.EngineSpec{
					Runner: &v1beta1.RunnerSpec{Container: corev1.Container{Name: "runner", Image: "private.example/runtime:secret"}},
				}},
			}
			omeFake := omefake.NewSimpleClientset(isvc)
			ome := &deadlineRecordingOMEClient{Interface: omeFake}
			kube := k8sfake.NewSimpleClientset()
			runtimeClient := &recordingRuntimeClient{Client: ctrlfake.NewClientBuilder().
				WithScheme(scheme(t)).WithObjects(runtimeObject).Build()}
			f := &acquisitionFactory{
				ome: ome, kube: kube,
				runtime:   runtimeClient,
				namespace: "team-a",
			}
			var out, errOut bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
			cmd.SetArgs([]string{"effective", "service", "--ome-namespace", "control-plane"})

			err := cmd.Execute()

			require.NoError(t, err)
			require.Len(t, omeFake.Actions(), 1)
			assert.Equal(t, "get", omeFake.Actions()[0].GetVerb())
			assert.Equal(t, "inferenceservices", omeFake.Actions()[0].GetResource().Resource)
			assert.Equal(t, "team-a", omeFake.Actions()[0].GetNamespace())
			primaryGET, ok := omeFake.Actions()[0].(k8stesting.GetAction)
			require.True(t, ok)
			assert.Equal(t, "service", primaryGET.GetName())
			require.Len(t, ome.primaryGETs, 1)
			primaryContext := ome.primaryGETs[0]
			require.True(t, primaryContext.hasDeadline, "primary GET must carry a deadline")
			assert.WithinDuration(t,
				primaryContext.observedAt.Add(10*time.Second), primaryContext.deadline, 100*time.Millisecond,
				"primary GET deadline must use the production 10-second request limit",
			)

			var revisionGETs []string
			for _, action := range kube.Actions() {
				assert.Equal(t, "get", action.GetVerb(), "effective must not list revision history")
				assert.Equal(t, "controllerrevisions", action.GetResource().Resource)
				assert.Equal(t, "control-plane", action.GetNamespace())
				get, ok := action.(k8stesting.GetAction)
				require.True(t, ok)
				revisionGETs = append(revisionGETs, get.GetName())
			}
			assert.Equal(t, test.wantRevisionGETs, revisionGETs)
			assert.Equal(t, []runtimeClientOperation{
				{
					verb: "get", objectType: "*v1beta1.ClusterServingRuntime",
					key: ctrlclient.ObjectKey{Name: "cluster-runtime"},
				},
				{
					verb: "get", objectType: "*v1beta1.ClusterServingRuntime",
					key: ctrlclient.ObjectKey{Name: "cluster-runtime"},
				},
				{
					verb: "get", objectType: "*v1beta1.ClusterServingRuntime",
					key: ctrlclient.ObjectKey{Name: "cluster-runtime"},
				},
			}, runtimeClient.operations)
			assert.NotContains(t, out.String(), "private.example/runtime:secret")
			assert.Contains(t, out.String(), "RevisionMissing")
			assert.Empty(t, errOut.String())
		})
	}
}

// TestEffectiveRejectsUnboundPrimarySnapshots catches trusting a nil or
// identity-incomplete server response and constructing later clients from it.
func TestEffectiveRejectsUnboundPrimarySnapshots(t *testing.T) {
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
		{name: "wrong name", got: func() *v1beta1.InferenceService { v := valid(); v.Name = "hostile-returned-name"; return v }, want: errInferenceServiceNameMismatch},
		{name: "wrong namespace", got: func() *v1beta1.InferenceService { v := valid(); v.Namespace = "hostile-returned-namespace"; return v }, want: errInferenceServiceNamespaceMismatch},
		{name: "empty UID", got: func() *v1beta1.InferenceService { v := valid(); v.UID = ""; return v }, want: errInferenceServiceUIDEmpty},
		{name: "empty resource version", got: func() *v1beta1.InferenceService { v := valid(); v.ResourceVersion = ""; return v }, want: errInferenceServiceVersionEmpty},
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
			var out, errOut bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
			cmd.SetArgs([]string{"effective", "service"})

			err := cmd.Execute()

			require.Error(t, err)
			assert.ErrorIs(t, err, test.want)
			assert.NotContains(t, err.Error(), "hostile-returned")
			assert.Zero(t, f.kubeGet)
			assert.Zero(t, f.runtimeGet)
			assert.Empty(t, out.String())
			assert.Empty(t, errOut.String())
		})
	}
}

type acquisitionFactory struct {
	ome          versioned.Interface
	kube         kubernetes.Interface
	runtime      ctrlclient.Client
	namespace    string
	namespaceGet int
	omeGet       int
	kubeGet      int
	runtimeGet   int
}

func (*acquisitionFactory) RESTConfig() (*rest.Config, error) { panic("unexpected RESTConfig call") }
func (f *acquisitionFactory) KubeClient() (kubernetes.Interface, error) {
	f.kubeGet++
	return f.kube, nil
}
func (f *acquisitionFactory) OMEClient() (versioned.Interface, error) {
	f.omeGet++
	return f.ome, nil
}
func (f *acquisitionFactory) RuntimeClient() (ctrlclient.Client, error) {
	f.runtimeGet++
	return f.runtime, nil
}
func (f *acquisitionFactory) Namespace() (string, bool, error) {
	f.namespaceGet++
	return f.namespace, false, nil
}

// TestEffectiveAcquiresBoundSnapshotBeforeOptionalEvidence catches namespace
// cross-wiring, duplicate primary GETs, and construction of later clients
// before the primary snapshot has been acquired and bound.
func TestEffectiveAcquiresBoundSnapshotBeforeOptionalEvidence(t *testing.T) {
	isvc := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "service", Namespace: "team-a", UID: types.UID("isvc-uid"), ResourceVersion: "17",
	}}
	ome := omefake.NewSimpleClientset(isvc)
	kube := k8sfake.NewSimpleClientset()
	f := &acquisitionFactory{
		ome:       ome,
		kube:      kube,
		runtime:   ctrlfake.NewClientBuilder().WithScheme(scheme(t)).Build(),
		namespace: "team-a",
	}
	var out, errOut bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
	cmd.SetArgs([]string{"effective", "service", "--ome-namespace", "control-plane"})

	err := cmd.Execute()

	require.NoError(t, err)
	assert.Equal(t, 1, f.namespaceGet)
	assert.Equal(t, 1, f.omeGet)
	assert.Equal(t, 1, f.kubeGet)
	assert.Equal(t, 1, f.runtimeGet)
	require.Len(t, ome.Actions(), 1)
	assert.Equal(t, "get", ome.Actions()[0].GetVerb())
	assert.Equal(t, "inferenceservices", ome.Actions()[0].GetResource().Resource)
	assert.Equal(t, "team-a", ome.Actions()[0].GetNamespace())
	primaryGET, ok := ome.Actions()[0].(k8stesting.GetAction)
	require.True(t, ok)
	assert.Equal(t, "service", primaryGET.GetName())
	assert.Empty(t, kube.Actions(), "no revision is named and history is disabled")
	assert.Contains(t, out.String(), "SCOPE")
	assert.Contains(t, out.String(), "NotConfigured")
	assert.Empty(t, errOut.String())
}

type validationFactory struct {
	namespaceCalls int
	namespace      string
}

func (*validationFactory) RESTConfig() (*rest.Config, error) { panic("unexpected RESTConfig call") }
func (*validationFactory) KubeClient() (kubernetes.Interface, error) {
	panic("unexpected KubeClient call")
}
func (*validationFactory) OMEClient() (versioned.Interface, error) {
	panic("unexpected OMEClient call")
}
func (*validationFactory) RuntimeClient() (ctrlclient.Client, error) {
	panic("unexpected RuntimeClient call")
}
func (f *validationFactory) Namespace() (string, bool, error) {
	f.namespaceCalls++
	return f.namespace, false, nil
}

// TestEffectiveValidationPrecedesAcquisition catches validation moving after
// namespace resolution or client construction, which could issue API calls for
// an invocation that is already known to be invalid.
func TestEffectiveValidationPrecedesAcquisition(t *testing.T) {
	tests := []struct {
		name               string
		args               []string
		workloadNamespace  string
		wantError          string
		wantNamespaceCalls int
	}{
		{name: "zero arguments", wantError: "accepts 1 arg(s)"},
		{name: "two arguments", args: []string{"one", "two"}, wantError: "accepts 1 arg(s)"},
		{name: "invalid service name", args: []string{"Bad_Name"}, wantError: "InferenceService name", wantNamespaceCalls: 0},
		{name: "invalid output", args: []string{"service", "--output", "xml"}, wantError: "unsupported output format", wantNamespaceCalls: 0},
		{name: "invalid output before invalid service name", args: []string{"Bad_Name", "--output", "xml"}, wantError: "unsupported output format", wantNamespaceCalls: 0},
		{name: "invalid workload namespace", args: []string{"service"}, workloadNamespace: "Bad_NS", wantError: "workload namespace", wantNamespaceCalls: 1},
		{name: "invalid OME namespace", args: []string{"service", "--ome-namespace", "Bad_NS"}, workloadNamespace: "team-a", wantError: "OME namespace", wantNamespaceCalls: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := &validationFactory{namespace: test.workloadNamespace}
			var out, errOut bytes.Buffer
			cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &errOut})
			cmd.SetArgs(append([]string{"effective"}, test.args...))

			err := cmd.Execute()

			require.Error(t, err)
			assert.Truef(t, strings.Contains(err.Error(), test.wantError), "error %q does not contain %q", err, test.wantError)
			assert.Equal(t, test.wantNamespaceCalls, f.namespaceCalls)
			assert.Empty(t, out.String())
			assert.Empty(t, errOut.String())
		})
	}
}
