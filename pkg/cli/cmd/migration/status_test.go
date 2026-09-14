package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ktesting "k8s.io/client-go/testing"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/migrationcollection"
	"sigs.k8s.io/ome/pkg/cli/migrationprojection"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

var commandNow = time.Date(2026, time.September, 14, 19, 0, 0, 0, time.UTC)

func TestStatusProductionWiringReadsOnlyParentAndRelatedReplicas(t *testing.T) {
	t.Parallel()

	parent := commandISVC()
	ir := commandIR(parent, omev1beta1.EngineComponent)
	ir.Status.Migrations = []omev1beta1.MigrationStatus{{
		RequestUUID: "12345678-1234-1234-1234-123456789abc", Trigger: omev1beta1.MigrationTriggerManual,
		SourceInstance: 1, Phase: omev1beta1.MigrationPhaseAccepted, Message: "waiting for target capacity to become available",
		StartedAt: metav1.NewTime(commandNow.Add(-time.Minute)), Deadline: metav1.NewTime(commandNow.Add(time.Hour)),
	}}
	client := omefake.NewSimpleClientset(parent, &ir)
	deps := defaultStatusDependencies()
	deps.clock = commandClock{commandNow}

	out, err := executeStatus(t, factory.Static{OME: client, NS: "prod"}, deps, "chat")

	require.NoError(t, err)
	assert.Equal(t,
		"SUBJECT/COMP      STATUS                    DETAIL\n"+
			"12345678/engine   Accepted/Active/Current   MSG: waiting for target...\n",
		out,
	)
	actions := client.Actions()
	require.Len(t, actions, 2)
	get := actions[0].(ktesting.GetAction)
	assert.Equal(t, "inferenceservices", get.GetResource().Resource)
	assert.Equal(t, "prod", get.GetNamespace())
	assert.Equal(t, "chat", get.GetName())
	list := actions[1].(ktesting.ListAction)
	assert.Equal(t, "inferencereplicas", list.GetResource().Resource)
	assert.Equal(t, "prod", list.GetNamespace())
	value, found := list.GetListRestrictions().Labels.RequiresExactMatch(constants.InferenceServicePodLabelKey)
	assert.True(t, found)
	assert.Equal(t, "chat", value)
}

func TestStatusHelpDocumentsBoundedMessageRendering(t *testing.T) {
	t.Parallel()

	command := newStatusCmd(panicFactory{}, genericiooptions.IOStreams{})

	assert.Contains(t, command.Long, "256 display columns")
	assert.Contains(t, command.Long, "80 columns")
}

func TestStatusComponentFilterAndMachineFormatsUseTypedContract(t *testing.T) {
	t.Parallel()

	parent := commandISVC()
	engine := commandIR(parent, omev1beta1.EngineComponent)
	engine.Status.Migrations = []omev1beta1.MigrationStatus{{
		RequestUUID: "engine-record", Trigger: omev1beta1.MigrationTriggerManual,
		SourceInstance: 1, Phase: omev1beta1.MigrationPhaseAccepted,
		StartedAt: metav1.NewTime(commandNow.Add(-time.Minute)), Deadline: metav1.NewTime(commandNow.Add(time.Hour)),
	}}
	router := commandIR(parent, omev1beta1.RouterComponent)
	router.Status.Migrations = []omev1beta1.MigrationStatus{{
		RequestUUID: "router-record", Trigger: omev1beta1.MigrationTriggerManual,
		SourceInstance: 2, Phase: omev1beta1.MigrationPhaseAccepted,
		StartedAt: metav1.NewTime(commandNow.Add(-time.Minute)), Deadline: metav1.NewTime(commandNow.Add(time.Hour)),
	}}
	client := omefake.NewSimpleClientset(parent, &engine, &router)
	deps := defaultStatusDependencies()
	deps.clock = commandClock{commandNow}

	jsonOutput, err := executeStatus(t, factory.Static{OME: client, NS: "prod"}, deps, "chat", "--component", "engine", "-o", "json")
	require.NoError(t, err)
	var typed reportv1alpha1.MigrationStatusReport
	require.NoError(t, json.Unmarshal([]byte(jsonOutput), &typed))
	assert.Equal(t, reportv1alpha1.APIVersion, typed.APIVersion)
	assert.Equal(t, reportv1alpha1.MigrationStatusReportKind, typed.Kind)
	require.Len(t, typed.Content.Migrations, 1)
	assert.Equal(t, "engine-record", typed.Content.Migrations[0].RequestID)
	assert.NotContains(t, jsonOutput, "router-record")

	yamlOutput, err := executeStatus(t, factory.Static{OME: omefake.NewSimpleClientset(parent, &engine, &router), NS: "prod"}, deps, "chat", "--component=engine", "--output=yaml")
	require.NoError(t, err)
	assert.Contains(t, yamlOutput, "apiVersion: cli.ome.io/v1alpha1\n")
	assert.Contains(t, yamlOutput, "kind: MigrationStatusReport\n")
	assert.Contains(t, yamlOutput, "requestID: engine-record\n")
	assert.NotContains(t, yamlOutput, "router-record")
}

func TestStatusValidatesArgumentsBeforeFactoryAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want error
	}{
		{name: "missing name", want: nil},
		{name: "extra name", args: []string{"chat", "other"}, want: nil},
		{name: "invalid name", args: []string{"Bad_Name"}, want: ErrInvalidInferenceServiceName},
		{name: "invalid component", args: []string{"chat", "--component", "predictor"}, want: ErrInvalidComponent},
		{name: "noncanonical component", args: []string{"chat", "--component", "Engine"}, want: ErrInvalidComponent},
		{name: "unsupported output", args: []string{"chat", "--output", "wide"}, want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := executeStatus(t, panicFactory{}, defaultStatusDependencies(), test.args...)
			require.Error(t, err)
			if test.want != nil {
				assert.ErrorIs(t, err, test.want)
			}
		})
	}
}

func TestStatusValidatesResolvedNamespaceBeforeClientAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		namespace string
		err       error
		want      error
	}{
		{name: "resolver", err: errors.New("resolver failed")},
		{name: "empty", want: ErrInvalidNamespace},
		{name: "invalid", namespace: "Bad_Namespace", want: ErrInvalidNamespace},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := executeStatus(t, namespaceFactory{namespace: test.namespace, err: test.err}, defaultStatusDependencies(), "chat")
			require.Error(t, err)
			if test.want != nil {
				assert.ErrorIs(t, err, test.want)
			}
			if test.err != nil {
				assert.ErrorIs(t, err, test.err)
			}
		})
	}
}

func TestStatusPropagatesCollectionProjectionAndWriterErrors(t *testing.T) {
	t.Parallel()

	parent := commandISVC()
	baseFactory := factory.Static{OME: omefake.NewSimpleClientset(parent), NS: "prod"}
	t.Run("collection", func(t *testing.T) {
		boom := errors.New("collection failed")
		deps := defaultStatusDependencies()
		deps.collect = func(context.Context, collectionClient, string, string, collectionLimits) (migrationcollection.Result, error) {
			return migrationcollection.Result{}, boom
		}
		deps.project = func(migrationcollection.Result, string, migrationprojection.Limits, reportv1alpha1.Clock) (reportv1alpha1.MigrationStatusReport, error) {
			panic("project must not run")
		}

		out, err := executeStatus(t, baseFactory, deps, "chat")
		require.ErrorIs(t, err, boom)
		assert.Empty(t, out)
	})

	t.Run("projection", func(t *testing.T) {
		boom := errors.New("projection failed")
		deps := defaultStatusDependencies()
		deps.collect = func(context.Context, collectionClient, string, string, collectionLimits) (migrationcollection.Result, error) {
			return migrationcollection.Result{InferenceService: parent}, nil
		}
		deps.project = func(migrationcollection.Result, string, migrationprojection.Limits, reportv1alpha1.Clock) (reportv1alpha1.MigrationStatusReport, error) {
			return reportv1alpha1.MigrationStatusReport{}, boom
		}

		out, err := executeStatus(t, baseFactory, deps, "chat")
		require.ErrorIs(t, err, boom)
		assert.Empty(t, out)
	})

	t.Run("writer", func(t *testing.T) {
		boom := errors.New("writer failed")
		deps := defaultStatusDependencies()
		deps.collect = func(context.Context, collectionClient, string, string, collectionLimits) (migrationcollection.Result, error) {
			return migrationcollection.Result{InferenceService: parent}, nil
		}
		deps.project = func(migrationcollection.Result, string, migrationprojection.Limits, reportv1alpha1.Clock) (reportv1alpha1.MigrationStatusReport, error) {
			return reportv1alpha1.NewMigrationStatusReport(
				reportv1alpha1.Metadata{Namespace: "prod", Name: "chat"},
				reportv1alpha1.MigrationStatusContent{Summary: reportv1alpha1.MigrationSummary{State: reportv1alpha1.MigrationReportStateEmpty}},
				commandClock{commandNow},
			), nil
		}
		cmd := newStatusCmdWithDependencies(baseFactory, genericiooptions.IOStreams{
			In: &bytes.Buffer{}, Out: errorWriter{boom}, ErrOut: &bytes.Buffer{},
		}, deps)
		cmd.SetArgs([]string{"chat"})

		err := cmd.Execute()
		require.ErrorIs(t, err, boom)
	})
}

func executeStatus(t *testing.T, f factory.Factory, deps statusDependencies, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := newStatusCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	}, deps)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func commandISVC() *omev1beta1.InferenceService {
	return &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 5,
	}}
}

func commandIR(parent *omev1beta1.InferenceService, component omev1beta1.ComponentType) omev1beta1.InferenceReplica {
	controller := true
	name := parent.Name + "-" + string(component)
	return omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: parent.Namespace, UID: types.UID("uid-" + name), Generation: 2,
			Labels: map[string]string{constants.InferenceServicePodLabelKey: parent.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: parent.Name, UID: parent.UID, Controller: &controller,
			}},
		},
		Spec:   omev1beta1.InferenceReplicaSpec{ParentRef: omev1beta1.ParentReference{Name: parent.Name}, Component: component},
		Status: omev1beta1.InferenceReplicaStatus{ObservedGeneration: 2},
	}
}

type panicFactory struct{ factory.Factory }

type namespaceFactory struct {
	factory.Factory
	namespace string
	err       error
}

func (f namespaceFactory) Namespace() (string, bool, error) { return f.namespace, false, f.err }

type commandClock struct{ now time.Time }

func (c commandClock) Now() time.Time { return c.now }

type errorWriter struct{ err error }

func (w errorWriter) Write([]byte) (int, error) { return 0, w.err }
