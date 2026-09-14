package migration

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/migrationhistorycollection"
	"sigs.k8s.io/ome/pkg/cli/migrationhistoryprojection"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestHistoryProductionWiringShowsThreeSourcesAndBoundsRequests(t *testing.T) {
	t.Parallel()

	parent := historyCommandISVC()
	parent.Status.MigrationHistory = []omev1beta1.MigrationHistoryEntry{{
		ID: "parent-request", Component: omev1beta1.EngineComponent, Instance: 1,
		Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhaseCompleted,
		RequestedAt: metav1.NewTime(commandNow.Add(-10 * time.Minute)),
		CompletedAt: historyTimePtr(commandNow.Add(-2 * time.Minute)), OutcomeReason: "ReplacementReady",
	}}
	ir := historyCommandIR(parent)
	ir.Status.Migrations = []omev1beta1.MigrationStatus{{
		RequestUUID: "active-request", Trigger: omev1beta1.MigrationTriggerManual,
		SourceInstance: 2, Phase: omev1beta1.MigrationPhaseAccepted,
		StartedAt: metav1.NewTime(commandNow.Add(-time.Minute)), Deadline: metav1.NewTime(commandNow.Add(time.Hour)),
	}}
	omeClient := omefake.NewSimpleClientset(parent, &ir)
	audit := historyCommandAudit(parent, `{"entries":[{"requestUUID":"audit-request","component":"router","sourceInstance":0,"phase":"Failed","startedAt":"2026-09-14T18:40:00Z","completedAt":"2026-09-14T18:41:00Z","outcome":"NodeUnavailable"}]}`)
	kubeClient := k8sfake.NewSimpleClientset(audit)
	deps := defaultHistoryDependencies()
	deps.clock = commandClock{commandNow}

	out, err := executeHistory(t, factory.Static{OME: omeClient, Kube: kubeClient, NS: "prod"}, deps, "chat")

	require.NoError(t, err)
	assert.Equal(t,
		"EVID     REQUEST/COMP   PHASE/STATE   WHEN           DETAIL\n"+
			"AUTH     active-r/E     Accepted/A    09-14T18:59Z   -\n"+
			"PARENT   parent-r/E     Completed/T   09-14T18:58Z   ReplacementReady\n"+
			"AUDIT    audit-re/R     Failed/T      09-14T18:41Z   NodeUnavailable\n",
		out,
	)
	require.Len(t, omeClient.Actions(), 2)
	assert.Equal(t, "inferenceservices", omeClient.Actions()[0].GetResource().Resource)
	list := omeClient.Actions()[1].(ktesting.ListAction)
	value, found := list.GetListRestrictions().Labels.RequiresExactMatch(constants.InferenceServicePodLabelKey)
	assert.True(t, found)
	assert.Equal(t, "chat", value)
	require.Len(t, kubeClient.Actions(), 1)
	assert.Equal(t, "configmaps", kubeClient.Actions()[0].GetResource().Resource)
}

func TestHistorySupportsComponentAndExactWideJSONYAML(t *testing.T) {
	t.Parallel()

	parent := historyCommandISVC()
	parent.Status.MigrationHistory = []omev1beta1.MigrationHistoryEntry{
		{ID: "engine-request", Component: omev1beta1.EngineComponent, Instance: 1, Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhasePending, RequestedAt: metav1.NewTime(commandNow.Add(-time.Minute))},
		{ID: "router-request", Component: omev1beta1.RouterComponent, Instance: 2, Mode: omev1beta1.MigrationModeSurge, Phase: omev1beta1.MigrationPhasePending, RequestedAt: metav1.NewTime(commandNow.Add(-time.Minute))},
	}
	deps := defaultHistoryDependencies()
	deps.clock = commandClock{commandNow}

	for _, output := range []string{"wide", "json", "yaml"} {
		omeClient := omefake.NewSimpleClientset(parent)
		got, err := executeHistory(t, factory.Static{OME: omeClient, Kube: k8sfake.NewSimpleClientset(), NS: "prod"}, deps, "chat", "--component", "engine", "-o", output)
		require.NoError(t, err, output)
		assert.Contains(t, got, "engine-request", output)
		assert.NotContains(t, got, "router-request", output)
		switch output {
		case "wide":
			assert.Contains(t, got, "EVIDENCE")
			assert.Contains(t, got, "ParentSummary")
		case "json":
			var typed reportv1alpha1.MigrationHistoryReport
			require.NoError(t, json.Unmarshal([]byte(got), &typed))
			assert.Equal(t, reportv1alpha1.MigrationHistoryReportKind, typed.Kind)
		case "yaml":
			assert.Contains(t, got, "kind: MigrationHistoryReport\n")
		}
	}
}

func TestHistoryHelpExplainsAuthorityBoundsRedactionAndNamespace(t *testing.T) {
	t.Parallel()

	cmd := newHistoryCmd(panicFactory{}, genericiooptions.IOStreams{})
	long := strings.Join(strings.Fields(cmd.Long), " ")

	for _, fragment := range []string{
		"InferenceReplica status is authoritative work state",
		"InferenceService history and the optional audit ConfigMap are bounded historical evidence",
		"workload namespace",
		"does not read the OME control-plane namespace",
		"Raw ConfigMap data, annotations, caller identity, reasons, event messages, UIDs",
		"80 columns",
		"-o wide",
	} {
		assert.Contains(t, long, fragment)
	}
	assert.Nil(t, cmd.Flags().Lookup("ome-namespace"))
}

func TestHistoryValidatesArgumentsBeforeFactoryAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want error
		text string
	}{
		{name: "missing name", args: nil},
		{name: "extra name", args: []string{"chat", "other"}},
		{name: "bad name", args: []string{"Bad_Name"}, want: ErrInvalidInferenceServiceName},
		{name: "bad component", args: []string{"chat", "--component", "predictor"}, want: ErrInvalidComponent},
		{name: "bad format", args: []string{"chat", "-o", "xml"}, text: `unsupported output format "xml" (supported: table, wide, json, yaml)`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := executeHistory(t, panicFactory{}, defaultHistoryDependencies(), test.args...)
			require.Error(t, err)
			if test.want != nil {
				assert.ErrorIs(t, err, test.want)
			}
			if test.text != "" {
				assert.Contains(t, err.Error(), test.text)
			}
		})
	}
}

func TestHistoryValidatesNamespaceAndClientConstruction(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name     string
		factory  factory.Factory
		want     error
		wantText string
	}{
		{name: "resolver", factory: namespaceFactory{err: errors.New("resolver failed")}, wantText: "resolver failed"},
		{name: "empty", factory: namespaceFactory{}, want: ErrInvalidNamespace},
		{name: "invalid", factory: namespaceFactory{namespace: "Bad_NS"}, want: ErrInvalidNamespace},
		{name: "OME", factory: factory.Static{NS: "prod", Kube: k8sfake.NewSimpleClientset()}, wantText: "no OME client"},
		{name: "Kube", factory: factory.Static{NS: "prod", OME: omefake.NewSimpleClientset()}, wantText: "no kube client"},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := executeHistory(t, test.factory, defaultHistoryDependencies(), "chat")
			require.Error(t, err)
			if test.want != nil {
				assert.ErrorIs(t, err, test.want)
			}
			if test.wantText != "" {
				assert.Contains(t, err.Error(), test.wantText)
			}
		})
	}
}

func TestHistoryPropagatesCollectorProjectorAndWriterErrors(t *testing.T) {
	t.Parallel()

	parent := historyCommandISVC()
	f := factory.Static{NS: "prod", OME: omefake.NewSimpleClientset(parent), Kube: k8sfake.NewSimpleClientset()}

	t.Run("collector", func(t *testing.T) {
		boom := errors.New("collect failed")
		deps := defaultHistoryDependencies()
		deps.collect = func(context.Context, omeclient.OmeV1beta1Interface, kubernetes.Interface, string, string, paging.Limits) (migrationhistorycollection.Result, error) {
			return migrationhistorycollection.Result{}, boom
		}
		deps.project = func(migrationhistorycollection.Result, string, migrationhistoryprojection.Limits, reportv1alpha1.Clock) (reportv1alpha1.MigrationHistoryReport, error) {
			panic("project must not run")
		}
		out, err := executeHistory(t, f, deps, "chat")
		require.ErrorIs(t, err, boom)
		assert.Empty(t, out)
	})

	t.Run("projector", func(t *testing.T) {
		boom := errors.New("project failed")
		deps := defaultHistoryDependencies()
		deps.collect = func(context.Context, omeclient.OmeV1beta1Interface, kubernetes.Interface, string, string, paging.Limits) (migrationhistorycollection.Result, error) {
			return migrationhistorycollection.Result{InferenceService: parent}, nil
		}
		deps.project = func(migrationhistorycollection.Result, string, migrationhistoryprojection.Limits, reportv1alpha1.Clock) (reportv1alpha1.MigrationHistoryReport, error) {
			return reportv1alpha1.MigrationHistoryReport{}, boom
		}
		out, err := executeHistory(t, f, deps, "chat")
		require.ErrorIs(t, err, boom)
		assert.Empty(t, out)
	})

	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run("writer "+format, func(t *testing.T) {
			boom := errors.New("write failed")
			deps := defaultHistoryDependencies()
			deps.collect = func(context.Context, omeclient.OmeV1beta1Interface, kubernetes.Interface, string, string, paging.Limits) (migrationhistorycollection.Result, error) {
				return migrationhistorycollection.Result{InferenceService: parent}, nil
			}
			deps.project = func(migrationhistorycollection.Result, string, migrationhistoryprojection.Limits, reportv1alpha1.Clock) (reportv1alpha1.MigrationHistoryReport, error) {
				return reportv1alpha1.NewMigrationHistoryReport(reportv1alpha1.Metadata{Name: "chat"}, reportv1alpha1.MigrationHistoryContent{}, commandClock{commandNow}), nil
			}
			cmd := newHistoryCmdWithDependencies(f, genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: historyErrorWriter{boom}, ErrOut: &bytes.Buffer{},
			}, deps)
			cmd.SetArgs([]string{"chat", "-o", format})
			err := cmd.Execute()
			require.ErrorIs(t, err, boom)
		})
	}
}

func TestMigrationCommandRegistersHistory(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(panicFactory{}, genericiooptions.IOStreams{})
	child, _, err := cmd.Find([]string{"history"})
	require.NoError(t, err)
	assert.Equal(t, "history INFERENCESERVICE", child.Use)

	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--help"})
	require.NoError(t, cmd.Execute())
	assert.Contains(t, out.String(), "history")
}

func executeHistory(t *testing.T, f factory.Factory, deps historyDependencies, args ...string) (string, error) {
	t.Helper()
	var output bytes.Buffer
	cmd := newHistoryCmdWithDependencies(f, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &output, ErrOut: &output,
	}, deps)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return output.String(), err
}

func historyCommandISVC() *omev1beta1.InferenceService {
	result := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("uid-chat"), Generation: 5,
	}}
	result.Status.ObservedGeneration = 5
	return result
}

func historyCommandIR(parent *omev1beta1.InferenceService) omev1beta1.InferenceReplica {
	controller := true
	return omev1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat-engine", Namespace: parent.Namespace, UID: "uid-chat-engine", Generation: 2,
			Labels: map[string]string{constants.InferenceServicePodLabelKey: parent.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
				Name: parent.Name, UID: parent.UID, Controller: &controller,
			}},
		},
		Spec:   omev1beta1.InferenceReplicaSpec{ParentRef: omev1beta1.ParentReference{Name: parent.Name}, Component: omev1beta1.EngineComponent},
		Status: omev1beta1.InferenceReplicaStatus{ObservedGeneration: 2},
	}
}

func historyCommandAudit(parent *omev1beta1.InferenceService, raw string) *corev1.ConfigMap {
	controller := true
	return &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{
		Name: parent.Name + migrationhistorycollection.AuditConfigMapSuffix, Namespace: parent.Namespace,
		OwnerReferences: []metav1.OwnerReference{{
			APIVersion: omev1beta1.SchemeGroupVersion.String(), Kind: "InferenceService",
			Name: parent.Name, UID: parent.UID, Controller: &controller,
		}},
	}, Data: map[string]string{migrationhistorycollection.AuditHistoryKey: raw}}
}

func historyTimePtr(value time.Time) *metav1.Time {
	result := metav1.NewTime(value)
	return &result
}

type historyErrorWriter struct{ err error }

func (w historyErrorWriter) Write([]byte) (int, error) { return 0, w.err }

func TestHistoryOutputNeverLeaksConfigMapMetadataOrRawFields(t *testing.T) {
	t.Parallel()

	parent := historyCommandISVC()
	cm := historyCommandAudit(parent, `{"entries":[],"SECRET_RAW_KEY":"SECRET_RAW_VALUE"}`)
	cm.Annotations = map[string]string{"SECRET_ANNOTATION": "SECRET_ANNOTATION_VALUE"}
	cm.Labels = map[string]string{"SECRET_LABEL": "SECRET_LABEL_VALUE"}
	cm.ResourceVersion = "SECRET_RESOURCE_VERSION"
	cm.UID = "SECRET_UID"
	deps := defaultHistoryDependencies()
	deps.clock = commandClock{commandNow}

	for _, format := range []string{"table", "wide", "json", "yaml"} {
		output, err := executeHistory(t, factory.Static{
			NS: "prod", OME: omefake.NewSimpleClientset(parent), Kube: k8sfake.NewSimpleClientset(cm),
		}, deps, "chat", "-o", format)
		require.NoError(t, err)
		for _, canary := range []string{"SECRET_RAW_KEY", "SECRET_RAW_VALUE", "SECRET_ANNOTATION", "SECRET_LABEL", "SECRET_RESOURCE_VERSION", "SECRET_UID"} {
			assert.NotContains(t, output, canary, format)
		}
	}
}

func TestHistoryCancellationStopsBeforeSecondaryEvidence(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	parent := historyCommandISVC()
	omeClient := omefake.NewSimpleClientset(parent)
	kubeClient := k8sfake.NewSimpleClientset()
	cmd := newHistoryCmdWithDependencies(factory.Static{NS: "prod", OME: omeClient, Kube: kubeClient}, genericiooptions.IOStreams{
		In: &bytes.Buffer{}, Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{},
	}, defaultHistoryDependencies())
	cmd.SetArgs([]string{"chat"})

	err := cmd.ExecuteContext(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Empty(t, omeClient.Actions())
	assert.Empty(t, kubeClient.Actions())
}

func TestHistoryLongNamesDoNotConstructInvalidAuditReads(t *testing.T) {
	t.Parallel()

	name := strings.Repeat("a", 253)
	parent := &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "prod", UID: "uid-parent"}}
	parent.Status.ObservedGeneration = 1
	parent.Generation = 1
	omeClient := omefake.NewSimpleClientset(parent)
	kubeClient := k8sfake.NewSimpleClientset()
	deps := defaultHistoryDependencies()
	deps.clock = commandClock{commandNow}

	out, err := executeHistory(t, factory.Static{NS: "prod", OME: omeClient, Kube: kubeClient}, deps, name, "-o", "json")

	require.NoError(t, err)
	assert.Contains(t, out, `"unavailableReason": "InvalidIdentity"`)
	assert.Empty(t, kubeClient.Actions())
}
