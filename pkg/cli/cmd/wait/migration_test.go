package wait

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericclioptions"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ktesting "k8s.io/client-go/testing"
	clocktesting "k8s.io/utils/clock/testing"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/yaml"
)

const waitMigrationID = "12345678-1234-4234-8234-123456789abc"

func waitMigrationFixture() (*ome.InferenceService, *ome.InferenceReplica) {
	parent := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: types.UID("PRIVATE-PARENT-UID"), ResourceVersion: "rv-1", Generation: 7, Annotations: map[string]string{"password": "PRIVATE-ANNOTATION"}}}
	controller := true
	ir := &ome.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "prod", UID: types.UID("PRIVATE-IR-UID"), ResourceVersion: "rv-ir", Generation: 2, Labels: map[string]string{constants.InferenceServicePodLabelKey: "chat"}, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: parent.UID, Controller: &controller}}},
		Spec:       ome.InferenceReplicaSpec{ParentRef: ome.ParentReference{Name: "chat"}, Component: ome.EngineComponent},
		Status:     ome.InferenceReplicaStatus{ObservedGeneration: 2, Migrations: []ome.MigrationStatus{{RequestUUID: waitMigrationID, Trigger: ome.MigrationTriggerManual, SourceInstance: 1, Phase: ome.MigrationPhaseFailed, StartedAt: metav1.NewTime(time.Unix(1000, 0)), Deadline: metav1.NewTime(time.Unix(2000, 0)), CompletedAt: waitMetaTime(time.Unix(1500, 0)), Message: "PRIVATE-STATUS-MESSAGE"}}},
	}
	return parent, ir
}

func waitMetaTime(value time.Time) *metav1.Time { v := metav1.NewTime(value); return &v }

type migrationWarningCounter struct{ calls atomic.Int32 }

func (h *migrationWarningCounter) HandleWarningHeader(int, string, string) { h.calls.Add(1) }
func (h *migrationWarningCounter) HandleWarningHeaderWithContext(context.Context, int, string, string) {
	h.calls.Add(1)
}

func TestMigrationWaitSuppressesRealHTTPWarnings(t *testing.T) {
	parent, ir := waitMigrationFixture()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Warning", `299 fixture "PRIVATE_WARNING_CREDENTIAL"`)
		switch r.URL.Path {
		case "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat":
			_ = json.NewEncoder(w).Encode(parent)
		case "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas":
			_ = json.NewEncoder(w).Encode(&ome.InferenceReplicaList{Items: []ome.InferenceReplica{*ir}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	flags := genericclioptions.NewConfigFlags(true)
	flags.APIServer = &server.URL
	ns := "prod"
	flags.Namespace = &ns
	f := factory.New(flags)
	config, err := f.RESTConfig()
	require.NoError(t, err)
	warnings := &migrationWarningCounter{}
	config.WarningHandler = warnings
	config.WarningHandlerWithContext = warnings
	out, stderr, err := execute(t, f, "chat", "--for=migration=terminal", "--request-id="+waitMigrationID, "-o", "json")
	require.NoError(t, err)
	require.Equal(t, int32(2), requests.Load(), "one parent GET and one related-IR LIST")
	require.Zero(t, warnings.calls.Load(), "server Warning text must not reach inherited handlers")
	require.NotContains(t, out+stderr, "PRIVATE_WARNING_CREDENTIAL")
	require.Contains(t, out, `"outcome": "Matched"`)
}

func TestMigrationTerminalCommandReportsExactRequestWithoutPrivateFields(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			parent, ir := waitMigrationFixture()
			client := omefake.NewSimpleClientset(parent, ir)
			out, stderr, err := execute(t, factory.Static{NS: "prod", OME: client}, "chat", "--for=migration=terminal", "--request-id="+waitMigrationID, "-o", format)
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.Contains(t, out, waitMigrationID)
			require.Contains(t, out, "Failed")
			require.Contains(t, out, "Matched")
			require.NotContains(t, out, "PRIVATE")
			if format == "table" || format == "wide" {
				for _, line := range strings.Split(out, "\n") {
					require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
				}
				if format == "table" {
					t.Logf("kubectl ome wait chat --for=migration=terminal --request-id=%s -n prod (synthetic fixture):\n%s", waitMigrationID, out)
				}
				return
			}
			data := []byte(out)
			if format == "yaml" {
				data, err = yaml.YAMLToJSONStrict(data)
				require.NoError(t, err)
			}
			var report reportv1alpha1.WaitReport
			require.NoError(t, json.Unmarshal(data, &report))
			require.Equal(t, reportv1alpha1.WaitRequestedMigrationTerminal, report.Content.Requested)
			require.Equal(t, reportv1alpha1.MigrationPhaseFailed, report.Content.Migration.Phase)
			require.Equal(t, 0, report.Content.Counts.Watches)
		})
	}
}

func TestMigrationRequestGrammarRejectsInputsBeforeAcquisition(t *testing.T) {
	for _, args := range [][]string{
		{"chat", "--for=migration=terminal"},
		{"chat", "--for=migration=terminal", "--request-id=Bearer-PRIVATE"},
		{"chat", "--for=migration=completed", "--request-id=" + waitMigrationID},
		{"chat", "--for=condition=Ready", "--request-id=" + waitMigrationID},
		{"chat", "--for=rollout=stable", "--request-id=" + waitMigrationID},
	} {
		f := &waitFactory{Static: factory.Static{NS: "prod"}}
		out, stderr, err := execute(t, f, args...)
		require.Error(t, err)
		require.Equal(t, 1, exitcode.FromError(err))
		require.Empty(t, out)
		require.Empty(t, stderr)
		require.Zero(t, f.calls.Load())
		require.NotContains(t, err.Error(), "PRIVATE")
	}
}

func TestMigrationCommandPollsAuthoritativeIRStatusWithoutParentWatch(t *testing.T) {
	parent, ir := waitMigrationFixture()
	active := ir.DeepCopy()
	active.Status.Migrations[0].Phase = ome.MigrationPhaseAccepted
	active.Status.Migrations[0].CompletedAt = nil
	terminal := ir.DeepCopy()
	client := omefake.NewSimpleClientset(parent)
	var lists atomic.Int32
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		value := active
		if lists.Add(1) > 1 {
			value = terminal
		}
		return true, &ome.InferenceReplicaList{Items: []ome.InferenceReplica{*value}}, nil
	})
	clk := clocktesting.NewFakeClock(time.Unix(3000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(factory.Static{NS: "prod", OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=migration=terminal", "--request-id=" + waitMigrationID, "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return lists.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("migration wait did not complete after terminal IR status")
	}
	require.Empty(t, stderr.String())
	require.Equal(t, int32(2), lists.Load())
	var report reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.Equal(t, "Matched", string(report.Content.Outcome))
	require.Equal(t, "Poll", string(report.Content.Method))
	require.Equal(t, 0, report.Content.Counts.Watches)
	require.Equal(t, 1, report.Content.Counts.Polls)
	require.False(t, report.Content.Fallback)
}

func TestMigrationMissingRequestTimesOutWithUnmetExitAndOneReport(t *testing.T) {
	parent, ir := waitMigrationFixture()
	ir.Status.Migrations[0].RequestUUID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	client := omefake.NewSimpleClientset(parent, ir)
	clk := clocktesting.NewFakeClock(time.Unix(3000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(factory.Static{NS: "prod", OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=migration=terminal", "--request-id=" + waitMigrationID, "--timeout=20s", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return len(client.Actions()) == 2 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(20 * time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("migration timeout did not finish")
	}
	require.Empty(t, stderr.String())
	var report reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.Equal(t, "TimedOut", string(report.Content.Outcome))
	require.Equal(t, "MigrationNotRecorded", string(report.Content.Reason))
	require.Equal(t, waitMigrationID, report.Content.Migration.RequestID)
	require.NotContains(t, out.String(), "PRIVATE")
}

func TestMigrationParentReplacementCannotMatchTerminalIR(t *testing.T) {
	parent, ir := waitMigrationFixture()
	ir.Status.Migrations[0].Phase = ome.MigrationPhaseAccepted
	ir.Status.Migrations[0].CompletedAt = nil
	client := omefake.NewSimpleClientset()
	var gets atomic.Int32
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		copy := parent.DeepCopy()
		if gets.Add(1) > 1 {
			copy.UID = "replacement-uid"
		}
		return true, copy, nil
	})
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &ome.InferenceReplicaList{Items: []ome.InferenceReplica{*ir}}, nil
	})
	clk := clocktesting.NewFakeClock(time.Unix(3000, 0))
	var out bytes.Buffer
	cmd := newCmd(factory.Static{NS: "prod", OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}}, clk)
	cmd.SetArgs([]string{"chat", "--for=migration=terminal", "--request-id=" + waitMigrationID, "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return gets.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("replacement did not stop wait")
	}
	require.Contains(t, out.String(), `"outcome": "Replaced"`)
	require.NotContains(t, out.String(), "PRIVATE")
	var report reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.Equal(t, "Unavailable", report.Content.Migration.Validity)
	require.Equal(t, reportv1alpha1.MigrationPhaseUnknown, report.Content.Migration.Phase)
	require.Equal(t, reportv1alpha1.MigrationOutcomeUnknown, report.Content.Migration.Outcome)
	require.Equal(t, reportv1alpha1.EvidenceUnavailable, report.Content.Evidence)
	require.Equal(t, "MigrationNotRecorded", string(report.Content.Reason))
}

func TestMigrationParentDeletionCannotRetainLiveIRAttribution(t *testing.T) {
	parent, ir := waitMigrationFixture()
	ir.Status.Migrations[0].Phase = ome.MigrationPhaseAccepted
	ir.Status.Migrations[0].CompletedAt = nil
	client := omefake.NewSimpleClientset()
	var gets atomic.Int32
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		copy := parent.DeepCopy()
		if gets.Add(1) > 1 {
			deleting := metav1.NewTime(time.Unix(3000, 0))
			copy.DeletionTimestamp = &deleting
		}
		return true, copy, nil
	})
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &ome.InferenceReplicaList{Items: []ome.InferenceReplica{*ir}}, nil
	})
	clk := clocktesting.NewFakeClock(time.Unix(3000, 0))
	var out bytes.Buffer
	cmd := newCmd(factory.Static{NS: "prod", OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}}, clk)
	cmd.SetArgs([]string{"chat", "--for=migration=terminal", "--request-id=" + waitMigrationID, "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return gets.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("deletion did not stop wait")
	}
	var report reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &report))
	require.Equal(t, "Deleted", string(report.Content.Outcome))
	require.Equal(t, "Unavailable", report.Content.Migration.Validity)
	require.Equal(t, reportv1alpha1.MigrationPhaseUnknown, report.Content.Migration.Phase)
	require.Equal(t, reportv1alpha1.MigrationOutcomeUnknown, report.Content.Migration.Outcome)
	require.Equal(t, reportv1alpha1.EvidenceUnavailable, report.Content.Evidence)
	require.Equal(t, "MigrationNotRecorded", string(report.Content.Reason))
	require.NotContains(t, out.String(), "PRIVATE")
}

func TestMigrationNotFoundAndReadFailureKeepExitSemanticsPrivate(t *testing.T) {
	t.Run("not found", func(t *testing.T) {
		out, _, err := execute(t, factory.Static{NS: "prod", OME: omefake.NewSimpleClientset()}, "chat", "--for=migration=terminal", "--request-id="+waitMigrationID, "-o", "json")
		require.Equal(t, 2, exitcode.FromError(err))
		require.Contains(t, out, `"outcome": "NotFound"`)
	})
	t.Run("read failed", func(t *testing.T) {
		client := omefake.NewSimpleClientset()
		client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
			return true, nil, errors.New("Bearer PRIVATE-credential")
		})
		out, _, err := execute(t, factory.Static{NS: "prod", OME: client}, "chat", "--for=migration=terminal", "--request-id="+waitMigrationID, "-o", "json")
		require.Equal(t, 1, exitcode.FromError(err))
		require.Empty(t, out)
		require.NotContains(t, err.Error(), "PRIVATE")
	})
	t.Run("parent canceled", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		var out bytes.Buffer
		cmd := NewCmd(factory.Static{NS: "prod", OME: omefake.NewSimpleClientset()}, genericiooptions.IOStreams{Out: &out})
		cmd.SetContext(ctx)
		cmd.SetArgs([]string{"chat", "--for=migration=terminal", "--request-id=" + waitMigrationID})
		err := cmd.Execute()
		require.Equal(t, 1, exitcode.FromError(err))
		require.Empty(t, out.String())
	})
}
