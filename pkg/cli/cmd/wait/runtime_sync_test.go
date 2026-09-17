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
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/yaml"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	"sigs.k8s.io/ome/pkg/constants"
)

const syncRequestID = "123e4567-e89b-42d3-a456-426614174000"

func runtimeSyncService() *ome.InferenceService {
	autoSync := false
	token := "cli-runtime-sync-" + syncRequestID
	return &ome.InferenceService{
		TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"},
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat", Namespace: "prod", UID: "PRIVATE-UID", ResourceVersion: "private-rv", Generation: 3,
			Annotations: map[string]string{constants.RuntimeSyncAnnotationKey: token, "private.example/secret": "PRIVATE-ANNOTATION"},
		},
		Spec:   ome.InferenceServiceSpec{Runtime: &ome.ServingRuntimeRef{Name: "runtime-a", AutoSync: &autoSync}},
		Status: ome.InferenceServiceStatus{PinnedRevisionName: "runtime-a-abc", LastRuntimeSyncToken: token},
	}
}

func TestRuntimeSyncWaitReportsExactAcknowledgmentInAllFormats(t *testing.T) {
	var jsonContent reportv1alpha1.WaitContent
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var gets, watches atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("watch") != "" {
					watches.Add(1)
				}
				gets.Add(1)
				require.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat", r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				require.NoError(t, json.NewEncoder(w).Encode(runtimeSyncService()))
			}))
			defer server.Close()
			var output, errorOutput bytes.Buffer
			clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
			cmd := newCmd(&waitFactory{Static: factory.Static{NS: "prod"}, config: &rest.Config{Host: server.URL}},
				genericiooptions.IOStreams{Out: &output, ErrOut: &errorOutput}, clk)
			cmd.SetArgs([]string{"chat", "--for=runtime-sync=acknowledged", "--request-id=" + syncRequestID, "-o", format})
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			err := cmd.Execute()
			out, stderr := output.String(), errorOutput.String()
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.Equal(t, int32(1), gets.Load())
			require.Zero(t, watches.Load())
			require.NotContains(t, out, "PRIVATE")
			require.NotContains(t, out, server.URL)
			t.Logf("kubectl ome wait chat --for=runtime-sync=acknowledged --request-id=%s -n prod -o %s (synthetic fixture):\n%s", syncRequestID, format, out)
			if format == "table" || format == "wide" {
				require.Contains(t, out, "RuntimeSync=Acknowledged")
				require.Contains(t, out, "Matched")
				require.Contains(t, out, "Acknowledged")
				require.NotContains(t, out, "Observed Ready")
				for _, line := range strings.Split(out, "\n") {
					require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
				}
				return
			}
			data := []byte(out)
			if format == "yaml" {
				data, err = yaml.YAMLToJSONStrict(data)
				require.NoError(t, err)
			}
			var value reportv1alpha1.WaitReport
			require.NoError(t, json.Unmarshal(data, &value))
			require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
			require.Equal(t, reportv1alpha1.WaitRequestedRuntimeSyncAcknowledged, value.Content.Requested)
			require.NotNil(t, value.Content.RuntimeSync)
			require.Equal(t, syncRequestID, value.Content.RuntimeSync.RequestID)
			require.Equal(t, "Acknowledged", string(value.Content.RuntimeSync.TokenState))
			require.Equal(t, "Unverifiable", value.Content.RuntimeSync.GenerationFreshness)
			require.Equal(t, 0, value.Content.Counts.Watches)
			if format == "json" {
				jsonContent = value.Content
			} else {
				require.Equal(t, jsonContent, value.Content)
			}
		})
	}
}

func TestRuntimeSyncWaitRejectsInvalidAndStrayFlagsBeforeAcquisition(t *testing.T) {
	for _, args := range [][]string{
		{"chat", "--for=runtime-sync=acknowledged"},
		{"chat", "--for=runtime-sync=acknowledged", "--request-id=PRIVATE"},
		{"chat", "--for=runtime-sync=acknowledged", "--request-id=123e4567-e89b-12d3-a456-426614174000"},
		{"chat", "--for=runtime-sync=acknowledged", "--request-id=" + syncRequestID, "--component=engine"},
		{"chat", "--for=runtime-sync=acknowledged", "--request-id=" + syncRequestID, "--replicas=0"},
		{"chat", "--for=runtime-sync=done", "--request-id=" + syncRequestID},
		{"chat", "--for=condition=Ready", "--request-id=" + syncRequestID},
	} {
		f := &waitFactory{Static: factory.Static{NS: "prod"}, configErr: errors.New("PRIVATE-CONFIG")}
		out, stderr, err := execute(t, f, args...)
		require.Error(t, err, "%v", args)
		require.Equal(t, 1, exitcode.FromError(err))
		require.Empty(t, out)
		require.Empty(t, stderr)
		require.Zero(t, f.calls.Load())
		require.NotContains(t, err.Error(), "PRIVATE")
	}
}

func TestRuntimeSyncAdditionPreservesMigrationValidationError(t *testing.T) {
	out, stderr, err := execute(t, &waitFactory{Static: factory.Static{NS: "prod"}},
		"chat", "--for=migration=terminal", "--request-id=bad")
	require.EqualError(t, err, "InvalidMigrationRequestID: require canonical UUID only with migration=terminal")
	require.Empty(t, out)
	require.Empty(t, stderr)
}

func TestRuntimeSyncWaitPollsSameParentUIDAndClearsLostEvidence(t *testing.T) {
	for _, tc := range []struct {
		name       string
		mutate     func(*ome.InferenceService)
		outcome    waitengine.Outcome
		tokenState string
	}{
		{name: "acknowledged", mutate: func(v *ome.InferenceService) {
			v.Status.LastRuntimeSyncToken = v.Annotations[constants.RuntimeSyncAnnotationKey]
		}, outcome: waitengine.OutcomeMatched, tokenState: "Acknowledged"},
		{name: "replaced", mutate: func(v *ome.InferenceService) {
			v.UID = "PRIVATE-REPLACEMENT"
			v.Status.LastRuntimeSyncToken = v.Annotations[constants.RuntimeSyncAnnotationKey]
		}, outcome: waitengine.OutcomeReplaced, tokenState: "Unavailable"},
		{name: "deleted", mutate: func(v *ome.InferenceService) {
			now := metav1.NewTime(time.Unix(1000, 0))
			v.DeletionTimestamp = &now
			v.Status.LastRuntimeSyncToken = v.Annotations[constants.RuntimeSyncAnnotationKey]
		}, outcome: waitengine.OutcomeDeleted, tokenState: "Unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gets, watches atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				v := runtimeSyncService()
				if gets.Add(1) == 1 {
					v.Status.LastRuntimeSyncToken = ""
				} else {
					tc.mutate(v)
					v.ResourceVersion = "private-rv2"
				}
				if r.URL.Query().Get("watch") != "" {
					watches.Add(1)
				}
				w.Header().Set("Content-Type", "application/json")
				require.NoError(t, json.NewEncoder(w).Encode(v))
			}))
			defer server.Close()
			clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
			var out, stderr bytes.Buffer
			cmd := newCmd(&waitFactory{Static: factory.Static{NS: "prod"}, config: &rest.Config{Host: server.URL}}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
			cmd.SetArgs([]string{"chat", "--for=runtime-sync=acknowledged", "--request-id=" + syncRequestID, "-o", "json"})
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			done := make(chan error, 1)
			go func() { done <- cmd.Execute() }()
			require.Eventually(t, func() bool { return gets.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
			clk.Step(5 * time.Second)
			select {
			case err := <-done:
				if tc.outcome == waitengine.OutcomeMatched {
					require.NoError(t, err)
				} else {
					require.Equal(t, 2, exitcode.FromError(err))
				}
			case <-time.After(time.Second):
				t.Fatal("runtime-sync wait did not stop after second snapshot")
			}
			require.Empty(t, stderr.String())
			require.Equal(t, int32(2), gets.Load())
			require.Zero(t, watches.Load())
			var value reportv1alpha1.WaitReport
			require.NoError(t, json.Unmarshal(out.Bytes(), &value))
			require.Equal(t, tc.outcome, value.Content.Outcome)
			if tc.outcome != waitengine.OutcomeMatched {
				require.Equal(t, "RuntimeSyncNotRecorded", string(value.Content.Reason))
			}
			require.Equal(t, tc.tokenState, string(value.Content.RuntimeSync.TokenState))
			require.NotContains(t, out.String(), "PRIVATE")
		})
	}
}

func TestRuntimeSyncWaitTimeoutKeepsOnlyLastReportedState(t *testing.T) {
	var gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		gets.Add(1)
		v := runtimeSyncService()
		v.Status.LastRuntimeSyncToken = ""
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(v))
	}))
	defer server.Close()
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(&waitFactory{Static: factory.Static{NS: "prod"}, config: &rest.Config{Host: server.URL}}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=runtime-sync=acknowledged", "--request-id=" + syncRequestID, "--timeout=20s", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return gets.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(20 * time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("runtime-sync wait did not time out")
	}
	require.Empty(t, stderr.String())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeTimedOut, value.Content.Outcome)
	require.Equal(t, "Pending", string(value.Content.RuntimeSync.TokenState))
	require.Equal(t, "Valid", value.Content.RuntimeSync.Validity)
	require.NotContains(t, out.String(), "PRIVATE")
}

func TestRuntimeSyncWaitCancellationEmitsNoReport(t *testing.T) {
	var gets atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		gets.Add(1)
		v := runtimeSyncService()
		v.Status.LastRuntimeSyncToken = ""
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(v))
	}))
	defer server.Close()
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(&waitFactory{Static: factory.Static{NS: "prod"}, config: &rest.Config{Host: server.URL}}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=runtime-sync=acknowledged", "--request-id=" + syncRequestID, "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- cmd.ExecuteContext(ctx) }()
	require.Eventually(t, func() bool { return gets.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.Equal(t, 1, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("runtime-sync wait did not stop after cancellation")
	}
	require.Empty(t, out.String())
	require.Empty(t, stderr.String())
}
