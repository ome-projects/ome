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
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	clocktesting "k8s.io/utils/clock/testing"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
	"sigs.k8s.io/yaml"
)

func rolloutService() *ome.InferenceService {
	mode := constants.OMENative
	v := &ome.InferenceService{TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "InferenceService"}, ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "work", UID: "PRIVATE-UID", ResourceVersion: "PRIVATE-RV", Generation: 7}, Spec: ome.InferenceServiceSpec{DeploymentMode: &mode, Engine: &ome.EngineSpec{}}}
	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {Lifecycle: &ome.LifecycleStatus{CurrentRevision: "service-engine-aaaaaaaa", UpdateRevision: "service-engine-aaaaaaaa"}}}
	return v
}

func terminalRollout(phase ome.RolloutPhase) *ome.InferenceService {
	v := rolloutService()
	v.Spec.Rollout = &ome.RolloutSpec{Groups: []ome.RolloutGroup{{Components: []ome.ComponentType{ome.EngineComponent}, Canary: &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}}}}}
	v.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {RolloutPhase: phase, LatestRolledoutRevision: "service-engine-rev-aaaaaaaa", Traffic: []ome.ComponentTrafficTarget{{RevisionName: "service-engine-rev-aaaaaaaa", Percent: 100}}}}
	v.Status.Canary = &ome.CanaryStatus{CanaryRevisionHash: "bbbbbbbb", StableRevisionHash: "aaaaaaaa", CurrentStep: 0}
	if phase == ome.RolloutPhaseRolledBack {
		v.Status.Canary.RolledBackRevisionHash = "bbbbbbbb"
	}
	return v
}

func TestRequestedFailureAndRollbackMatchSuccessfully(t *testing.T) {
	for _, tc := range []struct {
		predicate string
		phase     ome.RolloutPhase
		state     string
	}{{"failed", ome.RolloutPhaseFailed, "Failed"}, {"rolled-back", ome.RolloutPhaseRolledBack, "RolledBack"}} {
		for _, format := range []string{"table", "wide", "json", "yaml"} {
			t.Run(tc.predicate+format, func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					require.Empty(t, r.URL.Query().Get("watch"))
					w.Header().Set("Content-Type", "application/json")
					v := terminalRollout(tc.phase)
					v.Annotations = map[string]string{"password": "PRIVATE-SECRET"}
					require.NoError(t, json.NewEncoder(w).Encode(v))
				}))
				defer server.Close()
				out, stderr, err := execute(t, &waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}, "service", "--for=rollout="+tc.predicate, "-o", format)
				require.NoError(t, err)
				require.Empty(t, stderr)
				require.Contains(t, out, tc.state)
				require.Contains(t, out, "Matched")
				require.NotContains(t, out, "PRIVATE")
				if format == "table" || format == "wide" {
					for _, line := range strings.Split(out, "\n") {
						require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
					}
				}
			})
		}
	}
}

func TestPinnedConcurrentCanaryWaitShowsBoundedReportedFailure(t *testing.T) {
	v := terminalRollout(ome.RolloutPhaseFailed)
	v.Spec.Router = &ome.RouterSpec{}
	ordering := ome.RolloutGroupOrderingConcurrent
	v.Spec.Rollout.GroupOrdering = &ordering
	v.Spec.Rollout.Groups = append(v.Spec.Rollout.Groups, ome.RolloutGroup{
		Components: []ome.ComponentType{ome.RouterComponent},
		Canary:     &ome.GroupCanary{Steps: []ome.RolloutGroupStep{{Capacity: intstr.FromString("100%"), Traffic: 100}}},
	})
	engine := v.Status.Components[ome.EngineComponent]
	engine.Canary = v.Status.Canary.DeepCopy()
	v.Status.Components[ome.EngineComponent] = engine
	v.Status.Components[ome.RouterComponent] = ome.ComponentStatusSpec{
		RolloutPhase: ome.RolloutPhaseStable, LatestRolledoutRevision: "service-router-rev-dddddddd",
		Traffic: []ome.ComponentTrafficTarget{{RevisionName: "service-router-rev-dddddddd", Percent: 100}},
		Canary:  &ome.CanaryStatus{CanaryRevisionHash: "dddddddd", CurrentStep: 1, ObservedTrafficWeight: 100},
	}
	stamp := metav1.NewTime(time.Date(2025, time.January, 1, 0, 0, 0, 0, time.UTC))
	run := &ome.RolloutRun{RunID: "service-0123456789ab", OpenedAt: stamp, PinnedAt: stamp,
		TargetRevisions: []ome.RolloutRunTarget{{Component: ome.EngineComponent, Revision: "bbbbbbbb"}, {Component: ome.RouterComponent, Revision: "dddddddd"}},
	}
	for _, group := range v.Spec.Rollout.Groups {
		digest, err := rolloutpolicy.ProgressionDigest(&group)
		require.NoError(t, err)
		run.Plan.Groups = append(run.Plan.Groups, ome.RolloutRunGroup{Source: ome.RolloutPlanSourceInline, PortableDigest: digest, Group: group})
	}
	v.Status.Rollout = &ome.RolloutStatus{ActiveRun: run}
	v.Annotations = map[string]string{"password": "PRIVATE-SECRET"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Empty(t, r.URL.Query().Get("watch"))
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(v))
	}))
	defer server.Close()

	out, stderr, err := execute(t, &waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}, "service", "--for=rollout=failed", "-o", "wide")
	require.NoError(t, err)
	require.Empty(t, stderr)
	require.Contains(t, out, "Reported rollout")
	require.Contains(t, out, "Failed")
	require.Contains(t, out, "Pinned groups")
	require.NotContains(t, out, "PRIVATE")
	for _, line := range strings.Split(out, "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
	t.Logf("kubectl ome wait service --for=rollout=failed -o wide (synthetic fixture):\n%s", out)
}

func TestRolloutGrammarIsClosedBeforeAcquisition(t *testing.T) {
	for _, predicate := range []string{"rollout=Stable", "rollout=Failed", "rollout=rolledback", "rollout=stable=True", "rollout=unknown", "rollout=", "rollout", "rollout=stable=failed", "rollout=stable,failed", "rollout=sk-proj-PRIVATE"} {
		f := &waitFactory{Static: factory.Static{NS: "work"}, configErr: errors.New("PRIVATE")}
		out, stderr, err := execute(t, f, "service", "--for="+predicate)
		require.Error(t, err)
		require.Empty(t, out)
		require.Empty(t, stderr)
		require.Zero(t, f.calls.Load())
		require.NotContains(t, err.Error(), "PRIVATE")
	}
}

func TestPredicateValidationResetsPriorSelection(t *testing.T) {
	o := options{forValue: "rollout=stable", timeout: time.Minute, output: "json"}
	require.NoError(t, o.validate("service"))
	require.Equal(t, reportv1alpha1.WaitRequestedRolloutStable, o.requestedRollout)

	o.forValue = "condition=Ready=False"
	require.NoError(t, o.validate("service"))
	require.Empty(t, o.requestedRollout)
	require.Equal(t, "False", string(o.requested))

	o.forValue = "rollout=failed"
	require.NoError(t, o.validate("service"))
	require.Empty(t, o.requested)
	require.Equal(t, reportv1alpha1.WaitRequestedRolloutFailed, o.requestedRollout)
}

func TestRolloutWatchStillBindsUIDBeforePredicate(t *testing.T) {
	for _, outcome := range []string{"Matched", "Replaced", "Deleted"} {
		t.Run(outcome, func(t *testing.T) {
			var gets, watches atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				v := rolloutService()
				if r.URL.Query().Get("watch") == "true" {
					watches.Add(1)
					require.Equal(t, "metadata.name=service", r.URL.Query().Get("fieldSelector"))
					require.Equal(t, "PRIVATE-RV", r.URL.Query().Get("resourceVersion"))
					v.ResourceVersion = "PRIVATE-NEXT"
					if outcome == "Replaced" {
						v.UID = "PRIVATE-REPLACEMENT"
					}
					kind := "MODIFIED"
					if outcome == "Deleted" {
						kind = "DELETED"
					}
					require.NoError(t, json.NewEncoder(w).Encode(map[string]any{"type": kind, "object": v}))
					return
				}
				gets.Add(1)
				v.Status.Components = nil
				require.NoError(t, json.NewEncoder(w).Encode(v))
			}))
			defer server.Close()
			out, stderr, err := execute(t, &waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}, "service", "--for=rollout=stable", "-o", "json")
			if outcome == "Matched" {
				require.NoError(t, err)
			} else {
				require.Equal(t, 2, exitcode.FromError(err))
			}
			require.Empty(t, stderr)
			require.Contains(t, out, `"outcome": "`+outcome+`"`)
			require.NotContains(t, out, "PRIVATE")
			require.Equal(t, int32(1), gets.Load())
			require.Equal(t, int32(1), watches.Load())
		})
	}
}

func TestInvalidOrNonmatchingRolloutTimesOutWithoutUpgrade(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "nonmatching", true: "invalid"}[invalid], func(t *testing.T) {
			clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
			opened := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.URL.Query().Get("watch") == "true" {
					w.WriteHeader(200)
					w.(http.Flusher).Flush()
					close(opened)
					<-r.Context().Done()
					return
				}
				v := terminalRollout(ome.RolloutPhaseFailed)
				if invalid {
					v = rolloutService()
					v.Status.Components[ome.EngineComponent].Lifecycle.ReadyReplicas = -1
				}
				require.NoError(t, json.NewEncoder(w).Encode(v))
			}))
			defer server.Close()
			var out, stderr bytes.Buffer
			cmd := newCmd(&waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
			cmd.SetArgs([]string{"service", "--for=rollout=stable", "-o", "json"})
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true
			done := make(chan error, 1)
			go func() { done <- cmd.Execute() }()
			select {
			case <-opened:
			case <-time.After(2 * time.Second):
				t.Fatal("watch not opened")
			}
			clk.Step(time.Minute)
			select {
			case err := <-done:
				require.Equal(t, 2, exitcode.FromError(err))
			case <-time.After(2 * time.Second):
				t.Fatal("timeout not observed")
			}
			require.Contains(t, out.String(), `"outcome": "TimedOut"`)
			require.NotContains(t, out.String(), "PRIVATE")
			require.Empty(t, stderr.String())
			if invalid {
				require.Contains(t, out.String(), `"validity": "Invalid"`)
			}
		})
	}
}

func TestRolloutErrorsRemainFixedAndGeneral(t *testing.T) {
	for _, tc := range []string{"forbidden", "identity", "limit", "writer", "canceled"} {
		t.Run(tc, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if tc == "forbidden" {
					http.Error(w, "PRIVATE", 403)
					return
				}
				v := rolloutService()
				if tc == "identity" {
					v.UID = ""
				}
				if tc == "limit" {
					v.Annotations = map[string]string{"x": strings.Repeat("PRIVATE", 600)}
				}
				require.NoError(t, json.NewEncoder(w).Encode(v))
			}))
			defer server.Close()
			var out, stderr bytes.Buffer
			streams := genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}
			if tc == "writer" {
				streams.Out = brokenWriter{}
			}
			cmd := NewCmd(&waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}, streams)
			cmd.SilenceUsage = true
			cmd.SilenceErrors = true
			if tc == "canceled" {
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				cmd.SetContext(ctx)
			}
			cmd.SetArgs([]string{"service", "--for=rollout=stable", "-o", "json"})
			err := cmd.Execute()
			require.Equal(t, 1, exitcode.FromError(err))
			require.NotContains(t, err.Error(), "PRIVATE")
			require.Empty(t, out.String())
			require.Empty(t, stderr.String())
		})
	}
}

func TestRolloutAbsentReportAndAllWriterFailures(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		for _, absent := range []bool{false, true} {
			t.Run(format+map[bool]string{false: "writer", true: "absent"}[absent], func(t *testing.T) {
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if absent {
						http.Error(w, "PRIVATE", 404)
						return
					}
					w.Header().Set("Content-Type", "application/json")
					require.NoError(t, json.NewEncoder(w).Encode(rolloutService()))
				}))
				defer server.Close()
				var out, stderr bytes.Buffer
				streams := genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}
				if !absent {
					streams.Out = brokenWriter{}
				}
				cmd := NewCmd(&waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}, streams)
				cmd.SilenceUsage = true
				cmd.SilenceErrors = true
				cmd.SetArgs([]string{"service", "--for=rollout=stable", "-o", format})
				err := cmd.Execute()
				if absent {
					require.Equal(t, 2, exitcode.FromError(err))
					require.Contains(t, out.String(), "NotFound")
					require.Contains(t, out.String(), "Unavailable")
				} else {
					require.EqualError(t, err, "WaitOutputFailed")
					require.Empty(t, out.String())
				}
				require.NotContains(t, out.String(), "PRIVATE")
				require.Empty(t, stderr.String())
			})
		}
	}
}

func TestInitialRolloutStableAllFormats(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				require.Equal(t, "GET", r.Method)
				require.Equal(t, "/apis/ome.io/v1beta1/namespaces/work/inferenceservices/service", r.URL.Path)
				require.Equal(t, "10s", r.URL.Query().Get("timeout"))
				require.Len(t, r.URL.Query(), 1)
				w.Header().Set("Content-Type", "application/json")
				require.NoError(t, json.NewEncoder(w).Encode(rolloutService()))
			}))
			defer server.Close()
			f := &waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}
			out, stderr, err := execute(t, f, "service", "--for=rollout=stable", "-o", format)
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.Equal(t, int32(1), requests.Load())
			require.NotContains(t, out, "PRIVATE")
			if format == "json" || format == "yaml" {
				data := []byte(out)
				if format == "yaml" {
					data, err = yaml.YAMLToJSONStrict(data)
					require.NoError(t, err)
				}
				decoder := json.NewDecoder(bytes.NewReader(data))
				decoder.DisallowUnknownFields()
				var value reportv1alpha1.WaitReport
				require.NoError(t, decoder.Decode(&value))
				require.Equal(t, reportv1alpha1.WaitRequestedRolloutStable, value.Content.Requested)
				require.Equal(t, reportv1alpha1.RolloutStateSucceeded, value.Content.Rollout.Summary.ReportedState)
				require.Equal(t, reportv1alpha1.RolloutStateUnknown, value.Content.Rollout.Summary.State)
				require.Equal(t, reportv1alpha1.RolloutEpochUnverifiable, value.Content.Rollout.Summary.Epoch)
			} else {
				require.Contains(t, out, "Succeeded")
				require.Contains(t, out, "Unverifiable")
			}
		})
	}
}

func TestRolloutHelpDescribesExactReportedAssertions(t *testing.T) {
	out, _, err := execute(t, &waitFactory{}, "--help")
	require.NoError(t, err)
	for _, text := range []string{"rollout=stable|failed|rolled-back", "Succeeded", "NotConfigured", "Staged", "preceding action", "aggregate"} {
		require.Contains(t, out, text)
	}
}
