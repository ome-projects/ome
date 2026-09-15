package rollout

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	jsonpatch "github.com/evanphx/json-patch/v5"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/rolloutpolicy"
)

func TestCanaryActionsAreRegisteredWithClosedLocalFlags(t *testing.T) {
	for _, action := range []string{"promote", "rollback"} {
		f := &actionParserFactory{}
		var stdout, stderr bytes.Buffer
		cmd := NewCmd(f, genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr})
		found, _, err := cmd.Find([]string{action})
		require.NoError(t, err)
		require.Equal(t, action, found.Name(), "the action must be registered")
		require.Nil(t, found.Flags().Lookup("force"))
		require.Nil(t, found.Flags().Lookup("revision"))
		require.Nil(t, found.Flags().Lookup("discard-pending-actions"))
		if action == "promote" {
			require.NotNil(t, found.Flags().Lookup("override-analysis"))
		} else {
			require.Nil(t, found.Flags().Lookup("override-analysis"))
		}
		for _, suffix := range [][]string{{"--yes=PRIVATE_TOKEN"}, {"--dry-run=PRIVATE_TOKEN"}, {"--output=PRIVATE_TOKEN"}, {"--force=PRIVATE_TOKEN"}, {"--override-analysis"}} {
			f.calls = nil
			cmd := NewCmd(f, genericiooptions.IOStreams{Out: &stdout, ErrOut: &stderr})
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			cmd.SetArgs(append([]string{action, "chat"}, suffix...))
			err := cmd.Execute()
			require.Error(t, err)
			require.Empty(t, f.calls, "invalid flags must not acquire clients or namespace")
			require.NotContains(t, err.Error(), "PRIVATE_TOKEN")
		}
		require.False(t, strings.Contains(found.Long, "Resume clears"))
	}
}

var canaryNow = time.Date(2026, 9, 15, 21, 0, 0, 0, time.UTC)
var canaryClock = reportv1alpha1.ClockFunc(func() time.Time { return canaryNow })

func canaryActionFixture(t *testing.T, analysis bool) (*v1beta1.InferenceService, *v1beta1.ServingRuntime, *v1beta1.InferenceReplica) {
	t.Helper()
	v, rt, ir := actionFixture()
	v.Status.ObservedGeneration = 0
	g := v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{{Capacity: intstr.FromString("50%"), Traffic: 50, Pause: &v1beta1.RolloutPause{}}, {Capacity: intstr.FromString("100%"), Traffic: 100}}}}
	if analysis {
		g.Canary.Steps[0].Analysis = &v1beta1.RolloutAnalysis{Interval: metav1.Duration{Duration: time.Second}, FailureLimit: 2, Metrics: []v1beta1.AnalysisMetric{{Name: "PRIVATE_METRIC", Query: "PRIVATE_QUERY", Operator: v1beta1.ComparisonLTE, Threshold: "0.05"}}}
	}
	digest, err := rolloutpolicy.ProgressionDigest(&g)
	require.NoError(t, err)
	v.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{RunID: "chat-0123456789ab", OpenedAt: metav1.NewTime(canaryNow.Add(-time.Minute)), PinnedAt: metav1.NewTime(canaryNow.Add(-30 * time.Second)), Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Source: v1beta1.RolloutPlanSourceInline, PortableDigest: digest, Group: g}}}, TargetRevisions: []v1beta1.RolloutRunTarget{{Component: v1beta1.EngineComponent, Revision: "bbbbbbbb", StableRevision: "aaaaaaaa"}}}}
	x := metav1.NewTime(canaryNow.Add(-20 * time.Second))
	v.Status.Canary = &v1beta1.CanaryStatus{CanaryRevisionHash: "bbbbbbbb", StableRevisionHash: "aaaaaaaa", CurrentStep: 0, ObservedTrafficWeight: 50, StepEnteredTime: &x, TargetID: "ct1:" + rolloutpolicy.ShortHash([]byte("engine=bbbbbbbb"))}
	v.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{v1beta1.EngineComponent: {RolloutPhase: v1beta1.RolloutPhasePaused, LatestRolledoutRevision: "chat-engine-rev-aaaaaaaa", LatestReadyRevision: "chat-engine-rev-bbbbbbbb", Traffic: []v1beta1.ComponentTrafficTarget{{RevisionName: "chat-engine-rev-aaaaaaaa", Percent: 50}, {RevisionName: "chat-engine-rev-bbbbbbbb", Percent: 50}}}}
	return v, rt, ir
}

func serveCanaryRead(t *testing.T, w http.ResponseWriter, r *http.Request, v *v1beta1.InferenceService, ir *v1beta1.InferenceReplica) {
	t.Helper()
	require.Equal(t, "GET", r.Method)
	if strings.HasSuffix(r.URL.Path, "/inferenceservices/chat") {
		require.NoError(t, json.NewEncoder(w).Encode(v))
		return
	}
	require.True(t, strings.HasSuffix(r.URL.Path, "/inferencereplicas"), "no other resource, including Secrets, may be read")
	require.Equal(t, "ome.io/inferenceservice=chat", r.URL.Query().Get("labelSelector"))
	require.Equal(t, "16", r.URL.Query().Get("limit"))
	require.NoError(t, json.NewEncoder(w).Encode(&v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir}}))
}

func TestCanaryActionWireExactPatchDryRunAndFourOutputs(t *testing.T) {
	for _, action := range []string{"promote", "rollback", "override"} {
		for _, mode := range []string{"client", "server", "none"} {
			for _, format := range []string{"table", "wide", "json", "yaml"} {
				t.Run(action+"/"+mode+"/"+format, func(t *testing.T) {
					v, rt, ir := canaryActionFixture(t, action == "override")
					original, err := json.Marshal(v)
					require.NoError(t, err)
					patches, reads := 0, 0
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						w.Header().Set("Content-Type", "application/json")
						if r.Method != "PATCH" {
							reads++
							serveCanaryRead(t, w, r, v, ir)
							return
						}
						patches++
						require.Equal(t, "/apis/ome.io/v1beta1/namespaces/prod/inferenceservices/chat", r.URL.Path)
						require.Equal(t, "application/json-patch+json", r.Header.Get("Content-Type"))
						if mode == "server" {
							require.Equal(t, "All", r.URL.Query().Get("dryRun"))
						} else {
							require.Empty(t, r.URL.Query().Get("dryRun"))
						}
						body, e := io.ReadAll(r.Body)
						require.NoError(t, e)
						key, value := "promote", "bbbbbbbb"
						if action == "rollback" {
							key, value = "rollback", "true"
						}
						require.JSONEq(t, `[{"op":"test","path":"/metadata/uid","value":"uid-chat"},{"op":"test","path":"/metadata/resourceVersion","value":"42"},{"op":"add","path":"/metadata/annotations/ome.io~1rollout-`+key+`","value":"`+value+`"}]`, string(body))
						patch, e := jsonpatch.DecodePatch(body)
						require.NoError(t, e)
						next, e := patch.Apply(original)
						require.NoError(t, e)
						var got v1beta1.InferenceService
						require.NoError(t, json.Unmarshal(next, &got))
						expected := v.DeepCopy()
						expected.Annotations["ome.io/rollout-"+key] = value
						expectedBody, e := json.Marshal(expected)
						require.NoError(t, e)
						require.JSONEq(t, string(expectedBody), string(next), "only the mailbox may change; status, spec and unrelated metadata remain exact")
						_, e = w.Write(next)
						require.NoError(t, e)
					}))
					defer server.Close()
					var out, stderr bytes.Buffer
					f := newWireFactory(t, server, rt)
					f.Context = "-moirai"
					cmd := newCmdWithClock(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, canaryClock)
					cmd.SilenceErrors, cmd.SilenceUsage = true, true
					command := action
					if action == "override" {
						command = "promote"
					}
					args := []string{command, "chat", "--yes", "--dry-run=" + mode, "-o", format}
					if action == "override" {
						args = append(args, "--override-analysis")
					}
					cmd.SetArgs(args)
					require.NoError(t, cmd.Execute())
					require.Equal(t, 2, reads)
					if mode == "client" {
						require.Zero(t, patches)
					} else {
						require.Equal(t, 1, patches)
					}
					if format == "json" || format == "yaml" {
						body := out.Bytes()
						if format == "yaml" {
							body, err = yaml.YAMLToJSON(body)
							require.NoError(t, err)
						}
						decoder := json.NewDecoder(bytes.NewReader(body))
						var result reportv1alpha1.ActionResult
						require.NoError(t, decoder.Decode(&result))
						var extra any
						require.ErrorIs(t, decoder.Decode(&extra), io.EOF)
						if format == "yaml" {
							require.NotContains(t, out.String(), "\n---")
						}
						require.Equal(t, "bbbbbbbb", result.RevisionHash)
						require.Equal(t, mode != "client", result.Accepted)
						require.Equal(t, mode == "none", result.Applied)
						require.Equal(t, "42", result.Target.ResourceVersion)
						require.Equal(t, "uid-chat", result.Target.UID)
						require.Contains(t, result.FollowUp, "--context=-moirai")
						if mode == "none" {
							require.Contains(t, result.Message, "convergence not observed")
						}
						if action == "override" {
							require.Contains(t, result.Message, "Analysis override")
						}
					}
					require.NotContains(t, out.String()+stderr.String(), "PRIVATE_")
					require.NotContains(t, out.String()+stderr.String(), "SECRET_")
					require.Contains(t, stderr.String(), "chat-0123456789ab")
					for _, line := range strings.Split(stderr.String(), "\n") {
						require.LessOrEqual(t, printers.CellDisplayWidth(line), 80, line)
					}
					if action == "override" {
						require.Contains(t, stderr.String(), "ANALYSIS OVERRIDE")
					}
				})
			}
		}
	}
}

func TestCanaryActionParentSnapshotRacesRejectOnePatch(t *testing.T) {
	for _, action := range []string{"promote", "rollback"} {
		for _, race := range []string{"uid", "step", "run", "repin", "mailbox", "pause", "placement"} {
			t.Run(action+"/"+race, func(t *testing.T) {
				v, rt, ir := canaryActionFixture(t, false)
				changed := v.DeepCopy()
				if race == "uid" {
					changed.UID = "recreated"
				} else {
					changed.ResourceVersion = "43"
				}
				switch race {
				case "step":
					changed.Status.Canary.CurrentStep = 1
				case "run":
					changed.Status.Rollout.ActiveRun.RunID = "chat-999999999999"
				case "repin":
					changed.Status.Rollout.ActiveRun.PinnedAt = metav1.NewTime(canaryNow)
				case "mailbox":
					changed.Annotations[constants.RolloutRollbackAnnotation] = "false"
				case "pause":
					changed.Annotations[constants.PausedRolloutAnnotation] = "freeze"
				case "placement":
					changed.Finalizers = []string{"ome.io/placement"}
				}
				before, err := json.Marshal(changed)
				require.NoError(t, err)
				patches, reads := 0, 0
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					w.Header().Set("Content-Type", "application/json")
					if r.Method != "PATCH" {
						reads++
						serveCanaryRead(t, w, r, v, ir)
						return
					}
					patches++
					body, e := io.ReadAll(r.Body)
					require.NoError(t, e)
					patch, e := jsonpatch.DecodePatch(body)
					require.NoError(t, e)
					next, e := patch.Apply(before)
					require.Error(t, e)
					require.Empty(t, next)
					w.WriteHeader(422)
					status := apierrors.NewGenericServerResponse(422, "", schema.GroupResource{}, "", "PRIVATE_PATCH_TEST", 0, false).ErrStatus
					status.TypeMeta = metav1.TypeMeta{Kind: "Status", APIVersion: "v1"}
					require.NoError(t, json.NewEncoder(w).Encode(status))
				}))
				defer server.Close()
				var out, stderr bytes.Buffer
				cmd := newCmdWithClock(newWireFactory(t, server, rt), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, canaryClock)
				cmd.SilenceErrors, cmd.SilenceUsage = true, true
				cmd.SetArgs([]string{action, "chat", "--yes"})
				err = cmd.Execute()
				require.Error(t, err)
				require.Equal(t, 3, exitcode.FromError(err))
				require.Equal(t, 1, patches)
				require.Equal(t, 2, reads)
				require.Empty(t, out.String())
				require.NotContains(t, err.Error()+stderr.String(), "PRIVATE_PATCH_TEST")
				after, e := json.Marshal(changed)
				require.NoError(t, e)
				require.Equal(t, before, after)
			})
		}
	}
}

func TestCanaryActionRefusalsAndUnknownOutcomeNeverReplay(t *testing.T) {
	for _, scenario := range []string{"ordinary analysis", "missing yes override", "non-tty", "preview failure", "stale child", "wrong owner", "wrong stamp", "mailbox false", "analysis invalid", "forbidden", "admission invalid", "malformed response", "unbound response", "oversized response", "result writer failure", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			analysis := scenario == "ordinary analysis" || scenario == "missing yes override" || scenario == "analysis invalid"
			v, rt, ir := canaryActionFixture(t, analysis)
			patches, reads := 0, 0
			switch scenario {
			case "stale child":
				ir.Status.ObservedGeneration = 1
			case "wrong owner":
				ir.OwnerReferences[0].UID = "other"
			case "wrong stamp":
				ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = "6"
			case "mailbox false":
				v.Annotations[constants.RolloutPromoteAnnotation] = "false"
			case "analysis invalid":
				v.Status.Canary.LastEvaluationTime = &metav1.Time{Time: canaryNow.Add(time.Hour)}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				if r.Method != "PATCH" {
					reads++
					serveCanaryRead(t, w, r, v, ir)
					return
				}
				patches++
				switch scenario {
				case "forbidden", "admission invalid":
					code := 403
					reason := metav1.StatusReasonForbidden
					if scenario == "admission invalid" {
						code = 422
						reason = metav1.StatusReasonInvalid
					}
					w.WriteHeader(code)
					require.NoError(t, json.NewEncoder(w).Encode(metav1.Status{Status: "Failure", Code: int32(code), Reason: reason, Message: "PRIVATE_API_MESSAGE"}))
				case "malformed response":
					_, _ = w.Write([]byte(`{"metadata":`))
				case "unbound response":
					got := v.DeepCopy()
					got.UID = "other"
					require.NoError(t, json.NewEncoder(w).Encode(got))
				case "oversized response":
					_, _ = w.Write([]byte(strings.Repeat("x", 1_048_577)))
				default:
					require.NoError(t, json.NewEncoder(w).Encode(v))
				}
			}))
			defer server.Close()
			var out, stderr bytes.Buffer
			streams := genericiooptions.IOStreams{In: bytes.NewBufferString("yes\n"), Out: &out, ErrOut: &stderr}
			if scenario == "preview failure" {
				streams.ErrOut = privateFailWriter{}
			}
			if scenario == "result writer failure" {
				streams.Out = privateFailWriter{}
			}
			cmd := newCmdWithClock(newWireFactory(t, server, rt), streams, canaryClock)
			cmd.SilenceErrors, cmd.SilenceUsage = true, true
			args := []string{"promote", "chat"}
			if scenario != "non-tty" && scenario != "missing yes override" {
				args = append(args, "--yes")
			}
			if scenario == "missing yes override" || scenario == "analysis invalid" {
				args = append(args, "--override-analysis")
			}
			cmd.SetArgs(args)
			ctx, cancel := context.WithCancel(context.Background())
			if scenario == "canceled" {
				cancel()
			}
			defer cancel()
			err := cmd.ExecuteContext(ctx)
			require.Error(t, err)
			require.Equal(t, 1, exitcode.FromError(err))
			require.Empty(t, out.String())
			require.NotContains(t, err.Error()+stderr.String(), "PRIVATE_")
			require.NotContains(t, err.Error()+stderr.String(), "SECRET_")
			submitted := scenario == "forbidden" || scenario == "admission invalid" || scenario == "malformed response" || scenario == "unbound response" || scenario == "oversized response" || scenario == "result writer failure"
			if submitted {
				require.Equal(t, 1, patches)
			} else {
				require.Zero(t, patches)
			}
			if scenario == "missing yes override" || scenario == "canceled" {
				require.Zero(t, reads)
			}
		})
	}
}
