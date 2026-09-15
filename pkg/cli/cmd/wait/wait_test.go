package wait

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

type waitFactory struct {
	factory.Static
	config    *rest.Config
	configErr error
	calls     atomic.Int32
}

func (f *waitFactory) RESTConfig() (*rest.Config, error) {
	f.calls.Add(1)
	return f.config, f.configErr
}
func execute(t *testing.T, f factory.Factory, args ...string) (string, string, error) {
	t.Helper()
	var out, stderr bytes.Buffer
	cmd := NewCmd(f, genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &stderr})
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), stderr.String(), err
}
func TestInitialReadySingleReportAllFormats(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				require.Equal(t, "GET", r.Method)
				require.Equal(t, "/apis/ome.io/v1beta1/namespaces/work/inferenceservices/service", r.URL.Path)
				require.Empty(t, r.URL.Query().Get("watch"))
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"work","uid":"PRIVATE-UID","resourceVersion":"PRIVATE-RV","generation":9,"labels":{"token":"PRIVATE-LABEL"}},"status":{"observedGeneration":0,"conditions":[{"type":"Ready","status":"True","reason":"PRIVATE-REASON","message":"password=PRIVATE-MESSAGE"}]}}`)
			}))
			defer server.Close()
			f := &waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}
			out, stderr, err := execute(t, f, "service", "--for=condition=Ready", "-o", format)
			require.NoError(t, err)
			require.Equal(t, 0, exitcode.FromError(err))
			require.Empty(t, stderr)
			require.Equal(t, int32(1), requests.Load())
			require.NotContains(t, out, "PRIVATE")
			require.NotContains(t, out, server.URL)
			if format == "json" || format == "yaml" {
				data := []byte(out)
				if format == "yaml" {
					data, err = yaml.YAMLToJSONStrict(data)
					require.NoError(t, err)
				}
				d := json.NewDecoder(bytes.NewReader(data))
				d.DisallowUnknownFields()
				var r reportv1alpha1.WaitReport
				require.NoError(t, d.Decode(&r))
				require.Equal(t, "Matched", string(r.Content.Outcome))
				require.Equal(t, "Unverifiable", r.Content.Observed.GenerationFreshness)
				require.Equal(t, 1, r.Content.Counts.Gets)
				require.Zero(t, r.Content.Counts.Watches)
				var extra any
				require.Equal(t, io.EOF, d.Decode(&extra))
			} else {
				require.Contains(t, out, "Matched")
				require.Contains(t, out, "Unverifiable")
			}
		})
	}
}
func TestInvalidArgumentsDoNotResolveConfig(t *testing.T) {
	for _, args := range [][]string{{"service"}, {"service", "--for=condition=Other"}, {"service", "--for=condition=Ready=true"}, {"service", "--for=condition=Ready=False=Unknown"}, {"service", "--for=jsonpath=x"}, {"../PRIVATE", "--for=condition=Ready"}, {"service", "--for=condition=Ready", "--timeout=0s"}, {"service", "--for=condition=Ready", "--timeout=25h"}, {"service", "--for=condition=Ready", "-o", "PRIVATE"}, {"service", "other", "--for=condition=Ready"}} {
		f := &waitFactory{Static: factory.Static{NS: "work"}, configErr: errors.New("PRIVATE")}
		out, _, err := execute(t, f, args...)
		require.Error(t, err)
		require.Equal(t, 1, exitcode.FromError(err))
		require.Empty(t, out)
		require.Zero(t, f.calls.Load())
	}
}

func TestFlagParsingErrorsAreClosedAndDoNotResolveConfig(t *testing.T) {
	const credential = "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	for _, tc := range []struct {
		name string
		flag string
	}{
		{name: "malformed timeout", flag: "--timeout=" + credential},
		{name: "missing timeout", flag: "--timeout"},
		{name: "unknown flag", flag: "--" + credential},
		{name: "malformed help", flag: "--help=" + credential},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &waitFactory{Static: factory.Static{NS: "work"}}
			out, stderr, err := execute(t, f, "service", "--for=condition=Ready", tc.flag)
			require.Error(t, err)
			require.Equal(t, 1, exitcode.FromError(err))
			require.Empty(t, out)
			require.Empty(t, stderr)
			require.NotContains(t, err.Error(), credential)
			require.EqualError(t, err, "InvalidWaitFlags")
			require.Zero(t, f.calls.Load())
		})
	}
}
func TestNotFoundUnmetAndAcquisitionFailureNoReport(t *testing.T) {
	for _, status := range []int{404, 401, 403, 429, 500} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				w.WriteHeader(status)
				_, _ = io.WriteString(w, `{"apiVersion":"v1","kind":"Status","status":"Failure","code":`+stringStatus(status)+`,"message":"PRIVATE Bearer credential"}`)
			}))
			defer server.Close()
			f := &waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}
			out, _, err := execute(t, f, "service", "--for=condition=Ready", "-o", "json")
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE")
			require.NotContains(t, err.Error(), server.URL)
			if status == 404 {
				require.Equal(t, 2, exitcode.FromError(err))
				require.Contains(t, out, "NotFound")
				require.Contains(t, out, "NotRecorded")
			} else {
				require.Equal(t, 1, exitcode.FromError(err))
				require.Empty(t, out)
			}
		})
	}
}
func stringStatus(status int) string {
	switch status {
	case 404:
		return "404"
	case 401:
		return "401"
	case 403:
		return "403"
	case 429:
		return "429"
	default:
		return "500"
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("PRIVATE writer error") }
func TestCanceledAndOutputFailureAreGeneral(t *testing.T) {
	f := &waitFactory{Static: factory.Static{NS: "work"}, configErr: errors.New("PRIVATE")}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := NewCmd(f, genericiooptions.IOStreams{Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}})
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"service", "--for=condition=Ready"})
	err := cmd.Execute()
	require.Equal(t, 1, exitcode.FromError(err))
	require.NotContains(t, err.Error(), "PRIVATE")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"work","uid":"uid","resourceVersion":"rv"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}`)
	}))
	defer server.Close()
	f.configErr = nil
	f.config = &rest.Config{Host: server.URL}
	cmd = NewCmd(f, genericiooptions.IOStreams{Out: brokenWriter{}, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"service", "--for=condition=Ready", "-o", "json"})
	err = cmd.Execute()
	require.Equal(t, 1, exitcode.FromError(err))
	require.NotContains(t, err.Error(), "PRIVATE")
}
func TestHelpExplainsExplicitReportedCondition(t *testing.T) {
	out, _, err := execute(t, &waitFactory{}, "--help")
	require.NoError(t, err)
	for _, fact := range []string{"condition=Ready", "Unverifiable", "reported", "cooperative", "24h"} {
		require.True(t, strings.Contains(out, fact), "missing %s", fact)
	}
}

func TestExplicitFalseAndUnknownInitialMatch(t *testing.T) {
	for _, status := range []string{"False", "Unknown"} {
		t.Run(status, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"work","uid":"uid","resourceVersion":"rv"},"status":{"conditions":[{"type":"Ready","status":"`+status+`"}]}}`)
			}))
			defer server.Close()
			f := &waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}
			out, stderr, err := execute(t, f, "service", "--for=condition=Ready="+status, "-o", "json")
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.Contains(t, out, `"outcome": "Matched"`)
			require.Contains(t, out, `"status": "`+status+`"`)
		})
	}
}
func TestMissingReadyIsNotExplicitUnknownAndTimeoutIsUnmet(t *testing.T) {
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	watchOpened := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Query().Get("watch") == "true" {
			w.WriteHeader(200)
			w.(http.Flusher).Flush()
			close(watchOpened)
			<-r.Context().Done()
			return
		}
		_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"work","uid":"uid","resourceVersion":"rv"},"status":{}}`)
	}))
	defer server.Close()
	f := &waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}
	var out, stderr bytes.Buffer
	cmd := newCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"service", "--for=condition=Ready=Unknown", "-o", "json"})
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	select {
	case <-watchOpened:
	case <-time.After(time.Second):
		t.Fatal("missing Ready did not open watch")
	}
	clk.Step(time.Minute)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("command timeout not observed")
	}
	require.Empty(t, stderr.String())
	require.Contains(t, out.String(), `"outcome": "TimedOut"`)
	require.Contains(t, out.String(), `"status": "NotRecorded"`)
	require.NotContains(t, out.String(), `"status": "Unknown"`)
}
func TestWholeInspectionWarningAppearsInEveryFormat(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"service","namespace":"work","uid":"uid","resourceVersion":"rv"},"status":{"conditions":[{"type":"Ready","status":"True"},{"type":"Other","status":"False","message":"`+strings.Repeat("PRIVATE", 600)+`"}]}}`)
			}))
			defer server.Close()
			f := &waitFactory{Static: factory.Static{NS: "work"}, config: &rest.Config{Host: server.URL}}
			out, stderr, err := execute(t, f, "service", "--for=condition=Ready", "-o", format)
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.Contains(t, out, "OversizedConditionRecord")
			require.Contains(t, out, "Partial")
			require.Contains(t, out, "Matched")
			require.NotContains(t, out, "PRIVATE")
		})
	}
}
func TestConfigurationAndIdentityErrorsStayGeneralAndPrivate(t *testing.T) {
	for _, f := range []*waitFactory{{Static: factory.Static{NS: "work"}, configErr: errors.New("PRIVATE")}, {Static: factory.Static{NS: "../PRIVATE"}, config: &rest.Config{Host: "http://localhost"}}, {Static: factory.Static{NS: "work"}, config: &rest.Config{Host: "http://[PRIVATE"}}} {
		out, _, err := execute(t, f, "service", "--for=condition=Ready")
		require.Equal(t, 1, exitcode.FromError(err))
		require.Empty(t, out)
		require.NotContains(t, err.Error(), "PRIVATE")
	}
}

func TestRealWireCredentialShapedMetadataNeverReachesOutput(t *testing.T) {
	const credential = "sk-proj-0123456789abcdefghijklmnopqrstuvwxyz"
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "GET", r.Method)
				require.Equal(t, "/apis/ome.io/v1beta1/namespaces/"+credential+"/inferenceservices/"+credential, r.URL.Path)
				w.Header().Set("Content-Type", "application/json")
				_, _ = io.WriteString(w, `{"apiVersion":"ome.io/v1beta1","kind":"InferenceService","metadata":{"name":"`+credential+`","namespace":"`+credential+`","uid":"uid","resourceVersion":"rv"},"status":{"conditions":[{"type":"Ready","status":"True"}]}}`)
			}))
			defer server.Close()
			f := &waitFactory{Static: factory.Static{NS: credential}, config: &rest.Config{Host: server.URL}}
			out, stderr, err := execute(t, f, credential, "--for=condition=Ready", "-o", format)
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.NotContains(t, out, credential)
			require.Contains(t, out, "[REDACTED]")
		})
	}
}
