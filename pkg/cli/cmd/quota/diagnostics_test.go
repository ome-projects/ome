package quota

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"
	api "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	clientset "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	fake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/yaml"
)

func runFamily(t *testing.T, client *fake.Clientset, args ...string) (string, string, error) {
	t.Helper()
	var out, stderr bytes.Buffer
	cmd := NewCmd(factory.Static{OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr})
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), stderr.String(), err
}

const quotaSnapshotWire = `{"apiVersion":"ome.io/v1beta1","kind":"AcceleratorQuotaList","metadata":{},"items":[{"metadata":{"name":"root","generation":7},"spec":{"role":"Cohort","budgets":[{"resourceName":"nvidia.com/gpu","resourceFlavor":"a100","nominal":"8"}]}},{"metadata":{"name":"team","generation":7},"spec":{"role":"ClusterQueue","parentRef":{"name":"root"},"budgets":[{"resourceName":"nvidia.com/gpu","resourceFlavor":"a100","nominal":"2"}]}}]}`
const quotaStatusWire = `{"apiVersion":"ome.io/v1beta1","kind":"AcceleratorQuota","metadata":{"name":"team","generation":7,"uid":"SECRET","resourceVersion":"SECRET","labels":{"token":"SECRET"},"annotations":{"token":"SECRET"}},"status":{"observedGeneration":7,"parent":"root","budgets":[{"resourceName":"nvidia.com/gpu","resourceFlavor":"a100","nominal":"2","admitted":"1","reserved":"2"}],"conditions":[{"type":"Ready","status":"True","observedGeneration":7,"reason":"Admitted","message":"SECRET","lastTransitionTime":"2026-09-15T10:00:00Z"}]}}`

func diagnostic(t *testing.T, f factory.Factory, validate bool, args ...string) (string, string, error) {
	t.Helper()
	var out, stderr bytes.Buffer
	cmd := newDiagnosticCmd(f, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, v.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) }), paging.Limits{PageSize: 2, MaxItems: 4, MaxPages: 2, RequestTimeout: time.Second}, validate)
	cmd.SilenceErrors, cmd.SilenceUsage = true, true
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), stderr.String(), err
}

func TestDiagnosticExactWireReadsAllFormatsAndParity(t *testing.T) {
	for _, validate := range []bool{true, false} {
		t.Run(map[bool]string{true: "validate", false: "status"}[validate], func(t *testing.T) {
			requests := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests++
				assert.Equal(t, "GET", r.Method)
				assert.Empty(t, r.Header.Get("Authorization"))
				w.Header().Set("Content-Type", "application/json")
				if validate {
					assert.Equal(t, "/apis/ome.io/v1beta1/acceleratorquotas", r.URL.Path)
					assert.Equal(t, "2", r.URL.Query().Get("limit"))
					assert.Empty(t, r.URL.Query().Get("fieldSelector"))
					_, _ = io.WriteString(w, quotaSnapshotWire)
				} else {
					assert.Equal(t, "/apis/ome.io/v1beta1/acceleratorquotas/team", r.URL.Path)
					assert.Empty(t, r.URL.RawQuery)
					_, _ = io.WriteString(w, quotaStatusWire)
				}
			}))
			defer server.Close()
			client, err := clientset.NewForConfig(&rest.Config{Host: server.URL})
			require.NoError(t, err)
			var jsonDoc, yamlDoc map[string]any
			for _, format := range []string{"table", "wide", "json", "yaml"} {
				out, stderr, err := diagnostic(t, factory.Static{OME: client}, validate, "team", "-o", format)
				require.NoError(t, err)
				assert.Empty(t, stderr)
				assert.NotContains(t, out, "SECRET")
				if format == "json" {
					decoder := json.NewDecoder(bytes.NewBufferString(out))
					require.NoError(t, decoder.Decode(&jsonDoc))
					assert.ErrorIs(t, decoder.Decode(&struct{}{}), io.EOF)
				}
				if format == "yaml" {
					require.NoError(t, yaml.UnmarshalStrict([]byte(out), &yamlDoc))
				}
			}
			assert.Equal(t, jsonDoc, yamlDoc)
			assert.Equal(t, 4, requests)
		})
	}
}

func TestValidateSelectedTargetStillReportsWholeSnapshotViolation(t *testing.T) {
	client := fake.NewSimpleClientset()
	client.PrependReactor("list", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) {
		var list api.AcceleratorQuotaList
		require.NoError(t, json.Unmarshal([]byte(quotaSnapshotWire), &list))
		orphan := *list.Items[1].DeepCopy()
		orphan.Name = "orphan"
		orphan.Spec.ParentRef.Name = "missing"
		list.Items = append(list.Items, orphan)
		return true, &list, nil
	})
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		out, stderr, err := diagnostic(t, factory.Static{OME: client}, true, "team", "-o", format)
		assert.Equal(t, 2, exitcode.FromError(err))
		assert.Empty(t, stderr)
		assert.Contains(t, out, "ParentMissing")
		assert.Contains(t, out, "orphan")
	}
}

func TestDiagnosticErrorsNeverPrintRawReadMessages(t *testing.T) {
	for _, tt := range []struct {
		server error
		want   string
	}{{apierrors.NewForbidden(schema.GroupResource{Resource: "acceleratorquotas"}, "SECRET", errors.New("Bearer SECRET")), "Forbidden"}, {apierrors.NewUnauthorized("SECRET"), "Forbidden"}, {apierrors.NewNotFound(schema.GroupResource{Resource: "acceleratorquotas"}, "SECRET"), "UnsupportedAPI"}, {context.Canceled, "Canceled"}, {context.DeadlineExceeded, "TimedOut"}, {errors.New("https://SECRET@host"), "Unreadable"}} {
		for _, validate := range []bool{true, false} {
			client := fake.NewSimpleClientset()
			verb := "get"
			if validate {
				verb = "list"
			}
			client.PrependReactor(verb, "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, tt.server })
			out, stderr, err := diagnostic(t, factory.Static{OME: client}, validate, "team", "-o", "json")
			require.Error(t, err)
			assert.Equal(t, 1, exitcode.FromError(err))
			want := tt.want
			if !validate && apierrors.IsNotFound(tt.server) {
				want = "NotFound"
			}
			assert.Contains(t, err.Error(), want)
			if !validate && apierrors.IsNotFound(tt.server) {
				assert.NotContains(t, err.Error(), "UnsupportedAPI")
			}
			assert.NotContains(t, err.Error(), "SECRET")
			assert.Empty(t, out)
			assert.Empty(t, stderr)
		}
	}
}

func TestDiagnosticArgumentsFactoryAndNamedIdentityFailClosed(t *testing.T) {
	for _, validate := range []bool{true, false} {
		for _, args := range [][]string{{"BAD"}, {"a", "b"}, {"-o", "SECRET"}} {
			client := fake.NewSimpleClientset()
			out, stderr, err := diagnostic(t, factory.Static{OME: client}, validate, args...)
			require.Error(t, err)
			assert.Empty(t, out)
			assert.Empty(t, stderr)
			assert.Empty(t, client.Actions())
		}
		out, _, err := diagnostic(t, factory.Static{}, validate)
		require.EqualError(t, err, "AcceleratorQuota client unavailable")
		assert.Empty(t, out)
	}
	client := fake.NewSimpleClientset()
	client.PrependReactor("get", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, &api.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: "other"}}, nil
	})
	out, _, err := diagnostic(t, factory.Static{OME: client}, false, "team")
	require.EqualError(t, err, "AcceleratorQuota target response identity is invalid")
	assert.Empty(t, out)
}

func TestDiagnosticCanceledIncompleteAndWriterFailureNeverClaimSuccess(t *testing.T) {
	for _, validate := range []bool{true, false} {
		client := fake.NewSimpleClientset()
		calls := 0
		client.PrependReactor("list", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) {
			calls++
			return true, &api.AcceleratorQuotaList{ListMeta: metav1.ListMeta{Continue: strconv.Itoa(calls)}}, nil
		})
		out, stderr, err := diagnostic(t, factory.Static{OME: client}, validate, "-o", "json")
		require.Error(t, err)
		assert.Equal(t, 1, exitcode.FromError(err))
		assert.Empty(t, out)
		assert.Empty(t, stderr)
		assert.Equal(t, 2, calls)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		client = fake.NewSimpleClientset()
		cmd := newDiagnosticCmd(factory.Static{OME: client}, genericiooptions.IOStreams{Out: io.Discard, ErrOut: io.Discard}, nil, paging.Limits{RequestTimeout: time.Second}, validate)
		cmd.SetContext(ctx)
		cmd.SilenceErrors, cmd.SilenceUsage = true, true
		assert.Error(t, cmd.Execute())
		assert.Empty(t, client.Actions())
		cmd = newDiagnosticCmd(factory.Static{OME: fake.NewSimpleClientset()}, genericiooptions.IOStreams{Out: failureWriter{}, ErrOut: io.Discard}, nil, paging.Limits{PageSize: 2, MaxItems: 4, MaxPages: 2, RequestTimeout: time.Second}, validate)
		cmd.SilenceErrors, cmd.SilenceUsage = true, true
		err = cmd.Execute()
		require.Error(t, err)
		assert.Equal(t, 1, exitcode.FromError(err))
	}
}

type failureWriter struct{}

func (failureWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

// Missing registration or a success return for an empty tree must fail this
// assertion: a complete negative observation still writes exactly one report.
func TestValidateEmptyWritesNegativeAssertion(t *testing.T) {
	client := fake.NewSimpleClientset()
	out, stderr, err := runFamily(t, client, "validate", "-o", "json")
	assert.Equal(t, 2, exitcode.FromError(err))
	assert.Empty(t, stderr)
	var value struct {
		Kind    string `json:"kind"`
		Content struct {
			Valid bool `json:"valid"`
		} `json:"content"`
	}
	require.NoError(t, json.Unmarshal([]byte(out), &value))
	assert.Equal(t, "QuotaValidationReport", value.Kind)
	assert.False(t, value.Content.Valid)
	require.Len(t, client.Actions(), 1)
	assert.Equal(t, "list", client.Actions()[0].GetVerb())
}

func TestStatusEmptyIsObservationNotValidation(t *testing.T) {
	client := fake.NewSimpleClientset()
	out, stderr, err := runFamily(t, client, "status")
	require.NoError(t, err)
	assert.Empty(t, stderr)
	assert.Equal(t, "QUOTA STATUS (reported)\nReported evidence only; no enforcement or free-capacity claim.\nSnapshot: Complete; 0 items / 1 pages\nNo AcceleratorQuotas observed.\n", out)
	require.Len(t, client.Actions(), 1)
	assert.Equal(t, "list", client.Actions()[0].GetVerb())
}
