package quota

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ktesting "k8s.io/client-go/testing"
	api "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	v "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	fake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/yaml"
)

func execute(t *testing.T, client *fake.Clientset, args ...string) (string, string, error) {
	t.Helper()
	var out, stderr bytes.Buffer
	cmd := newTreeCmd(factory.Static{OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, v.ClockFunc(func() time.Time { return time.Date(2026, 9, 15, 12, 0, 0, 0, time.UTC) }), paging.Limits{PageSize: 2, MaxItems: 4, MaxPages: 2, RequestTimeout: time.Second})
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	cmd.SetOut(&out)
	cmd.SetErr(&stderr)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), stderr.String(), err
}

func TestTreeEmptyOutputLiteralAndEquivalentMachineDocuments(t *testing.T) {
	client := fake.NewSimpleClientset()
	out, stderr, err := execute(t, client)
	require.NoError(t, err)
	assert.Empty(t, stderr)
	assert.Equal(t, "QUOTA TREE (advisory)\nDeclared budgets; computed ancestry; advisory only.\nNo admission, enforcement, controller, Kueue, or capacity claim.\nSnapshot: Complete; 0 items / 1 pages; ProblemsDetected\nNo AcceleratorQuotas observed.\n! root: RootMissing (Computed; whole snapshot)\n", out)
	jsonText, stderr, err := execute(t, client, "-o", "json")
	require.NoError(t, err)
	assert.Empty(t, stderr)
	yamlText, stderr, err := execute(t, client, "-o", "yaml")
	require.NoError(t, err)
	assert.Empty(t, stderr)
	var a, b v.Envelope[v.QuotaTreeContent]
	dec := json.NewDecoder(bytes.NewBufferString(jsonText))
	dec.DisallowUnknownFields()
	require.NoError(t, dec.Decode(&a))
	require.NoError(t, yaml.UnmarshalStrict([]byte(yamlText), &b))
	assert.Equal(t, a, b)
	assert.Equal(t, "QuotaTreeReport", a.Kind)
	assert.Equal(t, "Cluster", a.Content.Snapshot.Scope)
	wide, _, err := execute(t, client, "-o", "wide")
	require.NoError(t, err)
	assert.Contains(t, wide, "2026-09-15T12:00:00Z")
	assert.Contains(t, wide, "RootMissing")
	for _, action := range client.Actions() {
		assert.Equal(t, "list", action.GetVerb())
		assert.Equal(t, "acceleratorquotas", action.GetResource().Resource)
		assert.Empty(t, action.GetNamespace())
	}
}

func TestTreeFailsClosedWithFixedErrorReasons(t *testing.T) {
	for _, tt := range []struct {
		name   string
		server error
		want   string
	}{
		{"forbidden", apierrors.NewForbidden(schema.GroupResource{Resource: "acceleratorquotas"}, "secret-name", errors.New("Bearer SECRET")), "Forbidden"},
		{"unauthorized", apierrors.NewUnauthorized("Bearer SECRET"), "Forbidden"},
		{"missing API", apierrors.NewNotFound(schema.GroupResource{Resource: "acceleratorquotas"}, "secret-name"), "UnsupportedAPI"},
		{"timeout", context.DeadlineExceeded, "TimedOut"},
		{"cancel", context.Canceled, "Canceled"},
		{"generic", errors.New("https://secret-user:SECRET@host"), "Unreadable"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			client := fake.NewSimpleClientset()
			client.PrependReactor("list", "acceleratorquotas", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, tt.server })
			for _, format := range []string{"table", "wide", "json", "yaml"} {
				out, stderr, err := execute(t, client, "-o", format)
				require.Error(t, err)
				assert.Equal(t, "AcceleratorQuota snapshot unavailable: "+tt.want, err.Error())
				assert.Empty(t, out)
				assert.Empty(t, stderr)
			}
		})
	}
}

func TestTreeInputValidationAndLimits(t *testing.T) {
	for _, args := range [][]string{{"-o", "secret\nformat"}, {"BAD_NAME"}, {"a", "b"}} {
		client := fake.NewSimpleClientset()
		out, _, err := execute(t, client, args...)
		require.Error(t, err)
		assert.Empty(t, out)
		assert.Empty(t, client.Actions())
		assert.NotContains(t, err.Error(), "secret")
	}
	client := fake.NewSimpleClientset()
	calls := 0
	client.PrependReactor("list", "acceleratorquotas", func(action ktesting.Action) (bool, runtime.Object, error) {
		calls++
		opts := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		assert.Equal(t, int64(2), opts.Limit)
		return true, &api.AcceleratorQuotaList{ListMeta: metav1.ListMeta{Continue: strconv.Itoa(calls)}}, nil
	})
	out, _, err := execute(t, client)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "incomplete")
	assert.Empty(t, out)
	assert.Equal(t, 2, calls)
}

func TestTreeHelpAndFactoryFailure(t *testing.T) {
	var out bytes.Buffer
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: &out, ErrOut: &out})
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"tree", "--help"})
	require.NoError(t, cmd.Execute())
	assert.Contains(t, out.String(), "ClusterQueue leaf is its own tenant")
	assert.Contains(t, out.String(), "80 columns")
	out.Reset()
	cmd = NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: &out, ErrOut: &out})
	cmd.SilenceUsage = true
	cmd.SilenceErrors = true
	cmd.SetArgs([]string{"tree"})
	err := cmd.Execute()
	require.EqualError(t, err, "AcceleratorQuota client unavailable")
	assert.Empty(t, out.String())
}

func TestNamedTreeUsesCompletePaginatedSnapshotAndIgnoresStatus(t *testing.T) {
	client := fake.NewSimpleClientset()
	calls := 0
	root := api.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: "root", Generation: 4}, Spec: api.AcceleratorQuotaSpec{Role: api.AcceleratorQuotaRoleCohort}}
	team := api.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: "team", Generation: 5, Annotations: map[string]string{"token": "SECRET"}}, Spec: api.AcceleratorQuotaSpec{Role: api.AcceleratorQuotaRoleClusterQueue, ParentRef: &api.AcceleratorQuotaParentRef{Name: "root"}, Budgets: []api.AcceleratorBudget{{ResourceName: "nvidia.com/gpu", ResourceFlavor: "a100", Nominal: resource.MustParse("2")}}}}
	team.Status.ObservedGeneration = 1
	team.Status.Conditions = []metav1.Condition{{Message: "SECRET", Reason: "SECRET"}}
	client.PrependReactor("list", "acceleratorquotas", func(action ktesting.Action) (bool, runtime.Object, error) {
		calls++
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		assert.Empty(t, options.FieldSelector)
		assert.Empty(t, options.LabelSelector)
		if calls == 1 {
			assert.Empty(t, options.Continue)
			return true, &api.AcceleratorQuotaList{Items: []api.AcceleratorQuota{team}, ListMeta: metav1.ListMeta{Continue: "next"}}, nil
		}
		assert.Equal(t, "next", options.Continue)
		return true, &api.AcceleratorQuotaList{Items: []api.AcceleratorQuota{root}}, nil
	})
	out, stderr, err := execute(t, client, "team", "-o", "json")
	require.NoError(t, err)
	assert.Empty(t, stderr)
	assert.NotContains(t, out, "SECRET")
	assert.NotContains(t, out, "Reported")
	var decoded v.Envelope[v.QuotaTreeContent]
	require.NoError(t, json.Unmarshal([]byte(out), &decoded))
	assert.Equal(t, 2, calls)
	assert.Equal(t, 2, decoded.Content.Snapshot.ObservedPages)
	require.Len(t, decoded.Content.Nodes, 2)
	assert.Equal(t, "team", decoded.Content.Nodes[1].Tenant)
	assert.Equal(t, "/root/team", decoded.Content.Nodes[1].ComputedPath)
}

func TestCanceledCommandInvalidSnapshotAndWriter(t *testing.T) {
	client := fake.NewSimpleClientset()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := newTreeCmd(factory.Static{OME: client}, genericiooptions.IOStreams{Out: io.Discard, ErrOut: io.Discard}, nil, paging.Limits{RequestTimeout: time.Second})
	cmd.SetContext(ctx)
	cmd.SilenceErrors = true
	cmd.SilenceUsage = true
	require.EqualError(t, cmd.Execute(), "AcceleratorQuota snapshot unavailable: Canceled")
	assert.Empty(t, client.Actions())
	client = fake.NewSimpleClientset(&api.AcceleratorQuota{ObjectMeta: metav1.ObjectMeta{Name: "Bad_Name"}})
	out, stderr, err := execute(t, client)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "invalid")
	assert.Empty(t, out)
	assert.Empty(t, stderr)
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		cmd = newTreeCmd(factory.Static{OME: fake.NewSimpleClientset()}, genericiooptions.IOStreams{Out: failingWriter{}, ErrOut: io.Discard}, nil, paging.Limits{PageSize: 1, MaxItems: 1, MaxPages: 1, RequestTimeout: time.Second})
		cmd.SilenceErrors = true
		cmd.SilenceUsage = true
		cmd.SetArgs([]string{"-o", format})
		assert.ErrorIs(t, cmd.Execute(), io.ErrClosedPipe)
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }
