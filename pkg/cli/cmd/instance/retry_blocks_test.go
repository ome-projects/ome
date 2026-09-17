package instance

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ktesting "k8s.io/client-go/testing"

	omev1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	"sigs.k8s.io/ome/pkg/cli/retryblockprojection"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func TestRetryBlocksReadsExactParentAndBoundedRelatedReplicas(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 3, Reason: "image pull failed",
	}}
	client := omefake.NewSimpleClientset(isvc, ir)
	deps := retryBlocksDependencies{
		clock:          commandClock,
		limits:         paging.Limits{PageSize: 2, MaxItems: 9, MaxPages: 5, RequestTimeout: time.Second},
		maxRetryBlocks: 100,
	}

	out, err := executeRetryBlocks(
		t, factory.Static{OME: client, NS: "prod"}, deps,
		"chat", "--component", "engine",
	)

	require.NoError(t, err)
	assert.Equal(t,
		"COMP     STATE   TARGET           ATT   NEXT   REL   REASON\n"+
			"engine   HELD    chat-#a29b545d   3     -      YES   image pull...\n",
		out,
	)
	require.Len(t, client.Actions(), 2)
	get := client.Actions()[0].(ktesting.GetAction)
	assert.Equal(t, "prod", get.GetNamespace())
	assert.Equal(t, "chat", get.GetName())
	list := client.Actions()[1].(ktesting.ListAction)
	assert.Equal(t, "prod", list.GetNamespace())
	value, found := list.GetListRestrictions().Labels.RequiresExactMatch(constants.InferenceServiceLabel)
	assert.True(t, found)
	assert.Equal(t, "chat", value)
	options := client.Actions()[1].(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
	assert.Equal(t, int64(2), options.Limit)
}

func TestRetryBlocksKeepsBlocksWhenColumnarRowsExceedRowBudget(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, omev1beta1.OMENativeInstanceStatus{
		Index: 1, Phase: omev1beta1.OMENativeInstanceReady,
	})
	columns, err := irstatus.EncodeColumns(ir.Status.InstanceStatuses, 2)
	require.NoError(t, err)
	encoding := omev1beta1.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatusEncoding = &encoding
	ir.Status.InstanceStatusColumns = columns
	ir.Status.InstanceStatuses = nil
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 3, Reason: "image pull failed",
	}}

	out, err := executeRetryBlocks(t, factory.Static{
		OME: omefake.NewSimpleClientset(isvc, ir), NS: "prod",
	}, retryCommandDependencies(), "chat", "--component", "engine")
	require.NoError(t, err)
	assert.Contains(t, out, "engine   HELD")
	assert.NotContains(t, out, "UNAVAILABLE")
}

func TestRetryBlocksMalformedColumnarCannotClaimReleaseEligible(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	columns, err := irstatus.EncodeColumns(ir.Status.InstanceStatuses, 1)
	require.NoError(t, err)
	columns.Phases = append(columns.Phases, omev1beta1.InstanceStatusPhaseGroup{
		Value: omev1beta1.OMENativeInstanceUpdating, Indexes: "0",
	})
	encoding := omev1beta1.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatusEncoding = &encoding
	ir.Status.InstanceStatusColumns = columns
	ir.Status.InstanceStatuses = nil
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 3, Reason: "image pull failed",
	}}

	out, err := executeRetryBlocks(t, factory.Static{
		OME: omefake.NewSimpleClientset(isvc, ir), NS: "prod",
	}, retryCommandDependencies(), "chat", "--component", "engine")
	require.NoError(t, err)
	assert.Contains(t, out, "COLL_UNAV")
	assert.NotContains(t, out, " YES ")
}

func TestRetryBlocksWritesTypedJSONAndYAML(t *testing.T) {
	t.Parallel()

	for _, format := range []string{"json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			t.Parallel()
			isvc := commandISVC()
			ir := commandIR(isvc)
			ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
				TargetRevision: "chat-engine-complete-revision-aaaaaaaa",
				State:          omev1beta1.RetryBlockHeld, AttemptsStarted: 3,
			}}
			client := omefake.NewSimpleClientset(isvc, ir)

			out, err := executeRetryBlocks(
				t, factory.Static{OME: client, NS: "prod"}, retryCommandDependencies(),
				"chat", "--component", "engine", "--output", format,
			)

			require.NoError(t, err)
			assert.Contains(t, out, "InstanceRetryBlocksReport")
			assert.Contains(t, out, "chat-engine-complete-revision-aaaaaaaa")
			assert.Contains(t, out, "releaseEligible")
			if format == "json" {
				var decoded map[string]any
				require.NoError(t, json.Unmarshal([]byte(out), &decoded))
				assert.Equal(t, "InstanceRetryBlocksReport", decoded["kind"])
			}
		})
	}
}

func TestRetryBlocksSupportsLongParentNamesWithoutInvalidLabelSelector(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	isvc.Name = strings.Repeat("a", 64)
	ir := commandIR(isvc)
	ir.Name = "engine"
	ir.UID = "engine-uid"
	ir.Spec.ParentRef.Name = isvc.Name
	ir.OwnerReferences[0].Name = isvc.Name
	delete(ir.Labels, constants.InferenceServiceLabel)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld, AttemptsStarted: 2,
	}}
	otherISVC := commandISVC()
	otherISVC.Name = "other"
	otherISVC.UID = "other-uid"
	unrelated := commandIR(otherISVC)
	unrelated.Name = "other-engine"
	unrelated.UID = "other-engine-uid"
	client := omefake.NewSimpleClientset(isvc, ir, unrelated)

	out, err := executeRetryBlocks(
		t, factory.Static{OME: client, NS: "prod"}, retryCommandDependencies(),
		isvc.Name, "--component", "engine",
	)

	require.NoError(t, err)
	assert.Contains(t, out, "HELD")
	assert.Contains(t, out, "YES")
	assert.NotContains(t, out, "ID_REJECT")
	require.Len(t, client.Actions(), 2)
	options := client.Actions()[1].(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
	assert.Empty(t, options.LabelSelector)
}

func TestRetryBlocksLongParentExactOwnerUIDClaimFailsClosed(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	isvc.Name = strings.Repeat("a", 64)
	ir := commandIR(isvc)
	ir.Name = "engine"
	ir.UID = "engine-uid"
	ir.Spec.ParentRef.Name = isvc.Name
	ir.OwnerReferences[0].Name = isvc.Name
	delete(ir.Labels, constants.InferenceServiceLabel)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld, AttemptsStarted: 2,
	}}
	otherISVC := commandISVC()
	otherISVC.Name = "other"
	otherISVC.UID = "other-uid"
	suspicious := commandIR(otherISVC)
	suspicious.Name = "suspicious-engine"
	suspicious.UID = "suspicious-engine-uid"
	suspicious.OwnerReferences[0].APIVersion = "malformed/v1"
	suspicious.OwnerReferences[0].Kind = "Other"
	suspicious.OwnerReferences[0].Name = "other"
	suspicious.OwnerReferences[0].UID = isvc.UID
	client := omefake.NewSimpleClientset(isvc, ir, suspicious)

	out, err := executeRetryBlocks(
		t, factory.Static{OME: client, NS: "prod"}, retryCommandDependencies(),
		isvc.Name, "--component", "engine", "-o", "json",
	)

	require.NoError(t, err)
	assert.Contains(t, out, `"targetRevision": "engine-aaaaaaaa"`)
	assert.Contains(t, out, `"releaseEligible": false`)
	assert.Contains(t, out, `"code": "IdentityRejected"`)
	assert.NotContains(t, out, `"releaseEligible": true`)
}

func TestRetryBlocksReturnsPrimaryAcquisitionAndIdentityErrors(t *testing.T) {
	t.Parallel()

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		client := omefake.NewSimpleClientset()
		out, err := executeRetryBlocks(
			t, factory.Static{OME: client, NS: "prod"}, retryCommandDependencies(),
			"chat", "--component", "engine",
		)
		require.Error(t, err)
		assert.True(t, apierrors.IsNotFound(err))
		assert.Contains(t, err.Error(), `get InferenceService "prod/chat"`)
		assert.Empty(t, out)
	})

	tests := []struct {
		name   string
		object *omev1beta1.InferenceService
		want   error
	}{
		{name: "nil", want: ErrReturnedInferenceServiceNil},
		{
			name: "name mismatch",
			object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "other", Namespace: "prod", UID: "uid",
			}},
			want: ErrReturnedInferenceServiceNameMismatch,
		},
		{
			name: "namespace mismatch",
			object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "chat", Namespace: "other", UID: "uid",
			}},
			want: ErrReturnedInferenceServiceNamespaceMismatch,
		},
		{
			name: "missing uid",
			object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "chat", Namespace: "prod",
			}},
			want: ErrReturnedInferenceServiceUIDMissing,
		},
		{
			name: "unsafe uid",
			object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "chat", Namespace: "prod", UID: "uid\u202e\nSECRET",
			}},
			want: ErrReturnedInferenceServiceUIDInvalid,
		},
		{
			name: "oversized uid",
			object: &omev1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{
				Name: "chat", Namespace: "prod", UID: types.UID(strings.Repeat("x", 129)),
			}},
			want: ErrReturnedInferenceServiceUIDInvalid,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			client := omefake.NewSimpleClientset()
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				if test.object == nil {
					var nilISVC *omev1beta1.InferenceService
					return true, nilISVC, nil
				}
				return true, test.object.DeepCopy(), nil
			})

			out, err := executeRetryBlocks(
				t, factory.Static{OME: client, NS: "prod"}, retryCommandDependencies(),
				"chat", "--component", "engine",
			)

			require.ErrorIs(t, err, test.want)
			assert.Empty(t, out)
			require.Len(t, client.Actions(), 1)
		})
	}
}

func TestRetryBlocksHonorsParentReadTimeout(t *testing.T) {
	t.Parallel()

	client := omefake.NewSimpleClientset()
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		time.Sleep(10 * time.Millisecond)
		return true, commandISVC(), nil
	})
	deps := retryCommandDependencies()
	deps.limits.RequestTimeout = time.Millisecond

	out, err := executeRetryBlocks(
		t, factory.Static{OME: client, NS: "prod"}, deps,
		"chat", "--component", "engine",
	)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, out)
	require.Len(t, client.Actions(), 1)
}

func TestRetryBlocksValidatesArgumentsBeforeFactoryAccess(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "missing name", args: []string{"--component", "engine"}, want: "accepts 1 arg(s), received 0"},
		{name: "extra name", args: []string{"chat", "other", "--component", "engine"}, want: "accepts 1 arg(s), received 2"},
		{name: "missing component", args: []string{"chat"}, want: "--component is required"},
		{name: "invalid component", args: []string{"chat", "--component", "predictor"}, want: "component must be engine, decoder, or router"},
		{name: "invalid name", args: []string{"Bad_Name", "--component", "engine"}, want: ErrInvalidInferenceServiceName.Error()},
		{name: "unsupported output", args: []string{"chat", "--component", "engine", "-o", "wide"}, want: `unsupported output format "wide"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := executeRetryBlocks(t, panicFactory{}, retryCommandDependencies(), test.args...)
			require.Error(t, err)
			assert.Contains(t, err.Error(), test.want)
		})
	}
}

func TestRetryBlocksSurfacesCollectionFailureAsTypedUnavailableEvidence(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	client := omefake.NewSimpleClientset(isvc)
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"},
			"", errors.New("SECRET denied"),
		)
	})

	out, err := executeRetryBlocks(
		t, factory.Static{OME: client, NS: "prod"}, retryCommandDependencies(),
		"chat", "--component", "engine", "-o", "json",
	)

	require.NoError(t, err)
	assert.Contains(t, out, `"state": "Unavailable"`)
	assert.Contains(t, out, `"code": "CollectionUnavailable"`)
	assert.Contains(t, out, `"unavailableReason": "Forbidden"`)
	assert.NotContains(t, out, "SECRET")
}

func TestRetryBlocksReportsInternalListTimeoutAsTypedUnavailableEvidence(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 3,
	}}
	client := omefake.NewSimpleClientset(isvc)
	client.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		if options.Continue == "" {
			return true, &omev1beta1.InferenceReplicaList{
				ListMeta: metav1.ListMeta{Continue: "next"},
				Items:    []omev1beta1.InferenceReplica{*ir},
			}, nil
		}
		time.Sleep(50 * time.Millisecond)
		return true, &omev1beta1.InferenceReplicaList{}, nil
	})
	deps := retryCommandDependencies()
	deps.limits.RequestTimeout = 20 * time.Millisecond

	out, err := executeRetryBlocks(
		t, factory.Static{OME: client, NS: "prod"}, deps,
		"chat", "--component", "engine", "-o", "json",
	)

	require.NoError(t, err)
	assert.Contains(t, out, `"state": "Partial"`)
	assert.Contains(t, out, `"targetRevision": "chat-engine-aaaaaaaa"`)
	assert.Contains(t, out, `"releaseEligible": false`)
	assert.Contains(t, out, `"code": "CollectionUnavailable"`)
	assert.Contains(t, out, `"unavailableReason": "Unreadable"`)
}

func TestRetryBlocksPreservesPartialBlocksButWithholdsReleaseEligibility(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	ir.Status.RetryBlocks = []omev1beta1.RetryBlock{{
		TargetRevision: "chat-engine-aaaaaaaa", State: omev1beta1.RetryBlockHeld,
		AttemptsStarted: 3,
	}}
	client := omefake.NewSimpleClientset(isvc)
	client.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		options := action.(interface{ GetListOptions() metav1.ListOptions }).GetListOptions()
		if options.Continue == "" {
			return true, &omev1beta1.InferenceReplicaList{
				ListMeta: metav1.ListMeta{Continue: "next"},
				Items:    []omev1beta1.InferenceReplica{*ir},
			}, nil
		}
		return true, nil, apierrors.NewForbidden(
			schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"},
			"", errors.New("SECRET denied"),
		)
	})
	deps := retryCommandDependencies()
	deps.limits.PageSize = 1

	out, err := executeRetryBlocks(
		t, factory.Static{OME: client, NS: "prod"}, deps,
		"chat", "--component", "engine", "-o", "json",
	)

	require.NoError(t, err)
	assert.Contains(t, out, `"state": "Partial"`)
	assert.Contains(t, out, `"targetRevision": "chat-engine-aaaaaaaa"`)
	assert.Contains(t, out, `"releaseEligible": false`)
	assert.Contains(t, out, `"unavailableReason": "Forbidden"`)
	assert.NotContains(t, out, "SECRET")
}

func TestRetryBlocksSurfacesNestedWorkLimitWithoutScanningBlocks(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	ir := commandIR(isvc)
	ir.Status.RetryBlocks = make([]omev1beta1.RetryBlock, 10_000)
	for index := range ir.Status.RetryBlocks {
		ir.Status.RetryBlocks[index] = omev1beta1.RetryBlock{
			TargetRevision: "chat-engine-oversized", State: omev1beta1.RetryBlockHeld,
			AttemptsStarted: 3, Reason: "SECRET-UNBOUNDED-REASON",
		}
	}
	deps := retryCommandDependencies()
	deps.maxRetryBlocks = 1

	out, err := executeRetryBlocks(
		t, factory.Static{OME: omefake.NewSimpleClientset(isvc, ir), NS: "prod"}, deps,
		"chat", "--component", "engine", "-o", "json",
	)

	require.NoError(t, err)
	assert.Contains(t, out, `"code": "RetryBlocksTruncated"`)
	assert.Contains(t, out, `"truncated": true`)
	assert.NotContains(t, out, "SECRET-UNBOUNDED-REASON")
}

func TestRetryBlocksDoesNotSwallowCallerCancellation(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	client := omefake.NewSimpleClientset(isvc)
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, context.Canceled
	})

	_, err := executeRetryBlocks(
		t, factory.Static{OME: client, NS: "prod"}, retryCommandDependencies(),
		"chat", "--component", "engine",
	)

	assert.ErrorIs(t, err, context.Canceled)
}

func TestRetryBlocksDoesNotSwallowCallerDeadline(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	client := omefake.NewSimpleClientset(isvc)
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		time.Sleep(50 * time.Millisecond)
		return true, &omev1beta1.InferenceReplicaList{}, nil
	})
	deps := retryCommandDependencies()
	deps.limits.RequestTimeout = time.Second
	var out bytes.Buffer
	cmd := newRetryBlocksCmd(
		factory.Static{OME: client, NS: "prod"},
		genericiooptions.IOStreams{Out: &out},
		deps,
	)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	t.Cleanup(cancel)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"chat", "--component", "engine"})

	err := cmd.Execute()

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Empty(t, out.String())
}

func TestRetryBlocksReturnsWriterErrors(t *testing.T) {
	t.Parallel()

	isvc := commandISVC()
	want := errors.New("writer failed")
	cmd := newRetryBlocksCmd(
		factory.Static{OME: omefake.NewSimpleClientset(isvc, commandIR(isvc)), NS: "prod"},
		genericiooptions.IOStreams{Out: retryErrorWriter{err: want}}, retryCommandDependencies(),
	)
	cmd.SetArgs([]string{"chat", "--component", "engine"})

	err := cmd.Execute()

	require.ErrorIs(t, err, want)
	assert.Contains(t, err.Error(), "write instance retry blocks")
}

func TestRetryBlocksHelpDefinesOperationalColumnsAndSafety(t *testing.T) {
	t.Parallel()

	cmd := newRetryBlocksCmd(panicFactory{}, genericiooptions.IOStreams{}, retryCommandDependencies())
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"--help"})
	require.NoError(t, cmd.Execute())

	help := out.String()
	for _, want := range []string{
		"collection evidence is complete",
		"current and valid; release-held may submit that exact target revision",
		"ATT counts lifecycle attempts",
		"NEXT uses compact UTC",
		"does not patch or release",
		"--component",
		"table, json or yaml",
		"COLL_TRUNC=CollectionTruncated",
	} {
		assert.Contains(t, help, want)
	}
}

func TestInstanceCommandRegistersRetryBlocks(t *testing.T) {
	t.Parallel()

	cmd := NewCmd(panicFactory{}, genericiooptions.IOStreams{})
	command, _, err := cmd.Find([]string{"retry-blocks"})

	require.NoError(t, err)
	assert.Equal(t, "retry-blocks", command.Name())
}

func executeRetryBlocks(
	t *testing.T,
	f factory.Factory,
	deps retryBlocksDependencies,
	args ...string,
) (string, error) {
	t.Helper()
	var out bytes.Buffer
	cmd := newRetryBlocksCmd(
		f,
		genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &bytes.Buffer{}},
		deps,
	)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func retryCommandDependencies() retryBlocksDependencies {
	return retryBlocksDependencies{
		clock:          commandClock,
		limits:         paging.Limits{PageSize: 50, MaxItems: 100, MaxPages: 10, RequestTimeout: time.Second},
		maxRetryBlocks: 1000,
		project:        retryblockprojection.Project,
	}
}

type retryErrorWriter struct{ err error }

func (w retryErrorWriter) Write([]byte) (int, error) { return 0, w.err }

var _ io.Writer = retryErrorWriter{}
