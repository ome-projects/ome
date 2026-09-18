package wait

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	ktesting "k8s.io/client-go/testing"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/yaml"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func scaleCurrentFixture() (*ome.InferenceService, *ome.InferenceReplica) {
	controller := true
	count := int32(2)
	parent := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("parent-uid"), ResourceVersion: "17", Generation: 7,
	}}
	parent.Status.Components = map[ome.ComponentType]ome.ComponentStatusSpec{ome.EngineComponent: {
		ScaleTargetRef: &ome.ScaleTargetRef{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: "custom-engine"},
	}}
	ir := &ome.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-engine", Namespace: "prod", UID: types.UID("ir-uid"), ResourceVersion: "31", Generation: 2,
			Annotations:     map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "7"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: parent.UID, Controller: &controller}},
		},
		Spec: ome.InferenceReplicaSpec{ParentRef: ome.ParentReference{Name: "chat"}, Component: ome.EngineComponent, Replicas: &count},
		Status: ome.InferenceReplicaStatus{ObservedGeneration: 2, Replicas: 2, ReadyReplicas: 1,
			InstanceStatuses: []ome.OMENativeInstanceStatus{{Index: 0, Phase: ome.OMENativeInstanceReady}, {Index: 1, Phase: ome.OMENativeInstanceCreating}}},
	}
	return parent, ir
}

func TestScaleCurrentWaitRendersExactLogicalCountInAllFormats(t *testing.T) {
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			parent, ir := scaleCurrentFixture()
			client := omefake.NewSimpleClientset(parent, ir)
			client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
				t.Fatal("scale wait must not LIST sibling replicas")
				return true, nil, nil
			})
			out, stderr, err := execute(t, exactReplicaWaitFactory(t, client),
				"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "-o", format)
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.Len(t, client.Actions(), 3) // parent, exact IR, parent refresh
			require.Equal(t, "custom-engine", client.Actions()[1].(ktesting.GetAction).GetName())
			t.Logf("kubectl ome wait chat --for=replicas=current --component=engine --replicas=2 -n prod -o %s (synthetic fixture):\n%s", format, out)
			if format == "table" || format == "wide" {
				require.Contains(t, out, "Matched")
				require.Contains(t, out, "2")
				require.Contains(t, out, "Ready")
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
			require.Equal(t, int32(2), *value.Content.Scale.CurrentReplicas)
			require.Equal(t, int32(1), *value.Content.Scale.ReadyReplicas)
			require.Equal(t, "DenseV1", value.Content.Scale.Encoding)
			require.Zero(t, value.Content.Counts.Watches)
		})
	}
}

func TestScaleCurrentWaitAcceptsCompactedColumnarStatus(t *testing.T) {
	parent, ir := scaleCurrentFixture()
	columns, err := irstatus.EncodeColumns(ir.Status.InstanceStatuses, 2)
	require.NoError(t, err)
	encoding := ome.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatuses = nil
	ir.Status.InstanceStatusEncoding = &encoding
	ir.Status.InstanceStatusColumns = columns
	out, stderr, err := execute(t, exactReplicaWaitFactory(t, omefake.NewSimpleClientset(parent, ir)),
		"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "-o", "json")
	require.NoError(t, err)
	require.Empty(t, stderr)
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal([]byte(out), &value))
	require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
	require.Equal(t, "ColumnarV2", value.Content.Scale.Encoding)
	require.Equal(t, int32(2), *value.Content.Scale.CurrentReplicas)
	require.Equal(t, int32(1), *value.Content.Scale.ReadyReplicas)
}

func TestScaleCurrentWaitRejectsInvalidFlagsBeforeClientReads(t *testing.T) {
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"missing component", []string{"chat", "--for=replicas=current", "--replicas=2"}},
		{"missing count", []string{"chat", "--for=replicas=current", "--component=engine"}},
		{"zero count", []string{"chat", "--for=replicas=current", "--component=engine", "--replicas=0"}},
		{"negative count", []string{"chat", "--for=replicas=current", "--component=engine", "--replicas=-1"}},
		{"unpaired IR name", []string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "--ir-name=custom-engine"}},
		{"unpaired IR UID", []string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "--ir-uid=ir-uid"}},
		{"stray revision", []string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "--revision=chat-engine-aaaaaaaa"}},
		{"stray request ID", []string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "--request-id=123e4567-e89b-42d3-a456-426614174000"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &readyFailFactory{Static: factory.Static{NS: "prod"}}
			out, stderr, err := execute(t, f, tc.args...)
			require.Error(t, err)
			require.Equal(t, 1, exitcode.FromError(err))
			require.Empty(t, out)
			require.Empty(t, stderr)
			require.Zero(t, f.reads.Load())
		})
	}
}

func TestScaleCurrentWaitPollsExactIRStatusAndSeparatesReady(t *testing.T) {
	parent, ir := scaleCurrentFixture()
	client := omefake.NewSimpleClientset(parent, ir)
	var irGets atomic.Int32
	client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		require.Equal(t, "custom-engine", action.(ktesting.GetAction).GetName())
		current := ir.DeepCopy()
		if irGets.Add(1) == 1 {
			current.Status.Replicas = 1
			current.Status.ReadyReplicas = 0
			current.Status.InstanceStatuses = current.Status.InstanceStatuses[:1]
		}
		return true, current, nil
	})
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(exactReplicaWaitFactory(t, client), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return irGets.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("scale wait did not finish after second exact IR observation")
	}
	require.Empty(t, stderr.String())
	require.Equal(t, int32(2), irGets.Load())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
	require.Equal(t, int32(1), *value.Content.Scale.ReadyReplicas)
	require.Equal(t, 2, value.Content.Counts.Gets)
	require.Equal(t, 0, value.Content.Counts.Watches)
	require.Equal(t, 1, value.Content.Counts.Polls)
}

func TestScaleCurrentWaitBindsExactActionTargetWithoutParentRef(t *testing.T) {
	parent, ir := scaleCurrentFixture()
	parent.Status.Components = nil
	client := omefake.NewSimpleClientset(parent, ir)
	out, stderr, err := execute(t, exactReplicaWaitFactory(t, client),
		"chat", "--for=replicas=current", "--component=engine", "--replicas=2",
		"--ir-name=custom-engine", "--ir-uid=ir-uid", "-o", "json")
	require.NoError(t, err)
	require.Empty(t, stderr)
	require.Len(t, client.Actions(), 3)
	require.Equal(t, "custom-engine", client.Actions()[1].(ktesting.GetAction).GetName())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal([]byte(out), &value))
	require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
}

func TestScaleCurrentWaitParentReplacementClearsPriorIRCounts(t *testing.T) {
	parent, ir := scaleCurrentFixture()
	ir.Status.Replicas = 1
	ir.Status.InstanceStatuses = ir.Status.InstanceStatuses[:1]
	client := omefake.NewSimpleClientset(parent, ir)
	var parentGets atomic.Int32
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		current := parent.DeepCopy()
		if parentGets.Add(1) >= 3 {
			current.UID = "replacement-uid"
			current.ResourceVersion = "18"
		}
		return true, current, nil
	})
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(exactReplicaWaitFactory(t, client), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return parentGets.Load() == 2 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("scale wait did not stop on parent replacement")
	}
	require.Empty(t, stderr.String())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeReplaced, value.Content.Outcome)
	require.Equal(t, "Unavailable", value.Content.Scale.Validity)
	require.Nil(t, value.Content.Scale.CurrentReplicas)
	require.Zero(t, value.Content.Counts.Watches)
}

func TestScaleCurrentWaitTimeoutDoesNotClaimDesiredAsCurrent(t *testing.T) {
	parent, ir := scaleCurrentFixture()
	ir.Status.Replicas = 1
	ir.Status.InstanceStatuses = ir.Status.InstanceStatuses[:1]
	client := omefake.NewSimpleClientset(parent, ir)
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(exactReplicaWaitFactory(t, client), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "--timeout=1s", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("scale wait did not time out")
	}
	require.Empty(t, stderr.String())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeTimedOut, value.Content.Outcome)
	require.Equal(t, int32(2), *value.Content.Scale.SpecReplicas)
	require.Equal(t, int32(1), *value.Content.Scale.CurrentReplicas)
	require.Equal(t, "ReplicaScaleNotMatched", string(value.Content.Reason))
}

func TestScaleCurrentWaitActionUIDMismatchNeverMatches(t *testing.T) {
	parent, ir := scaleCurrentFixture()
	client := omefake.NewSimpleClientset(parent, ir)
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(exactReplicaWaitFactory(t, client), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2",
		"--ir-name=custom-engine", "--ir-uid=old-ir-uid", "--timeout=1s", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("scale wait did not time out on action UID mismatch")
	}
	require.Empty(t, stderr.String())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeTimedOut, value.Content.Outcome)
	require.NotEqual(t, "Valid", value.Content.Scale.Validity)
	require.Nil(t, value.Content.Scale.CurrentReplicas)
}

func TestScaleCurrentWaitParentDeletionClearsPriorIRCounts(t *testing.T) {
	parent, ir := scaleCurrentFixture()
	ir.Status.Replicas = 1
	ir.Status.InstanceStatuses = ir.Status.InstanceStatuses[:1]
	client := omefake.NewSimpleClientset(parent, ir)
	var parentGets atomic.Int32
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		current := parent.DeepCopy()
		if parentGets.Add(1) >= 3 {
			now := metav1.NewTime(time.Unix(1000, 0))
			current.DeletionTimestamp = &now
			current.ResourceVersion = "18"
		}
		return true, current, nil
	})
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(exactReplicaWaitFactory(t, client), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return parentGets.Load() == 2 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("scale wait did not stop on parent deletion")
	}
	require.Empty(t, stderr.String())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeDeleted, value.Content.Outcome)
	require.Equal(t, "Unavailable", value.Content.Scale.Validity)
	require.Nil(t, value.Content.Scale.CurrentReplicas)
}

func TestScaleCurrentWaitCancellationProducesNoReport(t *testing.T) {
	parent, ir := scaleCurrentFixture()
	ir.Status.Replicas = 1
	ir.Status.InstanceStatuses = ir.Status.InstanceStatuses[:1]
	client := omefake.NewSimpleClientset(parent, ir)
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var out, stderr bytes.Buffer
	cmd := newCmd(exactReplicaWaitFactory(t, client), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"chat", "--for=replicas=current", "--component=engine", "--replicas=2", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.Equal(t, 1, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("scale wait did not cancel")
	}
	require.Empty(t, stderr.String())
	require.Empty(t, out.String())
}
