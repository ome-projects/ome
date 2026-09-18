package wait

import (
	"bytes"
	"encoding/json"
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
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/rest"
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
)

const heldRevision = "chat-engine-aaaaaaaa"

type heldRESTFactory struct {
	factory.Static
	host string
}

func (f heldRESTFactory) RESTConfig() (*rest.Config, error) {
	return &rest.Config{Host: f.host}, nil
}

func heldWaitFactory(t *testing.T, client *omefake.Clientset) factory.Factory {
	t.Helper()
	const prefix = "/apis/ome.io/v1beta1/namespaces/prod/inferencereplicas/"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, prefix)
		if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, prefix) || name == "" || strings.Contains(name, "/") {
			t.Errorf("unexpected IR request: %s %s", r.Method, r.URL.Path)
			http.NotFound(w, r)
			return
		}
		ir, err := client.OmeV1beta1().InferenceReplicas("prod").Get(r.Context(), name, metav1.GetOptions{})
		if err != nil {
			http.NotFound(w, r)
			return
		}
		copyIR := ir.DeepCopy()
		copyIR.APIVersion, copyIR.Kind = "ome.io/v1beta1", "InferenceReplica"
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(copyIR); err != nil {
			t.Errorf("encode IR response: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return heldRESTFactory{Static: factory.Static{NS: "prod", OME: client}, host: server.URL}
}

func heldWaitFixture() (*ome.InferenceService, *ome.InferenceReplica) {
	parent := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("parent-uid"), ResourceVersion: "17", Generation: 7,
		Annotations: map[string]string{"private.example/token": "PRIVATE-ANNOTATION"},
	}}
	controller := true
	ir := &ome.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: "custom-engine", Namespace: "prod", UID: types.UID("ir-uid"), ResourceVersion: "31", Generation: 2,
			Labels: map[string]string{constants.InferenceServiceLabel: "chat", constants.OMEComponentLabel: "engine"},
			Annotations: map[string]string{
				constants.InferenceReplicaParentGenerationAnnotationKey: "7",
				constants.InferenceReplicaControllerWriteAnnotationKey:  "true",
				"private.example/token":                                 "PRIVATE-ANNOTATION",
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "chat", UID: parent.UID, Controller: &controller}},
		},
		Spec:   ome.InferenceReplicaSpec{ParentRef: ome.ParentReference{Name: "chat"}, Component: ome.EngineComponent},
		Status: ome.InferenceReplicaStatus{ObservedGeneration: 2},
	}
	return parent, ir
}

func TestHeldRevisionWaitRendersStateOnlyExactTargetInAllFormats(t *testing.T) {
	var jsonContent reportv1alpha1.WaitContent
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			parent, ir := heldWaitFixture()
			client := omefake.NewSimpleClientset(parent, ir)
			gets := 0
			client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
				t.Fatal("exact held-revision wait must never LIST siblings")
				return true, nil, nil
			})
			client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
				gets++
				require.Equal(t, "custom-engine", action.(ktesting.GetAction).GetName())
				return false, nil, nil
			})
			var stdout, stderrBuffer bytes.Buffer
			cmd := newCmd(heldWaitFactory(t, client), genericiooptions.IOStreams{
				In: &bytes.Buffer{}, Out: &stdout, ErrOut: &stderrBuffer,
			}, clocktesting.NewFakeClock(time.Unix(1000, 0)))
			cmd.SetArgs([]string{"chat", "--for=held-revision=unheld", "--component=engine", "--revision=" + heldRevision,
				"--ir-name=custom-engine", "--ir-uid=ir-uid", "-o", format})
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			err := cmd.Execute()
			out, stderr := stdout.String(), stderrBuffer.String()
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.Equal(t, 1, gets)
			require.NotContains(t, out, "PRIVATE")
			t.Logf("kubectl ome wait chat --for=held-revision=unheld --component=engine --revision=%s --ir-name=custom-engine --ir-uid=ir-uid -n prod -o %s (synthetic fixture):\n%s", heldRevision, format, out)
			if format == "table" || format == "wide" {
				require.Regexp(t, `(?m)^Elapsed milliseconds +0$`, out)
				require.Contains(t, out, "HeldRevision=Unheld")
				require.Contains(t, out, "Matched")
				require.Contains(t, out, "Unverifiable")
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
			var decoded reportv1alpha1.WaitReport
			require.NoError(t, json.Unmarshal(data, &decoded))
			require.Equal(t, waitengine.OutcomeMatched, decoded.Content.Outcome)
			require.Equal(t, reportv1alpha1.WaitRequested("HeldRevision=Unheld"), decoded.Content.Requested)
			require.Zero(t, decoded.Content.Counts.Watches)
			if format == "json" {
				jsonContent = decoded.Content
			} else {
				jsonContent.ElapsedMilliseconds = 0
				decoded.Content.ElapsedMilliseconds = 0
				require.Equal(t, jsonContent, decoded.Content)
			}
			require.Contains(t, out, "custom-engine")
			require.Contains(t, out, "chat-engine-aaaaaaaa")
			require.Contains(t, out, "Unverifiable")
		})
	}
}

func TestHeldRevisionWaitAcceptsCompactStatusOverExactRESTRead(t *testing.T) {
	parent, ir := heldWaitFixture()
	encoding := ome.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatusEncoding = &encoding
	ir.Status.InstanceStatusColumns = &ome.InstanceStatusColumns{
		Members: "0",
		Phases:  []ome.InstanceStatusPhaseGroup{{Value: ome.OMENativeInstanceReady, Indexes: "0"}},
	}
	client := omefake.NewSimpleClientset(parent, ir)
	out, stderr, err := execute(t, heldWaitFactory(t, client),
		"chat", "--for=held-revision=unheld", "--component=engine", "--revision="+heldRevision,
		"--ir-name=custom-engine", "--ir-uid=ir-uid", "-o", "json")
	require.NoError(t, err)
	require.Empty(t, stderr)
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal([]byte(out), &value))
	require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
	require.Equal(t, "ColumnarV2", string(value.Content.HeldRevision.Encoding))
}

func TestHeldRevisionWaitRejectsMissingMalformedAndStrayFlagsBeforeReads(t *testing.T) {
	valid := []string{"chat", "--for=held-revision=unheld", "--component=engine", "--revision=" + heldRevision, "--ir-name=custom-engine", "--ir-uid=ir-uid"}
	for _, tc := range []struct {
		name string
		args []string
	}{
		{name: "missing exact IR name", args: valid[:4]},
		{name: "missing exact IR UID", args: valid[:5]},
		{name: "hash only", args: []string{"chat", "--for=held-revision=unheld", "--component=engine", "--revision=aaaaaaaa", "--ir-name=custom-engine", "--ir-uid=ir-uid"}},
		{name: "invalid IR name", args: []string{"chat", "--for=held-revision=unheld", "--component=engine", "--revision=" + heldRevision, "--ir-name=custom_engine", "--ir-uid=ir-uid"}},
		{name: "oversized exact IR UID", args: []string{"chat", "--for=held-revision=unheld", "--component=engine", "--revision=" + heldRevision, "--ir-name=custom-engine", "--ir-uid=" + strings.Repeat("u", 257)}},
		{name: "credential userinfo IR UID", args: []string{"chat", "--for=held-revision=unheld", "--component=engine", "--revision=" + heldRevision, "--ir-name=custom-engine", "--ir-uid=user:pass@host"}},
		{name: "stray held flags", args: []string{"chat", "--for=condition=Ready", "--revision=" + heldRevision, "--ir-name=custom-engine", "--ir-uid=ir-uid"}},
		{name: "stray request ID", args: append(append([]string{}, valid...), "--request-id=123e4567-e89b-42d3-a456-426614174000")},
		{name: "stray replica count", args: append(append([]string{}, valid...), "--replicas=0")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := &readyFailFactory{Static: factory.Static{NS: "prod"}}
			out, stderr, err := execute(t, f, tc.args...)
			require.Error(t, err)
			require.Equal(t, 1, exitcode.FromError(err))
			require.Empty(t, out)
			require.Empty(t, stderr)
			require.Zero(t, f.reads.Load())
			require.NotContains(t, err.Error(), "PRIVATE")
			if tc.name == "missing exact IR name" {
				require.Contains(t, err.Error(), "InvalidHeldRevisionTarget")
			}
		})
	}
}

func TestHeldRevisionWaitPollsExactOriginalIRToUnheld(t *testing.T) {
	parent, ir := heldWaitFixture()
	ir.Status.RetryBlocks = []ome.RetryBlock{{TargetRevision: heldRevision, State: ome.RetryBlockHeld, AttemptsStarted: 1}}
	client := omefake.NewSimpleClientset(parent, ir)
	var irGets atomic.Int32
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		t.Error("held-revision wait must not LIST")
		return true, nil, nil
	})
	client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		if action.(ktesting.GetAction).GetName() != "custom-engine" {
			t.Errorf("wrong IR name: %q", action.(ktesting.GetAction).GetName())
		}
		current := ir.DeepCopy()
		if irGets.Add(1) == 2 {
			current.Status.RetryBlocks = nil
			current.ResourceVersion = "32"
		}
		return true, current, nil
	})
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(heldWaitFactory(t, client), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=held-revision=unheld", "--component=engine", "--revision=" + heldRevision,
		"--ir-name=custom-engine", "--ir-uid=ir-uid", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return irGets.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("held-revision wait did not finish after second exact IR observation")
	}
	require.Empty(t, stderr.String())
	require.Equal(t, int32(2), irGets.Load())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
	require.Equal(t, "Unheld", string(value.Content.Reason))
	require.Equal(t, 2, value.Content.Counts.Gets)
	require.Equal(t, 0, value.Content.Counts.Watches)
	require.Equal(t, 1, value.Content.Counts.Polls)
}

func TestHeldRevisionWaitParentReplacementClearsPriorIREvidence(t *testing.T) {
	parent, ir := heldWaitFixture()
	ir.Status.RetryBlocks = []ome.RetryBlock{{TargetRevision: heldRevision, State: ome.RetryBlockHeld, AttemptsStarted: 1}}
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
	cmd := newCmd(heldWaitFactory(t, client), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=held-revision=unheld", "--component=engine", "--revision=" + heldRevision,
		"--ir-name=custom-engine", "--ir-uid=ir-uid", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return parentGets.Load() == 2 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("held-revision wait did not stop on parent replacement")
	}
	require.Empty(t, stderr.String())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeReplaced, value.Content.Outcome)
	require.Equal(t, "Unavailable", string(value.Content.HeldRevision.Validity))
	require.False(t, value.Content.HeldRevision.Matched)
	require.Equal(t, "Unknown", string(value.Content.HeldRevision.TargetState))
	require.Equal(t, 0, value.Content.Counts.Watches)
}

func TestHeldRevisionWaitTimeoutReportsLastHeldState(t *testing.T) {
	parent, ir := heldWaitFixture()
	ir.Status.RetryBlocks = []ome.RetryBlock{{TargetRevision: heldRevision, State: ome.RetryBlockHeld, AttemptsStarted: 1}}
	client := omefake.NewSimpleClientset(parent, ir)
	clk := clocktesting.NewFakeClock(time.Unix(1000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(heldWaitFactory(t, client), genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=held-revision=unheld", "--component=engine", "--revision=" + heldRevision,
		"--ir-name=custom-engine", "--ir-uid=ir-uid", "--timeout=1s", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("held-revision wait did not time out")
	}
	require.Empty(t, stderr.String())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeTimedOut, value.Content.Outcome)
	require.Equal(t, "Held", string(value.Content.HeldRevision.TargetState))
	require.Equal(t, "Valid", string(value.Content.HeldRevision.Validity))
	require.Equal(t, "Held", string(value.Content.Reason))
	require.False(t, value.Content.HeldRevision.Matched)
}

func TestHeldRevisionWaitRedactsCredentialShapedActionTarget(t *testing.T) {
	parent, ir := heldWaitFixture()
	secretName := "sk-proj-0123456789abcdefghijklmnopqrst"
	secretUID := "ghp_0123456789abcdefghijklmnopqrst"
	ir.Name, ir.UID = secretName, types.UID(secretUID)
	client := omefake.NewSimpleClientset(parent, ir)
	out, stderr, err := execute(t, heldWaitFactory(t, client),
		"chat", "--for=held-revision=unheld", "--component=engine", "--revision="+heldRevision,
		"--ir-name="+secretName, "--ir-uid="+secretUID, "-o", "json")
	require.NoError(t, err)
	require.Empty(t, stderr)
	require.NotContains(t, out, secretName)
	require.NotContains(t, out, secretUID)
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal([]byte(out), &value))
	require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
	require.Equal(t, "[REDACTED]", value.Content.HeldRevision.InferenceReplica)
	require.Equal(t, "[REDACTED]", value.Content.HeldRevision.InferenceReplicaUID)
}

func TestHeldRevisionWaitAcceptsLongParentAndOpaqueActionIdentities(t *testing.T) {
	parent, ir := heldWaitFixture()
	parent.Name = strings.Repeat("a", 64)
	parent.UID = "arn:aws:parent/abc+v1"
	parent.ResourceVersion = "rv:parent/17+1"
	ir.Spec.ParentRef.Name = parent.Name
	ir.OwnerReferences[0].Name = parent.Name
	ir.OwnerReferences[0].UID = parent.UID
	delete(ir.Labels, constants.InferenceServiceLabel)
	delete(ir.Labels, constants.OMEComponentLabel)
	ir.UID = types.UID(strings.Repeat("u", 256))
	ir.ResourceVersion = "rv:ir/31+1"
	revision := parent.Name + "-engine-aaaaaaaa"
	client := omefake.NewSimpleClientset(parent, ir)
	out, stderr, err := execute(t, heldWaitFactory(t, client),
		parent.Name, "--for=held-revision=unheld", "--component=engine", "--revision="+revision,
		"--ir-name=custom-engine", "--ir-uid="+string(ir.UID), "-o", "json")
	require.NoError(t, err)
	require.Empty(t, stderr)
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal([]byte(out), &value))
	require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
	require.Equal(t, revision, value.Content.HeldRevision.Revision)
	require.Equal(t, string(ir.UID), value.Content.HeldRevision.InferenceReplicaUID)
}
