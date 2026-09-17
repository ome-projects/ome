package wait

import (
	"bytes"
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
	"sigs.k8s.io/ome/pkg/cli/waitengine"
	versioned "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/yaml"
)

func readyWaitFixture(count int32) (*ome.InferenceService, *ome.InferenceReplica) {
	parent := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{
		Name: "chat", Namespace: "prod", UID: types.UID("PRIVATE-PARENT-UID"),
		ResourceVersion: "rv-parent", Generation: 3,
		Annotations: map[string]string{"private.example/token": "PRIVATE-ANNOTATION"},
	}}
	controller := true
	ir := &ome.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name: "chat-engine", Namespace: "prod", UID: types.UID("PRIVATE-IR-UID"), ResourceVersion: "rv-ir", Generation: 2,
			Annotations:     map[string]string{constants.InferenceReplicaParentGenerationAnnotationKey: "3"},
			Labels:          map[string]string{constants.InferenceServiceLabel: "chat", constants.OMEComponentLabel: "engine"},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: ome.SchemeGroupVersion.String(), Kind: "InferenceService", Name: "chat", UID: parent.UID, Controller: &controller}},
		},
		Spec: ome.InferenceReplicaSpec{ParentRef: ome.ParentReference{Name: "chat"}, Component: ome.EngineComponent},
		Status: ome.InferenceReplicaStatus{
			ObservedGeneration: 2, Replicas: count, ReadyReplicas: count,
			ServingReplicas: count, AvailableReplicas: count,
		},
	}
	for i := int32(0); i < count; i++ {
		ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, ome.OMENativeInstanceStatus{
			Index: i, Incarnation: 1, Phase: ome.OMENativeInstanceReady,
			PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true,
		})
	}
	return parent, ir
}

func TestReadyReplicaWaitReportsExactCountInAllFormats(t *testing.T) {
	var jsonContent reportv1alpha1.WaitContent
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			parent, ir := readyWaitFixture(0)
			client := omefake.NewSimpleClientset(parent, ir)
			out, stderr, err := execute(t, factory.Static{NS: "prod", OME: client},
				"chat", "--for=replicas=ready", "--component=engine", "--replicas=0", "-o", format)
			require.NoError(t, err)
			require.Empty(t, stderr)
			require.Equal(t, 0, exitcode.FromError(err))
			require.NotContains(t, out, "PRIVATE")
			require.NotContains(t, out, "Observed Ready")
			t.Logf("kubectl ome wait chat --for=replicas=ready --component=engine --replicas=0 -n prod -o %s (synthetic fixture):\n%s", format, out)
			if format == "table" || format == "wide" {
				require.Contains(t, out, "Replicas=Ready")
				require.Contains(t, out, "Matched")
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
			require.Equal(t, reportv1alpha1.WaitRequestedReadyReplicas, decoded.Content.Requested)
			require.NotNil(t, decoded.Content.ReadyReplicas)
			require.Equal(t, reportv1alpha1.RuntimeComponentEngine, decoded.Content.ReadyReplicas.Component)
			require.Equal(t, int32(0), decoded.Content.ReadyReplicas.Requested)
			require.NotNil(t, decoded.Content.ReadyReplicas.Observed)
			require.Zero(t, *decoded.Content.ReadyReplicas.Observed)
			require.Equal(t, "Valid", decoded.Content.ReadyReplicas.Validity)
			require.Equal(t, 0, decoded.Content.Counts.Watches)
			if format == "json" {
				jsonContent = decoded.Content
			} else {
				require.Equal(t, jsonContent, decoded.Content, "JSON and YAML must describe the same observation")
			}
		})
	}
}

type readyFailFactory struct {
	factory.Static
	reads atomic.Int32
}

func (f *readyFailFactory) OMEClient() (versioned.Interface, error) {
	f.reads.Add(1)
	return nil, errors.New("PRIVATE-CLIENT-CREDENTIAL")
}

func TestReadyReplicaWaitRejectsInvalidAndStrayFlagsBeforeClientAccess(t *testing.T) {
	for _, args := range [][]string{
		{"chat", "--for=replicas=ready"},
		{"chat", "--for=replicas=ready", "--component=engine"},
		{"chat", "--for=replicas=ready", "--replicas=0"},
		{"chat", "--for=replicas=ready", "--component=future", "--replicas=1"},
		{"chat", "--for=replicas=ready", "--component=engine", "--replicas=-1"},
		{"chat", "--for=replicas=ready", "--component=engine", "--replicas=1", "--request-id=12345678-1234-4234-8234-123456789abc"},
		{"chat", "--for=condition=Ready", "--component=engine"},
		{"chat", "--for=rollout=stable", "--replicas=1"},
		{"chat", "--for=migration=terminal", "--request-id=12345678-1234-4234-8234-123456789abc", "--replicas=1"},
	} {
		f := &readyFailFactory{Static: factory.Static{NS: "prod"}}
		out, stderr, err := execute(t, f, args...)
		require.Error(t, err, "%v", args)
		require.Equal(t, 1, exitcode.FromError(err), "%v", args)
		require.Empty(t, out)
		require.Empty(t, stderr)
		require.Zero(t, f.reads.Load())
		require.NotContains(t, err.Error(), "PRIVATE")
	}
}

func TestReadyReplicaWaitPollsIRStatusWithoutParentWatch(t *testing.T) {
	parent, ir := readyWaitFixture(1)
	client := omefake.NewSimpleClientset(parent)
	var lists atomic.Int32
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		value := ir.DeepCopy()
		if lists.Add(1) > 1 {
			value.Status.Replicas = 2
			value.Status.ReadyReplicas = 2
			value.Status.ServingReplicas = 2
			value.Status.AvailableReplicas = 2
			value.Status.InstanceStatuses = append(value.Status.InstanceStatuses, ome.OMENativeInstanceStatus{
				Index: 1, Incarnation: 1, Phase: ome.OMENativeInstanceReady,
				PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true,
			})
		}
		return true, &ome.InferenceReplicaList{Items: []ome.InferenceReplica{*value}}, nil
	})
	clk := clocktesting.NewFakeClock(time.Unix(3000, 0))
	var out, stderr bytes.Buffer
	cmd := newCmd(factory.Static{NS: "prod", OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &stderr}, clk)
	cmd.SetArgs([]string{"chat", "--for=replicas=ready", "--component=engine", "--replicas=2", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return lists.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("ready-count wait did not complete after IR status changed")
	}
	require.Empty(t, stderr.String())
	require.Equal(t, int32(2), lists.Load())
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeMatched, value.Content.Outcome)
	require.Equal(t, waitengine.MethodPoll, value.Content.Method)
	require.Equal(t, 0, value.Content.Counts.Watches)
	require.Equal(t, 1, value.Content.Counts.Polls)
	require.False(t, value.Content.Fallback)
	require.Equal(t, int32(2), *value.Content.ReadyReplicas.Observed)
}

func TestReadyReplicaWaitDoesNotMatchUnobservedZero(t *testing.T) {
	parent, ir := readyWaitFixture(0)
	ir.Status.ObservedGeneration = 0
	client := omefake.NewSimpleClientset(parent, ir)
	clk := clocktesting.NewFakeClock(time.Unix(3000, 0))
	var out bytes.Buffer
	cmd := newCmd(factory.Static{NS: "prod", OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}}, clk)
	cmd.SetArgs([]string{"chat", "--for=replicas=ready", "--component=engine", "--replicas=0", "--timeout=20s", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return len(client.Actions()) == 2 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(20 * time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("ready-count wait did not time out")
	}
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeTimedOut, value.Content.Outcome)
	require.Nil(t, value.Content.ReadyReplicas.Observed)
	require.NotEqual(t, "Valid", value.Content.ReadyReplicas.Validity)
	require.NotContains(t, out.String(), `"observed": 0`)
}

func TestReadyReplicaWaitParentLossClearsPriorIRCount(t *testing.T) {
	for _, tc := range []struct {
		name    string
		mutate  func(*ome.InferenceService)
		outcome waitengine.Outcome
	}{
		{name: "replacement", mutate: func(parent *ome.InferenceService) { parent.UID = "replacement-uid" }, outcome: waitengine.OutcomeReplaced},
		{name: "deletion", mutate: func(parent *ome.InferenceService) {
			deleting := metav1.NewTime(time.Unix(3000, 0))
			parent.DeletionTimestamp = &deleting
		}, outcome: waitengine.OutcomeDeleted},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, ir := readyWaitFixture(1)
			client := omefake.NewSimpleClientset()
			var gets atomic.Int32
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				value := parent.DeepCopy()
				if gets.Add(1) > 1 {
					tc.mutate(value)
				}
				return true, value, nil
			})
			client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, &ome.InferenceReplicaList{Items: []ome.InferenceReplica{*ir}}, nil
			})
			clk := clocktesting.NewFakeClock(time.Unix(3000, 0))
			var out bytes.Buffer
			cmd := newCmd(factory.Static{NS: "prod", OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}}, clk)
			cmd.SetArgs([]string{"chat", "--for=replicas=ready", "--component=engine", "--replicas=2", "-o", "json"})
			cmd.SilenceUsage, cmd.SilenceErrors = true, true
			done := make(chan error, 1)
			go func() { done <- cmd.Execute() }()
			require.Eventually(t, func() bool { return gets.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
			clk.Step(5 * time.Second)
			select {
			case err := <-done:
				require.Equal(t, 2, exitcode.FromError(err))
			case <-time.After(time.Second):
				t.Fatal("parent loss did not stop ready-count wait")
			}
			var value reportv1alpha1.WaitReport
			require.NoError(t, json.Unmarshal(out.Bytes(), &value))
			require.Equal(t, tc.outcome, value.Content.Outcome)
			require.Equal(t, waitengine.ReasonReplicaReadyNotRecorded, value.Content.Reason)
			require.Equal(t, "Unavailable", value.Content.ReadyReplicas.Validity)
			require.Nil(t, value.Content.ReadyReplicas.Observed)
			require.Equal(t, reportv1alpha1.EvidenceUnavailable, value.Content.Evidence)
			require.NotContains(t, out.String(), "PRIVATE")
		})
	}
}

func TestReadyReplicaWaitDoesNotMatchReplacementIRUID(t *testing.T) {
	parent, ir := readyWaitFixture(1)
	client := omefake.NewSimpleClientset(parent)
	var lists atomic.Int32
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		value := ir.DeepCopy()
		if lists.Add(1) > 1 {
			value.UID = "replacement-ir-uid"
			value.Status.Replicas = 2
			value.Status.ReadyReplicas = 2
			value.Status.ServingReplicas = 2
			value.Status.AvailableReplicas = 2
			value.Status.InstanceStatuses = append(value.Status.InstanceStatuses, ome.OMENativeInstanceStatus{
				Index: 1, Incarnation: 1, Phase: ome.OMENativeInstanceReady,
				PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true,
			})
		}
		return true, &ome.InferenceReplicaList{Items: []ome.InferenceReplica{*value}}, nil
	})
	clk := clocktesting.NewFakeClock(time.Unix(3000, 0))
	var out bytes.Buffer
	cmd := newCmd(factory.Static{NS: "prod", OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}}, clk)
	cmd.SetArgs([]string{"chat", "--for=replicas=ready", "--component=engine", "--replicas=2", "--timeout=20s", "-o", "json"})
	cmd.SilenceUsage, cmd.SilenceErrors = true, true
	done := make(chan error, 1)
	go func() { done <- cmd.Execute() }()
	require.Eventually(t, func() bool { return lists.Load() == 1 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(5 * time.Second)
	require.Eventually(t, func() bool { return lists.Load() == 2 && clk.Waiters() == 2 }, time.Second, time.Millisecond)
	clk.Step(15 * time.Second)
	select {
	case err := <-done:
		require.Equal(t, 2, exitcode.FromError(err))
	case <-time.After(time.Second):
		t.Fatal("replacement IR wait did not time out")
	}
	var value reportv1alpha1.WaitReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &value))
	require.Equal(t, waitengine.OutcomeTimedOut, value.Content.Outcome)
	require.NotEqual(t, "Valid", value.Content.ReadyReplicas.Validity)
	require.Nil(t, value.Content.ReadyReplicas.Observed)
	require.NotContains(t, out.String(), "PRIVATE")
}

func TestReadyReplicaWaitSuppressesRealHTTPWarnings(t *testing.T) {
	parent, ir := readyWaitFixture(0)
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
	out, stderr, err := execute(t, f, "chat", "--for=replicas=ready", "--component=engine", "--replicas=0", "-o", "json")
	require.NoError(t, err)
	require.Equal(t, int32(2), requests.Load(), "one parent GET and one bounded IR LIST")
	require.Zero(t, warnings.calls.Load(), "server Warning text must not reach inherited handlers")
	require.NotContains(t, out+stderr, "PRIVATE_WARNING_CREDENTIAL")
	require.Contains(t, out, `"outcome": "Matched"`)
}
