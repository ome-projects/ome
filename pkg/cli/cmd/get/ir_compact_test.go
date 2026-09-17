package get

import (
	"bytes"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

type inventoryTerminalBuffer struct{ bytes.Buffer }

func (*inventoryTerminalBuffer) TerminalWidth() (int, bool) { return 80, true }

func executeInventoryTerminal(t *testing.T, f factory.Factory, args ...string) string {
	t.Helper()
	out := &inventoryTerminalBuffer{}
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: out, ErrOut: &bytes.Buffer{}}
	cmd := NewCmd(f, streams)
	cmd.SetArgs(args)
	require.NoError(t, cmd.Execute())
	return out.String()
}

// Moving either AVAILABLE or REASON back into the default view makes the
// header too wide for an ordinary 80-column terminal and forces one record
// into a vertically stacked field list.
func TestInferenceReplicaDefaultInventoryFits80Columns(t *testing.T) {
	desired := int32(4)
	f := factory.Static{OME: omefake.NewSimpleClientset(&v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "demo", Generation: 2},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: v1beta1.EngineComponent, ParentRef: v1beta1.ParentReference{Name: "chat"}, Replicas: &desired,
		},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas: 3, ReadyReplicas: 2, AvailableReplicas: 2,
			Conditions: []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "MinimumAvailable", ObservedGeneration: 2}},
		},
	}), NS: "demo"}

	out := executeInventoryTerminal(t, f, "ir")
	lines := strings.Split(strings.TrimSuffix(out, "\n"), "\n")
	require.Len(t, lines, 2, "one replica should remain a compact table, not a field stack")
	assert.Equal(t, []string{"NAME", "COMPONENT", "PARENT", "DESIRED", "CURRENT", "READY", "LIFECYCLE", "AGE"}, strings.Fields(lines[0]))
	assert.Equal(t, []string{"chat-engine", "engine", "chat", "4", "3", "2", "Ready=True", "-"}, strings.Fields(lines[1]))
	for _, line := range lines {
		assert.LessOrEqual(t, len(line), 80, "terminal line %q", line)
	}
	t.Logf("default output (synthetic fixture, 80-column terminal):\n%s", out)
}

func TestInferenceReplicaWideInventoryBoundsLinesAndKeepsDetails(t *testing.T) {
	columnar := v1beta1.InstanceStatusEncodingColumnarV2
	desired := int32(4)
	f := factory.Static{OME: omefake.NewSimpleClientset(&v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "chat-engine", Namespace: "demo", Generation: 2},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: v1beta1.EngineComponent, ParentRef: v1beta1.ParentReference{Name: "chat"}, Replicas: &desired,
		},
		Status: v1beta1.InferenceReplicaStatus{
			Replicas: 3, ReadyReplicas: 2, AvailableReplicas: 2,
			InstanceStatusEncoding: &columnar,
			InstanceStatusColumns:  &v1beta1.InstanceStatusColumns{Members: "0", Phases: []v1beta1.InstanceStatusPhaseGroup{{Value: v1beta1.OMENativeInstanceReady, Indexes: "0"}}},
			Conditions:             []metav1.Condition{{Type: "Ready", Status: metav1.ConditionTrue, Reason: "MinimumAvailable", ObservedGeneration: 2}},
		},
	}), NS: "demo"}

	out := executeInventoryTerminal(t, f, "ir", "-o", "wide")
	for _, want := range []string{"AVAILABLE", "REASON", "ENCODING", "ColumnarV2", "MinimumAvailable"} {
		assert.Contains(t, out, want)
	}
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 80, "terminal line %q", line)
	}
	redirected, err := execute(t, f, "ir", "-o", "wide")
	require.NoError(t, err)
	redirectedLines := strings.Split(strings.TrimSuffix(redirected, "\n"), "\n")
	require.Len(t, redirectedLines, 2)
	headers, values := strings.Fields(redirectedLines[0]), strings.Fields(redirectedLines[1])
	require.Len(t, values, len(headers))
	for i, header := range headers {
		switch header {
		case "AVAILABLE":
			assert.Equal(t, "2", values[i])
		case "REASON":
			assert.Equal(t, "MinimumAvailable", values[i])
		}
	}
	t.Logf("wide output (synthetic fixture, 80-column terminal):\n%s", out)
}

func TestInferenceReplicaWideInventoryKeepsLongIdentifiers(t *testing.T) {
	// A DNS-label-sized identity cannot fit beside a stacked wide label, so
	// the printer must put it on a continuation line without truncating it.
	name := "engine-" + strings.Repeat("a", 56)
	parent := "service-" + strings.Repeat("b", 55)
	f := factory.Static{OME: omefake.NewSimpleClientset(&v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "demo"},
		Spec: v1beta1.InferenceReplicaSpec{
			Component: v1beta1.EngineComponent, ParentRef: v1beta1.ParentReference{Name: parent},
		},
	}), NS: "demo"}

	out := executeInventoryTerminal(t, f, "ir", "-o", "wide")
	assert.Contains(t, out, name)
	assert.Contains(t, out, parent)
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		assert.LessOrEqual(t, len(line), 80, "terminal line %q", line)
	}
}
