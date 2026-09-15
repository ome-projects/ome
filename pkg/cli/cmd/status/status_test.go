package status

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	kubefake "k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

func execute(t *testing.T, f factory.Factory, args ...string) (string, error) {
	t.Helper()
	var out bytes.Buffer
	streams := genericiooptions.IOStreams{In: &bytes.Buffer{}, Out: &out, ErrOut: &out}
	cmd := NewCmd(f, streams)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestStatusDefaultsToCompactReport(t *testing.T) {
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-isvc", Namespace: "team-a"},
		}),
		Kube: kubefake.NewSimpleClientset(),
		NS:   "team-a",
	}
	out, err := execute(t, f, "demo-isvc")
	require.NoError(t, err)
	assert.Equal(t,
		"FIELD       VALUE\n"+
			"NAME        demo-isvc\n"+
			"NAMESPACE   team-a\n"+
			"READY       Unknown\n"+
			"RUNTIME     (auto-selected)\n"+
			"\n"+
			"FEATURE   STATUS\n"+
			"TRAFFIC   -\n"+
			"ROLLOUT   -\n",
		out,
	)
}

func TestStatusWidePreservesDetailedReport(t *testing.T) {
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-isvc", Namespace: "team-a"},
		}),
		Kube: kubefake.NewSimpleClientset(),
		NS:   "team-a",
	}

	out, err := execute(t, f, "demo-isvc", "-o", "wide")

	require.NoError(t, err)
	assert.Equal(t,
		"Name:       demo-isvc\n"+
			"Namespace:  team-a\n"+
			"Ready:      Unknown\n"+
			"Runtime:    (auto-selected)\n"+
			"\n"+
			"Conditions:\n"+
			"  TYPE   STATUS   REASON   MESSAGE\n"+
			"\n"+
			"Components:\n"+
			"\n"+
			"Model Status:\n"+
			"  Transition: -\n",
		out,
	)
}

func TestStatusRejectsUnsupportedOutputBeforeReads(t *testing.T) {
	out, err := execute(t, factory.Static{}, "demo-isvc", "-o", "json")

	require.Error(t, err)
	assert.Contains(t, err.Error(), `unsupported output format "json" (supported: table, wide)`)
	assert.Empty(t, out)
}

func TestStatusRequiresExactlyOneArg(t *testing.T) {
	_, err := execute(t, factory.Static{})
	require.Error(t, err)

	_, err = execute(t, factory.Static{}, "a", "b")
	require.Error(t, err)
}

func TestStatusNotFound(t *testing.T) {
	f := factory.Static{
		OME:  omefake.NewSimpleClientset(),
		Kube: kubefake.NewSimpleClientset(),
		NS:   "team-a",
	}
	_, err := execute(t, f, "missing")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}
