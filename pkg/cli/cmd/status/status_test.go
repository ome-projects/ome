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
	cmd := newCmd(f, streams, statusClock)
	cmd.SetArgs(args)
	err := cmd.Execute()
	return out.String(), err
}

func TestStatusDefaultsToCompactReport(t *testing.T) {
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-isvc", Namespace: "team-a", UID: "fixture-uid", ResourceVersion: "fixture-rv"},
		}),
		Kube: kubefake.NewSimpleClientset(),
		NS:   "team-a",
	}
	out, err := execute(t, f, "demo-isvc")
	require.NoError(t, err)
	assert.Equal(t,
		"FIELD                VALUE\n"+
			"Name                 demo-isvc\n"+
			"Namespace            team-a\n"+
			"Ready                NotRecorded / Unavailable\n"+
			"Ready reason\n"+
			"Declared runtime\n"+
			"Model\n"+
			"Generation           0 observed=0; advisory Unverifiable\n"+
			"Pod observation      Reported count=0 truncated=false \n"+
			"Event observation    Reported count=0 truncated=false \n"+
			"Rollout              NotConfigured reported=NotConfigured\n"+
			"Rollout evidence     Declared / NotApplicable\n"+
			"Autoscaling          Unavailable / Unavailable parent status\n"+
			"Traffic              Unavailable / Unavailable parent status\n"+
			"Runtime active       NotConfigured / Unavailable\n"+
			"Accelerator          NotConfigured / Unavailable\n"+
			"Full safe values     Use -o json or -o yaml\n"+
			"Rollout detail       kubectl ome rollout status NAME\n"+
			"Autoscale detail     kubectl ome autoscale status NAME\n"+
			"Traffic detail       kubectl ome traffic status NAME\n"+
			"Runtime detail       kubectl ome runtime effective NAME\n"+
			"Accelerator detail   kubectl ome accelerator explain NAME\n",
		out,
	)
}

func TestStatusWidePreservesDetailedReport(t *testing.T) {
	f := factory.Static{
		OME: omefake.NewSimpleClientset(&v1beta1.InferenceService{
			ObjectMeta: metav1.ObjectMeta{Name: "demo-isvc", Namespace: "team-a", UID: "fixture-uid", ResourceVersion: "fixture-rv"},
		}),
		Kube: kubefake.NewSimpleClientset(),
		NS:   "team-a",
	}

	out, err := execute(t, f, "demo-isvc", "-o", "wide")

	require.NoError(t, err)
	assert.Equal(t,
		"FIELD                  VALUE\n"+
			"Name                   demo-isvc\n"+
			"Namespace              team-a\n"+
			"Ready                  NotRecorded / Unavailable\n"+
			"Ready reason\n"+
			"Ready message\n"+
			"Condition inspection   Complete 0/0\n"+
			"Declared runtime\n"+
			"Model\n"+
			"Generation             0 observed=0; advisory Unverifiable\n"+
			"Pod observation        Reported count=0 truncated=false \n"+
			"Event observation      Reported count=0 truncated=false \n"+
			"Pod targets skipped    0\n"+
			"Event targets skip     0\n"+
			"Rollout                NotConfigured reported=NotConfigured\n"+
			"Rollout evidence       Declared / NotApplicable\n"+
			"Coordination Ready     NotApplicable\n"+
			"Autoscaling            Unavailable / Unavailable parent status\n"+
			"Traffic                Unavailable / Unavailable parent status\n"+
			"Runtime active         NotConfigured / Unavailable\n"+
			"Accelerator            NotConfigured / Unavailable\n"+
			"Full safe values       Use -o json or -o yaml\n"+
			"Rollout detail         kubectl ome rollout status NAME\n"+
			"Autoscale detail       kubectl ome autoscale status NAME\n"+
			"Traffic detail         kubectl ome traffic status NAME\n"+
			"Runtime detail         kubectl ome runtime effective NAME\n"+
			"Accelerator detail     kubectl ome accelerator explain NAME\n"+
			"Collected at           2026-09-15T12:00:00Z\n"+
			"Source generation      0\n"+
			"Source evidence        Observed\n",
		out,
	)
}

func TestStatusRejectsUnsupportedOutputBeforeReads(t *testing.T) {
	out, err := execute(t, factory.Static{}, "demo-isvc", "-o", "csv")

	require.Error(t, err)
	assert.Equal(t, "status output must be table, wide, json, or yaml", err.Error())
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
