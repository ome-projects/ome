package status

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubefake "k8s.io/client-go/kubernetes/fake"
	knapis "knative.dev/pkg/apis"
	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestProjectStatusSummarizesReportedPlacementWithoutEndpointSecrets(t *testing.T) {
	v := typedISVC()
	v.Spec.Placement = &ome.PlacementSpec{Mode: ome.PlacementModeSplit}
	v.Status.Placement = &ome.PlacementStatus{
		Phase: ome.PlacementPhasePlaced, Cluster: "west",
		Endpoint: &knapis.URL{Scheme: "https", Host: "gateway.example", Path: "/private-path", RawQuery: "token=secret"},
		Candidates: []ome.CandidatePlacement{
			{Cluster: "west", Phase: ome.CandidatePhaseAdmitted, AdmittedReplicas: 2, ReadyReplicas: 1},
			{Cluster: "east", Phase: ome.CandidatePhaseAdmitted, AdmittedReplicas: 3, ReadyReplicas: 2},
		},
	}
	got, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	data, err := json.Marshal(got)
	require.NoError(t, err)
	var document map[string]any
	require.NoError(t, json.Unmarshal(data, &document))
	content := document["content"].(map[string]any)
	placement, ok := content["placement"].(map[string]any)
	require.True(t, ok, "status JSON must contain a typed placement summary")
	require.Equal(t, "Reported", placement["state"])
	require.Equal(t, "Split", placement["mode"])
	require.Equal(t, "Placed", placement["phase"])
	require.Equal(t, "Unverifiable", placement["freshness"])
	require.Equal(t, float64(5), placement["admittedReplicas"].(map[string]any)["value"])
	require.Equal(t, float64(3), placement["readyReplicas"].(map[string]any)["value"])
	require.NotContains(t, string(data), "private-path")
	require.NotContains(t, string(data), "token=secret")
	var table bytes.Buffer
	require.NoError(t, got.Table().Write(&table))
	require.Contains(t, table.String(), "Placement")
	require.Contains(t, table.String(), "Split / Placed")
	require.NotContains(t, table.String(), "private-path")
	require.NotContains(t, table.String(), "token=secret")
	for _, line := range strings.Split(strings.TrimSuffix(table.String(), "\n"), "\n") {
		require.LessOrEqual(t, len([]rune(line)), 80, line)
	}
}

func TestProjectStatusPlacementDoesNotAggregateTruncatedCandidates(t *testing.T) {
	for _, size := range []int{65, 257} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			v := typedISVC()
			v.Spec.Placement = &ome.PlacementSpec{Mode: ome.PlacementModeSplit}
			v.Status.Placement = &ome.PlacementStatus{Phase: ome.PlacementPhaseRacing}
			for i := 0; i < size; i++ {
				v.Status.Placement.Candidates = append(v.Status.Placement.Candidates, ome.CandidatePlacement{Cluster: fmt.Sprintf("home-%03d", i), AdmittedReplicas: 1, ReadyReplicas: 1})
			}
			got, err := projectStatus(observedReport(v), statusClock)
			require.NoError(t, err)
			require.Equal(t, "Partial", string(got.Content.Placement.State))
			require.True(t, got.Content.Placement.Candidates.Truncated)
			require.Equal(t, "Unavailable", string(got.Content.Placement.AdmittedReplicas.State))
			require.Nil(t, got.Content.Placement.AdmittedReplicas.Value)
			require.Equal(t, "Unavailable", string(got.Content.Placement.ReadyReplicas.State))
		})
	}
}

func TestProjectStatusPlacedSplitWithUnknownCountIsPartial(t *testing.T) {
	v := typedISVC()
	v.Spec.Placement = &ome.PlacementSpec{Mode: ome.PlacementModeSplit}
	v.Status.Placement = &ome.PlacementStatus{
		Phase: ome.PlacementPhasePlaced,
		Candidates: []ome.CandidatePlacement{
			{Cluster: "west", Phase: ome.CandidatePhaseAdmitted, AdmittedReplicas: 2, ReadyReplicas: 1},
			{Cluster: "east", Phase: ome.CandidatePhaseAdmitted, AdmittedReplicas: 1},
		},
	}
	got, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, "Partial", string(got.Content.Placement.State))
	require.Equal(t, "Reported", string(got.Content.Placement.AdmittedReplicas.State))
	require.Equal(t, "Unknown", string(got.Content.Placement.ReadyReplicas.State))
	require.Nil(t, got.Content.Placement.ReadyReplicas.Value)
}

func TestProjectStatusPlacedSplitWithoutHomesIsPartial(t *testing.T) {
	v := typedISVC()
	v.Spec.Placement = &ome.PlacementSpec{Mode: ome.PlacementModeSplit}
	v.Status.Placement = &ome.PlacementStatus{Phase: ome.PlacementPhasePlaced}
	got, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, "Partial", string(got.Content.Placement.State))
	require.Equal(t, "Unknown", string(got.Content.Placement.AdmittedReplicas.State))
	require.Equal(t, "Unknown", string(got.Content.Placement.ReadyReplicas.State))
	require.Nil(t, got.Content.Placement.AdmittedReplicas.Value)
	require.Nil(t, got.Content.Placement.ReadyReplicas.Value)
}

func TestStatusPlacementCobraOutputUsesParentOnly(t *testing.T) {
	v := &ome.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "chat", Namespace: "prod", UID: "fixture-uid", ResourceVersion: "fixture-rv", Generation: 4}}
	v.Spec.Placement = &ome.PlacementSpec{Mode: ome.PlacementModeSingle}
	v.Status.Placement = &ome.PlacementStatus{Phase: ome.PlacementPhasePlaced, Cluster: "west", Endpoint: &knapis.URL{Scheme: "https", Host: "gateway.example", Path: "/secret", RawQuery: "token=secret"}, Candidates: []ome.CandidatePlacement{{Cluster: "west", Phase: ome.CandidatePhaseAdmitted}}}
	omeClient := omefake.NewSimpleClientset(v)
	out, err := execute(t, factory.Static{OME: omeClient, Kube: kubefake.NewSimpleClientset(), NS: "prod"}, "chat")
	require.NoError(t, err)
	require.Contains(t, out, "Placement")
	require.Contains(t, out, "Reported / Single / Placed")
	require.Contains(t, out, "Placement detail")
	require.NotContains(t, out, "/secret")
	require.NotContains(t, out, "token=secret")
	for _, line := range strings.Split(strings.TrimSuffix(out, "\n"), "\n") {
		require.LessOrEqual(t, len([]rune(line)), 80, line)
	}
	require.Len(t, omeClient.Actions(), 1)
	require.Equal(t, "get", omeClient.Actions()[0].GetVerb())
	require.Equal(t, "inferenceservices", omeClient.Actions()[0].GetResource().Resource)
	t.Logf("$ kubectl ome status chat -n prod -o table\n%s", out)
}

func TestProjectStatusPlacementIntentWithoutStatusKeepsSplitCountsUnknown(t *testing.T) {
	v := typedISVC()
	v.Spec.Placement = &ome.PlacementSpec{Mode: ome.PlacementModeSplit}
	got, err := projectStatus(observedReport(v), statusClock)
	require.NoError(t, err)
	require.Equal(t, "NotReported", string(got.Content.Placement.State))
	require.Equal(t, "NotRecorded", string(got.Content.Placement.Phase))
	require.Equal(t, "Unknown", string(got.Content.Placement.AdmittedReplicas.State))
	require.Equal(t, "Unknown", string(got.Content.Placement.ReadyReplicas.State))
	require.Equal(t, "Unavailable", string(got.Content.Placement.Evidence))
}

func TestProjectStatusPlacementSignalsDoNotClaimUnconfigured(t *testing.T) {
	for _, tc := range []struct {
		name string
		mark func(*ome.InferenceService)
		want string
	}{
		{"ordinary", func(*ome.InferenceService) {}, "NotConfigured"},
		{"finalizer", func(v *ome.InferenceService) { v.Finalizers = []string{"ome.io/placement"} }, "NotReported"},
		{"legacy annotation", func(v *ome.InferenceService) { v.Annotations = map[string]string{"ome.io/cluster-selector": ""} }, "NotReported"},
		{"derived marker", func(v *ome.InferenceService) { v.Labels = map[string]string{constants.PlacementOrigin: ""} }, "NotReported"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v := typedISVC()
			tc.mark(v)
			got, err := projectStatus(observedReport(v), statusClock)
			require.NoError(t, err)
			require.Equal(t, tc.want, string(got.Content.Placement.State))
		})
	}
}
