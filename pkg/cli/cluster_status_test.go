package cli

import (
	"bytes"
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	ome "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	fake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
)

// Removing the cluster status route or treating a declaration as a probe
// breaks this real root-command test; the object is a synthetic fixture.
func TestClusterStatusCurrentContextReportedReady(t *testing.T) {
	client := fake.NewSimpleClientset(&ome.WorkloadCluster{
		ObjectMeta: metav1.ObjectMeta{Name: "gpu-east", Generation: 3},
		Spec: ome.WorkloadClusterSpec{ClusterSource: ome.ClusterConnectionSource{
			ClusterProfileRef: &ome.ClusterProfileRef{Name: "east-profile"},
		}},
		Status: ome.WorkloadClusterStatus{Conditions: []metav1.Condition{{
			Type: "Ready", Status: metav1.ConditionTrue, ObservedGeneration: 3,
			Reason: "Connected", LastTransitionTime: metav1.NewTime(metav1.Now().Add(-1)),
		}}},
	})
	var out bytes.Buffer
	root := NewRootCmdWithFactory(factory.Static{OME: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &out})
	root.SetArgs([]string{"cluster", "status", "gpu-east", "-o", "json"})
	if err := root.Execute(); err != nil {
		t.Fatalf("observational cluster status is missing: %v", err)
	}
	for _, literal := range []string{`"kind": "ClusterStatusReport"`, `"name": "gpu-east"`, `"reportedReady": "True"`, `"freshness": "Current"`, `"profileResolution": "NotAttempted"`} {
		if !strings.Contains(out.String(), literal) {
			t.Fatalf("missing literal %s in report:\n%s", literal, &out)
		}
	}
	actions := client.Actions()
	if len(actions) != 1 || actions[0].GetVerb() != "get" || actions[0].GetResource().Resource != "workloadclusters" || actions[0].GetNamespace() != "" {
		t.Fatalf("only one cluster-scoped WorkloadCluster GET is allowed: %v", actions)
	}
}
