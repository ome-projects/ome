package coordination

import (
	"flag"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

// updateServingCountAllocations re-records the serving count's golden.
var updateServingCountAllocations = flag.Bool("update-serving-count", false, "re-record testdata/serving_count_allocations.json from this run")

// servingCountMember is one Component at rest: every pod serving on the one
// revision, the list the coordination pass and the canary dispatch count.
type servingCountMember struct {
	// name is <shape>/<component>.
	name string
	pods []*corev1.Pod
}

// servingCountShapes is every reference shape of the pass fixture and the
// fleet of 1,725 gangs of two.
func servingCountShapes() []passFleetShape {
	return append(passFleetShapes(), passFleetShape{name: "engine/1725x2", members: []passFleetMember{{v1beta1.EngineComponent, 1725, 1}}})
}

// servingCountMembers builds each shape's Components with the pods the pass
// fixture renders: the engine's labels, Running, Ready and serving, a gang's
// leader and workers under one index.
func servingCountMembers() []servingCountMember {
	var members []servingCountMember
	for _, shape := range servingCountShapes() {
		isvc := passFleetService(shape)
		for _, member := range shape.members {
			name := irprojector.InferenceReplicaName(isvc.Name, member.component)
			replica := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: name, UID: types.UID(name + "-uid")}}
			members = append(members, servingCountMember{name: shape.name + "/" + string(member.component), pods: passFleetPods(isvc, replica, member)})
		}
	}
	return members
}

// count runs the serving count over the member and holds its answer: every
// pod serves under the one revision, a gang's workers with their leader.
func (m servingCountMember) count(tb testing.TB) {
	counts := servingPodsByRevision(m.pods)
	if want := int32(len(m.pods)); len(counts) != 1 || counts[passFleetHash] != want {
		tb.Fatalf("%s: the serving count reads %v over %d serving pods; want %d on %s alone", m.name, counts, len(m.pods), want, passFleetHash)
	}
}

// TestServingCountAtRestStaysWithinBudget holds what one serving count costs
// per Component at rest. The count runs twice per Component on every
// coordination pass (the observation and the endpoint reading) and once per
// member on every canary dispatch, over every pod of the Component.
func TestServingCountAtRestStaysWithinBudget(t *testing.T) {
	golden := allocbudget.Open(t, filepath.Join("testdata", "serving_count_allocations.json"), *updateServingCountAllocations)
	defer golden.Close()
	for _, m := range servingCountMembers() {
		allocations, bytes := allocbudget.Smallest(func() { m.count(t) })
		golden.CheckCount(m.name+"/allocations", allocations)
		golden.CheckCount(m.name+"/bytes", bytes)
	}
}

// BenchmarkServingCountAtRest prints the time of one serving count per
// Component, with its pods beside it.
func BenchmarkServingCountAtRest(b *testing.B) {
	for _, m := range servingCountMembers() {
		b.Run(m.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				m.count(b)
			}
			b.ReportMetric(float64(len(m.pods)), "pods/op")
		})
	}
}
