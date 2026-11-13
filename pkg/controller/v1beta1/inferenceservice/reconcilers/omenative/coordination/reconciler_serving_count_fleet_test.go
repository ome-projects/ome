package coordination

import (
	"cmp"
	"flag"
	"math/rand/v2"
	"path/filepath"
	"slices"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/utils/testing/allocbudget"
)

// updateServingCountAllocations re-records the serving count's golden.
var updateServingCountAllocations = flag.Bool("update-serving-count", false, "re-record testdata/serving_count_allocations.json from this run")

// servingCountOrder is the order a Component's pods are listed in: as the
// fixture renders them (a gang's leader before its workers), every worker
// before every leader, or shuffled, as the cache lists them.
type servingCountOrder string

const (
	orderRendered     servingCountOrder = "rendered"
	orderWorkersFirst servingCountOrder = "workers-first"
	orderShuffled     servingCountOrder = "shuffled"
)

// servingCountMember is one Component at rest in one list order: every pod
// serving on the one revision, the list the coordination pass and the
// canary dispatch count.
type servingCountMember struct {
	// name is <shape>/<component>/<order>.
	name string
	pods []*corev1.Pod
}

// servingCountShapes is every reference shape of the pass fixture and the
// fleet of 1,725 gangs of two.
func servingCountShapes() []passFleetShape {
	return append(passFleetShapes(), passFleetShape{name: "engine/1725x2", members: []passFleetMember{{v1beta1.EngineComponent, 1725, 1}}})
}

// servingCountOrders is the list orders a member is counted in: one for a
// Component of single pods, whose order carries no leader or worker, three
// for a Component of gangs.
func servingCountOrders(member passFleetMember) []servingCountOrder {
	if member.workers == 0 {
		return []servingCountOrder{orderRendered}
	}
	return []servingCountOrder{orderRendered, orderWorkersFirst, orderShuffled}
}

// podsInOrder is a copy of pods in the order named.
func podsInOrder(pods []*corev1.Pod, order servingCountOrder) []*corev1.Pod {
	out := slices.Clone(pods)
	switch order {
	case orderWorkersFirst:
		slices.SortStableFunc(out, func(a, b *corev1.Pod) int { return cmp.Compare(leaderRank(a), leaderRank(b)) })
	case orderShuffled:
		rand.New(rand.NewPCG(44, uint64(len(out)))).Shuffle(len(out), func(i, j int) { out[i], out[j] = out[j], out[i] })
	case orderRendered:
	}
	return out
}

// leaderRank sorts a gang's workers ahead of every other pod.
func leaderRank(pod *corev1.Pod) int {
	if pod.Labels[query.LabelRunner] == string(v1beta1.RunnerNameWorker) {
		return 0
	}
	return 1
}

// servingCountMembers builds each shape's Components with the pods the pass
// fixture renders (the engine's labels, Running, Ready and serving, a gang's
// leader and workers under one index), in each order they are counted in.
func servingCountMembers() []servingCountMember {
	var members []servingCountMember
	for _, shape := range servingCountShapes() {
		isvc := passFleetService(shape)
		for _, member := range shape.members {
			name := irprojector.InferenceReplicaName(isvc.Name, member.component)
			replica := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Namespace: isvc.Namespace, Name: name, UID: types.UID(name + "-uid")}}
			pods := passFleetPods(isvc, replica, member)
			for _, order := range servingCountOrders(member) {
				members = append(members, servingCountMember{name: shape.name + "/" + string(member.component) + "/" + string(order), pods: podsInOrder(pods, order)})
			}
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
// per Component at rest, in every order the list may come in. The count runs
// twice per Component on every coordination pass (the observation and the
// endpoint reading) and once per member on every canary dispatch, over every
// pod of the Component.
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
// Component and order, with its pods beside it.
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
