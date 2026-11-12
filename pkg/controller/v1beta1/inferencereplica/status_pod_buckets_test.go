package inferencereplica

import (
	"context"
	"testing"
	"time"

	"github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// nodesAbsent reads every Node as present but the ones it names.
type nodesAbsent map[string]struct{}

func (n nodesAbsent) NodeExists(_ context.Context, name string) (bool, error) {
	_, absent := n[name]
	return !absent, nil
}

// twoReadyInstances is a replica of two single-pod Instances, each Ready
// on rev-a, its plan, and the serving pod of each Instance on its own Node.
func twoReadyInstances(t *testing.T) (*v1beta1.InferenceReplica, workloadtypes.ComponentPlan, []*corev1.Pod) {
	t.Helper()
	ir := baselineIR("model-engine", "default", 2)
	readySince := metav1.NewTime(time.Date(2026, time.March, 1, 12, 0, 0, 0, time.UTC))
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{
		{Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-a", ReadySince: &readySince},
		{Index: 1, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: "rev-a", ReadySince: &readySince},
	}
	plan := workloadtypes.ComponentPlan{
		Component: v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component),
		Replicas:  2,
		Instances: []workloadtypes.InstancePlan{
			{Index: 0, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
			{Index: 1, Incarnation: 1, Runners: []workloadtypes.RunnerPlan{{Name: "default", Size: 1}}},
		},
	}
	pods := []*corev1.Pod{podForIR(ir, 0, "default", 0, true, true), podForIR(ir, 1, "default", 0, true, true)}
	pods[0].Spec.NodeName, pods[1].Spec.NodeName = "node-a", "node-b"
	return ir, plan, pods
}

// TestAggregateAndWriteStatusReadsTheBucketsThePassListHolds: the status
// write reads each Instance's pods from the buckets the pass's list holds,
// built once per list by the first reader of the pass, and does not bucket
// the pods again. With the list's buckets leaving the second Instance's pod
// out, the write publishes that Instance with no pod.
func TestAggregateAndWriteStatusReadsTheBucketsThePassListHolds(t *testing.T) {
	g := gomega.NewWithT(t)
	ir, plan, pods := twoReadyInstances(t)
	r, stored := newReconciler(t, ir)
	list, taken := passListOf(t, r.Client, pods)
	list.ByInstance(taken, func([]*corev1.Pod) map[int32][]*corev1.Pod { return map[int32][]*corev1.Pod{0: taken[:1]} })

	_, err := r.aggregateAndWriteStatus(context.Background(), ir, taken, list, plan, nil, everyNodePresent{}, false, nil, instanceIndices(ir.Status.InstanceStatuses))
	g.Expect(err).To(gomega.Succeed())
	got := &v1beta1.InferenceReplica{}
	g.Expect(stored.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].PodCount).To(gomega.Equal(int32(1)))
	g.Expect(got.Status.InstanceStatuses[1].PodCount).To(gomega.BeZero(), "the list's buckets hold no pod of Instance 1")
	g.Expect(got.Status.InstanceStatuses[1].Phase).To(gomega.Equal(v1beta1.OMENativeInstancePending))
}

// TestAggregateAndWriteStatusBucketsAListCutOnAMissingNodeOnItsOwn: a pod
// bound to a Node the cluster no longer holds is cut from the list before
// the write, and the cut list is bucketed on its own: the write publishes
// that Instance without the pod, while the pass's list and its buckets
// still hold it for the readers that take the list as the cache gave it.
func TestAggregateAndWriteStatusBucketsAListCutOnAMissingNodeOnItsOwn(t *testing.T) {
	g := gomega.NewWithT(t)
	ir, plan, pods := twoReadyInstances(t)
	r, stored := newReconciler(t, ir)
	list, taken := passListOf(t, r.Client, pods)
	list.ByInstance(taken, query.BucketPodsByInstanceIdx)

	_, err := r.aggregateAndWriteStatus(context.Background(), ir, taken, list, plan, nil, nodesAbsent{"node-b": {}}, false, nil, instanceIndices(ir.Status.InstanceStatuses))
	g.Expect(err).To(gomega.Succeed())
	got := &v1beta1.InferenceReplica{}
	g.Expect(stored.Get(context.Background(), client.ObjectKeyFromObject(ir), got)).To(gomega.Succeed())
	g.Expect(got.Status.InstanceStatuses[0].PodCount).To(gomega.Equal(int32(1)))
	g.Expect(got.Status.InstanceStatuses[1].PodCount).To(gomega.BeZero(), "the pod on the missing Node is cut from the list the write reads")
	g.Expect(got.Status.InstanceStatuses[1].Phase).To(gomega.Equal(v1beta1.OMENativeInstancePending))
	g.Expect(list.ByInstance(taken, query.BucketPodsByInstanceIdx)[1]).To(gomega.HaveLen(1), "the list's own buckets still hold the pod")
}
