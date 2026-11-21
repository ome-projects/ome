package ops

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// gangMember is one pod of a gang, named by its runner and its ordinal.
type gangMember struct {
	runner  string
	ordinal int32
}

// threePodGang is a leader and two workers in gang-rank order, the order
// the TPU peer list is written in.
var threePodGang = []gangMember{
	{workload.RunnerLeader, 0},
	{workload.RunnerWorker, 0},
	{workload.RunnerWorker, 1},
}

// hostnameForm is one way a pod set spells the hostnames of its pods.
type hostnameForm struct {
	name     string
	hostname func(podName string, recreationCount int64) string
}

// hostnameForms are the two forms a pod set can have: the pod name alone,
// and the pod name followed by the set's recreation count.
var hostnameForms = []hostnameForm{
	{name: "hostnames are the pod names", hostname: func(podName string, _ int64) string { return podName }},
	{name: "hostnames carry the recreation count", hostname: func(podName string, count int64) string {
		return fmt.Sprintf("%s-i%d", podName, count)
	}},
}

// peerSet is one pod set of a three-pod gang: the pods of one Instance
// index at one recreation count, all in one hostname form.
type peerSet struct {
	client       client.Client
	namespace    string
	owner        string
	index        int32
	count        int64
	revisionHash string
	form         hostnameForm
}

func (s peerSet) podName(m gangMember) string {
	return query.PodName(s.owner, workload.ComponentEngine, s.index, m.runner, m.ordinal)
}

// address is the name the headless Service publishes for a member whose
// hostname has the set's form.
func (s peerSet) address(m gangMember) string {
	return s.form.hostname(s.podName(m), s.count) + "." + query.HeadlessServiceName(s.owner, workload.ComponentEngine)
}

// seed stores every member but missing as the set's form renders it: the
// hostname, and the leader address and the TPU peer list the pod's runtime
// was started with.
func (s peerSet) seed(t *testing.T, missing gangMember) {
	t.Helper()
	peers := make([]string, 0, len(threePodGang))
	for _, m := range threePodGang {
		peers = append(peers, s.address(m))
	}
	for _, m := range threePodGang {
		if m == missing {
			continue
		}
		labels := legacyTestPodLabels(s.owner, workload.ComponentEngine, s.index, m.runner, s.count, m.ordinal)
		labels[query.LabelRevisionHash] = s.revisionHash
		pod := &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: s.podName(m), Namespace: s.namespace, Labels: labels},
			Spec: corev1.PodSpec{
				Hostname:  s.form.hostname(s.podName(m), s.count),
				Subdomain: query.HeadlessServiceName(s.owner, workload.ComponentEngine),
				Containers: []corev1.Container{{Name: "main", Image: "test:v1", Env: []corev1.EnvVar{
					{Name: "OME_LEADER_ADDRESS", Value: s.address(threePodGang[0])},
					{Name: "TPU_WORKER_HOSTNAMES", Value: strings.Join(peers, ",")},
				}}},
			},
		}
		if err := s.client.Create(context.Background(), pod); err != nil {
			t.Fatalf("seed pod %s: %v", pod.Name, err)
		}
	}
}

// requireEveryAddressResolves reads the set's three pods and checks what
// they say to each other against what each one answers to: the leader
// address every pod holds is the leader pod's hostname under its subdomain,
// and every entry of every pod's TPU peer list is the pod of that rank.
func (s peerSet) requireEveryAddressResolves(t *testing.T) {
	t.Helper()
	pods := make([]*corev1.Pod, 0, len(threePodGang))
	answersTo := make([]string, 0, len(threePodGang))
	for _, m := range threePodGang {
		pod := &corev1.Pod{}
		if err := s.client.Get(context.Background(), client.ObjectKey{Namespace: s.namespace, Name: s.podName(m)}, pod); err != nil {
			t.Fatalf("pod %s of the set: %v", s.podName(m), err)
		}
		pods = append(pods, pod)
		answersTo = append(answersTo, pod.Spec.Hostname+"."+pod.Spec.Subdomain)
	}
	for _, pod := range pods {
		env := envByName(pod.Spec.Containers[0].Env)
		if got := env["OME_LEADER_ADDRESS"]; got != answersTo[0] {
			t.Errorf("pod %s looks its leader up as %q, and the leader pod answers to %q", pod.Name, got, answersTo[0])
		}
		if got, want := env["TPU_WORKER_HOSTNAMES"], strings.Join(answersTo, ","); got != want {
			t.Errorf("pod %s holds the TPU peer list %q, and the pods answer to %q", pod.Name, got, want)
		}
	}
}

// tpuGangSpec is a pod template that asks for TPUs, so a rendered gang pod
// carries the TPU peer list beside the leader address.
func tpuGangSpec(image string) *corev1.PodSpec {
	spec := legacyTargetSpecImage(image)
	spec.Containers[0].Resources = corev1.ResourceRequirements{
		Limits: corev1.ResourceList{corev1.ResourceName(constants.GoogleTPUResourceType): resource.MustParse("4")},
	}
	return spec
}

// threePodGangPlan is the plan of one engine Instance that is a leader and
// two workers.
func threePodGangPlan(strategy workload.UpdateStrategyType) workload.ComponentPlan {
	plan := legacyMultiPodComponentPlan(strategy)
	plan.Instances[0].Runners[1].Size = 2
	return plan
}

// TestCreate_MemberMadeAgainKeepsThePeerAddressesResolving locks what a
// gang's pods say to each other when one of them is made again beside the
// others. Under restart policy None the Create pass makes a lost pod again
// at the recreation count the set already has, so the pods that stayed keep
// the hostnames and the peer addresses they were rendered with. The pod
// made again must fit them: its leader address and its TPU peer list name
// hostnames the other pods answer to, and it answers to the hostname their
// addresses name. A pod set therefore keeps one hostname form for its whole
// life, the pod name alone or the pod name with the recreation count,
// whichever its pods already have. A pod made in the other form finds no
// leader, or is found by no worker, and the gang never assembles.
func TestCreate_MemberMadeAgainKeepsThePeerAddressesResolving(t *testing.T) {
	for _, form := range hostnameForms {
		for _, lost := range []gangMember{threePodGang[0], threePodGang[2]} {
			t.Run(fmt.Sprintf("%s, %s-%d made again", form.name, lost.runner, lost.ordinal), func(t *testing.T) {
				input, _, target, _, c, isvc := createGateFixture(t, time.Now(), v1beta1.OMENativeInstanceStatus{
					Index: 0, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady,
				})
				input.DesiredSpec.PodSpec = tpuGangSpec("test:v1")
				plan := threePodGangPlan(workload.UpdateStrategySurgeThenDrain)
				plan.RestartPolicy = workload.RestartPolicyNone
				set := peerSet{client: c, namespace: isvc.Namespace, owner: isvc.Name, index: 0, count: 1,
					revisionHash: query.RevisionOf(target).Hash(), form: form}
				set.seed(t, lost)

				if _, err := Create(context.Background(), legacyTestDeps(c), *input, plan, target); err != nil {
					t.Fatalf("create: %v", err)
				}
				set.requireEveryAddressResolves(t)
			})
		}
	}
}

// TestCreate_MemberMadeAgainBesideAnEarlierRebuildKeepsThePeerAddressesResolving
// locks the same rule for a row that is one recreation count ahead of its
// live pods. A recreate roll moves the row to the next count before it
// drains and deletes the old set, so a roll that ends in between leaves the
// old pods running under a row of the new count, and the row can return to
// Ready over them. Under restart policy None the Create pass then makes a
// lost pod again at the row's count, beside pods rendered at the count
// before. The pod made again carries the row's count on its label and must
// still fit the pods it joins: its leader address and its TPU peer list
// name hostnames they answer to, and it answers to the hostname their
// addresses name.
func TestCreate_MemberMadeAgainBesideAnEarlierRebuildKeepsThePeerAddressesResolving(t *testing.T) {
	const rowCount, podCount = 2, 1
	for _, form := range hostnameForms {
		for _, lost := range []gangMember{threePodGang[0], threePodGang[2]} {
			t.Run(fmt.Sprintf("%s, %s-%d made again", form.name, lost.runner, lost.ordinal), func(t *testing.T) {
				input, _, target, _, c, isvc := createGateFixture(t, time.Now(), v1beta1.OMENativeInstanceStatus{
					Index: 0, Incarnation: rowCount, Phase: v1beta1.OMENativeInstanceReady,
				})
				input.DesiredSpec.PodSpec = tpuGangSpec("test:v1")
				plan := threePodGangPlan(workload.UpdateStrategySurgeThenDrain)
				plan.RestartPolicy = workload.RestartPolicyNone
				plan.Instances[0].Incarnation = rowCount
				set := peerSet{client: c, namespace: isvc.Namespace, owner: isvc.Name, index: 0, count: podCount,
					revisionHash: query.RevisionOf(target).Hash(), form: form}
				set.seed(t, lost)

				if _, err := Create(context.Background(), legacyTestDeps(c), *input, plan, target); err != nil {
					t.Fatalf("create: %v", err)
				}
				set.requireEveryAddressResolves(t)
				made := &corev1.Pod{}
				if err := c.Get(context.Background(), client.ObjectKey{Namespace: isvc.Namespace, Name: set.podName(lost)}, made); err != nil {
					t.Fatalf("the pod made again: %v", err)
				}
				if count, ok := query.InstanceIncarnationFromLabels(made); !ok || count != rowCount {
					t.Errorf("the pod made again names recreation count %d on its label (named: %t), want the row's count %d", count, ok, rowCount)
				}
			})
		}
	}
}

// rebuildInFlight stores a service whose engine Instance 0 is a three-pod
// gang in the middle of an attempt that builds a new pod set: the row the
// attempt wrote, at the recreation count the new set carries, and the
// stored revision the set renders. It returns the pass input and that
// revision.
func rebuildInFlight(t *testing.T, row func(revision string) v1beta1.OMENativeInstanceStatus) (client.Client, *v1beta1.InferenceService, workload.ReconcileInput, *appsv1.ControllerRevision) {
	t.Helper()
	legacyResetExpectations(t)
	isvc, ir := legacyISVCReadyAtIncarnation("llama-70b", "prod", 1)
	base := legacyNewFakeClient(t, isvc, ir)
	revision := legacyEnsureTargetCR(t, base, isvc, tpuGangSpec("test:v1"))
	ir.Status.InstanceStatuses[0] = row(revision.Name)
	if err := base.Status().Update(context.Background(), ir); err != nil {
		t.Fatalf("seed the attempt's row: %v", err)
	}
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	input.ObservedState.UpdateRevision = revision.Name
	input.DesiredSpec.PodSpec = tpuGangSpec("test:v1")
	return base, isvc, input, revision
}

// TestRestart_RebuiltSetLeftShortKeepsItsPeerAddressesResolving locks the
// same rule for a repair's rebuilt set. The repair rebuilds a gang whole at
// the next recreation count and creates whatever member of that set no pod
// holds the name of. A set an earlier pass left one worker short is
// finished beside the two pods it has, in the form those pods have.
func TestRestart_RebuiltSetLeftShortKeepsItsPeerAddressesResolving(t *testing.T) {
	for _, form := range hostnameForms {
		t.Run(form.name, func(t *testing.T) {
			base, isvc, input, revision := rebuildInFlight(t, func(revision string) v1beta1.OMENativeInstanceStatus {
				row := restartPhaseBRow()
				row.RunningRevision = revision
				return row
			})
			plan := threePodGangPlan(workload.UpdateStrategySurgeThenDrain)
			plan.RestartPolicy = workload.RestartPolicyRecreateInstance
			set := peerSet{client: base, namespace: isvc.Namespace, owner: isvc.Name, index: 0, count: 2,
				revisionHash: query.RevisionOf(revision).Hash(), form: form}
			set.seed(t, threePodGang[2])

			if _, err := Restart(context.Background(), legacyTestDeps(base), input, plan, plan.Instances[0], revision, "pod lost"); err != nil {
				t.Fatalf("Restart: %v", err)
			}
			set.requireEveryAddressResolves(t)
		})
	}
}

// TestRecreateUpdate_RebuiltSetLeftShortKeepsItsPeerAddressesResolving locks
// the rule for a RecreatePod roll, which deletes the old set and builds the
// new one at the next recreation count: a new set left one worker short is
// finished in the form its two pods have.
func TestRecreateUpdate_RebuiltSetLeftShortKeepsItsPeerAddressesResolving(t *testing.T) {
	for _, form := range hostnameForms {
		t.Run(form.name, func(t *testing.T) {
			base, isvc, input, target := rebuildInFlight(t, func(revision string) v1beta1.OMENativeInstanceStatus {
				return v1beta1.OMENativeInstanceStatus{
					Index: 0, Incarnation: 2, Phase: v1beta1.OMENativeInstanceUpdating, TargetRevision: revision,
					Operation: &v1beta1.InstanceOperation{
						Type: v1beta1.InstanceOperationUpdate, Step: workload.UpdateStepDrain, TargetRevision: revision,
					},
				}
			})
			plan := threePodGangPlan(workload.UpdateStrategyRecreatePod)
			set := peerSet{client: base, namespace: isvc.Namespace, owner: isvc.Name, index: 0, count: 2,
				revisionHash: query.RevisionOf(target).Hash(), form: form}
			set.seed(t, threePodGang[2])

			if _, err := Update(context.Background(), legacyTestDeps(base), input, plan, plan.Instances[0], target, input.DesiredSpec.PodSpec); err != nil {
				t.Fatalf("Update: %v", err)
			}
			set.requireEveryAddressResolves(t)
		})
	}
}

// TestGangSurge_ReplacementLeftShortKeepsItsPeerAddressesResolving locks the
// rule for a gang surge, which builds the replacement gang under its own
// Instance index: a replacement left one worker short is finished in the
// form its two pods have.
func TestGangSurge_ReplacementLeftShortKeepsItsPeerAddressesResolving(t *testing.T) {
	for _, form := range hostnameForms {
		t.Run(form.name, func(t *testing.T) {
			input, _, surgeIndex, revision := gangSurgeRejectionFixture(t)
			input.DesiredSpec.PodSpec = tpuGangSpec("example.com/app:v2")
			base := legacyNewFakeClient(t)
			plan := threePodGangPlan(workload.UpdateStrategySurgeThenDrain)
			target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}
			set := peerSet{client: base, namespace: input.Key.Namespace, owner: input.Key.OwnerName, index: surgeIndex, count: 1,
				revisionHash: query.RevisionOf(target).Hash(), form: form}
			set.seed(t, threePodGang[2])

			if _, err := gangSurgeUpdate(context.Background(), legacyTestDeps(base), input, plan, plan.Instances[0], target); err != nil {
				t.Fatalf("gangSurgeUpdate: %v", err)
			}
			set.requireEveryAddressResolves(t)
		})
	}
}

// TestMigrate_SurgeSetLeftShortKeepsItsPeerAddressesResolving locks the rule
// for a move, which builds the gang again under a surge index before the
// source drains: a surge set left one worker short is finished in the form
// its two pods have.
func TestMigrate_SurgeSetLeftShortKeepsItsPeerAddressesResolving(t *testing.T) {
	for _, form := range hostnameForms {
		t.Run(form.name, func(t *testing.T) {
			workerSpec := tpuGangSpec("llama:v1")
			workerSpec.Containers[0].Name = "worker"
			f := newGangMigFixtureWithWorkerSpec(t, workerSpec)
			const uuid = "mig-surge-left-short"
			f.records = []workload.MigrationRecord{mkMigRecord(uuid, 0, "node-a")}
			// The first pass accepts the request and the second creates the
			// surge set; its pods name the surge index and the revision.
			f.pass(t, uuid)
			f.pass(t, uuid)
			set := peerSet{client: f.c, namespace: f.isvc.Namespace, owner: f.isvc.Name, count: 1, form: form}
			created := 0
			for _, pod := range f.listPods(t) {
				index, ok := query.InstanceIdxFromLabels(pod)
				if !ok || index == 0 {
					continue
				}
				set.index, set.revisionHash = index, pod.Labels[query.LabelRevisionHash]
				if err := f.c.Delete(context.Background(), pod); err != nil {
					t.Fatalf("clear surge pod %s: %v", pod.Name, err)
				}
				created++
			}
			if created != len(threePodGang) {
				t.Fatalf("the second pass built %d surge pod(s), want %d", created, len(threePodGang))
			}
			set.seed(t, threePodGang[2])

			f.pass(t, uuid)
			set.requireEveryAddressResolves(t)
		})
	}
}
