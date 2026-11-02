// Package testfixture builds the pods, rows, objects and inputs the
// workload engine's tests read, so that every package under workload
// builds its fixtures from one place instead of declaring its own copy.
// Nothing in production imports it. Every builder returns a value; the
// callers mutate what they get.
package testfixture

import (
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/podreadiness"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Epoch is the fixed clock the fixtures measure their timestamps from: a
// serving pod has been Ready for an hour at Epoch and was created two
// hours before it. A test that drives a fake clock starts it here.
var Epoch = time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

const (
	// RevisionHashA is the revision hash a ServingPod carries.
	RevisionHashA = "0a1b2c3d"
	// RevisionHashB is a second revision hash for a pod on another revision.
	RevisionHashB = "4e5f6a7b"
	// ContainerName is the runner container's name in the Instance pods and
	// pod specs of the update fixtures (PodForInstance, PodSpecWithImage,
	// ReconcileInput). ServingPod and RunningNotReadyPod name theirs
	// constants.MainContainerName, the name the engine's pod readers match.
	ContainerName = "main"
)

// PodOption changes one pod in place; Pod applies them in order.
type PodOption func(*corev1.Pod)

// Pod builds a ServingPod and applies the options in order, so a case
// reads as "this shape, that change" rather than as a page of pod status.
func Pod(name string, opts ...PodOption) *corev1.Pod {
	pod := ServingPod(name)
	for _, opt := range opts {
		opt(pod)
	}
	return pod
}

// Pods collects pods into the slice the readers take.
func Pods(pods ...*corev1.Pod) []*corev1.Pod { return pods }

// ServingPod is the healthy shape: Running, its runner container ready,
// PodReady and the serving gate True for an hour before Epoch, on
// RevisionHashA as the default runner of a single-pod Instance.
func ServingPod(name string) *corev1.Pod {
	t := metav1.NewTime(Epoch.Add(-time.Hour))
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(Epoch.Add(-2 * time.Hour)),
			Labels: map[string]string{
				query.LabelRevisionHash: RevisionHashA,
				query.LabelRunner:       types.RunnerDefault,
			},
		},
		Spec: corev1.PodSpec{
			ReadinessGates: []corev1.PodReadinessGate{{ConditionType: podreadiness.ConditionType}},
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{
				{Type: corev1.ContainersReady, Status: corev1.ConditionTrue, LastTransitionTime: t},
				{Type: podreadiness.ConditionType, Status: corev1.ConditionTrue, LastTransitionTime: t},
				{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: t},
			},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  constants.MainContainerName,
				Ready: true,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// SetCondition writes one pod condition with its transition time, adding
// the condition when the pod has none of that type.
func SetCondition(pod *corev1.Pod, t corev1.PodConditionType, status corev1.ConditionStatus, at time.Time) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == t {
			pod.Status.Conditions[i].Status = status
			pod.Status.Conditions[i].LastTransitionTime = metav1.NewTime(at)
			return
		}
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{Type: t, Status: status, LastTransitionTime: metav1.NewTime(at)})
}

// DropCondition removes every condition of the given type.
func DropCondition(pod *corev1.Pod, t corev1.PodConditionType) {
	kept := pod.Status.Conditions[:0]
	for _, c := range pod.Status.Conditions {
		if c.Type != t {
			kept = append(kept, c)
		}
	}
	pod.Status.Conditions = kept
}

// Runner and label shapes.

// AsWorker labels the pod as a gang's worker runner.
func AsWorker(pod *corev1.Pod) { pod.Labels[query.LabelRunner] = types.RunnerWorker }

// AsLeader labels the pod as a gang's leader runner.
func AsLeader(pod *corev1.Pod) { pod.Labels[query.LabelRunner] = types.RunnerLeader }

// NoRevision removes the revision label, the shape of a pod the engine did
// not render.
func NoRevision(pod *corev1.Pod) { delete(pod.Labels, query.LabelRevisionHash) }

// OnRevision puts the pod on the given revision hash.
func OnRevision(hash string) PodOption {
	return func(pod *corev1.Pod) { pod.Labels[query.LabelRevisionHash] = hash }
}

// WithPairingProtocol labels the pod with a prefill/decode pairing protocol.
func WithPairingProtocol(protocol string) PodOption {
	return func(pod *corev1.Pod) { pod.Labels[query.LabelPairingProtocol] = protocol }
}

// Gate shapes.

// GateAbsent is a pod whose serving condition was never written while the
// kubelet reports PodReady True: the shape coordination counts as serving
// and podreadiness does not.
func GateAbsent(pod *corev1.Pod) { DropCondition(pod, podreadiness.ConditionType) }

// GateNotFolded is the pass that wrote the gate: the gate reads True but
// the kubelet has not folded it into PodReady yet.
func GateNotFolded(pod *corev1.Pod) { SetCondition(pod, corev1.PodReady, corev1.ConditionFalse, Epoch) }

// GateHeldBy is a pod a writer holds out of rotation, with PodReady already
// following the gate.
func GateHeldBy(writer, key string) PodOption {
	return func(pod *corev1.Pod) {
		SetCondition(pod, podreadiness.ConditionType, corev1.ConditionFalse, Epoch)
		SetCondition(pod, corev1.PodReady, corev1.ConditionFalse, Epoch)
		for i := range pod.Status.Conditions {
			if pod.Status.Conditions[i].Type == podreadiness.ConditionType {
				pod.Status.Conditions[i].Message = fmt.Sprintf(`[{"userAgent":%q,"key":%q}]`, writer, key)
			}
		}
	}
}

// NodeLost is a serving pod whose kubelet stopped reporting: the node
// lifecycle controller set PodReady False and left everything else.
func NodeLost(pod *corev1.Pod) { SetCondition(pod, corev1.PodReady, corev1.ConditionFalse, Epoch) }

// Readiness shapes.

// ReadySince is a serving pod whose PodReady condition turned True at the
// given instant, so its availability window is measured from there.
func ReadySince(at time.Time) PodOption {
	return func(pod *corev1.Pod) { SetCondition(pod, corev1.PodReady, corev1.ConditionTrue, at) }
}

// ReadyWithoutTransitionTime clears the PodReady transition time, the
// shape of a condition an older kubelet wrote.
func ReadyWithoutTransitionTime(pod *corev1.Pod) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type == corev1.PodReady {
			pod.Status.Conditions[i].LastTransitionTime = metav1.Time{}
		}
	}
}

// UnreadySince is a promoted pod whose readiness probe stopped passing at
// since: the gate stays True, ContainersReady and PodReady are False.
func UnreadySince(since time.Time) PodOption {
	return func(pod *corev1.Pod) {
		SetCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, since)
		SetCondition(pod, corev1.PodReady, corev1.ConditionFalse, since)
		pod.Status.ContainerStatuses[0].Ready = false
	}
}

// NeverReady is a pod that is still coming up: Running, its container not
// ready since its creation, no gate written yet.
func NeverReady(pod *corev1.Pod) {
	UnreadySince(pod.CreationTimestamp.Time)(pod)
	GateAbsent(pod)
}

// ContainerWaiting is a pod unready for ten minutes whose container the
// kubelet holds in the waiting state with the given reason.
func ContainerWaiting(reason string) PodOption {
	return func(pod *corev1.Pod) {
		UnreadySince(Epoch.Add(-10 * time.Minute))(pod)
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: reason}}
	}
}

// CreateReservedMessage is the message a container runtime writes on a
// CreateContainerError, in the shape it writes it, when the kubelet's retry
// finds the container name still reserved by a first create the runtime
// has not finished.
const CreateReservedMessage = `failed to reserve container name "ome-container_example-model-engine-0-default-0_team-a_00000000-0000-4000-8000-000000000001_0"; ` +
	`check if another CreateContainer request is in progress: ` +
	`name "ome-container_example-model-engine-0-default-0_team-a_00000000-0000-4000-8000-000000000001_0" ` +
	`is reserved for "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"`

// ContainersUnready is a pod whose ContainersReady reads False while
// PodReady and the gate still read True, a shape no kubelet writes.
func ContainersUnready(pod *corev1.Pod) {
	SetCondition(pod, corev1.ContainersReady, corev1.ConditionFalse, Epoch.Add(-2*time.Minute))
}

// Restart shapes.

// RestartedAt is a pod whose runner container the kubelet restarted at the
// given instant: its last run ended then and its current run began then.
func RestartedAt(at time.Time) PodOption {
	return func(pod *corev1.Pod) {
		cs := &pod.Status.ContainerStatuses[0]
		cs.RestartCount++
		cs.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error", FinishedAt: metav1.NewTime(at),
		}}
		cs.State = corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: metav1.NewTime(at)}}
	}
}

// ExitedAt is a promoted pod whose runner container exited at the given
// instant and has not been started again: readiness failed then, the gate
// stays True.
func ExitedAt(at time.Time) PodOption {
	return func(pod *corev1.Pod) {
		UnreadySince(at)(pod)
		pod.Status.ContainerStatuses[0].State = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: "Error", FinishedAt: metav1.NewTime(at),
		}}
	}
}

// CreatedAt moves the pod's creation to the given instant.
func CreatedAt(at time.Time) PodOption {
	return func(pod *corev1.Pod) { pod.CreationTimestamp = metav1.NewTime(at) }
}

// Lifecycle shapes.

// Failed is a terminal pod whose conditions were never updated after the
// phase moved, so they still read healthy.
func Failed(pod *corev1.Pod) { pod.Status.Phase = corev1.PodFailed }

// Succeeded is a pod whose containers all exited with success.
func Succeeded(pod *corev1.Pod) { pod.Status.Phase = corev1.PodSucceeded }

// Terminating is a pod whose deletion the apiserver recorded at Epoch.
func Terminating(pod *corev1.Pod) {
	ts := metav1.NewTime(Epoch)
	pod.DeletionTimestamp = &ts
}

// Pending is a pod the kubelet has not started: no conditions, no
// container status.
func Pending(pod *corev1.Pod) {
	pod.Status = corev1.PodStatus{Phase: corev1.PodPending}
}

// Standalone shapes.

// RunningNotReadyPod is the shape of a pod that runs but has never been
// ready: phase Running, every container started, ContainersReady False
// since the given instant with the kubelet's own reason and message, and
// created a minute before that.
func RunningNotReadyPod(name string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodRunning,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.ContainersReady,
				Status:             corev1.ConditionFalse,
				Reason:             "ContainersNotReady",
				Message:            fmt.Sprintf("containers with unready status: [%s]", constants.MainContainerName),
				LastTransitionTime: metav1.NewTime(since),
			}},
			ContainerStatuses: []corev1.ContainerStatus{{
				Name:  constants.MainContainerName,
				Ready: false,
				State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
			}},
		},
	}
}

// UnschedulablePod is a Pending pod the scheduler reports it cannot place,
// with the scheduler's message, since the given instant.
func UnschedulablePod(name, message string, since time.Time) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			CreationTimestamp: metav1.NewTime(since.Add(-time.Minute)),
		},
		Status: corev1.PodStatus{
			Phase: corev1.PodPending,
			Conditions: []corev1.PodCondition{{
				Type:               corev1.PodScheduled,
				Status:             corev1.ConditionFalse,
				Reason:             types.WaitingReasonUnschedulable,
				Message:            message,
				LastTransitionTime: metav1.NewTime(since),
			}},
		},
	}
}
