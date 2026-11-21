package ops

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// ShortenGraceAfterLeaderGone gives each worker pod of a multi-pod Instance
// that still terminates after its leader pod has gone one more delete, with
// the replica's lifecycle.leaderGoneGracePeriod as its grace. It starts no
// deletion and never names a grace of zero, so a pod object stays until its
// kubelet reports the containers stopped. The pods come bucketed by Instance
// index, as the pass observed them, and one pod's error does not keep the
// others from the step.
func ShortenGraceAfterLeaderGone(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, byInstance map[int32][]*corev1.Pod) error {
	bound, ok := input.DesiredSpec.Lifecycle.LeaderGoneGraceSeconds()
	if !ok {
		return nil
	}
	indices := make([]int32, 0, len(byInstance))
	for idx := range byInstance {
		indices = append(indices, idx)
	}
	slices.Sort(indices)
	var errs []error
	for _, idx := range indices {
		if err := shortenInstanceGrace(ctx, deps, input, idx, byInstance[idx], bound); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// shortenedPods are the pods of one Instance that one pass asked to stop on
// the shorter grace, with the grace each showed before.
type shortenedPods struct {
	names  []string
	graces []int64
}

func (s *shortenedPods) add(pod *corev1.Pod) {
	s.names = append(s.names, pod.Name)
	s.graces = append(s.graces, *pod.DeletionGracePeriodSeconds)
}

// shortenInstanceGrace applies the step to the pods of one Instance, in name
// order so the writes land in the same order on every pass. It records one
// event for the pods that take the shorter grace as named, and one for the
// pods whose first delete is older than that grace.
func shortenInstanceGrace(ctx context.Context, deps workload.Deps, input workload.ReconcileInput, idx int32, instancePods []*corev1.Pod, bound int64) error {
	pods := make([]*corev1.Pod, 0, len(instancePods))
	for _, pod := range instancePods {
		if pod != nil {
			pods = append(pods, pod)
		}
	}
	slices.SortFunc(pods, func(a, b *corev1.Pod) int { return strings.Compare(a.Name, b.Name) })
	now := input.Now()
	var (
		errs           []error
		inTime, behind shortenedPods
	)
	for _, pod := range pods {
		if !leaderGoneGraceApplies(pod, pods, bound, now) || input.Swept.Swept(pod.UID) {
			continue
		}
		done, err := shortenPodGrace(ctx, deps, pod, bound)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if !done {
			continue
		}
		if firstDeleteOlderThan(pod, bound, now) {
			behind.add(pod)
		} else {
			inTime.add(pod)
		}
	}
	recordLeaderGoneGrace(deps, input, idx, inTime, bound, false)
	recordLeaderGoneGrace(deps, input, idx, behind, bound, true)
	return errors.Join(errs...)
}

// recordLeaderGoneGrace records the event for the pods of one outcome, and
// nothing when there are none.
func recordLeaderGoneGrace(deps workload.Deps, input workload.ReconcileInput, idx int32, pods shortenedPods, bound int64, behind bool) {
	if len(pods.names) == 0 {
		return
	}
	workload.RecordNormal(deps.Recorder, workload.EventTarget(input), workload.EventReasonLeaderGoneGraceShortened,
		"%s", leaderGoneGraceText(input.Key.Component, idx, pods, bound, behind))
}

// leaderGoneGraceApplies reports whether pod, one of instancePods, is a
// worker that is still terminating after the leader built with it has gone,
// and that shows more than bound seconds of grace with more than bound
// seconds of it left at now.
//
// A pod that shows no grace is left alone: the API server removes such a
// pod at once on any further delete. A pod whose own grace ends within the
// bound is left alone too: its kubelet counts a shortened grace from the
// instant it learns of it, which would be later than the pod's own end. A
// worker that has stopped has nothing left to stop.
func leaderGoneGraceApplies(pod *corev1.Pod, instancePods []*corev1.Pod, bound int64, now time.Time) bool {
	if pod == nil || pod.DeletionTimestamp == nil || pod.UID == "" || query.IsTerminalPod(pod) {
		return false
	}
	if pod.Labels[query.LabelRunner] != workload.RunnerWorker {
		return false
	}
	if grace := pod.DeletionGracePeriodSeconds; grace == nil || *grace <= bound {
		return false
	}
	if !pod.DeletionTimestamp.Time.After(now.Add(time.Duration(bound) * time.Second)) {
		return false
	}
	return leaderGone(pod, instancePods)
}

// leaderGone reports whether the leader pod built together with worker is
// gone. It is gone when no leader among instancePods carries worker's
// recreation count, and when the one that does is in a terminal phase,
// which its kubelet reports once every container has stopped. A leader of
// a later count belongs to the set that replaces worker's. A worker or a
// leader whose count cannot be read leaves no set to compare, and the
// worker is read as still led.
func leaderGone(worker *corev1.Pod, instancePods []*corev1.Pod) bool {
	count, ok := query.InstanceIncarnationFromLabels(worker)
	if !ok {
		return false
	}
	for _, pod := range instancePods {
		if pod == nil || pod.Labels[query.LabelRunner] != workload.RunnerLeader {
			continue
		}
		leaderCount, ok := query.InstanceIncarnationFromLabels(pod)
		if !ok {
			return false
		}
		if leaderCount == count {
			return query.IsTerminalPod(pod)
		}
	}
	return true
}

// firstDeleteOlderThan reports whether pod's first delete is more than bound
// seconds old at now. That instant is the pod's deletion time less the grace
// the pod shows. The API server counts a shorter grace from it, and leaves a
// pod whose shorter grace would end in the past a grace of one second.
func firstDeleteOlderThan(pod *corev1.Pod, bound int64, now time.Time) bool {
	first := pod.DeletionTimestamp.Add(-time.Duration(*pod.DeletionGracePeriodSeconds) * time.Second)
	return first.Add(time.Duration(bound) * time.Second).Before(now)
}

// shortenPodGrace gives a terminating pod one more delete that names bound
// seconds of grace, under the pod's UID so a pod created since at the same
// name is never touched. It reads the pod from the API server first and
// sends nothing when the pod there is another one or already shows a grace
// at or under bound: the pass may hold the pod as it was before an earlier
// write. It records no expected delete: the watch reports the pod's removal
// once, and the delete that began it expects that. It reports false when
// nothing was written.
func shortenPodGrace(ctx context.Context, deps workload.Deps, pod *corev1.Pod, bound int64) (bool, error) {
	live := &corev1.Pod{}
	if err := deps.Reader().Get(ctx, client.ObjectKeyFromObject(pod), live); err != nil {
		if apierrors.IsNotFound(err) {
			return false, nil
		}
		return false, fmt.Errorf("read pod %s/%s before shortening its termination grace: %w", pod.Namespace, pod.Name, err)
	}
	if live.UID != pod.UID {
		return false, nil
	}
	if grace := live.DeletionGracePeriodSeconds; grace != nil && *grace <= bound {
		return false, nil
	}
	uid := pod.UID
	err := deps.Client.Delete(ctx, pod, client.GracePeriodSeconds(bound), client.Preconditions{UID: &uid})
	if err == nil {
		return true, nil
	}
	if apierrors.IsNotFound(err) || apierrors.IsConflict(err) {
		return false, nil
	}
	return false, fmt.Errorf("shorten the termination grace of pod %s/%s: %w", pod.Namespace, pod.Name, err)
}

// leaderGoneGraceText says, for the event, which pods of an Instance were
// asked to stop on the shorter grace, the grace each showed, and when their
// kubelets stop them. behind is set for pods whose first delete is older
// than the shorter grace, which the API server leaves a grace of one second.
func leaderGoneGraceText(component workload.ComponentType, idx int32, pods shortenedPods, bound int64, behind bool) string {
	plural := len(pods.names) != 1
	still := "1 pod is"
	if plural {
		still = fmt.Sprintf("%d pods are", len(pods.names))
	}
	had := fmt.Sprintf("%ds", slices.Max(pods.graces))
	if slices.Min(pods.graces) != slices.Max(pods.graces) {
		had = "up to " + had
	}
	head := fmt.Sprintf("OMENative %s: the leader pod of this gang is gone, and %s still terminating: %s. ",
		workload.InstanceKey(component, idx), still, strings.Join(pods.names, ", "))
	const why = "because a pod of a gang cannot finish its work without the leader. "
	switch {
	case behind && plural:
		return head + fmt.Sprintf("OME asked for a termination grace of %ds (lifecycle.leaderGoneGracePeriod) in place of %s, "+why+
			"The pods were deleted more than %ds ago, so the API server leaves them a grace of 1s and each kubelet sends SIGKILL about 2s from now.",
			bound, had, bound)
	case behind:
		return head + fmt.Sprintf("OME asked for a termination grace of %ds (lifecycle.leaderGoneGracePeriod) in place of %s, "+why+
			"The pod was deleted more than %ds ago, so the API server leaves it a grace of 1s and its kubelet sends SIGKILL about 2s from now.",
			bound, had, bound)
	case plural:
		return head + fmt.Sprintf("OME shortened their termination grace from %s to %ds (lifecycle.leaderGoneGracePeriod), "+why+
			"Each kubelet counts the %ds from the moment it learns of them and then sends SIGKILL.",
			had, bound, bound)
	}
	return head + fmt.Sprintf("OME shortened its termination grace from %s to %ds (lifecycle.leaderGoneGracePeriod), "+why+
		"Its kubelet counts the %ds from the moment it learns of them and then sends SIGKILL.",
		had, bound, bound)
}
