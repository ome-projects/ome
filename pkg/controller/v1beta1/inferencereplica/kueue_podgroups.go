package inferencereplica

import (
	"context"
	"fmt"
	"maps"
	"slices"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// releasingKueuePodGroups runs one reconcile of ir, the teardown or the pass,
// and then lets ir's pods that are being deleted leave their Kueue pod groups.
// It runs on every reconcile, whatever the reconcile returns, because Kueue
// would otherwise hold those pods, and their quota, until a replacement that
// cannot be created while they exist. It runs after the reconcile so that the
// reconcile acts on the first pod list it takes from the cache. A release
// failure is returned only when the reconcile itself succeeds, so it never
// hides the reconcile's own error.
func (r *Reconciler) releasingKueuePodGroups(ctx context.Context, ir *v1beta1.InferenceReplica, reconcile func() (ctrl.Result, error)) (ctrl.Result, error) {
	result, err := reconcile()
	if releaseErr := r.releaseKueuePodGroups(ctx, ir); releaseErr != nil && err == nil {
		return result, fmt.Errorf("InferenceReplica reconciler: release Kueue pod groups: %w", releaseErr)
	}
	return result, err
}

// releaseKueuePodGroups lets ir's pods that are being deleted leave the Kueue
// pod groups OME put them in. Kueue keeps a finalizer on every pod of a group
// until a replacement pod joins the group or the group's Workload is deleted.
// OME recreates a pod under the same name, so the replacement cannot exist
// until the old pod is gone: left alone, a rebuilt or torn-down Instance
// waits forever, and its Workload keeps the quota.
//
// So for every pod of an OME-named group that is being deleted, OME removes
// Kueue's finalizer, as Kueue's own StatefulSet integration does for its
// pods. When every observed pod of a group is being deleted, OME also deletes
// the group's Workload, which returns the quota at once instead of when
// garbage collection reaches it.
//
// Pods are read from the cache. A pod the cache does not show yet keeps its
// group's Workload, through the owner check in deleteKueuePodGroupWorkload.
func (r *Reconciler) releaseKueuePodGroups(ctx context.Context, ir *v1beta1.InferenceReplica) error {
	component := v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component)
	pods, err := query.ListCachedOMENativePodsByName(ctx, r.Client, ir.Namespace, ir.NamePrefix(), component)
	if err != nil {
		return fmt.Errorf("list pods for Kueue pod groups: %w", err)
	}
	groups := kueuePodGroups(podsControlledBy(pods, ir.UID), ir.NamePrefix(), component)
	for _, name := range slices.Sorted(maps.Keys(groups)) {
		members := groups[name]
		if query.AllTerminating(members) {
			if err := r.deleteKueuePodGroupWorkload(ctx, ir.Namespace, name, members); err != nil {
				return err
			}
		}
		for _, pod := range members {
			if pod.DeletionTimestamp == nil {
				continue
			}
			if err := r.dropKueuePodFinalizer(ctx, pod); err != nil {
				return err
			}
		}
	}
	return nil
}

// kueuePodGroups buckets pods by the Kueue pod group OME gave them. A pod
// joins a bucket only when its group label is the name OME renders for the
// pod's own Instance and build, so a group that a pod template names itself
// is left to whoever named it.
func kueuePodGroups(pods []*corev1.Pod, isvc string, component workloadtypes.ComponentType) map[string][]*corev1.Pod {
	groups := make(map[string][]*corev1.Pod)
	for _, pod := range pods {
		name := pod.Labels[constants.KueuePodGroupNameLabelKey]
		if name == "" {
			continue
		}
		idx, ok := query.InstanceIdxFromLabels(pod)
		if !ok {
			continue
		}
		recreations, ok := query.InstanceIncarnationFromLabels(pod)
		if !ok || name != query.KueuePodGroupName(isvc, component, idx, recreations) {
			continue
		}
		groups[name] = append(groups[name], pod)
	}
	return groups
}

// deleteKueuePodGroupWorkload deletes the Workload Kueue made for the pod group
// name, provided it is that group's and every pod it names is going or gone.
// Kueue gives a group's Workload the group's name and one non-controller owner
// reference per member pod. members are the pods OME saw being deleted. A pod
// that OME let go in an earlier pass is not among them, so every other owner
// is read live. A Workload with a controller is left alone: it belongs to
// another job. So is a Workload with an owner that still counts for the
// group: that pod joined the group after the cached read.
func (r *Reconciler) deleteKueuePodGroupWorkload(ctx context.Context, namespace, name string, members []*corev1.Pod) error {
	wl := &kueuev1beta2.Workload{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: name}, wl); err != nil {
		// A cluster or a client without Kueue's Workload kind has no Workload
		// to delete; the pods' finalizers still come off below.
		if apierrors.IsNotFound(err) || apimeta.IsNoMatchError(err) || k8sruntime.IsNotRegisteredError(err) {
			return nil
		}
		return fmt.Errorf("get Kueue Workload %s/%s: %w", namespace, name, err)
	}
	if wl.DeletionTimestamp != nil {
		return nil
	}
	others, owned := podOwnersOutside(wl.OwnerReferences, members)
	if !owned {
		return nil
	}
	for _, owner := range others {
		gone, err := r.kueuePodGroupOwnerGone(ctx, namespace, name, owner)
		if err != nil {
			return err
		}
		if !gone {
			return nil
		}
	}
	uid := wl.UID
	if err := r.Client.Delete(ctx, wl, client.Preconditions{UID: &uid}); err != nil &&
		!apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return fmt.Errorf("delete Kueue Workload %s/%s: %w", namespace, name, err)
	}
	return nil
}

// podOwnersOutside reads the owner references of a pod group's Workload. It
// returns the pod owners that are not among pods, and whether refs are a pod
// group's at all: no controller, only pod owners, and at least one of pods.
func podOwnersOutside(refs []metav1.OwnerReference, pods []*corev1.Pod) ([]metav1.OwnerReference, bool) {
	uids := make(map[types.UID]struct{}, len(pods))
	for _, pod := range pods {
		uids[pod.UID] = struct{}{}
	}
	var others []metav1.OwnerReference
	owned := false
	for _, ref := range refs {
		if (ref.Controller != nil && *ref.Controller) || ref.Kind != "Pod" {
			return nil, false
		}
		if _, ok := uids[ref.UID]; ok {
			owned = true
			continue
		}
		others = append(others, ref)
	}
	return others, owned
}

// kueuePodGroupOwnerGone reports whether owner, a pod that the Workload of pod
// group names, is gone from that group. It reads the pod's name live. The
// owner is gone when no pod holds the name. OME recreates a pod under the same
// name, so another pod may hold it. A replacement inside the same group keeps
// the Workload. A pod of the Instance's next build has a group and a Workload
// of its own.
func (r *Reconciler) kueuePodGroupOwnerGone(ctx context.Context, namespace, group string, owner metav1.OwnerReference) (bool, error) {
	pod := &corev1.Pod{}
	if err := r.APIReader.Get(ctx, types.NamespacedName{Namespace: namespace, Name: owner.Name}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("get pod %s/%s: %w", namespace, owner.Name, err)
	}
	return pod.UID != owner.UID && pod.Labels[constants.KueuePodGroupNameLabelKey] != group, nil
}

// dropKueuePodFinalizer removes Kueue's finalizer from a pod being deleted.
// The pod comes from the cache, so the patch works on a copy, and it carries
// the read's resourceVersion: a pod changed since is left for the next pass
// instead of overwritten.
func (r *Reconciler) dropKueuePodFinalizer(ctx context.Context, pod *corev1.Pod) error {
	if !controllerutil.ContainsFinalizer(pod, constants.KueuePodFinalizer) {
		return nil
	}
	patched := pod.DeepCopy()
	controllerutil.RemoveFinalizer(patched, constants.KueuePodFinalizer)
	if err := r.Client.Patch(ctx, patched, client.MergeFromWithOptions(pod, client.MergeFromWithOptimisticLock{})); err != nil &&
		!apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
		return fmt.Errorf("remove Kueue finalizer from pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return nil
}
