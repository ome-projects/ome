package inferencereplica

import (
	"context"
	"errors"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

const (
	podGroupTestNamespace = "team-a"
	podGroupTestService   = "example-model"
)

// podGroupTestReplica is the engine replica of podGroupTestService.
func podGroupTestReplica() *v1beta1.InferenceReplica {
	return &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Name:      podGroupTestService + "-engine",
			Namespace: podGroupTestNamespace,
			UID:       types.UID("replica-uid"),
		},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: podGroupTestService},
			Component: v1beta1.EngineComponent,
		},
	}
}

// groupPodOptions describes one pod of a Kueue pod group.
type groupPodOptions struct {
	name        string
	group       string
	recreations string
	controller  types.UID
	deleting    bool
}

// groupPod returns a pod of Instance 0 of the replica's engine that carries
// Kueue's finalizer and the given Kueue pod group.
func groupPod(opts groupPodOptions) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      opts.name,
			Namespace: podGroupTestNamespace,
			UID:       types.UID(opts.name + "-uid"),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: podGroupTestService,
				constants.OMEComponentLabel:           string(workloadtypes.ComponentEngine),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelInstanceIdx:                "0",
				query.LabelInstanceIncarnation:        opts.recreations,
				constants.KueuePodGroupNameLabelKey:   opts.group,
			},
			Finalizers: []string{constants.KueuePodFinalizer},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceReplica",
				Name:       podGroupTestService + "-engine",
				UID:        opts.controller,
				Controller: ptr.To(true),
			}},
		},
	}
	if opts.deleting {
		pod.DeletionTimestamp = ptr.To(metav1.NewTime(time.Now()))
	}
	return pod
}

// groupWorkload returns the Workload Kueue makes for a pod group: named after
// the group and owned, without a controller, by each member pod.
func groupWorkload(name string, members ...*corev1.Pod) *kueuev1beta2.Workload {
	wl := &kueuev1beta2.Workload{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: podGroupTestNamespace}}
	for _, pod := range members {
		wl.OwnerReferences = append(wl.OwnerReferences, metav1.OwnerReference{
			APIVersion: "v1", Kind: "Pod", Name: pod.Name, UID: pod.UID,
		})
	}
	return wl
}

func podGroupTestReconciler(t *testing.T, objs ...client.Object) (*Reconciler, client.Client) {
	t.Helper()
	scheme := testScheme(t)
	if err := kueuev1beta2.AddToScheme(scheme); err != nil {
		t.Fatalf("kueuev1beta2.AddToScheme: %v", err)
	}
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()
	return &Reconciler{Client: c, APIReader: c, Log: logf.Log.WithName("test")}, c
}

// hasKueueFinalizer reports whether the pod still exists and still carries
// Kueue's finalizer. A deleting pod whose last finalizer was removed is gone.
func hasKueueFinalizer(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	pod := &corev1.Pod{}
	err := c.Get(context.Background(), client.ObjectKey{Namespace: podGroupTestNamespace, Name: name}, pod)
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatalf("get pod %s: %v", name, err)
	}
	for _, f := range pod.Finalizers {
		if f == constants.KueuePodFinalizer {
			return true
		}
	}
	return false
}

func workloadExists(t *testing.T, c client.Client, name string) bool {
	t.Helper()
	err := c.Get(context.Background(), client.ObjectKey{Namespace: podGroupTestNamespace, Name: name}, &kueuev1beta2.Workload{})
	if apierrors.IsNotFound(err) {
		return false
	}
	if err != nil {
		t.Fatalf("get Workload %s: %v", name, err)
	}
	return true
}

// TestReleaseKueuePodGroups_DeletedGangReturnsItsQuota checks the teardown of a
// whole gang: every pod being deleted loses Kueue's finalizer, and the group's
// Workload is deleted so its quota comes back at once.
func TestReleaseKueuePodGroups_DeletedGangReturnsItsQuota(t *testing.T) {
	ir := podGroupTestReplica()
	group := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 1)
	leader := groupPod(groupPodOptions{name: "leader", group: group, recreations: "1", controller: ir.UID, deleting: true})
	worker := groupPod(groupPodOptions{name: "worker", group: group, recreations: "1", controller: ir.UID, deleting: true})
	r, c := podGroupTestReconciler(t, leader, worker, groupWorkload(group, leader, worker))

	if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
		t.Fatalf("releaseKueuePodGroups: %v", err)
	}
	for _, name := range []string{"leader", "worker"} {
		if hasKueueFinalizer(t, c, name) {
			t.Errorf("pod %s still carries Kueue's finalizer", name)
		}
	}
	if workloadExists(t, c, group) {
		t.Errorf("Workload %s of a fully deleted gang still exists", group)
	}
}

// TestReleaseKueuePodGroups_GangSeenGoingOnePodAtATime checks the teardown of
// a gang whose pods the cache shows being deleted one after the other. The
// first pass sees the leader going and the worker running, so it keeps the
// Workload and lets the leader go. The second pass sees only the worker, and
// the Workload still names the leader that is gone. That pass deletes
// the Workload, so the quota comes back without the garbage collector.
func TestReleaseKueuePodGroups_GangSeenGoingOnePodAtATime(t *testing.T) {
	ctx := context.Background()
	ir := podGroupTestReplica()
	group := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 1)
	leader := groupPod(groupPodOptions{name: "leader", group: group, recreations: "1", controller: ir.UID, deleting: true})
	worker := groupPod(groupPodOptions{name: "worker", group: group, recreations: "1", controller: ir.UID})
	r, c := podGroupTestReconciler(t, leader, worker, groupWorkload(group, leader, worker))

	if err := r.releaseKueuePodGroups(ctx, ir); err != nil {
		t.Fatalf("releaseKueuePodGroups, first pass: %v", err)
	}
	if hasKueueFinalizer(t, c, "leader") {
		t.Fatal("the deleted leader still carries Kueue's finalizer")
	}
	if !workloadExists(t, c, group) {
		t.Fatalf("Workload %s was deleted while the worker still ran", group)
	}

	if err := c.Delete(ctx, worker); err != nil {
		t.Fatalf("delete the worker: %v", err)
	}
	if err := r.releaseKueuePodGroups(ctx, ir); err != nil {
		t.Fatalf("releaseKueuePodGroups, second pass: %v", err)
	}
	if hasKueueFinalizer(t, c, "worker") {
		t.Error("the deleted worker still carries Kueue's finalizer")
	}
	if workloadExists(t, c, group) {
		t.Errorf("Workload %s still exists after both of its pods are gone", group)
	}
}

// TestReleaseKueuePodGroups_OneReplacedPodKeepsTheGroup checks a pod recreated
// inside a running gang: the deleted pod loses Kueue's finalizer so its
// replacement can be created under the same name, while the running pod and
// the group's admitted Workload stay as they are.
func TestReleaseKueuePodGroups_OneReplacedPodKeepsTheGroup(t *testing.T) {
	ir := podGroupTestReplica()
	group := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 1)
	leader := groupPod(groupPodOptions{name: "leader", group: group, recreations: "1", controller: ir.UID})
	worker := groupPod(groupPodOptions{name: "worker", group: group, recreations: "1", controller: ir.UID, deleting: true})
	r, c := podGroupTestReconciler(t, leader, worker, groupWorkload(group, leader, worker))

	if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
		t.Fatalf("releaseKueuePodGroups: %v", err)
	}
	if hasKueueFinalizer(t, c, "worker") {
		t.Error("the deleted worker still carries Kueue's finalizer")
	}
	if !hasKueueFinalizer(t, c, "leader") {
		t.Error("the running leader lost Kueue's finalizer")
	}
	if !workloadExists(t, c, group) {
		t.Errorf("Workload %s of a running gang was deleted", group)
	}
}

// TestReleaseKueuePodGroups_LeavesGroupsOMEDidNotName checks what the step does
// not touch: a group name that is not the one OME renders for the pod's
// Instance and build, a pod another owner controls, and a Workload that has a
// controller of its own.
func TestReleaseKueuePodGroups_LeavesGroupsOMEDidNotName(t *testing.T) {
	ir := podGroupTestReplica()
	ownGroup := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 1)

	t.Run("group named by the pod template", func(t *testing.T) {
		pod := groupPod(groupPodOptions{name: "named", group: "team-group", recreations: "1", controller: ir.UID, deleting: true})
		r, c := podGroupTestReconciler(t, pod, groupWorkload("team-group", pod))
		if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
			t.Fatalf("releaseKueuePodGroups: %v", err)
		}
		if !hasKueueFinalizer(t, c, "named") || !workloadExists(t, c, "team-group") {
			t.Error("a group the pod template named was changed")
		}
	})

	t.Run("group name of an earlier build", func(t *testing.T) {
		pod := groupPod(groupPodOptions{name: "earlier", group: ownGroup, recreations: "2", controller: ir.UID, deleting: true})
		r, c := podGroupTestReconciler(t, pod, groupWorkload(ownGroup, pod))
		if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
			t.Fatalf("releaseKueuePodGroups: %v", err)
		}
		if !hasKueueFinalizer(t, c, "earlier") || !workloadExists(t, c, ownGroup) {
			t.Error("a pod whose group does not match its own build was changed")
		}
	})

	t.Run("pod another owner controls", func(t *testing.T) {
		pod := groupPod(groupPodOptions{name: "other", group: ownGroup, recreations: "1", controller: types.UID("other-uid"), deleting: true})
		r, c := podGroupTestReconciler(t, pod, groupWorkload(ownGroup, pod))
		if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
			t.Fatalf("releaseKueuePodGroups: %v", err)
		}
		if !hasKueueFinalizer(t, c, "other") || !workloadExists(t, c, ownGroup) {
			t.Error("a pod another owner controls was changed")
		}
	})

	t.Run("Workload owned by a pod the cache does not show yet", func(t *testing.T) {
		pod := groupPod(groupPodOptions{name: "seen", group: ownGroup, recreations: "1", controller: ir.UID, deleting: true})
		// unseen exists, and is not one of the pods the replica's cached read returns.
		unseen := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
			Name: "unseen", Namespace: podGroupTestNamespace, UID: types.UID("unseen-uid"),
			Labels: map[string]string{constants.KueuePodGroupNameLabelKey: ownGroup},
		}}
		r, c := podGroupTestReconciler(t, pod, unseen, groupWorkload(ownGroup, pod, unseen))
		if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
			t.Fatalf("releaseKueuePodGroups: %v", err)
		}
		if !workloadExists(t, c, ownGroup) {
			t.Error("a Workload with a member pod OME did not see being deleted was deleted")
		}
	})

	t.Run("Workload with a controller", func(t *testing.T) {
		pod := groupPod(groupPodOptions{name: "member", group: ownGroup, recreations: "1", controller: ir.UID, deleting: true})
		wl := groupWorkload(ownGroup, pod)
		wl.OwnerReferences = append(wl.OwnerReferences, metav1.OwnerReference{
			APIVersion: "example.com/v1", Kind: "Job", Name: "job", UID: types.UID("job-uid"), Controller: ptr.To(true),
		})
		r, c := podGroupTestReconciler(t, pod, wl)
		if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
			t.Fatalf("releaseKueuePodGroups: %v", err)
		}
		if !workloadExists(t, c, ownGroup) {
			t.Error("a Workload with a controller of its own was deleted")
		}
		if hasKueueFinalizer(t, c, "member") {
			t.Error("the deleted member still carries Kueue's finalizer")
		}
	})
}

// TestReleaseKueuePodGroups_OwnerNameHeldAgain checks a Workload that names a
// pod that is gone, when another pod holds that pod's name. A
// replacement inside the same group keeps the Workload. A pod of the
// Instance's next build has a group of its own, so the Workload is deleted.
func TestReleaseKueuePodGroups_OwnerNameHeldAgain(t *testing.T) {
	ir := podGroupTestReplica()
	group := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 1)
	nextGroup := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 2)
	for _, tc := range []struct {
		name     string
		heldIn   string
		wantKept bool
	}{
		{name: "by a replacement inside the same group", heldIn: group, wantKept: true},
		{name: "by a pod of the next build", heldIn: nextGroup, wantKept: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := groupPod(groupPodOptions{name: "worker", group: group, recreations: "1", controller: ir.UID, deleting: true})
			gone := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "leader", UID: types.UID("gone-leader-uid")}}
			// holder exists, and is not one of the pods the replica's cached read returns.
			holder := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "leader", Namespace: podGroupTestNamespace, UID: types.UID("new-leader-uid"),
				Labels: map[string]string{constants.KueuePodGroupNameLabelKey: tc.heldIn},
			}}
			r, c := podGroupTestReconciler(t, worker, holder, groupWorkload(group, gone, worker))
			if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
				t.Fatalf("releaseKueuePodGroups: %v", err)
			}
			if kept := workloadExists(t, c, group); kept != tc.wantKept {
				t.Errorf("Workload %s kept = %v, want %v", group, kept, tc.wantKept)
			}
			if hasKueueFinalizer(t, c, "worker") {
				t.Error("the deleted worker still carries Kueue's finalizer")
			}
		})
	}
}

// failingPodReader is a live reader whose pod reads fail with err. Every
// other read goes to the reader it wraps.
type failingPodReader struct {
	client.Reader
	err error
}

func (r failingPodReader) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*corev1.Pod); ok {
		return r.err
	}
	return r.Reader.Get(ctx, key, obj, opts...)
}

// TestReleaseKueuePodGroups_FailedLiveReadKeepsTheWorkload checks a live read
// that fails. The Workload names a leader the cache does not show, and the
// read of the leader's name returns an error. A failed read says nothing
// about the leader, so the pass returns the error and keeps the Workload.
func TestReleaseKueuePodGroups_FailedLiveReadKeepsTheWorkload(t *testing.T) {
	ir := podGroupTestReplica()
	group := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 1)
	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "a timeout", err: apierrors.NewTimeoutError("the request timed out", 1)},
		{name: "a 500", err: apierrors.NewInternalError(errors.New("the store is down"))},
		{name: "a 503", err: apierrors.NewServiceUnavailable("the server is overloaded")},
		{name: "a 429", err: apierrors.NewTooManyRequests("slow down", 1)},
		{name: "a forbidden answer", err: apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, "leader", errors.New("no access"))},
		{name: "a passed deadline", err: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := groupPod(groupPodOptions{name: "worker", group: group, recreations: "1", controller: ir.UID, deleting: true})
			gone := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "leader", UID: types.UID("gone-leader-uid")}}
			r, c := podGroupTestReconciler(t, worker, groupWorkload(group, gone, worker))
			r.APIReader = failingPodReader{Reader: c, err: tc.err}

			err := r.releaseKueuePodGroups(context.Background(), ir)
			if !errors.Is(err, tc.err) {
				t.Errorf("error = %v, want the failed read", err)
			}
			if !workloadExists(t, c, group) {
				t.Errorf("Workload %s was deleted after a read that failed", group)
			}
		})
	}
}

// TestReleaseKueuePodGroups_NamedPodKeepsTheWorkloadWhateverItsLabel checks an
// owner that still exists under the UID the Workload names, while its group
// label is gone or names another group. The replica's cached read leaves such
// a pod out. The Workload names that very pod, so the Workload stays.
func TestReleaseKueuePodGroups_NamedPodKeepsTheWorkloadWhateverItsLabel(t *testing.T) {
	ir := podGroupTestReplica()
	group := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 1)
	nextGroup := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 2)
	for _, tc := range []struct {
		name   string
		labels map[string]string
	}{
		{name: "with no group label"},
		{name: "with the label of another group", labels: map[string]string{constants.KueuePodGroupNameLabelKey: nextGroup}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := groupPod(groupPodOptions{name: "worker", group: group, recreations: "1", controller: ir.UID, deleting: true})
			// leader exists, and is not one of the pods the replica's cached read returns.
			leader := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{
				Name: "leader", Namespace: podGroupTestNamespace, UID: types.UID("leader-uid"), Labels: tc.labels,
			}}
			r, c := podGroupTestReconciler(t, worker, leader, groupWorkload(group, leader, worker))
			if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
				t.Fatalf("releaseKueuePodGroups: %v", err)
			}
			if !workloadExists(t, c, group) {
				t.Errorf("Workload %s was deleted while a pod it names by UID still exists", group)
			}
		})
	}
}

// TestReleaseKueuePodGroups_LeavesAWorkloadThatNamesNoPodOMESaw checks what
// makes a Workload the group's own: it names at least one pod OME saw being
// deleted. A Workload under the group's name that names no pod, or only a pod
// OME never saw, stays, even when every pod it names is gone.
func TestReleaseKueuePodGroups_LeavesAWorkloadThatNamesNoPodOMESaw(t *testing.T) {
	ir := podGroupTestReplica()
	group := query.KueuePodGroupName(podGroupTestService, workloadtypes.ComponentEngine, 0, 1)
	gone := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "leader", UID: types.UID("gone-leader-uid")}}
	for _, tc := range []struct {
		name   string
		owners []*corev1.Pod
	}{
		{name: "a Workload that names no pod"},
		{name: "a Workload that names only a pod that is gone", owners: []*corev1.Pod{gone}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			worker := groupPod(groupPodOptions{name: "worker", group: group, recreations: "1", controller: ir.UID, deleting: true})
			r, c := podGroupTestReconciler(t, worker, groupWorkload(group, tc.owners...))
			if err := r.releaseKueuePodGroups(context.Background(), ir); err != nil {
				t.Fatalf("releaseKueuePodGroups: %v", err)
			}
			if !workloadExists(t, c, group) {
				t.Errorf("Workload %s names no pod OME saw being deleted, and was deleted", group)
			}
		})
	}
}

// TestReleasingKueuePodGroups_ReconcileErrorWins checks how a failed release
// is reported: the reconcile runs and keeps its result, its own error wins,
// and the release error is returned only when the reconcile itself succeeds.
func TestReleasingKueuePodGroups_ReconcileErrorWins(t *testing.T) {
	listErr := errors.New("pod list refused")
	scheme := testScheme(t)
	c := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return listErr },
	}).Build()
	r := &Reconciler{Client: c, APIReader: c, Log: logf.Log.WithName("test")}
	ir := podGroupTestReplica()

	ran := false
	result, err := r.releasingKueuePodGroups(context.Background(), ir, func() (ctrl.Result, error) {
		ran = true
		return ctrl.Result{RequeueAfter: time.Second}, nil
	})
	if !ran {
		t.Fatal("the reconcile did not run")
	}
	if !errors.Is(err, listErr) {
		t.Errorf("error = %v, want the release failure", err)
	}
	if result.RequeueAfter != time.Second {
		t.Errorf("result = %+v, want the reconcile's own result", result)
	}

	reconcileErr := errors.New("engine failed")
	if _, err := r.releasingKueuePodGroups(context.Background(), ir, func() (ctrl.Result, error) {
		return ctrl.Result{}, reconcileErr
	}); !errors.Is(err, reconcileErr) || errors.Is(err, listErr) {
		t.Errorf("error = %v, want only the reconcile's own error", err)
	}
}
