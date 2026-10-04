package inferencereplica

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/sliceprovision"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/tpuslice/gke"
)

// sliceProvisioner returns the provisioner of ir's TPU slices, or nil when
// the controller provisions no slices.
func (r *Reconciler) sliceProvisioner(ir *v1beta1.InferenceReplica) (*sliceprovision.Provisioner, error) {
	if r.TPUSliceProvisioning == nil || r.sliceReader == nil {
		return nil, nil
	}
	p, err := sliceprovision.New(r.TPUSliceProvisioning, r.sliceReader, r.APIReader, r.Client, sliceprovision.Owner{
		Kind:      irKind,
		Namespace: ir.Namespace,
		Name:      ir.Name,
		UID:       ir.UID,
	})
	if err != nil {
		return nil, err
	}
	p.SetPodAnnotations(podTemplateAnnotations(ir))
	p.OnReleaseDeferred(func(s gke.Slice, holders []string) {
		if r.Recorder == nil {
			return
		}
		r.Recorder.Eventf(ir, corev1.EventTypeWarning, string(sliceprovision.EventReasonSliceReleaseDeferred),
			"TPU slice %s is kept: pods of other workloads hold chips on its hosts (%s); it is released once they are gone",
			s.Name, strings.Join(holders, ", "))
	})
	return p, nil
}

// slicesOptedIn reports whether ir's pods run on provisioned slices.
func slicesOptedIn(ir *v1beta1.InferenceReplica) bool {
	return podTemplateAnnotations(ir)[constants.TPUSliceProvisioningAnnotationKey] == "true"
}

// podTemplateAnnotations returns the annotations of ir's own default or
// leader pod template: a rollback renders pods from a stored template that
// lacks the annotations ir inherits from its parent.
func podTemplateAnnotations(ir *v1beta1.InferenceReplica) map[string]string {
	for i := range ir.Spec.Runners {
		runner := &ir.Spec.Runners[i]
		if runner.Name == v1beta1.RunnerNameDefault || runner.Name == v1beta1.RunnerNameLeader {
			return runner.Template.Annotations
		}
	}
	return nil
}

// slicesInUse reports whether ir's slices must be provisioned or released:
// ir opts in, or a cached read finds a slice ir holds.
func slicesInUse(ctx context.Context, ir *v1beta1.InferenceReplica, p *sliceprovision.Provisioner) (bool, error) {
	if slicesOptedIn(ir) {
		return true, nil
	}
	return p.Holds(ctx)
}

// releasingSlices extends finalize, which may be nil, to release the
// Instance's slices. The Instance is finalized only once both are complete.
func releasingSlices(finalize func(context.Context, int32) (bool, error), p *sliceprovision.Provisioner) func(context.Context, int32) (bool, error) {
	return func(ctx context.Context, idx int32) (bool, error) {
		complete := true
		if finalize != nil {
			var err error
			if complete, err = finalize(ctx, idx); err != nil {
				return false, err
			}
		}
		released, err := p.ReleaseInstance(ctx, idx)
		if err != nil {
			return false, err
		}
		return complete && released, nil
	}
}

// componentPods returns a live read of ir's component pods, terminating ones
// included. Every component pod counts, whatever its controller: a pod holds
// its slice's chips until it is gone.
func (r *Reconciler) componentPods(ir *v1beta1.InferenceReplica) func(context.Context) ([]*corev1.Pod, error) {
	return func(ctx context.Context) ([]*corev1.Pod, error) {
		return query.LiveListPodsForComponent(ctx, r.APIReader, ir.Namespace, ir.NamePrefix(),
			v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component))
	}
}

// pinnedSlices returns a live read of the slices ir's component pods are
// confined to.
func (r *Reconciler) pinnedSlices(ir *v1beta1.InferenceReplica, p *sliceprovision.Provisioner) func(context.Context) (map[string]struct{}, error) {
	pods := r.componentPods(ir)
	return func(ctx context.Context) (map[string]struct{}, error) {
		list, err := pods(ctx)
		if err != nil {
			return nil, err
		}
		return p.Pinned(list), nil
	}
}

// slicePass is one reconcile pass's slice provisioning.
type slicePass struct {
	p *sliceprovision.Provisioner
	// placer is set when ir opts in; without it the pass only releases, and
	// pinned is its source of pinned slices.
	placer *sliceprovision.Placer
	pinned func(context.Context) (map[string]struct{}, error)
}

// provisionSlices prepares the pass's slices. An opted-in ir's pods are
// placed on provisioned slices; while ir opts in or holds a slice, an
// Instance is finalized only once its slices are released. It returns nil
// when neither holds.
func (r *Reconciler) provisionSlices(ctx context.Context, ir *v1beta1.InferenceReplica, deps *workloadtypes.Deps, input *workloadtypes.ReconcileInput) (*slicePass, error) {
	p, err := r.sliceProvisioner(ir)
	if err != nil || p == nil {
		return nil, err
	}
	inUse, err := slicesInUse(ctx, ir, p)
	if err != nil || !inUse {
		return nil, err
	}
	pass := &slicePass{p: p}
	if slicesOptedIn(ir) {
		pass.placer = sliceprovision.NewPlacer(p, r.Client, r.componentPods(ir), r.Recorder)
		deps.Provisioner = pass.placer
	} else {
		pass.pinned = r.pinnedSlices(ir, p)
	}
	input.FinalizeInstanceResources = releasingSlices(input.FinalizeInstanceResources, p)
	return pass, nil
}

// sweep releases the slices no pod of the pass may use. A nil pass releases
// nothing.
func (s *slicePass) sweep(ctx context.Context, input workloadtypes.ReconcileInput, plan workloadtypes.ComponentPlan) error {
	if s == nil {
		return nil
	}
	if s.placer != nil {
		return s.placer.Sweep(ctx, input, plan)
	}
	return s.p.Sweep(ctx, nil, s.pinned)
}

// recoverLostSlices deletes ir's pods that run on a slice deleted, or moved
// off their nodes, from outside, so that their Instance is rebuilt. A cached
// read of the pods finds them and a live read confirms them before any is
// deleted. A nil pass recovers nothing.
func (r *Reconciler) recoverLostSlices(ctx context.Context, ir *v1beta1.InferenceReplica, input workloadtypes.ReconcileInput, s *slicePass) error {
	if s == nil {
		return nil
	}
	cached, err := query.ListOMENativePodsByName(ctx, r.Client, ir.Namespace, ir.NamePrefix(),
		v1beta1convert.ComponentTypeToWorkload(ir.Spec.Component), true)
	if err != nil {
		return err
	}
	if lost, err := s.p.Lost(ctx, cached); err != nil || len(lost) == 0 {
		return err
	}
	pods, err := r.componentPods(ir)(ctx)
	if err != nil {
		return err
	}
	lost, err := s.p.Lost(ctx, pods)
	if err != nil {
		return err
	}
	for _, l := range lost {
		names := make([]string, 0, len(l.Pods))
		for _, pod := range l.Pods {
			uid := pod.UID
			if err := r.Client.Delete(ctx, pod, client.Preconditions{UID: &uid}); err != nil && !apierrors.IsNotFound(err) && !apierrors.IsConflict(err) {
				return fmt.Errorf("delete pod %s on lost slice %s: %w", pod.Name, l.Name, err)
			}
			names = append(names, pod.Name)
		}
		s.p.Recovered(l)
		workloadtypes.RecordWarning(r.Recorder, workloadtypes.EventTarget(input), sliceprovision.EventReasonSliceLost,
			"TPU slice %s %s while pods %s ran on it; they are deleted so that their Instance is rebuilt",
			l.Name, l.Why, strings.Join(names, ", "))
	}
	return nil
}

// releaseSlicesPastDeadline releases every slice ir holds, pods or not. Once
// the teardown finalizer lifts, background GC deletes ir's pods and no later
// pass remains to release their slices, so a failure is reported and never
// keeps the finalizer.
func (r *Reconciler) releaseSlicesPastDeadline(ctx context.Context, ir *v1beta1.InferenceReplica) {
	p, err := r.sliceProvisioner(ir)
	if err == nil && p != nil {
		_, err = p.ReleaseAll(ctx)
	}
	if err != nil {
		r.warnTeardown(ir, ReasonTeardownDeadlineExceeded, fmt.Sprintf(
			"teardown deadline exceeded; releasing owned TPU slices failed: %v; delete the slices labeled %s=%s by hand",
			err, sliceprovision.LabelOwnerUID, ir.UID))
	}
}

// irUIDIndexField is the cache field index keyed by an InferenceReplica's
// UID, which a TPU slice records as its owner. The field name is an internal
// cache identifier, not a Kubernetes field.
const irUIDIndexField = "ome.io/inferencereplica-uid"

// irUIDIndexExtractor indexes InferenceReplicas by UID.
func irUIDIndexExtractor(obj client.Object) []string {
	if obj.GetUID() == "" {
		return nil
	}
	return []string{string(obj.GetUID())}
}

// irExists reports whether reader, which carries irUIDIndexField, holds the
// InferenceReplica with uid.
func irExists(reader client.Reader) func(context.Context, types.UID) (bool, error) {
	return func(ctx context.Context, uid types.UID) (bool, error) {
		list := &v1beta1.InferenceReplicaList{}
		if err := reader.List(ctx, list, client.MatchingFields{irUIDIndexField: string(uid)}, client.UnsafeDisableDeepCopy); err != nil {
			return false, fmt.Errorf("list the InferenceReplica with UID %s: %w", uid, err)
		}
		return len(list.Items) > 0, nil
	}
}

// reportSliceStates reports the controller's TPU slices by state, read
// through reader, which carries irUIDIndexField. The manager runs it only on
// the leader. It reports once the slice and InferenceReplica informers have
// synced, so a scrape never waits for a sync.
func (r *Reconciler) reportSliceStates(informers cache.Informers, reader client.Reader) manager.RunnableFunc {
	return func(ctx context.Context) error {
		for _, obj := range []client.Object{gke.NewObject(), &v1beta1.InferenceReplica{}} {
			if _, err := informers.GetInformer(ctx, obj); err != nil {
				if ctx.Err() == nil {
					r.Log.Error(err, "Not reporting TPU slices by state because an informer failed to start")
				}
				return nil
			}
		}
		stop := sliceprovision.ReportStates(sliceprovision.StateSource{
			Slices:      reader,
			Config:      r.TPUSliceProvisioning,
			OwnerKind:   irKind,
			OwnerExists: irExists(reader),
		})
		defer stop()
		<-ctx.Done()
		return nil
	}
}
