package inferenceservice

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	knapis "knative.dev/pkg/apis"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/components"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/external_service"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/ingress"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/canary"
	traffic "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/traffic"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/validation"
)

// Reasons the ready condition of a referenced role carries while the service
// has no replica to front for it: the named replica does not exist, or it is
// not a standalone replica of the role's component.
const (
	ReasonReplicaRefMissing = "ReplicaRefMissing"
	ReasonReplicaRefInvalid = "ReplicaRefInvalid"
)

// reconcileReferencedReplicas is the reconcile pass of a service whose roles
// are standalone replicas the user created (spec.replicaRefs). The service
// renders and projects nothing: per role it keeps the stable Service and
// PodMonitor in front of the replica's pods, mirrors the replica's counters
// onto its status, and reconciles the route and the external Service as the
// inline pass does. Rollout groups, canary and pairing are rejected at
// admission for such a service, so none of those engines run here.
func (r *InferenceServiceReconciler) reconcileReferencedReplicas(ctx context.Context, isvc *v1beta1.InferenceService, deploymentMode constants.DeploymentModeType, isvcConfig *controllerconfig.InferenceServicesConfig, base *canary.Base) (ctrl.Result, error) {
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{}
	for _, c := range irprojector.ReferencedRoles(isvc) {
		fronted, err := r.frontReferencedReplica(ctx, isvc, isvcConfig, c)
		if err != nil {
			r.Recorder.Event(isvc, v1.EventTypeWarning, "ComponentReconcileError", err.Error())
			return reconcile.Result{}, err
		}
		if fronted {
			modes[c] = constants.OMENative
		}
	}
	// A fronted replica's counters become the role's Lifecycle block and
	// ready condition; a role without one keeps the condition set above.
	if err := irprojector.AggregateIRStatus(ctx, r.Client, r.APIReader, isvc, modes); err != nil {
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}
	ingressConfig, err := controllerconfig.NewIngressConfigCached(r.ConfigCache, r.Clientset)
	if err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to create IngressConfig")
	}
	resolvedIngressConfig := isvcutils.ResolveIngressConfig(ingressConfig, isvc.Annotations)
	// A referenced role is OMENative: the route targets the stable
	// <name>-<role> Service, as for a projected replica.
	ingressReconciler := ingress.NewIngressReconciler(r.Client, r.Clientset, r.Scheme, resolvedIngressConfig, isvcConfig)
	if err := ingressReconciler.(*ingress.IngressReconciler).ReconcileWithDeploymentMode(ctx, isvc, constants.OMENative); err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile ingress")
	}
	if err := external_service.NewExternalServiceReconciler(r.Client, r.Clientset, r.Scheme, resolvedIngressConfig).Reconcile(ctx, isvc); err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile external service")
	}
	if r.TrafficReconciler != nil {
		targetRoutes := traffic.ComputeTargetHTTPRoutes(isvc, irprojector.RoleDeclared(isvc, v1beta1.DecoderComponent), irprojector.RoleDeclared(isvc, v1beta1.RouterComponent))
		trafficStatus, err := r.TrafficReconciler.Reconcile(ctx, isvc, targetRoutes)
		isvc.Status.Traffic = trafficStatus
		if err != nil {
			r.Recorder.Event(isvc, v1.EventTypeWarning, "TrafficReconcileError", err.Error())
			return reconcile.Result{}, errors.Wrapf(err, "fails to reconcile traffic policy")
		}
	}
	if resolvedIngressConfig.DisableIngressCreation {
		if err := r.ensureIngressDisableAnnotation(isvc); err != nil {
			return reconcile.Result{}, errors.Wrapf(err, "fails to add ingress disable annotation")
		}
		if err := r.setExternalServiceURL(ctx, isvc, resolvedIngressConfig); err != nil && !apierrors.IsNotFound(err) {
			r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
			return reconcile.Result{}, errors.Wrapf(err, "fails to set external service URL")
		}
	}
	if err := r.updateStatus(isvc, deploymentMode, base); err != nil {
		if apierrors.IsConflict(err) {
			return ctrl.Result{Requeue: true}, nil
		}
		r.Recorder.Event(isvc, v1.EventTypeWarning, "InternalError", err.Error())
		return reconcile.Result{}, err
	}
	return ctrl.Result{}, nil
}

// frontReferencedReplica keeps the stable Service and PodMonitor of one
// referenced role selecting the replica's pods and records the replica as
// the role's scale target. It writes nothing on the replica. Returns whether
// the role has a replica to front: a replica that is missing, projected by
// a service or of another component leaves the role's ready condition False
// with the reason and a Warning event, and the pass goes on; the replica
// watch brings the service back when the replica changes. A replica whose
// named revision is not readable yet is fronted as it is: the Service keeps
// its ports until the replica's status moves and the pass returns.
func (r *InferenceServiceReconciler) frontReferencedReplica(ctx context.Context, isvc *v1beta1.InferenceService, isvcConfig *controllerconfig.InferenceServicesConfig, c v1beta1.ComponentType) (bool, error) {
	field := validation.ReplicaRefsField(c)
	name := irprojector.RoleReplicaName(isvc, c)
	ir := &v1beta1.InferenceReplica{}
	if err := r.Get(ctx, types.NamespacedName{Namespace: isvc.Namespace, Name: name}, ir); err != nil {
		if !apierrors.IsNotFound(err) {
			return false, errors.Wrapf(err, "get InferenceReplica %s/%s", isvc.Namespace, name)
		}
		r.markReplicaRefUnavailable(isvc, c, ReasonReplicaRefMissing, fmt.Sprintf("InferenceReplica %s named by %s does not exist", name, field))
		return false, nil
	}
	if parent := validation.ProjectedBy(ir); parent != "" {
		r.markReplicaRefUnavailable(isvc, c, ReasonReplicaRefInvalid, fmt.Sprintf("InferenceReplica %s named by %s is projected by InferenceService %s", name, field, parent))
		return false, nil
	}
	if ir.Spec.Component != c {
		r.markReplicaRefUnavailable(isvc, c, ReasonReplicaRefInvalid, fmt.Sprintf("InferenceReplica %s named by %s has component %s", name, field, ir.Spec.Component))
		return false, nil
	}
	podSpec, leaderOnly, err := replicaServingTemplate(ctx, r.Client, ir)
	if err != nil {
		return false, err
	}
	if podSpec != nil {
		b := &components.BaseComponentFields{Piece: render.Piece{InferenceServiceConfig: isvcConfig}, Client: r.Client, Scheme: r.Scheme}
		meta := metav1.ObjectMeta{
			Name:      render.ComponentName(isvc, c),
			Namespace: isvc.Namespace,
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: isvc.Name,
				constants.OMEComponentLabel:           string(c),
			},
		}
		if err := components.ReconcileStableSubresources(ctx, b, isvc, c, ir.NamePrefix(), leaderOnly, nil, meta, podSpec); err != nil {
			return false, errors.Wrapf(err, "front InferenceReplica %s for %s", name, c)
		}
	}
	cs := isvc.Status.Components[c]
	cs.ScaleTargetRef = &v1beta1.ScaleTargetRef{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica", Name: ir.Name}
	isvc.Status.Components[c] = cs
	return true, nil
}

// markReplicaRefUnavailable sets a role's ready condition False with the
// reason, drops the role's scale target so the route builders stop treating
// it as serving, and records a Warning event. The service cannot create what
// it does not own, so this is a report, not an error.
func (r *InferenceServiceReconciler) markReplicaRefUnavailable(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, reason, message string) {
	readyType := roleReadyCondition(c)
	isvc.Status.SetCondition(readyType, &knapis.Condition{Type: readyType, Status: v1.ConditionFalse, Reason: reason, Message: message})
	cs := isvc.Status.Components[c]
	cs.ScaleTargetRef = nil
	isvc.Status.Components[c] = cs
	r.Recorder.Event(isvc, v1.EventTypeWarning, reason, message)
}

// roleReadyCondition is the top-level condition that reports a role's readiness.
func roleReadyCondition(c v1beta1.ComponentType) knapis.ConditionType {
	switch c {
	case v1beta1.DecoderComponent:
		return v1beta1.DecoderReady
	case v1beta1.RouterComponent:
		return v1beta1.RouterReady
	default:
		return v1beta1.EngineReady
	}
}

// replicaServingTemplate returns the pod template a role's stable Service
// publishes ports from and whether each Instance has a leader the Service
// selects: the stored runners when the replica carries them (the leader's
// template when there is one, else the single runner's), else the payload of
// the replica's current ControllerRevision (a replica rendered from refs
// stores no runners), else, while the replica names no revision, an empty
// template, which the Service reconciler maps to the default port. A nil
// template without an error means the named revision is not found: the
// caller leaves the Service as it is rather than rewrite its ports.
func replicaServingTemplate(ctx context.Context, c client.Client, ir *v1beta1.InferenceReplica) (*v1.PodSpec, bool, error) {
	var primary *v1.PodSpec
	leader := false
	for i := range ir.Spec.Runners {
		switch runner := &ir.Spec.Runners[i]; runner.Name {
		case v1beta1.RunnerNameLeader:
			primary, leader = &runner.Template.Spec, true
		case v1beta1.RunnerNameDefault:
			primary = &runner.Template.Spec
		}
	}
	if primary != nil {
		return primary.DeepCopy(), leader, nil
	}
	name := ir.Status.CurrentRevision
	if name == "" {
		name = ir.Status.UpdateRevision
	}
	if name != "" {
		cr := &appsv1.ControllerRevision{}
		if err := c.Get(ctx, types.NamespacedName{Namespace: ir.Namespace, Name: name}, cr); err != nil {
			if apierrors.IsNotFound(err) {
				return nil, false, nil
			}
			return nil, false, errors.Wrapf(err, "get ControllerRevision %s/%s", ir.Namespace, name)
		}
		if payload, err := revision.PayloadFromControllerRevision(cr); err == nil && payload != nil && payload.PodSpec != nil {
			return payload.PodSpec, payload.WorkerPodSpec != nil, nil
		}
	}
	return &v1.PodSpec{Containers: []v1.Container{{}}}, false, nil
}

// servicesReferencingReplica maps a standalone replica event to the
// InferenceServices of its namespace whose spec.replicaRefs name it, so their
// Services and status follow the replica. A projected replica reaches its
// service through the owner reference and is skipped here.
func (r *InferenceServiceReconciler) servicesReferencingReplica(ctx context.Context, obj client.Object) []reconcile.Request {
	ir, ok := obj.(*v1beta1.InferenceReplica)
	if !ok || ir.ParentName() != "" {
		return nil
	}
	services := &v1beta1.InferenceServiceList{}
	if err := r.List(ctx, services, client.InNamespace(ir.Namespace)); err != nil {
		r.Log.Error(err, "fan-out InferenceReplica event: list InferenceServices failed", "namespace", ir.Namespace)
		return nil
	}
	var reqs []reconcile.Request
	for i := range services.Items {
		isvc := &services.Items[i]
		for _, c := range irprojector.ReferencedRoles(isvc) {
			if irprojector.RoleReplicaRef(isvc, c) == ir.Name {
				reqs = append(reqs, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(isvc)})
				break
			}
		}
	}
	return reqs
}
