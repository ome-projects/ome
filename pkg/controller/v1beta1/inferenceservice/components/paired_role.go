package components

import (
	"context"
	"fmt"

	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/common"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/pdb"
	"sigs.k8s.io/ome/pkg/render"
)

// pairedRole is what reconciling a role whose pods pair with the engine's
// pods reads off the role's merged block: the decoder and the encoder. Both
// render a pod set, dispatch it by deployment mode, and publish the role's
// stable Service, PodMonitor and status the same way.
type pairedRole struct {
	component v1beta1.ComponentType
	ext       *v1beta1.ComponentExtensionSpec
	// leader and worker report whether the block declares a Leader and a
	// Worker; placement demand reads the gang shape from them. multiPod
	// reports whether they form a gang.
	leader, worker, multiPod bool
	// The topology fields the InferenceReplica carries for the role.
	topologyKey       *string
	topologySpread    *v1beta1.TopologySpreadPolicy
	topologySpreadKey *string
	// render renders the role's pod templates from its merged block.
	render func(ctx context.Context, p *render.Piece, isvc *v1beta1.InferenceService) (render.Rendered, error)
}

// reconcilePairedRole renders the role, resolves its disruption budget,
// checks placement demand, dispatches the pod set by deployment mode, then
// writes the OMENative stable Service and PodMonitor and the role's status.
func reconcilePairedRole(ctx context.Context, b *BaseComponentFields, deployments *common.DeploymentReconciler, isvc *v1beta1.InferenceService, role pairedRole) (ctrl.Result, error) {
	b.Log.V(1).Info(fmt.Sprintf("Reconciling %s component", role.component), "inferenceService", isvc.Name, "namespace", isvc.Namespace)

	// Render the pod templates: fine-tuned weights, object metadata, the
	// primary pod and the worker pod. The role's spec is not mutated.
	rendered, err := role.render(ctx, &b.Piece, isvc)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to render %s pods", role.component)
	}
	objectMeta, podSpec, workerPodSpec, size := rendered.ObjectMeta, rendered.Primary, rendered.Worker, rendered.WorkerSize

	pdbRequest, err := resolveComponentPDBRequest(b, isvc, b.DeploymentMode, role.component, objectMeta, role.ext)
	if err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to resolve %s PodDisruptionBudget", role.component)
	}
	if err := preflightComponentPDB(ctx, b, pdbRequest); err != nil {
		return ctrl.Result{}, errors.Wrapf(err, "failed to preflight %s PodDisruptionBudget", role.component)
	}

	if err := checkPlacementDemand(ctx, b, isvc, role.component, role.leader, role.worker,
		ReplicaTemplates{Primary: podSpec, Worker: workerPodSpec, WorkerSize: size}); err != nil {
		return ctrl.Result{}, err
	}

	// Reconcile deployment based on deployment mode. The deployment
	// reconciler's RequeueAfter MUST be preserved through the rest of
	// this function — OMENative's per-Instance dispatcher uses it to
	// re-run after a Sequential / Ratio gate denial OR to poll an
	// in-flight Update / Create. Discarding it strands the rollout
	// until an unrelated watch event happens to fire another reconcile.
	deploymentResult, err := reconcilePairedRoleDeployment(ctx, b, deployments, isvc, role, objectMeta, podSpec, size, workerPodSpec, pdbRequest)
	if err != nil {
		return deploymentResult, err
	}

	// Per-Component stable Service + PodMonitor for OMENative; engine,
	// decoder, encoder and router emit byte-identical pairs, only the
	// component, the extension and the multi-pod shape differ.
	if b.DeploymentMode == constants.OMENative {
		if err := ReconcileOMENativeSubresources(ctx, b, isvc, role.component, role.ext, objectMeta, podSpec, role.multiPod); err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to reconcile %s OMENative sub-resources", role.component)
		}
	}

	if err := UpdateComponentStatus(b, isvc, role.component, objectMeta, role.ext); err != nil {
		return ctrl.Result{}, err
	}
	return deploymentResult, nil
}

// reconcilePairedRoleDeployment dispatches the role's pod set by deployment
// mode: a Deployment, a LeaderWorkerSet, or an InferenceReplica with its
// disruption budget and autoscaler.
func reconcilePairedRoleDeployment(ctx context.Context, b *BaseComponentFields, deployments *common.DeploymentReconciler, isvc *v1beta1.InferenceService, role pairedRole, objectMeta metav1.ObjectMeta, podSpec *v1.PodSpec, workerSize int, workerPodSpec *v1.PodSpec, pdbRequest pdb.Request) (ctrl.Result, error) {
	switch b.DeploymentMode {
	case constants.RawDeployment:
		rawResolved, err := resolveRawComponentAutoscaling(ctx, b, isvc, role.component, role.ext, objectMeta.Annotations)
		if err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to resolve autoscaler for raw %s", role.component)
		}
		result, err := deployments.ReconcileRawDeployment(ctx, isvc, objectMeta, podSpec, role.ext, role.component, rawResolved, pdbRequest)
		if err != nil {
			return result, err
		}
		// A policy hold recovers via config edits that emit no event, so the
		// hold carries its own periodic requeue. Smaller-nonzero merge: never
		// delay a dispatcher poll already scheduled on this result.
		return mergeRequeueAfter(result, rawResolved.RequeueAfter), nil
	case constants.MultiNode:
		return deployments.ReconcileMultiNodeDeployment(isvc, objectMeta, podSpec, workerSize, workerPodSpec, role.ext, role.component)
	case constants.OMENative:
		// OMENative-mode Components dispatch through the InferenceReplica
		// path. Resolve the authoritative ComponentAutoscaler from the
		// ISVC → policy → runtime → default chain and dispatch HPA / KEDA /
		// external / none against the committed IR (autoscaler dispatch is
		// always-on per Component).
		res, err := resolveComponentAutoscaling(ctx, b, isvc, role.component, role.ext)
		if err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to resolve autoscaler for %s", role.component)
		}
		ir, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
			ISVC:                      isvc,
			Component:                 role.component,
			ComponentExt:              role.ext,
			ObjectMeta:                objectMeta,
			PodSpec:                   podSpec,
			WorkerPodSpec:             workerPodSpec,
			WorkerSize:                workerSize,
			MultiPod:                  role.multiPod,
			TopologyKey:               role.topologyKey,
			TopologySpread:            role.topologySpread,
			TopologySpreadKey:         role.topologySpreadKey,
			PacingPartition:           b.PacingPartition,
			CoordinatorFollower:       b.CoordinatorFollower,
			CoordinatorTarget:         b.CoordinatorTarget,
			ResolvedAutoscaler:        res.Resolved,
			PreserveAutoscaler:        res.Hold,
			QuotaAcceleratorResources: b.QuotaAcceleratorResources,
			Client:                    b.Client,
			Reader:                    b.APIReader,
		})
		if err != nil {
			if apierrors.IsConflict(err) {
				// Benign: a concurrent IR status write changed the object.
				// Re-read and reproject next pass instead of surfacing a hard
				// error, which would fast-loop the ISVC reconcile.
				return ctrl.Result{Requeue: true}, nil
			}
			return ctrl.Result{}, errors.Wrapf(err, "failed to project InferenceReplica for %s", role.component)
		}
		if err := ReconcileOMENativePDB(ctx, b, role.component, ir, pdbRequest); err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to reconcile %s OMENative PodDisruptionBudget", role.component)
		}
		dispatchResult, err := dispatchIRAutoscaler(ctx, b, isvc, ir, role.ext, res)
		if err != nil {
			return ctrl.Result{}, errors.Wrapf(err, "failed to dispatch autoscaler for %s", role.component)
		}
		// A policy hold recovers via config edits that emit no event; carry
		// its periodic requeue on the result (smaller-nonzero merge).
		return mergeRequeueAfter(dispatchResult, res.RequeueAfter), nil
	default:
		return ctrl.Result{}, fmt.Errorf("invalid deployment mode for %s", role.component)
	}
}
