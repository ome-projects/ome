// Package inferencereplica implements the validating admission webhook
// for the InferenceReplica CRD.
//
// A replica has one of two forms. A projected replica carries an
// InferenceService controller owner reference or spec.parentRef; only the
// InferenceService controller may write its spec, and anyone RBAC admits
// may still change its metadata (the migration, release-held and
// reset-instances mailboxes are annotations). A standalone replica carries
// neither and is governed by RBAC, except for the placement fields
// placementExecution and placementReplicaLimit, which only the controller
// may set, and for a few settings nothing acts on without an
// InferenceService (HPA and KEDA scaler classes, the nested
// lifecycle.minReadySeconds, manual migration requests), which are
// rejected rather than silently ignored.
//
// spec.parentRef is set only by the InferenceService controller on the
// replicas it projects and is rejected on a standalone create, one without
// the InferenceService controller owner reference. Because parentRef is
// immutable, a replica whose owner reference an orphan delete removed
// stays projected. Only the controller may install a controller owner
// reference on a projected replica, or an InferenceService one on a
// standalone replica; anyone may keep or remove a replica's controller
// owner reference, as the garbage collector does when it clears
// blockOwnerDeletion or orphans a replica. InferenceService owner
// references are matched by group and kind, so any served version counts.
//
// The controller is recognized by the identity configured in the
// inferenceReplica block of inferenceservice-config (a ServiceAccount
// username or a group). With no identity configured the webhook falls back
// to the ome.io/controller-write=true annotation the projector stamps,
// which is a convention rather than a boundary. An update that keeps the
// spec and installs no controller owner reference is decided without the
// identity, so an unreadable config never refuses one.
//
// A standalone replica renders from exactly one template source:
// spec.runners, or spec.modelRef and/or spec.runtimeRef; a runtime pin is
// rejected. A named runtime that resolves without the piece for the
// replica's component, or whose inherit-from chain names a runtime that does
// not exist, is rejected when the reference is set or changed; one that is
// not found is admitted, and the controller reports it on the replica.
//
// spec.parentRef and spec.component are immutable for every writer, and
// deletion is never gated.
package inferencereplica

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"reflect"

	admissionv1 "k8s.io/api/admission/v1"
	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/sliceprovision"
	"sigs.k8s.io/ome/pkg/render"
	"sigs.k8s.io/ome/pkg/runtimeinheritance"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/validation"
)

var log = logf.Log.WithName("inferencereplica-validation-webhook")

// +kubebuilder:webhook:verbs=create;update,path=/validate-ome-io-v1beta1-inferencereplica,mutating=false,failurePolicy=fail,groups=ome.io,resources=inferencereplicas,versions=v1beta1,name=inferencereplica.ome-webhook-server.validator,sideEffects=None,admissionReviewVersions=v1
// +kubebuilder:object:generate=false
type Validator struct {
	Decoder admission.Decoder
	// TPUSliceProvisioning is the slice provisioning the controller runs,
	// nil when it provisions no slices. Nodes, set with it, reads the nodes
	// an Instance's slice demand is resolved against.
	TPUSliceProvisioning *controllerconfig.TPUSliceProvisioningConfig
	Nodes                client.Reader
	// Reader resolves the InferenceService whose name a standalone replica
	// would collide with, and the runtime a standalone replica names. Nil
	// skips both checks.
	Reader client.Reader
	// Clientset and ConfigCache load the inferenceReplica block of
	// inferenceservice-config, which names the controller identity. A nil
	// Clientset means no identity is configured.
	Clientset   kubernetes.Interface
	ConfigCache *controllerconfig.ConfigCache
}

// actor classifies the requesting identity.
type actor struct {
	// controller is true for the InferenceService controller: the
	// configured identity when one is set, otherwise a request carrying the
	// controller-write annotation.
	controller bool
	// identityConfigured selects the wording of denials.
	identityConfigured bool
}

func actorFor(identity *controllerconfig.ControllerIdentityConfig, user authenticationv1.UserInfo, obj *v1beta1.InferenceReplica) actor {
	if identity.Configured() {
		return actor{controller: identity.Matches(user), identityConfigured: true}
	}
	return actor{controller: hasControllerWriteAnnotation(obj.Annotations)}
}

func (v *Validator) controllerIdentity() (*controllerconfig.ControllerIdentityConfig, error) {
	if v.Clientset == nil {
		return nil, nil
	}
	cfg, err := controllerconfig.NewInferenceReplicaConfigCached(v.ConfigCache, v.Clientset)
	if err != nil {
		return nil, fmt.Errorf("load %s config: %w", controllerconfig.InferenceReplicaConfigName, err)
	}
	return cfg.Identity(), nil
}

// hasControllerWriteAnnotation accepts only the literal "true";
// truthy variants (yes/True) reject so hand edits can't sneak through.
func hasControllerWriteAnnotation(annotations map[string]string) bool {
	if annotations == nil {
		return false
	}
	return annotations[constants.InferenceReplicaControllerWriteAnnotationKey] ==
		constants.InferenceReplicaControllerWriteAnnotationVal
}

const projectedWriteHint = "edit the InferenceService instead"

func denyProjectedWrite(req admission.Request, parent string, who actor) admission.Response {
	if who.identityConfigured {
		return admission.Denied(fmt.Sprintf(
			"InferenceReplica %s/%s is projected by InferenceService %s/%s, whose controller alone writes its spec; %s (request from %q)",
			req.Namespace, req.Name, req.Namespace, parent, projectedWriteHint, req.UserInfo.Username))
	}
	return admission.Denied(fmt.Sprintf(
		"InferenceReplica %s/%s is projected by InferenceService %s/%s and rejects a direct %s without the %s=%s annotation; %s",
		req.Namespace, req.Name, req.Namespace, parent, req.Operation,
		constants.InferenceReplicaControllerWriteAnnotationKey, constants.InferenceReplicaControllerWriteAnnotationVal,
		projectedWriteHint))
}

func denyControllerOnlyFields(req admission.Request) admission.Response {
	return admission.Denied(fmt.Sprintf(
		"InferenceReplica %s/%s: spec.placementExecution and spec.placementReplicaLimit are written only by the InferenceService controller",
		req.Namespace, req.Name))
}

func denyStandaloneMigration(req admission.Request) admission.Response {
	return admission.Denied(fmt.Sprintf(
		"InferenceReplica %s/%s: manual migration is not available on a standalone replica; it surges through the per-revision Service only an InferenceService creates",
		req.Namespace, req.Name))
}

// admitCreate applies the create rows of the ownership table; shape checks
// run afterwards in Handle.
func (v *Validator) admitCreate(ctx context.Context, obj *v1beta1.InferenceReplica, who actor, req admission.Request) admission.Response {
	// The projector creates parentRef together with the controller owner
	// reference, so parentRef alone is refused for every writer.
	if obj.Spec.ParentRef != nil && projectingParent(obj) == nil {
		return admission.Denied(fmt.Sprintf(
			"InferenceReplica %s/%s: spec.parentRef is set by the InferenceService controller on the replicas it projects; omit it on a standalone InferenceReplica",
			req.Namespace, req.Name))
	}
	if parent, projected := projectedForm(obj); projected {
		if !who.controller {
			return denyProjectedWrite(req, parent, who)
		}
		return admission.Allowed("")
	}
	if placementFieldsSet(obj.Spec) && !who.controller {
		return denyControllerOnlyFields(req)
	}
	if err := standaloneSpecError(obj.Spec); err != nil {
		return admission.Denied(fmt.Sprintf("InferenceReplica %s/%s: %s", req.Namespace, req.Name, err))
	}
	if err := validation.ValidateInferenceServiceName(req.Name); err != nil {
		return admission.Denied(fmt.Sprintf("InferenceReplica %s/%s: a standalone replica's name is the prefix of its pod and Service names and must match %q", req.Namespace, req.Name, validation.IsvcNameFmt))
	}
	if migrationRequested(nil, obj) {
		return denyStandaloneMigration(req)
	}
	if v.Reader != nil {
		if resp := v.admitRuntimePiece(ctx, obj, req); !resp.Allowed {
			return resp
		}
		for _, candidate := range collidingInferenceServiceNames(req.Name) {
			isvc := &v1beta1.InferenceService{}
			err := v.Reader.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: candidate}, isvc)
			switch {
			case err == nil:
				return admission.Denied(fmt.Sprintf(
					"InferenceReplica %s/%s: InferenceService %s/%s exists and the replicas it projects would share pod and Service names with this one; choose another name",
					req.Namespace, req.Name, req.Namespace, candidate))
			case !apierrors.IsNotFound(err):
				return admission.Errored(http.StatusInternalServerError, fmt.Errorf("check InferenceService %s/%s: %w", req.Namespace, candidate, err))
			}
		}
	}
	return admission.Allowed("")
}

// admitUpdate applies the update rows of the ownership table. The form is
// read from the old object; immutability and metadata-only updates are
// decided by Handle before this runs.
func (v *Validator) admitUpdate(ctx context.Context, oldObj, newObj *v1beta1.InferenceReplica, who actor, req admission.Request) admission.Response {
	// A non-controller may keep or remove a controller owner reference but
	// not install one on a projected replica, whose projector would refuse
	// it and wedge the InferenceService, nor attach a standalone replica to
	// an InferenceService. Removal is how the garbage collector orphans a
	// replica; the projector re-stamps the reference while the
	// InferenceService exists.
	parent, projected := projectedForm(oldObj)
	if !who.controller && controllerRefInstalled(oldObj, newObj) && (projected || projectingParent(newObj) != nil) {
		return admission.Denied(fmt.Sprintf(
			"InferenceReplica %s/%s: only the InferenceService controller may set an InferenceService controller owner reference or change a projected replica's",
			req.Namespace, req.Name))
	}
	if projected {
		if !who.controller && specChanged(oldObj.Spec, newObj.Spec) {
			return denyProjectedWrite(req, parent, who)
		}
		return admission.Allowed("")
	}
	if !who.controller && controllerOnlyChanged(oldObj.Spec, newObj.Spec) {
		return denyControllerOnlyFields(req)
	}
	if err := standaloneSpecError(newObj.Spec); err != nil {
		return admission.Denied(fmt.Sprintf("InferenceReplica %s/%s: %s", req.Namespace, req.Name, err))
	}
	if migrationRequested(oldObj, newObj) {
		return denyStandaloneMigration(req)
	}
	// A runtime is checked when the update names it; an update that keeps
	// the reference is admitted even if the runtime has since lost the
	// piece, which the controller reports, so a scale or pause never waits
	// on the runtime.
	if v.Reader != nil && runtimeRefChanged(oldObj.Spec, newObj.Spec) {
		if resp := v.admitRuntimePiece(ctx, newObj, req); !resp.Allowed {
			return resp
		}
	}
	return admission.Allowed("")
}

// admitRuntimePiece denies a standalone replica whose named runtime resolves
// without the piece for its component, or whose inherit-from chain names a
// runtime that does not exist, a configuration no retry resolves; a replica
// that names no runtime is admitted.
func (v *Validator) admitRuntimePiece(ctx context.Context, obj *v1beta1.InferenceReplica, req admission.Request) admission.Response {
	if obj.Spec.RuntimeRef == nil {
		return admission.Allowed("")
	}
	msg, err := v.runtimePieceMissing(ctx, req.Namespace, obj.Spec)
	var parentMissing *runtimeinheritance.ParentNotFoundError
	if errors.As(err, &parentMissing) {
		return admission.Denied(fmt.Sprintf("InferenceReplica %s/%s: runtime %q inherits from %q, which does not exist, so the runtime cannot be resolved",
			req.Namespace, req.Name, obj.Spec.RuntimeRef.Name, parentMissing.Parent))
	}
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("check runtime %q: %w", obj.Spec.RuntimeRef.Name, err))
	}
	if msg != "" {
		return admission.Denied(fmt.Sprintf("InferenceReplica %s/%s: %s", req.Namespace, req.Name, msg))
	}
	return admission.Allowed("")
}

// runtimePieceMissing reports a named runtime that resolves and lacks the
// piece for the replica's component. The runtime resolves as the replica
// controller resolves it: the reference's kind picks the scope and an
// inherit-from chain is merged first. A runtime that is not found admits:
// the controller reports it on the replica and renders when it appears.
func (v *Validator) runtimePieceMissing(ctx context.Context, namespace string, spec v1beta1.InferenceReplicaSpec) (string, error) {
	ref := spec.RuntimeRef
	rt, isCluster, err := runtimeselector.NewDefaultRuntimeFetcher(v.Reader).GetRuntime(ctx, ref.Name, namespace, runtimeselector.RefKind(ref))
	if runtimeselector.IsRuntimeNotFoundError(err) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	if render.RuntimeDeclaresPiece(rt, spec.Component) {
		return "", nil
	}
	kind := runtimeselector.KindServingRuntime
	if isCluster {
		kind = runtimeselector.KindClusterServingRuntime
	}
	return fmt.Sprintf("%s %q has no %s piece; a %s replica cannot render from it", kind, ref.Name, spec.Component, spec.Component), nil
}

// Handle applies, in order: the immutability of spec.parentRef and
// spec.component, the metadata-only exemption, the ownership table for the
// replica's form and the requester's identity, then the shape checks that
// hold for every writer. An update that keeps the spec and installs no
// controller owner reference is decided without the identity.
func (v *Validator) Handle(ctx context.Context, req admission.Request) admission.Response {
	switch req.Operation {
	case admissionv1.Create, admissionv1.Update:
	default:
		return admission.Allowed("")
	}

	newObj := &v1beta1.InferenceReplica{}
	if err := v.Decoder.Decode(req, newObj); err != nil {
		log.Error(err, "Failed to decode InferenceReplica",
			"namespace", req.Namespace, "name", req.Name)
		return admission.Errored(http.StatusBadRequest, err)
	}

	var oldObj *v1beta1.InferenceReplica
	if req.Operation == admissionv1.Update {
		oldObj = &v1beta1.InferenceReplica{}
		if err := v.Decoder.DecodeRaw(req.OldObject, oldObj); err != nil {
			log.Error(err, "Failed to decode old InferenceReplica",
				"namespace", req.Namespace, "name", req.Name)
			return admission.Errored(http.StatusBadRequest, err)
		}
		if !reflect.DeepEqual(oldObj.Spec.ParentRef, newObj.Spec.ParentRef) {
			return admission.Denied(fmt.Sprintf(
				"InferenceReplica %s/%s: spec.parentRef is immutable",
				req.Namespace, req.Name))
		}
		if oldObj.Spec.Component != newObj.Spec.Component {
			return admission.Denied(fmt.Sprintf(
				"InferenceReplica %s/%s: spec.component is immutable",
				req.Namespace, req.Name))
		}
		// An update that keeps the spec and installs no controller owner
		// reference is decided without the identity: a finalizer change, a
		// label or annotation edit and the garbage collector's owner
		// reference patches must pass this fail-closed webhook even when the
		// config is unreadable. The unchanged spec is not re-checked.
		if !specChanged(oldObj.Spec, newObj.Spec) && !controllerRefInstalled(oldObj, newObj) {
			if _, projected := projectedForm(oldObj); !projected && migrationRequested(oldObj, newObj) {
				return denyStandaloneMigration(req)
			}
			return admission.Allowed("metadata-only update")
		}
	}

	// Only creates and the updates that change the spec or install a
	// controller owner reference need the identity.
	identity, err := v.controllerIdentity()
	if err != nil {
		log.Error(err, "Failed to load the InferenceReplica controller identity",
			"namespace", req.Namespace, "name", req.Name)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	who := actorFor(identity, req.UserInfo, newObj)

	if oldObj != nil {
		if resp := v.admitUpdate(ctx, oldObj, newObj, who, req); !resp.Allowed {
			return resp
		}
	} else if resp := v.admitCreate(ctx, newObj, who, req); !resp.Allowed {
		return resp
	}

	// Shape checks hold for every writer.
	if err := validateIRAutoscaler(newObj); err != nil {
		return admission.Denied(fmt.Sprintf("InferenceReplica %s/%s: %s", req.Namespace, req.Name, err.Error()))
	}
	if err := validateIRPacing(newObj); err != nil {
		return admission.Denied(fmt.Sprintf("InferenceReplica %s/%s: %s", req.Namespace, req.Name, err.Error()))
	}
	if err := validateIRRunnerVolumes(newObj); err != nil {
		return admission.Denied(fmt.Sprintf("InferenceReplica %s/%s: %s", req.Namespace, req.Name, err.Error()))
	}

	// The controller withholds the pods of an Instance that cannot fill a
	// provisioned slice, so such runners would leave every Instance they
	// reach without pods. Rejecting them leaves the IR as it was.
	if err := v.validateIRTPUSlices(ctx, oldObj, newObj); err != nil {
		return admission.Denied(fmt.Sprintf(
			"InferenceReplica %s/%s: %s",
			req.Namespace, req.Name, err.Error()))
	}

	return admission.Allowed("")
}

// validateIRAutoscaler runs the shared Autoscaler shape check against
// an IR's projected Autoscaler block. The IR spec carries its own
// Replicas field (not a ComponentExtensionSpec); we pass it as the
// effective MinReplicas floor for the KEDA idle-vs-min check. Calls
// validation.ValidateAutoscaler directly — the
// (*ComponentAutoscaler, *int) signature accepts this shape without
// synthesizing a parent ComponentExtensionSpec. Shared with the
// InferenceService and ServingRuntime webhooks.
func validateIRAutoscaler(ir *v1beta1.InferenceReplica) error {
	if ir == nil {
		return nil
	}
	var minPtr *int
	if ir.Spec.Replicas != nil {
		min := int(*ir.Spec.Replicas)
		minPtr = &min
	}
	return validation.ValidateAutoscaler(ir.Spec.Autoscaler, minPtr)
}

// validateIRPacing checks that Pacing.Partition <= effective Replicas.
// An over-Partition (Partition > Replicas) silently holds every
// Instance back forever — the rollout engine treats Partition as a
// per-index threshold and freezes any Instance with index < Partition,
// so Partition >= Replicas freezes the whole replica set. Matches the
// effective-Replicas default used by convert.desiredFromIR (nil or 0
// → 1).
func validateIRPacing(ir *v1beta1.InferenceReplica) error {
	if ir == nil || ir.Spec.Pacing == nil || ir.Spec.Pacing.Partition == nil {
		return nil
	}
	partition := *ir.Spec.Pacing.Partition
	// Match convert.desiredFromIR's defaulting: nil OR *r == 0 → 1.
	replicas := int32(1)
	if ir.Spec.Replicas != nil && *ir.Spec.Replicas > 0 {
		replicas = *ir.Spec.Replicas
	}
	if partition > replicas {
		return fmt.Errorf(
			"spec.pacing.partition (%d) must be <= spec.replicas (%d); "+
				"an over-partition silently holds every Instance back forever",
			partition, replicas)
	}
	return nil
}

// validateIRRunnerVolumes checks every Runner's fully-rendered pod
// template so that each container/initContainer volumeMount references a
// volume declared in that same pod spec. The IR webhook sees the final
// rendered leader+worker templates, so this is the most precise place to
// catch a dangling mount before it reaches the apiserver at pod-create
// time (where it fails as "spec.containers[i].volumeMounts[j].name: Not
// found: <name>" and surfaces only as a buried reconcile error). Names
// the runner, container, and missing volume so the operator can fix the
// runtime spec — including the multi-node hint, since a dshm/shared
// volume must be declared under engineConfig.leader.volumes /
// engineConfig.worker.volumes for the leader+worker templates that mount
// it.
func validateIRRunnerVolumes(ir *v1beta1.InferenceReplica) error {
	if ir == nil {
		return nil
	}
	for i := range ir.Spec.Runners {
		runner := &ir.Spec.Runners[i]
		spec := &runner.Template.Spec

		declared := make(map[string]struct{}, len(spec.Volumes))
		for j := range spec.Volumes {
			declared[spec.Volumes[j].Name] = struct{}{}
		}

		// initContainers and containers share the pod's volume set, so
		// validate both against the same declared map.
		if err := validateContainerVolumeMounts(
			string(runner.Name), spec.InitContainers, declared); err != nil {
			return err
		}
		if err := validateContainerVolumeMounts(
			string(runner.Name), spec.Containers, declared); err != nil {
			return err
		}
	}
	return nil
}

// validateContainerVolumeMounts verifies every volumeMount in the given
// containers resolves to a name in `declared`. Returns a message naming
// the runner, container, and missing volume on the first violation.
func validateContainerVolumeMounts(
	runnerName string, containers []corev1.Container, declared map[string]struct{},
) error {
	for c := range containers {
		container := &containers[c]
		for m := range container.VolumeMounts {
			name := container.VolumeMounts[m].Name
			if _, ok := declared[name]; !ok {
				return fmt.Errorf(
					"runner %q container %q: volumeMount %q has no matching "+
						"volume in the pod (for a multi-node runtime declare it "+
						"under engineConfig.leader.volumes / "+
						"engineConfig.worker.volumes)",
					runnerName, container.Name, name)
			}
		}
	}
	return nil
}

// validateIRTPUSlices rejects runners whose Instance the controller cannot
// place on a provisioned slice. An update is rejected only when it
// introduces the failure: one that keeps the runners, or whose previous
// runners fail too, is admitted. A failed node read admits the IR; the
// controller repeats the check before it creates pods.
func (v *Validator) validateIRTPUSlices(ctx context.Context, oldObj, newObj *v1beta1.InferenceReplica) error {
	if v.TPUSliceProvisioning == nil || v.Nodes == nil {
		return nil
	}
	if oldObj != nil && reflect.DeepEqual(oldObj.Spec.Runners, newObj.Spec.Runners) {
		return nil
	}
	err := v.invalidSliceDemand(ctx, newObj)
	if err == nil || (oldObj != nil && v.invalidSliceDemand(ctx, oldObj) != nil) {
		return nil
	}
	return err
}

// invalidSliceDemand returns why an Instance of ir cannot run on a
// provisioned slice. It returns nil when it can, when ir does not opt in,
// and when the nodes cannot be read.
func (v *Validator) invalidSliceDemand(ctx context.Context, ir *v1beta1.InferenceReplica) error {
	if !slicesOptedIn(ir) {
		return nil
	}
	_, _, err := sliceprovision.Resolve(ctx, v.Nodes, v.TPUSliceProvisioning, instancePodSpecs(ir))
	if err != nil && !errors.Is(err, sliceprovision.ErrInvalidDemand) {
		log.Error(err, "Admitting InferenceReplica without checking its TPU slice demand",
			"namespace", ir.Namespace, "name", ir.Name)
		return nil
	}
	return err
}

// slicesOptedIn reports whether the controller places ir's pods on
// provisioned slices: the template of its default or leader runner opts in.
func slicesOptedIn(ir *v1beta1.InferenceReplica) bool {
	for i := range ir.Spec.Runners {
		runner := &ir.Spec.Runners[i]
		if runner.Name == v1beta1.RunnerNameDefault || runner.Name == v1beta1.RunnerNameLeader {
			return runner.Template.Annotations[constants.TPUSliceProvisioningAnnotationKey] == "true"
		}
	}
	return false
}

// instancePodSpecs are the specs of one Instance's pods, one per pod, as
// the controller creates them: one from the default or leader runner's
// template, and Size from the worker runner's.
func instancePodSpecs(ir *v1beta1.InferenceReplica) []*corev1.PodSpec {
	var specs []*corev1.PodSpec
	for i := range ir.Spec.Runners {
		runner := &ir.Spec.Runners[i]
		n := int32(1)
		if runner.Name == v1beta1.RunnerNameWorker {
			n = runner.Size
		}
		for ; n > 0; n-- {
			specs = append(specs, &runner.Template.Spec)
		}
	}
	return specs
}
