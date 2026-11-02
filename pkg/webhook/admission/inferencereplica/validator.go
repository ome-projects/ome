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
// On a replica it does not project, the InferenceService controller is a
// composer, not the owner: it may not create the replica, and it may change
// only the rollout-control fields pacing.partition, paused, pauseMode,
// pacing.rollbackToRevision and pairingProtocol, the controller-only fields,
// and the ome.io/composed-fields annotation that records the fields it
// holds; a change to any other spec field is rejected, and
// pacing.maxUnavailable is a user field to it. Only the controller adds,
// changes or removes that annotation, on either form; anyone may keep it.
// The rule is by identity and field group, not by whether an
// InferenceService names the replica at that moment: the write that clears
// the held fields arrives after the reference is gone.
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
// which is a convention rather than a boundary; a composer's write on a
// replica it does not project carries no such stamp, so in that mode no
// InferenceService takes or releases rollout control on a referenced
// replica. An update that keeps the spec, installs no controller owner
// reference and leaves ome.io/composed-fields unchanged is decided without
// the identity, so an unreadable config never refuses one.
//
// A standalone replica renders from exactly one template source:
// spec.runners, or spec.modelRef and/or spec.runtimeRef; a fixed runtime
// revision is rejected. A named runtime that resolves without the piece for
// the replica's component, or whose inherit-from chain names a runtime that
// does not exist, is rejected when the reference is set or changed; one that
// is not found is admitted, and the controller reports it on the replica.
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
	"strings"

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

// write is one create or update as the ownership table reads it: the
// request, the requester, the object before the write (nil on create), the
// object written, and the replica's form. The form is read from the old
// object on update, so a write is decided by the form the replica had.
type write struct {
	req       admission.Request
	who       actor
	old, obj  *v1beta1.InferenceReplica
	parent    string
	projected bool
}

func newWrite(req admission.Request, old, obj *v1beta1.InferenceReplica) write {
	w := write{req: req, old: old, obj: obj}
	before := obj
	if old != nil {
		before = old
	}
	w.parent, w.projected = projectedForm(before)
	return w
}

func (w write) create() bool { return w.old == nil }

// specWritten reports whether the write sets the spec: every create does,
// and an update that changes it.
func (w write) specWritten() bool {
	return w.old == nil || specChanged(w.old.Spec, w.obj.Spec)
}

// oldSpec is the spec before the write, empty on create.
func (w write) oldSpec() v1beta1.InferenceReplicaSpec {
	if w.old == nil {
		return v1beta1.InferenceReplicaSpec{}
	}
	return w.old.Spec
}

// deny refuses the write, naming the replica before the reason.
func deny(w write, reason string) admission.Response {
	return admission.Denied(fmt.Sprintf("InferenceReplica %s/%s: %s", w.req.Namespace, w.req.Name, reason))
}

const projectedWriteHint = "edit the InferenceService instead"

func denyProjectedWrite(w write) admission.Response {
	req := w.req
	if w.who.identityConfigured {
		return admission.Denied(fmt.Sprintf(
			"InferenceReplica %s/%s is projected by InferenceService %s/%s, whose controller alone writes its spec; %s (request from %q)",
			req.Namespace, req.Name, req.Namespace, w.parent, projectedWriteHint, req.UserInfo.Username))
	}
	return admission.Denied(fmt.Sprintf(
		"InferenceReplica %s/%s is projected by InferenceService %s/%s and rejects a direct %s without the %s=%s annotation; %s",
		req.Namespace, req.Name, req.Namespace, w.parent, req.Operation,
		constants.InferenceReplicaControllerWriteAnnotationKey, constants.InferenceReplicaControllerWriteAnnotationVal,
		projectedWriteHint))
}

func denyParentRefWithoutOwner(w write) admission.Response {
	return deny(w, "spec.parentRef is set by the InferenceService controller on the replicas it projects; omit it on a standalone InferenceReplica")
}

func denyOwnerReferenceInstall(w write) admission.Response {
	return deny(w, "only the InferenceService controller may set an InferenceService controller owner reference or change a projected replica's")
}

func denyComposedFieldsWrite(w write) admission.Response {
	return deny(w, fmt.Sprintf(
		"the %s annotation is written by the InferenceService controller; to release the fields it names, remove the reference on the InferenceService instead",
		constants.InferenceReplicaComposedFieldsAnnotationKey))
}

// denyControllerStandaloneCreate refuses a standalone create by the
// controller. With no identity configured the controller is recognized by
// the controller-write annotation, which a manifest copied from a projected
// replica carries, so the denial names it as the thing to drop.
func denyControllerStandaloneCreate(w write) admission.Response {
	if w.who.identityConfigured {
		return deny(w, "the InferenceService controller creates only the replicas it projects; create a standalone replica as a user")
	}
	return admission.Denied(fmt.Sprintf(
		"InferenceReplica %s/%s carries the %s=%s annotation, which marks a write by the InferenceService controller, and the controller creates only the replicas it projects; create a standalone replica without it",
		w.req.Namespace, w.req.Name,
		constants.InferenceReplicaControllerWriteAnnotationKey, constants.InferenceReplicaControllerWriteAnnotationVal))
}

func denyControllerOnlyFields(w write) admission.Response {
	return deny(w, "spec.placementExecution and spec.placementReplicaLimit are written only by the InferenceService controller")
}

func denyControllerUserFieldWrite(w write) admission.Response {
	return deny(w, fmt.Sprintf(
		"the InferenceService controller writes only rollout-control fields (%s) on a replica it does not project; the replica's owner writes its other fields",
		rolloutControlFieldList))
}

func denySpecError(w write) admission.Response {
	return deny(w, standaloneSpecError(w.obj.Spec).Error())
}

func denyStandaloneName(w write) admission.Response {
	return deny(w, fmt.Sprintf("a standalone replica's name is the prefix of its pod and Service names and must match %q", validation.IsvcNameFmt))
}

func denyStandaloneMigration(w write) admission.Response {
	return deny(w, "manual migration is not available on a standalone replica; it surges through the per-revision Service only an InferenceService creates")
}

// replicaForm selects the rows read for a write: those of its form and
// those of either form.
type replicaForm uint8

const (
	eitherForm replicaForm = iota
	projectedReplica
	standaloneReplica
)

// operation selects the rows read on create, on update, or on both.
type operation uint8

const (
	createOrUpdate operation = iota
	onCreate
	onUpdate
)

// ownershipRow is one row of the ownership table: the form and operation
// it is read on, the condition under which the write breaks it, and the
// refusal.
type ownershipRow struct {
	form    replicaForm
	on      operation
	applies func(w write) bool
	deny    func(w write) admission.Response
}

// refuses reports whether the row is read on this write, by form and
// operation, and its condition holds.
func (r ownershipRow) refuses(w write) bool {
	if r.form != eitherForm && (r.form == projectedReplica) != w.projected {
		return false
	}
	if r.on != createOrUpdate && (r.on == onCreate) != w.create() {
		return false
	}
	return r.applies(w)
}

// ownershipTable is the ownership rule the package comment states, as the
// rows Handle applies in order once a write has passed the immutability
// and metadata-only checks. The first row that refuses answers the write;
// a write no row refuses goes on to the cluster reads and the shape checks.
var ownershipTable = []ownershipRow{
	// The projector creates spec.parentRef together with the controller
	// owner reference, so parentRef alone is refused for every writer.
	{on: onCreate,
		applies: func(w write) bool { return w.obj.Spec.ParentRef != nil && projectingParent(w.obj) == nil },
		deny:    denyParentRefWithoutOwner},
	// A non-controller may keep or remove a controller owner reference but
	// not install one on a projected replica, whose projector would refuse
	// it and stop the InferenceService, nor attach a standalone replica to
	// an InferenceService. Removal is how the garbage collector orphans a
	// replica; the projector restores the reference while the
	// InferenceService exists.
	{on: onUpdate,
		applies: func(w write) bool {
			return !w.who.controller && controllerRefInstalled(w.old, w.obj) && (w.projected || projectingParent(w.obj) != nil)
		},
		deny: denyOwnerReferenceInstall},
	// The composed-fields annotation records the fields the controller
	// holds on a replica, so on either form only the controller changes it.
	{applies: func(w write) bool { return !w.who.controller && composedFieldsChanged(w.old, w.obj) },
		deny: denyComposedFieldsWrite},
	// Only the controller writes a projected replica's spec.
	{form: projectedReplica,
		applies: func(w write) bool { return !w.who.controller && w.specWritten() },
		deny:    denyProjectedWrite},
	// The controller creates only the replicas it projects, so a
	// standalone create comes from a user.
	{form: standaloneReplica, on: onCreate,
		applies: func(w write) bool { return w.who.controller },
		deny:    denyControllerStandaloneCreate},
	// The placement fields are the controller's on every replica.
	{form: standaloneReplica,
		applies: func(w write) bool { return !w.who.controller && controllerOnlyChanged(w.oldSpec(), w.obj.Spec) },
		deny:    denyControllerOnlyFields},
	// On a replica it does not project the controller is a composer, not
	// the owner: it steers the rollout through the rollout-control fields
	// and leaves every user field alone.
	{form: standaloneReplica, on: onUpdate,
		applies: func(w write) bool { return w.who.controller && userFieldsChanged(w.old.Spec, w.obj.Spec) },
		deny:    denyControllerUserFieldWrite},
	// A standalone spec renders from one template source and carries no
	// setting nothing acts on.
	{form: standaloneReplica,
		applies: func(w write) bool { return standaloneSpecError(w.obj.Spec) != nil },
		deny:    denySpecError},
	// A standalone replica's name is the prefix of its pod and Service
	// names.
	{form: standaloneReplica, on: onCreate,
		applies: func(w write) bool { return validation.ValidateInferenceServiceName(w.req.Name) != nil },
		deny:    denyStandaloneName},
	// A manual migration surges through the per-revision Service only an
	// InferenceService creates.
	{form: standaloneReplica,
		applies: func(w write) bool { return migrationRequested(w.old, w.obj) },
		deny:    denyStandaloneMigration},
}

// admit applies the ownership table to one write and, on a standalone
// replica, the checks that read the cluster.
func (v *Validator) admit(ctx context.Context, w write) admission.Response {
	for _, row := range ownershipTable {
		if row.refuses(w) {
			return row.deny(w)
		}
	}
	if w.projected || v.Reader == nil {
		return admission.Allowed("")
	}
	return v.admitStandaloneReads(ctx, w)
}

// admitStandaloneReads checks a standalone replica against the cluster. The
// runtime it names must declare its piece, read on create and when the
// reference changes: an update that keeps the reference is admitted even if
// the runtime has since lost the piece, which the controller reports, so a
// scale or pause never waits on the runtime. On create no InferenceService
// may project replicas of the same name.
func (v *Validator) admitStandaloneReads(ctx context.Context, w write) admission.Response {
	if w.create() || runtimeRefChanged(w.old.Spec, w.obj.Spec) {
		if resp := v.admitRuntimePiece(ctx, w); !resp.Allowed {
			return resp
		}
	}
	if !w.create() {
		return admission.Allowed("")
	}
	for _, candidate := range collidingInferenceServiceNames(w.req.Name) {
		isvc := &v1beta1.InferenceService{}
		err := v.Reader.Get(ctx, types.NamespacedName{Namespace: w.req.Namespace, Name: candidate}, isvc)
		switch {
		case err == nil:
			return deny(w, fmt.Sprintf(
				"InferenceService %s/%s exists and the replicas it projects would share pod and Service names with this one; choose another name",
				w.req.Namespace, candidate))
		case !apierrors.IsNotFound(err):
			return admission.Errored(http.StatusInternalServerError, fmt.Errorf("check InferenceService %s/%s: %w", w.req.Namespace, candidate, err))
		}
	}
	return admission.Allowed("")
}

// admitRuntimePiece denies a standalone replica whose named runtime resolves
// without the piece for its component, or whose inherit-from chain names a
// runtime that does not exist, a configuration no retry resolves; a replica
// that names no runtime is admitted.
func (v *Validator) admitRuntimePiece(ctx context.Context, w write) admission.Response {
	ref := w.obj.Spec.RuntimeRef
	if ref == nil {
		return admission.Allowed("")
	}
	msg, err := v.runtimePieceMissing(ctx, w.req.Namespace, w.obj.Spec)
	var parentMissing *runtimeinheritance.ParentNotFoundError
	if errors.As(err, &parentMissing) {
		return deny(w, fmt.Sprintf("runtime %q inherits from %q, which does not exist, so the runtime cannot be resolved", ref.Name, parentMissing.Parent))
	}
	if err != nil {
		return admission.Errored(http.StatusInternalServerError, fmt.Errorf("check runtime %q: %w", ref.Name, err))
	}
	if msg != "" {
		return deny(w, msg)
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
// hold for every writer. An update that keeps the spec, installs no
// controller owner reference and leaves the composed-fields annotation
// unchanged is decided without the identity.
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
	}
	w := newWrite(req, oldObj, newObj)
	if oldObj == nil {
		// The component picks the runtime piece, the pod label and the
		// workload role; a value that names no role would run with none.
		// It is immutable, so checking the create covers every update.
		if _, ok := v1beta1.RoleOf(newObj.Spec.Component); !ok {
			return deny(w, fmt.Sprintf("spec.component %q is not a role: use one of %s", newObj.Spec.Component, roleNames()))
		}
	}
	if oldObj != nil {
		if !reflect.DeepEqual(oldObj.Spec.ParentRef, newObj.Spec.ParentRef) {
			return deny(w, "spec.parentRef is immutable")
		}
		if oldObj.Spec.Component != newObj.Spec.Component {
			return deny(w, "spec.component is immutable")
		}
		// An update that keeps the spec, installs no controller owner
		// reference and leaves the composed-fields annotation unchanged is
		// decided without the identity: a finalizer change, a label or
		// annotation edit and the garbage collector's owner reference patches
		// must pass this fail-closed webhook even when the config is
		// unreadable. The unchanged spec is not re-checked.
		if !specChanged(oldObj.Spec, newObj.Spec) && !controllerRefInstalled(oldObj, newObj) && !composedFieldsChanged(oldObj, newObj) {
			if !w.projected && migrationRequested(oldObj, newObj) {
				return denyStandaloneMigration(w)
			}
			return admission.Allowed("metadata-only update")
		}
	}

	// Only creates and the updates that change the spec, install a
	// controller owner reference or touch the composed-fields annotation
	// need the identity.
	identity, err := v.controllerIdentity()
	if err != nil {
		log.Error(err, "Failed to load the InferenceReplica controller identity",
			"namespace", req.Namespace, "name", req.Name)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	w.who = actorFor(identity, req.UserInfo, newObj)
	if resp := v.admit(ctx, w); !resp.Allowed {
		return resp
	}

	for _, check := range shapeChecks {
		if err := check(newObj); err != nil {
			return deny(w, err.Error())
		}
	}
	// The controller withholds the pods of an Instance that cannot fill a
	// provisioned slice, so such runners would leave every Instance they
	// reach without pods. Rejecting them leaves the IR as it was.
	if err := v.validateIRTPUSlices(ctx, oldObj, newObj); err != nil {
		return deny(w, err.Error())
	}
	return admission.Allowed("")
}

// roleNames lists every role's name in the order of v1beta1.Roles.
func roleNames() string {
	names := make([]string, 0, len(v1beta1.Roles))
	for _, c := range v1beta1.RoleTypes() {
		names = append(names, string(c))
	}
	return strings.Join(names, ", ")
}

// shapeChecks hold for every writer and run after the ownership table.
var shapeChecks = []func(*v1beta1.InferenceReplica) error{
	validateIRAutoscaler,
	validateIRPacing,
	validateIRRunnerVolumes,
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
