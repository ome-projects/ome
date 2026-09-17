package mutate

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/paging"
	reportv1alpha1 "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
)

// MigrationEvidence grants no caller-supplied completeness shortcut. Its
// private copies are never output; only MigrationPlan's reviewed values are.
type MigrationEvidence struct {
	complete           bool
	uid, rv, options   string
	ir                 *v1beta1.InferenceReplica
	source             *v1beta1.OMENativeInstanceStatus
	fromNode           string
	mode               string
	nodes              []string
	pending            map[string]migrationRequest
	known, conflicting map[string]bool
	warnings           []string
	fingerprint        string
}

func (MigrationEvidence) MarshalJSON() ([]byte, error) {
	return nil, errors.New("private migration evidence cannot be serialized")
}
func (MigrationEvidence) String() string   { return "<mutate.MigrationEvidence redacted>" }
func (MigrationEvidence) GoString() string { return "<mutate.MigrationEvidence redacted>" }

type migrationIdentity struct {
	component string
	index     int32
	from      string
}

func exactMigrationOwner(refs []metav1.OwnerReference, kind, name string, uid types.UID) bool {
	count := 0
	matched := false
	for _, ref := range refs {
		if ref.Controller != nil && *ref.Controller {
			count++
			matched = ref.APIVersion == "ome.io/v1beta1" && ref.Kind == kind && ref.Name == name && ref.UID == uid
		}
	}
	return count == 1 && matched
}

// CollectMigrationEvidence acquires only current-context typed resources.
// Object/collection budgets are post-decode inspection bounds, not HTTP GET
// body limits. No evidence is returned on an incomplete required acquisition.
func CollectMigrationEvidence(ctx context.Context, ome omeclient.OmeV1beta1Interface, kube kubernetes.Interface, parent *v1beta1.InferenceService, native []string, o MigrationOptions, clock reportv1alpha1.Clock) (MigrationEvidence, error) {
	if err := ValidateTarget(parent); err != nil {
		return MigrationEvidence{}, err
	}
	if err := ValidateMigrationOptions(o); err != nil {
		return MigrationEvidence{}, err
	}
	if ctx == nil || ome == nil || kube == nil {
		return MigrationEvidence{}, ErrRuntime
	}
	e := MigrationEvidence{uid: string(parent.UID), rv: parent.ResourceVersion, options: migrationOptionKey(o), pending: map[string]migrationRequest{}, known: map[string]bool{}, conflicting: map[string]bool{}}
	var replicas []v1beta1.InferenceReplica
	_, err := collectReplicaEvidence(ctx, ome, parent, native, clock, &replicas, nil)
	if err != nil {
		if errors.Is(err, ErrStale) {
			return MigrationEvidence{}, migrationConflict()
		}
		return MigrationEvidence{}, err
	}
	identities := map[string]migrationIdentity{}
	add := func(id string, identity migrationIdentity) {
		e.known[id] = true
		if prior, ok := identities[id]; ok && (prior.component != identity.component || prior.index != identity.index || prior.from != "" && identity.from != "" && prior.from != identity.from) {
			e.conflicting[id] = true
		} else if !ok {
			identities[id] = identity
		}
	}
	for key, raw := range parent.Annotations {
		if !strings.HasPrefix(key, constants.MigrationRequestAnnotationPrefix) {
			continue
		}
		id := strings.TrimPrefix(key, constants.MigrationRequestAnnotationPrefix)
		if !validMigrationUUID(id) {
			return MigrationEvidence{}, errors.New("retained migration mailbox identity is invalid")
		}
		request, err := parseMigrationRequest(raw)
		if err != nil {
			return MigrationEvidence{}, err
		}
		e.pending[id] = request
		add(id, migrationIdentity{request.Component, request.Instance, request.FromNode})
	}
	for i := range replicas {
		ir := &replicas[i]
		if ir.Spec.Component == o.Component {
			e.ir = ir
		}
		for _, r := range ir.Status.Migrations {
			add(r.RequestUUID, migrationIdentity{string(ir.Spec.Component), r.SourceInstance, r.FromNode})
		}
	}
	if e.ir == nil || !slices.Contains(native, string(o.Component)) {
		return MigrationEvidence{}, migrationConflict()
	}
	if len(parent.Status.MigrationHistory) > 256 {
		return MigrationEvidence{}, ErrBounds
	}
	for _, r := range parent.Status.MigrationHistory {
		if !SafeScalar(r.ID) || r.Instance < 0 {
			return MigrationEvidence{}, errors.New("retained parent migration history is invalid")
		}
		add(r.ID, migrationIdentity{string(r.Component), r.Instance, ""})
	}
	if err := e.collectAudit(ctx, kube, parent, add); err != nil {
		if ctx.Err() != nil {
			return MigrationEvidence{}, ctx.Err()
		}
		if o.RequestID != "" {
			return MigrationEvidence{}, err
		}
		e.warnings = append(e.warnings, "Audit lookup unavailable; retained delivery history is incomplete.")
	}
	if o.RequestID != "" {
		e.complete = true
		return e, nil
	}
	paused, _ := constants.RolloutPauseState(parent.Annotations)
	if paused || e.ir.Spec.Paused {
		return MigrationEvidence{}, migrationConflict()
	}
	for _, key := range []string{constants.RolloutPromoteAnnotation, constants.RolloutRollbackAnnotation} {
		if _, ok := parent.Annotations[key]; ok {
			return MigrationEvidence{}, migrationConflict()
		}
	}
	for _, request := range e.pending {
		if request.Component == string(o.Component) && request.Instance == o.Instance {
			return MigrationEvidence{}, migrationConflict()
		}
		e.warnings = append(e.warnings, "Other migration requests are queued; dispatch is serial and capacity is not guaranteed.")
	}
	for _, r := range e.ir.Status.Migrations {
		if !r.Phase.Terminal() && (r.SourceInstance == o.Instance || r.SurgeInstance != nil && *r.SurgeInstance == o.Instance) {
			return MigrationEvidence{}, migrationConflict()
		}
		if !r.Phase.Terminal() {
			e.warnings = append(e.warnings, "Other migration work is queued or executing; dispatch is serial and capacity is not guaranteed.")
		}
	}
	desired := workload.WorkloadDesiredSpec{Replicas: 1}
	if e.ir.Spec.Replicas != nil {
		if *e.ir.Spec.Replicas < 0 {
			return MigrationEvidence{}, migrationConflict()
		}
		// Bound desired slot expansion before the non-cancelable pure planner.
		// This is a count budget, never a limit on canonical sparse indices.
		if *e.ir.Spec.Replicas > 2048 {
			return MigrationEvidence{}, ErrBounds
		}
		desired.Replicas = *e.ir.Spec.Replicas
	}
	if e.ir.Spec.Lifecycle != nil {
		// The actor adapter intentionally maps unknown API enums to empty for
		// version skew. Mutation authority must reject before that lossy step.
		if policy := e.ir.Spec.Lifecycle.MigrationPolicy; policy != nil && policy.Mode != "" && policy.Mode != v1beta1.MigrationPolicyModeAuto && policy.Mode != v1beta1.MigrationPolicyModeSurge {
			return MigrationEvidence{}, migrationConflict()
		}
		desired.Lifecycle = v1beta1convert.LifecycleSpecToWorkload(*e.ir.Spec.Lifecycle)
	}
	mode := workload.MigrationModeOrDefault(desired.Lifecycle.MigrationPolicy)
	if mode != workload.MigrationModeAuto && mode != workload.MigrationModeSurge {
		return MigrationEvidence{}, migrationConflict()
	}
	e.mode = string(mode)
	if !validMigrationRunners(e.ir) {
		return MigrationEvidence{}, migrationConflict()
	}
	for _, runner := range e.ir.Spec.Runners {
		desired.Runners = append(desired.Runners, workload.Runner{Name: string(runner.Name), Size: runner.Size})
		desired.MultiPod = desired.MultiPod || runner.Name != v1beta1.RunnerNameDefault
	}
	plan, err := workload.BuildPlan(workload.ComponentType(o.Component), desired, workload.WorkloadObservedState{InstanceStatuses: v1beta1convert.InstanceStatusSliceToWorkload(e.ir.Status.InstanceStatuses)})
	if err != nil {
		return MigrationEvidence{}, migrationConflict()
	}
	inPlan := false
	for _, inst := range plan.Instances {
		inPlan = inPlan || inst.Index == o.Instance
	}
	for i := range e.ir.Status.InstanceStatuses {
		r := &e.ir.Status.InstanceStatuses[i]
		if r.Operation != nil && r.Operation.SurgeIndex != nil && *r.Operation.SurgeIndex == o.Instance {
			return MigrationEvidence{}, migrationConflict()
		}
		if r.Index == o.Instance {
			e.source = r
		}
	}
	if !inPlan || e.source == nil || e.source.Phase != v1beta1.OMENativeInstanceReady || e.source.Operation != nil || e.source.RunningRevision == "" || e.source.TargetRevision != "" && e.source.TargetRevision != e.source.RunningRevision {
		return MigrationEvidence{}, migrationConflict()
	}
	pods, cr, err := collectMigrationSource(ctx, kube, parent, e.ir, e.source)
	if err != nil {
		return MigrationEvidence{}, err
	}
	nodes, err := inspectMigrationPods(parent, e.ir, e.source, pods)
	if err != nil {
		return MigrationEvidence{}, err
	}
	e.nodes = nodes
	e.fromNode = o.FromNode
	if e.fromNode == "" {
		if len(nodes) != 1 {
			return MigrationEvidence{}, migrationConflict()
		}
		e.fromNode = nodes[0]
	}
	if !slices.Contains(nodes, e.fromNode) {
		return MigrationEvidence{}, migrationConflict()
	}
	var payload revision.DataPayload
	if len(cr.Data.Raw) > 1024*1024 || !strictJSON(cr.Data.Raw) || json.Unmarshal(cr.Data.Raw, &payload) != nil || payload.PodSpec == nil || !boundedPrivatePayload(payload) {
		return MigrationEvidence{}, errors.New("source revision payload is invalid or exceeds bounds")
	}
	if desired.MultiPod != (payload.WorkerPodSpec != nil) {
		return MigrationEvidence{}, migrationConflict()
	}
	overlay := &workload.MigrationOverlay{FromNode: e.fromNode}
	if ops.WouldOverlayConflictWithNodeAffinity(payload.PodSpec, overlay) || ops.WouldOverlayConflictWithNodeAffinity(payload.WorkerPodSpec, overlay) {
		return MigrationEvidence{}, migrationConflict()
	}
	e.fingerprint = migrationSourceFingerprint(e.ir, pods, cr)
	e.complete = true
	e.warnings = append(e.warnings, "Node hints are soft preferences; capacity, scheduling and controller convergence are not guaranteed.", "Parent UID/resourceVersion CAS is not an IR/Pod/runtime transaction.")
	return e, nil
}

func validMigrationRunners(ir *v1beta1.InferenceReplica) bool {
	if len(ir.Spec.Runners) == 1 {
		return ir.Spec.Runners[0].Name == v1beta1.RunnerNameDefault && ir.Spec.Runners[0].Size == 1
	}
	if len(ir.Spec.Runners) != 2 {
		return false
	}
	leader, worker, total := false, false, int64(0)
	for _, r := range ir.Spec.Runners {
		total += int64(r.Size)
		switch r.Name {
		case v1beta1.RunnerNameLeader:
			if leader || r.Size != 1 {
				return false
			}
			leader = true
		case v1beta1.RunnerNameWorker:
			if worker || r.Size < 1 {
				return false
			}
			worker = true
		default:
			return false
		}
	}
	return leader && worker && total <= 128
}

type migrationAuditEntry struct {
	ID        string   `json:"requestUUID"`
	Component string   `json:"component"`
	Index     *int32   `json:"sourceInstance"`
	FromNode  string   `json:"fromNode"`
	Hints     []string `json:"hintTargetNodes"`
	Phase     string   `json:"phase"`
}

func (e *MigrationEvidence) collectAudit(ctx context.Context, kube kubernetes.Interface, parent *v1beta1.InferenceService, add func(string, migrationIdentity)) error {
	name := parent.Name + "-ome-migration-audit"
	if !validMigrationNode(name) {
		return errors.New("audit lookup target is invalid")
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cm, err := kube.CoreV1().ConfigMaps(parent.Namespace).Get(readCtx, name, metav1.GetOptions{})
	if readCtx.Err() != nil {
		return readCtx.Err()
	}
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return SafeAPIError(err)
	}
	if cm == nil || cm.Name != name || cm.Namespace != parent.Namespace || !boundedPrivatePayload(cm) || len(cm.OwnerReferences) > 16 || !exactMigrationOwner(cm.OwnerReferences, "InferenceService", parent.Name, parent.UID) {
		return errors.New("audit lookup source is invalid or exceeds bounds")
	}
	raw := cm.Data["history.json"]
	if raw == "" {
		return nil
	}
	if len(raw) > 1024*1024 || !strings.HasPrefix(strings.TrimSpace(raw), "{") || !strictJSON([]byte(raw)) {
		return errors.New("audit lookup payload is invalid or exceeds bounds")
	}
	var ledger struct {
		Entries []migrationAuditEntry `json:"entries"`
	}
	if json.Unmarshal([]byte(raw), &ledger) != nil || len(ledger.Entries) > 800 {
		return errors.New("audit lookup payload is invalid or exceeds bounds")
	}
	for _, r := range ledger.Entries {
		if !SafeScalar(r.ID) || r.Index == nil || *r.Index < 0 || len(r.Hints) > 64 || r.FromNode != "" && !validMigrationNode(r.FromNode) || r.Phase != "Started" && r.Phase != "Completed" && r.Phase != "Failed" {
			return errors.New("audit lookup record is invalid")
		}
		for _, node := range r.Hints {
			if !validMigrationNode(node) {
				return errors.New("audit lookup node hint is invalid")
			}
		}
		add(r.ID, migrationIdentity{r.Component, *r.Index, r.FromNode})
	}
	return nil
}

func collectMigrationSource(ctx context.Context, kube kubernetes.Interface, parent *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, source *v1beta1.OMENativeInstanceStatus) ([]corev1.Pod, *appsv1.ControllerRevision, error) {
	selector := labels.Set{constants.InferenceServicePodLabelKey: parent.Name, constants.OMEComponentLabel: string(ir.Spec.Component), query.LabelManagedBy: query.ManagedByOMENative, query.LabelInstanceIdx: strconv.FormatInt(int64(source.Index), 10)}.AsSelector().String()
	list, err := paging.ListBounded(ctx, metav1.ListOptions{LabelSelector: selector}, paging.Limits{PageSize: 16, MaxItems: 128, MaxPages: 8, RequestTimeout: 10 * time.Second}, func(requestCtx context.Context, options metav1.ListOptions) (paging.Page[corev1.Pod], error) {
		value, err := kube.CoreV1().Pods(parent.Namespace).List(requestCtx, options)
		if requestCtx.Err() != nil {
			return paging.Page[corev1.Pod]{}, requestCtx.Err()
		}
		if err != nil {
			return paging.Page[corev1.Pod]{}, SafeAPIError(err)
		}
		if value == nil {
			return paging.Page[corev1.Pod]{}, migrationConflict()
		}
		return paging.Page[corev1.Pod]{Items: value.Items, Continue: value.Continue}, nil
	})
	if err != nil {
		return nil, nil, err
	}
	if list.Truncated {
		return nil, nil, ErrBounds
	}
	for i := range list.Items {
		if !boundedPrivatePayload(&list.Items[i]) {
			return nil, nil, ErrBounds
		}
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	cr, err := kube.AppsV1().ControllerRevisions(ir.Namespace).Get(readCtx, source.RunningRevision, metav1.GetOptions{})
	if readCtx.Err() != nil {
		return nil, nil, readCtx.Err()
	}
	if apierrors.IsNotFound(err) {
		return nil, nil, migrationConflict()
	}
	if err != nil {
		return nil, nil, SafeAPIError(err)
	}
	if cr == nil || cr.Name != source.RunningRevision || cr.Namespace != ir.Namespace || !SafeScalar(string(cr.UID)) || !SafeScalar(cr.ResourceVersion) || cr.DeletionTimestamp != nil || len(cr.OwnerReferences) > 16 || !exactMigrationOwner(cr.OwnerReferences, "InferenceReplica", ir.Name, ir.UID) || cr.Labels[constants.InferenceServicePodLabelKey] != parent.Name || cr.Labels[constants.OMEComponentLabel] != string(ir.Spec.Component) || cr.Labels[query.LabelManagedBy] != query.ManagedByOMENative {
		return nil, nil, migrationConflict()
	}
	if !boundedPrivatePayload(cr) {
		return nil, nil, ErrBounds
	}
	return list.Items, cr, nil
}

func inspectMigrationPods(parent *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, source *v1beta1.OMENativeInstanceStatus, pods []corev1.Pod) ([]string, error) {
	expected := map[string]bool{}
	multi := len(ir.Spec.Runners) == 2
	if !multi && (source.ActiveOrdinal < 0 || source.ActiveOrdinal > 1) {
		return nil, migrationConflict()
	}
	for _, r := range ir.Spec.Runners {
		for ordinal := int32(0); ordinal < r.Size; ordinal++ {
			if !multi {
				ordinal = source.ActiveOrdinal
			}
			expected[string(r.Name)+"/"+strconv.FormatInt(int64(ordinal), 10)] = true
			if !multi {
				break
			}
		}
	}
	nodes := map[string]bool{}
	names := map[string]bool{}
	uids := map[types.UID]bool{}
	for i := range pods {
		p := &pods[i]
		inc, incOK := query.InstanceIncarnationFromLabels(p)
		ordinal, ordOK := query.PodOrdinalFromLabels(p)
		if !SafeScalar(p.Name) || !SafeScalar(string(p.UID)) || !SafeScalar(p.ResourceVersion) || p.Namespace != parent.Namespace || len(p.OwnerReferences) > 16 || !exactMigrationOwner(p.OwnerReferences, "InferenceReplica", ir.Name, ir.UID) || p.Labels[constants.InferenceServicePodLabelKey] != parent.Name || p.Labels[constants.OMEComponentLabel] != string(ir.Spec.Component) || p.Labels[query.LabelManagedBy] != query.ManagedByOMENative || p.Labels[query.LabelInstanceIdx] != strconv.FormatInt(int64(source.Index), 10) || !incOK || !ordOK || p.Labels[query.LabelPodOrdinal] != strconv.FormatInt(int64(ordinal), 10) || p.Labels[query.LabelInstanceIncarnation] != strconv.FormatInt(inc, 10) || p.Name != query.PodName(parent.Name, workload.ComponentType(ir.Spec.Component), source.Index, p.Labels[query.LabelRunner], ordinal) {
			return nil, migrationConflict()
		}
		if p.DeletionTimestamp != nil {
			continue
		}
		key := p.Labels[query.LabelRunner] + "/" + strconv.FormatInt(int64(ordinal), 10)
		if inc != source.Incarnation || p.Labels[query.LabelRevisionHash] != strings.TrimPrefix(source.RunningRevision, ir.Name+"-") || !expected[key] || names[p.Name] || uids[p.UID] || !validMigrationNode(p.Spec.NodeName) {
			return nil, migrationConflict()
		}
		delete(expected, key)
		names[p.Name] = true
		uids[p.UID] = true
		nodes[p.Spec.NodeName] = true
	}
	if len(expected) != 0 || len(nodes) == 0 {
		return nil, migrationConflict()
	}
	result := make([]string, 0, len(nodes))
	for node := range nodes {
		result = append(result, node)
	}
	sort.Strings(result)
	return result, nil
}

func migrationSourceFingerprint(ir *v1beta1.InferenceReplica, pods []corev1.Pod, cr *appsv1.ControllerRevision) string {
	values := []string{string(ir.UID), ir.ResourceVersion, string(cr.UID), cr.ResourceVersion}
	for _, p := range pods {
		values = append(values, p.Name+"/"+string(p.UID)+"/"+p.ResourceVersion)
	}
	sort.Strings(values)
	return strings.Join(values, "\n")
}

// RecheckMigration rejects changed source evidence without rebuilding a plan,
// regenerating an ID, re-prompting or replaying any request.
func RecheckMigration(ctx context.Context, ome omeclient.OmeV1beta1Interface, kube kubernetes.Interface, parent *v1beta1.InferenceService, p MigrationPlan, clock reportv1alpha1.Clock) error {
	if p.existing {
		return nil
	}
	if p.evidence.ir == nil || !p.evidence.complete {
		return migrationConflict()
	}
	readCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	ir, err := ome.InferenceReplicas(parent.Namespace).Get(readCtx, p.evidence.ir.Name, metav1.GetOptions{})
	readErr := readCtx.Err()
	cancel()
	if readErr != nil {
		return readErr
	}
	if apierrors.IsNotFound(err) {
		return migrationConflict()
	}
	if err != nil {
		return SafeAPIError(err)
	}
	if clock == nil {
		clock = reportv1alpha1.SystemClock{}
	}
	if _, err := inspectReplica(ir, parent, []string{string(p.request.Component)}, clock.Now()); err != nil {
		if errors.Is(err, ErrBounds) {
			return err
		}
		return migrationConflict()
	}
	if ir.UID != p.evidence.ir.UID || ir.ResourceVersion != p.evidence.ir.ResourceVersion {
		return migrationConflict()
	}
	pods, cr, err := collectMigrationSource(ctx, kube, parent, ir, p.evidence.source)
	if err != nil {
		return err
	}
	if _, err := inspectMigrationPods(parent, ir, p.evidence.source, pods); err != nil {
		return err
	}
	if migrationSourceFingerprint(ir, pods, cr) != p.evidence.fingerprint {
		return migrationConflict()
	}
	return ctx.Err()
}
