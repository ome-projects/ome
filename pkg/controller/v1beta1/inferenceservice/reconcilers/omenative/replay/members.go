package replay

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferencereplica"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	isvcstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/status"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadstatus "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// containerName and servingPort are the rendered template's single
// container and its serving port; both are fixture identities, like the
// service name.
const (
	containerName = "main"
	servingPort   = 8000
	defaultRunner = "default"
	// heldRecordReason is the last failure a member's Held retry record
	// names; the stand-in knows no cause of its own.
	heldRecordReason = "the replacement never served"
)

// member is the InferenceReplica controller's stand-in for one Component:
// the revision pair, the per-Instance rows and the pods the scenario sets,
// published onto the replica's status before every pass as the controller
// would publish them.
type member struct {
	component v1beta1.ComponentType
	// image is the spec's template image, the revision the projection
	// targets; current and target are the replica's revision pair, as
	// images.
	image   string
	current string
	target  string
	rows    []rowState
	pods    []podState
	// pendingTarget is an image the projection does not carry yet: the one
	// a runtime edit renders for the member, or a push of the service's own
	// image staged while the recorded runtime revision has drifted. The
	// member records it as its update revision on the first publication
	// after the projection carries it, as the replica controller mints a
	// revision from the spec it is given and not before.
	pendingTarget string
	// stale publishes the next status as one written before the spec in
	// hand: an ObservedGeneration behind the replica's generation and no
	// template hash acknowledged, then clears.
	stale bool
	// keepStatus skips the next publish except for a promotion the stored
	// rows earned, so the status the replica carries stands as the last one
	// written, then clears.
	keepStatus bool
	// missing marks a replica the scenario removed: the member publishes
	// nothing and syncs no pod until the projector has created the object
	// again.
	missing bool
	// heldAttempts is the failed attempts the member's retry record for its
	// target counts while that record is Held; zero when none is.
	heldAttempts int32
	// failedAttempts is the failed attempts of the member's push that its
	// retry schedule counted: the record for the target reads Backoff after
	// them until it is Held, released, or the push moves on. A failure the
	// environment caused is not among them.
	failedAttempts int32
	// promoteStaged is set by an event that stands for the replica
	// controller's promotion write (member.converged, ir.converged,
	// replica.currentPromoted, ir.straggling): a promotion the rows have
	// earned lands in this publication. Every publication clears it.
	promoteStaged bool
	// promotePending records a promotion the rows earned at the previous
	// publication with no event standing for its write: the pass read
	// between the row write and the promotion, as it can once. The
	// promotion lands in the next publication whatever the scenario stages.
	promotePending bool
	// published is set by the member's first publication; before it the
	// scenario's current revision stands in for the stored one.
	published bool
	// reprojected is set between the replica's deletion and the first
	// publication onto the replica the pass projected again: that
	// publication seeds the rows Pending on the roll target, as the replica
	// controller's first pass creates them, and clears it.
	reprojected bool
	// revisions maps an image to the ControllerRevision its template minted.
	revisions map[string]*appsv1.ControllerRevision
	// readyAt is when the replica's Ready condition last flipped.
	readyAt     time.Time
	readyStatus metav1.ConditionStatus
}

// rowState is one per-Instance row as the scenario holds it.
type rowState struct {
	index      int32
	phase      v1beta1.OMENativeInstancePhase
	running    string
	target     string
	readySince time.Time
	// served records that a Ready pod backed the row on the revision it
	// runs. A settled row whose pods are all unready publishes Restarting
	// once it has served and Pending before: the stand-in's picture of a
	// row a scenario promoted before its pod was Ready, which the engine
	// would carry as an open attempt.
	served bool
	// replacement is the index a gang surge builds this row's replacement
	// at while the pair stands; replaces is the source's index on that
	// replacement row. Both are nil for a single-pod surge, which adds its
	// new pod to the row itself.
	replacement *int32
	replaces    *int32
	// failureReason is the reason the row's last failure recorded, published
	// on the row; empty when no failure named one.
	failureReason string
	// waiting is the token the row's open attempt waits on in the cluster,
	// published on its operation; cleared when a pod of the attempt is
	// Ready or the attempt ends.
	waiting string
}

// podState is one pod of a member.
type podState struct {
	index   int32
	ordinal int32
	image   string
	ready   bool
	serving bool
	// crashedAt is when the runner's restarted run died again, from when
	// the pod reports a crash loop; zero for a runner on its first run.
	crashedAt time.Time
	// readyAt is when the pod last turned Ready; zero while it is not.
	readyAt time.Time
	// listedAfter is how long after readyAt the endpoints controller lists
	// the pod in its revision's routed Service; zero lists it with the pod.
	listedAfter time.Duration
}

// listed reports whether the endpoints controller publishes the pod as a
// Ready endpoint of its revision's routed Service at now: the pod is Ready
// and serving, and the controller's delay has passed.
func (p podState) listed(now time.Time) bool {
	return p.ready && p.serving && !now.Before(p.readyAt.Add(p.listedAfter))
}

// creatingRow is the row the replica controller writes for an Instance it
// has no row and no pod for: a Create attempt toward image, which names the
// revision on the attempt and leaves the running revision to the promote.
func creatingRow(index int32, image string) rowState {
	return rowState{index: index, phase: v1beta1.OMENativeInstanceCreating, target: image}
}

// createAttempt reports whether the row is an open Create attempt: Creating
// toward a revision with no running revision yet, and not the replacement
// row of a gang surge.
func (r rowState) createAttempt() bool {
	return r.phase == v1beta1.OMENativeInstanceCreating && r.running == "" && r.target != "" && r.replaces == nil
}

// attemptImage is the revision the row's pods render: the running revision,
// else the revision a Create attempt brings the Instance up on.
func (r rowState) attemptImage() string {
	if r.running != "" {
		return r.running
	}
	return r.target
}

func (d *driver) member(name string) (*member, error) {
	m, ok := d.members[v1beta1.ComponentType(name)]
	if !ok {
		return nil, fmt.Errorf("replay: component %q is not declared by the service", name)
	}
	return m, nil
}

// selectorLabels are the labels every object of a Component carries and
// the engines select on.
func (d *driver) selectorLabels(c v1beta1.ComponentType) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: d.opts.ServiceName,
		constants.OMEComponentLabel:           string(c),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
}

// podTemplate renders the single-container template for an image.
func (d *driver) podTemplate(image string) *corev1.PodSpec {
	return &corev1.PodSpec{
		Containers: []corev1.Container{{
			Name:  containerName,
			Image: image,
			Ports: []corev1.ContainerPort{{Name: "http", ContainerPort: servingPort, Protocol: corev1.ProtocolTCP}},
		}},
	}
}

// runnerPorts is the serving port set the per-revision routing Services
// publish, per Component.
func (d *driver) runnerPorts() map[v1beta1.ComponentType][]corev1.ContainerPort {
	out := map[v1beta1.ComponentType][]corev1.ContainerPort{}
	for c := range d.members {
		out[c] = d.podTemplate("").Containers[0].Ports
	}
	return out
}

// replica reads a Component's live InferenceReplica, the owner every
// revision and pod of the Component is stamped with.
func (d *driver) replica(ctx context.Context, c v1beta1.ComponentType) (*v1beta1.InferenceReplica, error) {
	ir := &v1beta1.InferenceReplica{}
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: irprojector.InferenceReplicaName(d.opts.ServiceName, c)}
	if err := d.cluster.cli.Get(ctx, key, ir); err != nil {
		return nil, fmt.Errorf("replay: read replica %s: %w", key.Name, err)
	}
	return ir, nil
}

// retarget records image as the member's update revision. A retry record
// belongs to one revision, so the failed attempts counted toward the
// previous target do not carry over to the new one.
func (m *member) retarget(image string) {
	m.target = image
	m.failedAttempts = 0
}

// promotedOn is the retry record's prune at an Instance's promotion onto
// image, as the replica controller prunes the record of the revision an
// Instance converged on: the failed attempts counted toward the target
// are dropped. A Held record stays until a release or another target.
func (m *member) promotedOn(image string) {
	if image == m.target {
		m.failedAttempts = 0
	}
}

// resolveImage maps a scenario revision name to an image: "current" is the
// Component's own image, an image reference is itself.
func (m *member) resolveImage(name string) (string, error) {
	switch {
	case name == "" || name == "current":
		return m.image, nil
	case containsAny(name, "/:"):
		return name, nil
	}
	return "", fmt.Errorf("replay: %s: revision %q: name a revision as \"current\" or as the image whose template minted it", m.component, name)
}

func containsAny(s, chars string) bool {
	for _, c := range chars {
		for _, r := range s {
			if r == c {
				return true
			}
		}
	}
	return false
}

// ensureRevision mints, or reuses, the ControllerRevision an image's
// template hashes to, controlled by the Component's replica, owned by the
// service and scoped to the service's UID, as the replica controller mints
// it.
func (d *driver) ensureRevision(ctx context.Context, m *member, image string) (*appsv1.ControllerRevision, error) {
	if cr, ok := m.revisions[image]; ok {
		if err := d.cluster.cli.Get(ctx, client.ObjectKeyFromObject(cr), &appsv1.ControllerRevision{}); err == nil {
			return cr, nil
		}
	}
	ir, err := d.replica(ctx, m.component)
	if err != nil {
		return nil, err
	}
	scope := revision.ScopeOwner(ir, v1beta1.SchemeGroupVersion.WithKind("InferenceService").GroupKind())
	key := revision.Key{
		Namespace: d.opts.Namespace,
		Name:      irprojector.InferenceReplicaName(d.opts.ServiceName, m.component),
		Labels:    d.selectorLabels(m.component),
	}
	hash, raw, err := revision.HashTemplate(revision.Template{PodSpec: d.podTemplate(image), PairingProtocol: d.protocolOf(image)}, nil, d.isvcUID())
	if err != nil {
		return nil, fmt.Errorf("replay: hash revision for %s %s: %w", m.component, image, err)
	}
	var cr *appsv1.ControllerRevision
	err = d.cluster.staging(func() error {
		var collision bool
		var err error
		cr, collision, err = revision.EnsureControllerRevisionFromHash(ctx, d.cluster.cli, d.cluster.cli, ir,
			v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"), key, hash, raw, scope)
		if err != nil {
			return err
		}
		if collision {
			return fmt.Errorf("revision hash collision on image %q", image)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("replay: ensure revision for %s %s: %w", m.component, image, err)
	}
	m.revisions[image] = cr
	return cr, nil
}

// protocolOf is the pairing protocol the revision minted from an image
// carries: the one the scenario declares for it, else none.
func (d *driver) protocolOf(image string) string {
	return d.service.RevisionProtocols[image]
}

// revisionName is the ControllerRevision name an image's template hashes
// to, "" for no image.
func (d *driver) revisionName(ctx context.Context, m *member, image string) (string, error) {
	if image == "" {
		return "", nil
	}
	cr, err := d.ensureRevision(ctx, m, image)
	if err != nil {
		return "", err
	}
	return cr.Name, nil
}

// rollTargetImage is the image of the revision the replica controller rolls
// a member's Instances onto: the rollback target when the replica carries
// one that is retained for it (the replica controls it, or the service
// owns it), else the spec target.
func (d *driver) rollTargetImage(ctx context.Context, m *member) (string, error) {
	ir, err := d.replica(ctx, m.component)
	if err != nil {
		return "", err
	}
	return d.rollTargetImageFrom(ctx, m, ir, m.target)
}

// rollTargetImageFrom is rollTargetImage over a replica already read and
// the spec target the caller resolved.
func (d *driver) rollTargetImageFrom(ctx context.Context, m *member, ir *v1beta1.InferenceReplica, specTarget string) (string, error) {
	if ir.Spec.Pacing == nil || ir.Spec.Pacing.RollbackToRevision == nil || *ir.Spec.Pacing.RollbackToRevision == "" {
		return specTarget, nil
	}
	name := *ir.Spec.Pacing.RollbackToRevision
	for image, cr := range m.revisions {
		if cr.Name != name {
			continue
		}
		live := &appsv1.ControllerRevision{}
		if err := d.cluster.cli.Get(ctx, client.ObjectKeyFromObject(cr), live); err != nil {
			if apierrors.IsNotFound(err) {
				return specTarget, nil
			}
			return "", fmt.Errorf("replay: read rollback revision %s: %w", name, err)
		}
		if revision.RetainedBy(live, ir.UID, irprojector.HistoryScope(ir)) {
			return image, nil
		}
	}
	return specTarget, nil
}

// stepStableImage is the image of the revision a canary step keeps a
// member's stable side on while the pacing partition the replica carries
// holds Instances: the stored current revision, when the roll target is
// another one. "" when no pacing partition holds (the user's rollingUpdate
// partition is no step), when the replica has promoted no revision, or
// when the roll returns to the current revision.
func (d *driver) stepStableImage(m *member, ir *v1beta1.InferenceReplica, target string) string {
	current := ir.Status.CurrentRevision
	pacing := ir.Spec.Pacing
	if pacing == nil || pacing.Partition == nil || *pacing.Partition <= 0 || current == "" || target == "" {
		return ""
	}
	if cr, ok := m.revisions[target]; ok && cr.Name == current {
		return ""
	}
	for image, cr := range m.revisions {
		if cr.Name == current {
			return image
		}
	}
	return ""
}

// scaleUpImage is the image the replica controller's create pass builds an
// Instance on that has run no revision and has no pod: the step's stable
// revision while a canary step holds the member at a partition, else the
// roll target.
func (d *driver) scaleUpImage(ctx context.Context, m *member) (string, error) {
	target, err := d.rollTargetImage(ctx, m)
	if err != nil {
		return "", err
	}
	ir, err := d.replica(ctx, m.component)
	if err != nil {
		return "", err
	}
	if stable := d.stepStableImage(m, ir, target); stable != "" {
		return stable, nil
	}
	return target, nil
}

// revisionHash is the hash of an image's revision, "" for no image.
func (d *driver) revisionHash(ctx context.Context, m *member, image string) (string, error) {
	name, err := d.revisionName(ctx, m, image)
	if err != nil {
		return "", err
	}
	return query.RevisionHashFromControllerRevisionName(name), nil
}

// workloadKey is the Component's identity as the workload renderer reads it.
func (d *driver) workloadKey(c v1beta1.ComponentType) types.Key {
	return types.Key{
		Namespace: d.opts.Namespace,
		Component: v1beta1convert.ComponentTypeToWorkload(c),
		OwnerName: d.opts.ServiceName,
	}
}

func (d *driver) podName(c v1beta1.ComponentType, p podState) string {
	return query.PodName(d.opts.ServiceName, v1beta1convert.ComponentTypeToWorkload(c), p.index, defaultRunner, p.ordinal)
}

// renderPod renders one pod of a member through the production renderer,
// so its names, labels and gates are the ones the engine writes.
func (d *driver) renderPod(ctx context.Context, m *member, p podState) (*corev1.Pod, error) {
	ir, err := d.replica(ctx, m.component)
	if err != nil {
		return nil, err
	}
	hash, err := d.revisionHash(ctx, m, p.image)
	if err != nil {
		return nil, err
	}
	plan := types.ComponentPlan{Component: v1beta1convert.ComponentTypeToWorkload(m.component), Replicas: int32(len(m.rows)), PairingProtocol: d.protocolOf(p.image)}
	inst := types.InstancePlan{Index: p.index, Incarnation: 1, Runners: []types.RunnerPlan{{Name: defaultRunner, Size: 1}}}
	runner := types.RunnerPlan{Name: defaultRunner, Size: 1}
	pod, err := workloadops.RenderWithRevision(ir, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"), d.workloadKey(m.component),
		d.podTemplate(p.image), nil, plan, inst, runner, p.ordinal, hash, nil)
	if err != nil {
		return nil, fmt.Errorf("replay: render pod: %w", err)
	}
	applyPodStatus(pod, p, d.clock.Now())
	return pod, nil
}

// applyPodStatus writes the kubelet's view of a pod: Running, and Ready
// with the serving gate when the scenario says so. The runner's container
// status carries the name the engine reads the runner under
// (constants.MainContainerName); the template's own container name is a
// fixture identity hashed into every revision and stays as it is. A pod
// whose runner crashed reports the kubelet's back-off, one restart and the
// restarted run's death, which is how the crash reading dates the loop.
func applyPodStatus(pod *corev1.Pod, p podState, now time.Time) {
	pod.Status.Phase = corev1.PodRunning
	ready := corev1.ConditionFalse
	if p.ready {
		ready = corev1.ConditionTrue
	}
	serving := corev1.ConditionFalse
	if p.serving {
		serving = corev1.ConditionTrue
	}
	at := metav1.NewTime(now)
	pod.Status.Conditions = []corev1.PodCondition{
		{Type: corev1.ContainersReady, Status: ready, LastTransitionTime: at},
		{Type: corev1.PodReady, Status: ready, LastTransitionTime: at},
		{Type: query.ServingConditionType, Status: serving, LastTransitionTime: at},
	}
	runner := corev1.ContainerStatus{
		Name:  constants.MainContainerName,
		Image: pod.Spec.Containers[0].Image,
		Ready: p.ready,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: at}},
	}
	if !p.crashedAt.IsZero() {
		died := metav1.NewTime(p.crashedAt)
		runner.RestartCount = 1
		runner.State = corev1.ContainerState{Waiting: &corev1.ContainerStateWaiting{Reason: constants.StateReasonCrashLoopBackOff}}
		runner.LastTerminationState = corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{
			ExitCode: 1, Reason: constants.StateReasonError, StartedAt: died, FinishedAt: died,
		}}
	}
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{runner}
}

// syncPods makes the store's pods of a member match the scenario's: a pod
// the scenario names exists with the status it declares, and no other pod
// of the member remains. The Ready clock of each pod is kept here, then
// the endpoints the controller would publish for them.
func (d *driver) syncPods(ctx context.Context, m *member) error {
	now := d.clock.Now()
	for i := range m.pods {
		switch {
		case !m.pods[i].ready:
			m.pods[i].readyAt = time.Time{}
		case m.pods[i].readyAt.IsZero():
			m.pods[i].readyAt = now
		}
	}
	if err := d.syncEndpoints(ctx, m); err != nil {
		return err
	}
	want := map[string]podState{}
	for _, p := range m.pods {
		want[d.podName(m.component, p)] = p
	}
	live, err := d.memberPods(ctx, m)
	if err != nil {
		return err
	}
	return d.cluster.staging(func() error {
		for i := range live.Items {
			pod := &live.Items[i]
			p, keep := want[pod.Name]
			if !keep {
				if err := d.cluster.cli.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("replay: delete pod %s: %w", pod.Name, err)
				}
				continue
			}
			rendered, err := d.renderPod(ctx, m, p)
			if err != nil {
				return err
			}
			if pod.Labels[query.LabelRevisionHash] != rendered.Labels[query.LabelRevisionHash] {
				// A pod never changes revision in place; the scenario's new
				// revision is a new pod under the same name.
				if err := d.cluster.cli.Delete(ctx, pod); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("replay: delete pod %s: %w", pod.Name, err)
				}
				if err := d.cluster.cli.Create(ctx, rendered); err != nil {
					return fmt.Errorf("replay: recreate pod %s: %w", pod.Name, err)
				}
				delete(want, pod.Name)
				continue
			}
			if !equality.Semantic.DeepEqual(pod.Status.Conditions, rendered.Status.Conditions) ||
				!equality.Semantic.DeepEqual(pod.Status.ContainerStatuses, rendered.Status.ContainerStatuses) {
				pod.Status = rendered.Status
				if err := d.cluster.cli.Status().Update(ctx, pod); err != nil {
					return fmt.Errorf("replay: update pod status %s: %w", pod.Name, err)
				}
			}
			delete(want, pod.Name)
		}
		names := make([]string, 0, len(want))
		for name := range want {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			rendered, err := d.renderPod(ctx, m, want[name])
			if err != nil {
				return err
			}
			if err := d.cluster.cli.Create(ctx, rendered); err != nil {
				return fmt.Errorf("replay: create pod %s: %w", name, err)
			}
		}
		return nil
	})
}

// sliceNameFor is the one EndpointSlice the stand-in keeps per routed
// Service; the engine reads endpoints, not slice topology.
func sliceNameFor(serviceName string) string {
	return serviceName + "-replay"
}

// syncEndpoints is the endpoints controller's stand-in for one member: for
// every revision with a pod it keeps the EndpointSlice of the revision's
// routed Service, listing the pods that are listed at the clock as Ready
// endpoints; a revision with no pod left loses its slice, as the Service
// it belongs to is collected.
func (d *driver) syncEndpoints(ctx context.Context, m *member) error {
	component := v1beta1convert.ComponentTypeToWorkload(m.component)
	byService := map[string][]discoveryv1.Endpoint{}
	now := d.clock.Now()
	for _, p := range m.pods {
		hash, err := d.revisionHash(ctx, m, p.image)
		if err != nil {
			return err
		}
		service := query.PerRevisionServiceName(d.opts.ServiceName, component, hash)
		endpoints := byService[service]
		if p.listed(now) {
			ready := true
			endpoints = append(endpoints, discoveryv1.Endpoint{
				Addresses:  []string{fmt.Sprintf("10.%d.%d.%d", len(byService), p.index%256, p.ordinal+1)},
				Conditions: discoveryv1.EndpointConditions{Ready: &ready},
				TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: d.opts.Namespace, Name: d.podName(m.component, p)},
			})
		}
		byService[service] = endpoints
	}
	live := &discoveryv1.EndpointSliceList{}
	if err := d.cluster.cli.List(ctx, live, client.InNamespace(d.opts.Namespace), client.MatchingLabels(d.selectorLabels(m.component))); err != nil {
		return fmt.Errorf("replay: list endpoint slices of %s: %w", m.component, err)
	}
	return d.cluster.staging(func() error {
		kept := map[string]bool{}
		for i := range live.Items {
			slice := &live.Items[i]
			endpoints, ok := byService[slice.Labels[discoveryv1.LabelServiceName]]
			if !ok {
				if err := d.cluster.cli.Delete(ctx, slice); err != nil && !apierrors.IsNotFound(err) {
					return fmt.Errorf("replay: delete endpoint slice %s: %w", slice.Name, err)
				}
				continue
			}
			kept[slice.Labels[discoveryv1.LabelServiceName]] = true
			if equality.Semantic.DeepEqual(slice.Endpoints, endpoints) {
				continue
			}
			slice.Endpoints = endpoints
			if err := d.cluster.cli.Update(ctx, slice); err != nil {
				return fmt.Errorf("replay: update endpoint slice %s: %w", slice.Name, err)
			}
		}
		services := make([]string, 0, len(byService))
		for service := range byService {
			if !kept[service] {
				services = append(services, service)
			}
		}
		sort.Strings(services)
		for _, service := range services {
			labels := d.selectorLabels(m.component)
			labels[discoveryv1.LabelServiceName] = service
			slice := &discoveryv1.EndpointSlice{
				ObjectMeta:  metav1.ObjectMeta{Namespace: d.opts.Namespace, Name: sliceNameFor(service), Labels: labels},
				AddressType: discoveryv1.AddressTypeIPv4,
				Endpoints:   byService[service],
			}
			if err := d.cluster.cli.Create(ctx, slice); err != nil {
				return fmt.Errorf("replay: create endpoint slice %s: %w", slice.Name, err)
			}
		}
		return nil
	})
}

// publishedOperation is the attempt a row publishes while a surge is in
// flight, nil otherwise: a gang surge's source names the row its replacement
// gang is built at, as the engine writes the pair; the replacement gang's
// own row stands at the GangSurgeTarget step until the old gang leaves and
// it is promoted in the source's place; a single-pod surge keeps the attempt
// on its own row while the replacement stands beside the old pod.
func (m *member) publishedOperation(r rowState, phase v1beta1.OMENativeInstancePhase, running, rowTarget string) *v1beta1.InstanceOperation {
	switch {
	case r.replacement != nil:
		surgeIndex := *r.replacement
		return &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: types.UpdateStepSurge, TargetRevision: rowTarget, SurgeIndex: &surgeIndex}
	case r.replaces != nil:
		return &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: types.UpdateStepGangSurgeTarget, TargetRevision: rowTarget}
	case r.createAttempt():
		return &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationCreate, Step: workloadstatus.CreateStepCreatePods, TargetRevision: rowTarget}
	case phase == v1beta1.OMENativeInstanceUpdating && rowTarget != "" && rowTarget != running && m.podOn(r.index, r.target) != nil:
		return &v1beta1.InstanceOperation{Type: v1beta1.InstanceOperationUpdate, Step: types.UpdateStepSurge, TargetRevision: rowTarget}
	}
	return nil
}

// instancePods counts a row's pods: all, Ready, and serving.
func (m *member) instancePods(index int32) (total, ready, serving int32) {
	for _, p := range m.pods {
		if p.index != index {
			continue
		}
		total++
		if p.ready {
			ready++
		}
		if p.ready && p.serving {
			serving++
		}
	}
	return total, ready, serving
}

// publish writes the member's status onto its InferenceReplica, as the
// replica controller publishes it: the rows and the counters derived from
// them and their pods, the update revision, the current revision the
// engine's own rule decides over those rows and pods, the Ready and
// CurrentRevisionWithdrawn conditions and the observed generation.
func (d *driver) publish(ctx context.Context, m *member) error {
	ir, err := d.replica(ctx, m.component)
	if err != nil {
		if m.reprojected && apierrors.IsNotFound(err) {
			// Deleted by the scenario; the pass projects it again.
			return nil
		}
		return err
	}
	if m.keepStatus {
		m.keepStatus = false
		return d.landPendingPromotion(ctx, m, ir)
	}
	if m.reprojected {
		if err := d.seedReprojected(ctx, m, ir); err != nil {
			return err
		}
	}
	if m.pendingTarget != "" && projectedImage(ir) == m.pendingTarget {
		m.retarget(m.pendingTarget)
		m.pendingTarget = ""
	}
	rollTarget, err := d.rollTargetImage(ctx, m)
	if err != nil {
		return err
	}
	m.retargetCreateAttempts(rollTarget, d.stepStableImage(m, ir, rollTarget))
	m.settleCreateAttempts(d.clock.Now())
	m.markServed()
	target, err := d.revisionName(ctx, m, m.target)
	if err != nil {
		return err
	}
	rows, counters, err := d.publishedRows(ctx, m, target)
	if err != nil {
		return err
	}
	pair, err := d.revisionPairToPublish(ctx, m, ir, rows, target)
	if err != nil {
		return err
	}
	status := v1beta1.InferenceReplicaStatus{
		CurrentRevision:      pair.current,
		UpdateRevision:       target,
		LabelSelector:        metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: d.selectorLabels(m.component)}),
		Replicas:             int32(len(rows)),
		ReadyReplicas:        counters.ready,
		AvailableReplicas:    counters.ready,
		ServingReplicas:      counters.serving,
		UpdatedReplicas:      counters.updated,
		UpdatedReadyReplicas: counters.updatedReady,
		InstanceStatuses:     rows,
		RetryBlocks:          m.heldRecords(target),
		ObservedGeneration:   ir.Generation,
		ObservedTemplateHash: ir.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey],
	}
	if m.stale {
		status.ObservedGeneration = ir.Generation - 1
		status.ObservedTemplateHash = ""
		m.stale = false
	}
	status.Conditions = d.publishedConditions(m, ir, &status, rows, pair)
	if equality.Semantic.DeepEqual(ir.Status, status) {
		return nil
	}
	ir.Status = status
	if err := d.writeStandInStatus(ctx, ir); err != nil {
		return fmt.Errorf("replay: publish replica %s: %w", ir.Name, err)
	}
	d.trace.line("publish component=%s current=%s update=%s observedGeneration=%d replicas=%d ready=%d serving=%d updated=%d updatedReady=%d rows=%s",
		m.component, orNil(pair.current), orNil(target), status.ObservedGeneration, status.Replicas, status.ReadyReplicas,
		status.ServingReplicas, status.UpdatedReplicas, status.UpdatedReadyReplicas, renderRows(rows))
	return nil
}

// landPendingPromotion is the tail of the pass that published the rows,
// landing while the replica controller has not run since (ir.stale with
// lag status): a promotion the stored rows earned at that publication is
// written onto the stored status, pair and condition alone, and nothing
// else moves.
func (d *driver) landPendingPromotion(ctx context.Context, m *member, ir *v1beta1.InferenceReplica) error {
	if !m.promotePending {
		return nil
	}
	m.promotePending = false
	target := ir.Status.UpdateRevision
	pair := publishedPair{recorded: ir.Status.CurrentRevision, clearedFrom: clearedFromRevisionOf(&ir.Status), current: target}
	status := ir.Status
	status.CurrentRevision = target
	status.Conditions = d.publishedConditions(m, ir, &status, status.InstanceStatuses, pair)
	ir.Status = status
	if err := d.writeStandInStatus(ctx, ir); err != nil {
		return fmt.Errorf("replay: land the promotion of %s: %w", ir.Name, err)
	}
	d.trace.line("publish component=%s current=%s update=%s promotion=landed", m.component, orNil(pair.current), orNil(target))
	return nil
}

// writeStandInStatus is the stand-in's one raw status write: every status
// the driver publishes for a replica, the pass's publication and the
// promotion landed after it, goes through here, as the replica controller's
// own writes go through its single writer.
func (d *driver) writeStandInStatus(ctx context.Context, ir *v1beta1.InferenceReplica) error {
	return d.cluster.staging(func() error { return d.cluster.cli.Status().Update(ctx, ir) })
}

// retargetCreateAttempts is the create pass's retirement of an attempt
// committed to a revision other than the roll target: an open Create
// attempt whose pods are not all Ready is replaced by a fresh attempt
// toward target and its pods are deleted; one whose pods are all Ready is
// left to promote on its revision, since retiring it would drop a set that
// serves, and one toward stable, the revision a canary step keeps the
// member's stable side on, is kept while the step holds. It reports how
// many attempts moved.
func (m *member) retargetCreateAttempts(target, stable string) int {
	moved := 0
	for i := range m.rows {
		row := &m.rows[i]
		if !row.createAttempt() || row.target == target || target == "" || (stable != "" && row.target == stable) {
			continue
		}
		if total, ready, _ := m.instancePods(row.index); total > 0 && ready == total {
			continue
		}
		row.target = target
		row.waiting = ""
		m.removePodsOf(row.index)
		moved++
	}
	return moved
}

// retargetUnservedUpdateAttempts is the pass's retirement of an update
// attempt toward a revision a push has left: an open attempt whose pods on
// its revision are not all Ready is turned toward target, and one turned
// back to the revision the Instance runs ends there, the row settled on it
// while its pod of the left revision stands until the scenario removes it.
// A served attempt keeps its revision and promotes on it. It reports how
// many attempts moved.
func (m *member) retargetUnservedUpdateAttempts(target string) int {
	moved := 0
	for i := range m.rows {
		row := &m.rows[i]
		if target == "" || row.target == "" || row.target == target || row.running == "" || row.replacement != nil || row.replaces != nil {
			continue
		}
		if total, ready := m.podsOn(row.index, row.target); total > 0 && ready == total {
			continue
		}
		row.target = target
		row.waiting = ""
		if row.target == row.running {
			row.target = ""
			row.phase = v1beta1.OMENativeInstanceReady
			row.served = true
		}
		moved++
	}
	return moved
}

// markServed records, on every settled row, a Ready pod on the revision it
// runs; the mark stays until the row is promoted onto another revision.
func (m *member) markServed() {
	for i := range m.rows {
		row := &m.rows[i]
		if row.target != "" || row.running == "" || row.served {
			continue
		}
		if _, ready := m.podsOn(row.index, row.running); ready > 0 {
			row.served = true
		}
	}
}

// podsOn counts an Instance's pods on one revision: all, and Ready.
func (m *member) podsOn(index int32, image string) (total, ready int32) {
	for _, p := range m.pods {
		if p.index != index || p.image != image {
			continue
		}
		total++
		if p.ready {
			ready++
		}
	}
	return total, ready
}

// removePodsOf drops every pod of an Instance.
func (m *member) removePodsOf(index int32) {
	kept := m.pods[:0]
	for _, p := range m.pods {
		if p.index != index {
			kept = append(kept, p)
		}
	}
	m.pods = kept
}

// settleCreateAttempts is the create pass's promote: an open Create attempt
// whose pods are all Ready is promoted onto the attempt's revision, the
// first moment the engine records a running revision for the row.
func (m *member) settleCreateAttempts(now time.Time) {
	for i := range m.rows {
		if !m.rows[i].createAttempt() {
			continue
		}
		total, ready, _ := m.instancePods(m.rows[i].index)
		if total == 0 || ready < total {
			continue
		}
		m.rows[i].running, m.rows[i].target = m.rows[i].target, ""
		m.rows[i].phase = v1beta1.OMENativeInstanceReady
		m.rows[i].readySince = now
		m.promotedOn(m.rows[i].running)
	}
}

// rowCounters are the Component counters a publication derives from the
// rows and their pods.
type rowCounters struct {
	ready, serving, updated, updatedReady int32
}

// publishedRows renders the rows as the replica controller stores them and
// counts the Component's Instances over them: Ready when every pod of the
// Instance is Ready, serving when every pod serves, and on the update
// revision by the engine's own reading of the row (status.RowOnRevision:
// running it, and not held to another revision by an open update attempt).
func (d *driver) publishedRows(ctx context.Context, m *member, target string) ([]v1beta1.OMENativeInstanceStatus, rowCounters, error) {
	var counters rowCounters
	targetID := query.RevisionFromName(target)
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, len(m.rows))
	for _, r := range m.rows {
		row, err := d.publishedRow(ctx, m, r)
		if err != nil {
			return nil, rowCounters{}, err
		}
		rows = append(rows, row)
		instanceReady := row.PodCount > 0 && row.AvailablePodCount == row.PodCount
		if instanceReady {
			counters.ready++
		}
		if row.PodCount > 0 && row.ServingPodCount == row.PodCount {
			counters.serving++
		}
		engineRow := v1beta1convert.InstanceStatusToWorkload(row)
		if target != "" && workloadstatus.RowOnRevision(&engineRow, targetID) {
			counters.updated++
			if instanceReady {
				counters.updatedReady++
			}
		}
	}
	return rows, counters, nil
}

// publishedRow is one row as the replica controller stores it: the
// revisions by name, the pod counts, the attempt it publishes, and the
// phase the publication writes over a settled Ready row from its pods.
func (d *driver) publishedRow(ctx context.Context, m *member, r rowState) (v1beta1.OMENativeInstanceStatus, error) {
	total, ready, serving := m.instancePods(r.index)
	running, err := d.revisionName(ctx, m, r.running)
	if err != nil {
		return v1beta1.OMENativeInstanceStatus{}, err
	}
	rowTarget, err := d.revisionName(ctx, m, r.target)
	if err != nil {
		return v1beta1.OMENativeInstanceStatus{}, err
	}
	operation := m.publishedOperation(r, r.phase, running, rowTarget)
	row := v1beta1.OMENativeInstanceStatus{
		Index:             r.index,
		Incarnation:       1,
		Phase:             publishedPhase(r.phase, operation, total, ready, serving, r.served),
		RunningRevision:   running,
		TargetRevision:    rowTarget,
		PodCount:          total,
		ServingPodCount:   serving,
		AvailablePodCount: ready,
		Operation:         operation,
	}
	if row.Operation != nil && r.waiting != "" {
		row.Operation.Waiting = r.waiting
	}
	if r.failureReason != "" {
		row.LastFailure = &v1beta1.InstanceTermination{Reason: r.failureReason}
	}
	if !r.readySince.IsZero() {
		at := metav1.NewTime(r.readySince)
		row.ReadySince = &at
	}
	return row, nil
}

// publishedPhase is the phase the publication writes over a settled Ready
// row (Ready with no attempt on it) from the pods of its own pass, as the
// controller's publication writes it: Pending when no pod backs the row,
// Restarting while the pods of a row that has served are not all serving,
// Ready otherwise. A row that never served reads Pending while none of its
// pods is Ready, the stand-in's shape for a row promoted before its pod
// was Ready. Every other row keeps the phase it holds.
func publishedPhase(phase v1beta1.OMENativeInstancePhase, operation *v1beta1.InstanceOperation, total, ready, serving int32, served bool) v1beta1.OMENativeInstancePhase {
	if phase != v1beta1.OMENativeInstanceReady || operation != nil {
		return phase
	}
	switch {
	case total == 0 || (ready == 0 && !served):
		return v1beta1.OMENativeInstancePending
	case serving < total:
		return v1beta1.OMENativeInstanceRestarting
	}
	return phase
}

// heldRecords is the member's retry record as the replica publishes it:
// one Held record for the target while the scenario holds it, else one
// Backoff record while failed attempts count toward the target, else none.
func (m *member) heldRecords(target string) []v1beta1.RetryBlock {
	switch {
	case target == "":
		return nil
	case m.heldAttempts > 0:
		return []v1beta1.RetryBlock{{TargetRevision: target, State: v1beta1.RetryBlockHeld, AttemptsStarted: m.heldAttempts, Reason: heldRecordReason}}
	case m.failedAttempts > 0:
		return []v1beta1.RetryBlock{{TargetRevision: target, State: v1beta1.RetryBlockBackoff, AttemptsStarted: m.failedAttempts, Reason: heldRecordReason}}
	}
	return nil
}

// retryBlocksToWorkload mirrors the published retry records into the
// engine's type, the fields the engine's revision rule reads.
func retryBlocksToWorkload(records []v1beta1.RetryBlock) []types.RetryBlock {
	out := make([]types.RetryBlock, 0, len(records))
	for _, r := range records {
		out = append(out, types.RetryBlock{TargetRevision: r.TargetRevision, State: types.RetryBlockState(r.State), AttemptsStarted: r.AttemptsStarted})
	}
	return out
}

// publishedPair is the current revision a publication writes, with the
// stored pair it was decided from.
type publishedPair struct {
	// recorded is the current revision the stored status named before this
	// publication; for a replica never published, the scenario's.
	recorded string
	// clearedFrom is the revision the stored status records the current
	// revision cleared from, "" when it records none.
	clearedFrom string
	current     string
}

// revisionPairToPublish decides the current revision as the replica
// controller does: status.CurrentRevisionFor over the rows, the revision
// their live pods carry, the retry record and the stored pair. The
// controller writes the pair at once when the update revision moves and
// otherwise after the pass that wrote the rows, so a service pass can read
// the rows once before the promotion they earn: a promotion lands in this
// publication when an event stands for its write (promoteStaged) and at the
// latest in the next one (promotePending); every other answer of the rule
// is published at once.
func (d *driver) revisionPairToPublish(ctx context.Context, m *member, ir *v1beta1.InferenceReplica, rows []v1beta1.OMENativeInstanceStatus, target string) (publishedPair, error) {
	pair := publishedPair{recorded: ir.Status.CurrentRevision, clearedFrom: clearedFromRevisionOf(&ir.Status)}
	if !m.published {
		// The member's first publication: the scenario's current revision
		// stands in for the stored one. An object the pass created anew
		// later, with an empty status, starts from no current revision as
		// the replica controller finds it.
		recorded, err := d.revisionName(ctx, m, m.current)
		if err != nil {
			return publishedPair{}, err
		}
		pair.recorded = recorded
	}
	m.published = true
	if m.promotePending {
		// The promotion the rows earned at the previous publication lands
		// first: it is the tail of the pass that wrote those rows, and the
		// pair this publication writes starts from it.
		pair.recorded = ir.Status.UpdateRevision
		pair.clearedFrom = ""
		m.promotePending = false
	}
	pods, err := d.podRevisions(ctx, m)
	if err != nil {
		return publishedPair{}, err
	}
	insts := v1beta1convert.InstanceStatusSliceToWorkload(rows)
	pair.current = workloadstatus.CurrentRevisionFor(insts, pair.recorded, target, retryBlocksToWorkload(m.heldRecords(target)), pair.clearedFrom, pods)
	pairWrite := ir.Status.UpdateRevision != target
	if pair.current == target && pair.recorded != target && !pairWrite && !m.promoteStaged {
		// The rows and the promotion are two writes of one pass, so a
		// service pass can read the rows once before the promotion.
		pair.current = pair.recorded
		m.promotePending = true
	}
	m.promoteStaged = false
	return pair, nil
}

// clearedFromRevisionOf names the revision a stored status records the
// current revision cleared from: the update revision while the current
// revision is empty and the CurrentRevisionWithdrawn condition is True,
// "" otherwise, as the replica controller reads it.
func clearedFromRevisionOf(st *v1beta1.InferenceReplicaStatus) string {
	if st.CurrentRevision != "" || !apimeta.IsStatusConditionTrue(st.Conditions, inferencereplica.InferenceReplicaConditionCurrentRevisionWithdrawn) {
		return ""
	}
	return st.UpdateRevision
}

// podRevisions reads the revision each Instance's live pods carry, off the
// Component's pods as the replica controller reads them before it decides
// the current revision.
func (d *driver) podRevisions(ctx context.Context, m *member) (workloadstatus.PodRevisions, error) {
	live, err := d.memberPods(ctx, m)
	if err != nil {
		return nil, err
	}
	pods := make([]*corev1.Pod, 0, len(live.Items))
	for i := range live.Items {
		pods = append(pods, &live.Items[i])
	}
	return workloadstatus.PodRevisionsByInstance(query.BucketPodsByInstanceIdx(pods)), nil
}

// memberPods lists a member's pods as the store holds them.
func (d *driver) memberPods(ctx context.Context, m *member) (*corev1.PodList, error) {
	live := &corev1.PodList{}
	if err := d.cluster.cli.List(ctx, live, client.InNamespace(d.opts.Namespace), client.MatchingLabels(d.selectorLabels(m.component))); err != nil {
		return nil, fmt.Errorf("replay: list pods of %s: %w", m.component, err)
	}
	return live, nil
}

// publishedConditions are the Ready condition the replica controller
// decides and, while the pair records an emptied current revision, the
// CurrentRevisionWithdrawn condition beside it, kept in step with the pair
// as the controller keeps it (status.WithdrawnRevisionAfter).
func (d *driver) publishedConditions(m *member, ir *v1beta1.InferenceReplica, status *v1beta1.InferenceReplicaStatus, rows []v1beta1.OMENativeInstanceStatus, pair publishedPair) []metav1.Condition {
	readyStatus, reason := standInReadyCondition(ir, status, rows)
	if m.readyStatus != readyStatus {
		m.readyStatus, m.readyAt = readyStatus, d.clock.Now()
	}
	conditions := []metav1.Condition{{
		Type:               "Ready",
		Status:             readyStatus,
		Reason:             reason,
		LastTransitionTime: metav1.NewTime(m.readyAt),
	}}
	if workloadstatus.WithdrawnRevisionAfter(pair.clearedFrom, pair.recorded, pair.current, status.UpdateRevision) == "" {
		return conditions
	}
	cleared := metav1.Condition{
		Type:               inferencereplica.InferenceReplicaConditionCurrentRevisionWithdrawn,
		Status:             metav1.ConditionTrue,
		Reason:             inferencereplica.ReasonInstanceOffRevision,
		LastTransitionTime: metav1.NewTime(d.clock.Now()),
	}
	if prior := apimeta.FindStatusCondition(ir.Status.Conditions, cleared.Type); prior != nil && prior.Status == cleared.Status {
		cleared.LastTransitionTime = prior.LastTransitionTime
	}
	return append(conditions, cleared)
}

// projectedImage is the image the replica's projected template renders,
// "" before the first projection.
func projectedImage(ir *v1beta1.InferenceReplica) string {
	if len(ir.Spec.Runners) == 0 || len(ir.Spec.Runners[0].Template.Spec.Containers) == 0 {
		return ""
	}
	return ir.Spec.Runners[0].Template.Spec.Containers[0].Image
}

// standInReadyCondition is the replica controller's Ready condition, decided
// in its order: a Failed row, the staged shape under a partition, a roll in
// flight, no Instances, every Instance Ready, then the serving minimum.
// The engine measures the Sequential wait from this condition's transition
// to True, so the stand-in publishes the transitions the replica would.
func standInReadyCondition(ir *v1beta1.InferenceReplica, status *v1beta1.InferenceReplicaStatus, rows []v1beta1.OMENativeInstanceStatus) (metav1.ConditionStatus, string) {
	for _, r := range rows {
		if r.Phase == v1beta1.OMENativeInstanceFailed {
			return metav1.ConditionFalse, "InstanceFailed"
		}
	}
	if partition := irprojector.IRPartition(ir); partition > 0 &&
		workloadstatus.ReachedDesiredShape(v1beta1convert.InstanceStatusSliceToWorkload(rows), status.UpdateRevision, partition, status.Replicas) {
		return metav1.ConditionTrue, "Staged"
	}
	switch {
	case status.UpdateRevision != "" && status.CurrentRevision != status.UpdateRevision:
		return metav1.ConditionUnknown, "RolloutInProgress"
	case status.Replicas == 0:
		return metav1.ConditionFalse, "NoReplicas"
	case status.ReadyReplicas == status.Replicas:
		return metav1.ConditionTrue, "AllInstancesReady"
	case status.ServingReplicas >= isvcstatus.InstanceAvailabilityFloor(status.Replicas, ir.Spec.Replicas, ir.Spec.Lifecycle):
		return metav1.ConditionTrue, "MinimumAvailable"
	}
	return metav1.ConditionFalse, "ReplicaCountMismatch"
}

// seedReprojected fills a member whose replica the pass projected again
// the way the replica controller's first pass leaves it: one Create attempt
// per replica toward the roll target, no pod yet, and no current revision.
func (d *driver) seedReprojected(ctx context.Context, m *member, ir *v1beta1.InferenceReplica) error {
	target, err := d.rollTargetImage(ctx, m)
	if err != nil {
		return err
	}
	replicas := int32(1)
	if ir.Spec.Replicas != nil {
		replicas = *ir.Spec.Replicas
	}
	m.rows, m.pods = nil, nil
	for i := int32(0); i < replicas; i++ {
		m.rows = append(m.rows, creatingRow(i, target))
	}
	m.reprojected = false
	return nil
}

// renderRows prints the dense rows as index:phase@hash.
func renderRows(rows []v1beta1.OMENativeInstanceStatus) string {
	parts := make([]string, 0, len(rows))
	for _, r := range rows {
		parts = append(parts, fmt.Sprintf("%d:%s@%s", r.Index, r.Phase, orNil(query.RevisionHashFromControllerRevisionName(r.RunningRevision))))
	}
	return "[" + joinFields(parts) + "]"
}

// sync publishes a member after the scenario touched it: its pods first,
// then its status. A member whose replica the scenario removed waits
// until the object exists again.
func (d *driver) sync(ctx context.Context, m *member) error {
	if m.missing {
		if _, err := d.replica(ctx, m.component); apierrors.IsNotFound(err) {
			return nil
		} else if err != nil {
			return err
		}
		m.missing = false
	}
	if err := d.syncPods(ctx, m); err != nil {
		return err
	}
	return d.publish(ctx, m)
}
