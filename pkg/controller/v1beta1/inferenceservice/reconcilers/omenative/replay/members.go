package replay

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// containerName and servingPort are the rendered template's single
// container and its serving port; both are fixture identities, like the
// service name.
const (
	containerName = "main"
	servingPort   = 8000
	defaultRunner = "default"
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
	// stale publishes the next status with an ObservedGeneration behind the
	// replica's generation, then clears.
	stale bool
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
}

// podState is one pod of a member.
type podState struct {
	index   int32
	ordinal int32
	image   string
	ready   bool
	serving bool
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
// template hashes to, controlled by the Component's replica and scoped to
// the service's UID, as the replica controller mints it.
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
	key := revision.Key{
		Namespace: d.opts.Namespace,
		Name:      irprojector.InferenceReplicaName(d.opts.ServiceName, m.component),
		Labels:    d.selectorLabels(m.component),
	}
	var cr *appsv1.ControllerRevision
	err = d.cluster.staging(func() error {
		var collision bool
		var err error
		cr, collision, err = revision.EnsureControllerRevision(ctx, d.cluster.cli, d.cluster.cli, ir,
			v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"), key, d.podTemplate(image), nil, nil, d.isvcUID())
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
		Namespace:      d.opts.Namespace,
		Component:      v1beta1convert.ComponentTypeToWorkload(c),
		OwnerName:      d.opts.ServiceName,
		SelectorLabels: d.selectorLabels(c),
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
	plan := types.ComponentPlan{Component: v1beta1convert.ComponentTypeToWorkload(m.component), Replicas: int32(len(m.rows))}
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
// with the serving gate when the scenario says so.
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
	pod.Status.ContainerStatuses = []corev1.ContainerStatus{{
		Name:  containerName,
		Image: pod.Spec.Containers[0].Image,
		Ready: p.ready,
		State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{StartedAt: at}},
	}}
}

// syncPods makes the store's pods of a member match the scenario's: a pod
// the scenario names exists with the status it declares, and no other pod
// of the member remains.
func (d *driver) syncPods(ctx context.Context, m *member) error {
	want := map[string]podState{}
	for _, p := range m.pods {
		want[d.podName(m.component, p)] = p
	}
	live := &corev1.PodList{}
	if err := d.cluster.cli.List(ctx, live, client.InNamespace(d.opts.Namespace), client.MatchingLabels(d.selectorLabels(m.component))); err != nil {
		return fmt.Errorf("replay: list pods of %s: %w", m.component, err)
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
// replica controller publishes it: the revision pair, the counters derived
// from the rows and their pods, the dense rows, the Ready condition and
// the observed generation.
func (d *driver) publish(ctx context.Context, m *member) error {
	ir, err := d.replica(ctx, m.component)
	if err != nil {
		return err
	}
	current, err := d.revisionName(ctx, m, m.current)
	if err != nil {
		return err
	}
	target, err := d.revisionName(ctx, m, m.target)
	if err != nil {
		return err
	}
	status := v1beta1.InferenceReplicaStatus{
		CurrentRevision: current,
		UpdateRevision:  target,
		LabelSelector:   metav1.FormatLabelSelector(&metav1.LabelSelector{MatchLabels: d.selectorLabels(m.component)}),
	}
	rows := make([]v1beta1.OMENativeInstanceStatus, 0, len(m.rows))
	for _, r := range m.rows {
		total, ready, serving := m.instancePods(r.index)
		running, err := d.revisionName(ctx, m, r.running)
		if err != nil {
			return err
		}
		rowTarget, err := d.revisionName(ctx, m, r.target)
		if err != nil {
			return err
		}
		phase := r.phase
		if total == 0 && phase == v1beta1.OMENativeInstanceReady {
			phase = v1beta1.OMENativeInstancePending
		}
		row := v1beta1.OMENativeInstanceStatus{
			Index:             r.index,
			Incarnation:       1,
			Phase:             phase,
			RunningRevision:   running,
			TargetRevision:    rowTarget,
			PodCount:          total,
			ServingPodCount:   serving,
			AvailablePodCount: ready,
		}
		if !r.readySince.IsZero() {
			at := metav1.NewTime(r.readySince)
			row.ReadySince = &at
		}
		rows = append(rows, row)
		instanceReady := total > 0 && ready == total
		if instanceReady {
			status.ReadyReplicas++
			status.AvailableReplicas++
		}
		if total > 0 && serving == total {
			status.ServingReplicas++
		}
		if target != "" && running == target {
			status.UpdatedReplicas++
			if instanceReady {
				status.UpdatedReadyReplicas++
			}
		}
	}
	status.Replicas = int32(len(rows))
	status.InstanceStatuses = rows
	status.ObservedGeneration = ir.Generation
	if m.stale {
		status.ObservedGeneration = ir.Generation - 1
		m.stale = false
	}
	readyStatus := metav1.ConditionFalse
	reason := "InstancesNotReady"
	if status.Replicas > 0 && status.ReadyReplicas == status.Replicas {
		readyStatus, reason = metav1.ConditionTrue, "AllInstancesReady"
	}
	if m.readyStatus != readyStatus {
		m.readyStatus, m.readyAt = readyStatus, d.clock.Now()
	}
	status.Conditions = []metav1.Condition{{
		Type:               "Ready",
		Status:             readyStatus,
		Reason:             reason,
		LastTransitionTime: metav1.NewTime(m.readyAt),
	}}
	if equality.Semantic.DeepEqual(ir.Status, status) {
		return nil
	}
	ir.Status = status
	if err := d.cluster.staging(func() error { return d.cluster.cli.Status().Update(ctx, ir) }); err != nil {
		return fmt.Errorf("replay: publish replica %s: %w", ir.Name, err)
	}
	d.trace.line("publish component=%s current=%s update=%s observedGeneration=%d replicas=%d ready=%d serving=%d updated=%d updatedReady=%d rows=%s",
		m.component, orNil(current), orNil(target), status.ObservedGeneration, status.Replicas, status.ReadyReplicas,
		status.ServingReplicas, status.UpdatedReplicas, status.UpdatedReadyReplicas, renderRows(rows))
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
// then its status.
func (d *driver) sync(ctx context.Context, m *member) error {
	if err := d.syncPods(ctx, m); err != nil {
		return err
	}
	return d.publish(ctx, m)
}
