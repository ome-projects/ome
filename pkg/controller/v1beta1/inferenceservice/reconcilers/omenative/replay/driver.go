package replay

import (
	"context"
	"fmt"
	"sort"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/rollout"
	"sigs.k8s.io/ome/pkg/validation"
)

// Options names the fixture identities a run is built on.
type Options struct {
	Namespace   string
	ServiceName string
	// Start is the instant the injected clock begins at and every trace
	// timestamp is rendered relative to.
	Start time.Time
}

// DefaultOptions returns the fixture identities scenarios are written
// against.
func DefaultOptions() Options {
	return Options{
		Namespace:   "ns-a",
		ServiceName: "svc-a",
		Start:       time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
	}
}

// serviceUID is the service's identity; every revision hash is scoped to
// it, so a hash depends only on the template a scenario declares.
const serviceUID k8stypes.UID = "svc-uid"

// Run replays one scenario and returns its normalized trace. The error
// return is the driver's own failure; what the engines do with a failure
// is recorded in the trace and the run keeps going.
func Run(ctx context.Context, s *Scenario, opts Options) (string, error) {
	d, err := newDriver(ctx, s, opts)
	if err != nil {
		return "", err
	}
	for _, tick := range s.Timeline {
		if err := d.runTick(ctx, tick); err != nil {
			return d.trace.String(), err
		}
	}
	return d.trace.String(), nil
}

type driver struct {
	opts    Options
	norm    *normalizer
	trace   *Trace
	clock   *clocktesting.FakeClock
	cluster *cluster
	// recorder receives the engines' Kubernetes events.
	recorder record.EventRecorder

	service ServiceState
	cfg     ConfigState
	members map[v1beta1.ComponentType]*member
	// removedPolicies keeps the body of a policy a scenario removed, for
	// the event that restores it.
	removedPolicies map[string]*v1beta1.RolloutPolicy
	// collected names the revisions a scenario garbage-collected, per
	// Component, so nothing re-mints one by accident.
	collected map[v1beta1.ComponentType]map[string]bool
	// stage names the stage of the pass an engine write lands in.
	stage string
	// staleClaim is the stale-read variant staged for the next pass, checked
	// against the live object and the snapshot when the pass begins.
	staleClaim string
}

func newDriver(ctx context.Context, s *Scenario, opts Options) (*driver, error) {
	defaults := DefaultOptions()
	if opts.Namespace == "" {
		opts.Namespace = defaults.Namespace
	}
	if opts.ServiceName == "" {
		opts.ServiceName = defaults.ServiceName
	}
	if opts.Start.IsZero() {
		opts.Start = defaults.Start
	}
	clk := clocktesting.NewFakeClock(opts.Start)
	cl, err := newCluster(clk)
	if err != nil {
		return nil, err
	}
	norm := &normalizer{start: opts.Start}
	d := &driver{
		opts:            opts,
		norm:            norm,
		clock:           clk,
		cluster:         cl,
		trace:           newTrace(norm, clk.Now),
		service:         cloneService(s.Initial.Service),
		cfg:             s.Initial.Config,
		members:         map[v1beta1.ComponentType]*member{},
		removedPolicies: map[string]*v1beta1.RolloutPolicy{},
		collected:       map[v1beta1.ComponentType]map[string]bool{},
	}
	d.recorder = &traceRecorder{trace: d.trace}
	cl.observe = func(w writeRecord) {
		if d.stage != "" {
			w.detail = joinFields(nonEmpty("stage="+d.stage, w.detail))
		}
		d.trace.write(w)
	}
	for _, tick := range s.Timeline {
		for _, ev := range tick.Events {
			if _, stale := staleReadEvents[ev.ID]; stale {
				cl.snapshots = true
			}
		}
	}
	if err := d.seed(ctx, s); err != nil {
		return nil, err
	}
	return d, nil
}

// cloneService detaches the driver's mutable spec from the scenario, so a
// run never writes into the state the next run starts from.
func cloneService(in ServiceState) ServiceState {
	out := ServiceState{Components: map[string]ComponentState{}, Annotations: copyMap(in.Annotations)}
	for name, cs := range in.Components {
		if cs.Partition != nil {
			partition := *cs.Partition
			cs.Partition = &partition
		}
		out.Components[name] = cs
	}
	for _, g := range in.Groups {
		out.Groups = append(out.Groups, *g.DeepCopy())
	}
	if in.PairingProtocol != nil {
		protocol := *in.PairingProtocol
		out.PairingProtocol = &protocol
	}
	return out
}

func nonEmpty(parts ...string) []string {
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func copyMap(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// components lists the declared Components in the order the controller
// walks them: engine, decoder, router.
func (d *driver) components() []v1beta1.ComponentType {
	var out []v1beta1.ComponentType
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		if _, ok := d.members[c]; ok {
			out = append(out, c)
		}
	}
	return out
}

func (d *driver) isvcUID() k8stypes.UID { return serviceUID }

func (d *driver) isvcKey() client.ObjectKey {
	return client.ObjectKey{Namespace: d.opts.Namespace, Name: d.opts.ServiceName}
}

// liveService reads the service as the store holds it.
func (d *driver) liveService(ctx context.Context) (*v1beta1.InferenceService, error) {
	isvc := &v1beta1.InferenceService{}
	if err := d.cluster.cli.Get(ctx, d.isvcKey(), isvc); err != nil {
		return nil, fmt.Errorf("replay: read service: %w", err)
	}
	return isvc, nil
}

// componentExt is the merged Component extension the projection and the
// canary partition read for one Component.
func (d *driver) componentExt(c v1beta1.ComponentType) *v1beta1.ComponentExtensionSpec {
	cs := d.service.Components[string(c)]
	replicas := int(cs.Replicas)
	ext := &v1beta1.ComponentExtensionSpec{MinReplicas: &replicas, MaxReplicas: replicas}
	if cs.Partition != nil {
		partition := *cs.Partition
		ext.Lifecycle = &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{RollingUpdate: &v1beta1.RollingUpdate{Partition: &partition}}}
	}
	return ext
}

// isvcSpec renders the service spec from the driver's mutable copy.
func (d *driver) isvcSpec() v1beta1.InferenceServiceSpec {
	spec := v1beta1.InferenceServiceSpec{}
	if cs, ok := d.service.Components[string(v1beta1.EngineComponent)]; ok {
		spec.Engine = &v1beta1.EngineSpec{PodSpec: v1beta1.PodSpec{Containers: d.podTemplate(cs.Image).Containers}, ComponentExtensionSpec: *d.componentExt(v1beta1.EngineComponent)}
	}
	if cs, ok := d.service.Components[string(v1beta1.DecoderComponent)]; ok {
		spec.Decoder = &v1beta1.DecoderSpec{PodSpec: v1beta1.PodSpec{Containers: d.podTemplate(cs.Image).Containers}, ComponentExtensionSpec: *d.componentExt(v1beta1.DecoderComponent)}
	}
	if cs, ok := d.service.Components[string(v1beta1.RouterComponent)]; ok {
		spec.Router = &v1beta1.RouterSpec{PodSpec: v1beta1.PodSpec{Containers: d.podTemplate(cs.Image).Containers}, ComponentExtensionSpec: *d.componentExt(v1beta1.RouterComponent)}
	}
	if len(d.service.Groups) > 0 || d.service.PairingProtocol != nil {
		spec.Rollout = &v1beta1.RolloutSpec{Groups: append([]v1beta1.RolloutGroup(nil), d.service.Groups...), PairingProtocol: d.service.PairingProtocol}
	}
	return spec
}

// writeSpec rewrites the live service's spec from the driver's copy; the
// store bumps the generation when the spec moved.
func (d *driver) writeSpec(ctx context.Context) (int64, error) {
	isvc, err := d.liveService(ctx)
	if err != nil {
		return 0, err
	}
	isvc.Spec = d.isvcSpec()
	if err := d.cluster.staging(func() error { return d.cluster.cli.Update(ctx, isvc) }); err != nil {
		return 0, fmt.Errorf("replay: write service spec: %w", err)
	}
	return isvc.Generation, nil
}

// setAnnotation writes, or with an empty value removes, one annotation on
// the live service.
func (d *driver) setAnnotation(ctx context.Context, key, value string) error {
	isvc, err := d.liveService(ctx)
	if err != nil {
		return err
	}
	if value == "" {
		delete(isvc.Annotations, key)
	} else {
		if isvc.Annotations == nil {
			isvc.Annotations = map[string]string{}
		}
		isvc.Annotations[key] = value
	}
	if err := d.cluster.staging(func() error { return d.cluster.cli.Update(ctx, isvc) }); err != nil {
		return fmt.Errorf("replay: write annotation %s: %w", key, err)
	}
	return nil
}

// observePolicies reads the referenced RolloutPolicy objects the way the
// run layer reads them, for the seed's projection.
func (d *driver) observePolicies(ctx context.Context, isvc *v1beta1.InferenceService) (rollout.Policies, error) {
	out := rollout.Policies{Namespace: d.opts.Namespace, Enabled: d.cfg.RolloutPolicyEnabled}
	if !d.cfg.RolloutPolicyEnabled || isvc.Spec.Rollout == nil {
		return out, nil
	}
	out.ByName = map[string]*v1beta1.RolloutPolicy{}
	out.Invalid = map[string]error{}
	for i := range isvc.Spec.Rollout.Groups {
		ref := isvc.Spec.Rollout.Groups[i].PolicyRef
		if ref == nil {
			continue
		}
		policy := &v1beta1.RolloutPolicy{}
		err := d.cluster.cli.Get(ctx, client.ObjectKey{Namespace: d.opts.Namespace, Name: ref.Name}, policy)
		switch {
		case err == nil:
			out.ByName[ref.Name] = policy
			if verr := validation.ValidateRolloutPolicySpec(&policy.Spec); verr != nil {
				out.Invalid[ref.Name] = verr
			}
		case apierrors.IsNotFound(err):
		default:
			return rollout.Policies{}, fmt.Errorf("replay: read policy %s: %w", ref.Name, err)
		}
	}
	return out, nil
}

// boundProviders is the set of metric provider names the cluster binds.
func (d *driver) boundProviders() map[string]struct{} {
	out := map[string]struct{}{}
	for _, name := range d.cfg.BoundProviders {
		out[name] = struct{}{}
	}
	return out
}

// seed materializes the initial state: the service, the policies, one
// replica per Component projected by the production projector, the
// revisions and pods the scenario names, and each replica's status.
func (d *driver) seed(ctx context.Context, s *Scenario) error {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{
			Namespace:   d.opts.Namespace,
			Name:        d.opts.ServiceName,
			UID:         serviceUID,
			Annotations: copyMap(d.service.Annotations),
		},
		Spec: d.isvcSpec(),
	}
	if err := d.cluster.staging(func() error { return d.cluster.cli.Create(ctx, isvc) }); err != nil {
		return fmt.Errorf("replay: create service: %w", err)
	}
	for _, p := range s.Initial.Policies {
		policy := &v1beta1.RolloutPolicy{ObjectMeta: metav1.ObjectMeta{Namespace: d.opts.Namespace, Name: p.Name}, Spec: p.Spec}
		if err := d.cluster.staging(func() error { return d.cluster.cli.Create(ctx, policy) }); err != nil {
			return fmt.Errorf("replay: create policy %s: %w", p.Name, err)
		}
	}
	names := make([]string, 0, len(d.service.Components))
	for name := range d.service.Components {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		cs := d.service.Components[name]
		if cs.Image == "" {
			return fmt.Errorf("replay: component %s declares no image", name)
		}
		if cs.Replicas <= 0 {
			return fmt.Errorf("replay: component %s declares no replicas", name)
		}
		d.members[v1beta1.ComponentType(name)] = &member{
			component: v1beta1.ComponentType(name),
			image:     cs.Image,
			current:   cs.Image,
			target:    cs.Image,
			revisions: map[string]*appsv1.ControllerRevision{},
		}
	}
	// The replicas are projected by the production projector with the
	// partition the first pass would project, so that pass changes nothing.
	policies, err := d.observePolicies(ctx, isvc)
	if err != nil {
		return err
	}
	for _, c := range d.components() {
		if err := d.cluster.staging(func() error {
			_, err := d.project(ctx, d.cluster.cli, d.cluster.cli, isvc, c, d.partitionFor(isvc, policies, c))
			return err
		}); err != nil {
			return err
		}
	}
	for _, c := range d.components() {
		m := d.members[c]
		if err := d.seedMember(ctx, m, s.Initial.Replicas[string(c)]); err != nil {
			return err
		}
		if err := d.sync(ctx, m); err != nil {
			return err
		}
	}
	return nil
}

// seedMember fills a member from the scenario's initial replica state: one
// Ready row and pod per replica on the current revision unless the
// scenario says otherwise.
func (d *driver) seedMember(ctx context.Context, m *member, state ReplicaState) error {
	current, err := m.resolveImage(state.Current)
	if err != nil {
		return err
	}
	m.current = current
	m.target = current
	if state.Target != "" {
		if m.target, err = m.resolveImage(state.Target); err != nil {
			return err
		}
	}
	m.stale = state.Stale
	replicas := d.service.Components[string(m.component)].Replicas
	if len(state.Rows) == 0 {
		for i := int32(0); i < replicas; i++ {
			m.rows = append(m.rows, rowState{index: i, phase: v1beta1.OMENativeInstanceReady, running: m.current, readySince: d.opts.Start})
		}
	}
	for _, spec := range state.Rows {
		row := rowState{index: spec.Index, phase: v1beta1.OMENativeInstancePhase(spec.Phase), readySince: d.opts.Start}
		if row.phase == "" {
			row.phase = v1beta1.OMENativeInstanceReady
		}
		if row.running, err = m.resolveImage(spec.RunningRevision); err != nil {
			return err
		}
		if spec.RunningRevision == "" {
			row.running = m.current
		}
		if spec.TargetRevision != "" {
			if row.target, err = m.resolveImage(spec.TargetRevision); err != nil {
				return err
			}
		}
		if spec.ReadySince != "" {
			delta, err := ParseDuration("rows.readySince", spec.ReadySince)
			if err != nil {
				return err
			}
			row.readySince = d.opts.Start.Add(delta)
		}
		m.rows = append(m.rows, row)
	}
	if len(state.Pods) == 0 {
		for _, row := range m.rows {
			m.pods = append(m.pods, podState{index: row.index, image: row.running, ready: row.phase == v1beta1.OMENativeInstanceReady, serving: row.phase == v1beta1.OMENativeInstanceReady})
		}
	}
	for _, spec := range state.Pods {
		pod := podState{index: spec.Index, ordinal: spec.Ordinal, ready: spec.Ready, serving: spec.Ready}
		if spec.Serving != nil {
			pod.serving = *spec.Serving
		}
		pod.image = m.rowRunning(spec.Index)
		if spec.Revision != "" {
			if pod.image, err = m.resolveImage(spec.Revision); err != nil {
				return err
			}
		}
		m.pods = append(m.pods, pod)
	}
	return nil
}

// rowRunning is the running revision of the row at index, the current
// revision when no row carries the index.
func (m *member) rowRunning(index int32) string {
	for _, row := range m.rows {
		if row.index == index {
			return row.running
		}
	}
	return m.current
}

// project runs the production projector for one Component: the replica
// spec the controller's component reconciler writes on every pass.
func (d *driver) project(ctx context.Context, cached client.Client, live client.Reader, isvc *v1beta1.InferenceService, c v1beta1.ComponentType, partition *int32) (*v1beta1.InferenceReplica, error) {
	cs := d.service.Components[string(c)]
	ir, err := irprojector.EnsureInferenceReplica(ctx, irprojector.Params{
		ISVC:            isvc,
		Component:       c,
		ComponentExt:    d.componentExt(c),
		ObjectMeta:      metav1.ObjectMeta{Labels: d.selectorLabels(c)},
		PodSpec:         d.podTemplate(cs.Image),
		PacingPartition: partition,
		Client:          cached,
		Reader:          live,
	})
	if err != nil {
		return nil, fmt.Errorf("replay: project %s: %w", c, err)
	}
	return ir, nil
}

// runTick applies a tick's events, lets every replica publish, and runs
// exactly one pass.
func (d *driver) runTick(ctx context.Context, tick Tick) error {
	d.trace.beginTick(tick.Tick, tick.Note)
	d.cluster.beginPass()
	for _, ev := range tick.Events {
		if err := d.apply(ctx, ev); err != nil {
			return err
		}
	}
	// The replica controller publishes between the service's passes.
	for _, c := range d.components() {
		if err := d.sync(ctx, d.members[c]); err != nil {
			return err
		}
	}
	if err := d.checkStaleClaim(ctx); err != nil {
		return err
	}
	if err := d.cluster.prepareSnapshot(ctx, tick.Tick); err != nil {
		return err
	}
	if err := d.runPass(ctx); err != nil {
		return err
	}
	d.cluster.finishPass()
	if unfired := d.cluster.unfiredConflicts(); len(unfired) > 0 {
		return fmt.Errorf("replay: tick %d: api.conflict armed for status write %v, but the pass made no such write", tick.Tick, unfired)
	}
	return nil
}

// traceRecorder writes the engines' Kubernetes events into the trace.
type traceRecorder struct {
	trace *Trace
}

func (r *traceRecorder) Event(_ runtime.Object, eventtype, reason, message string) {
	r.trace.event(eventtype, reason, message)
}

func (r *traceRecorder) Eventf(_ runtime.Object, eventtype, reason, messageFmt string, args ...any) {
	r.trace.event(eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

func (r *traceRecorder) AnnotatedEventf(_ runtime.Object, _ map[string]string, eventtype, reason, messageFmt string, args ...any) {
	r.trace.event(eventtype, reason, fmt.Sprintf(messageFmt, args...))
}

var _ record.EventRecorder = (*traceRecorder)(nil)
