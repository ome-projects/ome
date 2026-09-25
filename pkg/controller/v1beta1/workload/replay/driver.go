package replay

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clocktesting "k8s.io/utils/clock/testing"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadservice "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/service"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// Options names the fixture identities a run is built on. Every field has
// a neutral default; a caller overrides one only to make a trace read
// better for a particular family of scenarios.
type Options struct {
	Namespace string
	// OwnerName is the parent name every pod, Service and revision is
	// composed from.
	OwnerName string
	Component types.ComponentType
	// Start is the instant the injected clock begins at and every trace
	// timestamp is rendered relative to.
	Start time.Time
	// ContainerName is the single container of the rendered template.
	ContainerName string
}

// DefaultOptions returns the fixture identities scenarios are written
// against.
func DefaultOptions() Options {
	return Options{
		Namespace:     "ns-a",
		OwnerName:     "svc-a",
		Component:     types.ComponentEngine,
		Start:         time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
		ContainerName: "main",
	}
}

// parentUID is the revision-scope identity every run shares, so a
// revision hash depends only on the pod template a scenario declares.
const parentUID k8stypes.UID = "parent-uid"

// ownerUID identifies the workload owner the engine stamps on pods.
const ownerUID k8stypes.UID = "owner-uid"

// Run replays one scenario and returns its normalized trace. The error
// return is the driver's own failure — a scenario the engine rejects
// records the rejection in the trace and keeps going, because what the
// engine does with a failure is exactly what a behavior lock must hold.
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
	opts  Options
	norm  *normalizer
	trace *Trace
	clock *clocktesting.FakeClock

	cli          client.Client
	owner        *v1beta1.InferenceReplica
	store        *RowStore
	expectations *types.Expectations
	recorder     *traceRecorder

	spec SpecState
	cfg  ConfigState

	// rejections are the injected apiserver refusals still owed to a
	// matching write.
	rejections []*pendingRejection
	// collisionCount salts the revision hash; the driver never bumps it,
	// but the engine's hash inputs include it.
	collisionCount *int32

	currentRevision string

	// quiet suppresses effect recording while the driver itself is
	// staging a scenario event.
	quiet bool
	// podUIDs numbers the identities the driver assigns on admission.
	podUIDs int
	// terminating holds, by pod name, the deletions the driver is keeping
	// open — the instant each landed on the injected clock, and whether a
	// dead kubelet is what keeps the object from leaving.
	terminating map[string]*podTeardown
	// migrations is the owner's persisted status.migrations list.
	migrations []types.MigrationRecord
	// migrationRequests counts the requests the scenario has made, so an
	// unnamed one gets a stable identity.
	migrationRequests int
	// teardown marks every pass from the owner's deletion onward: the
	// planned index set reads as empty and every Instance is a
	// scale-down extra.
	teardown bool
	// rollbackImage pins the roll to the stored revision that image's
	// template minted. Empty rolls toward the rendered template.
	rollbackImage string
}

func newDriver(ctx context.Context, s *Scenario, opts Options) (*driver, error) {
	defaults := DefaultOptions()
	if opts.Namespace == "" {
		opts.Namespace = defaults.Namespace
	}
	if opts.OwnerName == "" {
		opts.OwnerName = defaults.OwnerName
	}
	if opts.Component == "" {
		opts.Component = defaults.Component
	}
	if opts.Start.IsZero() {
		opts.Start = defaults.Start
	}
	if opts.ContainerName == "" {
		opts.ContainerName = defaults.ContainerName
	}

	norm := newNormalizer(opts.Start)
	clk := clocktesting.NewFakeClock(opts.Start)
	d := &driver{
		opts:         opts,
		norm:         norm,
		clock:        clk,
		trace:        newTrace(norm, clk.Now),
		expectations: types.NewExpectationsWithClock(clk),
		spec:         s.Initial.Spec,
		cfg:          s.Initial.Config,
		terminating:  map[string]*podTeardown{},
	}
	d.recorder = &traceRecorder{trace: d.trace}

	scheme, err := buildScheme()
	if err != nil {
		return nil, err
	}
	d.owner = &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: opts.Namespace,
			Name:      opts.OwnerName + "-" + string(opts.Component),
			UID:       ownerUID,
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: v1beta1.SchemeGroupVersion.String(),
				Kind:       "InferenceService",
				Name:       opts.OwnerName,
				UID:        parentUID,
				Controller: boolPtr(true),
			}},
		},
	}
	d.cli = fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceReplica{}).
		WithObjects(d.owner).
		WithInterceptorFuncs(d.interceptors()).
		Build()

	if err := workloadservice.ReconcileHeadlessService(ctx, d.cli, types.PerComponentServiceSpec{
		Name:      query.HeadlessServiceName(opts.OwnerName, opts.Component),
		Namespace: opts.Namespace,
		Selector:  d.selectorLabels(),
		Labels:    d.selectorLabels(),
	}); err != nil {
		return nil, fmt.Errorf("replay: reconcile headless service: %w", err)
	}

	rows, err := d.seedRows(ctx, s.Initial.Rows)
	if err != nil {
		return nil, err
	}
	blocks, err := d.seedRetryBlocks(ctx, s.Initial.RetryBlocks)
	if err != nil {
		return nil, err
	}
	d.store = NewRowStore(rows, blocks, d.trace)
	d.store.SetOwner(d.owner.UID, d.owner.Generation)

	if err := d.seedPods(ctx, s.Initial.Pods); err != nil {
		return nil, err
	}
	return d, nil
}

func buildScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		appsv1.AddToScheme,
		discoveryv1.AddToScheme,
		v1beta1.AddToScheme,
		schedulingv1alpha1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, fmt.Errorf("replay: build scheme: %w", err)
		}
	}
	return scheme, nil
}

func (d *driver) selectorLabels() map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: d.opts.OwnerName,
		constants.OMEComponentLabel:           string(d.opts.Component),
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
}

func (d *driver) key() types.Key {
	return types.Key{
		Namespace:      d.opts.Namespace,
		Component:      d.opts.Component,
		OwnerName:      d.opts.OwnerName,
		SelectorLabels: d.selectorLabels(),
	}
}

// containerName resolves the single container the template renders. The
// scenario's name wins over the fixture default, because the restart
// trigger identifies the runner container by the name production renders
// it under.
func (d *driver) containerName() string {
	if d.spec.ContainerName != "" {
		return d.spec.ContainerName
	}
	return d.opts.ContainerName
}

// podTemplate renders the container template for one image. Everything
// outside the image comes from the driver's mutable spec, so a template
// edit that is not an image rewrite mints a revision the in-place
// strategies are not allowed to converge on.
func (d *driver) podTemplate(image string) *corev1.PodSpec {
	if image == "" {
		image = "registry.example.com/runtime:v1"
	}
	return &corev1.PodSpec{
		SchedulerName: d.spec.SchedulerName,
		Containers: []corev1.Container{{
			Name:  d.containerName(),
			Image: image,
			Env:   sortedEnv(d.spec.Env),
		}},
	}
}

// sortedEnv renders the scenario's environment in key order, because the
// template is hashed and a map iteration would mint a different revision
// on every run.
func sortedEnv(env map[string]string) []corev1.EnvVar {
	if len(env) == 0 {
		return nil
	}
	names := make([]string, 0, len(env))
	for name := range env {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]corev1.EnvVar, 0, len(names))
	for _, name := range names {
		out = append(out, corev1.EnvVar{Name: name, Value: env[name]})
	}
	return out
}

// desiredSpec projects the driver's mutable spec onto the engine's input.
func (d *driver) desiredSpec() (types.WorkloadDesiredSpec, error) {
	strategy := types.UpdateStrategy{Type: types.UpdateStrategyType(d.spec.Strategy)}
	if d.spec.MaxUnavailable != "" || d.spec.MaxSurge != "" {
		rolling := &types.RollingUpdate{}
		if d.spec.MaxUnavailable != "" {
			value := intstr.Parse(d.spec.MaxUnavailable)
			rolling.MaxUnavailable = &value
		}
		if d.spec.MaxSurge != "" {
			value := intstr.Parse(d.spec.MaxSurge)
			rolling.MaxSurge = &value
		}
		strategy.RollingUpdate = rolling
	}
	if d.spec.MarkNotReady != nil {
		strategy.InPlaceUpdateStrategy = &types.InPlaceUpdateStrategy{MarkNotReadyDuringLifecycle: d.spec.MarkNotReady}
	}
	lifecycle := types.Lifecycle{UpdateStrategy: &strategy}
	if d.spec.RestartPolicy != "" {
		policy := types.RestartPolicy(d.spec.RestartPolicy)
		lifecycle.RestartPolicy = &policy
	}
	if d.spec.InstanceReadyTimeout != "" {
		timeout, err := ParseDuration("spec.instanceReadyTimeout", d.spec.InstanceReadyTimeout)
		if err != nil {
			return types.WorkloadDesiredSpec{}, err
		}
		lifecycle.InstanceReadyTimeout = &metav1.Duration{Duration: timeout}
	}
	if d.spec.MigrationMode != "" {
		lifecycle.MigrationPolicy = &types.MigrationPolicy{Mode: types.MigrationMode(d.spec.MigrationMode)}
	}
	desired := types.WorkloadDesiredSpec{
		Replicas:                d.spec.Replicas,
		MinReadySeconds:         d.spec.MinReadySeconds,
		Runners:                 []types.Runner{{Name: DefaultRunner, Size: 1}},
		PodSpec:                 d.podTemplate(d.spec.Image),
		Lifecycle:               lifecycle,
		Paused:                  d.spec.Paused,
		PauseFreeze:             d.spec.PauseFreeze,
		GangSchedulingAvailable: d.spec.GangScheduling,
	}
	// A worker count is what makes the Component multi-pod: the plan
	// splits an Instance into one leader and that many workers, which is
	// the unit a gang is scheduled in.
	if d.spec.Workers > 0 {
		desired.MultiPod = true
		desired.Runners = []types.Runner{{Name: RunnerLeader, Size: 1}, {Name: RunnerWorker, Size: d.spec.Workers}}
		desired.WorkerPodSpec = d.podTemplate(d.spec.Image)
		desired.TopologyKey = d.spec.TopologyKey
	}
	return desired, nil
}

// ensureRevision mints (or reuses) the ControllerRevision for an image,
// exactly as the adapter does before every reconcile.
func (d *driver) ensureRevision(ctx context.Context, image string) (*appsv1.ControllerRevision, error) {
	key := revision.Key{
		Namespace: d.opts.Namespace,
		Name:      d.opts.OwnerName + "-" + string(d.opts.Component),
		Labels:    d.selectorLabels(),
	}
	target, collision, err := revision.EnsureControllerRevision(
		ctx, d.cli, d.cli, d.owner, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"),
		key, d.podTemplate(image), nil, d.collisionCount, parentUID,
	)
	if err != nil {
		return nil, fmt.Errorf("replay: ensure revision: %w", err)
	}
	if collision {
		return nil, fmt.Errorf("replay: revision hash collision on image %q", image)
	}
	return target, nil
}

// bumpGeneration advances the owner's spec generation, which is what an
// apiserver does on every spec write. The delete-admission guard compares
// the generation it planned against the one the status write observes, so
// an owner whose generation never moves turns that guard into a
// tautology.
func (d *driver) bumpGeneration(ctx context.Context) error {
	d.owner.Generation++
	stored := &v1beta1.InferenceReplica{}
	key := client.ObjectKeyFromObject(d.owner)
	if err := d.cli.Get(ctx, key, stored); err != nil {
		return fmt.Errorf("replay: read owner for generation bump: %w", err)
	}
	stored.Generation = d.owner.Generation
	if err := d.staging(func() error { return d.cli.Update(ctx, stored) }); err != nil {
		return fmt.Errorf("replay: bump owner generation: %w", err)
	}
	return nil
}

// revisionFor resolves a scenario's symbolic revision name.
func (d *driver) revisionFor(ctx context.Context, symbol string) (string, error) {
	switch symbol {
	case "":
		return "", nil
	case "current":
		rev, err := d.ensureRevision(ctx, d.spec.Image)
		if err != nil {
			return "", err
		}
		return rev.Name, nil
	default:
		return "", fmt.Errorf("replay: unknown revision symbol %q (use \"current\")", symbol)
	}
}

func (d *driver) seedRows(ctx context.Context, specs []RowSpec) ([]types.InstanceStatus, error) {
	rows := make([]types.InstanceStatus, 0, len(specs))
	for _, spec := range specs {
		row := types.InstanceStatus{
			Index:         spec.Index,
			Incarnation:   spec.Incarnation,
			Phase:         types.InstancePhase(spec.Phase),
			ActiveOrdinal: spec.ActiveOrdinal,
		}
		if row.Incarnation == 0 {
			row.Incarnation = 1
		}
		running, err := d.revisionFor(ctx, spec.RunningRevision)
		if err != nil {
			return nil, err
		}
		row.RunningRevision = running
		if spec.ReadySince != "" {
			at, err := d.offset("rows.readySince", spec.ReadySince)
			if err != nil {
				return nil, err
			}
			stamp := metav1.NewTime(at)
			row.ReadySince = &stamp
		}
		rows = append(rows, row)
	}
	return rows, nil
}

func (d *driver) seedRetryBlocks(ctx context.Context, specs []RetryBlockSpec) ([]types.RetryBlock, error) {
	blocks := make([]types.RetryBlock, 0, len(specs))
	for _, spec := range specs {
		target, err := d.revisionFor(ctx, spec.Revision)
		if err != nil {
			return nil, err
		}
		block := types.RetryBlock{
			TargetRevision:  target,
			State:           types.RetryBlockState(spec.State),
			AttemptsStarted: spec.AttemptsStarted,
			Reason:          spec.Reason,
		}
		if spec.NextRetryAt != "" {
			at, err := d.offset("retryBlocks.nextRetryAt", spec.NextRetryAt)
			if err != nil {
				return nil, err
			}
			stamp := metav1.NewTime(at)
			block.NextRetryAt = &stamp
		}
		blocks = append(blocks, block)
	}
	return blocks, nil
}

// offset resolves a scenario duration into an absolute instant relative
// to the scenario start.
func (d *driver) offset(field, value string) (time.Time, error) {
	delta, err := ParseDuration(field, value)
	if err != nil {
		return time.Time{}, err
	}
	return d.opts.Start.Add(delta), nil
}

// seedPods materializes the pods the first tick observes, through the
// production renderer so their names, labels and gates are the ones the
// engine would have written itself.
func (d *driver) seedPods(ctx context.Context, specs []PodSpec) error {
	for _, spec := range specs {
		image := d.spec.Image
		if spec.PreviousImage != "" {
			image = spec.PreviousImage
		}
		rev, err := d.ensureRevision(ctx, image)
		if err != nil {
			return err
		}
		pod, err := d.renderPod(spec, rev)
		if err != nil {
			return err
		}
		if err := d.staging(func() error { return d.cli.Create(ctx, pod) }); err != nil {
			return fmt.Errorf("replay: seed pod %s: %w", pod.Name, err)
		}
		if err := d.applyPodStatus(ctx, pod.Name, spec); err != nil {
			return err
		}
		if spec.Routed {
			if err := d.setRotation(ctx, pod.Name, true); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *driver) renderPod(spec PodSpec, rev *appsv1.ControllerRevision) (*corev1.Pod, error) {
	incarnation := spec.Incarnation
	if incarnation == 0 {
		incarnation = 1
	}
	plan := types.ComponentPlan{Component: d.opts.Component, Replicas: d.spec.Replicas}
	inst := types.InstancePlan{
		Index:       spec.Index,
		Incarnation: incarnation,
		Runners:     []types.RunnerPlan{{Name: DefaultRunner, Size: 1}},
	}
	runner := types.RunnerPlan{Name: spec.Runner, Size: 1}
	if runner.Name == "" {
		runner.Name = DefaultRunner
	}
	image := d.spec.Image
	if spec.PreviousImage != "" {
		image = spec.PreviousImage
	}
	pod, err := workloadops.RenderWithRevision(
		d.owner, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"), d.key(),
		d.podTemplate(image), nil, plan, inst, runner, spec.Ordinal,
		query.RevisionHashFromControllerRevisionName(rev.Name), nil,
	)
	if err != nil {
		return nil, fmt.Errorf("replay: render seed pod: %w", err)
	}
	return pod, nil
}

// applyPodStatus writes the observed pod status a seeded pod declares.
func (d *driver) applyPodStatus(ctx context.Context, name string, spec PodSpec) error {
	readyAt := d.opts.Start
	if spec.ReadyAt != "" {
		at, err := d.offset("pods.readyAt", spec.ReadyAt)
		if err != nil {
			return err
		}
		readyAt = at
	}
	return d.patchPod(ctx, name, func(pod *corev1.Pod) {
		if spec.Node != "" {
			pod.Spec.NodeName = spec.Node
		}
		if spec.Phase != "" {
			pod.Status.Phase = corev1.PodPhase(spec.Phase)
		} else {
			pod.Status.Phase = corev1.PodRunning
		}
		if spec.ContainersReady || spec.Ready {
			setPodCondition(pod, corev1.ContainersReady, corev1.ConditionTrue, readyAt)
		}
		if spec.Serving {
			setPodCondition(pod, query.ServingConditionType, corev1.ConditionTrue, readyAt)
		}
		if spec.Ready {
			setPodCondition(pod, corev1.PodReady, corev1.ConditionTrue, readyAt)
			setContainerStatusesReady(pod)
		}
	})
}

// runTick applies a tick's events and then runs exactly one reconcile.
func (d *driver) runTick(ctx context.Context, tick Tick) error {
	d.trace.beginTick(tick.Tick, tick.Note)
	for _, ev := range tick.Events {
		if err := d.apply(ctx, ev); err != nil {
			return err
		}
	}

	desired, err := d.desiredSpec()
	if err != nil {
		return err
	}
	specTarget, err := d.ensureRevision(ctx, d.spec.Image)
	if err != nil {
		return err
	}
	if d.currentRevision == "" {
		d.currentRevision = specTarget.Name
	}
	// A pinned rollback overrides the rendered template with the stored
	// revision's payload and makes that revision the ROLL target, while
	// UpdateRevision keeps reporting the spec target. The forward-roll
	// machinery then drains the current pods onto the pinned revision.
	target := specTarget
	if d.rollbackImage != "" {
		pinned, perr := d.ensureRevision(ctx, d.rollbackImage)
		if perr != nil {
			return perr
		}
		payload, perr := revision.PayloadFromControllerRevision(pinned)
		if perr != nil {
			return fmt.Errorf("replay: rollback revision payload: %w", perr)
		}
		if payload == nil || payload.PodSpec == nil {
			return fmt.Errorf("replay: rollback revision %s carries no pod template", pinned.Name)
		}
		desired.PodSpec = payload.PodSpec
		desired.WorkerPodSpec = payload.WorkerPodSpec
		target = pinned
	}

	observed := types.WorkloadObservedState{
		CurrentRevision:  d.currentRevision,
		UpdateRevision:   specTarget.Name,
		InstanceStatuses: d.store.Rows(),
		RetryBlocks:      d.store.RetryBlocks(),
		Migrations:       append([]types.MigrationRecord(nil), d.migrations...),
	}
	plan, err := workload.BuildPlan(d.opts.Component, desired, observed)
	if err != nil {
		return fmt.Errorf("replay: build plan: %w", err)
	}

	input, err := d.buildInput(desired, observed)
	if err != nil {
		return err
	}
	deps := types.Deps{
		Client:       d.cli,
		APIReader:    d.cli,
		Recorder:     d.recorder,
		Expectations: d.expectations,
		Clock:        d.clock,
	}
	if err := d.reconcileGangs(ctx, &deps, input, plan); err != nil {
		return err
	}
	// Pausing parks every in-flight deadline before the dispatcher runs, so
	// a pause that lands mid-operation cannot spend the operation's clock
	// while nothing is allowed to progress.
	if desired.Paused {
		if err := d.reconcileHeldDeadlines(ctx, input, true, 0); err != nil {
			return err
		}
		input.ObservedState.InstanceStatuses = d.store.Rows()
	}
	result, rerr := workload.Reconcile(ctx, deps, input, plan, target)
	d.trace.emit("pass result=%s", renderResult(result, rerr))
	// The status-aggregate pass runs after the dispatcher on every
	// reconcile: it parks the deadline of an Instance held by admission or
	// by an external wait, and rearms one that has been released.
	return d.reconcileHeldDeadlines(ctx, input, desired.Paused, plan.InstanceReadyTimeout)
}

// reconcileHeldDeadlines is the adapter's deadline park/rearm step. An
// Instance is held when a pause covers it, when one of its pods is behind
// an admission scheduling gate, or when the surge bucket its operation
// points at is. Held parks the deadline; released rearms it from now.
func (d *driver) reconcileHeldDeadlines(ctx context.Context, input types.ReconcileInput, paused bool, timeout time.Duration) error {
	rows := d.store.Rows()
	byIndex, err := d.podsByInstance(ctx)
	if err != nil {
		return err
	}
	held := make(map[int32]bool, len(byIndex))
	for idx, pods := range byIndex {
		for _, pod := range pods {
			if types.PodAdmissionGated(pod) {
				held[idx] = true
				break
			}
		}
	}
	// A gang-surge source's attempt pods live in its Operation.SurgeIndex
	// bucket, so the source's own deadline parks with them.
	for i := range rows {
		op := rows[i].Operation
		if op == nil || op.Type != types.InstanceOperationUpdate || op.SurgeIndex == nil {
			continue
		}
		if held[*op.SurgeIndex] {
			held[rows[i].Index] = true
		}
	}
	if paused {
		for i := range rows {
			held[rows[i].Index] = true
		}
	}
	parkInput := types.ReconcileInput{
		OwnerObject:             d.owner,
		OwnerGVK:                input.OwnerGVK,
		Key:                     input.Key,
		ObservedState:           types.WorkloadObservedState{InstanceStatuses: rows},
		Clock:                   d.clock,
		MutateInstance:          d.store.MutateInstance,
		RemoveInstance:          func(context.Context, int32) (bool, error) { return false, nil },
		WriteAggregateCondition: func(context.Context, metav1.Condition) error { return nil },
		WarnInstanceFailed:      func(int32, string, string) {},
	}
	if err := escalation.ReconcileGatedDeadlines(ctx, parkInput, rows, held, timeout); err != nil {
		return fmt.Errorf("replay: reconcile held deadlines: %w", err)
	}
	return nil
}

// podsByInstance buckets the Component's pods by the Instance index they
// are labelled with.
func (d *driver) podsByInstance(ctx context.Context) (map[int32][]*corev1.Pod, error) {
	pods, err := query.ListOMENativePodsByName(ctx, d.cli, d.opts.Namespace, d.opts.OwnerName, d.opts.Component, false)
	if err != nil {
		return nil, fmt.Errorf("replay: list pods for deadline parking: %w", err)
	}
	byIndex := make(map[int32][]*corev1.Pod, len(pods))
	for _, pod := range pods {
		index, err := instanceIndexOf(pod)
		if err != nil {
			return nil, err
		}
		byIndex[index] = append(byIndex[index], pod)
	}
	return byIndex, nil
}

func (d *driver) buildInput(desired types.WorkloadDesiredSpec, observed types.WorkloadObservedState) (types.ReconcileInput, error) {
	requeueOperation, err := ParseDuration("config.requeueOperation", d.cfg.RequeueOperation)
	if err != nil {
		return types.ReconcileInput{}, err
	}
	requeueGate, err := ParseDuration("config.requeueGate", d.cfg.RequeueGate)
	if err != nil {
		return types.ReconcileInput{}, err
	}
	scaleDown, err := ParseDuration("config.scaleDownRequeueInterval", d.cfg.ScaleDownRequeueInterval)
	if err != nil {
		return types.ReconcileInput{}, err
	}
	stuckPodGrace, err := ParseDuration("config.stuckPodGrace", d.cfg.StuckPodGrace)
	if err != nil {
		return types.ReconcileInput{}, err
	}
	unschedulableGrace, err := ParseDuration("config.unschedulableGrace", d.cfg.UnschedulableGrace)
	if err != nil {
		return types.ReconcileInput{}, err
	}

	input := types.ReconcileInput{
		OwnerObject:              d.owner,
		OwnerGVK:                 v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"),
		EventTarget:              d.owner,
		Key:                      d.key(),
		DesiredSpec:              desired,
		ObservedState:            observed,
		ScaleDownRequeueInterval: scaleDown,
		StuckPodGrace:            stuckPodGrace,
		UnschedulableGrace:       unschedulableGrace,
		Requeue:                  types.RequeueIntervals{Operation: requeueOperation, Gate: requeueGate},
		Gangs:                    types.NewGangObservations(),
		Teardown:                 d.teardown,
		Clock:                    d.clock,
		Disposition: types.DispositionDeps{
			PodSpec:                desired.PodSpec,
			AutoMigrateMaxAttempts: d.cfg.AutoMigrateBudget,
		},
	}
	if policy := d.cfg.UpdateRetry; policy != nil {
		initial, err := ParseDuration("config.updateRetry.initialDelay", policy.InitialDelay)
		if err != nil {
			return types.ReconcileInput{}, err
		}
		maxDelay, err := ParseDuration("config.updateRetry.maxDelay", policy.MaxDelay)
		if err != nil {
			return types.ReconcileInput{}, err
		}
		input.UpdateRetryPolicy = &types.RetryPolicy{
			MaxAttempts:  policy.MaxAttempts,
			InitialDelay: initial,
			MaxDelay:     maxDelay,
			Multiplier:   policy.Multiplier,
		}
	}
	if policy := d.cfg.ForceDelete; policy != nil {
		slack, err := ParseDuration("config.forceDelete.overdueSlack", policy.OverdueSlack)
		if err != nil {
			return types.ReconcileInput{}, err
		}
		threshold, err := ParseDuration("config.forceDelete.nodeUnreachableThreshold", policy.NodeUnreachableThreshold)
		if err != nil {
			return types.ReconcileInput{}, err
		}
		input.ForceDelete = &types.ForceDeletePolicy{OverdueSlack: slack, NodeUnreachableThreshold: threshold}
	}
	if policy := d.cfg.MigrationAudit; policy != nil {
		window, err := ParseDuration("config.migrationAudit.window", policy.Window)
		if err != nil {
			return types.ReconcileInput{}, err
		}
		input.MigrationAudit = &types.MigrationAuditPolicy{
			MaxInFlight:  policy.MaxInFlight,
			MaxPerWindow: policy.MaxPerWindow,
			Window:       window,
		}
	}

	d.store.Install(&input)
	d.wrapCallbacks(&input)
	return input, nil
}

// wrapCallbacks records every seam the engine reaches back through, so a
// trace shows what the engine asked of its caller as well as what it
// wrote.
func (d *driver) wrapCallbacks(input *types.ReconcileInput) {
	// A row that really left the owner status takes its expectation with
	// it: the adapter forgets the entry so a later index reusing the slot
	// does not inherit a create or delete nobody is waiting for.
	input.RemoveInstance = func(ctx context.Context, idx int32) (bool, error) {
		removed, err := d.store.RemoveInstance(ctx, idx)
		if removed {
			d.expectations.Forget(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, idx)
		}
		return removed, err
	}
	input.WriteAggregateCondition = func(_ context.Context, cond metav1.Condition) error {
		d.trace.callback("condition type=%s status=%s reason=%s message=%s",
			cond.Type, cond.Status, cond.Reason, d.norm.text(cond.Message))
		return nil
	}
	input.WarnInstanceFailed = func(idx int32, podName, reason string) {
		d.trace.callback("warn instanceFailed index=%d pod=%s reason=%s", idx, orNil(podName), d.norm.text(reason))
	}
	input.WarnRetryHeld = func(targetRevision string, attempts int32, reason string) {
		d.trace.callback("warn retryHeld target=%s attempts=%d reason=%s",
			targetRevision, attempts, d.norm.text(reason))
	}
	input.RecordRolloutHold = func(hold *types.RolloutHold) {
		if hold == nil {
			d.trace.callback("hold cleared")
			return
		}
		d.trace.callback("hold gate=%s target=%s reason=%s",
			hold.Gate, hold.Target, d.norm.text(hold.Reason))
	}
	// The owner's migration records are real state: the executor resumes
	// from the phase it last persisted, so a driver that dropped the write
	// would replay a migration that never advances.
	input.MutateMigration = func(_ context.Context, requestUUID string, mutate func(*types.MigrationRecord) bool) error {
		position := -1
		for i := range d.migrations {
			if d.migrations[i].RequestUUID == requestUUID {
				position = i
				break
			}
		}
		if position < 0 {
			// A trimmed or never-created record is a clean no-op, and the
			// callback is deliberately not invoked: a stamper must not
			// resurrect a record as a phantom.
			d.trace.callback("migration uuid=%s absent", d.norm.uid(requestUUID))
			return nil
		}
		before := d.migrations[position]
		next := cloneMigrationRecord(before)
		if !mutate(&next) {
			d.trace.callback("migration uuid=%s no-op", d.norm.uid(requestUUID))
			return nil
		}
		d.migrations[position] = next
		d.trace.callback("migration uuid=%s write %s", d.norm.uid(requestUUID), d.migrationDiff(&before, &next))
		return nil
	}
	input.AppendMigration = func(_ context.Context, rec types.MigrationRecord) error {
		for i := range d.migrations {
			if d.migrations[i].RequestUUID == rec.RequestUUID {
				d.trace.callback("migration uuid=%s append-existing", d.norm.uid(rec.RequestUUID))
				return nil
			}
		}
		d.migrations = append(d.migrations, rec)
		d.trace.callback("migration uuid=%s append %s", d.norm.uid(rec.RequestUUID), d.migrationDiff(nil, &rec))
		return nil
	}
	input.UpdateGate = func(strategy types.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, types.RolloutHoldGate, string) {
		d.trace.callback("gate consult strategy=%s surge=%d unavail=%d verdict=allow", strategy, inFlightSurge, inFlightUnavail)
		return true, "", ""
	}
}

// cloneMigrationRecord isolates a record from the store so a mutation
// cannot reach back through a shared pointer.
func cloneMigrationRecord(rec types.MigrationRecord) types.MigrationRecord {
	out := rec
	if rec.SurgeInstance != nil {
		surge := *rec.SurgeInstance
		out.SurgeInstance = &surge
	}
	if rec.AllocatedAt != nil {
		at := *rec.AllocatedAt
		out.AllocatedAt = &at
	}
	if rec.CompletedAt != nil {
		at := *rec.CompletedAt
		out.CompletedAt = &at
	}
	if rec.Succeeded != nil {
		ok := *rec.Succeeded
		out.Succeeded = &ok
	}
	out.HintTargetNodes = append([]string(nil), rec.HintTargetNodes...)
	return out
}

// migrationDiff renders the fields one migration write changed. A nil
// before renders the whole record, for an append.
func (d *driver) migrationDiff(before, after *types.MigrationRecord) string {
	fields := func(rec *types.MigrationRecord) map[string]string {
		return map[string]string{
			"trigger":     string(rec.Trigger),
			"phase":       string(rec.Phase),
			"source":      fmt.Sprint(rec.SourceInstance),
			"surge":       indexOrNil(rec.SurgeInstance),
			"fromNode":    orNil(rec.FromNode),
			"attempt":     fmt.Sprint(rec.Attempt),
			"reason":      d.norm.text(orNil(rec.Reason)),
			"message":     d.norm.text(orNil(rec.Message)),
			"allocatedAt": d.norm.atMeta(rec.AllocatedAt),
			"deadline":    d.norm.at(rec.Deadline.Time),
			"completedAt": d.norm.atMeta(rec.CompletedAt),
		}
	}
	order := []string{"trigger", "phase", "source", "surge", "fromNode", "attempt", "reason", "message", "allocatedAt", "deadline", "completedAt"}
	to := fields(after)
	if before == nil {
		parts := make([]string, 0, len(order))
		for _, key := range order {
			parts = append(parts, key+"="+to[key])
		}
		return strings.Join(parts, " ")
	}
	from := fields(before)
	var parts []string
	for _, key := range order {
		if from[key] != to[key] {
			parts = append(parts, fmt.Sprintf("%s=%s→%s", key, from[key], to[key]))
		}
	}
	if len(parts) == 0 {
		return "-"
	}
	return strings.Join(parts, " ")
}

func renderResult(result ctrl.Result, err error) string {
	parts := []string{}
	switch {
	case result.RequeueAfter > 0:
		parts = append(parts, "after:"+result.RequeueAfter.String())
	case result.Requeue: //nolint:staticcheck // the bare backoff is a distinct engine outcome
		parts = append(parts, "requeue")
	default:
		parts = append(parts, "none")
	}
	if err != nil {
		parts = append(parts, "err:"+err.Error())
	}
	return joinFields(parts)
}

func joinFields(parts []string) string {
	out := ""
	for i, p := range parts {
		if i > 0 {
			out += " "
		}
		out += p
	}
	return out
}

func boolPtr(v bool) *bool { return &v }
