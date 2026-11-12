package replay

import (
	"context"
	"fmt"
	"regexp"
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
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/escalation"
	workloadops "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/ops"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/revision"
	workloadservice "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/service"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/status"
	types "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils"
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
	// Observer, when set, receives a read-only view of every tick a pass
	// ran in, after the pass and the deadline park/rearm step. It writes
	// nothing, so a trace is the same with or without it.
	Observer TickObserver
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
	// quota is the namespace's pod-count quota the run enforces, nil when
	// the scenario names none.
	quota *QuotaSpec
	// collisionCount salts the revision hash; the driver never bumps it,
	// but the engine's hash inputs include it.
	collisionCount *int32
	// seedWithoutWidth mints the revisions the run starts on without a
	// worker count, so the run begins on revisions that record no width; set
	// while a scenario that asks for it seeds its rows and pods.
	seedWithoutWidth bool

	currentRevision string
	// recordedTarget is the spec target the last tick reported as the
	// owner's update revision, which is what the owner's status names.
	recordedTarget string

	// quiet suppresses effect recording while the driver itself is
	// staging a scenario event.
	quiet bool
	// podUIDs numbers the identities the driver assigns on admission.
	podUIDs int
	// terminating holds, by pod name, the deletions the driver is keeping
	// open — the instant each landed on the injected clock, and whether a
	// dead kubelet is what keeps the object from leaving.
	terminating map[string]*podTeardown
	// goneNodes names the Nodes a scenario has deleted under a bound pod.
	// The fake cluster holds no Node until a scenario names one, so the
	// engine's reading of which Nodes exist is answered from this set: a
	// node the scenario never deleted is present, whatever the cluster
	// holds, and one it deleted is missing until a scenario writes it again.
	goneNodes map[string]bool
	// migrations is the owner's persisted status.migrations list.
	migrations []types.MigrationRecord
	// published is what the status publication after the previous pass
	// wrote onto each row from that pass's pod observation; the engine
	// reads it on the rows at the start of the next pass.
	published map[int32]publishedCounters
	// desiredPodsByInstance is the pass's desired pod count per Instance,
	// which the stand-in group gate counts serving Instances against.
	desiredPodsByInstance map[int32]int32
	// migrationRequests counts the requests the scenario has made, so an
	// unnamed one gets a stable identity.
	migrationRequests int
	// teardown marks every pass from the owner's deletion onward: the
	// planned index set reads as empty and every Instance is a
	// scale-down extra. teardownAt is the owner's deletion instant, the
	// anchor of its teardown deadline.
	teardown   bool
	teardownAt time.Time
	// ownerGone marks the owner purged past its teardown deadline: no
	// object is left for a pass to act on.
	ownerGone bool
	// usesStaleReads marks a scenario that stages ctrl.staleRead, which is
	// what makes the driver snapshot the cluster at the end of every tick;
	// staleCache is that snapshot, staleCacheTick the tick it closed, and
	// stalePass marks the pass reading its cache from it.
	usesStaleReads bool
	staleCache     client.Reader
	staleCacheTick int
	stalePass      bool
	// rollbackImage pins the roll to the stored revision that image's
	// template minted. Empty rolls toward the rendered template.
	rollbackImage string
	// pairingHolds is the pairing drain gate's standing answer, by pod
	// name: the reason a drain question naming the pod is held with. Nil
	// until a scenario stages a gate verdict; the drain gate is wired only
	// then, so a scenario that stages none sees no gate, as before.
	pairingHolds map[string]string
	// startHold is a start-time gate's standing answer for every fresh
	// start the update gate is asked about: the gate and the reason a
	// scenario staged with gate.deny[<gate>] and no pod. Empty while none
	// is staged; a start the engine exempts from the consult never meets
	// it.
	startHold startTimeHold
	// tickHolds, tickCommits, tickRetryCommits and tickMigrationWrites are
	// the rollout holds the pass recorded and the row, retry-block and
	// migration record writes resolved in the current tick, kept for the
	// observer.
	tickHolds           []types.RolloutHold
	tickCommits         []RowCommit
	tickRetryCommits    []RetryBlockCommit
	tickMigrationWrites []MigrationCommit
	// tickPodWrites are the engine's pod writes of the current tick, in
	// order, each with the pod as stored once the write landed.
	tickPodWrites []PodWrite
	// passClocks is the clock reading of every pass that ran, in order:
	// the instants the engine can have stamped an expectation at, since
	// the clock does not move inside a pass. ttl is the expectation
	// cache's time-to-live once a scenario has asked for it.
	passClocks []time.Time
	// budgetEpochs are the rolling-update budgets the spec has carried, in
	// Instances, one entry per change, for the budgets-held-to reading the
	// observer's view carries.
	budgetEpochs []budgetEpoch
	ttl          time.Duration
}

// commitLog is the row sink the store writes through: every resolved write
// is rendered into the trace and kept for the tick's observer.
type commitLog struct {
	d *driver
}

func (l *commitLog) RowCommitted(commit RowCommit) {
	l.d.tickCommits = append(l.d.tickCommits, commit)
	l.d.trace.RowCommitted(commit)
}

func (l *commitLog) RetryBlockCommitted(commit RetryBlockCommit) {
	l.d.tickRetryCommits = append(l.d.tickRetryCommits, commit)
	l.d.trace.RetryBlockCommitted(commit)
}

func (l *commitLog) StatusConflict(conflict StatusConflict) {
	l.d.trace.StatusConflict(conflict)
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
		goneNodes:    map[string]bool{},
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

	if err := d.seedQuota(ctx, s.Initial.Quota); err != nil {
		return nil, err
	}
	if err := d.seedMigrations(s.Initial.Migrations); err != nil {
		return nil, err
	}
	d.seedWithoutWidth = s.Initial.RevisionsWithoutWidth
	rows, err := d.seedRows(ctx, s.Initial.Rows)
	if err != nil {
		return nil, err
	}
	blocks, err := d.seedRetryBlocks(ctx, s.Initial.RetryBlocks)
	if err != nil {
		return nil, err
	}
	d.store = NewRowStore(rows, blocks, &commitLog{d: d})
	d.store.SetOwner(d.owner.UID, d.owner.Generation)

	if d.currentRevision, err = d.revisionFor(ctx, s.Initial.CurrentRevision); err != nil {
		return nil, err
	}
	if err := d.seedPods(ctx, s.Initial.Pods); err != nil {
		return nil, err
	}
	d.seedWithoutWidth = false
	for _, tick := range s.Timeline {
		for _, ev := range tick.Events {
			if ev.ID == "ctrl.staleRead" {
				d.usesStaleReads = true
			}
		}
	}
	if err := d.refreshStaleCache(ctx, 0); err != nil {
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
		Namespace: d.opts.Namespace,
		Component: d.opts.Component,
		OwnerName: d.opts.OwnerName,
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

// podMeta renders the pod template's own labels and annotations, nil when
// the scenario declares none so the revision keeps its metadata-free
// payload.
func (d *driver) podMeta() *metav1.ObjectMeta {
	if len(d.spec.PodLabels) == 0 && len(d.spec.PodAnnotations) == 0 {
		return nil
	}
	return &metav1.ObjectMeta{Labels: copyMap(d.spec.PodLabels), Annotations: copyMap(d.spec.PodAnnotations)}
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
	if d.spec.MaxUnavailable != "" || d.spec.MaxSurge != "" || d.spec.Partition != nil {
		rolling := &types.RollingUpdate{}
		if d.spec.Partition != nil {
			partition := *d.spec.Partition
			rolling.Partition = &partition
		}
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
		PodTemplateObjectMeta:   d.podMeta(),
		Lifecycle:               lifecycle,
		Paused:                  d.spec.Paused,
		PauseFreeze:             d.spec.PauseFreeze,
		GangSchedulingAvailable: d.spec.GangScheduling,
	}
	if d.spec.CanaryPartition != nil {
		partition := *d.spec.CanaryPartition
		desired.Pacing = &types.WorkloadPacing{Partition: &partition}
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

// template is what the adapter hashes for an image: the leader template and
// its metadata, and for a gang the worker template beside it, the topology
// key and the worker count.
func (d *driver) template(image string) revision.Template {
	t := revision.Template{PodSpec: d.podTemplate(image), Meta: d.podMeta()}
	if d.spec.Workers > 0 {
		t.WorkerPodSpec = d.podTemplate(image)
		t.TopologyKey = d.spec.TopologyKey
		if !d.seedWithoutWidth {
			t.WorkerSize = d.spec.Workers
		}
	}
	return t
}

// observedWorkerWidth reads the worker count the seeded gangs run on a
// revision, from the pods the fake cluster holds and only while a row is
// Ready on it; before the rows are seeded nothing is observed.
func (d *driver) observedWorkerWidth(ctx context.Context) func(string) (int32, bool) {
	return func(revisionName string) (int32, bool) {
		if d.store == nil || !revision.HasReadyRowOn(d.store.Rows(), revisionName) {
			return 0, false
		}
		pods, err := query.ListOMENativePodsByName(ctx, d.cli, d.opts.Namespace, d.opts.OwnerName, d.opts.Component, false)
		if err != nil {
			return 0, false
		}
		return revision.ObservedWorkerWidth(d.store.Rows(), pods, revisionName)
	}
}

// ensureRevision mints (or reuses) the ControllerRevision for an image,
// exactly as the adapter does before every reconcile.
func (d *driver) ensureRevision(ctx context.Context, image string) (*appsv1.ControllerRevision, error) {
	key := revision.Key{
		Namespace: d.opts.Namespace,
		Name:      d.opts.OwnerName + "-" + string(d.opts.Component),
		Labels:    d.selectorLabels(),
	}
	tmpl := d.template(image)
	hash, raw, err := revision.HashTemplate(tmpl, d.collisionCount, parentUID)
	if err != nil {
		return nil, fmt.Errorf("replay: hash revision: %w", err)
	}
	// As the adapter does: a revision minted without a width stays the
	// target while its gangs run the desired width.
	twin, _, err := revision.WidthFreeTwinOf(tmpl, d.collisionCount, parentUID)
	if err != nil {
		return nil, fmt.Errorf("replay: hash width-free revision: %w", err)
	}
	// The owner is a projected replica, so its history names the service
	// that controls it, as the adapter's does.
	scope := revision.ScopeOwner(d.owner, v1beta1.SchemeGroupVersion.WithKind("InferenceService").GroupKind())
	adopted, err := revision.AdoptRevisionWithoutWidth(ctx, d.cli, d.cli, d.owner, scope, key, hash, twin, tmpl.WorkerSize, d.recordedTarget, d.observedWorkerWidth(ctx))
	if err != nil {
		return nil, fmt.Errorf("replay: adopt revision minted without a width: %w", err)
	}
	if adopted != nil {
		return adopted, nil
	}
	target, collision, err := revision.EnsureControllerRevisionFromHash(
		ctx, d.cli, d.cli, d.owner, v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"),
		key, hash, raw, scope,
	)
	if err != nil {
		return nil, fmt.Errorf("replay: ensure revision: %w", err)
	}
	if collision {
		return nil, fmt.Errorf("replay: revision hash collision on image %q", image)
	}
	// The apiserver sets creationTimestamp on admission and the fake
	// client does not. The engine reads a revision's age against a row's
	// readySince, so a revision minted without one would read as older
	// than every promote.
	if target.CreationTimestamp.IsZero() {
		target.CreationTimestamp = metav1.NewTime(d.clock.Now())
		if err := d.staging(func() error { return d.cli.Update(ctx, target) }); err != nil {
			return nil, fmt.Errorf("replay: set revision creation time: %w", err)
		}
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

// revisionFor resolves a scenario's revision name to the ControllerRevision
// it mints: "current" is the initial spec's template, an image reference
// is that image's template, and empty is no revision.
func (d *driver) revisionFor(ctx context.Context, name string) (string, error) {
	image, err := revisionImage(name, d.spec.Image)
	if err != nil || image == "" {
		return "", err
	}
	rev, err := d.ensureRevision(ctx, image)
	if err != nil {
		return "", err
	}
	return rev.Name, nil
}

// revisionImage maps a revision name to the image whose template mints
// it. An image reference is recognized by its registry path or tag.
func revisionImage(name, current string) (string, error) {
	switch {
	case name == "":
		return "", nil
	case name == "current":
		return current, nil
	case strings.ContainsAny(name, "/:"):
		return name, nil
	}
	return "", fmt.Errorf("replay: revision %q: name a revision as \"current\" or as the image whose template minted it", name)
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
		if row.TargetRevision, err = d.revisionFor(ctx, spec.TargetRevision); err != nil {
			return nil, err
		}
		if spec.ReadySince != "" {
			at, err := d.offset("rows.readySince", spec.ReadySince)
			if err != nil {
				return nil, err
			}
			stamp := metav1.NewTime(at)
			row.ReadySince = &stamp
		}
		if spec.Operation != nil {
			if row.Operation, err = d.seedOperation(ctx, spec, row); err != nil {
				return nil, err
			}
		}
		// The engine's own ownership table (types.StateOf) decides whether the row is a state
		// of the machine; a shape it does not name is a seed nothing drives.
		if types.StateOf(&row) == types.StateUnknown {
			return nil, fmt.Errorf("replay: rows[%d]: phase %q with operation %s names no state of the instance machine",
				spec.Index, spec.Phase, describeOperation(row.Operation))
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// seedOperation builds the operation a row resumes from, with the
// identity and timing fields the engine's own stamps write.
func (d *driver) seedOperation(ctx context.Context, spec RowSpec, row types.InstanceStatus) (*types.InstanceOperation, error) {
	o := spec.Operation
	field := fmt.Sprintf("rows[%d].operation", spec.Index)
	if o.Type == "" || o.Step == "" {
		return nil, fmt.Errorf("replay: %s: an operation names its type and step", field)
	}
	if spec.Phase == "" {
		return nil, fmt.Errorf("replay: %s: an operation needs the phase the row carries it in", field)
	}
	startedAt, err := d.offset(field+".startedAt", o.StartedAt)
	if err != nil {
		return nil, err
	}
	lastProgressAt := startedAt
	if o.LastProgressAt != "" {
		if lastProgressAt, err = d.offset(field+".lastProgressAt", o.LastProgressAt); err != nil {
			return nil, err
		}
	}
	target, err := d.revisionFor(ctx, o.TargetRevision)
	if err != nil {
		return nil, err
	}
	op := &types.InstanceOperation{
		Type:           types.InstanceOperationType(o.Type),
		Step:           o.Step,
		TargetRevision: target,
		Strategy:       types.UpdateStrategyType(o.Strategy),
		RequestUUID:    o.RequestUUID,
		Reason:         o.Reason,
		Waiting:        o.Waiting,
		RetryCount:     o.RetryCount,
		StartedAt:      metav1.NewTime(startedAt),
		LastProgressAt: metav1.NewTime(lastProgressAt),
	}
	if o.SurgeIndex != nil {
		sibling := *o.SurgeIndex
		op.SurgeIndex = &sibling
	}
	if err := d.seedClocks(field, spec, op); err != nil {
		return nil, err
	}
	switch op.Type {
	case types.InstanceOperationUpdate:
		if op.TargetRevision == "" {
			op.TargetRevision = row.TargetRevision
		}
		if op.TargetRevision == "" {
			return nil, fmt.Errorf("replay: %s: an Update operation converges toward a target revision", field)
		}
	case types.InstanceOperationMigrate:
		if op.RequestUUID == "" || op.SurgeIndex == nil {
			return nil, fmt.Errorf("replay: %s: a Migrate pin carries the request uuid and the paired index", field)
		}
		if types.FindMigrationRecord(d.migrations, op.RequestUUID) == nil {
			return nil, fmt.Errorf("replay: %s: no initial.migrations record carries uuid %q", field, op.RequestUUID)
		}
	}
	op.ID = seededOperationID(op, spec.Index, types.InstancePhase(spec.Phase))
	return op, nil
}

// seedClocks sets the clocks a seeded operation carries: the deadline and
// the placement wait. An attempt in flight carries the scenario's
// deadline, or startedAt plus the spec's instanceReadyTimeout as the
// engine derives it, and the placement wait the scenario names. A Failed
// row keeps its operation with both cleared, as the engine writes it
// (status.keepOperationWithoutDeadline): the row is outside every pass
// that reads the clock, and the attempt that starts again writes its own.
// A scenario that sets either on a Failed row asks for a shape the engine
// never writes, and fails the run.
func (d *driver) seedClocks(field string, spec RowSpec, op *types.InstanceOperation) error {
	o := spec.Operation
	if types.InstancePhase(spec.Phase) == types.InstancePhaseFailed {
		if o.Deadline != "" || o.PlacementWaitSince != "" {
			return fmt.Errorf("replay: %s: a Failed row keeps its operation without a deadline or a placement wait, as the engine writes it", field)
		}
		return nil
	}
	if o.Deadline != "" {
		at, err := d.offset(field+".deadline", o.Deadline)
		if err != nil {
			return err
		}
		op.Deadline = metav1.NewTime(at)
	} else {
		timeout, err := ParseDuration("spec.instanceReadyTimeout", d.spec.InstanceReadyTimeout)
		if err != nil {
			return err
		}
		op.Deadline = types.DeadlineAt(op.StartedAt, timeout)
	}
	if o.PlacementWaitSince != "" {
		at, err := d.offset(field+".placementWaitSince", o.PlacementWaitSince)
		if err != nil {
			return err
		}
		since := metav1.NewTime(at)
		op.PlacementWaitSince = &since
	}
	return nil
}

// seededOperationID mints the id in the shape the engine's own stamp for
// the operation would have minted it, so a resumed operation reads in a
// trace exactly as one the engine opened.
func seededOperationID(op *types.InstanceOperation, index int32, phase types.InstancePhase) string {
	unix := op.StartedAt.Unix()
	switch {
	case op.Type == types.InstanceOperationMigrate && phase == types.InstancePhaseCreating:
		return fmt.Sprintf("migrate-%s-surge-%d", op.RequestUUID, unix)
	case op.Type == types.InstanceOperationMigrate:
		return fmt.Sprintf("migrate-%s-%d", op.RequestUUID, unix)
	case op.Step == types.UpdateStepGangSurgeTarget || op.Step == types.UpdateStepGangSurgeTargetCleanup:
		return fmt.Sprintf("gangsurgetarget-%d-%d", index, unix)
	}
	return fmt.Sprintf("%s-%d-%d", strings.ToLower(string(op.Type)), index, unix)
}

func describeOperation(op *types.InstanceOperation) string {
	if op == nil {
		return "nil"
	}
	return string(op.Type) + "/" + op.Step
}

// seedMigrations records the owner's status.migrations entries, with the
// fields the request's acceptance and the surge allocation write.
func (d *driver) seedMigrations(specs []MigrationSpec) error {
	timeout, err := ParseDuration("spec.instanceReadyTimeout", d.spec.InstanceReadyTimeout)
	if err != nil {
		return err
	}
	for i, spec := range specs {
		field := fmt.Sprintf("migrations[%d]", i)
		d.migrationRequests++
		uuid := spec.UUID
		if uuid == "" {
			uuid = fmt.Sprintf("migration-%d", d.migrationRequests)
		}
		if types.FindMigrationRecord(d.migrations, uuid) != nil {
			return fmt.Errorf("replay: %s: uuid %s is already recorded", field, uuid)
		}
		startedAt, err := d.offset(field+".startedAt", spec.StartedAt)
		if err != nil {
			return err
		}
		phase := types.MigrationPhase(spec.Phase)
		if phase == "" {
			phase = types.MigrationPhaseAccepted
			if spec.SurgeIndex != nil {
				phase = types.MigrationPhaseSurgePending
			}
		}
		started := metav1.NewTime(startedAt)
		record := types.MigrationRecord{
			RequestUUID:    uuid,
			Trigger:        types.MigrationTriggerManual,
			SourceInstance: spec.Instance,
			FromNode:       spec.FromNode,
			Reason:         spec.Reason,
			Message:        spec.Message,
			Phase:          phase,
			StartedAt:      started,
			Deadline:       types.DeadlineAt(started, timeout),
		}
		if spec.SurgeIndex != nil {
			surge := *spec.SurgeIndex
			record.SurgeInstance = &surge
			allocatedAt := startedAt
			if spec.AllocatedAt != "" {
				if allocatedAt, err = d.offset(field+".allocatedAt", spec.AllocatedAt); err != nil {
					return err
				}
			}
			allocated := metav1.NewTime(allocatedAt)
			record.AllocatedAt = &allocated
		}
		d.migrations = append(d.migrations, record)
	}
	return nil
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
		labelRev := rev
		if spec.LabelImage != "" {
			if labelRev, err = d.ensureRevision(ctx, spec.LabelImage); err != nil {
				return err
			}
		}
		pod, err := d.renderPod(spec, labelRev)
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
		if spec.Terminating {
			if err := d.staging(func() error { return d.cli.Delete(ctx, pod) }); err != nil {
				return fmt.Errorf("replay: seed terminating pod %s: %w", pod.Name, err)
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
		d.podTemplate(image), d.podMeta(), plan, inst, runner, spec.Ordinal,
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
	d.store.BeginPass()
	d.tickHolds, d.tickCommits, d.tickRetryCommits, d.tickMigrationWrites, d.tickPodWrites = nil, nil, nil, nil, nil
	for _, ev := range tick.Events {
		if err := d.apply(ctx, ev); err != nil {
			return err
		}
	}
	// The adapter's teardown pass checks the deadline before it dispatches
	// anything, and once the owner is gone a reconcile finds no object and
	// runs no pass. Both ticks are still handed to the observer: the rows
	// are the slots the owner left empty, and an event staged in such a
	// tick is one the table says lands on an Empty row.
	if d.ownerGone {
		d.trace.callback("owner gone; nothing reconciles")
		d.trace.emit("pass result=none")
		return d.observeWithoutPass(ctx, tick.Tick)
	}
	overdue, err := d.teardownOverdue()
	if err != nil {
		return err
	}
	if overdue {
		return d.releaseTeardown(ctx, tick.Tick)
	}

	desired, err := d.desiredSpec()
	if err != nil {
		return err
	}
	specTarget, err := d.ensureRevision(ctx, d.spec.Image)
	if err != nil {
		return err
	}
	d.recordedTarget = specTarget.Name
	// An owner that never reported a rolled-out revision reports the one
	// the first tick's spec renders, as a freshly created owner does.
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
		if payload.WorkerSize != nil {
			desired.Runners = types.RunnersWithWorkerSize(desired.Runners, *payload.WorkerSize)
		}
		target = pinned
	}

	rows, podObservation, err := d.publishedRows(ctx)
	if err != nil {
		return err
	}
	observed := types.WorkloadObservedState{
		CurrentRevision:  d.currentRevision,
		UpdateRevision:   specTarget.Name,
		RollTarget:       target.Name,
		InstanceStatuses: rows,
		RetryBlocks:      d.store.RetryBlocks(),
		Migrations:       append([]types.MigrationRecord(nil), d.migrations...),
	}
	// As the adapter does: each Instance renders the width its own revision
	// recorded, read from the revisions the rows reference beside the target.
	observed.WorkerSizeByRevision, err = revision.WorkerSizesByRevision(ctx, d.cli, d.opts.Namespace,
		revision.ReferencedRevisionNames(observed.InstanceStatuses, target.Name))
	if err != nil {
		return fmt.Errorf("replay: read revision widths: %w", err)
	}
	if observed.CordonedInstances, err = d.cordonedInstances(ctx, desired, observed); err != nil {
		return err
	}
	plan, err := workload.BuildPlan(d.opts.Component, desired, observed)
	if err != nil {
		return fmt.Errorf("replay: build plan: %w", err)
	}
	d.desiredPodsByInstance = status.DesiredPodCountByInstance(plan)

	input, err := d.buildInput(ctx, desired, observed, target)
	if err != nil {
		return err
	}
	// The engine's two readers: the cache, which a staged stale read
	// serves from the previous tick, and the live reader, always current.
	// The cache is read through the pass's cached pod list, as the adapter
	// wires it: the pods are listed once per pass and again after a pod
	// write. The Nodes are answered from the scenario's own record of the
	// ones it deleted (goneNodes), since the cluster holds none it did not
	// name.
	cachedPods := types.NewCachedPodList(d.cachedClient())
	input.CachedPods = cachedPods
	deps := types.Deps{
		Client:       cachedPods.Client(),
		APIReader:    d.cli,
		Recorder:     d.recorder,
		Expectations: d.expectations,
		Clock:        d.clock,
		NodePresence: scenarioNodes{gone: d.goneNodes},
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
		input.ObservedState.InstanceStatuses = d.overlayPublished(d.store.Rows())
		input.ObservedState.Migrations = append([]types.MigrationRecord(nil), d.migrations...)
	}
	d.notePassClock()
	result, rerr := workload.Reconcile(ctx, deps, input, plan, target)
	d.trace.emit("pass result=%s", renderResult(result, rerr))
	// The status publication that follows the pass counts this pass's pod
	// observation onto the rows the pass left; the next pass reads those
	// counters.
	var pending []int32
	if d.published, pending, err = d.publish(ctx, podObservation, plan, specTarget); err != nil {
		return err
	}
	// The status-aggregate pass runs after the dispatcher on every
	// reconcile: it parks the deadline of an Instance held by admission or
	// by an external wait, and rearms one that has been released.
	if err := d.reconcileHeldDeadlines(ctx, input, desired.Paused, plan.InstanceReadyTimeout); err != nil {
		return err
	}
	if unfired := d.store.UnfiredConflicts(); len(unfired) > 0 {
		return fmt.Errorf("replay: tick %d: api.conflict armed for status write %v, but the pass made no such write", tick.Tick, unfired)
	}
	if err := d.writePublishedPending(ctx, input, pending); err != nil {
		return err
	}
	d.stalePass = false
	if err := d.observe(ctx, tick.Tick, desired, input.StuckPodGrace); err != nil {
		return err
	}
	return d.refreshStaleCache(ctx, tick.Tick)
}

// notePassClock records the instant a pass runs at, once per distinct
// reading, for the expectation deadlines a later tick resolves.
func (d *driver) notePassClock() {
	now := d.clock.Now()
	if n := len(d.passClocks); n > 0 && d.passClocks[n-1].Equal(now) {
		return
	}
	d.passClocks = append(d.passClocks, now)
}

// observe hands the tick's end state to the observer, when one is set. Pods
// and rotation are read live, as the next pass will list them; the rows
// carry the counters the publication after this pass wrote; the migration
// records are copies of the owner's list; the grace is the one the pass's
// readers received.
func (d *driver) observe(ctx context.Context, tick int, desired types.WorkloadDesiredSpec, stuckPodGrace time.Duration) error {
	if d.opts.Observer == nil {
		return nil
	}
	pods, err := d.podsByInstanceFrom(ctx, d.cli)
	if err != nil {
		return err
	}
	routed, err := d.routedRotation(ctx)
	if err != nil {
		return err
	}
	rows := d.overlayPublished(d.store.Rows())
	d.recordBudgetEpoch(desired)
	spec := TickSpec{
		Replicas:        desired.Replicas,
		PodsPerInstance: podsPerInstance(desired),
		MinReadySeconds: desired.MinReadySeconds,
		Paused:          desired.Paused,
		Teardown:        d.teardown,
		HeldTo:          d.budgetsHeldTo(rows, pods, podsPerInstance(desired)),
	}
	if strategy := desired.Lifecycle.UpdateStrategy; strategy != nil && strategy.RollingUpdate != nil {
		spec.MaxUnavailable = strategy.RollingUpdate.MaxUnavailable
		spec.MaxSurge = strategy.RollingUpdate.MaxSurge
		spec.Partition = strategy.RollingUpdate.Partition
	}
	migrations := make([]types.MigrationRecord, 0, len(d.migrations))
	for _, record := range d.migrations {
		migrations = append(migrations, cloneMigrationRecord(record))
	}
	revisions, err := d.revisionTemplates(ctx)
	if err != nil {
		return err
	}
	d.opts.Observer(TickView{
		Tick:            tick,
		Now:             d.clock.Now(),
		Rows:            rows,
		RetryBlocks:     d.store.RetryBlocks(),
		Migrations:      migrations,
		Pods:            pods,
		Routed:          routed,
		Holds:           append([]types.RolloutHold(nil), d.tickHolds...),
		Commits:         append([]RowCommit(nil), d.tickCommits...),
		RetryCommits:    append([]RetryBlockCommit(nil), d.tickRetryCommits...),
		MigrationWrites: append([]MigrationCommit(nil), d.tickMigrationWrites...),
		Spec:            spec,
		StuckPodGrace:   stuckPodGrace,
		PodWrites:       append([]PodWrite(nil), d.tickPodWrites...),
		Revisions:       revisions,
	})
	return nil
}

// revisionTemplates reads every stored revision of the namespace, keyed by
// the hash a pod's revision label carries, as the label-truth invariant
// compares pods against them.
func (d *driver) revisionTemplates(ctx context.Context) (map[string]RevisionTemplate, error) {
	list := &appsv1.ControllerRevisionList{}
	if err := d.cli.List(ctx, list, client.InNamespace(d.opts.Namespace)); err != nil {
		return nil, fmt.Errorf("replay: list revisions for the observer: %w", err)
	}
	out := make(map[string]RevisionTemplate, len(list.Items))
	for i := range list.Items {
		cr := &list.Items[i]
		payload, err := revision.PayloadFromControllerRevision(cr)
		if err != nil {
			return nil, fmt.Errorf("replay: decode revision %s for the observer: %w", cr.Name, err)
		}
		if payload == nil {
			continue
		}
		template := RevisionTemplate{Images: imagesByContainer(payload.PodSpec)}
		if payload.WorkerPodSpec != nil {
			template.WorkerImages = imagesByContainer(payload.WorkerPodSpec)
		}
		if payload.PodMeta != nil {
			template.Labels = payload.PodMeta.Labels
			template.Annotations = payload.PodMeta.Annotations
		}
		out[query.RevisionHashFromControllerRevisionName(cr.Name)] = template
	}
	return out, nil
}

func imagesByContainer(spec *corev1.PodSpec) map[string]string {
	if spec == nil {
		return nil
	}
	out := make(map[string]string, len(spec.Containers))
	for _, container := range spec.Containers {
		out[container.Name] = container.Image
	}
	return out
}

// podsPerInstance is the pod count one Instance of the desired spec runs:
// the sizes of its runners added up.
func podsPerInstance(desired types.WorkloadDesiredSpec) int32 {
	var n int32
	for _, runner := range desired.Runners {
		n += runner.Size
	}
	return n
}

// teardownOverdue reports whether the deleted owner has held its finalizer
// past the configured teardown deadline. An unconfigured deadline holds
// strictly, as the adapter does.
func (d *driver) teardownOverdue() (bool, error) {
	if !d.teardown {
		return false, nil
	}
	deadline, err := ParseDuration("config.teardownDeadline", d.cfg.TeardownDeadline)
	if err != nil || deadline <= 0 {
		return false, err
	}
	return d.clock.Now().After(d.teardownAt.Add(deadline)), nil
}

// teardownDeadlineExceeded is the Warning the adapter's teardown pass
// emits when it releases a deleted owner's finalizer past its deadline.
const teardownDeadlineExceeded = "TeardownDeadlineExceeded"

// releaseTeardown is the adapter's deadline release, in the adapter's
// order: the Component's pods listed through the live reader for the
// Warning's count, the Warning itself, then the finalizer lifted, which
// deletes the owner object and every row and retry block it carried in
// one stroke. The store records each row's removal, so the trace shows
// the state every row left from and the observer sees the move into
// Empty. The pods stay, left to background GC the fake apiserver does not
// run.
func (d *driver) releaseTeardown(ctx context.Context, tick int) error {
	pods, err := query.LiveListPodsForComponent(ctx, d.cli, d.opts.Namespace, d.opts.OwnerName, d.opts.Component)
	if err != nil {
		return fmt.Errorf("replay: list pods for the teardown release: %w", err)
	}
	d.recorder.Eventf(d.owner, corev1.EventTypeWarning, teardownDeadlineExceeded,
		"teardown deadline %s exceeded (%d owned pod(s) observed; selector: parent=%q, component=%q); releasing the teardown finalizer to background GC",
		d.cfg.TeardownDeadline, len(pods), d.opts.OwnerName, d.opts.Component)
	removed := d.store.Purge()
	d.trace.callback("owner deleted rows=%d", len(removed))
	d.ownerGone = true
	d.trace.emit("pass result=none")
	return d.observeWithoutPass(ctx, tick)
}

// observeWithoutPass hands the observer a tick in which no workload pass
// ran: the one that released the owner past its teardown deadline, and
// every tick after it. The view is read the way a pass's is, from the
// spec the scenario holds and the pods the fake apiserver still lists.
func (d *driver) observeWithoutPass(ctx context.Context, tick int) error {
	if d.opts.Observer == nil {
		return nil
	}
	desired, err := d.desiredSpec()
	if err != nil {
		return err
	}
	stuckPodGrace, err := ParseDuration("config.stuckPodGrace", d.cfg.StuckPodGrace)
	if err != nil {
		return err
	}
	return d.observe(ctx, tick, desired, stuckPodGrace)
}

// reconcileHeldDeadlines is the adapter's deadline park/rearm step. An
// Instance is held when a pause covers it, when one of its pods is behind
// an admission scheduling gate, or when the surge bucket its operation
// points at is. Held parks the deadline; released rearms it from now. The
// open move records follow the pause the same way.
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
	clockInput := types.ReconcileInput{
		OwnerObject:             d.owner,
		OwnerGVK:                input.OwnerGVK,
		Key:                     input.Key,
		ObservedState:           types.WorkloadObservedState{InstanceStatuses: rows, Migrations: append([]types.MigrationRecord(nil), d.migrations...)},
		Clock:                   d.clock,
		MutateInstance:          d.store.MutateInstance,
		MutateMigration:         input.MutateMigration,
		RemoveInstance:          func(context.Context, int32) (bool, error) { return false, nil },
		WriteAggregateCondition: func(context.Context, metav1.Condition) error { return nil },
		WarnInstanceFailed:      func(int32, string, string) {},
	}
	if err := escalation.ReconcileGatedDeadlines(ctx, clockInput, rows, held, timeout); err != nil {
		return fmt.Errorf("replay: reconcile held deadlines: %w", err)
	}
	if err := escalation.ReconcilePausedMigrationDeadlines(ctx, clockInput, paused, timeout); err != nil {
		return fmt.Errorf("replay: reconcile the move records' deadlines around the pause: %w", err)
	}
	return nil
}

// podsByInstance buckets the Component's pods by the Instance index they
// are labelled with, as the engine's cache lists them.
func (d *driver) podsByInstance(ctx context.Context) (map[int32][]*corev1.Pod, error) {
	return d.podsByInstanceFrom(ctx, d.cachedClient())
}

// podsByInstanceFrom is podsByInstance read through the given reader.
func (d *driver) podsByInstanceFrom(ctx context.Context, reader client.Reader) (map[int32][]*corev1.Pod, error) {
	pods, err := query.ListOMENativePodsByName(ctx, reader, d.opts.Namespace, d.opts.OwnerName, d.opts.Component, false)
	if err != nil {
		return nil, fmt.Errorf("replay: list pods by instance: %w", err)
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

// publishedCounters are the counters of a row's status publication that a
// pass reads back: how many pods the Instance had, how many of them
// served, PodReady with the serving gate and not being deleted, and how
// many were available, in the headless Service's rotation and, under a
// positive minReadySeconds window, Ready for at least that long. The
// publication writes more onto a row (the ready and scheduled counts, the
// admitted flag, the nodes occupied); no pass reads those.
type publishedCounters struct {
	podCount          int32
	servingPodCount   int32
	availablePodCount int32
}

// publishedRows is the engine's row observation for this pass: the store's
// rows carrying the counters the status publication wrote from the
// previous pass's pod observation, as the adapter's rollup hands them to
// the next reconcile. It also returns this pass's pod observation, the
// selector-scoped cache read the publication after the pass counts from.
func (d *driver) publishedRows(ctx context.Context) ([]types.InstanceStatus, workload.PodObservation, error) {
	pods, err := query.ListOMENativePodsByName(ctx, d.cachedClient(), d.opts.Namespace, d.opts.OwnerName, d.opts.Component, false)
	if err != nil {
		return nil, workload.PodObservation{}, fmt.Errorf("replay: list pods for the publication: %w", err)
	}
	// The publication counts what the adapter's does: a pod bound to a
	// Node that no longer exists is gone for it too.
	pods, _, err = query.SplitPodsOnMissingNodes(ctx, scenarioNodes{gone: d.goneNodes}, pods)
	if err != nil {
		return nil, workload.PodObservation{}, fmt.Errorf("replay: read the nodes under the pods: %w", err)
	}
	return d.overlayPublished(d.store.Rows()), workload.NewCachedSelectorPodObservation(pods, nil), nil
}

// cordonedInstances is the adapter's reading of cordoned nodes, as
// inferencereplica.cordonedInstances makes it: nothing is read unless the
// rows outnumber the desired count, and then every Instance with a pod on
// a Node marked unschedulable is handed to the plan, which retires those
// first at a scale-down.
func (d *driver) cordonedInstances(ctx context.Context, desired types.WorkloadDesiredSpec, observed types.WorkloadObservedState) (map[int32]struct{}, error) {
	if int32(len(observed.InstanceStatuses)) <= desired.Replicas {
		return nil, nil
	}
	nodes, err := query.CordonedNodes(ctx, d.cachedClient())
	if err != nil {
		return nil, fmt.Errorf("replay: list cordoned nodes: %w", err)
	}
	if len(nodes) == 0 {
		return nil, nil
	}
	pods, err := query.ListOMENativePodsByName(ctx, d.cachedClient(), d.opts.Namespace, d.opts.OwnerName, d.opts.Component, false)
	if err != nil {
		return nil, fmt.Errorf("replay: list pods for the cordon reading: %w", err)
	}
	return query.CordonedInstances(query.BucketPodsByInstanceIdx(pods), nodes), nil
}

// scenarioNodes answers the engine's Node reading from the Nodes a
// scenario has deleted: the fake cluster holds no Node a scenario did not
// name, so absence from the cluster proves nothing, and only a staged
// pod.nodeLost[NodeGone] makes a node missing.
type scenarioNodes struct {
	gone map[string]bool
}

// NodeExists implements types.NodePresence.
func (n scenarioNodes) NodeExists(_ context.Context, name string) (bool, error) {
	return !n.gone[name], nil
}

// publish runs the adapter's status publication over the pass's pod
// observation and keeps the counters it wrote onto each row, which the
// next pass reads. The publisher is the product's own
// (workload.ComponentObservation.TakeInlineV1Publication), fed what the
// adapter feeds it: the rows the pass left, the headless Service's
// rotation and the Component's availability window. A published counter
// is therefore the number the adapter would persist, never a formula of
// the driver's. Of the phases the publication projects, the store keeps
// the one the engine reads back as written: a settled Ready row with no
// pod left that could back it publishes Pending, and the Pending row is
// what the next pass observes and the Create pass rebuilds. The other
// projected phases read back as the stored phase, so the store holds the
// engine's for them. The rows to write Pending are returned beside the
// counters.
func (d *driver) publish(ctx context.Context, pods workload.PodObservation, plan types.ComponentPlan, target *appsv1.ControllerRevision) (map[int32]publishedCounters, []int32, error) {
	headless := query.HeadlessServiceName(d.opts.OwnerName, d.opts.Component)
	availableByPod, err := status.AvailablePodSet(ctx, d.cachedClient(), d.opts.Namespace, headless)
	if err != nil {
		return nil, nil, fmt.Errorf("replay: publication rotation: %w", err)
	}
	stored := d.store.Rows()
	// The publication writes the projected phase onto the rows it is
	// handed, so which rows were settled Ready is read before it runs.
	settledReady := make(map[int32]bool, len(stored))
	for i := range stored {
		if status.DemotableReady(&stored[i]) {
			settledReady[stored[i].Index] = true
		}
	}
	observation, err := workload.NewOwnedPublicationObservation(stored, pods, availableByPod,
		status.AvailabilityWindow{MinReadySeconds: plan.MinReadySeconds, Now: d.clock.Now()})
	if err != nil {
		return nil, nil, fmt.Errorf("replay: build publication observation: %w", err)
	}
	var targetName string
	if target != nil {
		targetName = target.Name
	}
	rows, _, err := observation.TakeInlineV1Publication(status.DesiredPodCountByInstance(plan), targetName)
	if err != nil {
		return nil, nil, fmt.Errorf("replay: consume publication observation: %w", err)
	}
	published := make(map[int32]publishedCounters, len(rows))
	var pending []int32
	for i := range rows {
		published[rows[i].Index] = publishedCounters{
			podCount:          rows[i].PodCount,
			servingPodCount:   rows[i].ServingPodCount,
			availablePodCount: rows[i].AvailablePodCount,
		}
		if rows[i].Phase == types.InstancePhasePending && settledReady[rows[i].Index] {
			pending = append(pending, rows[i].Index)
		}
	}
	return published, pending, nil
}

// writePublishedPending is the adapter's status write of the one published
// phase the engine reads back as written: the settled Ready rows the
// publication found no pod left that could back are written Pending, each
// re-checked on the stored row as the adapter's retry re-reads it.
func (d *driver) writePublishedPending(ctx context.Context, input types.ReconcileInput, pending []int32) error {
	for _, idx := range pending {
		d.trace.emit("publication row=%d phase=Pending: no pod left that could back the Ready row", idx)
		if _, err := status.PendingForUnbackedReady(ctx, input, idx); err != nil {
			return fmt.Errorf("replay: write the published phase of row %d: %w", idx, err)
		}
	}
	return nil
}

// overlayPublished writes the published counters onto a row observation;
// the store's rows carry none, so nothing is rendered for them.
func (d *driver) overlayPublished(rows []types.InstanceStatus) []types.InstanceStatus {
	for i := range rows {
		counters := d.published[rows[i].Index]
		rows[i].PodCount = counters.podCount
		rows[i].ServingPodCount = counters.servingPodCount
		rows[i].AvailablePodCount = counters.availablePodCount
	}
	return rows
}

// retryPolicyOf converts the scenario's retry ladder to the engine's
// policy; an unset ladder is nil, which the engine reads as unconfigured.
func retryPolicyOf(field string, spec *RetrySpec) (*types.RetryPolicy, error) {
	if spec == nil {
		return nil, nil
	}
	initial, err := ParseDuration(field+".initialDelay", spec.InitialDelay)
	if err != nil {
		return nil, err
	}
	maxDelay, err := ParseDuration(field+".maxDelay", spec.MaxDelay)
	if err != nil {
		return nil, err
	}
	return &types.RetryPolicy{
		MaxAttempts:  spec.MaxAttempts,
		InitialDelay: initial,
		MaxDelay:     maxDelay,
		Multiplier:   spec.Multiplier,
	}, nil
}

func (d *driver) buildInput(ctx context.Context, desired types.WorkloadDesiredSpec, observed types.WorkloadObservedState, target *appsv1.ControllerRevision) (types.ReconcileInput, error) {
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
	grace, err := ParseDuration("config.stuckPodGrace", d.cfg.StuckPodGrace)
	if err != nil {
		return types.ReconcileInput{}, err
	}
	unschedulableGrace, err := ParseDuration("config.unschedulableGrace", d.cfg.UnschedulableGrace)
	if err != nil {
		return types.ReconcileInput{}, err
	}
	gangDomainFullGrace, err := ParseDuration("config.gangDomainFullGrace", d.cfg.GangDomainFullGrace)
	if err != nil {
		return types.ReconcileInput{}, err
	}
	abandonedReplacementGrace, err := ParseDuration("config.abandonedReplacementGrace", d.cfg.AbandonedReplacementGrace)
	if err != nil {
		return types.ReconcileInput{}, err
	}

	input := types.ReconcileInput{
		OwnerObject:               d.owner,
		OwnerGVK:                  v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"),
		EventTarget:               d.owner,
		Key:                       d.key(),
		DesiredSpec:               desired,
		ObservedState:             observed,
		ScaleDownRequeueInterval:  scaleDown,
		StuckPodGrace:             grace,
		UnschedulableGrace:        unschedulableGrace,
		GangDomainFullGrace:       gangDomainFullGrace,
		AbandonedReplacementGrace: abandonedReplacementGrace,
		Requeue:                   types.RequeueIntervals{Operation: requeueOperation, Gate: requeueGate},
		Gangs:                     types.NewGangObservations(),
		DrainHolds:                &types.DrainHolds{},
		Teardown:                  d.teardown,
		Clock:                     d.clock,
		Disposition: types.DispositionDeps{
			PodSpec:                desired.PodSpec,
			AutoMigrateMaxAttempts: d.cfg.AutoMigrateBudget,
		},
	}
	if input.UpdateRetryPolicy, err = retryPolicyOf("config.updateRetry", d.cfg.UpdateRetry); err != nil {
		return types.ReconcileInput{}, err
	}
	if budget := d.cfg.GroupMaxUnavailable; budget != "" && !intOrPercent.MatchString(budget) {
		return types.ReconcileInput{}, fmt.Errorf("replay: config.groupMaxUnavailable %q is not an integer or a percent", budget)
	}
	if batch := d.cfg.ScaleDownPodBatchSize; batch != "" {
		if !intOrPercent.MatchString(batch) {
			return types.ReconcileInput{}, fmt.Errorf("replay: config.scaleDownPodBatchSize %q is not an integer or a percent", batch)
		}
		size := intstr.Parse(batch)
		input.ScaleDownPodBatchSize = &size
	}
	if err := d.installPolicies(&input); err != nil {
		return types.ReconcileInput{}, err
	}

	d.store.Install(&input)
	d.wrapCallbacks(ctx, &input, target)
	return input, nil
}

// installPolicies parses the scenario's optional force-delete and
// migration-audit policies onto the input.
func (d *driver) installPolicies(input *types.ReconcileInput) error {
	if policy := d.cfg.ForceDelete; policy != nil {
		slack, err := ParseDuration("config.forceDelete.overdueSlack", policy.OverdueSlack)
		if err != nil {
			return err
		}
		threshold, err := ParseDuration("config.forceDelete.nodeUnreachableThreshold", policy.NodeUnreachableThreshold)
		if err != nil {
			return err
		}
		input.ForceDelete = &types.ForceDeletePolicy{OverdueSlack: slack, NodeUnreachableThreshold: threshold}
	}
	if policy := d.cfg.SchedulingRetry; policy != nil {
		after, err := ParseDuration("config.schedulingRetry.after", policy.After)
		if err != nil {
			return err
		}
		input.SchedulingRetry = &types.SchedulingRetryPolicy{After: after, MaxAttempts: policy.MaxAttempts}
	}
	if policy := d.cfg.MigrationAudit; policy != nil {
		window, err := ParseDuration("config.migrationAudit.window", policy.Window)
		if err != nil {
			return err
		}
		input.MigrationAudit = &types.MigrationAuditPolicy{
			MaxInFlight:  policy.MaxInFlight,
			MaxPerWindow: policy.MaxPerWindow,
			Window:       window,
		}
	}
	return nil
}

// wrapCallbacks records every seam the engine reaches back through, so a
// trace shows what the engine asked of its caller as well as what it
// wrote.
func (d *driver) wrapCallbacks(ctx context.Context, input *types.ReconcileInput, target *appsv1.ControllerRevision) {
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
		d.tickHolds = append(d.tickHolds, *hold)
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
		was, is := cloneMigrationRecord(before), cloneMigrationRecord(next)
		d.tickMigrationWrites = append(d.tickMigrationWrites, MigrationCommit{RequestUUID: requestUUID, Before: &was, After: &is})
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
		is := cloneMigrationRecord(rec)
		d.tickMigrationWrites = append(d.tickMigrationWrites, MigrationCommit{RequestUUID: rec.RequestUUID, Appended: true, After: &is})
		d.trace.callback("migration uuid=%s append %s", d.norm.uid(rec.RequestUUID), d.migrationDiff(nil, &rec))
		return nil
	}
	d.wrapGateCallbacks(ctx, input, target)
}

// wrapGateCallbacks installs the stand-ins for the coordination gates. The
// update gate admits every fresh start, as a Component in no group is
// admitted, unless the scenario has staged a start-time gate's standing
// answer (gate.deny[<gate>] with no pod) or stands in for a group's
// unavailability budget (config.groupMaxUnavailable). The drain-time gate answers from the
// staged pairing holds: a question naming a held pod is denied under the
// Pairing gate, any other is admitted. Every consult is traced, so a golden
// shows what the engine asked before it started an Instance or took pods
// out of rotation.
func (d *driver) wrapGateCallbacks(ctx context.Context, input *types.ReconcileInput, target *appsv1.ControllerRevision) {
	gate := groupGateStandIn{
		rows:   input.ObservedState.InstanceStatuses,
		window: workloadops.ProvenWindowSeconds(d.spec.MinReadySeconds, input.StuckPodGrace),
		target: target,
	}
	if d.cfg.GroupMaxUnavailable != "" {
		gate.pods, gate.podsErr = d.podsByInstance(ctx)
	}
	input.UpdateGate = func(_ string, strategy types.UpdateStrategyType, inFlightSurge, inFlightUnavail int32) (bool, types.RolloutHoldGate, string) {
		consult := fmt.Sprintf("gate consult strategy=%s surge=%d unavail=%d", strategy, inFlightSurge, inFlightUnavail)
		if hold, text, held := d.updateGateHold(gate, strategy, inFlightUnavail); held {
			d.trace.callback("%s verdict=deny gate=%s reason=%s", consult, hold.gate, text)
			return false, hold.gate, hold.reason
		}
		d.trace.callback("%s verdict=allow", consult)
		return true, "", ""
	}
	if d.pairingHolds == nil {
		return
	}
	input.DrainGate = func(pods []string) (bool, types.RolloutHoldGate, string) {
		for _, name := range pods {
			if reason, held := d.pairingHolds[name]; held {
				d.trace.callback("gate consult drain pods=[%s] verdict=deny gate=%s reason=%s",
					strings.Join(pods, " "), types.RolloutHoldGatePairing, d.norm.text(reason))
				return false, types.RolloutHoldGatePairing, reason
			}
		}
		d.trace.callback("gate consult drain pods=[%s] verdict=allow", strings.Join(pods, " "))
		return true, "", ""
	}
}

// startTimeHold is the standing answer a scenario staged for the update
// gate: the start-time gate that holds and the reason it gives.
type startTimeHold struct {
	gate   types.RolloutHoldGate
	reason string
}

// updateGateHold is the stand-in update gate's answer for one consult:
// the staged start-time hold first, else the group's unavailability
// budget. The text is the reason as the trace spells it.
func (d *driver) updateGateHold(gate groupGateStandIn, strategy types.UpdateStrategyType, inFlightUnavail int32) (startTimeHold, string, bool) {
	if hold := d.startHold; hold.reason != "" {
		return hold, d.norm.text(hold.reason), true
	}
	if held, reason := d.groupUnavailabilityHold(gate, strategy, inFlightUnavail); held {
		return startTimeHold{gate: types.RolloutHoldGateBudget, reason: reason}, reason, true
	}
	return startTimeHold{}, "", false
}

// intOrPercent is the shape of a budget string: an integer or a percent.
var intOrPercent = regexp.MustCompile(`^[0-9]+%?$`)

// groupGateStandIn is what the stand-in group gate reads for one pass: the
// rows as the pass observed them, which stand for the published counters;
// the Component's pods listed once before the pass writes, as the adapter
// hands the gate one list per pass from a cache that has not seen the
// pass's own writes, so an Instance the gate counts as down at its first
// consult stays counted at every later one; the proven window; and the
// target the roll moves to.
type groupGateStandIn struct {
	rows    []types.InstanceStatus
	pods    map[int32][]*corev1.Pod
	podsErr error
	window  int32
	target  *appsv1.ControllerRevision
}

// groupUnavailabilityHold is the stand-in group gate's answer for a
// drain-first start: held while the Instances the published counters do
// not count as serving, plus the Instances those counters carry as
// serving whose own pods say otherwise (workloadops.ServingInstancesDown:
// a routed pod that came back and has not held Ready for the window, or
// one that left rotation since the publication), plus the starts this
// pass opened, plus this one, exceed config.groupMaxUnavailable resolved
// against the replica count, the way the group's unavailability gate
// projects a start and in its words. A surge start, or a scenario with no
// group budget, is never held; a pod list the gate could not make fails
// closed, as the gate's does.
func (d *driver) groupUnavailabilityHold(gate groupGateStandIn, strategy types.UpdateStrategyType, inFlightUnavail int32) (bool, string) {
	if d.cfg.GroupMaxUnavailable == "" || strategy == types.UpdateStrategySurgeThenDrain || strategy == "" {
		return false, ""
	}
	budgetSpec := intstr.Parse(d.cfg.GroupMaxUnavailable)
	budget := utils.ScaledCountFromIntOrString(&budgetSpec, d.spec.Replicas, true)
	var serving int32
	for i := range gate.rows {
		if status.InstanceMeetsThreshold(gate.rows[i].PodCount, gate.rows[i].ServingPodCount, d.desiredPodsByInstance[gate.rows[i].Index]) {
			serving++
		}
	}
	if gate.podsErr != nil {
		return true, fmt.Sprintf("cannot list %s pods, failing closed: %v", d.opts.Component, gate.podsErr)
	}
	targetName, targetBorn := "", time.Time{}
	if gate.target != nil {
		targetName, targetBorn = gate.target.Name, gate.target.CreationTimestamp.Time
	}
	down, reasons, _ := workloadops.ServingInstancesDown(gate.rows, gate.pods,
		func(row *types.InstanceStatus) int32 { return d.desiredPodsByInstance[row.Index] }, targetName, targetBorn, gate.window, d.clock.Now())
	unavailable := max(d.spec.Replicas-serving, 0) + int32(len(down))
	if unavailable+inFlightUnavail+1 > budget {
		return true, coordination.UnavailableBudgetHold(budget, unavailable, inFlightUnavail, "", reasons)
	}
	return false, ""
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
