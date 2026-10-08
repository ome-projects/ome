package replay

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	k8stypes "k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

// cluster is the fake apiserver one replay runs against: the object store,
// the injected clock, the write hooks the trace reads, and the refusals a
// scenario arms. The live reader is the store itself; the cached client is
// the store or, during a stale pass, a snapshot of it frozen at the end of
// the previous tick.
type cluster struct {
	scheme *runtime.Scheme
	cli    client.Client
	clock  *clocktesting.FakeClock

	// quiet suppresses recording and refusals while the driver itself is
	// staging a scenario event: the scenario's own hand is already in the
	// trace, and a refusal armed for the engine must not be spent on it.
	quiet bool

	// observe receives every engine write the trace records.
	observe func(write writeRecord)

	// rejections are the armed refusals still owed to matching engine
	// writes, in arming order.
	rejections []*pendingRejection

	// statusWrites counts the logical InferenceService status writes of
	// the pass; an armed conflict names the write it answers.
	statusWrites    int
	lastStatusWrite statusWriteState
	conflicts       []*statusConflict
	// flushFailures is how many InferenceService status writes still fail
	// with a non-conflict error.
	flushFailures int

	// stale is the frozen informer view served to the cached client while
	// stalePass is set: the store as the previous pass found it; staleTick
	// is that pass's tick. pending is the view taken before the running
	// pass, promoted to stale once the pass ends.
	stale       client.Reader
	staleTick   int
	stalePass   bool
	pending     client.Reader
	pendingTick int
	// snapshots marks a run that stages a stale read, which is what makes
	// the driver snapshot the store at the end of every tick.
	snapshots bool
}

// statusWriteState tells a retry of one logical status write from the next
// logical write: a retry follows a refusal the cluster itself injected.
type statusWriteState struct {
	key     client.ObjectKey
	refused bool
}

// statusConflict is an armed 409 on the write-th InferenceService status
// write of the pass, remaining attempts in a row.
type statusConflict struct {
	write     int
	remaining int
	fired     bool
}

// pendingRejection is an armed refusal owed to the engine's next writes of
// one verb on one resource.
type pendingRejection struct {
	verb      string
	resource  string
	remaining int
	err       error
	label     string
}

func (r *pendingRejection) matches(verb, resource string) bool {
	return r.remaining != 0 && r.verb == verb && r.resource == resource
}

// writeRecord is one engine write as the trace records it.
type writeRecord struct {
	kind   string
	verb   string
	name   string
	detail string
}

// buildScheme registers every kind the pass reads or writes.
func buildScheme() (*runtime.Scheme, error) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		appsv1.AddToScheme,
		discoveryv1.AddToScheme,
		v1beta1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			return nil, fmt.Errorf("replay: build scheme: %w", err)
		}
	}
	return scheme, nil
}

func newCluster(clock *clocktesting.FakeClock) (*cluster, error) {
	scheme, err := buildScheme()
	if err != nil {
		return nil, err
	}
	c := &cluster{scheme: scheme, clock: clock}
	c.cli = fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&v1beta1.InferenceService{}, &v1beta1.InferenceReplica{}).
		WithInterceptorFuncs(c.interceptors()).
		Build()
	return c, nil
}

// staging runs a driver-side write with recording and refusals suspended;
// a staging inside another leaves the outer one quiet.
func (c *cluster) staging(write func() error) error {
	was := c.quiet
	c.quiet = true
	defer func() { c.quiet = was }()
	return write()
}

// beginPass resets the per-pass write count an armed conflict is counted
// against.
func (c *cluster) beginPass() {
	c.statusWrites = 0
	c.lastStatusWrite = statusWriteState{}
}

// armConflict makes the write-th InferenceService status write of the next
// pass hit a 409, count attempts in a row.
func (c *cluster) armConflict(write, count int) {
	c.conflicts = append(c.conflicts, &statusConflict{write: write, remaining: count})
}

// unfiredConflicts reports the armed conflicts no status write reached and
// forgets every armed conflict: a conflict is a claim about one pass.
func (c *cluster) unfiredConflicts() []int {
	var unfired []int
	for _, sc := range c.conflicts {
		if !sc.fired {
			unfired = append(unfired, sc.write)
		}
	}
	c.conflicts = nil
	return unfired
}

func (c *cluster) conflictFor(write int) bool {
	for _, sc := range c.conflicts {
		if sc.write == write && sc.remaining > 0 {
			sc.remaining--
			sc.fired = true
			return true
		}
	}
	return false
}

// armRejection records a refusal owed to the engine's next count writes of
// verb on resource.
func (c *cluster) armRejection(verb, resource string, count int, err error, label string) {
	c.rejections = append(c.rejections, &pendingRejection{verb: verb, resource: resource, remaining: count, err: err, label: label})
}

// takeRejection consumes the first armed refusal matching an engine write.
func (c *cluster) takeRejection(verb string, obj client.Object) (error, string) {
	if c.quiet {
		return nil, ""
	}
	resource := resourceOf(obj)
	for _, rejection := range c.rejections {
		if !rejection.matches(verb, resource) {
			continue
		}
		if rejection.remaining > 0 {
			rejection.remaining--
		}
		return rejection.err, rejection.label
	}
	return nil, ""
}

// resourceOf names the resource a refusal is armed against, from the Go
// type so every kind gets the same predictable plural.
func resourceOf(obj client.Object) string {
	t := reflect.TypeOf(obj)
	for t != nil && t.Kind() == reflect.Pointer {
		t = t.Elem()
	}
	if t == nil {
		return "unknown"
	}
	return strings.ToLower(t.Name()) + "s"
}

// kindOf names an object in a trace line.
func kindOf(obj client.Object) string {
	switch obj.(type) {
	case *v1beta1.InferenceService:
		return "isvc"
	case *v1beta1.InferenceReplica:
		return "ir"
	case *corev1.Service:
		return "service"
	case *appsv1.ControllerRevision:
		return "controllerrevision"
	case *corev1.Pod:
		return "pod"
	case *v1beta1.RolloutPolicy:
		return "rolloutpolicy"
	default:
		return strings.ToLower(reflect.TypeOf(obj).Elem().Name())
	}
}

// record hands one engine write to the trace.
func (c *cluster) record(kind, verb, name, detail string) {
	if c.quiet || c.observe == nil {
		return
	}
	c.observe(writeRecord{kind: kind, verb: verb, name: name, detail: detail})
}

// interceptors are the apiserver behaviors the fake client lacks and the
// seams the trace and the armed refusals need: a UID on admission, a
// generation bump on a spec write, the status-write refusals, and the
// recording of every engine write.
func (c *cluster) interceptors() interceptor.Funcs {
	return interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if obj.GetUID() == "" {
				obj.SetUID(k8stypes.UID(obj.GetName() + "-uid"))
			}
			if created := obj.GetCreationTimestamp(); created.IsZero() {
				obj.SetCreationTimestamp(metav1.NewTime(c.clock.Now()))
			}
			if obj.GetGeneration() == 0 {
				obj.SetGeneration(1)
			}
			if err, label := c.takeRejection("create", obj); err != nil {
				c.record(kindOf(obj), "create-rejected", obj.GetName(), "rejection="+label)
				return err
			}
			if err := cl.Create(ctx, obj, opts...); err != nil {
				return err
			}
			c.record(kindOf(obj), "create", obj.GetName(), c.createDetail(obj))
			return nil
		},
		Update: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			if err, label := c.takeRejection("update", obj); err != nil {
				c.record(kindOf(obj), "update-rejected", obj.GetName(), "rejection="+label)
				return err
			}
			before := c.stored(ctx, cl, obj)
			if before != nil && specChanged(before, obj) {
				obj.SetGeneration(before.GetGeneration() + 1)
			}
			if err := cl.Update(ctx, obj, opts...); err != nil {
				return err
			}
			c.record(kindOf(obj), "update", obj.GetName(), c.writeDetail(before, obj))
			return nil
		},
		Patch: func(ctx context.Context, cl client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			if err, label := c.takeRejection("patch", obj); err != nil {
				c.record(kindOf(obj), "patch-rejected", obj.GetName(), "rejection="+label)
				return err
			}
			before := c.stored(ctx, cl, obj)
			if err := cl.Patch(ctx, obj, patch, opts...); err != nil {
				return err
			}
			// The apiserver bumps the generation when a patch changed the
			// spec; the fake client does not, so the bump is written back.
			if before != nil && specChanged(before, obj) {
				obj.SetGeneration(before.GetGeneration() + 1)
				if err := cl.Update(ctx, obj); err != nil {
					return fmt.Errorf("replay: bump generation after patch of %s: %w", obj.GetName(), err)
				}
			}
			c.record(kindOf(obj), "patch", obj.GetName(), c.writeDetail(before, obj))
			return nil
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err, label := c.takeRejection("delete", obj); err != nil {
				c.record(kindOf(obj), "delete-rejected", obj.GetName(), "rejection="+label)
				return err
			}
			if err := cl.Delete(ctx, obj, opts...); err != nil {
				return err
			}
			c.record(kindOf(obj), "delete", obj.GetName(), "")
			return nil
		},
		SubResourceUpdate: func(ctx context.Context, cl client.Client, subResource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			if isvc, ok := obj.(*v1beta1.InferenceService); ok && !c.quiet && subResource == "status" {
				if err := c.refuseStatusWrite(isvc); err != nil {
					return err
				}
			}
			return cl.SubResource(subResource).Update(ctx, obj, opts...)
		},
	}
}

// refuseStatusWrite answers an InferenceService status write with the
// refusal a scenario armed for it: a 409 on the named logical write, or the
// non-conflict failure of a flush. Every attempt of a write the cluster
// itself refused with a 409 belongs to the same logical write.
func (c *cluster) refuseStatusWrite(isvc *v1beta1.InferenceService) error {
	key := client.ObjectKeyFromObject(isvc)
	if !(c.lastStatusWrite.refused && c.lastStatusWrite.key == key) {
		c.statusWrites++
	}
	c.lastStatusWrite = statusWriteState{key: key}
	if c.conflictFor(c.statusWrites) {
		c.lastStatusWrite.refused = true
		c.record("isvc", "status-conflict", isvc.Name, fmt.Sprintf("write=%d", c.statusWrites))
		return apierrors.NewConflict(schema.GroupResource{Group: v1beta1.SchemeGroupVersion.Group, Resource: "inferenceservices"}, isvc.Name,
			errors.New("the object has been modified; please apply your changes to the latest version and try again"))
	}
	if c.flushFailures > 0 {
		c.flushFailures--
		c.record("isvc", "status-failed", isvc.Name, fmt.Sprintf("write=%d", c.statusWrites))
		return apierrors.NewInternalError(errors.New("etcdserver: request timed out"))
	}
	return nil
}

// stored reads the object as the store holds it, nil when it is absent.
func (c *cluster) stored(ctx context.Context, cl client.Reader, obj client.Object) client.Object {
	stored, ok := obj.DeepCopyObject().(client.Object)
	if !ok {
		return nil
	}
	if err := cl.Get(ctx, client.ObjectKeyFromObject(obj), stored); err != nil {
		return nil
	}
	return stored
}

// specChanged reports whether a write moves an object's spec, which is what
// the apiserver bumps the generation on. Only the two kinds whose
// generation the pass reads are compared.
func specChanged(before, after client.Object) bool {
	switch b := before.(type) {
	case *v1beta1.InferenceReplica:
		a, ok := after.(*v1beta1.InferenceReplica)
		return ok && !equality.Semantic.DeepEqual(b.Spec, a.Spec)
	case *v1beta1.InferenceService:
		a, ok := after.(*v1beta1.InferenceService)
		return ok && !equality.Semantic.DeepEqual(b.Spec, a.Spec)
	}
	return false
}

// createDetail names what a created object carries, for the kinds whose
// creation the trace renders in detail.
func (c *cluster) createDetail(obj client.Object) string {
	if ir, ok := obj.(*v1beta1.InferenceReplica); ok {
		return irSpecDetail(nil, ir)
	}
	return ""
}

// writeDetail names the fields an update or patch changed, for the kinds
// whose writes the trace renders in detail: an InferenceReplica's projected spec and an
// InferenceService's annotations. Everything else is traced by name alone.
func (c *cluster) writeDetail(before, after client.Object) string {
	switch a := after.(type) {
	case *v1beta1.InferenceReplica:
		b, _ := before.(*v1beta1.InferenceReplica)
		return irSpecDetail(b, a)
	case *v1beta1.InferenceService:
		b, _ := before.(*v1beta1.InferenceService)
		if b == nil {
			return ""
		}
		return "annotations=" + mapDiff(b.Annotations, a.Annotations)
	}
	return ""
}

// irSpecDetail renders the projected fields of an InferenceReplica spec,
// as a diff when a prior object is given.
func irSpecDetail(before, after *v1beta1.InferenceReplica) string {
	fields := func(ir *v1beta1.InferenceReplica) map[string]string {
		if ir == nil {
			return nil
		}
		out := map[string]string{
			"image":      "nil",
			"replicas":   int32PtrValue(ir.Spec.Replicas),
			"partition":  "nil",
			"rollbackTo": "nil",
			"paused":     fmt.Sprint(ir.Spec.Paused),
			"parentGen":  orNil(ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey]),
			"excluded":   orNil(ir.Annotations[constants.RevisionExcludedAnnotationKeysAnnotationKey]),
			"generation": fmt.Sprint(ir.Generation),
		}
		if len(ir.Spec.Runners) > 0 && len(ir.Spec.Runners[0].Template.Spec.Containers) > 0 {
			out["image"] = ir.Spec.Runners[0].Template.Spec.Containers[0].Image
		}
		if ir.Spec.Pacing != nil {
			out["partition"] = int32PtrValue(ir.Spec.Pacing.Partition)
			if ir.Spec.Pacing.RollbackToRevision != nil {
				out["rollbackTo"] = *ir.Spec.Pacing.RollbackToRevision
			}
		}
		return out
	}
	order := []string{"image", "replicas", "partition", "rollbackTo", "paused", "parentGen", "excluded", "generation"}
	to := fields(after)
	from := fields(before)
	parts := make([]string, 0, len(order))
	for _, key := range order {
		if from == nil {
			parts = append(parts, key+"="+to[key])
			continue
		}
		if from[key] != to[key] {
			parts = append(parts, key+"="+from[key]+"→"+to[key])
		}
	}
	if len(parts) == 0 {
		return "unchanged-fields"
	}
	return strings.Join(parts, " ")
}

// laggedClient is the engines' cached client during a stale pass: the
// service is read as the previous pass found it, while every other object
// and every write go to the store. The lag is the service's own: a stale-read event describes a pass whose copy of the service
// predates a status flush, with its replicas, pods, Services and policies
// read current.
type laggedClient struct {
	client.Client
	snapshot client.Reader
}

func (l *laggedClient) Get(ctx context.Context, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
	if _, ok := obj.(*v1beta1.InferenceService); ok {
		return l.snapshot.Get(ctx, key, obj, opts...)
	}
	return l.Client.Get(ctx, key, obj, opts...)
}

func (l *laggedClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*v1beta1.InferenceServiceList); ok {
		return l.snapshot.List(ctx, list, opts...)
	}
	return l.Client.List(ctx, list, opts...)
}

// cachedClient is the client the engines read their cache through this
// pass: the store itself, or the previous tick's objects when the scenario
// staged a stale read.
func (c *cluster) cachedClient() client.Client {
	if c.stalePass {
		return &laggedClient{Client: c.cli, snapshot: c.stale}
	}
	return c.cli
}

// snapshot copies the service as it stands into a reader frozen at this
// instant.
func (c *cluster) snapshot(ctx context.Context) (client.Reader, error) {
	list := &v1beta1.InferenceServiceList{}
	if err := c.cli.List(ctx, list); err != nil {
		return nil, fmt.Errorf("replay: snapshot the service: %w", err)
	}
	objects := make([]client.Object, 0, len(list.Items))
	for i := range list.Items {
		objects = append(objects, list.Items[i].DeepCopy())
	}
	return fake.NewClientBuilder().WithScheme(c.scheme).WithObjects(objects...).Build(), nil
}

// prepareSnapshot takes the view a later stale pass reads: the store as
// this tick's pass is about to find it, which misses everything the pass
// writes. Only a run that stages a stale read pays for it.
func (c *cluster) prepareSnapshot(ctx context.Context, tick int) error {
	if !c.snapshots {
		return nil
	}
	snapshot, err := c.snapshot(ctx)
	if err != nil {
		return err
	}
	c.pending, c.pendingTick = snapshot, tick
	return nil
}

// finishPass ends the stale pass, if one ran, and makes the view taken
// before this pass the one the next stale read serves.
func (c *cluster) finishPass() {
	c.stalePass = false
	if c.pending != nil {
		c.stale, c.staleTick = c.pending, c.pendingTick
		c.pending = nil
	}
}

func int32PtrValue(v *int32) string {
	if v == nil {
		return "nil"
	}
	return fmt.Sprint(*v)
}

func orNil(s string) string {
	if s == "" {
		return "nil"
	}
	return s
}

// mapDiff renders every key a write added, removed or rewrote.
func mapDiff(before, after map[string]string) string {
	keys := map[string]struct{}{}
	for k, v := range after {
		if old, had := before[k]; !had || old != v {
			keys[k] = struct{}{}
		}
	}
	for k := range before {
		if _, ok := after[k]; !ok {
			keys[k] = struct{}{}
		}
	}
	if len(keys) == 0 {
		return "unchanged"
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	parts := make([]string, 0, len(ordered))
	for _, k := range ordered {
		parts = append(parts, k+":"+valueOrNil(before, k)+"→"+valueOrNil(after, k))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func valueOrNil(m map[string]string, key string) string {
	if v, ok := m[key]; ok {
		return v
	}
	return "nil"
}
