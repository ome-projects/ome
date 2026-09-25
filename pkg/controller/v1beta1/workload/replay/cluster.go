package replay

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8stypes "k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// podLifecycleFinalizer keeps a deleted pod as a Terminating object until
// the scenario says it is gone. The engine's own delete therefore
// produces the two observations a real cluster produces — Terminating,
// then absent — instead of collapsing them into one.
const podLifecycleFinalizer = "replay.workload.ome.io/pod-lifecycle"

// podTeardown is a deletion the driver is holding open, as the engine has
// to see it. The fake client stamps a deletion timestamp off the wall
// clock, which no deadline measured on the injected clock can be compared
// against, so the driver records the instant the delete landed and the
// grace the request carried, and presents both on every read.
//
// kubeletStuck is the pod whose kubelet has stopped acknowledging the
// deletion: no finalizer holds it, nothing will ever remove it, and only a
// grace-zero delete gets the object out of the apiserver. It is the shape
// the force-delete escalation exists for, and the one shape a finalizer
// cannot stand in for — a finalizer-pinned pod classifies as
// foreign-finalizers and is report-only.
type podTeardown struct {
	at           time.Time
	grace        int64
	kubeletStuck bool
}

// sliceNameFor is the single EndpointSlice the driver keeps per Service.
// One slice per Service is enough: the drain and rotation checks read
// endpoints, not slice topology.
func sliceNameFor(serviceName string) string {
	return serviceName + "-replay"
}

// routedServiceForPod names the per-revision routed Service a pod is
// published through, resolved exactly as the engine resolves it for the
// same pod. A pod with no revision hash has none, and neither does a gang
// worker: workers serve no customer traffic and are never members of the
// routed Service.
func (d *driver) routedServiceForPod(pod *corev1.Pod) string {
	hash := pod.Labels[query.LabelRevisionHash]
	if hash == "" || pod.Labels[query.LabelRunner] == "worker" {
		return ""
	}
	return query.PerRevisionServiceName(d.opts.OwnerName, d.opts.Component, hash)
}

// routingViewsFor lists the Services a pod's endpoint is published in.
// The per-revision routed Service is what the drain gate and the
// source-rotation report read; the Component's headless Service is what
// the status aggregate counts available pods from. A pod in rotation is
// in both, so the driver writes both.
func (d *driver) routingViewsFor(pod *corev1.Pod) []string {
	views := []string{query.HeadlessServiceName(d.opts.OwnerName, d.opts.Component)}
	if routed := d.routedServiceForPod(pod); routed != "" {
		views = append(views, routed)
	}
	return views
}

// pendingRejection is an injected apiserver refusal owed to the next
// matching write.
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

// takeRejection consumes the first injected refusal matching a write the
// ENGINE issued. A driver-staged write is the scenario's own hand: letting
// it consume a refusal armed for the engine would silently disarm the
// scenario and the trace would show the engine succeeding against a quota
// it never met.
func (d *driver) takeRejection(verb string, obj client.Object) (error, string) {
	if d.quiet {
		return nil, ""
	}
	resource := resourceOf(obj)
	for _, rejection := range d.rejections {
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

// resourceOf names the resource a scenario arms a rejection against. It is
// derived from the Go type rather than from TypeMeta, which controller-
// runtime leaves empty on typed objects, so every kind the driver can see
// gets the same predictable plural.
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

// kindOf names the object in a trace line.
func kindOf(obj client.Object) string {
	switch obj.(type) {
	case *corev1.Pod:
		return "pod"
	case *schedulingv1alpha1.PodGroup:
		return "podgroup"
	case *discoveryv1.EndpointSlice:
		return "endpointslice"
	case *corev1.Service:
		return "service"
	default:
		gvk := obj.GetObjectKind().GroupVersionKind()
		if gvk.Kind != "" {
			return strings.ToLower(gvk.Kind)
		}
		return "object"
	}
}

// traced reports whether an object's writes belong in the trace. The
// owner status is recorded through the row store and the revisions are
// the driver's own bookkeeping, so neither is repeated here.
func (d *driver) traced(obj client.Object) bool {
	if d.quiet {
		return false
	}
	return tracedKind(obj)
}

func tracedKind(obj client.Object) bool {
	switch obj.(type) {
	case *corev1.Pod, *schedulingv1alpha1.PodGroup:
		return true
	default:
		return false
	}
}

// staging runs a driver-side write with recording suppressed: the
// scenario event that caused it is already the trace line for it, and
// re-recording the driver's own hand as an engine effect would make a
// golden unreadable.
func (d *driver) staging(write func() error) error {
	d.quiet = true
	defer func() { d.quiet = false }()
	return write()
}

// deleteObject performs one delete against the fake apiserver. A pod held
// Terminating by a dead kubelet is held by the driver rather than by a
// finalizer, so only a grace-zero delete — the escalation's own — takes
// the object out; a graceful delete on it changes nothing, exactly as it
// changes nothing on a real cluster where the object is already on its way
// out. Every other delete goes to the client untouched, and a pod that
// survives it has the instant it began terminating recorded.
func (d *driver) deleteObject(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
	pod, isPod := obj.(*corev1.Pod)
	if !isPod {
		return c.Delete(ctx, obj, opts...)
	}
	if hold := d.terminating[pod.Name]; hold != nil && hold.kubeletStuck {
		if !zeroGrace(opts) {
			return nil
		}
		return d.reapStuckPod(ctx, pod.Name)
	}
	if err := c.Delete(ctx, obj, opts...); err != nil {
		return err
	}
	return d.recordTeardown(ctx, c, pod.Name, opts)
}

// recordTeardown notes what a pod that outlived its delete is terminating
// against: the deletion instant on the injected clock, which the apiserver
// computes as the request time plus the grace the pod is owed, and the
// grace itself. A pod the delete really removed has nothing to record.
func (d *driver) recordTeardown(ctx context.Context, c client.WithWatch, name string, opts []client.DeleteOption) error {
	stored := &corev1.Pod{}
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: name}
	if err := c.Get(ctx, key, stored); err != nil {
		return client.IgnoreNotFound(err)
	}
	if stored.DeletionTimestamp == nil || d.terminating[name] != nil {
		return nil
	}
	grace := graceSeconds(opts, podGracePeriod(stored))
	d.terminating[name] = &podTeardown{
		at:    d.clock.Now().Add(time.Duration(grace) * time.Second),
		grace: grace,
	}
	return nil
}

// reapStuckPod completes the removal of a wedged pod: the escalation's
// grace-zero delete is what a real apiserver honors past a dead kubelet,
// so the object leaves exactly as the scenario's own pod.deleted would
// take it out.
func (d *driver) reapStuckPod(ctx context.Context, name string) error {
	delete(d.terminating, name)
	return d.staging(func() error { return d.removePodNow(ctx, name) })
}

// showTeardown presents a pod's deletion the way the apiserver would. A
// pod whose kubelet has stopped acknowledging it carries no finalizer,
// because it is the driver and not a finalizer that keeps the object from
// leaving — and a finalizer is exactly the evidence that would make the
// escalation decline it.
func (d *driver) showTeardown(pod *corev1.Pod) {
	hold := d.terminating[pod.Name]
	if hold == nil {
		return
	}
	at := metav1.NewTime(hold.at)
	pod.DeletionTimestamp = &at
	grace := hold.grace
	pod.DeletionGracePeriodSeconds = &grace
	if hold.kubeletStuck {
		pod.Finalizers = withoutLifecycleFinalizer(pod.Finalizers)
	}
}

// hideTeardown puts the stored deletion metadata back on an object read
// through showTeardown, so a whole-object write carries what the fake
// apiserver holds rather than what the driver displayed — the deletion
// timestamp is immutable there, and the finalizer is what keeps the object
// alive at all.
func (d *driver) hideTeardown(ctx context.Context, c client.Reader, obj client.Object) {
	pod, ok := obj.(*corev1.Pod)
	if !ok || d.terminating[pod.Name] == nil {
		return
	}
	stored := &corev1.Pod{}
	if err := c.Get(ctx, client.ObjectKeyFromObject(pod), stored); err != nil {
		return
	}
	pod.DeletionTimestamp = stored.DeletionTimestamp
	pod.DeletionGracePeriodSeconds = stored.DeletionGracePeriodSeconds
	pod.Finalizers = stored.Finalizers
}

func withoutLifecycleFinalizer(finalizers []string) []string {
	kept := make([]string, 0, len(finalizers))
	for _, f := range finalizers {
		if f != podLifecycleFinalizer {
			kept = append(kept, f)
		}
	}
	if len(kept) == 0 {
		return nil
	}
	return kept
}

// graceSeconds reads the grace a delete request names, falling back to the
// pod's own when it names none.
func graceSeconds(opts []client.DeleteOption, fallback int64) int64 {
	if grace := requestedGrace(opts); grace != nil {
		return *grace
	}
	return fallback
}

// zeroGrace reports whether a delete asks for immediate removal, which is
// the request a wedged pod only ever leaves the apiserver on.
func zeroGrace(opts []client.DeleteOption) bool {
	grace := requestedGrace(opts)
	return grace != nil && *grace == 0
}

func requestedGrace(opts []client.DeleteOption) *int64 {
	options := &client.DeleteOptions{}
	for _, opt := range opts {
		opt.ApplyToDelete(options)
	}
	return options.GracePeriodSeconds
}

// podGracePeriod is the termination grace the pod itself asks for. A
// fixture that names none terminates at the instant of the request, which
// is what a scenario's overdue slack is then measured from.
func podGracePeriod(pod *corev1.Pod) int64 {
	if pod.Spec.TerminationGracePeriodSeconds == nil {
		return 0
	}
	return *pod.Spec.TerminationGracePeriodSeconds
}

func (d *driver) interceptors() interceptor.Funcs {
	return interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if err := c.Get(ctx, key, obj, opts...); err != nil {
				return err
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				d.showTeardown(pod)
			}
			return nil
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if err := c.List(ctx, list, opts...); err != nil {
				return err
			}
			if pods, ok := list.(*corev1.PodList); ok {
				for i := range pods.Items {
					d.showTeardown(&pods.Items[i])
				}
			}
			return nil
		},
		Create: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			if pod, ok := obj.(*corev1.Pod); ok {
				// Pod names are stable and reused, so a successor starts
				// from no teardown of its own.
				delete(d.terminating, pod.Name)
				pod.Finalizers = append(pod.Finalizers, podLifecycleFinalizer)
				// The apiserver stamps creationTimestamp on admission and
				// the fake client does not. Every age the engine measures
				// off a pod — the stuck-pod grace above all — reads zero
				// without one, so a pod that never got one can never be
				// stuck however far the clock moves.
				if pod.CreationTimestamp.IsZero() {
					pod.CreationTimestamp = metav1.NewTime(d.clock.Now())
				}
				// The apiserver assigns a UID on admission and the engine
				// refuses to act on a pod without one; the fake client
				// assigns none, so the driver stands in for that.
				if pod.UID == "" {
					d.podUIDs++
					pod.UID = k8stypes.UID(fmt.Sprintf("pod-uid-%d", d.podUIDs))
					d.norm.uid(string(pod.UID))
				}
			}
			if err, label := d.takeRejection("create", obj); err != nil {
				if d.traced(obj) {
					d.trace.object(kindOf(obj), "create-rejected", obj.GetName(), "rejection="+label)
				}
				return err
			}
			if err := c.Create(ctx, obj, opts...); err != nil {
				if d.traced(obj) {
					d.trace.object(kindOf(obj), "create-failed", obj.GetName(), "err="+err.Error())
				}
				return err
			}
			if d.traced(obj) {
				d.trace.object(kindOf(obj), "create", obj.GetName(), d.createDetail(obj))
			}
			if pod, ok := obj.(*corev1.Pod); ok {
				// The pod watch confirms a create as soon as the object
				// exists; the driver has no informer, so the create is its
				// own observation.
				index, err := instanceIndexOf(pod)
				if err != nil {
					return err
				}
				d.expectations.ObservedCreate(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, index)
			}
			return nil
		},
		Delete: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if err, label := d.takeRejection("delete", obj); err != nil {
				if d.traced(obj) {
					d.trace.object(kindOf(obj), "delete-rejected", obj.GetName(), "rejection="+label)
				}
				return err
			}
			if err := d.deleteObject(ctx, c, obj, opts...); err != nil {
				if d.traced(obj) {
					d.trace.object(kindOf(obj), "delete-failed", obj.GetName(), "err="+err.Error())
				}
				return err
			}
			if d.traced(obj) {
				d.trace.object(kindOf(obj), "delete", obj.GetName(), deleteDetail(opts))
			}
			return nil
		},
		Update: func(ctx context.Context, c client.WithWatch, obj client.Object, opts ...client.UpdateOption) error {
			return d.recordWrite(ctx, obj, "update", "update", func() error {
				d.hideTeardown(ctx, c, obj)
				return c.Update(ctx, obj, opts...)
			})
		},
		Patch: func(ctx context.Context, c client.WithWatch, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
			return d.recordWrite(ctx, obj, "patch", "patch", func() error { return c.Patch(ctx, obj, patch, opts...) })
		},
		SubResourceUpdate: func(ctx context.Context, c client.Client, subResource string, obj client.Object, opts ...client.SubResourceUpdateOption) error {
			return d.recordWrite(ctx, obj, subResource+"-update", "update", func() error {
				return c.SubResource(subResource).Update(ctx, obj, opts...)
			})
		},
		SubResourcePatch: func(ctx context.Context, c client.Client, subResource string, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
			return d.recordWrite(ctx, obj, subResource+"-patch", "patch", func() error {
				return c.SubResource(subResource).Patch(ctx, obj, patch, opts...)
			})
		},
	}
}

// recordWrite runs one mutating write and records the fields of the
// object it actually changed, so a trace shows the effect rather than the
// patch encoding.
// recordWrite runs one mutating write and records the fields of the object
// it actually changed, so a trace shows the effect rather than the patch
// encoding. verb labels the trace line; baseVerb is what a scenario arms a
// rejection against, so a status patch and a spec patch are both refused by
// `verb: patch`. The rejection check runs for every kind, traced or not.
func (d *driver) recordWrite(ctx context.Context, obj client.Object, verb, baseVerb string, write func() error) error {
	traced := d.traced(obj)
	var before *corev1.Pod
	if traced {
		before = d.snapshotPod(ctx, obj)
	}
	if err, label := d.takeRejection(baseVerb, obj); err != nil {
		if traced {
			d.trace.object(kindOf(obj), verb+"-rejected", obj.GetName(), "rejection="+label)
		}
		return err
	}
	if err := write(); err != nil {
		if traced {
			d.trace.object(kindOf(obj), verb+"-failed", obj.GetName(), "err="+err.Error())
		}
		return err
	}
	if !traced {
		return nil
	}
	after := d.snapshotPod(ctx, obj)
	d.trace.object(kindOf(obj), verb, obj.GetName(), d.podWriteDiff(before, after))
	return nil
}

// snapshotPod reads the stored form of a pod so a write can be described
// by what it changed. Non-pod objects have no snapshot.
func (d *driver) snapshotPod(ctx context.Context, obj client.Object) *corev1.Pod {
	if _, ok := obj.(*corev1.Pod); !ok {
		return nil
	}
	stored := &corev1.Pod{}
	if err := d.cli.Get(ctx, client.ObjectKeyFromObject(obj), stored); err != nil {
		return nil
	}
	return stored
}

func (d *driver) createDetail(obj client.Object) string {
	pod, ok := obj.(*corev1.Pod)
	if !ok {
		return ""
	}
	return fmt.Sprintf("revision=%s incarnation=%s ordinal=%s",
		orNil(pod.Labels[query.LabelRevisionHash]),
		orNil(pod.Labels[query.LabelInstanceIncarnation]),
		orNil(pod.Labels[query.LabelPodOrdinal]))
}

func deleteDetail(opts []client.DeleteOption) string {
	options := &client.DeleteOptions{}
	for _, opt := range opts {
		opt.ApplyToDelete(options)
	}
	grace := "default"
	if options.GracePeriodSeconds != nil {
		grace = strconv.FormatInt(*options.GracePeriodSeconds, 10)
	}
	detail := "grace=" + grace
	if options.Preconditions != nil && options.Preconditions.UID != nil {
		detail += " uidPrecondition=set"
	}
	return detail
}

// podWriteDiff names every field of a pod the engine's write changed, with
// the values it changed them to. The values matter: the in-place roll keys
// its convergence on the exact contents of the image-transition annotation,
// and an image patch that lands the wrong tag differs from one that lands
// the right one only by its value.
//
// The serving condition is reported by status and never by its transition
// time: the engine stamps that from the wall clock, so recording it would
// make every trace differ from the last.
func (d *driver) podWriteDiff(before, after *corev1.Pod) string {
	if before == nil || after == nil {
		return ""
	}
	var parts []string
	if images(before) != images(after) {
		parts = append(parts, fmt.Sprintf("images=%s→%s", images(before), images(after)))
	}
	if from, to := conditionValue(before, query.ServingConditionType), conditionValue(after, query.ServingConditionType); from != to {
		parts = append(parts, fmt.Sprintf("serving=%s→%s", from, to))
	}
	if diff := d.mapDiff(before.Labels, after.Labels); diff != "" {
		parts = append(parts, "labels="+diff)
	}
	if diff := d.mapDiff(before.Annotations, after.Annotations); diff != "" {
		parts = append(parts, "annotations="+diff)
	}
	if diff := d.mapDiff(envOf(before), envOf(after)); diff != "" {
		parts = append(parts, "env="+diff)
	}
	if from, to := schedulingGates(before), schedulingGates(after); from != to {
		parts = append(parts, fmt.Sprintf("schedulingGates=%s→%s", from, to))
	}
	if from, to := listValue(before.Finalizers), listValue(after.Finalizers); from != to {
		parts = append(parts, fmt.Sprintf("finalizers=%s→%s", from, to))
	}
	if len(parts) == 0 {
		return "unchanged-fields"
	}
	return strings.Join(parts, " ")
}

// mapDiff renders every key a write added, removed or rewrote, with both
// values, normalized so a revision name inside a value reads as its token.
func (d *driver) mapDiff(before, after map[string]string) string {
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
		return ""
	}
	ordered := make([]string, 0, len(keys))
	for k := range keys {
		ordered = append(ordered, k)
	}
	sort.Strings(ordered)
	parts := make([]string, 0, len(ordered))
	for _, k := range ordered {
		parts = append(parts, fmt.Sprintf("%s:%s→%s", k,
			d.norm.text(valueOrNil(before, k)), d.norm.text(valueOrNil(after, k))))
	}
	return "[" + strings.Join(parts, " ") + "]"
}

func valueOrNil(m map[string]string, key string) string {
	if v, ok := m[key]; ok {
		return v
	}
	return "nil"
}

// envOf flattens every container's environment into one keyed view, so an
// injected or rewritten variable shows up as a keyed change rather than as
// a whole-spec rewrite.
func envOf(pod *corev1.Pod) map[string]string {
	out := map[string]string{}
	for _, container := range pod.Spec.Containers {
		for _, env := range container.Env {
			value := env.Value
			if env.ValueFrom != nil {
				value = "(valueFrom)"
			}
			out[container.Name+"."+env.Name] = value
		}
	}
	return out
}

func schedulingGates(pod *corev1.Pod) string {
	names := make([]string, 0, len(pod.Spec.SchedulingGates))
	for _, gate := range pod.Spec.SchedulingGates {
		names = append(names, gate.Name)
	}
	return listValue(names)
}

func listValue(values []string) string {
	if len(values) == 0 {
		return "nil"
	}
	sorted := append([]string(nil), values...)
	sort.Strings(sorted)
	return "[" + strings.Join(sorted, ",") + "]"
}

// images renders the container images a pod's spec asks for.
func images(pod *corev1.Pod) string {
	parts := make([]string, 0, len(pod.Spec.Containers))
	for _, c := range pod.Spec.Containers {
		parts = append(parts, c.Name+"="+c.Image)
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func conditionValue(pod *corev1.Pod, condType corev1.PodConditionType) string {
	for _, c := range pod.Status.Conditions {
		if c.Type == condType {
			return string(c.Status)
		}
	}
	return "absent"
}

// instanceIndexOf reads the Instance ordinal a pod is labelled with. A pod
// the engine rendered always carries it; a missing or unparsable label
// means the driver is about to attribute an expectation to the wrong
// Instance, which is worse than failing the run.
func instanceIndexOf(pod *corev1.Pod) (int32, error) {
	raw, ok := pod.Labels[query.LabelInstanceIdx]
	if !ok {
		return 0, fmt.Errorf("replay: pod %s carries no %s label", pod.Name, query.LabelInstanceIdx)
	}
	idx, err := strconv.ParseInt(raw, 10, 32)
	if err != nil {
		return 0, fmt.Errorf("replay: pod %s has an unparsable %s label %q: %w", pod.Name, query.LabelInstanceIdx, raw, err)
	}
	return int32(idx), nil
}

// patchPod applies a driver-side mutation to a stored pod. Driver writes
// bypass the interceptor's recording: they are the scenario speaking, and
// the scenario is already in the trace.
func (d *driver) patchPod(ctx context.Context, name string, mutate func(*corev1.Pod)) error {
	return d.staging(func() error { return d.patchPodNow(ctx, name, mutate) })
}

func (d *driver) patchPodNow(ctx context.Context, name string, mutate func(*corev1.Pod)) error {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: name}
	if err := d.cli.Get(ctx, key, pod); err != nil {
		return fmt.Errorf("replay: get pod %s: %w", name, err)
	}
	mutate(pod)
	// Pods carry a status subresource, so the spec/metadata write and the
	// status write are two calls and the second has to carry the mutated
	// status onto the object the first one left behind.
	desired := *pod.Status.DeepCopy()
	if err := d.cli.Update(ctx, pod); err != nil {
		return fmt.Errorf("replay: update pod %s: %w", name, err)
	}
	if err := d.cli.Get(ctx, key, pod); err != nil {
		return fmt.Errorf("replay: re-read pod %s: %w", name, err)
	}
	pod.Status = desired
	if err := d.cli.Status().Update(ctx, pod); err != nil {
		return fmt.Errorf("replay: update pod status %s: %w", name, err)
	}
	return nil
}

// removePod completes a deletion: the finalizer comes off and the object
// leaves the apiserver, which is the observation the engine waits for.
func (d *driver) removePod(ctx context.Context, name string) error {
	return d.staging(func() error { return d.removePodNow(ctx, name) })
}

func (d *driver) removePodNow(ctx context.Context, name string) error {
	// The object is about to leave the apiserver, so whatever the driver
	// was holding its deletion open with goes with it.
	delete(d.terminating, name)
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: name}
	if err := d.cli.Get(ctx, key, pod); err != nil {
		return fmt.Errorf("replay: get pod %s: %w", name, err)
	}
	index, err := instanceIndexOf(pod)
	if err != nil {
		return err
	}
	// A pod that leaves the apiserver leaves every Service that published
	// it; the endpoints go first, because the object the routed Service is
	// resolved from is about to be gone.
	if err := d.setPodRotationNow(ctx, pod, false); err != nil {
		return err
	}
	pod.Finalizers = nil
	if err := d.cli.Update(ctx, pod); err != nil {
		return fmt.Errorf("replay: clear finalizer on pod %s: %w", name, err)
	}
	if pod.DeletionTimestamp == nil {
		if err := d.cli.Delete(ctx, pod); err != nil {
			return fmt.Errorf("replay: delete pod %s: %w", name, err)
		}
	}
	d.expectations.ObservedDelete(d.opts.Namespace, d.opts.OwnerName, d.opts.Component, index)
	return nil
}

// setRotation adds or removes the pod's ready endpoint in every Service
// that publishes it.
func (d *driver) setRotation(ctx context.Context, podName string, ready bool) error {
	return d.staging(func() error { return d.setRotationNow(ctx, podName, ready) })
}

func (d *driver) setRotationNow(ctx context.Context, podName string, ready bool) error {
	pod := &corev1.Pod{}
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: podName}
	if err := d.cli.Get(ctx, key, pod); err != nil {
		return fmt.Errorf("replay: get pod %s for rotation: %w", podName, err)
	}
	return d.setPodRotationNow(ctx, pod, ready)
}

// setPodRotationNow takes the pod object rather than its name, for the
// one caller that rotates a pod out as part of removing it and so cannot
// read it back afterwards.
func (d *driver) setPodRotationNow(ctx context.Context, pod *corev1.Pod, ready bool) error {
	views := d.routingViewsFor(pod)
	for _, service := range views {
		if err := d.setEndpointNow(ctx, service, pod.Name, ready); err != nil {
			return err
		}
	}
	return d.pruneEndpointsOutsideViews(ctx, pod.Name, views)
}

// pruneEndpointsOutsideViews drops the pod from every Service that no
// longer publishes it. An in-place roll rewrites the pod's revision
// label, which moves it to a different per-revision routed Service; the
// endpoint it had in the old one is stale from that moment on.
func (d *driver) pruneEndpointsOutsideViews(ctx context.Context, podName string, views []string) error {
	slices := &discoveryv1.EndpointSliceList{}
	if err := d.cli.List(ctx, slices, client.InNamespace(d.opts.Namespace)); err != nil {
		return fmt.Errorf("replay: list endpointslices: %w", err)
	}
	for i := range slices.Items {
		service := slices.Items[i].Labels[discoveryv1.LabelServiceName]
		if service == "" || contains(views, service) {
			continue
		}
		if err := d.setEndpointNow(ctx, service, podName, false); err != nil {
			return err
		}
	}
	return nil
}

func (d *driver) setEndpointNow(ctx context.Context, serviceName, podName string, ready bool) error {
	slice := &discoveryv1.EndpointSlice{}
	key := client.ObjectKey{Namespace: d.opts.Namespace, Name: sliceNameFor(serviceName)}
	err := d.cli.Get(ctx, key, slice)
	create := false
	if err != nil {
		if !apierrors.IsNotFound(err) {
			return fmt.Errorf("replay: get endpointslice: %w", err)
		}
		create = true
		slice = &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: d.opts.Namespace,
				Name:      sliceNameFor(serviceName),
				Labels: map[string]string{
					discoveryv1.LabelServiceName: serviceName,
				},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
		}
	}
	position := -1
	for i := range slice.Endpoints {
		ref := slice.Endpoints[i].TargetRef
		if ref != nil && ref.Name == podName {
			position = i
			break
		}
	}
	switch {
	case ready && position < 0:
		readyFlag := true
		slice.Endpoints = append(slice.Endpoints, discoveryv1.Endpoint{
			Addresses:  []string{"10.0.0.1"},
			Conditions: discoveryv1.EndpointConditions{Ready: &readyFlag},
			TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: d.opts.Namespace, Name: podName},
		})
	case ready:
		readyFlag := true
		slice.Endpoints[position].Conditions.Ready = &readyFlag
	case position >= 0:
		slice.Endpoints = append(slice.Endpoints[:position], slice.Endpoints[position+1:]...)
	default:
		return nil
	}
	sort.Slice(slice.Endpoints, func(i, j int) bool {
		return endpointName(slice.Endpoints[i]) < endpointName(slice.Endpoints[j])
	})
	if create {
		return d.cli.Create(ctx, slice)
	}
	return d.cli.Update(ctx, slice)
}

func endpointName(ep discoveryv1.Endpoint) string {
	if ep.TargetRef == nil {
		return ""
	}
	return ep.TargetRef.Name
}

func setPodCondition(pod *corev1.Pod, condType corev1.PodConditionType, status corev1.ConditionStatus, at time.Time) {
	for i := range pod.Status.Conditions {
		if pod.Status.Conditions[i].Type != condType {
			continue
		}
		if pod.Status.Conditions[i].Status == status {
			return
		}
		pod.Status.Conditions[i].Status = status
		pod.Status.Conditions[i].LastTransitionTime = metav1.NewTime(at)
		return
	}
	pod.Status.Conditions = append(pod.Status.Conditions, corev1.PodCondition{
		Type:               condType,
		Status:             status,
		LastTransitionTime: metav1.NewTime(at),
	})
	sort.Slice(pod.Status.Conditions, func(i, j int) bool {
		return pod.Status.Conditions[i].Type < pod.Status.Conditions[j].Type
	})
}

// setContainerStatusesReady mirrors the kubelet's running-container
// report for the pod's declared containers.
func setContainerStatusesReady(pod *corev1.Pod) {
	statuses := make([]corev1.ContainerStatus, 0, len(pod.Spec.Containers))
	for _, c := range pod.Spec.Containers {
		statuses = append(statuses, corev1.ContainerStatus{
			Name:  c.Name,
			Image: c.Image,
			Ready: true,
			State: corev1.ContainerState{Running: &corev1.ContainerStateRunning{}},
		})
	}
	pod.Status.ContainerStatuses = statuses
}

// traceRecorder writes the engine's Kubernetes events into the trace.
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
