package types

import (
	"context"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CachedPodList is the Component's pods as the informer cache listed them
// for one pass, read by every cached reader of the pass: the plan's
// observation, the create pass and the adapter's status publication. The
// list is taken once and handed out again while the pass writes no pod;
// once the pass has written a pod, every reader lists again, since the
// cache shows a write some time after it and the list one reader took
// right after the write is not what a later reader would see. The pods
// are shared, not copied: a reader reads them and writes a pod through a
// fresh read of its own. So are the list's groups by Instance (ByInstance):
// built by the first reader that asks and read by every later one. One
// list per pass, used by one goroutine.
type CachedPodList struct {
	client client.Client
	pods   []*corev1.Pod
	taken  bool
	// byInstance is the list's pods grouped by Instance index, nil until
	// a reader asks for the groups of the list taken.
	byInstance map[int32][]*corev1.Pod
	// writes counts the pod writes made through Client this pass.
	writes int
	lists  int
}

// NewCachedPodList builds the list of one pass over c, the client the pass
// reads and writes through. Every pod write made through Client is seen by
// the list.
func NewCachedPodList(c client.Client) *CachedPodList {
	l := &CachedPodList{}
	l.client = podWriteObserver{Client: c, list: l}
	return l
}

// Client is the pass's client: the one the list was built over, with
// every pod write it makes reported to the list.
func (l *CachedPodList) Client() client.Client {
	return l.client
}

// Pods hands out the list taken this pass while the pass has written no
// pod, and takes it again through list for every reader once it has: the
// cache shows a write some time after it, so a list one reader took right
// after the write is not what a reader later in the pass would see. A nil
// CachedPodList lists every time.
func (l *CachedPodList) Pods(ctx context.Context, list func(context.Context) ([]*corev1.Pod, error)) ([]*corev1.Pod, error) {
	if l == nil {
		return list(ctx)
	}
	if l.taken && l.writes == 0 {
		return l.pods, nil
	}
	pods, err := list(ctx)
	if err != nil {
		return nil, err
	}
	l.pods, l.taken, l.byInstance = pods, true, nil
	l.lists++
	return pods, nil
}

// ByInstance is pods grouped by Instance index through group. The groups
// of the list this pass took are built once and handed out again to every
// reader that asks for that same list; any other list (one cut short on a
// missing Node, or a list taken before a pod write) is grouped anew. The
// groups are shared, not copied: a reader reads them and writes nothing
// through them. A nil CachedPodList groups every time.
func (l *CachedPodList) ByInstance(pods []*corev1.Pod, group func([]*corev1.Pod) map[int32][]*corev1.Pod) map[int32][]*corev1.Pod {
	if l == nil || !sameList(pods, l.pods) {
		return group(pods)
	}
	if l.byInstance == nil {
		l.byInstance = group(pods)
	}
	return l.byInstance
}

// sameList reports whether a and b are one list: the same length over the
// same array. Two empty lists are one.
func sameList(a, b []*corev1.Pod) bool {
	if len(a) != len(b) {
		return false
	}
	return len(a) == 0 || &a[0] == &b[0]
}

// Lists is how many times the pass took the list from the cache; 0 for a
// nil CachedPodList.
func (l *CachedPodList) Lists() int {
	if l == nil {
		return 0
	}
	return l.lists
}

// PodWrites is how many pod writes the list has seen; 0 for a nil
// CachedPodList.
func (l *CachedPodList) PodWrites() int {
	if l == nil {
		return 0
	}
	return l.writes
}

// podWritten records a write of obj when obj is a pod. A write is counted
// before it is sent, since a write that fails may still have reached the
// apiserver.
func (l *CachedPodList) podWritten(obj runtime.Object) {
	if _, ok := obj.(*corev1.Pod); ok {
		l.writes++
	}
}

// podApplied records an apply of obj when obj is a pod's apply
// configuration.
func (l *CachedPodList) podApplied(obj runtime.ApplyConfiguration) {
	if _, ok := obj.(*corev1apply.PodApplyConfiguration); ok {
		l.writes++
	}
}

// podWriteObserver is the pass's client with every pod write reported to
// the list: the writes made through the client itself, through its status
// writer and through any other subresource client it hands out.
type podWriteObserver struct {
	client.Client
	list *CachedPodList
}

func (o podWriteObserver) Create(ctx context.Context, obj client.Object, opts ...client.CreateOption) error {
	o.list.podWritten(obj)
	return o.Client.Create(ctx, obj, opts...)
}

func (o podWriteObserver) Delete(ctx context.Context, obj client.Object, opts ...client.DeleteOption) error {
	o.list.podWritten(obj)
	return o.Client.Delete(ctx, obj, opts...)
}

func (o podWriteObserver) Update(ctx context.Context, obj client.Object, opts ...client.UpdateOption) error {
	o.list.podWritten(obj)
	return o.Client.Update(ctx, obj, opts...)
}

func (o podWriteObserver) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.PatchOption) error {
	o.list.podWritten(obj)
	return o.Client.Patch(ctx, obj, patch, opts...)
}

func (o podWriteObserver) DeleteAllOf(ctx context.Context, obj client.Object, opts ...client.DeleteAllOfOption) error {
	o.list.podWritten(obj)
	return o.Client.DeleteAllOf(ctx, obj, opts...)
}

func (o podWriteObserver) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.ApplyOption) error {
	o.list.podApplied(obj)
	return o.Client.Apply(ctx, obj, opts...)
}

func (o podWriteObserver) Status() client.SubResourceWriter {
	return subResourceWriteObserver{SubResourceWriter: o.Client.Status(), list: o.list}
}

func (o podWriteObserver) SubResource(subResource string) client.SubResourceClient {
	sub := o.Client.SubResource(subResource)
	return subResourceObserver{SubResourceReader: sub, subResourceWriteObserver: subResourceWriteObserver{SubResourceWriter: sub, list: o.list}}
}

// subResourceWriteObserver is a subresource writer with every pod write
// reported to the list.
type subResourceWriteObserver struct {
	client.SubResourceWriter
	list *CachedPodList
}

func (o subResourceWriteObserver) Create(ctx context.Context, obj client.Object, subResource client.Object, opts ...client.SubResourceCreateOption) error {
	o.list.podWritten(obj)
	return o.SubResourceWriter.Create(ctx, obj, subResource, opts...)
}

func (o subResourceWriteObserver) Update(ctx context.Context, obj client.Object, opts ...client.SubResourceUpdateOption) error {
	o.list.podWritten(obj)
	return o.SubResourceWriter.Update(ctx, obj, opts...)
}

func (o subResourceWriteObserver) Patch(ctx context.Context, obj client.Object, patch client.Patch, opts ...client.SubResourcePatchOption) error {
	o.list.podWritten(obj)
	return o.SubResourceWriter.Patch(ctx, obj, patch, opts...)
}

func (o subResourceWriteObserver) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...client.SubResourceApplyOption) error {
	o.list.podApplied(obj)
	return o.SubResourceWriter.Apply(ctx, obj, opts...)
}

// subResourceObserver is a subresource client whose reads pass through and
// whose writes are reported to the list.
type subResourceObserver struct {
	client.SubResourceReader
	subResourceWriteObserver
}
