package types

import (
	"context"
	"errors"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apitypes "k8s.io/apimachinery/pkg/types"
	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// listedPods is a lister that counts its calls and answers a fresh slice
// of the pods it is given each time.
type listedPods struct {
	pods  []*corev1.Pod
	calls int
	err   error
}

func (l *listedPods) list(context.Context) ([]*corev1.Pod, error) {
	l.calls++
	if l.err != nil {
		return nil, l.err
	}
	return append([]*corev1.Pod(nil), l.pods...), nil
}

func cachedPodListPod(name string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"}}
}

// TestCachedPodList_ListIsTakenOnceWhileNoPodIsWritten: the first reader
// takes the list and every later reader of the pass reads the same slice
// without a list, while no pod is written through the list's client.
func TestCachedPodList_ListIsTakenOnceWhileNoPodIsWritten(t *testing.T) {
	ctx := context.Background()
	lister := &listedPods{pods: []*corev1.Pod{cachedPodListPod("a"), cachedPodListPod("b")}}
	list := NewCachedPodList(fakeclient.New(t))
	first, err := list.Pods(ctx, lister.list)
	if err != nil {
		t.Fatalf("Pods: %v", err)
	}
	second, err := list.Pods(ctx, lister.list)
	if err != nil {
		t.Fatalf("Pods again: %v", err)
	}
	if lister.calls != 1 || list.Lists() != 1 {
		t.Fatalf("lists: the lister was called %d times and the list counts %d, want 1 and 1", lister.calls, list.Lists())
	}
	if len(first) != 2 || len(second) != 2 || &first[0] != &second[0] {
		t.Fatalf("the second reader got a list of its own: %v and %v", first, second)
	}
	if list.PodWrites() != 0 {
		t.Fatalf("pod writes: %d, want none", list.PodWrites())
	}
}

// TestCachedPodList_NilListListsEveryTime: a pass without a list, as the
// engine's unit tests and a pass that ends early run, lists on every read.
func TestCachedPodList_NilListListsEveryTime(t *testing.T) {
	ctx := context.Background()
	lister := &listedPods{pods: []*corev1.Pod{cachedPodListPod("a")}}
	var list *CachedPodList
	for i := 0; i < 3; i++ {
		pods, err := list.Pods(ctx, lister.list)
		if err != nil || len(pods) != 1 {
			t.Fatalf("Pods: %v, %v", pods, err)
		}
	}
	if lister.calls != 3 || list.Lists() != 0 || list.PodWrites() != 0 {
		t.Fatalf("a nil list: lister called %d times (want 3), lists %d (want 0), writes %d (want 0)", lister.calls, list.Lists(), list.PodWrites())
	}
}

// TestCachedPodList_AListErrorIsReturnedAndNothingIsKept: a list that
// fails is not kept, and the next reader lists again.
func TestCachedPodList_AListErrorIsReturnedAndNothingIsKept(t *testing.T) {
	ctx := context.Background()
	lister := &listedPods{err: errors.New("the cache is not ready")}
	list := NewCachedPodList(fakeclient.New(t))
	if _, err := list.Pods(ctx, lister.list); err == nil || err.Error() != "the cache is not ready" {
		t.Fatalf("Pods: error %v, want the lister's", err)
	}
	lister.err = nil
	lister.pods = []*corev1.Pod{cachedPodListPod("a")}
	pods, err := list.Pods(ctx, lister.list)
	if err != nil || len(pods) != 1 {
		t.Fatalf("Pods after the error: %v, %v", pods, err)
	}
	if lister.calls != 2 || list.Lists() != 1 {
		t.Fatalf("lister called %d times (want 2), lists kept %d (want 1)", lister.calls, list.Lists())
	}
}

// podWrite is one way a pass writes a pod through the client.
type podWrite struct {
	name  string
	write func(ctx context.Context, c client.Client, pod *corev1.Pod) error
}

func podWrites() []podWrite {
	mergePatch := client.RawPatch(apitypes.MergePatchType, []byte(`{"metadata":{"labels":{"written":"yes"}}}`))
	return []podWrite{
		{"create", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.Create(ctx, cachedPodListPod("new"))
		}},
		{"update", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.Update(ctx, pod)
		}},
		{"patch", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.Patch(ctx, pod, mergePatch)
		}},
		{"delete", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.Delete(ctx, pod)
		}},
		{"delete-all-of", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.DeleteAllOf(ctx, &corev1.Pod{}, client.InNamespace("ns"))
		}},
		{"apply", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.Apply(ctx, corev1apply.Pod(pod.Name, pod.Namespace), client.FieldOwner("test"), client.ForceOwnership)
		}},
		{"status-update", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.Status().Update(ctx, pod)
		}},
		{"status-patch", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.Status().Patch(ctx, pod, mergePatch)
		}},
		{"status-apply", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.Status().Apply(ctx, corev1apply.Pod(pod.Name, pod.Namespace), client.FieldOwner("test"), client.ForceOwnership)
		}},
		{"subresource-patch", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.SubResource("status").Patch(ctx, pod, mergePatch)
		}},
		{"subresource-update", func(ctx context.Context, c client.Client, pod *corev1.Pod) error {
			return c.SubResource("status").Update(ctx, pod)
		}},
	}
}

// TestCachedPodList_EveryReaderAfterAPodWriteListsAgain: a pod written
// through the list's client, by any verb, through the client itself, its
// status writer or another subresource client, makes every reader after
// it list again: the cache shows the write some time after it, so the
// list one reader took is not what the reader after it would see.
func TestCachedPodList_EveryReaderAfterAPodWriteListsAgain(t *testing.T) {
	ctx := context.Background()
	for _, write := range podWrites() {
		t.Run(write.name, func(t *testing.T) {
			pod := cachedPodListPod("a")
			c := fakeclient.New(t, fakeclient.WithObjects(pod), fakeclient.WithStatusSubresource(&corev1.Pod{}))
			lister := &listedPods{pods: []*corev1.Pod{pod}}
			list := NewCachedPodList(c)
			if _, err := list.Pods(ctx, lister.list); err != nil {
				t.Fatalf("Pods: %v", err)
			}
			if err := write.write(ctx, list.Client(), pod.DeepCopy()); err != nil {
				t.Fatalf("%s: %v", write.name, err)
			}
			if list.PodWrites() != 1 {
				t.Fatalf("pod writes after %s: %d, want 1", write.name, list.PodWrites())
			}
			for i := 0; i < 2; i++ {
				if _, err := list.Pods(ctx, lister.list); err != nil {
					t.Fatalf("Pods after %s: %v", write.name, err)
				}
			}
			if lister.calls != 3 || list.Lists() != 3 {
				t.Fatalf("after %s: lister called %d times (want 3: once before the write, once per reader after it), lists %d", write.name, lister.calls, list.Lists())
			}
		})
	}
}

// TestCachedPodList_AWriteOfAnotherKindKeepsTheList: a write that is not a
// pod's, a ControllerRevision's here, leaves the list as it is: the next
// reader does not list again.
func TestCachedPodList_AWriteOfAnotherKindKeepsTheList(t *testing.T) {
	ctx := context.Background()
	c := fakeclient.New(t)
	lister := &listedPods{pods: []*corev1.Pod{cachedPodListPod("a")}}
	list := NewCachedPodList(c)
	if _, err := list.Pods(ctx, lister.list); err != nil {
		t.Fatalf("Pods: %v", err)
	}
	revision := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: "rev", Namespace: "ns"}}
	if err := list.Client().Create(ctx, revision); err != nil {
		t.Fatalf("create the revision: %v", err)
	}
	if err := list.Client().Delete(ctx, revision); err != nil {
		t.Fatalf("delete the revision: %v", err)
	}
	if _, err := list.Pods(ctx, lister.list); err != nil {
		t.Fatalf("Pods again: %v", err)
	}
	if list.PodWrites() != 0 || lister.calls != 1 {
		t.Fatalf("after two revision writes: pod writes %d (want 0), lister called %d times (want 1)", list.PodWrites(), lister.calls)
	}
}

// TestCachedPodList_AFailedPodWriteCountsToo: a pod write the apiserver
// refuses is counted all the same, since it may have reached the apiserver.
func TestCachedPodList_AFailedPodWriteCountsToo(t *testing.T) {
	ctx := context.Background()
	c := fakeclient.New(t)
	lister := &listedPods{pods: []*corev1.Pod{cachedPodListPod("a")}}
	list := NewCachedPodList(c)
	if _, err := list.Pods(ctx, lister.list); err != nil {
		t.Fatalf("Pods: %v", err)
	}
	if err := list.Client().Delete(ctx, cachedPodListPod("missing")); err == nil {
		t.Fatal("deleting a pod the store does not hold must fail")
	}
	if _, err := list.Pods(ctx, lister.list); err != nil {
		t.Fatalf("Pods again: %v", err)
	}
	if list.PodWrites() != 1 || lister.calls != 2 {
		t.Fatalf("after a refused delete: pod writes %d (want 1), lister called %d times (want 2)", list.PodWrites(), lister.calls)
	}
}

// TestCachedPodList_ClientReadsPassThrough: the list's client reads as the
// client it was built over, and a read counts as no write.
func TestCachedPodList_ClientReadsPassThrough(t *testing.T) {
	ctx := context.Background()
	pod := cachedPodListPod("a")
	list := NewCachedPodList(fakeclient.New(t, fakeclient.WithObjects(pod)))
	got := &corev1.Pod{}
	if err := list.Client().Get(ctx, client.ObjectKeyFromObject(pod), got); err != nil {
		t.Fatalf("Get: %v", err)
	}
	pods := &corev1.PodList{}
	if err := list.Client().List(ctx, pods, client.InNamespace("ns")); err != nil || len(pods.Items) != 1 {
		t.Fatalf("List: %d pods, %v", len(pods.Items), err)
	}
	if list.PodWrites() != 0 {
		t.Fatalf("reads counted as writes: %d", list.PodWrites())
	}
}

// countingGroups groups pods by their name's length, as a stand-in for the
// groups by Instance, and counts how often it is asked.
type countingGroups struct{ calls int }

func (g *countingGroups) group(pods []*corev1.Pod) map[int32][]*corev1.Pod {
	g.calls++
	by := map[int32][]*corev1.Pod{}
	for _, pod := range pods {
		by[int32(len(pod.Name))] = append(by[int32(len(pod.Name))], pod)
	}
	return by
}

// TestCachedPodList_GroupsAreBuiltOnceForTheListTaken: the first reader
// that asks for the groups of the list the pass took builds them, and
// every later reader of that list is handed the same groups.
func TestCachedPodList_GroupsAreBuiltOnceForTheListTaken(t *testing.T) {
	ctx := context.Background()
	lister := &listedPods{pods: []*corev1.Pod{cachedPodListPod("a"), cachedPodListPod("bb"), cachedPodListPod("c")}}
	list := NewCachedPodList(fakeclient.New(t))
	pods, err := list.Pods(ctx, lister.list)
	if err != nil {
		t.Fatalf("Pods: %v", err)
	}
	groups := &countingGroups{}
	first := list.ByInstance(pods, groups.group)
	again, err := list.Pods(ctx, lister.list)
	if err != nil {
		t.Fatalf("Pods again: %v", err)
	}
	second := list.ByInstance(again, groups.group)
	if groups.calls != 1 {
		t.Fatalf("the pods were grouped %d times, want once", groups.calls)
	}
	if len(first) != 2 || len(first[1]) != 2 || len(first[2]) != 1 {
		t.Fatalf("groups = %v, want two of length 1 and one of length 2", first)
	}
	if &first[1][0] != &second[1][0] || &first[2][0] != &second[2][0] {
		t.Fatalf("the second reader got groups of its own")
	}
}

// TestCachedPodList_AnotherListIsGroupedOnItsOwn: a reader that asks for
// the groups of a list that is not the one the pass took, one cut short or
// a copy, gets groups of that list alone, and the list's own groups stay.
func TestCachedPodList_AnotherListIsGroupedOnItsOwn(t *testing.T) {
	ctx := context.Background()
	lister := &listedPods{pods: []*corev1.Pod{cachedPodListPod("a"), cachedPodListPod("bb"), cachedPodListPod("c")}}
	list := NewCachedPodList(fakeclient.New(t))
	pods, err := list.Pods(ctx, lister.list)
	if err != nil {
		t.Fatalf("Pods: %v", err)
	}
	groups := &countingGroups{}
	whole := list.ByInstance(pods, groups.group)
	short := list.ByInstance(pods[:2], groups.group)
	copied := list.ByInstance(append([]*corev1.Pod(nil), pods...), groups.group)
	if groups.calls != 3 {
		t.Fatalf("the pods were grouped %d times, want three: the list, the short list and the copy", groups.calls)
	}
	if len(short[1]) != 1 || len(copied[1]) != 2 {
		t.Fatalf("short = %v, copied = %v", short, copied)
	}
	if again := list.ByInstance(pods, groups.group); groups.calls != 3 || &again[1][0] != &whole[1][0] {
		t.Fatalf("the list's own groups were not kept: grouped %d times", groups.calls)
	}
}

// TestCachedPodList_GroupsFollowTheListTakenAgain: after a pod write the
// list is taken again, and the groups of the list taken before it are not
// handed out for the new list.
func TestCachedPodList_GroupsFollowTheListTakenAgain(t *testing.T) {
	ctx := context.Background()
	lister := &listedPods{pods: []*corev1.Pod{cachedPodListPod("a"), cachedPodListPod("bb")}}
	list := NewCachedPodList(fakeclient.New(t))
	before, err := list.Pods(ctx, lister.list)
	if err != nil {
		t.Fatalf("Pods: %v", err)
	}
	groups := &countingGroups{}
	list.ByInstance(before, groups.group)
	if err := list.Client().Create(ctx, cachedPodListPod("written")); err != nil {
		t.Fatalf("Create: %v", err)
	}
	lister.pods = append(lister.pods, cachedPodListPod("ddd"))
	after, err := list.Pods(ctx, lister.list)
	if err != nil {
		t.Fatalf("Pods after the write: %v", err)
	}
	fresh := list.ByInstance(after, groups.group)
	if groups.calls != 2 || len(fresh) != 3 {
		t.Fatalf("the new list was grouped %d times in all and has %d groups, want 2 and 3", groups.calls, len(fresh))
	}
	if list.ByInstance(before, groups.group); groups.calls != 3 {
		t.Fatalf("the list taken before the write was handed the new list's groups")
	}
}

// TestCachedPodList_NilListGroupsEveryTime: a pass without a list groups
// on every read, as it lists on every read.
func TestCachedPodList_NilListGroupsEveryTime(t *testing.T) {
	var list *CachedPodList
	pods := []*corev1.Pod{cachedPodListPod("a")}
	groups := &countingGroups{}
	list.ByInstance(pods, groups.group)
	list.ByInstance(pods, groups.group)
	if groups.calls != 2 {
		t.Fatalf("the pods were grouped %d times, want twice", groups.calls)
	}
}
