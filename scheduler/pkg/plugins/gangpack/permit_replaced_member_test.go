package gangpack

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	clientfeatures "k8s.io/client-go/features"
	clientfeaturestesting "k8s.io/client-go/features/testing"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kube-scheduler/framework"
)

// TestRejectReplacedWaiter_OnlyAWaitingMemberWithANewUID_IsRejected: the handler
// rejects a gang member's waiting entry when an update carries a new UID under
// the pod's name, and nothing else: an update that keeps the UID, a replaced pod
// outside any gang, and a replaced member that is not at the gate leave the
// pool as it was.
func TestRejectReplacedWaiter_OnlyAWaitingMemberWithANewUID_IsRejected(t *testing.T) {
	before := namedMember("worker-3")
	before.UID = "worker-3-before-recreation"
	recreated := namedMember("worker-3")
	statusWrite := before.DeepCopy()
	statusWrite.Status.Phase = v1.PodPending
	lone := gpuPod("4")
	lone.Namespace, lone.Name, lone.UID = "team", "lone", "lone-before-recreation"
	loneRecreated := lone.DeepCopy()
	loneRecreated.UID = "lone"

	for name, tc := range map[string]struct {
		old, updated *v1.Pod
		waiting      *v1.Pod
		wantRejected bool
	}{
		"a member recreated under its name": {old: before, updated: recreated, waiting: before, wantRejected: true},
		"a status write that keeps the UID": {old: before, updated: statusWrite, waiting: before},
		"a pod outside any gang":            {old: lone, updated: loneRecreated, waiting: lone},
		"a replaced member not at the gate": {old: before, updated: recreated, waiting: namedMember("worker-5")},
	} {
		t.Run(name, func(t *testing.T) {
			waiting := &fakeWaitingPod{pod: tc.waiting}
			h := &fakeHandle{waiting: []framework.WaitingPod{waiting}}
			g := &GangPack{handle: h}
			g.rejectReplacedWaiter(tc.old, tc.updated)
			if waiting.rejected != tc.wantRejected {
				t.Fatalf("rejected = %v, want %v", waiting.rejected, tc.wantRejected)
			}
			if stillWaiting := h.GetWaitingPod(tc.waiting.UID) != nil; stillWaiting == tc.wantRejected {
				t.Fatalf("entry %s at the gate = %v, want %v", tc.waiting.UID, stillWaiting, !tc.wantRejected)
			}
		})
	}
	// The decision core under unit test has no scheduler handle, so no gate.
	(&GangPack{}).rejectReplacedWaiter(before, recreated)
}

// signalledWaitingPod is a fakeWaitingPod whose rejection the test goroutine can
// wait for while an informer goroutine rejects it.
type signalledWaitingPod struct {
	fakeWaitingPod
	rejection chan struct{}
	once      sync.Once
}

func (w *signalledWaitingPod) Reject(plugin, msg string) {
	w.fakeWaitingPod.Reject(plugin, msg)
	w.once.Do(func() { close(w.rejection) })
}

// replaceablePodList is the pod list a fake clientset serves an informer, which
// the test rewrites between the informer's first list and its relist, with the
// watches the informer opens.
type replaceablePodList struct {
	mu      sync.Mutex
	pods    []*v1.Pod
	version string
	watches chan *watch.FakeWatcher
}

// newReplaceablePodList serves pods to the informer: the list reactor answers
// with the current pods, the watch reactor hands each watch to the test.
func newReplaceablePodList(pods []*v1.Pod) (*fake.Clientset, *replaceablePodList) {
	list := &replaceablePodList{pods: append([]*v1.Pod(nil), pods...), version: "1", watches: make(chan *watch.FakeWatcher, 4)}
	client := fake.NewClientset()
	client.PrependReactor("list", "pods", func(clienttesting.Action) (bool, runtime.Object, error) {
		list.mu.Lock()
		defer list.mu.Unlock()
		podList := &v1.PodList{ListMeta: metav1.ListMeta{ResourceVersion: list.version}}
		for _, pod := range list.pods {
			podList.Items = append(podList.Items, *pod.DeepCopy())
		}
		return true, podList, nil
	})
	client.PrependWatchReactor("pods", func(clienttesting.Action) (bool, watch.Interface, error) {
		w := watch.NewFake()
		list.watches <- w
		return true, w, nil
	})
	return client, list
}

// replace puts pod in place of the pod at index for the next list.
func (l *replaceablePodList) replace(index int, pod *v1.Pod, version string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pods[index], l.version = pod, version
}

// firstWatch is the watch the informer opened after its first list.
func (l *replaceablePodList) firstWatch(t *testing.T) *watch.FakeWatcher {
	t.Helper()
	select {
	case w := <-l.watches:
		return w
	case <-time.After(30 * time.Second):
		t.Fatal("the informer opened no watch within 30s")
		return nil
	}
}

// uidChanges reports, from the informer's own handler chain, each update whose
// old and new pods differ by UID, so a missing rejection is told apart from a
// relist that never happened. A relist also delivers every unchanged pod as an
// update with its UID kept, which the plugin's handler must pass over.
func uidChanges(t *testing.T, informer cache.SharedIndexInformer) <-chan [2]types.UID {
	t.Helper()
	changes := make(chan [2]types.UID, 8)
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		UpdateFunc: func(oldObj, newObj interface{}) {
			update := [2]types.UID{oldObj.(*v1.Pod).UID, newObj.(*v1.Pod).UID}
			if update[0] == update[1] {
				return
			}
			select {
			case changes <- update:
			default:
			}
		},
	})
	if err != nil {
		t.Fatalf("add the observer: %v", err)
	}
	return changes
}

// waitingSiblings is one waiting entry per member other than the replaced one,
// as the entries the test reads and as the gate's pool.
func waitingSiblings(members []*v1.Pod, replaced *v1.Pod) (entries []*fakeWaitingPod, pool []framework.WaitingPod) {
	for _, member := range members {
		if member != replaced {
			entry := &fakeWaitingPod{pod: member}
			entries = append(entries, entry)
			pool = append(pool, entry)
		}
	}
	return entries, pool
}

// TestNew_MemberRecreatedUnderItsNameInOneRelist_RejectsItsWaitingEntry: a gang
// of 1 leader and 7 workers has 7 members waiting at the gate for the eighth.
// One worker is deleted and recreated under its name while the pod watch is
// down, so the relist delivers the deletion and the recreation as one update:
// the same name, a new UID. The plugin New builds rejects the old pod's entry
// on that update, and the six other entries stay. The informer is the real
// shared one over a fake clientset: its first watch ends with 410 Gone and the
// relist returns the recreated pod.
func TestNew_MemberRecreatedUnderItsNameInOneRelist_RejectsItsWaitingEntry(t *testing.T) {
	// The fake clientset's watch never ends its initial events, so a streaming
	// list would leave the informer unsynced; list then watch instead.
	clientfeaturestesting.SetFeatureDuringTest(t, clientfeatures.WatchListClient, false)
	ctx, cancel := context.WithCancel(context.Background())

	members := []*v1.Pod{namedMember("leader")}
	for i := 1; i <= 6; i++ {
		members = append(members, namedMember(fmt.Sprintf("worker-%d", i)))
	}
	before := members[3]
	before.UID, before.ResourceVersion = "worker-3-before-recreation", "1"
	recreated := namedMember("worker-3")
	recreated.ResourceVersion = "2"

	client, served := newReplaceablePodList(members)
	factory := informers.NewSharedInformerFactory(client, 0)
	defer func() {
		cancel()
		factory.Shutdown()
	}()
	stale := &signalledWaitingPod{fakeWaitingPod: fakeWaitingPod{pod: before}, rejection: make(chan struct{})}
	others, pool := waitingSiblings(members, before)
	h := &fakeHandle{waiting: append([]framework.WaitingPod{stale}, pool...), informerFactory: factory, kubeConfig: &rest.Config{Host: "https://127.0.0.1:1"}}
	if _, err := New(ctx, &runtime.Unknown{Raw: []byte(`{"gcIntervalSeconds": 3600}`)}, h); err != nil {
		t.Fatalf("New: %v", err)
	}
	changes := uidChanges(t, factory.Core().V1().Pods().Informer())
	factory.Start(ctx.Done())
	factory.WaitForCacheSync(ctx.Done())

	firstWatch := served.firstWatch(t)
	served.replace(3, recreated, "2")
	// 410 Gone ends the watch; the reflector lists again and finds the
	// recreated pod under the deleted one's key.
	firstWatch.Error(&metav1.Status{Code: http.StatusGone, Reason: metav1.StatusReasonGone, Message: "too old resource version"})

	merged := [2]types.UID{before.UID, recreated.UID}
	select {
	case update := <-changes:
		if update != merged {
			t.Fatalf("the relist delivered the update %v, want the deletion and the recreation of worker-3 as one update %v", update, merged)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("the relist delivered no update within 30s")
	}
	select {
	case <-stale.rejection:
	case <-time.After(10 * time.Second):
		t.Fatalf("the informer delivered the deletion and the recreation of team/worker-3 as one update (UID %s -> %s); 10s later the old pod's entry still waits at the gate", before.UID, recreated.UID)
	}
	if h.GetWaitingPod(before.UID) != nil {
		t.Fatal("the old pod's entry is still in the pool after its rejection")
	}
	for _, other := range others {
		if other.rejected {
			t.Fatalf("%s was rejected; only the replaced member's entry leaves the gate", other.pod.Name)
		}
	}
}
