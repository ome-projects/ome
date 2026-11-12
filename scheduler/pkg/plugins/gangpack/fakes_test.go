package gangpack

import (
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/kube-scheduler/framework"
	schedulerframework "k8s.io/kubernetes/pkg/scheduler/framework"

	"sigs.k8s.io/ome/scheduler/pkg/placement"
	"sigs.k8s.io/ome/scheduler/pkg/topology"
)

func newCycleState() framework.CycleState { return schedulerframework.NewCycleState() }

// choose asks the store for a domain for a group that names no owner, topology
// key or nodes, and answers with the domain alone.
func choose(a *placement.DomainAssignments, group string, free topology.FreeByDomain, size int) (string, bool) {
	domain, _, ok := a.Choose(placement.Gang{Group: group}, free, nil, size)
	return domain, ok
}

// assignedDomain is the domain the group holds, or "" and false.
func assignedDomain(a *placement.DomainAssignments, group string) (string, bool) {
	domain, _, _, ok := a.GetOwned(group)
	return domain.Name, ok
}

// fakeWaitingPod is a minimal framework.WaitingPod for tests: it records whether
// Allow/Reject was called. The embedded nil interface satisfies the methods we
// don't exercise (they panic if called, which surfaces accidental use).
type fakeWaitingPod struct {
	framework.WaitingPod
	pod      *v1.Pod
	allowed  bool
	rejected bool
}

func (w *fakeWaitingPod) GetPod() *v1.Pod       { return w.pod }
func (w *fakeWaitingPod) Allow(string)          { w.allowed = true }
func (w *fakeWaitingPod) Reject(string, string) { w.rejected = true }

// fakeHandle is a minimal framework.Handle exposing the waiting-pod lookup and
// rejection the gate uses, the iteration the unwind uses, the informer factory
// and kubeconfig New reads, plus a node snapshot some tests build, which the
// plugin does not read. The embedded nil Handle satisfies the rest.
type fakeHandle struct {
	framework.Handle
	waiting         []framework.WaitingPod
	snapshot        []framework.NodeInfo
	activated       map[string]*v1.Pod
	activateCalls   int
	informerFactory informers.SharedInformerFactory
	kubeConfig      *rest.Config
}

// GetWaitingPod finds a waiting pod by UID, as the framework's map does; a
// pod without a UID is never found.
func (h *fakeHandle) GetWaitingPod(uid types.UID) framework.WaitingPod {
	if uid == "" {
		return nil
	}
	for _, wp := range h.waiting {
		if wp.GetPod().UID == uid {
			return wp
		}
	}
	return nil
}

// RejectWaitingPod rejects the waiting pod with the UID and reports whether one
// was waiting, as the framework does. The entry leaves the pool before the
// rejection is delivered, the order a test waiting on the rejection relies on.
func (h *fakeHandle) RejectWaitingPod(uid types.UID) bool {
	if uid == "" {
		return false
	}
	for i, wp := range h.waiting {
		if wp.GetPod().UID == uid {
			h.waiting = append(h.waiting[:i:i], h.waiting[i+1:]...)
			wp.Reject("", "removed")
			return true
		}
	}
	return false
}

func (h *fakeHandle) SharedInformerFactory() informers.SharedInformerFactory {
	return h.informerFactory
}

func (h *fakeHandle) KubeConfig() *rest.Config { return h.kubeConfig }

func (h *fakeHandle) Activate(_ klog.Logger, pods map[string]*v1.Pod) {
	h.activateCalls++
	if h.activated == nil {
		h.activated = make(map[string]*v1.Pod)
	}
	for key, pod := range pods {
		h.activated[key] = pod
	}
}

func (h *fakeHandle) IterateOverWaitingPods(f func(framework.WaitingPod)) {
	for _, wp := range h.waiting {
		f(wp)
	}
}

func (h *fakeHandle) SnapshotSharedLister() framework.SharedLister {
	return &fakeSharedLister{nodes: h.snapshot}
}

// fakeSharedLister / fakeNodeInfoLister expose a fixed node slice; only List is
// exercised (by boundSiblings). The rest return empty/not-found.
type fakeSharedLister struct{ nodes []framework.NodeInfo }

func (l *fakeSharedLister) NodeInfos() framework.NodeInfoLister {
	return &fakeNodeInfoLister{nodes: l.nodes}
}
func (l *fakeSharedLister) StorageInfos() framework.StorageInfoLister { return nil }

type fakeNodeInfoLister struct{ nodes []framework.NodeInfo }

func (l *fakeNodeInfoLister) List() ([]framework.NodeInfo, error) { return l.nodes, nil }
func (l *fakeNodeInfoLister) HavePodsWithAffinityList() ([]framework.NodeInfo, error) {
	return nil, nil
}
func (l *fakeNodeInfoLister) HavePodsWithRequiredAntiAffinityList() ([]framework.NodeInfo, error) {
	return nil, nil
}
func (l *fakeNodeInfoLister) Get(string) (framework.NodeInfo, error) { return nil, nil }
