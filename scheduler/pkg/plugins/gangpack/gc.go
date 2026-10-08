package gangpack

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kube-scheduler/framework"
)

// gangPodLister yields the set of gang keys (namespace/podgroup) that still have
// at least one live pod. An interface so the GC is unit-testable without an
// informer.
type gangPodLister interface {
	liveGangKeys() (map[string]bool, error)
}

// gangMemberLister adds exact member enumeration for heterogeneous feasibility.
// Kept separate from gangPodLister so the GC remains independently testable.
type gangMemberLister interface {
	gangPods(namespace, name string) ([]*v1.Pod, error)
}

// runPinGC periodically reconciles the pin store against live gangs, releasing
// pins whose pods are all gone. It runs for the scheduler's lifetime (bounded by
// ctx) and is a no-op if no pod lister was wired.
func (g *GangPack) runPinGC(ctx context.Context) {
	if g.podLister == nil || g.gcInterval <= 0 {
		return
	}
	t := time.NewTicker(g.gcInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			g.gcPins()
		}
	}
}

// gcPins releases pins whose gangs no longer have live pods. Reservation
// accounting is owned synchronously by PreFilter/Reserve/PostFilter/Unreserve;
// a background goroutine must not read SnapshotSharedLister, whose data is only
// valid during a scheduling cycle and is mutated without reader locking.
func (g *GangPack) gcPins() {
	live, err := g.podLister.liveGangKeys()
	if err != nil {
		return
	}
	g.pruneTemplatesIncomplete()
	for group, owner := range g.pins.Owners() {
		stale := !live[group]
		// A live pod label only identifies namespace/name. Compare the immutable
		// PodGroup UID as well so deleting and recreating that name cannot keep or
		// inherit the former object's commitment.
		if !stale && owner != "" && g.pgReader != nil {
			namespace, name, ok := splitGangKey(group)
			if ok {
				_, _, _, currentOwner, found := g.pgReader.get(namespace, name)
				stale = !found || currentOwner != owner
			}
		}
		if !stale {
			continue
		}
		domain, token, _, found, ownerMatch := g.pins.GetOwnedBy(group, owner)
		if !found || !ownerMatch {
			continue
		}
		pin := &pinState{
			domain: domain.Name, topologyKey: domain.TopologyKey,
			gang: gangInfo{key: group, uid: owner}, commitment: token,
		}
		if released, _ := g.releaseAttempt(pin, nil, false); released {
			g.clearFailedDomains(pin.gang)
		}
	}
}

// informerPodLister is the live gangPodLister, backed by the scheduler's shared
// pod informer cache and filtered to pods that carry the pod-group label.
type informerPodLister struct {
	list      func() ([]*v1.Pod, error)
	listGroup func(namespace, name string) ([]*v1.Pod, error)
}

var _ gangPodLister = &informerPodLister{}

func (l *informerPodLister) liveGangKeys() (map[string]bool, error) {
	pods, err := l.list()
	if err != nil {
		return nil, err
	}
	keys := make(map[string]bool, len(pods))
	for _, p := range pods {
		if activeGangPod(p) {
			if ns, name, ok := podGroupNameOf(p); ok {
				keys[ns+"/"+name] = true
			}
		}
	}
	return keys, nil
}

func (l *informerPodLister) gangPods(namespace, name string) ([]*v1.Pod, error) {
	var (
		pods []*v1.Pod
		err  error
	)
	if l.listGroup != nil {
		pods, err = l.listGroup(namespace, name)
	} else {
		pods, err = l.list()
	}
	if err != nil {
		return nil, err
	}
	out := make([]*v1.Pod, 0, len(pods))
	for _, pod := range pods {
		ns, pg, ok := podGroupNameOf(pod)
		if ok && ns == namespace && pg == name && activeGangPod(pod) {
			out = append(out, pod)
		}
	}
	return out, nil
}

func activeGangPod(pod *v1.Pod) bool {
	return pod != nil && pod.DeletionTimestamp == nil && pod.Status.Phase != v1.PodSucceeded && pod.Status.Phase != v1.PodFailed
}

// newInformerPodLister builds a gang-pod lister from the scheduler's shared
// informer factory, selecting only pods that carry the pod-group label.
func newInformerPodLister(h framework.Handle) (*informerPodLister, error) {
	const gangKeyIndex = Name + "/pod-group"
	pods := h.SharedInformerFactory().Core().V1().Pods()
	informer := pods.Informer()
	if _, exists := informer.GetIndexer().GetIndexers()[gangKeyIndex]; !exists {
		if err := informer.AddIndexers(cache.Indexers{gangKeyIndex: func(obj interface{}) ([]string, error) {
			pod, ok := obj.(*v1.Pod)
			if !ok {
				return nil, nil
			}
			namespace, name, ok := podGroupNameOf(pod)
			if !ok {
				return nil, nil
			}
			return []string{namespace + "/" + name}, nil
		}}); err != nil {
			return nil, err
		}
	}
	lister := pods.Lister()
	req, _ := labels.NewRequirement(podGroupLabel, selection.Exists, nil)
	sel := labels.NewSelector().Add(*req)
	return &informerPodLister{
		list: func() ([]*v1.Pod, error) { return lister.List(sel) },
		listGroup: func(namespace, name string) ([]*v1.Pod, error) {
			objects, err := informer.GetIndexer().ByIndex(gangKeyIndex, namespace+"/"+name)
			if err != nil {
				return nil, err
			}
			out := make([]*v1.Pod, 0, len(objects))
			for _, object := range objects {
				if pod, ok := object.(*v1.Pod); ok {
					out = append(out, pod)
				}
			}
			return out, nil
		},
	}, nil
}
