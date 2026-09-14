package worker

import (
	"fmt"
	"sync"

	appsv1 "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	clienttesting "k8s.io/client-go/testing"
	"sigs.k8s.io/ome/alfred-simulator/protocol"
)

var podsResource = schema.GroupVersionResource{Version: "v1", Resource: "pods"}

// privateState serializes binding, completion and denial. Once sealed, late
// asynchronous scheduler work cannot alter the result or any snapshot object.
type privateState struct {
	mu        sync.Mutex
	requested map[types.NamespacedName]*v1.Pod
	nodes     map[string]*v1.Node
	excluded  exclusions
	bound     map[types.NamespacedName]string
	completed map[types.NamespacedName]bool
	denied    error
	sealed    bool
	wake      chan struct{}
	active    map[types.UID]bool
	cycles    sync.WaitGroup
}

func (s *privateState) signal() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}
func (s *privateState) deny(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.denied == nil {
		s.denied = err
	}
	s.signal()
}
func newPrivateClient(r protocol.Request, snapshot *protocol.Snapshot) (*fake.Clientset, *privateState) {
	excluded := exclusions{}
	for _, node := range r.ExcludedNodes {
		excluded[node] = true
	}
	objects := make([]runtime.Object, 0, len(snapshot.Objects))
	for _, o := range snapshot.Objects {
		switch o.(type) {
		case *v1.Pod, *v1.Node, *v1.Namespace, *v1.Service, *v1.ReplicationController, *appsv1.ReplicaSet, *appsv1.StatefulSet:
			copy := o.DeepCopyObject()
			if node, ok := copy.(*v1.Node); ok && excluded[node.Name] {
				// Apply the hard exclusion before OME's domain planning, not
				// only in Filter: otherwise spare source capacity can attract
				// a gang whose worker affinity then waits for a rejected leader.
				// Only this private copy changes; all occupied Pods stay put.
				node.Spec.Unschedulable = true
			}
			objects = append(objects, copy)
		}
	}
	s := &privateState{requested: map[types.NamespacedName]*v1.Pod{}, nodes: snapshot.Nodes, excluded: excluded, bound: map[types.NamespacedName]string{}, completed: map[types.NamespacedName]bool{}, wake: make(chan struct{}, 1), active: map[types.UID]bool{}}
	usedUIDs := map[types.UID]bool{}
	for _, pod := range snapshot.Pods {
		usedUIDs[pod.UID] = true
	}
	for _, pod := range r.ReplacementPods {
		usedUIDs[pod.UID] = true
	}
	for i := range r.ReplacementPods {
		p := r.ReplacementPods[i].DeepCopy()
		if p.Spec.SchedulerName == "" {
			p.Spec.SchedulerName = v1.DefaultSchedulerName
		}
		if p.UID == "" {
			for n := i; ; n++ {
				uid := types.UID(fmt.Sprintf("alfred-private-replacement-%d", n))
				if !usedUIDs[uid] {
					p.UID = uid
					usedUIDs[uid] = true
					break
				}
			}
		}
		objects = append(objects, p)
		s.requested[types.NamespacedName{Namespace: p.Namespace, Name: p.Name}] = p.DeepCopy()
	}
	client := fake.NewClientset(objects...)
	client.PrependReactor("*", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetVerb() == "get" || action.GetVerb() == "list" || action.GetVerb() == "watch" {
			return false, nil, nil
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		reject := func(reason string) (bool, runtime.Object, error) {
			err := fmt.Errorf("private client denied %s %s/%s: %s", action.GetVerb(), action.GetResource().Resource, action.GetSubresource(), reason)
			if s.denied == nil {
				s.denied = err
			}
			s.signal()
			return true, nil, err
		}
		if s.sealed {
			return true, nil, fmt.Errorf("simulation is closed")
		}
		if action.GetResource() != podsResource {
			return reject("unexpected resource mutation")
		}
		if action.GetVerb() == "create" && action.GetSubresource() == "binding" {
			create, ok := action.(clienttesting.CreateAction)
			if !ok {
				return reject("invalid binding action")
			}
			binding, ok := create.GetObject().(*v1.Binding)
			if !ok {
				return reject("invalid binding object")
			}
			key := types.NamespacedName{Namespace: action.GetNamespace(), Name: binding.Name}
			expected := s.requested[key]
			if expected == nil || binding.UID != expected.UID || s.nodes[binding.Target.Name] == nil || s.excluded[binding.Target.Name] || s.bound[key] != "" {
				return reject("binding must uniquely place an authorized replacement on an allowed node")
			}
			obj, err := client.Tracker().Get(podsResource, key.Namespace, key.Name)
			if err != nil {
				return reject(err.Error())
			}
			pod := obj.(*v1.Pod).DeepCopy()
			if pod.UID != expected.UID || pod.Spec.NodeName != "" {
				return reject("replacement identity or binding changed")
			}
			pod.Spec.NodeName = binding.Target.Name
			if err := client.Tracker().Update(podsResource, pod, key.Namespace); err != nil {
				return reject(err.Error())
			}
			s.bound[key] = binding.Target.Name
			s.signal()
			return true, &metav1.Status{Status: "Success"}, nil
		}
		if action.GetVerb() == "patch" && action.GetSubresource() == "status" {
			patch, ok := action.(clienttesting.PatchAction)
			if !ok {
				return reject("invalid status patch")
			}
			key := types.NamespacedName{Namespace: action.GetNamespace(), Name: patch.GetName()}
			if s.requested[key] == nil {
				return reject("status mutation of non-request Pod")
			}
			return true, s.requested[key].DeepCopy(), nil
		}
		return reject("only replacement binding and private scheduler status recording are supported")
	})
	return client, s
}
