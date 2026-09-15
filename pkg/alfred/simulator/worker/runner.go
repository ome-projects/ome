package worker

import (
	"context"
	"fmt"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/dynamic/dynamicinformer"
	dynamicfake "k8s.io/client-go/dynamic/fake"
	"k8s.io/client-go/rest"
	clienttesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/events"
	fwk "k8s.io/kube-scheduler/framework"
	"k8s.io/kubernetes/pkg/scheduler"
	config "k8s.io/kubernetes/pkg/scheduler/apis/config"
	"k8s.io/kubernetes/pkg/scheduler/framework"
	frameworkruntime "k8s.io/kubernetes/pkg/scheduler/framework/runtime"
	"sigs.k8s.io/ome/alfred-simulator/protocol"
	"sigs.k8s.io/ome/scheduler/pkg/plugins/gangpack"
)

// Evaluate always constructs a fresh scheduler. Unsupported outcomes carry no
// placements, including cancellation and any attempt to mutate occupied Pods.
func (p *Profile) Evaluate(ctx context.Context, r protocol.Request) (protocol.Result, error) {
	unsupported := protocol.ResultFor(r, protocol.DecisionUnsupported)
	if p == nil || p.config == nil {
		return unsupported, fmt.Errorf("profile is not initialized")
	}
	if p.Identity != p.identity || p.GangScheduling != p.gang || r.Profile != p.identity || !reflect.DeepEqual(featureState(), p.gates) {
		return unsupported, fmt.Errorf("request does not match immutable compiled scheduler profile")
	}
	snapshot, err := protocol.Validate(r)
	if err != nil {
		return unsupported, err
	}
	if err := validateGroups(r, snapshot, p.gang); err != nil {
		return unsupported, err
	}
	if ctx.Err() != nil {
		return unsupported, nil
	}
	// A library caller cannot leave the worker running forever.
	ctx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	client, state := newPrivateClient(r, snapshot)
	defer func() { state.mu.Lock(); state.sealed = true; state.mu.Unlock(); cancel(); state.cycles.Wait() }()
	informer := scheduler.NewInformerFactory(client, 0)
	dyn := dynamicfake.NewSimpleDynamicClientWithCustomListKinds(runtime.NewScheme(), map[schema.GroupVersionResource]string{{Group: "scheduling.x-k8s.io", Version: "v1alpha1", Resource: "podgroups"}: "PodGroupList"})
	dyn.PrependReactor("*", "*", func(action clienttesting.Action) (bool, runtime.Object, error) {
		if action.GetVerb() == "list" || action.GetVerb() == "get" || action.GetVerb() == "watch" {
			return false, nil, nil
		}
		err := fmt.Errorf("dynamic snapshot client denied %s %s", action.GetVerb(), action.GetResource().Resource)
		state.deny(err)
		return true, nil, err
	})
	dinformer := dynamicinformer.NewDynamicSharedInformerFactory(dyn, 0)
	configuration := p.config.DeepCopy()
	pr := &configuration.Profiles[0]
	pr.Plugins.Filter.Enabled = append(pr.Plugins.Filter.Enabled, config.Plugin{Name: exclusionPlugin})
	transport := &snapshotTransport{ctx: ctx, groups: snapshot.PodGroups}
	kubeconfig := &rest.Config{Host: "https://snapshot.invalid", Transport: transport, ContentConfig: rest.ContentConfig{ContentType: "application/json"}}
	// No event sink is installed: events cannot become external writes.
	recorder := &events.FakeRecorder{}
	sched, err := scheduler.New(ctx, client, informer, dinformer, func(string) events.EventRecorder { return recorder },
		scheduler.WithKubeConfig(kubeconfig), scheduler.WithProfiles(configuration.Profiles...), scheduler.WithComponentConfigVersion(configuration.APIVersion),
		scheduler.WithParallelism(configuration.Parallelism), scheduler.WithPercentageOfNodesToScore(configuration.PercentageOfNodesToScore),
		scheduler.WithPodInitialBackoffSeconds(configuration.PodInitialBackoffSeconds), scheduler.WithPodMaxBackoffSeconds(configuration.PodMaxBackoffSeconds),
		scheduler.WithFrameworkOutOfTreeRegistry(frameworkruntime.Registry{
			gangpack.Name: func(ctx context.Context, args runtime.Object, handle fwk.Handle) (fwk.Plugin, error) {
				plugin, err := gangpack.New(ctx, args, handle)
				if err != nil {
					return nil, err
				}
				gang, ok := plugin.(*gangpack.GangPack)
				if !ok {
					return nil, fmt.Errorf("unexpected OMEGangPack plugin type %T", plugin)
				}
				return &excludedGangPack{GangPack: gang, excluded: state.excluded}, nil
			},
			exclusionPlugin: func(context.Context, runtime.Object, fwk.Handle) (fwk.Plugin, error) { return state.excluded, nil },
		}))
	if err != nil {
		if ctx.Err() != nil {
			return unsupported, nil
		}
		return unsupported, fmt.Errorf("initialize scheduler: %w", err)
	}
	// Delegation preserves every configured hook; observation happens only after
	// the real PostBind plugins have completed, including OME's commitment update.
	for name, f := range sched.Profiles {
		sched.Profiles[name] = &observedFramework{Framework: f, state: state}
	}
	originalFailure := sched.FailureHandler
	sched.FailureHandler = func(ctx context.Context, f framework.Framework, info *framework.QueuedPodInfo, status *fwk.Status, nomination *fwk.NominatingInfo, start time.Time) {
		// Cancellation has no next scheduling attempt. Avoid updating status or
		// requeueing against the upstream queue while Run closes it.
		if ctx.Err() != nil {
			state.finishCycle(info.Pod.UID)
			return
		}
		originalFailure(ctx, f, info, status, nomination, start)
		state.finishCycle(info.Pod.UID)
		if !status.IsRejected() {
			state.deny(fmt.Errorf("scheduler plugin failure: %s", status.Message()))
		}
		// Gang rejection is not terminal: OME uses subsequent queue activations to
		// retry another domain. Single-Pod failure is conservatively Unsupported.
		key := types.NamespacedName{Namespace: info.Pod.Namespace, Name: info.Pod.Name}
		if state.replacements[key] && len(r.ReplacementPods) == 1 && !p.gang {
			state.deny(fmt.Errorf("single Pod scheduling did not complete: %s", status.Message()))
		}
	}
	informer.Start(ctx.Done())
	dinformer.Start(ctx.Done())
	defer func() { cancel(); informer.Shutdown(); dinformer.Shutdown() }()
	for _, synced := range informer.WaitForCacheSync(ctx.Done()) {
		if !synced {
			return unsupported, nil
		}
	}
	for _, synced := range dinformer.WaitForCacheSync(ctx.Done()) {
		if !synced {
			return unsupported, nil
		}
	}
	if err := sched.WaitForHandlersSync(ctx); err != nil {
		return unsupported, nil
	}
	runDone := make(chan struct{})
	go func() { defer close(runDone); sched.Run(ctx) }()
	defer func() { cancel(); <-runDone }()
	for {
		select {
		case <-ctx.Done():
			return unsupported, nil
		case <-state.wake:
		}
		state.mu.Lock()
		if state.denied != nil {
			state.mu.Unlock()
			return unsupported, nil
		}
		if len(state.completed) != len(r.ReplacementPods) {
			state.mu.Unlock()
			continue
		}
		if ctx.Err() != nil {
			state.mu.Unlock()
			return unsupported, nil
		}
		result := protocol.ResultFor(r, protocol.DecisionFeasible)
		for _, pod := range r.ReplacementPods {
			key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
			node := state.bound[key]
			if node == "" {
				state.mu.Unlock()
				return unsupported, fmt.Errorf("post-bind completed without private binding")
			}
			result.Placements = append(result.Placements, protocol.Placement{Pod: protocol.PodIdentity{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}, NodeName: node})
		}
		state.sealed = true
		state.mu.Unlock()
		return result, nil
	}
}

type observedFramework struct {
	framework.Framework
	state *privateState
}

func (f *observedFramework) RunPostBindPlugins(ctx context.Context, cycle fwk.CycleState, pod *v1.Pod, node string) {
	defer f.state.finishCycle(pod.UID)
	f.Framework.RunPostBindPlugins(ctx, cycle, pod, node)
	f.state.mu.Lock()
	defer f.state.mu.Unlock()
	key := types.NamespacedName{Namespace: pod.Namespace, Name: pod.Name}
	if !f.state.sealed && f.state.replacements[key] {
		f.state.completed[key] = true
		f.state.signal()
	}
}
func (f *observedFramework) RunReservePluginsReserve(ctx context.Context, cycle fwk.CycleState, pod *v1.Pod, node string) *fwk.Status {
	f.state.mu.Lock()
	if f.state.sealed || ctx.Err() != nil {
		f.state.mu.Unlock()
		return fwk.NewStatus(fwk.Error, "simulation closed")
	}
	if !f.state.active[pod.UID] {
		f.state.active[pod.UID] = true
		f.state.cycles.Add(1)
	}
	f.state.mu.Unlock()
	return f.Framework.RunReservePluginsReserve(ctx, cycle, pod, node)
}
func (f *observedFramework) WaitOnPermit(ctx context.Context, pod *v1.Pod) *fwk.Status {
	stop := context.AfterFunc(ctx, func() { f.Framework.RejectWaitingPod(pod.UID) })
	defer stop()
	return f.Framework.WaitOnPermit(ctx, pod)
}
func (s *privateState) finishCycle(uid types.UID) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.active[uid] {
		delete(s.active, uid)
		s.cycles.Done()
	}
}
