package scaling

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// releaseFixture is 18 engines under an external scaler with the decoder
// following at 0.666 above a minReplicas of 2, and a router following at 0.25
// when the test asks for one. The decoder declares autoscaler class None
// unless the test gives it no block.
type releaseFixture struct {
	t          *testing.T
	ctx        context.Context
	store      client.Client
	projector  *projection
	recorder   *record.FakeRecorder
	parent     *v1beta1.InferenceService
	policy     *v1beta1.ScalingPolicy
	components map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec
	modes      map[v1beta1.ComponentType]constants.DeploymentModeType
}

type releaseOptions struct {
	router         bool
	decoderNoBlock bool
}

func newReleaseFixture(t *testing.T, opts releaseOptions) *releaseFixture {
	t.Helper()
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving", Generation: 1}}
	replica := func(component v1beta1.ComponentType, count int32, autoscaler *v1beta1.ComponentAutoscaler) *v1beta1.InferenceReplica {
		return &v1beta1.InferenceReplica{
			ObjectMeta: metav1.ObjectMeta{Name: "service-" + string(component), Namespace: "serving"},
			Spec: v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: parent.Name},
				Component: component, Replicas: ptr.To(count), Autoscaler: autoscaler},
		}
	}
	none := &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone}
	objects := []client.Object{
		replica(v1beta1.EngineComponent, 18, &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}),
		replica(v1beta1.DecoderComponent, 8, none),
	}
	f := &releaseFixture{t: t, ctx: context.Background(), parent: parent, recorder: record.NewFakeRecorder(16)}
	f.policy = &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional, Proportional: &v1beta1.ProportionalPolicy{
		Anchor: v1beta1.EngineComponent,
		Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: resource.MustParse("0.666")},
	}}
	f.components = map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent:  {MinReplicas: ptr.To(12), MaxReplicas: 18},
		v1beta1.DecoderComponent: {MinReplicas: ptr.To(2), MaxReplicas: 16, Autoscaler: none.DeepCopy()},
	}
	if opts.decoderNoBlock {
		f.components[v1beta1.DecoderComponent].Autoscaler = nil
	}
	f.modes = map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative,
	}
	if opts.router {
		objects = append(objects, replica(v1beta1.RouterComponent, 3, none))
		f.policy.Proportional.Ratios[v1beta1.RouterComponent] = resource.MustParse("0.25")
		f.components[v1beta1.RouterComponent] = &v1beta1.ComponentExtensionSpec{MinReplicas: ptr.To(1), MaxReplicas: 8, Autoscaler: none.DeepCopy()}
		f.modes[v1beta1.RouterComponent] = constants.OMENative
	}
	f.store = fakeclient.New(t, fakeclient.WithObjects(objects...))
	f.projector = &projection{store: f.store}
	return f
}

// pass runs the coordinator around the stand-in projector: the targets
// before the projection, the status after it.
func (f *releaseFixture) pass() {
	f.t.Helper()
	targets, err := ResolveTargets(f.ctx, f.store, f.parent, f.policy, "isvc", f.components, f.modes)
	if err != nil {
		f.t.Fatal(err)
	}
	for _, component := range []v1beta1.ComponentType{v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		if f.components[component] == nil {
			continue
		}
		ir := &v1beta1.InferenceReplica{Spec: v1beta1.InferenceReplicaSpec{Component: component}}
		ir.Name, ir.Namespace = "service-"+string(component), "serving"
		if targets.Released(component) {
			// The projector keeps the count of a released Component; the
			// stand-in has nothing to write.
			continue
		}
		if targets.Replicas(component) == nil && fixedCount(f.components[component]) {
			// Nothing owns the count: the projector writes minReplicas.
			f.write(component, FollowerBounds(f.components[component]).Min)
			continue
		}
		f.projector.write(f.t, ir, targets)
	}
	if err := Reconcile(f.ctx, f.store, f.recorder, f.parent, targets); err != nil {
		f.t.Fatal(err)
	}
}

func (f *releaseFixture) write(component v1beta1.ComponentType, count int32) {
	f.t.Helper()
	live := f.replica(component)
	if ptr.Deref(live.Spec.Replicas, 0) == count {
		return
	}
	live.Spec.Replicas = ptr.To(count)
	if err := f.store.Update(f.ctx, live); err != nil {
		f.t.Fatal(err)
	}
}

func (f *releaseFixture) replica(component v1beta1.ComponentType) *v1beta1.InferenceReplica {
	f.t.Helper()
	live := &v1beta1.InferenceReplica{}
	if err := f.store.Get(f.ctx, client.ObjectKey{Namespace: "serving", Name: "service-" + string(component)}, live); err != nil {
		f.t.Fatal(err)
	}
	return live
}

func (f *releaseFixture) count(component v1beta1.ComponentType) int32 {
	return ptr.Deref(f.replica(component).Spec.Replicas, -1)
}

func (f *releaseFixture) events() []string {
	var out []string
	for {
		select {
		case event := <-f.recorder.Events:
			out = append(out, event)
		default:
			return out
		}
	}
}

func (f *releaseFixture) releaseEvents() []string {
	var out []string
	for _, event := range f.events() {
		if strings.Contains(event, "ScalingFollowerReleased") {
			out = append(out, event)
		}
	}
	return out
}

func (f *releaseFixture) entry(component v1beta1.ComponentType) *v1beta1.FollowerScalingStatus {
	if f.parent.Status.ScalingPolicy == nil {
		return nil
	}
	for i := range f.parent.Status.ScalingPolicy.Followers {
		if f.parent.Status.ScalingPolicy.Followers[i].Component == component {
			return &f.parent.Status.ScalingPolicy.Followers[i]
		}
	}
	return nil
}

func (f *releaseFixture) ready() *metav1.Condition {
	if f.parent.Status.ScalingPolicy == nil {
		return nil
	}
	return meta.FindStatusCondition(f.parent.Status.ScalingPolicy.Conditions, "Ready")
}

func (f *releaseFixture) expectReleased(component v1beta1.ComponentType, named, kept int32) {
	f.t.Helper()
	entry := f.entry(component)
	if entry == nil || !entry.Released || entry.Target != named || entry.Current != kept || entry.Ratio.String() != "666m" {
		f.t.Fatalf("%s entry = %+v; want released at %d Instances, %d named by its spec, the ratio it followed at", component, entry, kept, named)
	}
	ready := f.ready()
	if ready == nil || ready.Status != metav1.ConditionTrue || !strings.Contains(ready.Message, string(component)+" stopped following") {
		f.t.Fatalf("Ready = %+v; want True and a message saying %s stopped following", ready, component)
	}
}

// The policy is removed. The decoder stops following and keeps its 12
// Instances; the status records it as released beside the 2 its minReplicas
// names, and one Warning says so. The record stands until the operator edits
// that minReplicas; then the projector writes the new count and the block,
// with no policy and nothing kept, is cleared.
func TestReleasedFollowerKeepsItsCountUntilItsMinReplicasIsEdited(t *testing.T) {
	f := newReleaseFixture(t, releaseOptions{})
	f.pass()
	if f.count(v1beta1.DecoderComponent) != 12 || f.entry(v1beta1.DecoderComponent).Released {
		t.Fatalf("decoder = %d, entry %+v; want 12 and following", f.count(v1beta1.DecoderComponent), f.entry(v1beta1.DecoderComponent))
	}
	f.events()

	f.policy = nil
	f.pass()
	if got := f.count(v1beta1.DecoderComponent); got != 12 {
		t.Fatalf("decoder = %d after the policy was removed; want 12 kept", got)
	}
	f.expectReleased(v1beta1.DecoderComponent, 2, 12)
	if ready := f.ready(); ready.Reason != "Released" || f.parent.Status.ScalingPolicy.Mode != "" {
		t.Fatalf("Ready = %+v, mode %q; want reason Released and no mode", ready, f.parent.Status.ScalingPolicy.Mode)
	}
	if events := f.releaseEvents(); len(events) != 1 || !strings.Contains(events[0], "keeps 12 Instances") || !strings.Contains(events[0], "minReplicas names 2") {
		t.Fatalf("release events = %q; want one naming the 12 kept and the 2 named", events)
	}

	f.pass()
	f.expectReleased(v1beta1.DecoderComponent, 2, 12)
	if events := f.releaseEvents(); len(events) != 0 {
		t.Fatalf("a standing release warned again: %q", events)
	}

	f.components[v1beta1.DecoderComponent].MinReplicas = ptr.To(10)
	f.pass()
	if got := f.count(v1beta1.DecoderComponent); got != 10 {
		t.Fatalf("decoder = %d after its minReplicas was edited; want 10", got)
	}
	if f.parent.Status.ScalingPolicy != nil {
		t.Fatalf("status = %+v; want no block with no policy and nothing kept", f.parent.Status.ScalingPolicy)
	}
}

// The decoder is taken out of the ratios while the router stays. The router
// keeps following the anchor on the same pass, the decoder is released, and
// the Ready condition carries both: Applied for the router with the
// decoder's note. A policy that names the decoder again takes it back.
func TestFollowerDroppedFromTheRatiosIsReleasedWhileTheOtherKeepsFollowing(t *testing.T) {
	f := newReleaseFixture(t, releaseOptions{router: true})
	f.pass()
	if f.count(v1beta1.DecoderComponent) != 12 || f.count(v1beta1.RouterComponent) != 5 {
		t.Fatalf("decoder %d, router %d; want 12 and 5 (ceil(18 x 0.25))", f.count(v1beta1.DecoderComponent), f.count(v1beta1.RouterComponent))
	}
	f.events()

	delete(f.policy.Proportional.Ratios, v1beta1.DecoderComponent)
	f.write(v1beta1.EngineComponent, 24)
	f.pass()
	if f.count(v1beta1.DecoderComponent) != 12 || f.count(v1beta1.RouterComponent) != 6 {
		t.Fatalf("decoder %d, router %d; want the decoder kept at 12 and the router following to 6", f.count(v1beta1.DecoderComponent), f.count(v1beta1.RouterComponent))
	}
	f.expectReleased(v1beta1.DecoderComponent, 2, 12)
	router := f.entry(v1beta1.RouterComponent)
	if router == nil || router.Released || router.Target != 6 || router.Current != 6 {
		t.Fatalf("router entry = %+v; want following at 6", router)
	}
	if ready := f.ready(); ready.Reason != "Applied" || f.parent.Status.ScalingPolicy.Mode != v1beta1.ScalingProportional {
		t.Fatalf("Ready = %+v; want Applied under the standing policy", ready)
	}
	if events := f.releaseEvents(); len(events) != 1 {
		t.Fatalf("release events = %q; want one", events)
	}

	f.policy.Proportional.Ratios[v1beta1.DecoderComponent] = resource.MustParse("0.666")
	f.pass()
	decoder := f.entry(v1beta1.DecoderComponent)
	if f.count(v1beta1.DecoderComponent) != 16 || decoder == nil || decoder.Released || decoder.Target != 16 {
		t.Fatalf("decoder %d, entry %+v; want following again at 16 (ceil(24 x 0.666))", f.count(v1beta1.DecoderComponent), decoder)
	}
}

// With mode Independent the followers scale on their own again; a decoder
// above its minReplicas is released and the Independent condition says so.
func TestFollowerReleasedByModeIndependentKeepsItsCount(t *testing.T) {
	f := newReleaseFixture(t, releaseOptions{})
	f.pass()
	f.events()
	f.policy = &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingIndependent}
	f.pass()
	if got := f.count(v1beta1.DecoderComponent); got != 12 {
		t.Fatalf("decoder = %d under mode Independent; want 12 kept", got)
	}
	f.expectReleased(v1beta1.DecoderComponent, 2, 12)
	if ready := f.ready(); ready.Reason != "Independent" || !strings.HasPrefix(ready.Message, "components scale independently; ") {
		t.Fatalf("Ready = %+v; want Independent with the decoder's note", ready)
	}
}

// A released decoder with no autoscaler block gets its default autoscaler
// back, which owns the count from here. The coordinator keeps nothing, so no
// record is written and the block is cleared with the policy; the one
// Warning names the autoscaler as the owner.
func TestFollowerReleasedToItsAutoscalerIsNotRecorded(t *testing.T) {
	f := newReleaseFixture(t, releaseOptions{decoderNoBlock: true})
	f.pass()
	f.events()
	f.policy = nil
	f.pass()
	if f.parent.Status.ScalingPolicy != nil {
		t.Fatalf("status = %+v; want no block: an autoscaler owns the released count", f.parent.Status.ScalingPolicy)
	}
	if events := f.releaseEvents(); len(events) != 1 || !strings.Contains(events[0], "its autoscaler owns its count from here (12 Instances)") {
		t.Fatalf("release events = %q; want one naming the autoscaler", events)
	}
	f.pass()
	if events := f.releaseEvents(); len(events) != 0 {
		t.Fatalf("the release warned again: %q", events)
	}
}

// A follower that stood at its minReplicas when the policy was removed has
// nothing to keep: no record, no Warning.
func TestFollowerReleasedAtItsMinReplicasLeavesNoRecord(t *testing.T) {
	f := newReleaseFixture(t, releaseOptions{})
	f.write(v1beta1.EngineComponent, 3)
	f.pass()
	if got := f.count(v1beta1.DecoderComponent); got != 2 {
		t.Fatalf("decoder = %d at 3 engines; want the minReplicas 2", got)
	}
	f.events()
	f.policy = nil
	f.pass()
	if f.parent.Status.ScalingPolicy != nil || len(f.releaseEvents()) != 0 || f.count(v1beta1.DecoderComponent) != 2 {
		t.Fatalf("status %+v, decoder %d; want no block, no Warning and the decoder at 2", f.parent.Status.ScalingPolicy, f.count(v1beta1.DecoderComponent))
	}
}
