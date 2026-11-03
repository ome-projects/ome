package scaling

import (
	"context"
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

func allNative() map[v1beta1.ComponentType]constants.DeploymentModeType {
	return map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative, v1beta1.RouterComponent: constants.OMENative,
	}
}

func countReason(events []string, reason string) int {
	n := 0
	for _, e := range events {
		if strings.Contains(e, reason) {
			n++
		}
	}
	return n
}

// The decoder follows an engine with no maximum on either side while the
// router follower cannot be coordinated (skipped as RawDeployment, or not
// yet projected). The unbounded warning fires once over three passes, and
// the Ready message keeps the note so the next pass sees it.
func TestReconcileUnboundedWarningFiresOnceWhileAnotherFollowerCannotFollow(t *testing.T) {
	for _, tc := range []struct {
		name       string
		routerMode constants.DeploymentModeType
		reason     string
	}{
		{"router skipped as RawDeployment", constants.RawDeployment, "InvalidPolicy"},
		{"router not yet projected", constants.OMENative, "FollowerUnavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
			engine := projectedReplica(parent, v1beta1.EngineComponent, 8, v1beta1.AutoscalerExternal)
			decoder := projectedReplica(parent, v1beta1.DecoderComponent, 2, "")
			store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
			projector := &projection{store: store}
			components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
				v1beta1.EngineComponent:  {MinReplicas: ptr.To(2)},
				v1beta1.DecoderComponent: {MinReplicas: ptr.To(2)},
				v1beta1.RouterComponent:  {MinReplicas: ptr.To(1)},
			}
			modes := allNative()
			modes[v1beta1.RouterComponent] = tc.routerMode
			policy := proportionalPolicy("1")
			policy.Proportional.Ratios[v1beta1.RouterComponent] = resource.MustParse("0.5")
			recorder := record.NewFakeRecorder(16)
			for i := 0; i < 3; i++ {
				pass(t, store, projector, recorder, parent, policy, components, modes, decoder)
			}
			condition := readyCondition(t, parent)
			if condition.Reason != tc.reason || !strings.Contains(condition.Message, "no maxReplicas") {
				t.Fatalf("Ready must read %s and keep the unbounded note: %+v", tc.reason, condition)
			}
			if n := countReason(fakeclient.RecordedEvents(recorder), "ScalingFollowerUnbounded"); n != 1 {
				t.Fatalf("ScalingFollowerUnbounded fired %d times over 3 passes, want 1", n)
			}
		})
	}
}

// Engine 8 anchors a decoder held to 4 by its maxReplicas. The anchor loses
// its desired count for one pass. Status keeps anchorDesired 8 and the
// decoder entry through the hold, and the clamp warning does not fire again
// when the anchor comes back with the same count.
func TestReconcileAnchorHoldKeepsTheLastCountsAndTheClampWarnsOnce(t *testing.T) {
	ctx := context.Background()
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
	engine := projectedReplica(parent, v1beta1.EngineComponent, 8, v1beta1.AutoscalerExternal)
	decoder := projectedReplica(parent, v1beta1.DecoderComponent, 2, "")
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	projector := &projection{store: store}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent:  {MinReplicas: ptr.To(2), MaxReplicas: 10},
		v1beta1.DecoderComponent: {MinReplicas: ptr.To(1), MaxReplicas: 4},
	}
	policy := proportionalPolicy("1")
	recorder := record.NewFakeRecorder(16)
	run := func() {
		t.Helper()
		pass(t, store, projector, recorder, parent, policy, components, allNative(), decoder)
	}
	setAnchor := func(replicas *int32) {
		t.Helper()
		live := &v1beta1.InferenceReplica{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(engine), live); err != nil {
			t.Fatal(err)
		}
		live.Spec.Replicas = replicas
		if err := store.Update(ctx, live); err != nil {
			t.Fatal(err)
		}
	}

	run()
	if n := countReason(fakeclient.RecordedEvents(recorder), "ScalingTargetClamped"); n != 1 {
		t.Fatalf("first pass: %d clamp warnings, want 1", n)
	}

	setAnchor(nil)
	run()
	status := parent.Status.ScalingPolicy
	condition := readyCondition(t, parent)
	if condition.Reason != "AnchorUnavailable" {
		t.Fatalf("want AnchorUnavailable, got %+v", condition)
	}
	if status.AnchorDesired != 8 || len(status.Followers) != 1 || status.Followers[0].Target != 4 || !status.Followers[0].Clamped {
		t.Fatalf("hold must keep anchorDesired 8 and the decoder held at 4: %+v", status)
	}
	if !strings.Contains(condition.Message, "bounded to 4") {
		t.Fatalf("hold must keep the clamp note: %q", condition.Message)
	}

	setAnchor(ptr.To(int32(8)))
	run()
	if n := countReason(fakeclient.RecordedEvents(recorder), "ScalingTargetClamped"); n != 0 {
		t.Fatalf("clamp warning fired %d times again after the hold for an unchanged clamp", n)
	}
}

// A follower that picks up its own scaler holds every follower. Status keeps
// the last counts through that hold too, and the conflict warning still
// fires once.
func TestReconcileFollowerConflictHoldKeepsTheLastCounts(t *testing.T) {
	ctx := context.Background()
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
	engine := projectedReplica(parent, v1beta1.EngineComponent, 6, v1beta1.AutoscalerExternal)
	decoder := projectedReplica(parent, v1beta1.DecoderComponent, 2, "")
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	projector := &projection{store: store}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent:  {MinReplicas: ptr.To(1), MaxReplicas: 10},
		v1beta1.DecoderComponent: {MinReplicas: ptr.To(1)},
	}
	policy := proportionalPolicy("0.5")
	recorder := record.NewFakeRecorder(16)
	run := func() {
		t.Helper()
		pass(t, store, projector, recorder, parent, policy, components, allNative(), decoder)
	}
	run()
	live := &v1beta1.InferenceReplica{}
	if err := store.Get(ctx, client.ObjectKeyFromObject(decoder), live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
	if err := store.Update(ctx, live); err != nil {
		t.Fatal(err)
	}
	fakeclient.RecordedEvents(recorder)
	run()
	run()
	status := parent.Status.ScalingPolicy
	if readyCondition(t, parent).Reason != "FollowerAutoscalerConflict" {
		t.Fatalf("want FollowerAutoscalerConflict: %+v", status.Conditions)
	}
	if status.AnchorDesired != 6 || len(status.Followers) != 1 || status.Followers[0].Target != 3 {
		t.Fatalf("conflict hold must keep anchorDesired 6 and the decoder target 3: %+v", status)
	}
	if n := countReason(fakeclient.RecordedEvents(recorder), "FollowerAutoscalerConflict"); n != 1 {
		t.Fatalf("conflict warning fired %d times over 2 passes, want 1", n)
	}
}

// heldWorld is an engine at 8 anchoring a decoder held to 4 by its
// maxReplicas, with helpers to move the anchor and change the decoder's
// scaler between passes.
type heldWorld struct {
	t        *testing.T
	store    client.Client
	parent   *v1beta1.InferenceService
	engine   *v1beta1.InferenceReplica
	decoder  *v1beta1.InferenceReplica
	recorder *record.FakeRecorder
	run      func()
}

func newHeldWorld(t *testing.T) *heldWorld {
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
	engine := projectedReplica(parent, v1beta1.EngineComponent, 8, v1beta1.AutoscalerExternal)
	decoder := projectedReplica(parent, v1beta1.DecoderComponent, 2, "")
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	projector := &projection{store: store}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent:  {MinReplicas: ptr.To(1), MaxReplicas: 20},
		v1beta1.DecoderComponent: {MinReplicas: ptr.To(1), MaxReplicas: 4},
	}
	policy := proportionalPolicy("1")
	w := &heldWorld{t: t, store: store, parent: parent, engine: engine, decoder: decoder,
		recorder: record.NewFakeRecorder(64)}
	w.run = func() {
		t.Helper()
		pass(t, store, projector, w.recorder, parent, policy, components, allNative(), decoder)
	}
	return w
}

func (w *heldWorld) update(obj *v1beta1.InferenceReplica, change func(*v1beta1.InferenceReplica)) {
	w.t.Helper()
	live := &v1beta1.InferenceReplica{}
	if err := w.store.Get(context.Background(), client.ObjectKeyFromObject(obj), live); err != nil {
		w.t.Fatal(err)
	}
	change(live)
	if err := w.store.Update(context.Background(), live); err != nil {
		w.t.Fatal(err)
	}
}

func (w *heldWorld) setAnchor(replicas *int32) {
	w.t.Helper()
	w.update(w.engine, func(ir *v1beta1.InferenceReplica) { ir.Spec.Replicas = replicas })
}

func (w *heldWorld) setDecoderScaler(class v1beta1.AutoscalerClass) {
	w.t.Helper()
	w.update(w.decoder, func(ir *v1beta1.InferenceReplica) {
		ir.Spec.Autoscaler = &v1beta1.ComponentAutoscaler{Class: class}
	})
}

// requireSnapshotAt8 checks that status shows the counts of the last pass
// before the hold: anchor 8, decoder held at 4, and the clamp note.
func (w *heldWorld) requireSnapshotAt8(reason string) {
	w.t.Helper()
	status := w.parent.Status.ScalingPolicy
	condition := readyCondition(w.t, w.parent)
	if condition.Reason != reason {
		w.t.Fatalf("want %s, got %+v", reason, condition)
	}
	if status.AnchorDesired != 8 || len(status.Followers) != 1 ||
		status.Followers[0].Target != 4 || !status.Followers[0].Clamped {
		w.t.Fatalf("hold must show anchorDesired 8 beside the decoder held at 4: anchorDesired=%d followers=%+v",
			status.AnchorDesired, status.Followers)
	}
	for _, want := range []string{heldCountsNote, "bounded to 4 Instances by its maxReplicas (the ratio asks for 8)"} {
		if !strings.Contains(condition.Message, want) {
			w.t.Fatalf("hold message must carry %q: %q", want, condition.Message)
		}
	}
}

// The decoder, held to 4 while the engine asks for 8, gets its own HPA; the
// engine then moves to 2 during the hold. Status keeps showing the engine at
// 8 beside the decoder at 4, and the conflict warning fires once.
func TestReconcileConflictHoldWhileTheAnchorMoves(t *testing.T) {
	w := newHeldWorld(t)
	w.run()
	w.setDecoderScaler(v1beta1.AutoscalerHPA)
	fakeclient.RecordedEvents(w.recorder)
	w.run()
	w.setAnchor(ptr.To(int32(2)))
	w.run()
	w.run()
	w.run()
	if n := countReason(fakeclient.RecordedEvents(w.recorder), "FollowerAutoscalerConflict"); n != 1 {
		t.Errorf("conflict warning fired %d times over 4 hold passes, want 1", n)
	}
	w.requireSnapshotAt8("FollowerAutoscalerConflict")
}

// The engine moves on every pass of a conflict hold. Each pass shows the
// same snapshot from before the hold and the warning fires once. When the
// decoder's scaler is removed, status shows the fresh counts and the clamp
// warning does not fire again.
func TestReconcileConflictHoldKeepsOneSnapshotThenRecovers(t *testing.T) {
	w := newHeldWorld(t)
	w.run()
	if n := countReason(fakeclient.RecordedEvents(w.recorder), "ScalingTargetClamped"); n != 1 {
		t.Fatalf("first pass: %d clamp warnings, want 1", n)
	}
	w.setDecoderScaler(v1beta1.AutoscalerHPA)
	for _, anchor := range []int32{8, 2, 5, 3, 12, 1} {
		w.setAnchor(ptr.To(anchor))
		w.run()
		w.requireSnapshotAt8("FollowerAutoscalerConflict")
	}
	events := fakeclient.RecordedEvents(w.recorder)
	if n := countReason(events, "FollowerAutoscalerConflict"); n != 1 {
		t.Fatalf("conflict warning fired %d times over 6 hold passes, want 1: %v", n, events)
	}
	if n := countReason(events, "ScalingTargetClamped"); n != 0 {
		t.Fatalf("clamp warning fired %d times during the hold", n)
	}

	w.setAnchor(ptr.To(int32(3)))
	w.setDecoderScaler(v1beta1.AutoscalerNone)
	w.run()
	status := w.parent.Status.ScalingPolicy
	entry := followerEntry(t, w.parent)
	if status.AnchorDesired != 3 || entry.Target != 3 || entry.Clamped {
		t.Fatalf("recovery must show fresh counts (anchor 3, decoder target 3, not clamped): anchorDesired=%d entry=%+v",
			status.AnchorDesired, entry)
	}
	if message := readyCondition(t, w.parent).Message; strings.Contains(message, heldCountsNote) || strings.Contains(message, "bounded to") {
		t.Fatalf("recovery message must drop the hold and clamp notes: %q", message)
	}

	w.setAnchor(ptr.To(int32(8)))
	w.run()
	w.run()
	events = fakeclient.RecordedEvents(w.recorder)
	if n := countReason(events, "ScalingTargetClamped"); n != 1 {
		t.Fatalf("a new clamp after recovery warns once, got %d: %v", n, events)
	}
	if n := countReason(events, "FollowerAutoscalerConflict"); n != 0 {
		t.Fatalf("conflict warning fired %d times after the scaler was removed", n)
	}
}

// Recovering from a conflict hold at the count the hold showed keeps the
// clamp unchanged, so the clamp warning does not fire again.
func TestReconcileConflictHoldRecoveryAtTheSameCountDoesNotRewarnTheClamp(t *testing.T) {
	w := newHeldWorld(t)
	w.run()
	w.setDecoderScaler(v1beta1.AutoscalerHPA)
	w.setAnchor(ptr.To(int32(2)))
	w.run()
	w.run()
	w.setAnchor(ptr.To(int32(8)))
	w.setDecoderScaler(v1beta1.AutoscalerNone)
	fakeclient.RecordedEvents(w.recorder)
	w.run()
	if n := countReason(fakeclient.RecordedEvents(w.recorder), "ScalingTargetClamped"); n != 0 {
		t.Fatalf("clamp warning fired %d times after the hold for an unchanged clamp", n)
	}
	if entry := followerEntry(t, w.parent); entry.Target != 4 || !entry.Clamped {
		t.Fatalf("decoder must be held at 4 again: %+v", entry)
	}
}

// The engine loses its desired count for several passes. Every pass of the
// AnchorUnavailable hold shows the same snapshot from before the hold.
func TestReconcileAnchorUnavailableHoldKeepsTheSnapshot(t *testing.T) {
	w := newHeldWorld(t)
	w.run()
	w.setAnchor(nil)
	for i := 0; i < 3; i++ {
		w.run()
		w.requireSnapshotAt8("AnchorUnavailable")
	}
}

// An HPA or KEDA anchor always has a maximum: with maxReplicas unset it is
// its minimum. A follower of such an anchor is bounded, so no unbounded
// warning or note appears.
func TestReconcileScalerAnchorWithoutMaxReplicasIsBounded(t *testing.T) {
	for _, class := range []v1beta1.AutoscalerClass{v1beta1.AutoscalerHPA, v1beta1.AutoscalerKEDA} {
		t.Run(string(class), func(t *testing.T) {
			parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
			engine := projectedReplica(parent, v1beta1.EngineComponent, 2, class)
			decoder := projectedReplica(parent, v1beta1.DecoderComponent, 2, "")
			store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
			projector := &projection{store: store}
			components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
				v1beta1.EngineComponent:  {MinReplicas: ptr.To(2)},
				v1beta1.DecoderComponent: {MinReplicas: ptr.To(1)},
			}
			recorder := record.NewFakeRecorder(16)
			pass(t, store, projector, recorder, parent, proportionalPolicy("1"), components, allNative(), decoder)
			if n := countReason(fakeclient.RecordedEvents(recorder), "ScalingFollowerUnbounded"); n != 0 {
				t.Fatalf("unbounded warning for a %s anchor whose maximum is its minimum 2", class)
			}
			if message := readyCondition(t, parent).Message; strings.Contains(message, "no maxReplicas") {
				t.Fatalf("unexpected unbounded note: %q", message)
			}
		})
	}
}

// The engine at 8 anchors the decoder, held to 4 by its maxReplicas, and the
// router at ratio 0.5 with no maxReplicas on either side. The policy then
// drops the router or changes its ratio in the same pass the engine loses its
// desired count. The hold keeps the decoder's last counts and its note, and
// lists neither a router entry nor a router note under the new policy.
func TestReconcileHeldStatusListsOnlyCurrentFollowers(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.ScalingPolicy)
	}{
		{"router removed from the ratios", func(p *v1beta1.ScalingPolicy) {
			delete(p.Proportional.Ratios, v1beta1.RouterComponent)
		}},
		{"router ratio changed", func(p *v1beta1.ScalingPolicy) {
			p.Proportional.Ratios[v1beta1.RouterComponent] = resource.MustParse("0.25")
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
			engine := projectedReplica(parent, v1beta1.EngineComponent, 8, v1beta1.AutoscalerExternal)
			decoder := projectedReplica(parent, v1beta1.DecoderComponent, 4, "")
			router := projectedReplica(parent, v1beta1.RouterComponent, 4, "")
			store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder, router))
			projector := &projection{store: store}
			components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
				v1beta1.EngineComponent:  {MinReplicas: ptr.To(1)},
				v1beta1.DecoderComponent: {MinReplicas: ptr.To(1), MaxReplicas: 4},
				v1beta1.RouterComponent:  {MinReplicas: ptr.To(1)},
			}
			policy := proportionalPolicy("1")
			policy.Proportional.Ratios[v1beta1.RouterComponent] = resource.MustParse("0.5")
			recorder := record.NewFakeRecorder(16)
			pass(t, store, projector, recorder, parent, policy, components, allNative(), decoder)
			if n := len(parent.Status.ScalingPolicy.Followers); n != 2 {
				t.Fatalf("first pass: %d follower entries, want decoder and router", n)
			}
			routerNote := unboundedNote(v1beta1.RouterComponent, v1beta1.EngineComponent)
			if !strings.Contains(readyCondition(t, parent).Message, routerNote) {
				t.Fatalf("first pass must carry the router note: %q", readyCondition(t, parent).Message)
			}

			live := &v1beta1.InferenceReplica{}
			if err := store.Get(context.Background(), client.ObjectKeyFromObject(engine), live); err != nil {
				t.Fatal(err)
			}
			live.Spec.Replicas = nil
			if err := store.Update(context.Background(), live); err != nil {
				t.Fatal(err)
			}
			tc.change(policy)
			pass(t, store, projector, recorder, parent, policy, components, allNative(), decoder)

			status := parent.Status.ScalingPolicy
			condition := readyCondition(t, parent)
			if condition.Reason != "AnchorUnavailable" {
				t.Fatalf("want AnchorUnavailable, got %+v", condition)
			}
			if status.AnchorDesired != 8 || len(status.Followers) != 1 ||
				status.Followers[0].Component != v1beta1.DecoderComponent ||
				status.Followers[0].Target != 4 || !status.Followers[0].Clamped {
				t.Fatalf("hold must show anchorDesired 8 and only the decoder held at 4: anchorDesired=%d followers=%+v",
					status.AnchorDesired, status.Followers)
			}
			if !strings.Contains(condition.Message, "decoder target is bounded to 4") {
				t.Fatalf("hold must keep the decoder note: %q", condition.Message)
			}
			if strings.Contains(condition.Message, routerNote) {
				t.Fatalf("hold must drop the router note: %q", condition.Message)
			}
		})
	}
}
