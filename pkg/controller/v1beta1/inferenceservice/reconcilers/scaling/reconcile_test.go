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
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
)

// projection stands in for a follower's projector: it writes the count the
// coordinator resolved, and nothing when it resolved none.
type projection struct {
	store  client.Client
	writes int
}

func (p *projection) write(t *testing.T, ir *v1beta1.InferenceReplica, targets *Targets) {
	t.Helper()
	ctx := context.Background()
	target := targets.Replicas(ir.Spec.Component)
	if target == nil {
		return
	}
	live := &v1beta1.InferenceReplica{}
	if err := p.store.Get(ctx, client.ObjectKeyFromObject(ir), live); err != nil {
		t.Fatal(err)
	}
	if live.Spec.Replicas != nil && *live.Spec.Replicas == *target {
		return
	}
	live.Spec.Replicas = target
	if err := p.store.Update(ctx, live); err != nil {
		t.Fatal(err)
	}
	p.writes++
}

func TestReconcileFollowerScaleTransitions(t *testing.T) {
	ctx := context.Background()
	parent := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving", Generation: 3},
	}
	engine := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "service-engine", Namespace: "serving"},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef:  &v1beta1.ParentReference{Name: parent.Name},
			Component:  v1beta1.EngineComponent,
			Replicas:   ptr.To[int32](5),
			Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal},
		},
	}
	decoder := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "service-decoder", Namespace: "serving"},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: parent.Name},
			Component: v1beta1.DecoderComponent,
			Replicas:  ptr.To[int32](1),
		},
	}
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	projector := &projection{store: store}
	policy := &v1beta1.ScalingPolicy{
		Mode: v1beta1.ScalingProportional,
		Proportional: &v1beta1.ProportionalPolicy{
			Anchor: v1beta1.EngineComponent,
			Ratios: map[v1beta1.ComponentType]resource.Quantity{
				v1beta1.DecoderComponent: resource.MustParse("0.25"),
			},
		},
	}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent:  {},
		v1beta1.DecoderComponent: {MinReplicas: ptr.To(1), MaxReplicas: 12},
	}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent:  constants.OMENative,
		v1beta1.DecoderComponent: constants.OMENative,
	}
	// edit reads the stored copy of a component, applies the change and writes it back.
	edit := func(component *v1beta1.InferenceReplica, change func(live *v1beta1.InferenceReplica)) {
		t.Helper()
		live := &v1beta1.InferenceReplica{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(component), live); err != nil {
			t.Fatal(err)
		}
		change(live)
		if err := store.Update(ctx, live); err != nil {
			t.Fatal(err)
		}
	}
	assertDecoder := func(step string, want int32, wantWrites int) {
		t.Helper()
		live := &v1beta1.InferenceReplica{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(decoder), live); err != nil {
			t.Fatal(err)
		}
		if live.Spec.Replicas == nil || *live.Spec.Replicas != want || projector.writes != wantWrites {
			t.Fatalf("%s: decoder desired = %d, projections = %d; want %d, %d",
				step, ptr.Deref(live.Spec.Replicas, -1), projector.writes, want, wantWrites)
		}
		status := parent.Status.ScalingPolicy
		if status.AnchorDesired != *engine.Spec.Replicas || len(status.Followers) != 1 ||
			status.Followers[0].Target != want || status.Followers[0].Current != want {
			t.Fatalf("%s: unexpected applied policy status: %+v", step, status)
		}
	}

	steps := []struct {
		name        string
		arrange     func()
		wantReason  string // the Ready=False reason while the coordinator resolves no count for the follower; empty once the policy applied
		wantDecoder int32  // the follower's desired replicas once the policy applied
		wantWrites  int    // the follower projections made so far
	}{
		{
			name:        "the first pass scales the follower to a quarter of the anchor",
			wantDecoder: 2,
			wantWrites:  1,
		},
		{
			name:        "a pass with nothing changed writes nothing",
			wantDecoder: 2,
			wantWrites:  1,
		},
		{
			name: "the anchor shrinks and the follower follows it down",
			arrange: func() {
				edit(engine, func(live *v1beta1.InferenceReplica) { live.Spec.Replicas = ptr.To[int32](4) })
				engine.Spec.Replicas = ptr.To[int32](4)
			},
			wantDecoder: 1,
			wantWrites:  2,
		},
		{
			name: "a hand edit of the follower is written back",
			arrange: func() {
				edit(decoder, func(live *v1beta1.InferenceReplica) { live.Spec.Replicas = ptr.To[int32](9) })
			},
			wantDecoder: 1,
			wantWrites:  3,
		},
		{
			name: "a competing autoscaler on the follower holds the writes",
			arrange: func() {
				edit(decoder, func(live *v1beta1.InferenceReplica) {
					live.Spec.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerExternal}
				})
				edit(engine, func(live *v1beta1.InferenceReplica) { live.Spec.Replicas = ptr.To[int32](8) })
			},
			wantReason: "FollowerAutoscalerConflict",
			wantWrites: 3,
		},
		{
			name: "a missing anchor holds the writes",
			arrange: func() {
				live := &v1beta1.InferenceReplica{}
				if err := store.Get(ctx, client.ObjectKeyFromObject(engine), live); err != nil {
					t.Fatal(err)
				}
				if err := store.Delete(ctx, live); err != nil {
					t.Fatal(err)
				}
			},
			wantReason: "AnchorUnavailable",
			wantWrites: 3,
		},
	}
	for _, step := range steps {
		if step.arrange != nil {
			step.arrange()
		}
		targets, err := ResolveTargets(ctx, store, parent, policy, "isvc", components, modes)
		if err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		if step.wantReason != "" && targets.Replicas(v1beta1.DecoderComponent) != nil {
			t.Fatalf("%s: the coordinator must resolve no count for the follower", step.name)
		}
		projector.write(t, decoder, targets)
		if err := Reconcile(ctx, store, nil, parent, targets); err != nil {
			t.Fatalf("%s: %v", step.name, err)
		}
		if parent.Status.ScalingPolicy == nil {
			t.Fatalf("%s: coordinator did not report its decision", step.name)
		}
		ready := meta.FindStatusCondition(parent.Status.ScalingPolicy.Conditions, "Ready")
		if step.wantReason != "" {
			if ready == nil || ready.Status != metav1.ConditionFalse || ready.Reason != step.wantReason {
				t.Fatalf("%s: follower writes should hold with reason %s: %+v", step.name, step.wantReason, parent.Status.ScalingPolicy.Conditions)
			}
			if projector.writes != step.wantWrites {
				t.Fatalf("%s: caused %d projections; want %d", step.name, projector.writes, step.wantWrites)
			}
			continue
		}
		if ready == nil || ready.Status != metav1.ConditionTrue {
			t.Fatalf("%s: coordinator did not apply policy: %+v", step.name, parent.Status.ScalingPolicy.Conditions)
		}
		assertDecoder(step.name, step.wantDecoder, step.wantWrites)
	}
}

// A follower whose projection did not land this pass is reported with the
// count it carries and its target, and is applied once the projection lands.
func TestReconcileReportsAFollowerWhoseProjectionDidNotLand(t *testing.T) {
	ctx := context.Background()
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
	engine := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "service-engine", Namespace: "serving"},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: parent.Name}, Component: v1beta1.EngineComponent,
			Replicas: ptr.To[int32](16),
		},
	}
	decoder := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "service-decoder", Namespace: "serving"},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: parent.Name}, Component: v1beta1.DecoderComponent,
			Replicas: ptr.To[int32](8), Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
		},
	}
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	policy := &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional, Proportional: &v1beta1.ProportionalPolicy{
		Anchor: v1beta1.EngineComponent,
		Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: resource.MustParse("1")},
	}}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent: {}, v1beta1.DecoderComponent: {MinReplicas: ptr.To(1), MaxReplicas: 20},
	}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative,
	}
	targets, err := ResolveTargets(ctx, store, parent, policy, "isvc", components, modes)
	if err != nil {
		t.Fatal(err)
	}
	if got := targets.Replicas(v1beta1.DecoderComponent); got == nil || *got != 16 {
		t.Fatalf("decoder target = %v; want 16", got)
	}
	if targets.Replicas(v1beta1.EngineComponent) != nil {
		t.Fatal("the anchor has no target")
	}
	if err := Reconcile(ctx, store, nil, parent, targets); err != nil {
		t.Fatal(err)
	}
	status := parent.Status.ScalingPolicy
	condition := meta.FindStatusCondition(status.Conditions, "Ready")
	if condition == nil || condition.Status != metav1.ConditionFalse || condition.Reason != "FollowerPending" ||
		!strings.Contains(condition.Message, "decoder carries 8 Instances, target 16") {
		t.Fatalf("condition = %+v; want FollowerPending naming the decoder's 8 and 16", condition)
	}
	if status.AnchorDesired != 16 || len(status.Followers) != 1 || status.Followers[0].Target != 16 || status.Followers[0].Current != 8 {
		t.Fatalf("status = %+v; want anchor 16, decoder target 16 current 8", status)
	}
	projector := &projection{store: store}
	projector.write(t, decoder, targets)
	if err := Reconcile(ctx, store, nil, parent, targets); err != nil {
		t.Fatal(err)
	}
	condition = meta.FindStatusCondition(parent.Status.ScalingPolicy.Conditions, "Ready")
	if condition == nil || condition.Status != metav1.ConditionTrue || condition.Reason != "Applied" ||
		parent.Status.ScalingPolicy.Followers[0].Current != 16 {
		t.Fatalf("condition = %+v, followers = %+v; want Applied at 16", condition, parent.Status.ScalingPolicy.Followers)
	}
}

func TestReconcileSkipsInvalidFollowerButScalesValidFollower(t *testing.T) {
	ctx := context.Background()
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
	engine := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "service-engine", Namespace: "serving"},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: parent.Name}, Component: v1beta1.EngineComponent,
			Replicas: ptr.To[int32](10),
		},
	}
	decoder := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: "service-decoder", Namespace: "serving"},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: parent.Name}, Component: v1beta1.DecoderComponent,
			Replicas: ptr.To[int32](1), Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerNone},
		},
	}
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	projector := &projection{store: store}
	policy := &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional, Proportional: &v1beta1.ProportionalPolicy{
		Anchor: v1beta1.EngineComponent,
		Ratios: map[v1beta1.ComponentType]resource.Quantity{
			v1beta1.DecoderComponent: resource.MustParse("1"),
			v1beta1.RouterComponent:  resource.MustParse("0.5"),
		},
	}}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent: {}, v1beta1.DecoderComponent: {MinReplicas: ptr.To(1), MaxReplicas: 20},
	}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative,
	}
	check := func(want int32, reason, detail string, wantWrites int) {
		t.Helper()
		targets, err := ResolveTargets(ctx, store, parent, policy, "runtime", components, modes)
		if err != nil {
			t.Fatal(err)
		}
		projector.write(t, decoder, targets)
		if err := Reconcile(ctx, store, nil, parent, targets); err != nil {
			t.Fatal(err)
		}
		live := &v1beta1.InferenceReplica{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(decoder), live); err != nil {
			t.Fatal(err)
		}
		if *live.Spec.Replicas != want || projector.writes != wantWrites {
			t.Fatalf("decoder desired %d, projections %d; want %d, %d", *live.Spec.Replicas, projector.writes, want, wantWrites)
		}
		condition := meta.FindStatusCondition(parent.Status.ScalingPolicy.Conditions, "Ready")
		if condition == nil || condition.Reason != reason || condition.Status != metav1.ConditionFalse || !strings.Contains(condition.Message, detail) {
			t.Fatalf("condition = %+v; want %s mentioning %q", condition, reason, detail)
		}
	}
	check(10, "InvalidPolicy", "router is not declared on the InferenceService", 1)
	if len(parent.Status.ScalingPolicy.Followers) != 1 || parent.Status.ScalingPolicy.Followers[0].Component != v1beta1.DecoderComponent {
		t.Fatalf("valid follower must be reported: %+v", parent.Status.ScalingPolicy.Followers)
	}
	liveEngine := &v1beta1.InferenceReplica{}
	if err := store.Get(ctx, client.ObjectKeyFromObject(engine), liveEngine); err != nil {
		t.Fatal(err)
	}
	liveEngine.Spec.Replicas = ptr.To[int32](2)
	if err := store.Update(ctx, liveEngine); err != nil {
		t.Fatal(err)
	}
	check(2, "InvalidPolicy", "router is not declared on the InferenceService", 2)
	components[v1beta1.RouterComponent] = &v1beta1.ComponentExtensionSpec{}
	modes[v1beta1.RouterComponent] = constants.RawDeployment
	check(2, "InvalidPolicy", "router uses deployment mode \"RawDeployment\"", 2)
	modes[v1beta1.RouterComponent] = constants.OMENative
	check(2, "FollowerUnavailable", "router", 2)
	if !Follows(policy, v1beta1.DecoderComponent, components, modes) {
		t.Fatal("a missing projected router does not revoke decoder ownership")
	}
	modes[v1beta1.EngineComponent] = constants.RawDeployment
	if Follows(policy, v1beta1.DecoderComponent, components, modes) {
		t.Fatal("an invalid anchor must leave the decoder's independent autoscaler intact")
	}
	check(2, "InvalidPolicy", "anchor engine uses deployment mode", 2)
}

func TestReconcileFollowerAutoscalerConflictNamesSourceAndWarnsOnce(t *testing.T) {
	ctx := context.Background()
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"},
		Spec: v1beta1.InferenceServiceSpec{Decoder: &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
			AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: "decoder-keda"},
		}}},
	}
	parent.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
		v1beta1.DecoderComponent: {Autoscaler: &v1beta1.ComponentAutoscalerStatus{SpecSource: "policy"}},
	}
	engine := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "service-engine", Namespace: "serving"},
		Spec: v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: parent.Name}, Component: v1beta1.EngineComponent, Replicas: ptr.To[int32](8)},
	}
	decoder := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "service-decoder", Namespace: "serving"},
		Spec: v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: parent.Name}, Component: v1beta1.DecoderComponent,
			Replicas: ptr.To[int32](2), Autoscaler: &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerKEDA}},
	}
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	policy := &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional, Proportional: &v1beta1.ProportionalPolicy{
		Anchor: v1beta1.EngineComponent, Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: resource.MustParse("1")},
	}}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{v1beta1.EngineComponent: {}, v1beta1.DecoderComponent: {}}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative}
	recorder := record.NewFakeRecorder(2)
	for i := 0; i < 2; i++ {
		targets, err := ResolveTargets(ctx, store, parent, policy, "isvc", components, modes)
		if err != nil {
			t.Fatal(err)
		}
		if targets.Replicas(v1beta1.DecoderComponent) != nil {
			t.Fatal("a competing scaler must resolve no count")
		}
		if err := Reconcile(ctx, store, recorder, parent, targets); err != nil {
			t.Fatal(err)
		}
		condition := meta.FindStatusCondition(parent.Status.ScalingPolicy.Conditions, "Ready")
		if condition == nil || condition.Reason != "FollowerAutoscalerConflict" ||
			!strings.Contains(condition.Message, "decoder") || !strings.Contains(condition.Message, "autoscalerPolicyRef \"decoder-keda\"") {
			t.Fatalf("conflict must identify the follower and its source: %+v", condition)
		}
		if parent.Status.ScalingPolicy.AnchorDesired != 8 {
			t.Fatalf("the anchor's count stays published while the follower is held: %+v", parent.Status.ScalingPolicy)
		}
	}
	select {
	case event := <-recorder.Events:
		if !strings.Contains(event, "FollowerAutoscalerConflict") || !strings.Contains(event, "decoder-keda") {
			t.Fatalf("unexpected warning: %s", event)
		}
	default:
		t.Fatal("missing conflict warning")
	}
	select {
	case event := <-recorder.Events:
		t.Fatalf("repeated warning: %s", event)
	default:
	}
}

func TestFollowerConflictMessageSources(t *testing.T) {
	for _, tc := range []struct {
		source, want string
	}{
		{"isvc", "inline autoscaler block"},
		{"policy", "autoscalerPolicyRef \"decoder-keda\""},
		{"runtime", "serving runtime"},
	} {
		t.Run(tc.source, func(t *testing.T) {
			service := &v1beta1.InferenceService{Spec: v1beta1.InferenceServiceSpec{
				Decoder: &v1beta1.DecoderSpec{ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{
					AutoscalerPolicyRef: &v1beta1.AutoscalerPolicyRef{Name: "decoder-keda"},
				}},
			}}
			service.Status.Components = map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
				v1beta1.DecoderComponent: {Autoscaler: &v1beta1.ComponentAutoscalerStatus{SpecSource: tc.source}},
			}
			message := followerConflictMessage(service, v1beta1.DecoderComponent, v1beta1.AutoscalerKEDA)
			if !strings.Contains(message, tc.want) {
				t.Fatalf("message %q does not name %q", message, tc.want)
			}
		})
	}
}

// On a Split placement home the hub writes each component's bounds from the
// plan's shares. The coordinator must keep those counts, and a clamp to them is
// placement's decision rather than a local limit worth a warning.
func TestPlacementBoundsWinOverTheProportionalPolicy(t *testing.T) {
	envelope := func(t *testing.T, floors []v1beta1.PlacementComponentFloor) map[string]string {
		t.Helper()
		raw, err := protocol.Encode(&v1beta1.PlacementExecutionPolicy{PlanID: "plan-a", Revision: 2, SourceUID: "uid-1", ClusterUID: "member-a-uid", ReplicaFloors: floors})
		if err != nil {
			t.Fatal(err)
		}
		return map[string]string{constants.PlacementOriginUID: "uid-1", constants.PlacementExecution: raw}
	}
	splitHome := func(t *testing.T) map[string]string { return envelope(t, nil) }
	fullHome := func(t *testing.T) map[string]string {
		return envelope(t, []v1beta1.PlacementComponentFloor{{Component: v1beta1.EngineComponent, Replicas: 1}, {Component: v1beta1.DecoderComponent, Replicas: 6}})
	}
	for _, tt := range []struct {
		name                     string
		annotations              func(*testing.T) map[string]string
		engine                   int32
		decoderMin, decoderMax   int
		wantTarget               int32
		wantClamped, wantWarning bool
	}{
		{name: "a split home's own-weight share stands without a warning", annotations: splitHome,
			engine: 1, decoderMin: 6, decoderMax: 6, wantTarget: 6, wantClamped: true},
		{name: "a split home that follows the ratio is not clamped", annotations: splitHome,
			engine: 3, decoderMin: 9, decoderMax: 9, wantTarget: 9},
		{name: "a split home's floor under a local ceiling stands without a warning", annotations: splitHome,
			engine: 1, decoderMin: 6, decoderMax: 12, wantTarget: 6, wantClamped: true},
		{name: "a full-policy home keeps the warning", annotations: fullHome,
			engine: 1, decoderMin: 6, decoderMax: 6, wantTarget: 6, wantClamped: true, wantWarning: true},
		{name: "a local service keeps the warning", annotations: func(*testing.T) map[string]string { return nil },
			engine: 1, decoderMin: 6, decoderMax: 6, wantTarget: 6, wantClamped: true, wantWarning: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving", Generation: 1, Annotations: tt.annotations(t)}}
			engine := &v1beta1.InferenceReplica{
				ObjectMeta: metav1.ObjectMeta{Name: "service-engine", Namespace: "serving"},
				Spec:       v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: parent.Name}, Component: v1beta1.EngineComponent, Replicas: ptr.To(tt.engine)},
			}
			decoder := &v1beta1.InferenceReplica{
				ObjectMeta: metav1.ObjectMeta{Name: "service-decoder", Namespace: "serving"},
				Spec:       v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: parent.Name}, Component: v1beta1.DecoderComponent, Replicas: ptr.To(int32(tt.decoderMin))},
			}
			store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
			projector := &projection{store: store}
			recorder := record.NewFakeRecorder(4)
			policy := &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional, Proportional: &v1beta1.ProportionalPolicy{
				Anchor: v1beta1.EngineComponent,
				Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: resource.MustParse("3")},
			}}
			components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
				v1beta1.EngineComponent:  {MinReplicas: ptr.To(int(tt.engine)), MaxReplicas: int(tt.engine)},
				v1beta1.DecoderComponent: {MinReplicas: ptr.To(tt.decoderMin), MaxReplicas: tt.decoderMax},
			}
			modes := map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative}
			pass(t, store, projector, recorder, parent, policy, components, modes, decoder)
			followers := parent.Status.ScalingPolicy.Followers
			if len(followers) != 1 || followers[0].Target != tt.wantTarget || followers[0].Clamped != tt.wantClamped || followers[0].Current != tt.wantTarget {
				t.Fatalf("follower status = %+v; want target %d, clamped %t", followers, tt.wantTarget, tt.wantClamped)
			}
			if projector.writes != 0 {
				t.Fatalf("the follower was projected %d times over the counts placement set", projector.writes)
			}
			warned := false
			for len(recorder.Events) > 0 {
				warned = warned || strings.Contains(<-recorder.Events, "ScalingTargetClamped")
			}
			if warned != tt.wantWarning {
				t.Fatalf("ScalingTargetClamped warning = %t, want %t", warned, tt.wantWarning)
			}
		})
	}
}

// proportionalPolicy declares the engine as the anchor and the decoder as its
// only follower at the given ratio.
func proportionalPolicy(ratio string) *v1beta1.ScalingPolicy {
	return &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional, Proportional: &v1beta1.ProportionalPolicy{
		Anchor: v1beta1.EngineComponent,
		Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: resource.MustParse(ratio)},
	}}
}

func projectedReplica(parent *v1beta1.InferenceService, component v1beta1.ComponentType, replicas int32, class v1beta1.AutoscalerClass) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: parent.Name + "-" + string(component), Namespace: parent.Namespace},
		Spec: v1beta1.InferenceReplicaSpec{
			ParentRef: &v1beta1.ParentReference{Name: parent.Name}, Component: component, Replicas: ptr.To(replicas),
		},
	}
	if class != "" {
		ir.Spec.Autoscaler = &v1beta1.ComponentAutoscaler{Class: class}
	}
	return ir
}

func followerEntry(t *testing.T, parent *v1beta1.InferenceService) v1beta1.FollowerScalingStatus {
	t.Helper()
	status := parent.Status.ScalingPolicy
	if status == nil || len(status.Followers) != 1 {
		t.Fatalf("expected one follower entry: %+v", status)
	}
	return status.Followers[0]
}

// pass is one service pass for the follower: the coordinator resolves the
// count, the follower's projection writes it, the coordinator publishes.
func pass(t *testing.T, store client.Client, projector *projection, recorder record.EventRecorder,
	parent *v1beta1.InferenceService, policy *v1beta1.ScalingPolicy,
	components map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec,
	modes map[v1beta1.ComponentType]constants.DeploymentModeType, follower *v1beta1.InferenceReplica) {
	t.Helper()
	targets, err := ResolveTargets(context.Background(), store, parent, policy, "isvc", components, modes)
	if err != nil {
		t.Fatal(err)
	}
	projector.write(t, follower, targets)
	if err := Reconcile(context.Background(), store, recorder, parent, targets); err != nil {
		t.Fatal(err)
	}
}

func readyCondition(t *testing.T, parent *v1beta1.InferenceService) *metav1.Condition {
	t.Helper()
	condition := meta.FindStatusCondition(parent.Status.ScalingPolicy.Conditions, "Ready")
	if condition == nil {
		t.Fatalf("no Ready condition: %+v", parent.Status.ScalingPolicy)
	}
	return condition
}

// A follower that declares only a minimum follows the anchor all the way: its
// minimum is a floor, and nothing of its own caps the count.
func TestReconcileFollowerWithoutMaximumFollowsTheAnchor(t *testing.T) {
	ctx := context.Background()
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
	engine := projectedReplica(parent, v1beta1.EngineComponent, 8, v1beta1.AutoscalerExternal)
	decoder := projectedReplica(parent, v1beta1.DecoderComponent, 2, "")
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	projector := &projection{store: store}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent:  {MinReplicas: ptr.To(2), MaxReplicas: 16},
		v1beta1.DecoderComponent: {MinReplicas: ptr.To(2)},
	}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative,
	}
	recorder := record.NewFakeRecorder(8)
	policy := proportionalPolicy("1")
	setAnchor := func(desired int32) {
		t.Helper()
		live := &v1beta1.InferenceReplica{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(engine), live); err != nil {
			t.Fatal(err)
		}
		live.Spec.Replicas = ptr.To(desired)
		if err := store.Update(ctx, live); err != nil {
			t.Fatal(err)
		}
	}
	expect := func(anchor, want int32, clamped bool, wantWrites int) {
		t.Helper()
		pass(t, store, projector, recorder, parent, policy, components, modes, decoder)
		live := &v1beta1.InferenceReplica{}
		if err := store.Get(ctx, client.ObjectKeyFromObject(decoder), live); err != nil {
			t.Fatal(err)
		}
		if *live.Spec.Replicas != want || projector.writes != wantWrites {
			t.Fatalf("anchor %d: decoder desired %d after %d projections; want %d after %d", anchor, *live.Spec.Replicas, projector.writes, want, wantWrites)
		}
		entry := followerEntry(t, parent)
		if parent.Status.ScalingPolicy.AnchorDesired != anchor || entry.Target != want || entry.Current != want || entry.Clamped != clamped {
			t.Fatalf("anchor %d: status %+v, follower %+v; want target %d current %d clamped %v", anchor, parent.Status.ScalingPolicy, entry, want, want, clamped)
		}
		if condition := readyCondition(t, parent); condition.Status != metav1.ConditionTrue || condition.Reason != "Applied" {
			t.Fatalf("anchor %d: condition %+v", anchor, condition)
		}
	}

	expect(8, 8, false, 1)
	if events := fakeclient.RecordedEvents(recorder); len(events) != 0 {
		t.Fatalf("a follower inside its bounds must not warn: %v", events)
	}
	setAnchor(4)
	expect(4, 4, false, 2)
	setAnchor(1)
	expect(1, 2, true, 3)
	events := fakeclient.RecordedEvents(recorder)
	if len(events) != 1 || !strings.Contains(events[0], "ScalingTargetClamped") ||
		!strings.Contains(events[0], "minReplicas") || !strings.Contains(events[0], "asks for 1") {
		t.Fatalf("the minimum holding the follower must be named once: %v", events)
	}
	if !strings.Contains(readyCondition(t, parent).Message, "minReplicas") {
		t.Fatalf("condition must name the bound: %+v", readyCondition(t, parent))
	}
	expect(1, 2, true, 3)
	if events := fakeclient.RecordedEvents(recorder); len(events) != 0 {
		t.Fatalf("an unchanged clamp must not warn again: %v", events)
	}
	setAnchor(12)
	expect(12, 12, false, 4)
	if message := readyCondition(t, parent).Message; strings.Contains(message, "minReplicas") || strings.Contains(message, "maxReplicas") {
		t.Fatalf("a follower inside its bounds carries no bound note: %q", message)
	}
}

// A follower that declares a maximum is held there, and the status, the
// condition and one warning say which bound held it.
func TestReconcileFollowerIsHeldAtItsDeclaredMaximum(t *testing.T) {
	ctx := context.Background()
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
	engine := projectedReplica(parent, v1beta1.EngineComponent, 36, v1beta1.AutoscalerExternal)
	decoder := projectedReplica(parent, v1beta1.DecoderComponent, 1, "")
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	projector := &projection{store: store}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent:  {MinReplicas: ptr.To(1), MaxReplicas: 64},
		v1beta1.DecoderComponent: {MinReplicas: ptr.To(1), MaxReplicas: 8},
	}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative,
	}
	recorder := record.NewFakeRecorder(8)
	policy := proportionalPolicy("0.25")
	pass(t, store, projector, recorder, parent, policy, components, modes, decoder)
	live := &v1beta1.InferenceReplica{}
	if err := store.Get(ctx, client.ObjectKeyFromObject(decoder), live); err != nil {
		t.Fatal(err)
	}
	entry := followerEntry(t, parent)
	if *live.Spec.Replicas != 8 || projector.writes != 1 || entry.Target != 8 || entry.Current != 8 || !entry.Clamped {
		t.Fatalf("decoder desired %d after %d projections, follower %+v; want 8 held at the maximum", *live.Spec.Replicas, projector.writes, entry)
	}
	condition := readyCondition(t, parent)
	if condition.Status != metav1.ConditionTrue || condition.Reason != "Applied" || !strings.Contains(condition.Message, "maxReplicas") {
		t.Fatalf("condition must stay Applied and name the maximum: %+v", condition)
	}
	events := fakeclient.RecordedEvents(recorder)
	if len(events) != 1 || !strings.Contains(events[0], "ScalingTargetClamped") ||
		!strings.Contains(events[0], "maxReplicas") || !strings.Contains(events[0], "asks for 9") {
		t.Fatalf("the maximum holding the follower must be named once: %v", events)
	}

	if err := store.Get(ctx, client.ObjectKeyFromObject(engine), live); err != nil {
		t.Fatal(err)
	}
	live.Spec.Replicas = ptr.To[int32](8)
	if err := store.Update(ctx, live); err != nil {
		t.Fatal(err)
	}
	pass(t, store, projector, recorder, parent, policy, components, modes, decoder)
	if entry := followerEntry(t, parent); entry.Target != 2 || entry.Current != 2 || entry.Clamped || projector.writes != 2 {
		t.Fatalf("follower %+v after %d projections; want 2 inside its bounds", entry, projector.writes)
	}
	if events := fakeclient.RecordedEvents(recorder); len(events) != 0 {
		t.Fatalf("a follower back inside its bounds must not warn: %v", events)
	}
}

// When neither the follower nor its anchor declares a maximum, the policy is
// applied and the condition and one warning say that nothing caps the follower.
func TestReconcileWarnsOnceWhenNeitherSideDeclaresAMaximum(t *testing.T) {
	parent := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Name: "service", Namespace: "serving"}}
	engine := projectedReplica(parent, v1beta1.EngineComponent, 8, v1beta1.AutoscalerExternal)
	decoder := projectedReplica(parent, v1beta1.DecoderComponent, 2, "")
	store := fakeclient.New(t, fakeclient.WithObjects(engine, decoder))
	projector := &projection{store: store}
	components := map[v1beta1.ComponentType]*v1beta1.ComponentExtensionSpec{
		v1beta1.EngineComponent:  {MinReplicas: ptr.To(2)},
		v1beta1.DecoderComponent: {MinReplicas: ptr.To(2)},
	}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{
		v1beta1.EngineComponent: constants.OMENative, v1beta1.DecoderComponent: constants.OMENative,
	}
	recorder := record.NewFakeRecorder(8)
	policy := proportionalPolicy("1")
	for i := 0; i < 2; i++ {
		pass(t, store, projector, recorder, parent, policy, components, modes, decoder)
		entry := followerEntry(t, parent)
		if entry.Target != 8 || entry.Current != 8 || entry.Clamped || projector.writes != 1 {
			t.Fatalf("follower %+v after %d projections; want 8, not clamped, one projection", entry, projector.writes)
		}
		condition := readyCondition(t, parent)
		if condition.Status != metav1.ConditionTrue || condition.Reason != "Applied" ||
			!strings.Contains(condition.Message, "decoder") || !strings.Contains(condition.Message, "no maxReplicas") {
			t.Fatalf("condition must stay Applied and say nothing caps the decoder: %+v", condition)
		}
	}
	events := fakeclient.RecordedEvents(recorder)
	if len(events) != 1 || !strings.Contains(events[0], "ScalingFollowerUnbounded") ||
		!strings.Contains(events[0], "decoder") || !strings.Contains(events[0], "engine") {
		t.Fatalf("one warning naming both sides: %v", events)
	}

	components[v1beta1.EngineComponent].MaxReplicas = 16
	pass(t, store, projector, recorder, parent, policy, components, modes, decoder)
	if message := readyCondition(t, parent).Message; strings.Contains(message, "maxReplicas") {
		t.Fatalf("an anchor with a maximum caps the follower; no note expected: %q", message)
	}
	if events := fakeclient.RecordedEvents(recorder); len(events) != 0 {
		t.Fatalf("unexpected warning: %v", events)
	}
}
