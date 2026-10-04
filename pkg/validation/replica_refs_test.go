package validation

import (
	"strings"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func TestValidateReplicaRefs(t *testing.T) {
	refs := func(engine, decoder, router []string) *v1beta1.ReplicaRefs {
		return &v1beta1.ReplicaRefs{Engine: engine, Decoder: decoder, Router: router}
	}
	for _, tc := range []struct {
		name    string
		spec    v1beta1.InferenceServiceSpec
		wantErr string
	}{
		{name: "inline service", spec: v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}, Rollout: &v1beta1.RolloutSpec{}}},
		{name: "engine only", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil)}},
		{name: "router, engine and decoder", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, []string{"pool-d"}, []string{"router-a"})}},
		{name: "router and engine", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, []string{"router-a"})}},
		{name: "OMENative deployment mode", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), DeploymentMode: ptr.To(constants.OMENative)}},
		{name: "engine required", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs(nil, nil, []string{"router-a"})}, wantErr: "spec.replicaRefs.engine is required"},
		{name: "two engines", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a", "pool-b"}, nil, nil)}, wantErr: "spec.replicaRefs.engine names 2 replicas; one entry per role"},
		{name: "not a DNS label", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"Pool_A"}, nil, nil)}, wantErr: `spec.replicaRefs.engine[0] "Pool_A" must match`},
		{name: "one replica in two roles", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, []string{"pool-a"}, nil)}, wantErr: "InferenceReplica pool-a is named by both spec.replicaRefs.engine and spec.replicaRefs.decoder"},
		{name: "inline engine beside a reference", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), Engine: &v1beta1.EngineSpec{}}, wantErr: "spec.engine and spec.replicaRefs cannot both be set"},
		{name: "inline router beside a reference", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), Router: &v1beta1.RouterSpec{}}, wantErr: "spec.router and spec.replicaRefs cannot both be set"},
		{name: "rollout group", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), Rollout: &v1beta1.RolloutSpec{Groups: []v1beta1.RolloutGroup{{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}}}}}, wantErr: "spec.rollout is not accepted with spec.replicaRefs"},
		{name: "pairing protocol", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), Rollout: &v1beta1.RolloutSpec{PairingProtocol: ptr.To("v1")}}, wantErr: "spec.rollout is not accepted with spec.replicaRefs"},
		{name: "model", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), Model: &v1beta1.ModelRef{Name: "example-model"}}, wantErr: "spec.model is not accepted with spec.replicaRefs; the referenced replicas carry their own model and runtime"},
		{name: "runtime", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), Runtime: &v1beta1.ServingRuntimeRef{Name: "example-runtime"}}, wantErr: "spec.runtime is not accepted with spec.replicaRefs; the referenced replicas carry their own model and runtime"},
		{name: "scaling policy", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), ScalingPolicy: &v1beta1.ScalingPolicy{}}, wantErr: "spec.scalingPolicy is not accepted with spec.replicaRefs; the service writes nothing on, and renders nothing for, a referenced replica"},
		{name: "accelerator selector", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), AcceleratorSelector: &v1beta1.AcceleratorSelector{}}, wantErr: "spec.acceleratorSelector is not accepted with spec.replicaRefs; the service writes nothing on, and renders nothing for, a referenced replica"},
		{name: "placement", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), Placement: &v1beta1.PlacementSpec{}}, wantErr: "spec.placement is not accepted with spec.replicaRefs; the referenced replicas exist in this cluster only"},
		{name: "routing", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), Routing: &v1beta1.RoutingSpec{}}, wantErr: "spec.routing is not accepted with spec.replicaRefs; the referenced replicas exist in this cluster only"},
		{name: "raw deployment mode", spec: v1beta1.InferenceServiceSpec{ReplicaRefs: refs([]string{"pool-a"}, nil, nil), DeploymentMode: ptr.To(constants.RawDeployment)}, wantErr: "spec.deploymentMode RawDeployment is not accepted with spec.replicaRefs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateReplicaRefs(&tc.spec)
			if tc.wantErr == "" && err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if tc.wantErr != "" && (err == nil || !strings.Contains(err.Error(), tc.wantErr)) {
				t.Fatalf("error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

func TestValidateReplicaRefsUpdate(t *testing.T) {
	engine := func(names ...string) *v1beta1.InferenceServiceSpec {
		if names == nil {
			return &v1beta1.InferenceServiceSpec{}
		}
		return &v1beta1.InferenceServiceSpec{ReplicaRefs: &v1beta1.ReplicaRefs{Engine: names}}
	}
	for _, tc := range []struct {
		name     string
		old, new *v1beta1.InferenceServiceSpec
		wantErr  bool
	}{
		{name: "unchanged", old: engine("pool-a"), new: engine("pool-a")},
		{name: "inline stays inline", old: engine(), new: engine()},
		{name: "changed", old: engine("pool-a"), new: engine("pool-b"), wantErr: true},
		{name: "added", old: engine(), new: engine("pool-a"), wantErr: true},
		{name: "dropped", old: engine("pool-a"), new: engine(), wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := ValidateReplicaRefsUpdate(tc.old, tc.new)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tc.wantErr)
			}
		})
	}
}

func TestReferencedReplicaAndProjectedBy(t *testing.T) {
	spec := &v1beta1.InferenceServiceSpec{ReplicaRefs: &v1beta1.ReplicaRefs{Router: []string{"router-a"}}}
	if got := ReferencedReplica(spec, v1beta1.RouterComponent); got != "" {
		t.Fatalf("a block without an engine must not apply, got %q", got)
	}
	spec.ReplicaRefs.Engine = []string{"pool-a"}
	if got := ReferencedReplica(spec, v1beta1.RouterComponent); got != "router-a" {
		t.Fatalf("router reference = %q, want router-a", got)
	}
	if got := ReferencedReplica(spec, v1beta1.DecoderComponent); got != "" {
		t.Fatalf("decoder reference = %q, want none", got)
	}
	standalone := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "pool-a"}}
	if got := ProjectedBy(standalone); got != "" {
		t.Fatalf("standalone replica reported as projected by %q", got)
	}
	byParent := &v1beta1.InferenceReplica{Spec: v1beta1.InferenceReplicaSpec{ParentRef: &v1beta1.ParentReference{Name: "svc"}}}
	if got := ProjectedBy(byParent); got != "svc" {
		t.Fatalf("parentRef replica projected by %q, want svc", got)
	}
	byOwner := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{OwnerReferences: []metav1.OwnerReference{{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService", Name: "svc", Controller: ptr.To(true),
	}}}}
	if got := ProjectedBy(byOwner); got != "svc" {
		t.Fatalf("controller-owned replica projected by %q, want svc", got)
	}
}
