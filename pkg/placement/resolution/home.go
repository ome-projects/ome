package resolution

import (
	"context"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/placement/protocol"
	"sigs.k8s.io/ome/pkg/specdefaults"
)

// ResolveAssignedCluster resolves the member runtime on the cluster the
// placement chose, with member precedence, and the per-component replica
// minimums that runtime declares. The operator configuration is read only when
// a declared component leaves its minimum replicas unset. Every dependency read
// stays behind Runtime.Check.
func (r Resolver) ResolveAssignedCluster(ctx context.Context, desired, standing *v1beta1.InferenceService) (*Runtime, []v1beta1.PlacementComponentFloor, error) {
	runtime, err := r.ResolveNative(ctx, desired, standing)
	if err != nil {
		return nil, nil, err
	}
	if runtime.minimumReplicasUnset() {
		if err := r.applyDeployDefaults(ctx, runtime); err != nil {
			return nil, nil, err
		}
	}
	floors, err := protocol.ResolveReplicaFloors(runtime.Engine, runtime.Decoder, runtime.Router)
	if err != nil {
		return nil, nil, err
	}
	if err := runtime.Check(ctx); err != nil {
		return nil, nil, err
	}
	return runtime, floors, nil
}

// minimumReplicasUnset reports whether a declared component leaves its minimum
// replicas to the operator's deploy defaults.
func (r *Runtime) minimumReplicasUnset() bool {
	return (r.Engine != nil && r.Engine.MinReplicas == nil) ||
		(r.Decoder != nil && r.Decoder.MinReplicas == nil) ||
		(r.Router != nil && r.Router.MinReplicas == nil)
}

// applyDeployDefaults fills what the declared components leave unset from the
// member operator's deploy defaults.
func (r Resolver) applyDeployDefaults(ctx context.Context, runtime *Runtime) error {
	if err := requireOperatorNamespace(r.OperatorNamespace, "home floor resolution"); err != nil {
		return err
	}
	configMap, err := r.operatorConfigMap(ctx, runtime.reads)
	if err != nil {
		return err
	}
	cfg, err := controllerconfig.DeploymentConfig(configMap)
	if err != nil {
		return err
	}
	specdefaults.Engine(runtime.Engine, constants.OMENative, cfg)
	specdefaults.Decoder(runtime.Decoder, constants.OMENative, cfg)
	specdefaults.Router(runtime.Router, constants.OMENative, cfg)
	return nil
}
