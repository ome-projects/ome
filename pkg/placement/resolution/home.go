package resolution

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/specdefaults"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// ResolveHome resolves the complete per-home floor using member precedence.
// Configuration is a dependency only when a declared component inherits its
// floor from the operator. All dependencies remain fenced by Runtime.Check.
func (r Resolver) ResolveHome(ctx context.Context, desired, standing *v1beta1.InferenceService) (*Runtime, []v1beta1.PlacementComponentFloor, error) {
	runtime, err := r.ResolveNative(ctx, desired, standing)
	if err != nil {
		return nil, nil, err
	}
	if (runtime.Engine != nil && runtime.Engine.MinReplicas == nil) ||
		(runtime.Decoder != nil && runtime.Decoder.MinReplicas == nil) ||
		(runtime.Router != nil && runtime.Router.MinReplicas == nil) {
		if r.OperatorNamespace == "" {
			return nil, nil, fmt.Errorf("home floor resolution requires the member operator namespace")
		}
		cm := &corev1.ConfigMap{}
		if err := runtime.reads.Get(ctx, client.ObjectKey{Namespace: r.OperatorNamespace, Name: constants.InferenceServiceConfigMapName}, cm); err != nil {
			return nil, nil, err
		}
		cfg, err := controllerconfig.DeploymentConfig(cm)
		if err != nil {
			return nil, nil, err
		}
		specdefaults.Engine(runtime.Engine, constants.OMENative, cfg)
		specdefaults.Decoder(runtime.Decoder, constants.OMENative, cfg)
		specdefaults.Router(runtime.Router, constants.OMENative, cfg)
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
