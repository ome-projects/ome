package resolution

import (
	"context"
	"errors"
	"fmt"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/placement/protocol"
)

// ErrUnsupportedBackend distinguishes a resolved unsupported backend from
// unavailable runtime inputs. Both prevent placement actuation.
var ErrUnsupportedBackend = errors.New("unsupported multicluster backend")

// ResolveNative verifies every declared component without requiring hardware
// reports or rendering configuration. Dependencies remain available for Check.
func (r Resolver) ResolveNative(ctx context.Context, desired, standing *v1beta1.InferenceService) (*Runtime, error) {
	if isvcutils.InferenceServiceDeploymentMode(desired, "") == constants.VirtualDeployment {
		return nil, fmt.Errorf("%w: VirtualDeployment has no member admission or surge accounting", ErrUnsupportedBackend)
	}
	runtime, err := r.Resolve(ctx, desired, standing)
	if err != nil {
		return nil, err
	}
	engine, decoder, router, err := isvcutils.DetermineDeploymentModes(runtime.Engine, runtime.Decoder, runtime.Router, runtime.Spec, desired.Spec.DeploymentMode)
	if err != nil {
		return nil, err
	}
	modes := map[v1beta1.ComponentType]constants.DeploymentModeType{v1beta1.EngineComponent: engine}
	if runtime.Decoder != nil {
		modes[v1beta1.DecoderComponent] = decoder
	}
	if runtime.Router != nil {
		modes[v1beta1.RouterComponent] = router
	}
	if err := protocol.ValidateNativeModes(modes); err != nil {
		return nil, fmt.Errorf("%w: %w", ErrUnsupportedBackend, err)
	}
	return runtime, nil
}
