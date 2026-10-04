package omenative

import (
	"context"
	"strings"

	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// headlessServiceSuffix is the trailing component of the per-Component
// headless Service name (`<prefix>-<component>-headless`). The
// EndpointSlice mapper uses it to recognise an OMENative-owned slice
// and reverse-lookup the name prefix.
const headlessServiceSuffix = "-headless"

// EndpointSliceToISVC maps an EndpointSlice for an OMENative headless
// Service to the InferenceService reconcile key its name prefix spells.
// Returns an empty slice for any event that doesn't target an
// OMENative-managed Service — the EndpointSlice watch is unfiltered, so
// the mapper does all the filtering.
//
// EndpointSlice carries the `kubernetes.io/service-name` label pointing
// at its parent Service. OMENative headless Services are named
// `<prefix>-<component>-headless`, prefix being the replica's name
// prefix: the InferenceService name for a projected replica, which is
// what the parse recovers. The controller's mapper refines the result
// through the Service's owner when an InferenceService controls it.
func EndpointSliceToISVC(ctx context.Context, obj client.Object) []reconcile.Request {
	slice, ok := obj.(*discoveryv1.EndpointSlice)
	if !ok {
		return nil
	}
	serviceName := slice.Labels[discoveryv1.LabelServiceName]
	isvcName, _, ok := isvcFromHeadlessServiceName(serviceName)
	if !ok {
		return nil
	}
	return []reconcile.Request{{
		NamespacedName: types.NamespacedName{
			Namespace: slice.Namespace,
			Name:      isvcName,
		},
	}}
}

// isvcFromHeadlessServiceName parses `<isvc>-<component>-headless`
// into (isvc, component, ok). The component segment is matched
// against the v1beta1 component-type set so an arbitrary
// `-foo-headless` Service in the same namespace doesn't masquerade
// as OMENative-owned.
func isvcFromHeadlessServiceName(name string) (string, v1beta1.ComponentType, bool) {
	if name == "" || !strings.HasSuffix(name, headlessServiceSuffix) {
		return "", "", false
	}
	trimmed := strings.TrimSuffix(name, headlessServiceSuffix)
	for _, component := range []v1beta1.ComponentType{
		v1beta1.RouterComponent,
		v1beta1.EngineComponent,
		v1beta1.DecoderComponent,
	} {
		suffix := "-" + string(component)
		if strings.HasSuffix(trimmed, suffix) {
			isvc := strings.TrimSuffix(trimmed, suffix)
			if isvc == "" {
				return "", "", false
			}
			return isvc, component, true
		}
	}
	return "", "", false
}
