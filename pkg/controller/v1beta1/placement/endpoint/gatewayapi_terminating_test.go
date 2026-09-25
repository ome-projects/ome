package endpoint

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"
	gatewayapiv1 "sigs.k8s.io/gateway-api/apis/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const gatewayPublicationTestFinalizer = "example.com/hold"

var gatewayPublicationResourceKinds = []string{
	"HTTPRoute",
	"Service",
	"EndpointSlice",
	"BackendTLSPolicy",
}

func TestGatewayAPITrafficMapPreflightRetriesTerminatingDesiredResources(t *testing.T) {
	for _, resourceKind := range gatewayPublicationResourceKinds {
		t.Run(resourceKind, func(t *testing.T) {
			cfg := gatewayBackendConfig()
			addresses := staticBackendAddressResolver{
				"cluster-a": {{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4}},
			}
			owner := testISVC()
			trafficMap := gatewayTrafficMap(owner,
				gatewayTrafficMapEntry("cluster-a", "backend.example", 1))
			renderer := NewGatewayAPITrafficMapPublisher(
				nil,
				nil,
				cfg,
				WithBackendAddressResolver(addresses),
			)
			plan, err := renderer.Plan(owner, trafficMap, nil)
			require.NoError(t, err)
			gatewayPlan := plan.(*gatewayAPITrafficMapPlan)
			objects := gatewayPublicationObjects(t, renderer.publisher, gatewayPlan.owner, gatewayPlan.target, addresses)
			terminating := objects[resourceKind]
			now := metav1.Now()
			terminating.SetDeletionTimestamp(&now)
			terminating.SetFinalizers([]string{gatewayPublicationTestFinalizer})

			kubeClient := fakeclient.NewClientBuilder().
				WithScheme(pubScheme(t)).
				WithObjects(terminating).
				Build()
			publisher := NewGatewayAPITrafficMapPublisher(
				kubeClient,
				kubeClient,
				cfg,
				WithBackendAddressResolver(addresses),
			)
			before := getGatewayPublicationObject(t, kubeClient, terminating)

			err = publisher.Preflight(context.Background(), plan)
			require.ErrorContains(t, err, "is terminating")
			var terminal *terminalPublisherError
			assert.False(t, errors.As(err, &terminal), "termination must remain retryable")
			after := getGatewayPublicationObject(t, kubeClient, terminating)
			assertGatewayPublicationObjectState(t, before, after)
			for kind, object := range objects {
				if kind == resourceKind {
					continue
				}
				probe := object.DeepCopyObject().(client.Object)
				err := kubeClient.Get(context.Background(), client.ObjectKeyFromObject(object), probe)
				assert.True(t, apierrors.IsNotFound(err), "%s must not be written", kind)
			}

			after.SetFinalizers(nil)
			require.NoError(t, kubeClient.Update(context.Background(), after))
			probe := terminating.DeepCopyObject().(client.Object)
			require.True(t, apierrors.IsNotFound(kubeClient.Get(
				context.Background(), client.ObjectKeyFromObject(terminating), probe,
			)))
			require.NoError(t, publisher.Preflight(context.Background(), plan))
			_, err = publisher.Apply(context.Background(), plan)
			require.NoError(t, err)
			recreated := getGatewayPublicationObject(t, kubeClient, terminating)
			assert.Nil(t, recreated.GetDeletionTimestamp())
		})
	}
}

func TestGatewayAPITrafficMapPreflightProbesBothEndpointSliceFamiliesWithoutResolver(t *testing.T) {
	for _, suffix := range []string{ipv4EndpointSliceSuffix, ipv6EndpointSliceSuffix} {
		t.Run(suffix, func(t *testing.T) {
			cfg := gatewayBackendConfig()
			owner := testISVC()
			trafficMap := gatewayTrafficMap(owner,
				gatewayTrafficMapEntry("cluster-a", "backend.example", 1))
			renderer := NewGatewayAPITrafficMapPublisher(nil, nil, cfg)
			plan, err := renderer.Plan(owner, trafficMap, nil)
			require.NoError(t, err)
			serviceName := renderer.publisher.serviceName(owner, "cluster-a")
			now := metav1.Now()
			endpointSlice := &discoveryv1.EndpointSlice{ObjectMeta: metav1.ObjectMeta{
				Namespace:         renderer.publisher.routeNamespace(owner),
				Name:              boundedResourceName(serviceName + suffix),
				Labels:            renderer.publisher.serviceLabels(owner, "cluster-a"),
				DeletionTimestamp: &now,
				Finalizers:        []string{gatewayPublicationTestFinalizer},
			}}
			kubeClient := fakeclient.NewClientBuilder().
				WithScheme(pubScheme(t)).
				WithObjects(endpointSlice).
				Build()
			publisher := NewGatewayAPITrafficMapPublisher(kubeClient, kubeClient, cfg)

			err = publisher.Preflight(context.Background(), plan)
			require.ErrorContains(t, err, "is terminating")
			assert.NotContains(t, err.Error(), "resolver")

			live := getGatewayPublicationObject(t, kubeClient, endpointSlice)
			live.SetFinalizers(nil)
			require.NoError(t, kubeClient.Update(context.Background(), live))
			require.NoError(t, publisher.Preflight(context.Background(), plan),
				"preflight must not require backend address resolution")
		})
	}
}

func TestGatewayAPITrafficMapPreflightClassifiesChildOwnershipConflicts(t *testing.T) {
	for _, resourceKind := range []string{"Service", "EndpointSlice", "BackendTLSPolicy"} {
		t.Run(resourceKind, func(t *testing.T) {
			cfg := gatewayBackendConfig()
			addresses := staticBackendAddressResolver{
				"cluster-a": {{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4}},
			}
			owner := testISVC()
			trafficMap := gatewayTrafficMap(owner,
				gatewayTrafficMapEntry("cluster-a", "backend.example", 1))
			renderer := NewGatewayAPITrafficMapPublisher(nil, nil, cfg)
			plan, err := renderer.Plan(owner, trafficMap, nil)
			require.NoError(t, err)
			gatewayPlan := plan.(*gatewayAPITrafficMapPlan)
			objects := gatewayPublicationObjects(t, renderer.publisher, gatewayPlan.owner, gatewayPlan.target, addresses)
			collision := objects[resourceKind]
			collision.SetLabels(nil)
			kubeClient := fakeclient.NewClientBuilder().
				WithScheme(pubScheme(t)).
				WithObjects(collision).
				Build()
			publisher := NewGatewayAPITrafficMapPublisher(kubeClient, kubeClient, cfg)

			err = publisher.Preflight(context.Background(), plan)
			var terminal *terminalPublisherError
			require.ErrorAs(t, err, &terminal)
			assert.ErrorContains(t, terminal.err, "belongs to another source")
		})
	}
}

func TestGatewayAPIPublisherApplyRejectsTerminationAfterPreflight(t *testing.T) {
	for _, resourceKind := range gatewayPublicationResourceKinds {
		t.Run(resourceKind, func(t *testing.T) {
			cfg := gatewayBackendConfig()
			addresses := staticBackendAddressResolver{
				"cluster-a": {{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4}},
			}
			owner := testISVC()
			target := oneHome("svc.prod.global.example", "cluster-a", "backend.example")
			renderer := NewGatewayAPIPublisher(
				nil,
				cfg,
				withStrictResourceOwnership(),
				withTrafficMapBackendNaming(),
			)
			objects := gatewayPublicationObjects(t, renderer, owner, target, addresses)
			liveObjects := make([]client.Object, 0, len(objects))
			for kind, object := range objects {
				live := object.DeepCopyObject().(client.Object)
				if kind == resourceKind {
					live.SetFinalizers([]string{gatewayPublicationTestFinalizer})
					makeGatewayPublicationObjectStale(live)
				}
				liveObjects = append(liveObjects, live)
			}
			kubeClient := fakeclient.NewClientBuilder().
				WithScheme(pubScheme(t)).
				WithObjects(liveObjects...).
				Build()
			targetObject := objects[resourceKind]
			before := getGatewayPublicationObject(t, kubeClient, targetObject)
			reader := &terminateGatewayResourceOnGetReader{
				Reader:      kubeClient,
				writer:      kubeClient,
				targetKey:   client.ObjectKeyFromObject(targetObject),
				targetType:  reflect.TypeOf(targetObject),
				terminateOn: 2,
			}
			publisher := NewGatewayAPIPublisher(
				kubeClient,
				cfg,
				WithBackendAddressResolver(addresses),
				WithGatewayAPIReader(reader),
				withStrictResourceOwnership(),
				withTrafficMapBackendNaming(),
			)

			err := publisher.Publish(context.Background(), owner, target)
			require.ErrorContains(t, err, "is terminating")
			assert.Equal(t, 2, reader.targetGets, "preflight and apply must each read the target")
			after := getGatewayPublicationObject(t, kubeClient, targetObject)
			require.NotNil(t, after.GetDeletionTimestamp())
			assert.Equal(t, []string{gatewayPublicationTestFinalizer}, after.GetFinalizers())
			assertGatewayPublicationSpecAndLabels(t, before, after)

			after.SetFinalizers(nil)
			require.NoError(t, kubeClient.Update(context.Background(), after))
			require.NoError(t, publisher.Publish(context.Background(), owner, target))
			recreated := getGatewayPublicationObject(t, kubeClient, targetObject)
			assert.Nil(t, recreated.GetDeletionTimestamp())
			assertGatewayPublicationSpecAndLabels(t, targetObject, recreated)
		})
	}
}

func TestGatewayAPIPublisherPreservesFinalizersOnOwnedUpdates(t *testing.T) {
	cfg := gatewayBackendConfig()
	addresses := staticBackendAddressResolver{
		"cluster-a": {{Address: "192.0.2.10", Type: discoveryv1.AddressTypeIPv4}},
	}
	owner := testISVC()
	target := oneHome("svc.prod.global.example", "cluster-a", "backend.example")
	renderer := NewGatewayAPIPublisher(
		nil,
		cfg,
		withStrictResourceOwnership(),
		withTrafficMapBackendNaming(),
	)
	desired := gatewayPublicationObjects(t, renderer, owner, target, addresses)
	liveObjects := make([]client.Object, 0, len(desired))
	for _, object := range desired {
		live := object.DeepCopyObject().(client.Object)
		live.SetFinalizers([]string{gatewayPublicationTestFinalizer})
		makeGatewayPublicationObjectStale(live)
		liveObjects = append(liveObjects, live)
	}
	kubeClient := fakeclient.NewClientBuilder().
		WithScheme(pubScheme(t)).
		WithObjects(liveObjects...).
		Build()
	publisher := NewGatewayAPIPublisher(
		kubeClient,
		cfg,
		WithBackendAddressResolver(addresses),
		WithGatewayAPIReader(kubeClient),
		withStrictResourceOwnership(),
		withTrafficMapBackendNaming(),
	)

	require.NoError(t, publisher.Publish(context.Background(), owner, target))
	for kind, object := range desired {
		updated := getGatewayPublicationObject(t, kubeClient, object)
		assert.Equal(t, []string{gatewayPublicationTestFinalizer}, updated.GetFinalizers(), "%s", kind)
		assertGatewayPublicationSpecAndLabels(t, object, updated)
	}
}

type terminateGatewayResourceOnGetReader struct {
	client.Reader
	writer      client.Client
	targetKey   client.ObjectKey
	targetType  reflect.Type
	terminateOn int
	targetGets  int
}

func (r *terminateGatewayResourceOnGetReader) Get(
	ctx context.Context,
	key client.ObjectKey,
	object client.Object,
	options ...client.GetOption,
) error {
	if key == r.targetKey && reflect.TypeOf(object) == r.targetType {
		r.targetGets++
		if r.targetGets == r.terminateOn {
			live := object.DeepCopyObject().(client.Object)
			if err := r.Reader.Get(ctx, key, live, options...); err != nil {
				return fmt.Errorf("load resource before deletion race: %w", err)
			}
			if err := r.writer.Delete(ctx, live); err != nil {
				return fmt.Errorf("start resource deletion race: %w", err)
			}
		}
	}
	return r.Reader.Get(ctx, key, object, options...)
}

func gatewayPublicationObjects(
	t *testing.T,
	publisher *GatewayAPIPublisher,
	owner *v1beta1.InferenceService,
	target Target,
	addresses map[string][]BackendAddress,
) map[string]client.Object {
	t.Helper()
	require.Len(t, target.Homes, 1)
	home := target.Homes[0]
	serviceName := publisher.serviceName(owner, home.Cluster)
	endpointSlices, err := publisher.buildEndpointSlices(owner, serviceName, home, addresses[home.Cluster])
	require.NoError(t, err)
	require.Len(t, endpointSlices, 1)
	return map[string]client.Object{
		"HTTPRoute":        publisher.buildHTTPRoute(owner, target),
		"Service":          publisher.buildExternalNameService(owner, serviceName, home),
		"EndpointSlice":    endpointSlices[0],
		"BackendTLSPolicy": publisher.buildBackendTLSPolicy(owner, serviceName, home),
	}
}

func getGatewayPublicationObject(t *testing.T, c client.Client, object client.Object) client.Object {
	t.Helper()
	result := object.DeepCopyObject().(client.Object)
	require.NoError(t, c.Get(context.Background(), client.ObjectKeyFromObject(object), result))
	return result
}

func makeGatewayPublicationObjectStale(object client.Object) {
	switch typed := object.(type) {
	case *gatewayapiv1.HTTPRoute:
		typed.Spec.Hostnames = []gatewayapiv1.Hostname{"stale.example"}
	case *corev1.Service:
		typed.Spec.ExternalName = "stale.example"
	case *discoveryv1.EndpointSlice:
		typed.Endpoints[0].Addresses = []string{"192.0.2.99"}
	case *gatewayapiv1.BackendTLSPolicy:
		typed.Spec.Validation.Hostname = "stale.example"
	default:
		panic(fmt.Sprintf("unsupported Gateway publication object %T", object))
	}
}

func assertGatewayPublicationObjectState(t *testing.T, expected, actual client.Object) {
	t.Helper()
	assert.Equal(t, expected.GetDeletionTimestamp(), actual.GetDeletionTimestamp())
	assert.Equal(t, expected.GetFinalizers(), actual.GetFinalizers())
	assertGatewayPublicationSpecAndLabels(t, expected, actual)
}

func assertGatewayPublicationSpecAndLabels(t *testing.T, expected, actual client.Object) {
	t.Helper()
	assert.Equal(t, expected.GetLabels(), actual.GetLabels())
	switch expectedTyped := expected.(type) {
	case *gatewayapiv1.HTTPRoute:
		actualTyped, ok := actual.(*gatewayapiv1.HTTPRoute)
		require.True(t, ok)
		assert.Equal(t, expectedTyped.Spec, actualTyped.Spec)
	case *corev1.Service:
		actualTyped, ok := actual.(*corev1.Service)
		require.True(t, ok)
		assert.Equal(t, expectedTyped.Spec, actualTyped.Spec)
	case *discoveryv1.EndpointSlice:
		actualTyped, ok := actual.(*discoveryv1.EndpointSlice)
		require.True(t, ok)
		assert.Equal(t, expectedTyped.AddressType, actualTyped.AddressType)
		assert.Equal(t, expectedTyped.Endpoints, actualTyped.Endpoints)
		assert.Equal(t, expectedTyped.Ports, actualTyped.Ports)
	case *gatewayapiv1.BackendTLSPolicy:
		actualTyped, ok := actual.(*gatewayapiv1.BackendTLSPolicy)
		require.True(t, ok)
		assert.Equal(t, expectedTyped.Spec, actualTyped.Spec)
	default:
		t.Fatalf("unsupported Gateway publication object %T", expected)
	}
}
