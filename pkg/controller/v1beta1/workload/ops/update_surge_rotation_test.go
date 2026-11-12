package ops

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The single-pod surge reads the routed Service of the attempt's revision
// before it moves the source: the source leaves rotation, and is deleted,
// only behind a replacement that Service publishes for new traffic, or
// when there is no such Service for it to be published in.

// replacementRoutedServiceName is the per-revision routed Service the
// coordination layer creates for the fixture's replacement revision.
func replacementRoutedServiceName(f *midSurgeFixture) string {
	hash := query.RevisionHashFromControllerRevisionName(f.targetCR.Name)
	return query.PerRevisionServiceName(f.isvc.Name, workload.ComponentEngine, hash)
}

// createReplacementRoutedService creates that Service with no slice behind
// it, the moment before the endpoints controller writes one.
func createReplacementRoutedService(t *testing.T, f *midSurgeFixture) string {
	t.Helper()
	name := replacementRoutedServiceName(f)
	svc := &corev1.Service{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: f.isvc.Namespace}}
	if err := f.client.Create(context.Background(), svc); err != nil {
		t.Fatalf("create routed Service %s: %v", name, err)
	}
	return name
}

// replacementEndpoint is the Service's endpoint for the replacement with
// the given Ready and Terminating conditions.
func replacementEndpoint(f *midSurgeFixture, ready, terminating bool) discoveryv1.Endpoint {
	return discoveryv1.Endpoint{
		Addresses: []string{"10.0.0.2"},
		Conditions: discoveryv1.EndpointConditions{
			Ready:       ptr.To(ready),
			Terminating: ptr.To(terminating),
		},
		TargetRef: &corev1.ObjectReference{Kind: "Pod", Namespace: f.surgePod.Namespace, Name: f.surgePod.Name},
	}
}

// setReplacementSlice writes the Service's one EndpointSlice with exactly
// the given endpoints, creating it on the first call.
func setReplacementSlice(t *testing.T, f *midSurgeFixture, serviceName string, endpoints ...discoveryv1.Endpoint) {
	t.Helper()
	key := client.ObjectKey{Namespace: f.isvc.Namespace, Name: serviceName + "-abcde"}
	slice := &discoveryv1.EndpointSlice{}
	err := f.client.Get(context.Background(), key, slice)
	switch {
	case apierrors.IsNotFound(err):
		slice = &discoveryv1.EndpointSlice{
			ObjectMeta: metav1.ObjectMeta{
				Name:      key.Name,
				Namespace: key.Namespace,
				Labels:    map[string]string{discoveryv1.LabelServiceName: serviceName},
			},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints:   endpoints,
		}
		err = f.client.Create(context.Background(), slice)
	case err == nil:
		slice.Endpoints = endpoints
		err = f.client.Update(context.Background(), slice)
	}
	if err != nil {
		t.Fatalf("write EndpointSlice of %s: %v", serviceName, err)
	}
}

// surgeHeldWithBothServing is the pass outcome of a source kept in rotation
// at the Surge step beside a Ready, serving replacement.
var surgeHeldWithBothServing = surgeOutcome{
	phase:         v1beta1.OMENativeInstanceUpdating,
	step:          workload.UpdateStepSurge,
	sourceAlive:   true,
	sourceServing: true,
	targetAlive:   true,
	targetServing: true,
}

func TestSurgeUpdate_SourceServesUntilTheRoutedServiceListsTheReplacement(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurge, true /* surgeReady */, true /* sourceServing */)
	service := createReplacementRoutedService(t, f)

	// The Service stands with no slice yet: the endpoints controller is
	// behind, and the replacement is Ready to the kubelet alone.
	if got := runSurgePass(t, f, nil); got != surgeHeldWithBothServing {
		t.Fatalf("Service without a slice: got %+v want %+v", got, surgeHeldWithBothServing)
	}

	// A slice that names no pod: the replacement is still unpublished, and
	// a second pass on the same picture writes nothing.
	setReplacementSlice(t, f, service)
	rv := readRV(t, f.client, f.isvc)
	if got := runSurgePass(t, f, nil); got != surgeHeldWithBothServing {
		t.Fatalf("slice without the replacement: got %+v want %+v", got, surgeHeldWithBothServing)
	}
	if got := readRV(t, f.client, f.isvc); got != rv {
		t.Errorf("owner resourceVersion: got %s want %s (a held source writes nothing)", got, rv)
	}

	// The slice publishes the replacement: the step moves to the drain and
	// the source leaves rotation; with no routed Service of its own
	// revision it reads drained at once and is deleted in the same pass.
	setReplacementSlice(t, f, service, replacementEndpoint(f, true, false))
	got := runSurgePass(t, f, nil)
	if got.step != workload.UpdateStepSurgeDrain || got.sourceAlive || !got.targetServing {
		t.Fatalf("listed replacement: got %+v want the drain step with the source gone", got)
	}
}

func TestSurgeUpdate_ATerminatingReplacementEndpointKeepsTheSourceServing(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurge, true, true)
	service := createReplacementRoutedService(t, f)
	// Ready and Terminating at once is a pod kube-proxy keeps for the
	// requests in flight and sends no new ones: not a pod that can take
	// the source's traffic.
	setReplacementSlice(t, f, service, replacementEndpoint(f, true, true))

	if got := runSurgePass(t, f, nil); got != surgeHeldWithBothServing {
		t.Fatalf("terminating endpoint: got %+v want %+v", got, surgeHeldWithBothServing)
	}
}

func TestSurgeUpdate_WithNoRoutedServiceTheSourceLeavesRotationOnPodConditions(t *testing.T) {
	// No Service of the replacement's revision exists, as for a replica no
	// InferenceService controller fronts: there is nothing to be listed in,
	// and the pod conditions alone move the step.
	f := newMidSurgeFixture(t, workload.UpdateStepSurge, true, true)

	got := runSurgePass(t, f, nil)
	if got.step != workload.UpdateStepSurgeDrain || got.sourceAlive || !got.targetServing {
		t.Fatalf("no routed Service: got %+v want the drain step with the source gone", got)
	}
}

func TestSurgeDrain_DeletesTheSourceOnlyBehindAListedReplacement(t *testing.T) {
	f := newMidSurgeFixture(t, workload.UpdateStepSurgeDrain, true /* surgeReady */, false /* sourceServing */)
	service := createReplacementRoutedService(t, f)
	// The replacement dropped out of the Service while the source drained:
	// the source, already out of rotation, is kept rather than deleted.
	setReplacementSlice(t, f, service, replacementEndpoint(f, false, false))

	got := runSurgePass(t, f, nil)
	want := surgeOutcome{
		phase:         v1beta1.OMENativeInstanceUpdating,
		step:          workload.UpdateStepSurgeDrain,
		sourceAlive:   true,
		targetAlive:   true,
		targetServing: true,
	}
	if got != want {
		t.Fatalf("replacement unlisted during the drain: got %+v want %+v", got, want)
	}

	// The Service lists the replacement again: the drained source goes.
	setReplacementSlice(t, f, service, replacementEndpoint(f, true, false))
	got = runSurgePass(t, f, nil)
	if got.sourceAlive || got.step != workload.UpdateStepSurgeDrain {
		t.Fatalf("replacement listed again: got %+v want the source deleted at the drain step", got)
	}
}
