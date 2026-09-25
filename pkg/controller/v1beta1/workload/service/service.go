// Package service owns the per-Component headless Service that gives
// every workload-managed pod a stable peer-DNS FQDN of the form
// `<pod-name>.<owner>-<component>-headless.<namespace>.svc.<cluster-domain>`.
//
// Functions take `types.PerComponentServiceSpec` rather than an owner
// CRD object so the workload tree stays free of the owner-CRD typed
// import boundary. Invoked by the adapter caller before
// workload.Reconcile, alongside PodMonitor and PodGroup wiring.
package service

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// BuildHeadlessService produces the per-Component headless Service that
// gives every workload-managed pod a stable FQDN of the form
//
//	<pod-name>.<spec.Name>.<spec.Namespace>.svc.<cluster-domain>
//
// Properties:
//   - ClusterIP=None — no kube-proxy load-balancing; DNS lookups return
//     per-pod A records.
//   - PublishNotReadyAddresses=true — peer DNS resolves during gang init
//     before all pods are Ready, so workers can discover each other while
//     the controller-owned ome.io/serving gate is still False.
//   - Selector scoped by the adapter so the Service does not pick up
//     legacy LWS-managed or RawDeployment pods that share the bare
//     component label.
//   - OwnerReferences pointing back to the owner CR; deletion of the
//     owner cascades to this Service.
//
// Pure compute, no I/O. Returns an error (rather than panicking) when
// the spec is missing required fields so callers — including tests —
// can fail loud at the boundary.
func BuildHeadlessService(spec types.PerComponentServiceSpec) (*corev1.Service, error) {
	if spec.Name == "" {
		return nil, fmt.Errorf("BuildHeadlessService: empty Name")
	}
	if spec.Namespace == "" {
		return nil, fmt.Errorf("BuildHeadlessService: empty Namespace")
	}

	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:            spec.Name,
			Namespace:       spec.Namespace,
			Labels:          spec.Labels,
			OwnerReferences: spec.OwnerReferences,
		},
		Spec: corev1.ServiceSpec{
			ClusterIP:                corev1.ClusterIPNone,
			PublishNotReadyAddresses: true,
			Selector:                 spec.Selector,
			// Ports omitted by design: this Service exists for DNS-based
			// peer discovery, not for kube-proxy routing. Each runtime
			// resolves <pod-name>.<this-service>... and dials whatever
			// port it cares about. Adding a placeholder port would imply
			// load-balanceable endpoints, which is the opposite of what
			// the headless contract is for.
			Type: corev1.ServiceTypeClusterIP,
		},
	}, nil
}

// ReconcileHeadlessService ensures the per-Component headless Service
// (`spec.Name`) exists and matches the spec BuildHeadlessService
// produces. Idempotent and drift-correcting: on every reconcile it
// patches Selector / PublishNotReadyAddresses / Type / Labels /
// OwnerReferences back to the controller-desired shape, but never
// touches ClusterIP after Create (immutable post-create).
//
// Called at the top of the adapter's per-Component reconcile pass
// before any per-Instance work so the drain path's EndpointSlice
// reads have a Service to look at on the first reconcile. Without
// this, drain.IsPodDrained would fall through its "no slices ==
// drained" branch on every drain check and skip the wait entirely.
func ReconcileHeadlessService(ctx context.Context, c client.Client, spec types.PerComponentServiceSpec) error {
	if c == nil {
		return fmt.Errorf("ReconcileHeadlessService: nil client")
	}

	desired, err := BuildHeadlessService(spec)
	if err != nil {
		return fmt.Errorf("ReconcileHeadlessService: build desired: %w", err)
	}

	target := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}

	_, err = controllerutil.CreateOrUpdate(ctx, c, target, func() error {
		// ClusterIP is immutable after create. Only stamp it on the
		// fresh-create path; existing Services keep whatever the
		// apiserver assigned (or kept from previous reconciles, since
		// we always declare ClusterIPNone).
		if target.CreationTimestamp.IsZero() {
			target.Spec.ClusterIP = desired.Spec.ClusterIP
		}
		target.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		target.Spec.Selector = desired.Spec.Selector
		target.Spec.Type = desired.Spec.Type
		// Ports stay nil — see BuildHeadlessService comment for why
		// (peer-DNS only, no kube-proxy routing).
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		return nil
	})
	if err != nil {
		return fmt.Errorf("ReconcileHeadlessService: CreateOrUpdate %s/%s: %w",
			target.Namespace, target.Name, err)
	}
	return nil
}
