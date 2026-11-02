package types

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// PerComponentServiceSpec is the typed input the workload package
// consumes to render a per-Component supporting Service (today: the
// headless Service that gives every pod a stable peer-DNS FQDN).
// Adapters populate every field from their owner-CRD shape; workload
// code reads the spec opaquely and never reaches back into the owner.
//
// One Service per (workload-key, component). The ISVC adapter
// constructs one spec per Component reconcile pass; the IR adapter
// constructs one spec per InferenceReplica reconcile.
type PerComponentServiceSpec struct {
	// Name is the Service object name. The workload renderer stamps it
	// verbatim but does NOT compute it — adapters pass the canonical
	// name (e.g., query.HeadlessServiceName(owner.Name, component) on
	// the ISVC side) so the per-Component naming convention stays in
	// the adapter where the owner-CRD details live.
	Name string

	// Namespace is the Service object namespace. Matches the owner CR
	// namespace; adapters set it from owner.GetNamespace().
	Namespace string

	// Selector is the pod-label selector scoping which pods are members
	// of this Service. Adapters set it so the Service only selects pods
	// the workload owns (not legacy LWS / RawDeployment pods that may
	// share the bare Component label). The ISVC adapter wires
	// `ome.io/inferenceservice + component + managed-by=OMENative`; the
	// IR adapter wires the IR-side equivalent.
	Selector map[string]string

	// Labels are stamped on the Service object metadata block for
	// downstream tooling (`kubectl get svc -l`, dashboards). Typically
	// the same key/value pairs as Selector plus the controller's
	// `managed-by` label.
	Labels map[string]string

	// OwnerReferences point back to the workload's owner CR. Adapters
	// pass `*metav1.NewControllerRef(owner, ownerGVK)` so deletion of
	// the owner cascades to the Service via the K8s garbage collector.
	OwnerReferences []metav1.OwnerReference
}
