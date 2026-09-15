package doctorcollection

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

const MaxDiscoveryResources = 256

var discoveryGroups = []struct{ version, path string }{
	{"v1", "/api/v1"},
	{"apps/v1", "/apis/apps/v1"},
	{"ome.io/v1beta1", "/apis/ome.io/v1beta1"},
	{"autoscaling/v2", "/apis/autoscaling/v2"},
	{"keda.sh/v1alpha1", "/apis/keda.sh/v1alpha1"},
	{"gateway.networking.k8s.io/v1", "/apis/gateway.networking.k8s.io/v1"},
	{"kueue.x-k8s.io/v1beta1", "/apis/kueue.x-k8s.io/v1beta1"},
}

// discoveryRows never interprets advertised verbs as the caller's permissions.
// Limits apply after decoding; the REST seam does not promise a byte cap.
func discoveryRows(version string, document *metav1.APIResourceList, reason r.DoctorReason) []r.DoctorAPI {
	rows := []r.DoctorAPI{}
	if reason == "" {
		switch {
		case document == nil || document.GroupVersion != version:
			reason = r.DoctorMalformed
		case len(document.APIResources) > MaxDiscoveryResources:
			reason = r.DoctorTruncated
		}
	}
	for _, expected := range r.DoctorAPICatalog() {
		if expected.GroupVersion != version {
			continue
		}
		expected.Availability, expected.Evidence = r.DoctorUnavailable, r.EvidenceUnavailable
		expected.Reason = reason
		if reason == r.DoctorNotFound {
			expected.Availability = r.DoctorNotDiscoverable
		}
		if reason == "" {
			matches := 0
			valid := false
			for _, candidate := range document.APIResources {
				if candidate.Name != expected.Resource {
					continue
				}
				matches++
				valid = candidate.Kind == expected.Kind && candidate.Namespaced == (expected.Scope == "Namespaced")
			}
			switch {
			case matches == 0:
				expected.Availability = r.DoctorNotDiscoverable
			case matches != 1 || !valid:
				expected.Reason = r.DoctorMalformed
			default:
				expected.Availability, expected.Evidence = r.DoctorAvailable, r.EvidenceObserved
			}
		}
		rows = append(rows, expected)
	}
	return rows
}
