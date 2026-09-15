package doctorcollection

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	r "sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestDiscoveryMissingMalformedAndDecodedCaps(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*metav1.APIResourceList)
		raw    string
		want   r.DoctorAvailability
		reason r.DoctorReason
	}{
		{name: "missing", change: func(v *metav1.APIResourceList) { v.APIResources = []metav1.APIResource{} }, want: r.DoctorNotDiscoverable},
		{name: "wrong group", change: func(v *metav1.APIResourceList) { v.GroupVersion = "SECRET" }, want: r.DoctorUnavailable, reason: r.DoctorMalformed},
		{name: "wrong scope", change: func(v *metav1.APIResourceList) { v.APIResources[0].Namespaced = false }, want: r.DoctorUnavailable, reason: r.DoctorMalformed},
		{name: "wrong kind", change: func(v *metav1.APIResourceList) { v.APIResources[0].Kind = "SECRET" }, want: r.DoctorUnavailable, reason: r.DoctorMalformed},
		{name: "duplicate", change: func(v *metav1.APIResourceList) { v.APIResources = append(v.APIResources, v.APIResources[0]) }, want: r.DoctorUnavailable, reason: r.DoctorMalformed},
		{name: "oversized", change: func(v *metav1.APIResourceList) {
			for len(v.APIResources) < 257 {
				v.APIResources = append(v.APIResources, metav1.APIResource{Name: "SECRET"})
			}
		}, want: r.DoctorUnavailable, reason: r.DoctorTruncated},
		{name: "malformed JSON", raw: `{"SECRET":`, want: r.DoctorUnavailable, reason: r.DoctorMalformed},
		{name: "null document", raw: `null`, want: r.DoctorUnavailable, reason: r.DoctorMalformed},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clients, _, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
				if req.URL.Path != "/apis/ome.io/v1beta1" {
					return false
				}
				if tc.raw != "" {
					w.Write([]byte(tc.raw))
					return true
				}
				v := discoveryFixture(req.URL.Path)
				tc.change(&v)
				json.NewEncoder(w).Encode(v)
				return true
			})
			s, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			for _, a := range s.APIs {
				if a.Resource == "inferenceservices" && (a.Availability != tc.want || a.Reason != tc.reason) {
					t.Fatalf("got %+v want %s/%s", a, tc.want, tc.reason)
				}
			}
		})
	}
}

func TestManagerImageFactIsClosedAndSelectedByName(t *testing.T) {
	atContainerLimit := make([]corev1.Container, 32)
	atContainerLimit[31] = corev1.Container{Name: "manager", Image: "repo:v1.2.3"}
	for _, tc := range []struct {
		name       string
		containers []corev1.Container
		want       r.DoctorImageState
		version    string
	}{
		{"sidecar first", []corev1.Container{{Name: "sidecar", Image: "SECRET"}, {Name: "manager", Image: "registry:5000/repo:v1.2.3"}}, r.DoctorSelectedStableTag, "v1.2.3"},
		{"no manager", []corev1.Container{{Name: "other", Image: "repo:v1.2.3"}}, r.DoctorNoManagerContainer, ""},
		{"duplicate", []corev1.Container{{Name: "manager", Image: "repo:v1.2.3"}, {Name: "manager", Image: "repo:v1.2.4"}}, r.DoctorAmbiguousManager, ""},
		{"no tag", []corev1.Container{{Name: "manager", Image: "registry:5000/repo"}}, r.DoctorUnversionedImage, ""},
		{"digest", []corev1.Container{{Name: "manager", Image: "repo@sha256:SECRET"}}, r.DoctorDigestImage, ""},
		{"tag digest", []corev1.Container{{Name: "manager", Image: "repo:v1.2.3@sha256:SECRET"}}, r.DoctorDigestImage, ""},
		{"latest", []corev1.Container{{Name: "manager", Image: "repo:latest"}}, r.DoctorNonCanonicalTag, ""},
		{"date", []corev1.Container{{Name: "manager", Image: "repo:dev-20260915"}}, r.DoctorNonCanonicalTag, ""},
		{"git", []corev1.Container{{Name: "manager", Image: "repo:5069c980"}}, r.DoctorNonCanonicalTag, ""},
		{"prerelease", []corev1.Container{{Name: "manager", Image: "repo:v1.2.3-alpha"}}, r.DoctorNonCanonicalTag, ""},
		{"build", []corev1.Container{{Name: "manager", Image: "repo:v1.2.3+build"}}, r.DoctorNonCanonicalTag, ""},
		{"leading zero", []corev1.Container{{Name: "manager", Image: "repo:v01.2.3"}}, r.DoctorNonCanonicalTag, ""},
		{"empty", []corev1.Container{{Name: "manager"}}, r.DoctorMalformedImage, ""},
		{"oversized", []corev1.Container{{Name: "manager", Image: strings.Repeat("SECRET", 400)}}, r.DoctorImageUnavailable, ""},
		{"container cap", make([]corev1.Container, 33), r.DoctorImageUnavailable, ""},
		{"at container cap", atContainerLimit, r.DoctorSelectedStableTag, "v1.2.3"},
		{"at image cap", []corev1.Container{{Name: "manager", Image: strings.Repeat("a", 2041) + ":v1.2.3"}}, r.DoctorSelectedStableTag, "v1.2.3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clients, _, _ := fixtureClients(t, func(w http.ResponseWriter, req *http.Request) bool {
				if !strings.HasSuffix(req.URL.Path, "/deployments/ome-controller-manager") {
					return false
				}
				json.NewEncoder(w).Encode(appsv1.Deployment{TypeMeta: metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"}, ObjectMeta: metav1.ObjectMeta{Name: "ome-controller-manager", Namespace: "control"}, Spec: appsv1.DeploymentSpec{Template: corev1.PodTemplateSpec{Spec: corev1.PodSpec{Containers: tc.containers}}}})
				return true
			})
			s, err := Collect(context.Background(), clients, Selection{WorkloadNamespace: "team-a", OMENamespace: "control"}, time.Second)
			if err != nil {
				t.Fatal(err)
			}
			if s.Manager.ImageState != tc.want || s.Manager.ImageVersionCandidate != tc.version {
				t.Fatalf("fact=%+v want %s/%s", s.Manager, tc.want, tc.version)
			}
		})
	}
}
