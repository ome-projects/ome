package service

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// A Service port carries the appProtocol declared for its own name. An
// undeclared name, an empty value and no declarations at all leave it unset.
func TestAppProtocolForPort(t *testing.T) {
	declared := map[string]string{"http": "kubernetes.io/h2c", "grpc": ""}
	for _, tc := range []struct {
		name         string
		appProtocols map[string]string
		port         string
		want         *string
	}{
		{name: "declared port name", appProtocols: declared, port: "http", want: ptr.To("kubernetes.io/h2c")},
		{name: "undeclared port name", appProtocols: declared, port: "metrics"},
		{name: "empty declared value", appProtocols: declared, port: "grpc"},
		{name: "no declarations", port: "http"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := AppProtocolForPort(tc.appProtocols, tc.port); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("got %q want %q", ptr.Deref(got, ""), ptr.Deref(tc.want, ""))
			}
		})
	}
}

// A container without ports gets the default port, named after the
// container, so only a value declared for the container's name lands on it.
func TestBuildServiceDefaultPortAppProtocol(t *testing.T) {
	podSpec := &corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}}
	for _, tc := range []struct {
		name         string
		appProtocols map[string]string
		want         *string
	}{
		{name: "declared for the container name", appProtocols: map[string]string{"main": "kubernetes.io/h2c"}, want: ptr.To("kubernetes.io/h2c")},
		{name: "declared for another name", appProtocols: map[string]string{"http": "kubernetes.io/h2c"}},
		{name: "no declarations"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ext := &v1beta1.ComponentExtensionSpec{ServicePortAppProtocols: tc.appProtocols}
			svc := buildService(metav1.ObjectMeta{Name: "test-service"}, ext, podSpec, nil)
			if len(svc.Spec.Ports) != 1 || svc.Spec.Ports[0].Name != "main" {
				t.Fatalf("ports: got %+v want one default port named main", svc.Spec.Ports)
			}
			if got := svc.Spec.Ports[0].AppProtocol; !reflect.DeepEqual(got, tc.want) {
				t.Errorf("default port appProtocol: got %q want %q", ptr.Deref(got, ""), ptr.Deref(tc.want, ""))
			}
		})
	}
}
