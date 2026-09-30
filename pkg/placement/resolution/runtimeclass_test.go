package resolution

import (
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

func TestResolveRuntimeClass(t *testing.T) {
	resources := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("250m")}
	toleration := corev1.Toleration{Key: "sandbox", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute, TolerationSeconds: ptr.To[int64](30)}
	for _, tt := range []struct {
		name    string
		pod     corev1.PodSpec
		class   nodev1.RuntimeClass
		want    corev1.PodSpec
		wantErr bool
	}{
		{name: "no runtime class"},
		{name: "class without scheduling or overhead", pod: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox")}, want: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox")}},
		{name: "class supplies overhead", pod: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox")}, class: nodev1.RuntimeClass{Overhead: &nodev1.Overhead{PodFixed: resources}}, want: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), Overhead: resources}},
		{name: "matching overhead retained", pod: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), Overhead: resources}, class: nodev1.RuntimeClass{Overhead: &nodev1.Overhead{PodFixed: resources}}, want: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), Overhead: resources}},
		{name: "overhead mismatch", pod: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), Overhead: resources}, wantErr: true},
		{name: "class selector merges", pod: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), NodeSelector: map[string]string{"accelerator": "type-a"}}, class: nodev1.RuntimeClass{Scheduling: &nodev1.Scheduling{NodeSelector: map[string]string{"sandbox": "enabled"}}}, want: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), NodeSelector: map[string]string{"accelerator": "type-a", "sandbox": "enabled"}}},
		{name: "matching selector retained", pod: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), NodeSelector: map[string]string{"sandbox": "enabled"}}, class: nodev1.RuntimeClass{Scheduling: &nodev1.Scheduling{NodeSelector: map[string]string{"sandbox": "enabled"}}}, want: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), NodeSelector: map[string]string{"sandbox": "enabled"}}},
		{name: "selector conflict", pod: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), NodeSelector: map[string]string{"sandbox": "other"}}, class: nodev1.RuntimeClass{Scheduling: &nodev1.Scheduling{NodeSelector: map[string]string{"sandbox": "enabled"}}}, wantErr: true},
		{name: "class toleration added", pod: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox")}, class: nodev1.RuntimeClass{Scheduling: &nodev1.Scheduling{Tolerations: []corev1.Toleration{toleration}}}, want: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), Tolerations: []corev1.Toleration{toleration}}},
		{name: "duplicate toleration retained once", pod: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), Tolerations: []corev1.Toleration{toleration}}, class: nodev1.RuntimeClass{Scheduling: &nodev1.Scheduling{Tolerations: []corev1.Toleration{toleration}}}, want: corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), Tolerations: []corev1.Toleration{toleration}}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			f, _ := newUnitFixture()
			class := tt.class.DeepCopy()
			class.ObjectMeta = metav1.ObjectMeta{Name: "sandbox", UID: "class-uid"}
			class.Handler = "sandbox-handler"
			f.extra = append(f.extra, class)
			pod := tt.pod.DeepCopy()
			observed, err := resolveRuntimeClass(t.Context(), unitClient(t, f, interceptor.Funcs{}), pod)
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("runtime class error (-want +got):\n%s\n%v", diff, err)
			}
			if tt.wantErr {
				if observed != nil {
					t.Fatal("invalid runtime class returned")
				}
				return
			}
			if diff := cmp.Diff(&tt.want, pod, cmp.Comparer(func(a, b resource.Quantity) bool { return a.Cmp(b) == 0 })); diff != "" {
				t.Fatalf("runtime class defaults (-want +got):\n%s", diff)
			}
			before := unitJSON(t, pod)
			if observed != nil {
				if observed.Overhead != nil {
					observed.Overhead.PodFixed[corev1.ResourceCPU] = resource.MustParse("1")
				}
				if observed.Scheduling != nil && len(observed.Scheduling.Tolerations) > 0 {
					*observed.Scheduling.Tolerations[0].TolerationSeconds = 60
				}
			}
			if diff := cmp.Diff(before, unitJSON(t, pod)); diff != "" {
				t.Fatalf("aliased class inputs (-want +got):\n%s", diff)
			}
		})
	}
}
