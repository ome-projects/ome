package capacity

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
	corev1 "k8s.io/api/core/v1"
	nodev1 "k8s.io/api/node/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

func renderingFixture() ([]PodSet, *nodev1.RuntimeClass) {
	pod := &corev1.PodSpec{RuntimeClassName: ptr.To("sandbox"), Containers: []corev1.Container{{Name: "runner", Image: "example.com/serving:v1",
		Resources: corev1.ResourceRequirements{Limits: corev1.ResourceList{"example.com/gpu": resource.MustParse("2")}},
	}}}
	return []PodSet{{Name: "primary", Count: 1, Spec: pod}, {Name: "workers", Count: 2, Spec: pod.DeepCopy()}}, &nodev1.RuntimeClass{
		ObjectMeta: metav1.ObjectMeta{Name: "sandbox", UID: "runtime-a", ResourceVersion: "1"}, Handler: "sandbox-handler",
		Overhead:   &nodev1.Overhead{PodFixed: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("10m")}},
		Scheduling: &nodev1.Scheduling{NodeSelector: map[string]string{"sandbox": "enabled"}, Tolerations: []corev1.Toleration{{Key: "sandbox", Operator: corev1.TolerationOpExists, TolerationSeconds: ptr.To(int64(30))}}},
	}
}

func renderingClient(t *testing.T, class *nodev1.RuntimeClass, funcs interceptor.Funcs) client.Client {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := nodev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	return fake.NewClientBuilder().WithScheme(scheme).WithObjects(class).WithInterceptorFuncs(funcs).Build()
}

func TestComponentFingerprint(t *testing.T) {
	for _, tt := range []struct {
		name        string
		edit        func([]PodSet, *nodev1.RuntimeClass)
		mode        constants.DeploymentModeType
		component   v1beta1.ComponentType
		wantChanged bool
	}{
		{name: "same rendering"},
		{name: "pod set order", edit: func(p []PodSet, _ *nodev1.RuntimeClass) { slices.Reverse(p) }},
		{name: "class metadata and version", edit: func(_ []PodSet, c *nodev1.RuntimeClass) {
			c.ResourceVersion = "2"
			c.Labels = map[string]string{"note": "value"}
			c.APIVersion = "node.k8s.io/v1"
			c.Kind = "RuntimeClass"
		}},
		{name: "class defaults already applied", edit: func(p []PodSet, c *nodev1.RuntimeClass) {
			for i := range p {
				p[i].Spec.Overhead = c.Overhead.PodFixed.DeepCopy()
				p[i].Spec.NodeSelector = map[string]string{"sandbox": "enabled"}
				p[i].Spec.Tolerations = []corev1.Toleration{*c.Scheduling.Tolerations[0].DeepCopy()}
			}
		}},
		{name: "resource demand", wantChanged: true, edit: func(p []PodSet, _ *nodev1.RuntimeClass) {
			p[0].Spec.Containers[0].Resources.Limits["example.com/gpu"] = resource.MustParse("4")
		}},
		{name: "worker count", wantChanged: true, edit: func(p []PodSet, _ *nodev1.RuntimeClass) { p[1].Count++ }},
		{name: "worker template", wantChanged: true, edit: func(p []PodSet, _ *nodev1.RuntimeClass) { p[1].Spec.Containers[0].Image = "example.com/serving:v2" }},
		{name: "pod set identity", wantChanged: true, edit: func(p []PodSet, _ *nodev1.RuntimeClass) { p[0].Name = "different" }},
		{name: "scheduling selector", wantChanged: true, edit: func(p []PodSet, _ *nodev1.RuntimeClass) { p[0].Spec.NodeSelector = map[string]string{"hardware": "a"} }},
		{name: "admission overhead", wantChanged: true, edit: func(_ []PodSet, c *nodev1.RuntimeClass) {
			c.Overhead.PodFixed[corev1.ResourceCPU] = resource.MustParse("20m")
		}},
		{name: "class identity", wantChanged: true, edit: func(_ []PodSet, c *nodev1.RuntimeClass) { c.UID = "runtime-b" }},
		{name: "class handler", wantChanged: true, edit: func(_ []PodSet, c *nodev1.RuntimeClass) { c.Handler = "different-handler" }},
		{name: "class scheduling", wantChanged: true, edit: func(_ []PodSet, c *nodev1.RuntimeClass) { c.Scheduling.NodeSelector["hardware"] = "a" }},
		{name: "class toleration", wantChanged: true, edit: func(_ []PodSet, c *nodev1.RuntimeClass) { *c.Scheduling.Tolerations[0].TolerationSeconds = 60 }},
		{name: "deployment mode", mode: constants.MultiNode, wantChanged: true},
		{name: "component identity", component: v1beta1.DecoderComponent, wantChanged: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pods, class := renderingFixture()
			baseline, err := ComponentFingerprint(t.Context(), renderingClient(t, class, interceptor.Funcs{}), v1beta1.EngineComponent, constants.OMENative, pods)
			if err != nil {
				t.Fatal(err)
			}
			if tt.edit != nil {
				tt.edit(pods, class)
			}
			before := demandJSON(t, []any{pods, class})
			mode, component := tt.mode, tt.component
			if mode == "" {
				mode = constants.OMENative
			}
			if component == "" {
				component = v1beta1.EngineComponent
			}
			got, err := ComponentFingerprint(t.Context(), renderingClient(t, class, interceptor.Funcs{}), component, mode, pods)
			if err != nil {
				t.Fatal(err)
			}
			if diff := cmp.Diff(tt.wantChanged, baseline != got); diff != "" {
				t.Fatalf("fingerprint changed (-want +got):\n%s", diff)
			}
			if diff := cmp.Diff(before, demandJSON(t, []any{pods, class})); diff != "" {
				t.Fatalf("mutated inputs:\n%s", diff)
			}
		})
	}
}

func TestComponentFingerprintRejectsIncompleteRendering(t *testing.T) {
	for _, tt := range []struct {
		name      string
		edit      func(*[]PodSet)
		mode      constants.DeploymentModeType
		component v1beta1.ComponentType
		wantErr   string
	}{
		{name: "no sets", edit: func(p *[]PodSet) { *p = nil }, wantErr: "pod set"},
		{name: "router", component: v1beta1.RouterComponent, wantErr: "engine or decoder"},
		{name: "unsupported mode", mode: constants.VirtualDeployment, wantErr: "no supported replica shape"},
		{name: "no name", edit: func(p *[]PodSet) { (*p)[0].Name = "" }, wantErr: "unique named"},
		{name: "duplicate names", edit: func(p *[]PodSet) { (*p)[1].Name = (*p)[0].Name }, wantErr: "unique named"},
		{name: "zero count", edit: func(p *[]PodSet) { (*p)[0].Count = 0 }, wantErr: "positive count"},
		{name: "negative count", edit: func(p *[]PodSet) { (*p)[1].Count = -1 }, wantErr: "positive count"},
		{name: "nil spec", edit: func(p *[]PodSet) { (*p)[0].Spec = nil }, wantErr: "complete spec"},
		{name: "no containers", edit: func(p *[]PodSet) { (*p)[0].Spec.Containers = nil }, wantErr: "complete spec"},
		{name: "overhead without class", edit: func(p *[]PodSet) {
			(*p)[0].Spec.RuntimeClassName = nil
			(*p)[0].Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
		}, wantErr: "no identified"},
		{name: "empty class name", edit: func(p *[]PodSet) { (*p)[0].Spec.RuntimeClassName = ptr.To("") }, wantErr: "name is empty"},
		{name: "unavailable class", edit: func(p *[]PodSet) { (*p)[0].Spec.RuntimeClassName = ptr.To("missing") }, wantErr: "not found"},
		{name: "conflicting overhead", edit: func(p *[]PodSet) {
			(*p)[0].Spec.Overhead = corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}
		}, wantErr: "differs"},
		{name: "conflicting selector", edit: func(p *[]PodSet) { (*p)[0].Spec.NodeSelector = map[string]string{"sandbox": "other"} }, wantErr: "conflicts"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pods, class := renderingFixture()
			if tt.edit != nil {
				tt.edit(&pods)
			}
			mode, component := tt.mode, tt.component
			if mode == "" {
				mode = constants.OMENative
			}
			if component == "" {
				component = v1beta1.EngineComponent
			}
			before := demandJSON(t, pods)
			got, err := ComponentFingerprint(t.Context(), renderingClient(t, class, interceptor.Funcs{}), component, mode, pods)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
			if diff := cmp.Diff("", got); diff != "" {
				t.Fatalf("partial fingerprint:\n%s", diff)
			}
			if diff := cmp.Diff(before, demandJSON(t, pods)); diff != "" {
				t.Fatalf("mutated inputs:\n%s", diff)
			}
		})
	}
}

func TestComponentFingerprintRuntimeClassFence(t *testing.T) {
	for _, tt := range []struct {
		name         string
		read         int
		edit         func(*nodev1.RuntimeClass)
		fail, cancel bool
		wantErr      string
	}{
		{name: "initial read failed", read: 1, fail: true, wantErr: "read failed"},
		{name: "final read failed", read: 3, fail: true, wantErr: "read failed"},
		{name: "missing identity", read: 1, edit: func(c *nodev1.RuntimeClass) { c.UID = "" }, wantErr: "live identity"},
		{name: "missing version", read: 1, edit: func(c *nodev1.RuntimeClass) { c.ResourceVersion = "" }, wantErr: "live identity"},
		{name: "terminating class", read: 1, edit: func(c *nodev1.RuntimeClass) { c.DeletionTimestamp = &metav1.Time{Time: metav1.Now().Time} }, wantErr: "live identity"},
		{name: "class changes between pods", read: 2, edit: func(c *nodev1.RuntimeClass) { c.ResourceVersion = "2" }, wantErr: "changed"},
		{name: "class replaced between pods", read: 2, edit: func(c *nodev1.RuntimeClass) { c.UID = "runtime-b" }, wantErr: "changed"},
		{name: "class changes before return", read: 3, edit: func(c *nodev1.RuntimeClass) { c.ResourceVersion = "2" }, wantErr: "changed"},
		{name: "class replaced before return", read: 3, edit: func(c *nodev1.RuntimeClass) { c.UID = "runtime-b" }, wantErr: "changed"},
		{name: "class terminates before return", read: 3, edit: func(c *nodev1.RuntimeClass) { now := metav1.Now(); c.DeletionTimestamp = &now }, wantErr: "changed"},
		{name: "canceled during reads", read: 3, cancel: true, wantErr: "canceled"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			pods, class := renderingFixture()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			reads := 0
			cl := renderingClient(t, class, interceptor.Funcs{Get: func(ctx context.Context, cl client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				reads++
				if reads == tt.read && tt.fail {
					return errors.New("class read failed")
				}
				if err := cl.Get(ctx, key, obj, opts...); err != nil {
					return err
				}
				if reads == tt.read {
					if tt.edit != nil {
						tt.edit(obj.(*nodev1.RuntimeClass))
					}
					if tt.cancel {
						cancel()
					}
				}
				return nil
			}})
			got, err := ComponentFingerprint(ctx, cl, v1beta1.EngineComponent, constants.OMENative, pods)
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("error = %v, want %q", err, tt.wantErr)
			}
			if diff := cmp.Diff("", got); diff != "" {
				t.Fatalf("unverified fingerprint:\n%s", diff)
			}
		})
	}
}

func TestRenderingRequiresOnlyReferencedClass(t *testing.T) {
	for _, tt := range []struct {
		name              string
		pod               *corev1.PodSpec
		canceled, wantErr bool
	}{
		{name: "nil pod", wantErr: true},
		{name: "class needs reader", pod: &corev1.PodSpec{RuntimeClassName: ptr.To("sandbox")}, wantErr: true},
		{name: "no class needs no reader", pod: &corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "example.com/serving:v1"}}}},
		{name: "canceled context", canceled: true, pod: &corev1.PodSpec{Containers: []corev1.Container{{Name: "runner", Image: "example.com/serving:v1"}}}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.pod == nil {
				if _, err := ApplyRuntimeClass(t.Context(), nil, nil); err == nil {
					t.Fatal("nil pod accepted")
				}
				return
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if tt.canceled {
				cancel()
			}
			if len(tt.pod.Containers) == 0 {
				tt.pod.Containers = []corev1.Container{{Name: "runner", Image: "example.com/serving:v1"}}
			}
			_, err := ComponentFingerprint(ctx, nil, v1beta1.EngineComponent, constants.RawDeployment, []PodSet{{Name: "primary", Count: 1, Spec: tt.pod}})
			if diff := cmp.Diff(tt.wantErr, err != nil); diff != "" {
				t.Fatalf("error:\n%s\n%v", diff, err)
			}
		})
	}
}
