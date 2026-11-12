package autoscalerpolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"sigs.k8s.io/ome/pkg/webhook/admission/policyguard"

	admissionv1 "k8s.io/api/admission/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/autoscalerpolicy/render"
	"sigs.k8s.io/ome/pkg/constants"
)

const testNamespace = "llm-serving"

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := v1beta1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	if err := corev1.AddToScheme(s); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return s
}

func newValidator(t *testing.T, live ...client.Object) *Validator {
	t.Helper()
	s := testScheme(t)
	return &Validator{
		Client:  fake.NewClientBuilder().WithScheme(s).WithObjects(live...).Build(),
		Decoder: admission.NewDecoder(s),
	}
}

func activeNamespace() *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	}
}

func validPolicy(name string) *v1beta1.AutoscalerPolicy {
	return &v1beta1.AutoscalerPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: v1beta1.AutoscalerPolicySpec{
			Enforcement: v1beta1.PolicyEnforcementDefault,
			Class:       v1beta1.AutoscalerKEDA,
			Keda: &v1beta1.KedaPolicyTemplate{
				Triggers: []v1beta1.KedaTriggerTemplate{{
					Type:                        "prometheus",
					ProviderRef:                 &v1beta1.MetricProviderRef{Name: "cluster-prometheus"},
					MetricType:                  autoscalingv2.AverageValueMetricType,
					QueryReturnsDesiredReplicas: true,
					Metadata: map[string]string{
						"threshold":        "1",
						"ignoreNullValues": "false",
						"query":            `((sum(request_activity{namespace="{{ .Namespace }}",inferenceservice="{{ .ISVCName }}"}) > bool 0) * {{ .MaxReplicas }})`,
					},
				}},
			},
		},
	}
}

// consumer builds an InferenceService whose named components each carry a
// policy ref. Components map keys are "engine", "decoder", "router".
func consumer(name string, refs map[string]*v1beta1.AutoscalerPolicyRef) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
	}
	if ref, ok := refs["engine"]; ok {
		isvc.Spec.Engine.AutoscalerPolicyRef = ref
	}
	if ref, ok := refs["decoder"]; ok {
		isvc.Spec.Decoder = &v1beta1.DecoderSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{AutoscalerPolicyRef: ref},
		}
	}
	if ref, ok := refs["router"]; ok {
		isvc.Spec.Router = &v1beta1.RouterSpec{
			ComponentExtensionSpec: v1beta1.ComponentExtensionSpec{AutoscalerPolicyRef: ref},
		}
	}
	return isvc
}

func writeRequest(t *testing.T, op admissionv1.Operation, policy *v1beta1.AutoscalerPolicy) admission.Request {
	t.Helper()
	raw, err := json.Marshal(policy)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: op,
		Name:      policy.Name,
		Namespace: policy.Namespace,
	}}
	req.Object.Raw = raw
	return req
}

// deleteRequest builds a DELETE admission request. withOldObject controls
// whether the payload carries the object (the API server usually sends it)
// or arrives empty (the fetch-by-name fallback path).
func deleteRequest(t *testing.T, policy *v1beta1.AutoscalerPolicy, withOldObject bool) admission.Request {
	t.Helper()
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Delete,
		Name:      policy.Name,
		Namespace: policy.Namespace,
	}}
	if withOldObject {
		raw, err := json.Marshal(policy)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		req.OldObject.Raw = raw
	}
	return req
}

func denialMessage(t *testing.T, resp admission.Response) string {
	t.Helper()
	if resp.Allowed {
		t.Fatal("expected denial, got allowed")
	}
	if resp.Result == nil {
		t.Fatal("denied response has no result")
	}
	return resp.Result.Message
}

func TestHandleCreateUpdate(t *testing.T) {
	t.Run("valid policy admitted on create", func(t *testing.T) {
		v := newValidator(t)
		resp := v.Handle(context.Background(), writeRequest(t, admissionv1.Create, validPolicy("request-activity-v1")))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("invalid policy denied with reason", func(t *testing.T) {
		policy := validPolicy("request-activity-v1")
		policy.Spec.Keda = nil
		v := newValidator(t)
		msg := denialMessage(t, v.Handle(context.Background(), writeRequest(t, admissionv1.Create, policy)))
		if !strings.Contains(msg, render.ReasonClassTemplate) {
			t.Errorf("denial %q does not carry reason %s", msg, render.ReasonClassTemplate)
		}
	})

	t.Run("all issues joined in one denial", func(t *testing.T) {
		policy := validPolicy("request-activity-v1")
		policy.Spec.Enforcement = v1beta1.PolicyEnforcementRequired
		policy.Spec.Keda = nil
		v := newValidator(t)
		msg := denialMessage(t, v.Handle(context.Background(), writeRequest(t, admissionv1.Create, policy)))
		for _, want := range []string{render.ReasonEnforcementReserved, render.ReasonClassTemplate} {
			if !strings.Contains(msg, want) {
				t.Errorf("denial %q does not carry reason %s", msg, want)
			}
		}
	})

	t.Run("invalid policy denied on update", func(t *testing.T) {
		policy := validPolicy("request-activity-v1")
		policy.Spec.Enforcement = v1beta1.PolicyEnforcementRequired
		v := newValidator(t)
		msg := denialMessage(t, v.Handle(context.Background(), writeRequest(t, admissionv1.Update, policy)))
		if !strings.Contains(msg, render.ReasonEnforcementReserved) {
			t.Errorf("denial %q does not carry reason %s", msg, render.ReasonEnforcementReserved)
		}
	})
}

func TestHandleDelete(t *testing.T) {
	policyName := "request-activity-v1"
	ref := &v1beta1.AutoscalerPolicyRef{Name: policyName}

	t.Run("zero refs admits", func(t *testing.T) {
		v := newValidator(t, activeNamespace(), validPolicy(policyName),
			consumer("llm-a", nil),
			consumer("llm-b", map[string]*v1beta1.AutoscalerPolicyRef{"engine": {Name: "some-other-policy"}}),
		)
		resp := v.Handle(context.Background(), deleteRequest(t, validPolicy(policyName), true))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("referenced policy denies and names components", func(t *testing.T) {
		v := newValidator(t, activeNamespace(), validPolicy(policyName),
			consumer("llm-a", map[string]*v1beta1.AutoscalerPolicyRef{"engine": ref, "decoder": ref}),
		)
		msg := denialMessage(t, v.Handle(context.Background(), deleteRequest(t, validPolicy(policyName), true)))
		for _, want := range []string{"llm-a/engine", "llm-a/decoder", constants.AutoscalerPolicyAllowInUseDelete} {
			if !strings.Contains(msg, want) {
				t.Errorf("denial %q does not mention %s", msg, want)
			}
		}
	})

	t.Run("denial names at most the cap and counts the rest", func(t *testing.T) {
		live := []client.Object{activeNamespace(), validPolicy(policyName)}
		refs := map[string]*v1beta1.AutoscalerPolicyRef{"engine": ref, "decoder": ref, "router": ref}
		for i := 0; i < 4; i++ {
			live = append(live, consumer(fmt.Sprintf("llm-%d", i), refs))
		}
		v := newValidator(t, live...)
		msg := denialMessage(t, v.Handle(context.Background(), deleteRequest(t, validPolicy(policyName), true)))
		if !strings.Contains(msg, "12 component(s)") {
			t.Errorf("denial %q does not carry the total count", msg)
		}
		if !strings.Contains(msg, "and 2 more") {
			t.Errorf("denial %q does not summarize the overflow", msg)
		}
		if named := strings.Count(msg, "llm-"); named != policyguard.MaxNamedReferences {
			t.Errorf("denial names %d components, want %d:\n%s", named, policyguard.MaxNamedReferences, msg)
		}
	})

	t.Run("break-glass annotation admits while referenced", func(t *testing.T) {
		policy := validPolicy(policyName)
		policy.Annotations = map[string]string{constants.AutoscalerPolicyAllowInUseDelete: "true"}
		v := newValidator(t, activeNamespace(), policy,
			consumer("llm-a", map[string]*v1beta1.AutoscalerPolicyRef{"engine": ref}),
		)
		resp := v.Handle(context.Background(), deleteRequest(t, policy, true))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("break-glass annotation false still denies", func(t *testing.T) {
		policy := validPolicy(policyName)
		policy.Annotations = map[string]string{constants.AutoscalerPolicyAllowInUseDelete: "false"}
		v := newValidator(t, activeNamespace(), policy,
			consumer("llm-a", map[string]*v1beta1.AutoscalerPolicyRef{"engine": ref}),
		)
		resp := v.Handle(context.Background(), deleteRequest(t, policy, true))
		if resp.Allowed {
			t.Fatal("expected denial")
		}
	})

	t.Run("terminating namespace admits while referenced", func(t *testing.T) {
		terminating := activeNamespace()
		terminating.Status.Phase = corev1.NamespaceTerminating
		v := newValidator(t, terminating, validPolicy(policyName),
			consumer("llm-a", map[string]*v1beta1.AutoscalerPolicyRef{"engine": ref}),
		)
		resp := v.Handle(context.Background(), deleteRequest(t, validPolicy(policyName), true))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("missing namespace admits while referenced", func(t *testing.T) {
		v := newValidator(t, validPolicy(policyName),
			consumer("llm-a", map[string]*v1beta1.AutoscalerPolicyRef{"engine": ref}),
		)
		resp := v.Handle(context.Background(), deleteRequest(t, validPolicy(policyName), true))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("empty payload falls back to a live fetch", func(t *testing.T) {
		v := newValidator(t, activeNamespace(), validPolicy(policyName),
			consumer("llm-a", map[string]*v1beta1.AutoscalerPolicyRef{"engine": ref}),
		)
		msg := denialMessage(t, v.Handle(context.Background(), deleteRequest(t, validPolicy(policyName), false)))
		if !strings.Contains(msg, "llm-a/engine") {
			t.Errorf("denial %q does not name the referencing component", msg)
		}
	})

	t.Run("empty payload with the policy already gone admits", func(t *testing.T) {
		v := newValidator(t, activeNamespace(),
			consumer("llm-a", map[string]*v1beta1.AutoscalerPolicyRef{"engine": ref}),
		)
		resp := v.Handle(context.Background(), deleteRequest(t, validPolicy(policyName), false))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("reserved cluster kind ref does not count as a reference", func(t *testing.T) {
		clusterKindRef := &v1beta1.AutoscalerPolicyRef{Name: policyName, Kind: "ClusterAutoscalerPolicy"}
		v := newValidator(t, activeNamespace(), validPolicy(policyName),
			consumer("llm-a", map[string]*v1beta1.AutoscalerPolicyRef{"engine": clusterKindRef}),
		)
		resp := v.Handle(context.Background(), deleteRequest(t, validPolicy(policyName), true))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})
}

func TestHandleOtherOperations(t *testing.T) {
	v := newValidator(t)
	resp := v.Handle(context.Background(), admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Connect,
	}})
	if !resp.Allowed {
		t.Fatalf("expected allowed, got: %v", resp.Result)
	}
}
