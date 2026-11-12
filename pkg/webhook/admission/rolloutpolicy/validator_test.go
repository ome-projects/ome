package rolloutpolicy

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
	"sigs.k8s.io/ome/pkg/validation"
	"sigs.k8s.io/ome/pkg/webhook/admission/policyguard"
)

const testNamespace = "llm-serving"

func newValidator(t *testing.T, live ...client.Object) *Validator {
	t.Helper()
	c := fakeclient.New(t, fakeclient.WithObjects(live...))
	return &Validator{
		Client:  c,
		Decoder: admission.NewDecoder(c.Scheme()),
	}
}

func activeNamespace() *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	}
}

func validCanaryPolicy(name string) *v1beta1.RolloutPolicy {
	return &v1beta1.RolloutPolicy{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec: v1beta1.RolloutPolicySpec{
			Canary: &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
				{Capacity: intstr.FromString("10%"), Traffic: 10},
				{Capacity: intstr.FromString("100%"), Traffic: 100},
			}},
		},
	}
}

// consumer builds an InferenceService whose rollout groups carry the given
// policyRefs, one single-Component group per ref.
func consumer(name string, refs ...*v1beta1.RolloutPolicyRef) *v1beta1.InferenceService {
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: testNamespace},
		Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
	}
	if len(refs) == 0 {
		return isvc
	}
	groups := make([]v1beta1.RolloutGroup, 0, len(refs))
	for _, ref := range refs {
		groups = append(groups, v1beta1.RolloutGroup{
			Components: []v1beta1.ComponentType{v1beta1.EngineComponent},
			PolicyRef:  ref,
		})
	}
	isvc.Spec.Rollout = &v1beta1.RolloutSpec{Groups: groups}
	return isvc
}

func canaryRef(policyName string) *v1beta1.RolloutPolicyRef {
	return &v1beta1.RolloutPolicyRef{Name: policyName, Progression: v1beta1.RolloutProgressionCanary}
}

func writeRequest(t *testing.T, op admissionv1.Operation, policy *v1beta1.RolloutPolicy) admission.Request {
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

// updateRequest builds an UPDATE carrying both the new object and the prior
// one in OldObject, as the API server sends it.
func updateRequest(t *testing.T, old, updated *v1beta1.RolloutPolicy) admission.Request {
	t.Helper()
	req := writeRequest(t, admissionv1.Update, updated)
	raw, err := json.Marshal(old)
	if err != nil {
		t.Fatalf("marshal old: %v", err)
	}
	req.OldObject.Raw = raw
	return req
}

// deleteRequest builds a DELETE admission request. withOldObject controls
// whether the payload carries the object (the API server usually sends it)
// or arrives empty (the fetch-by-name fallback path).
func deleteRequest(t *testing.T, policy *v1beta1.RolloutPolicy, withOldObject bool) admission.Request {
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

func TestHandleCreate(t *testing.T) {
	t.Run("valid policy admitted", func(t *testing.T) {
		v := newValidator(t)
		resp := v.Handle(context.Background(), writeRequest(t, admissionv1.Create, validCanaryPolicy("canary-std-v1")))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("invalid body denied with reason", func(t *testing.T) {
		policy := validCanaryPolicy("canary-std-v1")
		policy.Spec.Canary.Steps = nil
		v := newValidator(t)
		msg := denialMessage(t, v.Handle(context.Background(), writeRequest(t, admissionv1.Create, policy)))
		if !strings.Contains(msg, validation.ReasonCanaryInvalid) {
			t.Errorf("denial %q does not carry reason %s", msg, validation.ReasonCanaryInvalid)
		}
	})

	t.Run("absolute step capacity denied as service-specific", func(t *testing.T) {
		policy := validCanaryPolicy("canary-std-v1")
		policy.Spec.Canary.Steps[0].Capacity = intstr.FromInt32(1)
		v := newValidator(t)
		msg := denialMessage(t, v.Handle(context.Background(), writeRequest(t, admissionv1.Create, policy)))
		if !strings.Contains(msg, validation.ReasonRolloutPolicyInvalid) || !strings.Contains(msg, "spec.canary.steps[0].capacity") {
			t.Errorf("denial %q does not name spec.canary.steps[0].capacity with reason %s", msg, validation.ReasonRolloutPolicyInvalid)
		}
	})

	t.Run("portability violation denied", func(t *testing.T) {
		policy := validCanaryPolicy("canary-std-v1")
		policy.Spec.Canary.Prometheus = &v1beta1.AnalysisPrometheus{ServerAddress: "http://prometheus.example.com:9090"}
		v := newValidator(t)
		msg := denialMessage(t, v.Handle(context.Background(), writeRequest(t, admissionv1.Create, policy)))
		if !strings.Contains(msg, validation.ReasonRolloutPolicyInvalid) {
			t.Errorf("denial %q does not carry reason %s", msg, validation.ReasonRolloutPolicyInvalid)
		}
	})

	t.Run("body over the configured cap denied as PlanTooLarge", func(t *testing.T) {
		v := newValidator(t)
		v.MaxPlanBytes = 16
		msg := denialMessage(t, v.Handle(context.Background(), writeRequest(t, admissionv1.Create, validCanaryPolicy("canary-std-v1"))))
		if !strings.Contains(msg, validation.ReasonRolloutPlanTooLarge) {
			t.Errorf("denial %q does not carry reason %s", msg, validation.ReasonRolloutPlanTooLarge)
		}
	})

	t.Run("zero cap means uncapped", func(t *testing.T) {
		v := newValidator(t)
		resp := v.Handle(context.Background(), writeRequest(t, admissionv1.Create, validCanaryPolicy("canary-std-v1")))
		if !resp.Allowed {
			t.Fatalf("expected allowed with no cap configured, got: %v", resp.Result)
		}
	})
}

func TestHandleUpdate(t *testing.T) {
	policyName := "canary-std-v1"

	blueGreenVariant := func() *v1beta1.RolloutPolicy {
		policy := validCanaryPolicy(policyName)
		policy.Spec = v1beta1.RolloutPolicySpec{BlueGreen: &v1beta1.GroupBlueGreen{}}
		return policy
	}
	editedBody := func() *v1beta1.RolloutPolicy {
		policy := validCanaryPolicy(policyName)
		policy.Spec.Canary.Steps = []v1beta1.RolloutGroupStep{
			{Capacity: intstr.FromString("5%"), Traffic: 5},
			{Capacity: intstr.FromString("100%"), Traffic: 100},
		}
		return policy
	}

	t.Run("kind change while referenced denied", func(t *testing.T) {
		v := newValidator(t, consumer("llm-a", canaryRef(policyName)))
		msg := denialMessage(t, v.Handle(context.Background(), updateRequest(t, validCanaryPolicy(policyName), blueGreenVariant())))
		for _, want := range []string{"canary", "blueGreen", "1 InferenceService(s)", "new versioned policy name"} {
			if !strings.Contains(msg, want) {
				t.Errorf("denial %q does not mention %s", msg, want)
			}
		}
	})

	t.Run("kind change with zero refs admitted", func(t *testing.T) {
		v := newValidator(t, consumer("llm-a", canaryRef("some-other-policy")))
		resp := v.Handle(context.Background(), updateRequest(t, validCanaryPolicy(policyName), blueGreenVariant()))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("body edit while referenced warns with the consumer count", func(t *testing.T) {
		v := newValidator(t,
			consumer("llm-a", canaryRef(policyName)),
			// Two groups referencing the same policy count as ONE consumer.
			consumer("llm-b", canaryRef(policyName), canaryRef(policyName)),
		)
		resp := v.Handle(context.Background(), updateRequest(t, validCanaryPolicy(policyName), editedBody()))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
		if len(resp.Warnings) != 1 || !strings.Contains(resp.Warnings[0], "2 InferenceService(s)") {
			t.Errorf("expected one warning naming 2 consumers, got: %v", resp.Warnings)
		}
	})

	t.Run("body edit with zero refs admits silently", func(t *testing.T) {
		v := newValidator(t)
		resp := v.Handle(context.Background(), updateRequest(t, validCanaryPolicy(policyName), editedBody()))
		if !resp.Allowed || len(resp.Warnings) != 0 {
			t.Fatalf("expected silent allow, got: %v / warnings %v", resp.Result, resp.Warnings)
		}
	})

	t.Run("spec-unchanged update admits silently while referenced", func(t *testing.T) {
		v := newValidator(t, consumer("llm-a", canaryRef(policyName)))
		relabeled := validCanaryPolicy(policyName)
		relabeled.Labels = map[string]string{"team": "serving"}
		resp := v.Handle(context.Background(), updateRequest(t, validCanaryPolicy(policyName), relabeled))
		if !resp.Allowed || len(resp.Warnings) != 0 {
			t.Fatalf("expected silent allow, got: %v / warnings %v", resp.Result, resp.Warnings)
		}
	})

	t.Run("invalid body still denied on update", func(t *testing.T) {
		broken := validCanaryPolicy(policyName)
		broken.Spec.Canary.Steps = broken.Spec.Canary.Steps[:1] // final traffic != 100
		v := newValidator(t)
		msg := denialMessage(t, v.Handle(context.Background(), updateRequest(t, validCanaryPolicy(policyName), broken)))
		if !strings.Contains(msg, validation.ReasonCanaryInvalid) {
			t.Errorf("denial %q does not carry reason %s", msg, validation.ReasonCanaryInvalid)
		}
	})
}

func TestHandleDelete(t *testing.T) {
	policyName := "canary-std-v1"
	ref := canaryRef(policyName)

	t.Run("zero refs admits", func(t *testing.T) {
		v := newValidator(t, activeNamespace(), validCanaryPolicy(policyName),
			consumer("llm-a"),
			consumer("llm-b", canaryRef("some-other-policy")),
		)
		resp := v.Handle(context.Background(), deleteRequest(t, validCanaryPolicy(policyName), true))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("referenced policy denies and names consumers once each", func(t *testing.T) {
		v := newValidator(t, activeNamespace(), validCanaryPolicy(policyName),
			consumer("llm-a", ref, ref),
		)
		msg := denialMessage(t, v.Handle(context.Background(), deleteRequest(t, validCanaryPolicy(policyName), true)))
		for _, want := range []string{"1 InferenceService(s)", "llm-a", constants.AutoscalerPolicyAllowInUseDelete} {
			if !strings.Contains(msg, want) {
				t.Errorf("denial %q does not mention %s", msg, want)
			}
		}
	})

	t.Run("denial names at most the cap and counts the rest", func(t *testing.T) {
		live := []client.Object{activeNamespace(), validCanaryPolicy(policyName)}
		for i := 0; i < policyguard.MaxNamedReferences+2; i++ {
			live = append(live, consumer(fmt.Sprintf("llm-%02d", i), ref))
		}
		v := newValidator(t, live...)
		msg := denialMessage(t, v.Handle(context.Background(), deleteRequest(t, validCanaryPolicy(policyName), true)))
		if !strings.Contains(msg, fmt.Sprintf("%d InferenceService(s)", policyguard.MaxNamedReferences+2)) {
			t.Errorf("denial %q does not carry the total count", msg)
		}
		if !strings.Contains(msg, "and 2 more") {
			t.Errorf("denial %q does not summarize the overflow", msg)
		}
		if named := strings.Count(msg, "llm-"); named != policyguard.MaxNamedReferences {
			t.Errorf("denial names %d consumers, want %d:\n%s", named, policyguard.MaxNamedReferences, msg)
		}
	})

	t.Run("break-glass annotation admits while referenced", func(t *testing.T) {
		policy := validCanaryPolicy(policyName)
		policy.Annotations = map[string]string{constants.AutoscalerPolicyAllowInUseDelete: "true"}
		v := newValidator(t, activeNamespace(), policy, consumer("llm-a", ref))
		resp := v.Handle(context.Background(), deleteRequest(t, policy, true))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("break-glass annotation false still denies", func(t *testing.T) {
		policy := validCanaryPolicy(policyName)
		policy.Annotations = map[string]string{constants.AutoscalerPolicyAllowInUseDelete: "false"}
		v := newValidator(t, activeNamespace(), policy, consumer("llm-a", ref))
		resp := v.Handle(context.Background(), deleteRequest(t, policy, true))
		if resp.Allowed {
			t.Fatal("expected denial")
		}
	})

	t.Run("terminating namespace admits while referenced", func(t *testing.T) {
		terminating := activeNamespace()
		terminating.Status.Phase = corev1.NamespaceTerminating
		v := newValidator(t, terminating, validCanaryPolicy(policyName), consumer("llm-a", ref))
		resp := v.Handle(context.Background(), deleteRequest(t, validCanaryPolicy(policyName), true))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("missing namespace admits while referenced", func(t *testing.T) {
		v := newValidator(t, validCanaryPolicy(policyName), consumer("llm-a", ref))
		resp := v.Handle(context.Background(), deleteRequest(t, validCanaryPolicy(policyName), true))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("empty payload falls back to a live fetch", func(t *testing.T) {
		v := newValidator(t, activeNamespace(), validCanaryPolicy(policyName), consumer("llm-a", ref))
		msg := denialMessage(t, v.Handle(context.Background(), deleteRequest(t, validCanaryPolicy(policyName), false)))
		if !strings.Contains(msg, "llm-a") {
			t.Errorf("denial %q does not name the referencing InferenceService", msg)
		}
	})

	t.Run("empty payload with the policy already gone admits", func(t *testing.T) {
		v := newValidator(t, activeNamespace(), consumer("llm-a", ref))
		resp := v.Handle(context.Background(), deleteRequest(t, validCanaryPolicy(policyName), false))
		if !resp.Allowed {
			t.Fatalf("expected allowed, got: %v", resp.Result)
		}
	})

	t.Run("reserved cluster kind ref does not count as a reference", func(t *testing.T) {
		clusterKindRef := canaryRef(policyName)
		clusterKindRef.Kind = "ClusterRolloutPolicy"
		v := newValidator(t, activeNamespace(), validCanaryPolicy(policyName), consumer("llm-a", clusterKindRef))
		resp := v.Handle(context.Background(), deleteRequest(t, validCanaryPolicy(policyName), true))
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
