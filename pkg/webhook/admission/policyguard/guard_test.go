package policyguard

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	admissionv1 "k8s.io/api/admission/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

const (
	testNamespace = "llm-serving"
	policyName    = "canary-slow"
)

type referenceCall struct{ namespace, name string }

// references is a reference rule the test controls: it records what it was
// asked and answers with a fixed list or error.
type references struct {
	list  []string
	err   error
	calls []referenceCall
}

func (r *references) lookup(_ context.Context, namespace, name string) ([]string, error) {
	r.calls = append(r.calls, referenceCall{namespace, name})
	return r.list, r.err
}

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

// newGuard reads the shared flow on its own: the policy kind is RolloutPolicy,
// the spec rule accepts everything and refs decides what references the policy.
func newGuard(t *testing.T, refs *references, funcs interceptor.Funcs, live ...client.Object) Guard[*v1beta1.RolloutPolicy] {
	t.Helper()
	s := testScheme(t)
	return Guard[*v1beta1.RolloutPolicy]{
		Client:       fake.NewClientBuilder().WithScheme(s).WithObjects(live...).WithInterceptorFuncs(funcs).Build(),
		Decoder:      admission.NewDecoder(s),
		Log:          logf.Log.WithName("policyguard-test"),
		Kind:         "RolloutPolicy",
		New:          func() *v1beta1.RolloutPolicy { return &v1beta1.RolloutPolicy{} },
		ValidateSpec: func(*v1beta1.RolloutPolicy) error { return nil },
		Referencing:  refs.lookup,
		Referent:     "InferenceService",
	}
}

func activeNamespace() *corev1.Namespace {
	return &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{Name: testNamespace},
		Status:     corev1.NamespaceStatus{Phase: corev1.NamespaceActive},
	}
}

func policy() *v1beta1.RolloutPolicy {
	return &v1beta1.RolloutPolicy{ObjectMeta: metav1.ObjectMeta{Name: policyName, Namespace: testNamespace}}
}

// deleteRequest is the request the API server sends for a delete; payload is
// the object it carries in OldObject, or nil for an empty payload.
func deleteRequest(t *testing.T, payload any) admission.Request {
	t.Helper()
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Delete,
		Name:      policyName,
		Namespace: testNamespace,
	}}
	if payload != nil {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		req.OldObject.Raw = raw
	}
	return req
}

func requireErrored(t *testing.T, resp admission.Response, code int) {
	t.Helper()
	if resp.Allowed {
		t.Fatal("expected an error response, got allowed")
	}
	if resp.Result == nil || int(resp.Result.Code) != code {
		t.Fatalf("expected status %d, got %+v", code, resp.Result)
	}
}

func TestValidateDeleteAdmitsWhenTheNamespaceIsGone(t *testing.T) {
	refs := &references{list: []string{"llm-a"}}
	resp := newGuard(t, refs, interceptor.Funcs{}).ValidateDelete(context.Background(), deleteRequest(t, policy()))
	if !resp.Allowed {
		t.Fatalf("a policy in a namespace that is gone must be deletable, got %+v", resp.Result)
	}
	if len(refs.calls) != 0 {
		t.Fatalf("the references were read although the namespace is gone: %v", refs.calls)
	}
}

func TestValidateDeleteAnswersAReadFailureWithAnError(t *testing.T) {
	down := apierrors.NewServiceUnavailable("apiserver down")
	cases := []struct {
		name    string
		refs    *references
		funcs   interceptor.Funcs
		payload any
	}{
		{
			name: "namespace read fails",
			refs: &references{},
			funcs: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*corev1.Namespace); ok {
					return down
				}
				return c.Get(ctx, key, obj, opts...)
			}},
			payload: policy(),
		},
		{
			name: "policy read fails on an empty payload",
			refs: &references{},
			funcs: interceptor.Funcs{Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
				if _, ok := obj.(*v1beta1.RolloutPolicy); ok {
					return down
				}
				return c.Get(ctx, key, obj, opts...)
			}},
		},
		{
			name:    "reference listing fails",
			refs:    &references{err: errors.New("list failed")},
			payload: policy(),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			g := newGuard(t, tc.refs, tc.funcs, activeNamespace(), policy())
			requireErrored(t, g.ValidateDelete(context.Background(), deleteRequest(t, tc.payload)), http.StatusInternalServerError)
		})
	}
}

func TestValidateDeleteReadsTheCoordinatesFromTheRequest(t *testing.T) {
	refs := &references{list: []string{"llm-a"}}
	g := newGuard(t, refs, interceptor.Funcs{}, activeNamespace())
	// A payload without metadata names nothing; the request does.
	resp := g.ValidateDelete(context.Background(), deleteRequest(t, map[string]any{"spec": map[string]any{}}))
	if resp.Allowed {
		t.Fatal("a referenced policy must not be deletable")
	}
	want := []referenceCall{{testNamespace, policyName}}
	if len(refs.calls) != 1 || refs.calls[0] != want[0] {
		t.Fatalf("references read for %v, want %v", refs.calls, want)
	}
	if msg := resp.Result.Message; !strings.Contains(msg, `RolloutPolicy "canary-slow" is referenced by 1 InferenceService(s): llm-a;`) {
		t.Fatalf("denial does not name the policy and its reference:\n%s", msg)
	}
}

func TestValidateWriteRejectsAPayloadThatDoesNotDecode(t *testing.T) {
	g := newGuard(t, &references{}, interceptor.Funcs{})
	req := admission.Request{AdmissionRequest: admissionv1.AdmissionRequest{
		Operation: admissionv1.Create,
		Name:      policyName,
		Namespace: testNamespace,
		Object:    runtime.RawExtension{Raw: []byte(`{"spec": [`)},
	}}
	decoded, resp := g.ValidateWrite(req)
	if resp == nil {
		t.Fatal("a payload that does not decode must be answered")
	}
	requireErrored(t, *resp, http.StatusBadRequest)
	if decoded != nil {
		t.Fatalf("no policy comes back with an error response, got %+v", decoded)
	}
}
