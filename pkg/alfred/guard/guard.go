package guard

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// PolicyName and BindingName are cluster-fixed: one Alfred identity per cluster.
const (
	PolicyName  = "ome-alfred-migration-writes"
	BindingName = PolicyName
)

// Guard verifies the admission boundary for the configured Alfred identity.
type Guard struct {
	Reader                    client.Reader
	Namespace, ServiceAccount string
}

// Check verifies the live, type-checked guard on every execution pass. The
// caller must provide an uncached API reader; Alfred never installs the guard.
func (g *Guard) Check(ctx context.Context) error {
	if g.Reader == nil || g.Namespace == "" || g.ServiceAccount == "" {
		return fmt.Errorf("admission guard reader and service account identity are required")
	}
	var p admissionv1.ValidatingAdmissionPolicy
	if err := g.Reader.Get(ctx, client.ObjectKey{Name: PolicyName}, &p); err != nil {
		return fmt.Errorf("read migration admission policy: %w", err)
	}
	want := Policy(PolicyName, "system:serviceaccount:"+g.Namespace+":"+g.ServiceAccount)
	if !p.DeletionTimestamp.IsZero() || !equality.Semantic.DeepEqual(p.Spec, want.Spec) {
		return fmt.Errorf("migration admission policy differs from the required guard")
	}
	if p.Generation < 1 || p.Status.ObservedGeneration != p.Generation || p.Status.TypeChecking == nil || len(p.Status.TypeChecking.ExpressionWarnings) != 0 {
		return fmt.Errorf("migration admission policy is not type-checked at its current generation")
	}
	var b admissionv1.ValidatingAdmissionPolicyBinding
	if err := g.Reader.Get(ctx, client.ObjectKey{Name: BindingName}, &b); err != nil {
		return fmt.Errorf("read migration admission binding: %w", err)
	}
	if !b.DeletionTimestamp.IsZero() || !equality.Semantic.DeepEqual(b.Spec, Binding(BindingName, PolicyName).Spec) {
		return fmt.Errorf("migration admission binding differs from the required guard")
	}
	return nil
}

// Policy is the canonical additive-only migration boundary. Resource versions
// and managed fields are API-server bookkeeping and may change during a patch.
// All other ObjectMeta fields and the entire spec/status remain unchanged.
func Policy(name, username string) *admissionv1.ValidatingAdmissionPolicy {
	fail, exact, scope := admissionv1.Fail, admissionv1.Exact, admissionv1.NamespacedScope
	var preserved []string
	for _, field := range []string{
		"apiVersion", "kind", "spec", "status", "metadata.name", "metadata.generateName",
		"metadata.namespace", "metadata.uid", "metadata.selfLink", "metadata.creationTimestamp", "metadata.generation",
		"metadata.deletionTimestamp", "metadata.deletionGracePeriodSeconds", "metadata.labels",
		"metadata.ownerReferences", "metadata.finalizers",
	} {
		preserved = append(preserved, fmt.Sprintf("has(object.%[1]s) == has(oldObject.%[1]s) && (!has(oldObject.%[1]s) || object.%[1]s == oldObject.%[1]s)", field))
	}
	old := "(has(oldObject.metadata.annotations) ? oldObject.metadata.annotations : {})"
	current := "(has(object.metadata.annotations) ? object.metadata.annotations : {})"
	return &admissionv1.ValidatingAdmissionPolicy{
		TypeMeta:   metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingAdmissionPolicy"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: admissionv1.ValidatingAdmissionPolicySpec{
			FailurePolicy: &fail,
			MatchConstraints: &admissionv1.MatchResources{
				MatchPolicy: &exact, NamespaceSelector: &metav1.LabelSelector{}, ObjectSelector: &metav1.LabelSelector{},
				ResourceRules: []admissionv1.NamedRuleWithOperations{{
					RuleWithOperations: admissionv1.RuleWithOperations{
						Operations: []admissionv1.OperationType{admissionv1.Update},
						Rule: admissionv1.Rule{
							APIGroups: []string{"ome.io"}, APIVersions: []string{"v1beta1"},
							Resources: []string{"inferenceservices", "inferenceservices/status"}, Scope: &scope,
						},
					},
				}},
			},
			MatchConditions: []admissionv1.MatchCondition{{Name: "alfred-identity", Expression: "request.userInfo.username == " + strconv.Quote(username)}},
			Validations: []admissionv1.Validation{
				{Expression: strings.Join(preserved, " && "), Message: "Alfred may not change workload fields or non-annotation metadata"},
				{Expression: old + ".all(k, k in " + current + " && " + current + "[k] == " + old + "[k])", Message: "Alfred may not remove or replace existing annotations"},
				{Expression: current + ".size() <= " + old + ".size() + 1 && " + current + ".all(k, k in " + old + " || (k.matches('^ome[.]io/migration-request-v1-[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$') && bytes(" + current + "[k]).size() <= 4096))", Message: "Alfred may add only one bounded v1 migration request"},
			},
		},
	}
}

// Binding denies requests rejected by the policy without narrowing its scope.
func Binding(name, policyName string) *admissionv1.ValidatingAdmissionPolicyBinding {
	return &admissionv1.ValidatingAdmissionPolicyBinding{
		TypeMeta:   metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingAdmissionPolicyBinding"},
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: admissionv1.ValidatingAdmissionPolicyBindingSpec{
			PolicyName: policyName, ValidationActions: []admissionv1.ValidationAction{admissionv1.Deny},
		},
	}
}
