package v1beta1testing

import (
	"fmt"
	"strings"

	"github.com/onsi/gomega/format"
	"github.com/onsi/gomega/types"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// This file holds Gomega matchers over the metav1.Condition slices that
// OME's CRD statuses expose (BaseModel/ClusterBaseModel.Status.Conditions,
// ServingRuntime/ClusterServingRuntime.Status.Conditions, etc.). They
// remove the FindStatusCondition + status/reason boilerplate that every
// readiness assertion would otherwise repeat.
//
// InferenceServiceStatus uses knative's duckv1 conditions (a different
// type), so these matchers do not apply to it — assert on
// InferenceService.Status.IsReady()/GetCondition there instead.

// HaveConditionStatusAndReason matches a []metav1.Condition that
// contains a condition of the given type with the given status and
// reason. Empty status or reason means "don't care" about that facet.
func HaveConditionStatusAndReason(conditionType string, status metav1.ConditionStatus, reason string) types.GomegaMatcher {
	if conditionType == "" {
		panic("conditionType is empty")
	}
	return &conditionMatcher{conditionType: conditionType, status: status, reason: reason}
}

// HaveConditionStatus matches a condition of the given type with the
// given status, ignoring the reason.
func HaveConditionStatus(conditionType string, status metav1.ConditionStatus) types.GomegaMatcher {
	return HaveConditionStatusAndReason(conditionType, status, "")
}

// HaveCondition matches the mere presence of a condition of the given
// type, ignoring status and reason.
func HaveCondition(conditionType string) types.GomegaMatcher {
	return HaveConditionStatusAndReason(conditionType, "", "")
}

// HaveConditionStatusTrue matches a condition of the given type whose
// status is True.
func HaveConditionStatusTrue(conditionType string) types.GomegaMatcher {
	return HaveConditionStatus(conditionType, metav1.ConditionTrue)
}

// HaveConditionStatusTrueAndReason matches a condition of the given type
// whose status is True and whose reason equals reason.
func HaveConditionStatusTrueAndReason(conditionType, reason string) types.GomegaMatcher {
	return HaveConditionStatusAndReason(conditionType, metav1.ConditionTrue, reason)
}

// HaveConditionStatusFalse matches a condition of the given type whose
// status is False.
func HaveConditionStatusFalse(conditionType string) types.GomegaMatcher {
	return HaveConditionStatus(conditionType, metav1.ConditionFalse)
}

// HaveConditionStatusFalseAndReason matches a condition of the given
// type whose status is False and whose reason equals reason.
func HaveConditionStatusFalseAndReason(conditionType, reason string) types.GomegaMatcher {
	return HaveConditionStatusAndReason(conditionType, metav1.ConditionFalse, reason)
}

type conditionMatcher struct {
	conditionType string
	status        metav1.ConditionStatus
	reason        string
}

func (m *conditionMatcher) Match(actual any) (bool, error) {
	conditions, ok := actual.([]metav1.Condition)
	if !ok {
		return false, fmt.Errorf("condition matcher expects a []metav1.Condition. Got:\n%s", format.Object(actual, 1))
	}
	found := apimeta.FindStatusCondition(conditions, m.conditionType)
	if found == nil {
		return false, nil
	}
	if m.status != "" && found.Status != m.status {
		return false, nil
	}
	if m.reason != "" && found.Reason != m.reason {
		return false, nil
	}
	return true, nil
}

func (m *conditionMatcher) FailureMessage(actual any) string {
	return m.buildMessage(actual, false)
}

func (m *conditionMatcher) NegatedFailureMessage(actual any) string {
	return m.buildMessage(actual, true)
}

func (m *conditionMatcher) buildMessage(actual any, negated bool) string {
	b := strings.Builder{}
	b.WriteString("Expected\n")
	b.WriteString(format.Object(actual, 1))
	b.WriteByte('\n')
	if negated {
		b.WriteString("not ")
	}
	b.WriteString("to have condition type ")
	b.WriteString(m.conditionType)
	if m.status != "" {
		if m.reason == "" {
			b.WriteString(" and")
		} else {
			b.WriteByte(',')
		}
		b.WriteString(" status ")
		b.WriteString(string(m.status))
	}
	if m.reason != "" {
		b.WriteString(" and reason ")
		b.WriteString(m.reason)
	}
	return b.String()
}

// BeNotFoundError matches an apierrors.IsNotFound error. The canonical
// use is asserting an object is gone after deletion:
//
//	g.Expect(c.Get(ctx, key, obj)).To(BeNotFoundError())
func BeNotFoundError() types.GomegaMatcher {
	return BeAPIError(NotFoundErrorType)
}

// BeForbiddenError matches an apierrors.IsForbidden error.
func BeForbiddenError() types.GomegaMatcher {
	return BeAPIError(ForbiddenErrorType)
}

// BeInvalidError matches an apierrors.IsInvalid error.
func BeInvalidError() types.GomegaMatcher {
	return BeAPIError(InvalidErrorType)
}

// BeConflictError matches an apierrors.IsConflict error (optimistic-lock
// rejection).
func BeConflictError() types.GomegaMatcher {
	return BeAPIError(ConflictErrorType)
}

// APIErrorType enumerates the apimachinery error classifiers the
// matchers recognize.
type APIErrorType int

const (
	NotFoundErrorType APIErrorType = iota
	ForbiddenErrorType
	InvalidErrorType
	ConflictErrorType
)

func (t APIErrorType) String() string {
	return []string{"NotFoundError", "ForbiddenError", "InvalidError", "ConflictError"}[t]
}

func (t APIErrorType) isError(err error) bool {
	return []func(error) bool{
		apierrors.IsNotFound,
		apierrors.IsForbidden,
		apierrors.IsInvalid,
		apierrors.IsConflict,
	}[t](err)
}

// BeAPIError matches an error classified by the given APIErrorType.
func BeAPIError(errorType APIErrorType) types.GomegaMatcher {
	return &apiErrorMatcher{errorType: errorType}
}

type apiErrorMatcher struct {
	errorType APIErrorType
}

func (m *apiErrorMatcher) Match(actual any) (bool, error) {
	if actual == nil {
		return false, nil
	}
	err, ok := actual.(error)
	if !ok {
		return false, fmt.Errorf("error matcher expects an error. Got:\n%s", format.Object(actual, 1))
	}
	return err != nil && m.errorType.isError(err), nil
}

func (m *apiErrorMatcher) FailureMessage(actual any) string {
	return fmt.Sprintf("Expected %s, but got:\n%s", m.errorType.String(), format.Object(actual, 1))
}

func (m *apiErrorMatcher) NegatedFailureMessage(any) string {
	return fmt.Sprintf("Expected not to be %s", m.errorType.String())
}
