package evidence_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/evidence"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// podGR / podGK name the resource the create paths address, so the
// fixture errors carry the same qualification the apiserver stamps.
var (
	podGR = schema.GroupResource{Resource: "pods"}
	podGK = schema.GroupKind{Kind: "Pod"}
)

// namespaceTerminatingError reproduces the apiserver's namespace-lifecycle
// admission rejection: a Forbidden carrying the NamespaceTerminating cause.
func namespaceTerminatingError() error {
	return &apierrors.StatusError{ErrStatus: metav1.Status{
		Status:  metav1.StatusFailure,
		Code:    403,
		Reason:  metav1.StatusReasonForbidden,
		Message: `pods "engine-0-default-0" is forbidden: unable to create new content in namespace prod because it is being terminated`,
		Details: &metav1.StatusDetails{
			Group: "", Kind: "pods", Name: "engine-0-default-0",
			Causes: []metav1.StatusCause{{
				Type:    corev1.NamespaceTerminatingCause,
				Message: "namespace prod is being terminated",
			}},
		},
	}}
}

// quotaError reproduces the ResourceQuota admission rejection: a Forbidden
// whose message names the exceeded quota.
func quotaError() error {
	return apierrors.NewForbidden(podGR, "engine-0-default-0", errors.New(
		"exceeded quota: team-quota, requested: requests.nvidia.com/gpu=8, used: requests.nvidia.com/gpu=56, limited: requests.nvidia.com/gpu=64"))
}

func TestClassifyAPIError(t *testing.T) {
	for _, tc := range []struct {
		name           string
		err            error
		wantClass      types.APIRejectionClass
		wantReason     string
		wantRetryAfter time.Duration
		wantInMessage  string
	}{
		{
			name:          "invalid pod spec is permanent and workload-caused",
			err:           apierrors.NewInvalid(podGK, "engine-0-default-0", field.ErrorList{field.Required(field.NewPath("spec", "containers"), "at least one container is required")}),
			wantClass:     types.APIRejectionPermanentWorkload,
			wantReason:    types.RejectionReasonInvalidPodSpec,
			wantInMessage: "spec.containers",
		},
		{
			name:          "namespace terminating is permanent and environment-caused",
			err:           namespaceTerminatingError(),
			wantClass:     types.APIRejectionPermanentEnvironment,
			wantReason:    types.RejectionReasonNamespaceTerminating,
			wantInMessage: "being terminated",
		},
		{
			name:          "quota rejection is capacity-blocked",
			err:           quotaError(),
			wantClass:     types.APIRejectionCapacityBlocked,
			wantReason:    types.RejectionReasonQuotaExceeded,
			wantInMessage: "exceeded quota",
		},
		{
			name:           "too many requests is throttled with the server delay",
			err:            apierrors.NewTooManyRequests("client rate limited", 7),
			wantClass:      types.APIRejectionThrottled,
			wantReason:     types.RejectionReasonThrottled,
			wantRetryAfter: 7 * time.Second,
		},
		{
			name:       "service unavailable is throttled",
			err:        apierrors.NewServiceUnavailable("apiserver is overloaded"),
			wantClass:  types.APIRejectionThrottled,
			wantReason: types.RejectionReasonThrottled,
		},
		{
			name:      "conflict stays transient",
			err:       apierrors.NewConflict(podGR, "engine-0-default-0", errors.New("object was modified")),
			wantClass: types.APIRejectionTransient,
		},
		{
			name:      "already exists stays transient",
			err:       apierrors.NewAlreadyExists(podGR, "engine-0-default-0"),
			wantClass: types.APIRejectionTransient,
		},
		{
			name:      "not found stays transient",
			err:       apierrors.NewNotFound(podGR, "engine-0-default-0"),
			wantClass: types.APIRejectionTransient,
		},
		{
			name:      "plain forbidden without a quota message stays transient",
			err:       apierrors.NewForbidden(podGR, "engine-0-default-0", errors.New("user cannot create pods")),
			wantClass: types.APIRejectionTransient,
		},
		{
			name:      "unknown error stays transient",
			err:       errors.New("dial tcp: connection refused"),
			wantClass: types.APIRejectionTransient,
		},
		{
			name:      "nil error stays transient",
			err:       nil,
			wantClass: types.APIRejectionTransient,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := evidence.ClassifyAPIError(tc.err)
			if got.Class != tc.wantClass {
				t.Fatalf("Class: got %v want %v", got.Class, tc.wantClass)
			}
			if got.Reason != tc.wantReason {
				t.Errorf("Reason: got %q want %q", got.Reason, tc.wantReason)
			}
			if got.RetryAfter != tc.wantRetryAfter {
				t.Errorf("RetryAfter: got %v want %v", got.RetryAfter, tc.wantRetryAfter)
			}
			if tc.wantInMessage != "" && !strings.Contains(got.Message, tc.wantInMessage) {
				t.Errorf("Message: got %q want it to contain %q", got.Message, tc.wantInMessage)
			}
		})
	}
}

// TestClassifyAPIError_WrappedError: the create paths wrap the apiserver
// error before it reaches the classifier, so classification must unwrap.
func TestClassifyAPIError_WrappedError(t *testing.T) {
	wrapped := errors.Join(errors.New("create pod engine-0-default-0"), quotaError())
	if got := evidence.ClassifyAPIError(wrapped); got.Class != types.APIRejectionCapacityBlocked {
		t.Errorf("wrapped quota rejection: got %v want CapacityBlocked", got.Class)
	}
}
