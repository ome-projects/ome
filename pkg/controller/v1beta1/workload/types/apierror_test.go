package types

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
		wantClass      APIRejectionClass
		wantReason     string
		wantRetryAfter time.Duration
		wantInMessage  string
	}{
		{
			name:          "invalid pod spec is permanent and workload-caused",
			err:           apierrors.NewInvalid(podGK, "engine-0-default-0", field.ErrorList{field.Required(field.NewPath("spec", "containers"), "at least one container is required")}),
			wantClass:     APIRejectionPermanentWorkload,
			wantReason:    RejectionReasonInvalidPodSpec,
			wantInMessage: "spec.containers",
		},
		{
			name:          "namespace terminating is permanent and environment-caused",
			err:           namespaceTerminatingError(),
			wantClass:     APIRejectionPermanentEnvironment,
			wantReason:    RejectionReasonNamespaceTerminating,
			wantInMessage: "being terminated",
		},
		{
			name:          "quota rejection is capacity-blocked",
			err:           quotaError(),
			wantClass:     APIRejectionCapacityBlocked,
			wantReason:    RejectionReasonQuotaExceeded,
			wantInMessage: "exceeded quota",
		},
		{
			name:           "too many requests is throttled with the server delay",
			err:            apierrors.NewTooManyRequests("client rate limited", 7),
			wantClass:      APIRejectionThrottled,
			wantReason:     RejectionReasonThrottled,
			wantRetryAfter: 7 * time.Second,
		},
		{
			name:       "service unavailable is throttled",
			err:        apierrors.NewServiceUnavailable("apiserver is overloaded"),
			wantClass:  APIRejectionThrottled,
			wantReason: RejectionReasonThrottled,
		},
		{
			name:      "conflict stays transient",
			err:       apierrors.NewConflict(podGR, "engine-0-default-0", errors.New("object was modified")),
			wantClass: APIRejectionTransient,
		},
		{
			name:      "already exists stays transient",
			err:       apierrors.NewAlreadyExists(podGR, "engine-0-default-0"),
			wantClass: APIRejectionTransient,
		},
		{
			name:      "not found stays transient",
			err:       apierrors.NewNotFound(podGR, "engine-0-default-0"),
			wantClass: APIRejectionTransient,
		},
		{
			name:      "plain forbidden without a quota message stays transient",
			err:       apierrors.NewForbidden(podGR, "engine-0-default-0", errors.New("user cannot create pods")),
			wantClass: APIRejectionTransient,
		},
		{
			name:      "unknown error stays transient",
			err:       errors.New("dial tcp: connection refused"),
			wantClass: APIRejectionTransient,
		},
		{
			name:      "nil error stays transient",
			err:       nil,
			wantClass: APIRejectionTransient,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := ClassifyAPIError(tc.err)
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
	if got := ClassifyAPIError(wrapped); got.Class != APIRejectionCapacityBlocked {
		t.Errorf("wrapped quota rejection: got %v want CapacityBlocked", got.Class)
	}
}

// TestRejectionClassPermanence pins which classes end the attempt: only
// the two permanent ones. The transient classes keep the attempt alive.
func TestRejectionClassPermanence(t *testing.T) {
	for _, tc := range []struct {
		class APIRejectionClass
		want  bool
	}{
		{APIRejectionTransient, false},
		{APIRejectionThrottled, false},
		{APIRejectionCapacityBlocked, false},
		{APIRejectionPermanentWorkload, true},
		{APIRejectionPermanentEnvironment, true},
	} {
		if got := tc.class.Permanent(); got != tc.want {
			t.Errorf("%v.Permanent(): got %v want %v", tc.class, got, tc.want)
		}
	}
}

// TestInvalidPodSpecIsWorkloadCaused: a 422 blames the pod template, so
// the reason the rejection stamps on LastFailure must charge the target
// revision's retry ladder exactly as a kubelet image-pull failure does.
func TestInvalidPodSpecIsWorkloadCaused(t *testing.T) {
	if !IsWorkloadCausedReason(RejectionReasonInvalidPodSpec) {
		t.Errorf("%s: got not workload-caused want workload-caused", RejectionReasonInvalidPodSpec)
	}
	if IsWorkloadCausedReason(RejectionReasonNamespaceTerminating) {
		t.Errorf("%s: got workload-caused want environment-caused", RejectionReasonNamespaceTerminating)
	}
	if IsWorkloadCausedReason(RejectionReasonQuotaExceeded) {
		t.Errorf("%s: got workload-caused want not workload-caused", RejectionReasonQuotaExceeded)
	}
}
