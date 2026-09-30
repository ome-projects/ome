package controllerconfig

import (
	"context"
	"errors"
	"fmt"

	authenticationv1 "k8s.io/api/authentication/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// +kubebuilder:rbac:groups=authentication.k8s.io,resources=selfsubjectreviews,verbs=create

// InferenceReplicaAdmissionMode names how the InferenceReplica admission
// webhook recognizes the InferenceService controller.
type InferenceReplicaAdmissionMode string

const (
	// InferenceReplicaAdmissionByIdentity: the configured controllerIdentity.
	InferenceReplicaAdmissionByIdentity InferenceReplicaAdmissionMode = "identity"
	// InferenceReplicaAdmissionByAnnotation: no identity is configured and the
	// webhook falls back to the controller-write annotation.
	InferenceReplicaAdmissionByAnnotation InferenceReplicaAdmissionMode = "annotation"
)

// ErrControllerIdentityUnverified marks a CheckControllerIdentity error from a
// SelfSubjectReview that failed: this process's identity is unknown, which is
// not a mismatch.
var ErrControllerIdentityUnverified = errors.New("controller identity not verified")

// CheckControllerIdentity reports the admission mode the inferenceReplica
// block selects and, in identity mode, asks the API server who this process
// is and compares the answer with the configured identity. A manager whose
// own projections the webhook would deny therefore says so at startup. The
// mode is returned even when the comparison fails, and the error of a review
// that could not run wraps ErrControllerIdentityUnverified.
func CheckControllerIdentity(ctx context.Context, clientset kubernetes.Interface) (InferenceReplicaAdmissionMode, error) {
	cfg, err := NewInferenceReplicaConfigCached(nil, clientset)
	if err != nil {
		return "", fmt.Errorf("load %s config: %w", InferenceReplicaConfigName, err)
	}
	identity := cfg.Identity()
	if !identity.Configured() {
		return InferenceReplicaAdmissionByAnnotation, nil
	}
	review, err := clientset.AuthenticationV1().SelfSubjectReviews().Create(ctx, &authenticationv1.SelfSubjectReview{}, metav1.CreateOptions{})
	if err != nil {
		return InferenceReplicaAdmissionByIdentity, fmt.Errorf("%w: self subject review: %w", ErrControllerIdentityUnverified, err)
	}
	if identity.Matches(review.Status.UserInfo) {
		return InferenceReplicaAdmissionByIdentity, nil
	}
	return InferenceReplicaAdmissionByIdentity, fmt.Errorf("this process authenticates as %q (groups %v) but %s.controllerIdentity lists usernames %v and groups %v",
		review.Status.UserInfo.Username, review.Status.UserInfo.Groups,
		InferenceReplicaConfigName, identity.Usernames, identity.Groups)
}
