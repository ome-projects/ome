package controllerconfig

import (
	"context"
	"errors"
	"strings"
	"testing"

	authenticationv1 "k8s.io/api/authentication/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	k8sfake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/constants"
)

const identityCheckUser = "system:serviceaccount:ome:ome-controller-manager"

// identityClientset holds the config block and answers a SelfSubjectReview
// with whoami.
func identityClientset(t *testing.T, block string, whoami authenticationv1.UserInfo) *k8sfake.Clientset {
	t.Helper()
	cs := k8sfake.NewSimpleClientset(&corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: constants.InferenceServiceConfigMapName, Namespace: constants.OMENamespace},
		Data:       map[string]string{InferenceReplicaConfigName: block},
	})
	cs.PrependReactor("create", "selfsubjectreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, &authenticationv1.SelfSubjectReview{Status: authenticationv1.SelfSubjectReviewStatus{UserInfo: whoami}}, nil
	})
	return cs
}

func TestCheckControllerIdentity(t *testing.T) {
	ctx := context.Background()
	usernameBlock := `{"controllerIdentity":{"usernames":["` + identityCheckUser + `"]}}`
	mode, err := CheckControllerIdentity(ctx, identityClientset(t, usernameBlock, authenticationv1.UserInfo{Username: identityCheckUser}))
	if err != nil || mode != InferenceReplicaAdmissionByIdentity {
		t.Fatalf("matching username: mode=%q err=%v", mode, err)
	}

	// Any one configured group identifies the controller, whatever its username.
	groupBlock := `{"controllerIdentity":{"groups":["group-a"]}}`
	inGroup := authenticationv1.UserInfo{Username: "system:serviceaccount:ome:other", Groups: []string{"system:authenticated", "group-a"}}
	mode, err = CheckControllerIdentity(ctx, identityClientset(t, groupBlock, inGroup))
	if err != nil || mode != InferenceReplicaAdmissionByIdentity {
		t.Fatalf("matching group: mode=%q err=%v", mode, err)
	}

	// A mismatch names the actual and the configured identity, still reports
	// identity mode, and is a verified answer rather than an unverified one.
	mode, err = CheckControllerIdentity(ctx, identityClientset(t, usernameBlock, authenticationv1.UserInfo{Username: "system:serviceaccount:ome:other"}))
	if err == nil || !strings.Contains(err.Error(), "system:serviceaccount:ome:other") || !strings.Contains(err.Error(), identityCheckUser) {
		t.Fatalf("mismatch should name the actual and the configured identity, got %v", err)
	}
	if errors.Is(err, ErrControllerIdentityUnverified) {
		t.Fatalf("mismatch must not wrap ErrControllerIdentityUnverified, got %v", err)
	}
	if mode != InferenceReplicaAdmissionByIdentity {
		t.Fatalf("mismatch: mode=%q, want %q", mode, InferenceReplicaAdmissionByIdentity)
	}

	// A failed review leaves the mode known and the identity unverified.
	failing := identityClientset(t, usernameBlock, authenticationv1.UserInfo{})
	failing.PrependReactor("create", "selfsubjectreviews", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("connection refused")
	})
	mode, err = CheckControllerIdentity(ctx, failing)
	if !errors.Is(err, ErrControllerIdentityUnverified) || !strings.Contains(err.Error(), "connection refused") || mode != InferenceReplicaAdmissionByIdentity {
		t.Fatalf("failed review: mode=%q err=%v, want identity mode and an error wrapping ErrControllerIdentityUnverified", mode, err)
	}

	// With no identity configured there is nothing to verify, so no review is
	// issued.
	unconfigured := identityClientset(t, "", authenticationv1.UserInfo{Username: "anyone"})
	mode, err = CheckControllerIdentity(ctx, unconfigured)
	if err != nil || mode != InferenceReplicaAdmissionByAnnotation {
		t.Fatalf("unconfigured identity: mode=%q err=%v", mode, err)
	}
	for _, action := range unconfigured.Actions() {
		if action.GetVerb() == "create" && action.GetResource().Resource == "selfsubjectreviews" {
			t.Fatalf("unconfigured identity issued a SelfSubjectReview")
		}
	}
}
