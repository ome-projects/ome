// Package policyguard holds the admission rules the policy kinds share. A
// write is decoded and its spec checked without reading the cluster. A
// delete is refused while an InferenceService in the same namespace
// references the policy; the break-glass annotation, a namespace that is
// gone or terminating, and a policy that is already gone each admit it
// instead, so a teardown never waits on the webhook.
package policyguard

import (
	"context"
	"fmt"
	"net/http"
	"strings"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"

	"sigs.k8s.io/ome/pkg/constants"
)

// MaxNamedReferences caps how many references the in-use deletion denial
// names. The denial covers every reference; the cap keeps the message
// readable when a policy serves a large fleet.
const MaxNamedReferences = 10

// Guard is the admission logic of one policy kind. The kind's webhook builds
// one from its own client, decoder and logger and supplies the words and
// the reference rule that differ between the kinds.
type Guard[P client.Object] struct {
	Client  client.Reader
	Decoder admission.Decoder
	Log     logr.Logger
	// Kind is the policy kind as the API names it; the log lines and the
	// denial carry it.
	Kind string
	// New returns an empty policy to decode or read into.
	New func() P
	// ValidateSpec checks a written policy without reading the cluster. Its
	// error text is the denial.
	ValidateSpec func(policy P) error
	// Referencing lists what references the named policy in the namespace,
	// one entry per reference as the denial names it. Referent is the word
	// for one entry, such as "component" or "InferenceService".
	Referencing func(ctx context.Context, namespace, name string) ([]string, error)
	Referent    string
}

// ValidateWrite decodes a CREATE or UPDATE and checks its spec. A non-nil
// response ends the request: a payload that does not decode is an error, a
// spec issue a denial.
func (g Guard[P]) ValidateWrite(req admission.Request) (P, *admission.Response) {
	policy := g.New()
	if err := g.Decoder.Decode(req, policy); err != nil {
		g.Log.Error(err, "Failed to decode "+g.Kind, "name", req.Name, "namespace", req.Namespace)
		return g.withoutPolicy(admission.Errored(http.StatusBadRequest, err))
	}
	if err := g.ValidateSpec(policy); err != nil {
		return g.withoutPolicy(admission.Denied(err.Error()))
	}
	return policy, nil
}

// ValidateDelete refuses a DELETE while something references the policy. It
// admits when nothing does, when the break-glass annotation is set (by a
// prior, reviewable update), when the namespace is gone or terminating (a
// teardown deletes objects in any order and must not wait), and when the
// policy is already gone.
func (g Guard[P]) ValidateDelete(ctx context.Context, req admission.Request) admission.Response {
	policy, earlyResponse := g.deletedPolicy(ctx, req)
	if earlyResponse != nil {
		return *earlyResponse
	}

	// The break-glass key is shared by every in-use deletion guard in the
	// API group.
	if policy.GetAnnotations()[constants.AutoscalerPolicyAllowInUseDelete] == "true" {
		return admission.Allowed(fmt.Sprintf("in-use deletion allowed by the %s annotation", constants.AutoscalerPolicyAllowInUseDelete))
	}

	name, namespace := policy.GetName(), policy.GetNamespace()
	ns := &corev1.Namespace{}
	if err := g.Client.Get(ctx, types.NamespacedName{Name: namespace}, ns); err != nil {
		if apierrors.IsNotFound(err) {
			return admission.Allowed("namespace is gone; nothing can reference the policy")
		}
		g.Log.Error(err, "Failed to get namespace for "+g.Kind+" deletion", "name", name, "namespace", namespace)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if ns.Status.Phase == corev1.NamespaceTerminating || !ns.DeletionTimestamp.IsZero() {
		return admission.Allowed("namespace is terminating; teardown deletes objects in arbitrary order")
	}

	referencing, err := g.Referencing(ctx, namespace, name)
	if err != nil {
		g.Log.Error(err, "Failed to list InferenceServices for "+g.Kind+" deletion", "name", name, "namespace", namespace)
		return admission.Errored(http.StatusInternalServerError, err)
	}
	if len(referencing) == 0 {
		return admission.Allowed("")
	}
	return admission.Denied(g.inUseDenial(name, referencing))
}

// inUseDenial names the references that hold the policy, up to the cap, and
// tells the operator how to proceed.
func (g Guard[P]) inUseDenial(name string, referencing []string) string {
	named := referencing
	overflow := ""
	if len(referencing) > MaxNamedReferences {
		named = referencing[:MaxNamedReferences]
		overflow = fmt.Sprintf(" and %d more", len(referencing)-MaxNamedReferences)
	}
	return fmt.Sprintf(
		"%s %q is referenced by %d %s(s): %s%s; remove the refs first, or set the %s=%q annotation to force deletion",
		g.Kind, name, len(referencing), g.Referent, strings.Join(named, ", "), overflow,
		constants.AutoscalerPolicyAllowInUseDelete, "true",
	)
}

// deletedPolicy materializes the policy under deletion. The API server
// usually sends the object in OldObject, but a DELETE request may arrive
// with an empty payload; then the policy is fetched by name, and a NotFound
// admits outright, because deleting an object that is already gone must
// never wait. The request coordinates are authoritative: a decoded object
// may lack them when the payload carries no metadata. A non-nil response
// ends the request.
func (g Guard[P]) deletedPolicy(ctx context.Context, req admission.Request) (P, *admission.Response) {
	policy := g.New()
	if len(req.OldObject.Raw) > 0 {
		if err := g.Decoder.DecodeRaw(req.OldObject, policy); err != nil {
			g.Log.Error(err, "Failed to decode "+g.Kind+" under deletion", "name", req.Name, "namespace", req.Namespace)
			return g.withoutPolicy(admission.Errored(http.StatusBadRequest, err))
		}
	} else if err := g.Client.Get(ctx, types.NamespacedName{Namespace: req.Namespace, Name: req.Name}, policy); err != nil {
		if apierrors.IsNotFound(err) {
			return g.withoutPolicy(admission.Allowed("policy is already gone"))
		}
		g.Log.Error(err, "Failed to get "+g.Kind+" under deletion", "name", req.Name, "namespace", req.Namespace)
		return g.withoutPolicy(admission.Errored(http.StatusInternalServerError, err))
	}
	if policy.GetName() == "" {
		policy.SetName(req.Name)
	}
	if policy.GetNamespace() == "" {
		policy.SetNamespace(req.Namespace)
	}
	return policy, nil
}

// withoutPolicy pairs the response that ends the request with no policy.
func (Guard[P]) withoutPolicy(response admission.Response) (P, *admission.Response) {
	var none P
	return none, &response
}
