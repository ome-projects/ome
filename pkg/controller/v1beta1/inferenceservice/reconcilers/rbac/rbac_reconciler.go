package rbac

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// RBACReconciler reconciles RBAC resources for components
type RBACReconciler struct {
	client           client.Client
	scheme           *runtime.Scheme
	objectMeta       metav1.ObjectMeta
	componentType    v1beta1.ComponentType
	inferenceService *v1beta1.InferenceService
	Log              logr.Logger
}

// NewRBACReconciler creates a new RBAC reconciler
func NewRBACReconciler(
	client client.Client,
	scheme *runtime.Scheme,
	objectMeta metav1.ObjectMeta,
	componentType v1beta1.ComponentType,
	inferenceService *v1beta1.InferenceService,
) *RBACReconciler {
	return &RBACReconciler{
		client:           client,
		scheme:           scheme,
		objectMeta:       objectMeta,
		componentType:    componentType,
		inferenceService: inferenceService,
		Log:              ctrl.Log.WithName("RBACReconciler"),
	}
}

// Reconcile ensures the RBAC resources are created and configured correctly
func (r *RBACReconciler) Reconcile(ctx context.Context) error {
	r.Log.V(1).Info("Reconciling RBAC resources", "name", r.objectMeta.Name, "namespace", r.objectMeta.Namespace, "component", r.componentType)

	for _, obj := range r.desiredObjects() {
		kind := resourceKind(obj)
		if err := r.setOwner(obj); err != nil {
			return fmt.Errorf("failed to set owner reference for %s: %w", kind, err)
		}
		if err := r.createOrUpdate(ctx, obj); err != nil {
			return fmt.Errorf("failed to reconcile %s: %w", kind, err)
		}
	}

	r.Log.V(1).Info("Successfully reconciled RBAC resources", "name", r.objectMeta.Name, "namespace", r.objectMeta.Namespace)
	return nil
}

// desiredObjects lists the objects the component owns, in the order they are
// written: the ServiceAccount every component runs as, and for the router the
// Role and RoleBinding that let it read the pods it routes to.
func (r *RBACReconciler) desiredObjects() []client.Object {
	name := r.GetServiceAccountName()
	objects := []client.Object{r.serviceAccount(name)}
	if r.componentType == v1beta1.RouterComponent {
		objects = append(objects, r.podReaderRole(name), r.podReaderRoleBinding(name))
	}
	return objects
}

func (r *RBACReconciler) serviceAccount(name string) *corev1.ServiceAccount {
	return &corev1.ServiceAccount{ObjectMeta: r.metadata(name)}
}

func (r *RBACReconciler) podReaderRole(name string) *rbacv1.Role {
	return &rbacv1.Role{
		ObjectMeta: r.metadata(name),
		Rules: []rbacv1.PolicyRule{
			{
				APIGroups: []string{""},
				Resources: []string{"pods"},
				Verbs:     []string{"get", "list", "watch"},
			},
		},
	}
}

func (r *RBACReconciler) podReaderRoleBinding(name string) *rbacv1.RoleBinding {
	return &rbacv1.RoleBinding{
		ObjectMeta: r.metadata(name),
		RoleRef: rbacv1.RoleRef{
			APIGroup: rbacv1.GroupName,
			Kind:     "Role",
			Name:     name,
		},
		Subjects: []rbacv1.Subject{
			{
				Kind:      "ServiceAccount",
				Name:      name,
				Namespace: r.objectMeta.Namespace,
			},
		},
	}
}

// metadata names an owned object in the component's namespace with the
// component's labels.
func (r *RBACReconciler) metadata(name string) metav1.ObjectMeta {
	return metav1.ObjectMeta{
		Name:      name,
		Namespace: r.objectMeta.Namespace,
		Labels:    r.objectMeta.Labels,
	}
}

// setOwner gives an owned object the component's owner references when the
// component has any, and otherwise makes the InferenceService its controller.
func (r *RBACReconciler) setOwner(obj client.Object) error {
	if len(r.objectMeta.OwnerReferences) > 0 {
		obj.SetOwnerReferences(r.objectMeta.OwnerReferences)
		return nil
	}
	return controllerutil.SetControllerReference(r.inferenceService, obj, r.scheme)
}

// GetServiceAccountName returns the name of the ServiceAccount using inference service name + component name
func (r *RBACReconciler) GetServiceAccountName() string {
	return fmt.Sprintf("%s-%s", r.inferenceService.Name, string(r.componentType))
}

// createOrUpdate creates the resource when absent, and otherwise issues an
// Update only when the live object differs from the desired one in a field OME
// manages. This avoids a no-op PUT (and the accompanying log line) on every
// reconcile when nothing has changed.
func (r *RBACReconciler) createOrUpdate(ctx context.Context, obj client.Object) error {
	key := client.ObjectKeyFromObject(obj)
	kind := resourceKind(obj)
	existing := obj.DeepCopyObject().(client.Object)

	err := r.client.Get(ctx, key, existing)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Create the resource
			r.Log.Info("Creating resource", "kind", kind, "name", obj.GetName())
			return r.client.Create(ctx, obj)
		}
		return err
	}

	if rbacSemanticEquals(obj, existing) {
		// Steady state: nothing OME manages has drifted, skip the PUT.
		r.Log.V(1).Info("Resource already up to date", "kind", kind, "name", obj.GetName())
		return nil
	}

	// Update the resource
	r.Log.Info("Updating resource", "kind", kind, "name", obj.GetName())
	obj.SetResourceVersion(existing.GetResourceVersion())
	return r.client.Update(ctx, obj)
}

// resourceKind derives a human-readable kind for logging. Typed objects built
// in-code don't carry a populated GroupVersionKind, so fall back to the Go type
// name instead of logging an empty string.
func resourceKind(obj client.Object) string {
	if k := obj.GetObjectKind().GroupVersionKind().Kind; k != "" {
		return k
	}
	switch obj.(type) {
	case *corev1.ServiceAccount:
		return "ServiceAccount"
	case *rbacv1.Role:
		return "Role"
	case *rbacv1.RoleBinding:
		return "RoleBinding"
	default:
		return fmt.Sprintf("%T", obj)
	}
}

// rbacSemanticEquals reports whether the desired object matches the live one in
// every field OME manages: the metadata it stamps (Labels, Annotations,
// OwnerReferences) plus the type-specific payload (Role.Rules,
// RoleBinding.RoleRef/Subjects). ServiceAccounts carry no managed payload beyond
// metadata. Server-populated fields (resourceVersion, secrets, etc.) are
// ignored so a steady object does not trigger a write.
func rbacSemanticEquals(desired, existing client.Object) bool {
	if !equality.Semantic.DeepEqual(desired.GetLabels(), existing.GetLabels()) {
		return false
	}
	if !equality.Semantic.DeepEqual(desired.GetAnnotations(), existing.GetAnnotations()) {
		return false
	}
	if !equality.Semantic.DeepEqual(desired.GetOwnerReferences(), existing.GetOwnerReferences()) {
		return false
	}

	switch d := desired.(type) {
	case *rbacv1.Role:
		e, ok := existing.(*rbacv1.Role)
		if !ok {
			return false
		}
		return equality.Semantic.DeepEqual(d.Rules, e.Rules)
	case *rbacv1.RoleBinding:
		e, ok := existing.(*rbacv1.RoleBinding)
		if !ok {
			return false
		}
		return equality.Semantic.DeepEqual(d.RoleRef, e.RoleRef) &&
			equality.Semantic.DeepEqual(d.Subjects, e.Subjects)
	default:
		// ServiceAccount and anything else: metadata is the only managed surface.
		return true
	}
}
