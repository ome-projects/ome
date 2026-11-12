package isvc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/runtimeselector"
)

// replicaRefsScheme is the scheme the fake readers of this file decode with.
func replicaRefsScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		t.Fatalf("scheme: %v", err)
	}
	return scheme
}

// replicaOf is a replica of one component in namespace team-a. projected
// names the service whose parentRef it carries; "" makes it standalone.
func replicaOf(name string, c v1beta1.ComponentType, projected string) *v1beta1.InferenceReplica {
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a"},
		Spec:       v1beta1.InferenceReplicaSpec{Component: c},
	}
	if projected != "" {
		ir.Spec.ParentRef = &v1beta1.ParentReference{Name: projected}
	}
	return ir
}

// serviceOverReplicas is a service in team-a whose roles are the named
// replicas and nothing else: no model, no runtime, no inline role.
func serviceOverReplicas(name string, refs *v1beta1.ReplicaRefs) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "team-a"},
		Spec:       v1beta1.InferenceServiceSpec{ReplicaRefs: refs},
	}
}

// replicaRefsNamespace is the namespace the live checks read: a standalone
// replica per role, a second engine replica the service "other" already
// fronts, an engine replica the service "svc" projects by parentRef, and
// one "svc" projects by a controller owner reference alone.
func replicaRefsNamespace(t *testing.T) client.Client {
	t.Helper()
	ownedEngine := replicaOf("owned-engine", v1beta1.EngineComponent, "")
	ownedEngine.OwnerReferences = []metav1.OwnerReference{{
		APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService", Name: "svc", UID: "svc-uid", Controller: ptr.To(true),
	}}
	return fake.NewClientBuilder().WithScheme(replicaRefsScheme(t)).WithObjects(
		replicaOf("pool-a", v1beta1.EngineComponent, ""),
		replicaOf("pool-d", v1beta1.DecoderComponent, ""),
		replicaOf("router-a", v1beta1.RouterComponent, ""),
		replicaOf("pool-b", v1beta1.EngineComponent, ""),
		replicaOf("svc-engine", v1beta1.EngineComponent, "svc"),
		ownedEngine,
		serviceOverReplicas("other", &v1beta1.ReplicaRefs{Engine: []string{"pool-b"}}),
	).Build()
}

// failingReader is a reader whose every Get and List fails.
func failingReader(t *testing.T) client.Client {
	t.Helper()
	return fake.NewClientBuilder().WithScheme(replicaRefsScheme(t)).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errors.New("connection refused")
		},
		List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error {
			return errors.New("connection refused")
		},
	}).Build()
}

func TestValidateReplicaRefs_OnCreate_DeniesWhatTheNamedReplicasCannotSatisfy(t *testing.T) {
	v := &InferenceServiceValidator{Reader: replicaRefsNamespace(t)}
	for _, tc := range []struct {
		name    string
		refs    *v1beta1.ReplicaRefs
		wantErr string
	}{
		{name: "one engine", refs: &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}}},
		{name: "router, engine and decoder", refs: &v1beta1.ReplicaRefs{Router: []string{"router-a"}, Engine: []string{"pool-a"}, Decoder: []string{"pool-d"}}},
		{name: "a replica that does not exist", refs: &v1beta1.ReplicaRefs{Engine: []string{"pool-z"}},
			wantErr: "spec.replicaRefs.engine names InferenceReplica team-a/pool-z, which does not exist; create it first"},
		{name: "the failing role is the one named", refs: &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}, Router: []string{"router-z"}},
			wantErr: "spec.replicaRefs.router names InferenceReplica team-a/router-z, which does not exist"},
		{name: "a replica projected by parentRef", refs: &v1beta1.ReplicaRefs{Engine: []string{"svc-engine"}},
			wantErr: "spec.replicaRefs.engine names InferenceReplica team-a/svc-engine, which InferenceService svc projects; name a standalone replica"},
		{name: "a replica projected by an owner reference", refs: &v1beta1.ReplicaRefs{Engine: []string{"owned-engine"}},
			wantErr: "which InferenceService svc projects; name a standalone replica"},
		{name: "a replica of another component", refs: &v1beta1.ReplicaRefs{Engine: []string{"router-a"}},
			wantErr: "spec.replicaRefs.engine names InferenceReplica team-a/router-a, whose component is router"},
		{name: "a replica another service already fronts", refs: &v1beta1.ReplicaRefs{Engine: []string{"pool-b"}},
			wantErr: "spec.replicaRefs.engine names InferenceReplica team-a/pool-b, which InferenceService other already fronts; a replica has one service"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := v.validateReplicaRefs(context.Background(), nil, serviceOverReplicas("new", tc.refs))
			if tc.wantErr == "" {
				assert.NoError(t, err)
				return
			}
			assert.ErrorContains(t, err, tc.wantErr)
			assert.False(t, apierrors.IsInternalError(err), "a refusal is the request's fault, not the webhook's")
		})
	}
}

// A stored copy of the service under admission is not a second service: the
// referrer list skips the service's own name.
func TestValidateReplicaRefs_OnCreate_TheServiceIsNotItsOwnSecondComposer(t *testing.T) {
	v := &InferenceServiceValidator{Reader: replicaRefsNamespace(t)}
	err := v.validateReplicaRefs(context.Background(), nil, serviceOverReplicas("other", &v1beta1.ReplicaRefs{Engine: []string{"pool-b"}}))
	assert.NoError(t, err)
}

// The shape rules need no cluster read and run first, so a service that
// carries an inline role beside a reference is refused for the shape even
// when the API server cannot be read.
func TestValidateReplicaRefs_OnCreate_ShapeRulesRunBeforeTheLiveChecks(t *testing.T) {
	v := &InferenceServiceValidator{Reader: failingReader(t)}
	isvc := serviceOverReplicas("new", &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}})
	isvc.Spec.Engine = &v1beta1.EngineSpec{}
	err := v.validateReplicaRefs(context.Background(), nil, isvc)
	assert.ErrorContains(t, err, "spec.engine and spec.replicaRefs cannot both be set; a service references all of its roles or none")
	assert.False(t, apierrors.IsInternalError(err))
}

// A read that fails for any reason but NotFound is the webhook's failure,
// not the request's: an internal error that names what could not be read.
func TestValidateReplicaRefs_OnCreate_AFailedReadIsTheWebhooksError(t *testing.T) {
	refs := &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}}
	listFails := &InferenceServiceValidator{Reader: failingReader(t)}
	err := listFails.validateReplicaRefs(context.Background(), nil, serviceOverReplicas("new", refs))
	assert.True(t, apierrors.IsInternalError(err), "got %v", err)
	assert.ErrorContains(t, err, "list InferenceServices in team-a")

	getFails := &InferenceServiceValidator{Reader: fake.NewClientBuilder().WithScheme(replicaRefsScheme(t)).WithInterceptorFuncs(interceptor.Funcs{
		Get: func(context.Context, client.WithWatch, client.ObjectKey, client.Object, ...client.GetOption) error {
			return errors.New("connection refused")
		},
	}).Build()}
	err = getFails.validateReplicaRefs(context.Background(), nil, serviceOverReplicas("new", refs))
	assert.True(t, apierrors.IsInternalError(err), "got %v", err)
	assert.ErrorContains(t, err, "read InferenceReplica team-a/pool-a")
}

// A validator built without a Reader applies the shape rules alone.
func TestValidateReplicaRefs_OnCreate_NoReaderSkipsTheLiveChecks(t *testing.T) {
	v := &InferenceServiceValidator{}
	err := v.validateReplicaRefs(context.Background(), nil, serviceOverReplicas("new", &v1beta1.ReplicaRefs{Engine: []string{"pool-z"}}))
	assert.NoError(t, err)
}

// On update the block must equal the stored one and nothing is read: the
// references were checked when the service was created and cannot change.
func TestValidateReplicaRefs_OnUpdate_KeepsTheBlockAndNeverReads(t *testing.T) {
	v := &InferenceServiceValidator{Reader: failingReader(t)}
	stored := serviceOverReplicas("svc", &v1beta1.ReplicaRefs{Engine: []string{"pool-a"}})

	same := stored.DeepCopy()
	same.Annotations = map[string]string{"example.com/note": "an unrelated edit"}
	assert.NoError(t, v.validateReplicaRefs(context.Background(), stored, same))

	for name, mutate := range map[string]func(*v1beta1.InferenceService){
		"another engine":    func(isvc *v1beta1.InferenceService) { isvc.Spec.ReplicaRefs.Engine = []string{"pool-b"} },
		"a router added":    func(isvc *v1beta1.InferenceService) { isvc.Spec.ReplicaRefs.Router = []string{"router-a"} },
		"the block dropped": func(isvc *v1beta1.InferenceService) { isvc.Spec.ReplicaRefs = nil },
	} {
		t.Run(name, func(t *testing.T) {
			changed := stored.DeepCopy()
			mutate(changed)
			err := v.validateReplicaRefs(context.Background(), stored, changed)
			assert.ErrorContains(t, err, "spec.replicaRefs is fixed at create; delete and recreate the InferenceService to front other replicas")
			assert.False(t, apierrors.IsInternalError(err))
		})
	}

	// An inline service has no block to keep and stays free to change.
	inline := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "inline", Namespace: "team-a"},
		Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
	}
	edited := inline.DeepCopy()
	edited.Spec.Engine.MinReplicas = ptr.To(3)
	assert.NoError(t, v.validateReplicaRefs(context.Background(), inline, edited))
}

// ValidateCreate admits a service whose three roles are standalone replicas
// and that names no model and no runtime, and refuses the one whose replica
// another service already fronts.
func TestValidateCreate_AdmitsAServiceOverStandaloneReplicasWithoutModelOrRuntime(t *testing.T) {
	c := replicaRefsNamespace(t)
	v := &InferenceServiceValidator{Client: c, Reader: c, RuntimeSelector: runtimeselector.New(c)}

	_, err := v.ValidateCreate(context.Background(), serviceOverReplicas("new", &v1beta1.ReplicaRefs{
		Router: []string{"router-a"}, Engine: []string{"pool-a"}, Decoder: []string{"pool-d"},
	}))
	assert.NoError(t, err)

	_, err = v.ValidateCreate(context.Background(), serviceOverReplicas("second", &v1beta1.ReplicaRefs{Engine: []string{"pool-b"}}))
	assert.ErrorContains(t, err, "which InferenceService other already fronts")
	assert.False(t, apierrors.IsInternalError(err))
}
