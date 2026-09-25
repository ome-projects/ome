package inferencereplica

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/go-logr/logr"
	kedav1 "github.com/kedacore/keda/v2/apis/keda/v1alpha1"
	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	k8sruntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	schedulingv1alpha1 "sigs.k8s.io/scheduler-plugins/apis/scheduling/v1alpha1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	workloadgang "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/gang"
	workloadtypes "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
	"sigs.k8s.io/ome/pkg/utils"
)

// validateWiring is the composition-root guard: production setup must
// fail fast when the authoritative reader is missing instead of
// silently degrading every live read to the lagging cache, and when the
// status write target was never loaded from configuration.

func TestValidateWiring(t *testing.T) {
	r := &Reconciler{}
	if err := r.validateWiring(); err == nil {
		t.Fatal("nil APIReader must be rejected at setup")
	}
	r.APIReader = &podListFailingReader{}
	err := r.validateWiring()
	if err == nil || !strings.Contains(err.Error(), "InstanceStatusTarget") {
		t.Fatalf("an unset status write target must be rejected at setup, got %v", err)
	}
	r.InstanceStatusTarget = irstatus.Encoding("Sparse")
	if err := r.validateWiring(); err == nil {
		t.Fatal("an unknown status write target must be rejected at setup")
	}
	r.InstanceStatusTarget = irstatus.EncodingDenseV1
	if err := r.validateWiring(); err != nil {
		t.Fatalf("wired APIReader and DenseV1 target must pass: %v", err)
	}
	r.InstanceStatusTarget = irstatus.EncodingColumnarV2
	if err := r.validateWiring(); err == nil || !strings.Contains(err.Error(), "maxDecodedInstances") {
		t.Fatalf("a ColumnarV2 target without a decode bound must be rejected at setup, got %v", err)
	}
	r.InstanceStatusDecoder = irstatus.NewDecoder(1)
	if err := r.validateWiring(); err != nil {
		t.Fatalf("wired APIReader, ColumnarV2 target, and bound must pass: %v", err)
	}
}

// newSetupManager builds a manager that never contacts an apiserver: the
// REST mapper is static, the CRD probes read a process-wide discovery cache
// the test seeds, and nothing is started. That is exactly the surface
// SetupWithManager touches: it registers watches and indexes but runs none
// of them.
//
// Controller names are unique per process unless validation is skipped, so
// every case but the one that proves registration through that uniqueness
// skips it.
func newSetupManager(t *testing.T, skipNameValidation bool) ctrl.Manager {
	t.Helper()
	scheme := testScheme(t)
	for name, add := range map[string]func(*k8sruntime.Scheme) error{
		"autoscalingv2": autoscalingv2.AddToScheme,
		"kedav1":        kedav1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			t.Fatalf("%s.AddToScheme: %v", name, err)
		}
	}
	mapper := meta.NewDefaultRESTMapper(nil)
	for _, gvk := range []schema.GroupVersionKind{
		v1beta1.SchemeGroupVersion.WithKind("InferenceReplica"),
		corev1.SchemeGroupVersion.WithKind("Pod"),
		appsv1.SchemeGroupVersion.WithKind("ControllerRevision"),
		autoscalingv2.SchemeGroupVersion.WithKind("HorizontalPodAutoscaler"),
		discoveryv1.SchemeGroupVersion.WithKind("EndpointSlice"),
		schedulingv1alpha1.SchemeGroupVersion.WithKind(constants.PodGroupKind),
		kedav1.SchemeGroupVersion.WithKind(constants.KEDAScaledObjectKind),
	} {
		mapper.Add(gvk, meta.RESTScopeNamespace)
	}
	mgr, err := ctrl.NewManager(&rest.Config{Host: "http://127.0.0.1:0"}, manager.Options{
		Scheme:                 scheme,
		Logger:                 logr.Discard(),
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: "0",
		MapperProvider: func(*rest.Config, *http.Client) (meta.RESTMapper, error) {
			return mapper, nil
		},
		Controller: config.Controller{SkipNameValidation: ptr.To(skipNameValidation)},
	})
	if err != nil {
		t.Fatalf("new manager: %v", err)
	}
	return mgr
}

// seedCRDDiscovery answers the two CRD probes SetupWithManager makes from
// the process-wide discovery cache instead of an apiserver.
func seedCRDDiscovery(podGroup, scaledObject bool) {
	seed := func(gv, kind string, present bool) {
		list := &metav1.APIResourceList{GroupVersion: gv}
		if present {
			list.APIResources = []metav1.APIResource{{Kind: kind}}
		}
		utils.SetAvailableResourcesForApi(gv, list)
	}
	seed(schedulingv1alpha1.SchemeGroupVersion.String(), constants.PodGroupKind, podGroup)
	seed(kedav1.SchemeGroupVersion.String(), constants.KEDAScaledObjectKind, scaledObject)
}

func wiredReconciler(mgr ctrl.Manager) *Reconciler {
	return &Reconciler{
		Client:               mgr.GetClient(),
		Log:                  logr.Discard(),
		InstanceStatusTarget: irstatus.EncodingDenseV1,
		ConfigCacheTTL:       time.Minute,
	}
}

// TestSetupWithManager_RejectsAMiswiredReconciler pins that the wiring
// guard runs inside setup: a reconciler whose status write target was never
// configured must not register, whatever the cluster offers.
func TestSetupWithManager_RejectsAMiswiredReconciler(t *testing.T) {
	seedCRDDiscovery(false, false)
	mgr := newSetupManager(t, true)
	r := wiredReconciler(mgr)
	r.InstanceStatusTarget = ""
	err := r.SetupWithManager(mgr)
	if err == nil || !strings.Contains(err.Error(), "InstanceStatusTarget") {
		t.Fatalf("setup must surface the wiring guard's rejection, got %v", err)
	}
}

// TestSetupWithManager_DefaultsProcessDependencies pins the defaults setup
// fills for a reconciler the composition root left bare: the expectations
// cache, the clock, the authoritative reader and the config cache; and that
// a cache the root did supply is kept, since the pod handler must feed the
// same instance the dispatcher reads.
func TestSetupWithManager_DefaultsProcessDependencies(t *testing.T) {
	seedCRDDiscovery(false, false)

	t.Run("bare reconciler is defaulted", func(t *testing.T) {
		mgr := newSetupManager(t, true)
		r := wiredReconciler(mgr)
		if err := r.SetupWithManager(mgr); err != nil {
			t.Fatalf("setup: %v", err)
		}
		if r.Expectations == nil || r.Clock == nil || r.APIReader == nil || r.ConfigCache == nil {
			t.Fatalf("setup must default Expectations, Clock, APIReader and ConfigCache; got %+v", r)
		}
		if r.GangSchedulingAvailable {
			t.Fatal("without the PodGroup CRD gang scheduling must be reported unavailable")
		}
	})

	t.Run("supplied expectations cache is kept", func(t *testing.T) {
		mgr := newSetupManager(t, true)
		r := wiredReconciler(mgr)
		supplied := workloadtypes.NewExpectations()
		r.Expectations = supplied
		if err := r.SetupWithManager(mgr); err != nil {
			t.Fatalf("setup: %v", err)
		}
		if r.Expectations != supplied {
			t.Fatal("setup replaced the supplied Expectations cache; the pod handler and the dispatcher would then disagree")
		}
	})
}

// TestSetupWithManager_RegistersTheController proves setup registered a
// controller by the one observable side effect registration has before
// Start: the controller name is taken, so a second setup on the same
// manager is rejected as a duplicate.
func TestSetupWithManager_RegistersTheController(t *testing.T) {
	seedCRDDiscovery(false, false)
	mgr := newSetupManager(t, false)
	if err := wiredReconciler(mgr).SetupWithManager(mgr); err != nil {
		t.Fatalf("first setup: %v", err)
	}
	err := wiredReconciler(mgr).SetupWithManager(mgr)
	if err == nil || !strings.Contains(err.Error(), "already exists") || !strings.Contains(err.Error(), "inferencereplica") {
		t.Fatalf("second setup must collide on the registered controller name, got %v", err)
	}
}

// TestSetupWithManager_GangSchedulingFollowsThePodGroupCRD pins the
// discovery-gated half of setup: with the PodGroup CRD present the
// reconciler reports gang scheduling available and registers the
// controller-UID index the PodGroup inventory lists by; without it,
// neither happens.
func TestSetupWithManager_GangSchedulingFollowsThePodGroupCRD(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		present bool
	}{
		{"PodGroup CRD present", true},
		{"PodGroup CRD absent", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			seedCRDDiscovery(tc.present, tc.present)
			mgr := newSetupManager(t, true)
			r := wiredReconciler(mgr)
			if err := r.SetupWithManager(mgr); err != nil {
				t.Fatalf("setup: %v", err)
			}
			if r.GangSchedulingAvailable != tc.present {
				t.Fatalf("GangSchedulingAvailable = %v, want %v", r.GangSchedulingAvailable, tc.present)
			}
			// The index can be registered exactly once per cache: a second
			// registration fails when setup already did it and succeeds
			// when setup correctly left it out.
			err := workloadgang.RegisterPodGroupControllerUIDIndex(ctx, mgr.GetFieldIndexer())
			if tc.present && err == nil {
				t.Fatal("setup must register the PodGroup controller-UID index when the CRD is present")
			}
			if !tc.present && err != nil {
				t.Fatalf("setup must not touch the PodGroup index when the CRD is absent, got %v", err)
			}
		})
	}
}
