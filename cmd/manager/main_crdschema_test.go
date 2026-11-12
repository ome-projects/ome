package main

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	ometesting "sigs.k8s.io/ome/pkg/utils/testing"
)

// The start-up test below runs the manager the way main does, so it owns
// the process's one signal handler and the one envtest control plane; a
// second such test in this package would have to share them.
const (
	startupTestTimeout = 3 * time.Minute
	startupTestPoll    = 500 * time.Millisecond
)

// lockedLog collects the manager's log lines from every goroutine.
type lockedLog struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *lockedLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *lockedLog) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

// repoRoot walks up from the package directory to the module root.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "no go.mod above the package directory")
		dir = parent
	}
}

// generatedCRDs loads the CRDs generated from this release's API types.
func generatedCRDs(t *testing.T, root string) []*apiextensionsv1.CustomResourceDefinition {
	t.Helper()
	paths, err := filepath.Glob(filepath.Join(root, "config", "crd", "full", v1beta1.SchemeGroupVersion.Group+"_*.yaml"))
	require.NoError(t, err)
	require.NotEmpty(t, paths, "no generated CRDs under config/crd/full")
	sort.Strings(paths)
	var crds []*apiextensionsv1.CustomResourceDefinition
	for _, path := range paths {
		data, err := os.ReadFile(path)
		require.NoError(t, err)
		crd := &apiextensionsv1.CustomResourceDefinition{}
		require.NoError(t, yaml.Unmarshal(data, crd), path)
		crds = append(crds, crd)
	}
	return crds
}

// dropFirstStatusField removes the first status field from the CRD's
// schema for the manager's version and returns its name: the CRD of the
// release before the one that added the field.
func dropFirstStatusField(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) string {
	t.Helper()
	for i := range crd.Spec.Versions {
		version := &crd.Spec.Versions[i]
		if version.Name != v1beta1.SchemeGroupVersion.Version {
			continue
		}
		status := version.Schema.OpenAPIV3Schema.Properties["status"]
		var names []string
		for name := range status.Properties {
			names = append(names, name)
		}
		sort.Strings(names)
		require.NotEmpty(t, names, "the status schema has no field to drop")
		delete(status.Properties, names[0])
		version.Schema.OpenAPIV3Schema.Properties["status"] = status
		return names[0]
	}
	t.Fatalf("CRD %s has no %s schema", crd.Name, v1beta1.SchemeGroupVersion.Version)
	return ""
}

// dropReadySinceColumns removes the three readySince columns, and the rule
// that keeps them together, from the InferenceReplica CRD's status schema
// for the manager's version and returns their names: the CRD of the release
// before the columns.
func dropReadySinceColumns(t *testing.T, crd *apiextensionsv1.CustomResourceDefinition) []string {
	t.Helper()
	names := []string{"readySinceBase", "readySinceIndexes", "readySinceOffsets"}
	for i := range crd.Spec.Versions {
		version := &crd.Spec.Versions[i]
		if version.Name != v1beta1.SchemeGroupVersion.Version {
			continue
		}
		status := version.Schema.OpenAPIV3Schema.Properties["status"]
		columns, ok := status.Properties["instanceStatusColumns"]
		require.True(t, ok, "the status schema has no instanceStatusColumns")
		for _, name := range names {
			_, ok := columns.Properties[name]
			require.True(t, ok, "the generated CRD lacks %s", name)
			delete(columns.Properties, name)
		}
		columns.XValidations = nil
		status.Properties["instanceStatusColumns"] = columns
		version.Schema.OpenAPIV3Schema.Properties["status"] = status
		return names
	}
	t.Fatalf("CRD %s has no %s schema", crd.Name, v1beta1.SchemeGroupVersion.Version)
	return nil
}

// gaugeValue reads one gauge without labels from the manager's registry.
func gaugeValue(t *testing.T, name string) (float64, bool) {
	t.Helper()
	families, err := metrics.Registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) == 0 && metric.GetGauge() != nil {
				return metric.GetGauge().GetValue(), true
			}
		}
	}
	return 0, false
}

// startupConfigMap loads the operator configuration the kustomize install
// ships, the start-up blocks every manager needs.
func startupConfigMap(t *testing.T, root string) *corev1.ConfigMap {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, "config", "configmap", "inferenceservice.yaml"))
	require.NoError(t, err)
	cm := &corev1.ConfigMap{}
	require.NoError(t, yaml.Unmarshal(data, cm))
	cm.Namespace = constants.OMENamespace
	return cm
}

func freePort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	return listener.Addr().(*net.TCPAddr).Port
}

// eventsNaming lists the events of the install namespace whose message
// names every one of the parts.
func eventsNaming(t *testing.T, clientset kubernetes.Interface, parts ...string) []corev1.Event {
	t.Helper()
	events, err := clientset.CoreV1().Events(constants.OMENamespace).List(context.Background(), metav1.ListOptions{})
	require.NoError(t, err)
	var named []corev1.Event
	for _, event := range events.Items {
		names := true
		for _, part := range parts {
			names = names && strings.Contains(event.Message, part)
		}
		if names {
			named = append(named, event)
		}
	}
	return named
}

// When a deploy upgrades the manager before the CRD chart, the InferenceReplica
// CRD, whose status is typed throughout, lacks a status field this release
// writes and the three readySince columns of its per-Instance status. The
// manager must start, keep running, and say so where an operator looks: a
// log line and an event in the install namespace that name the CRD and the
// field, and for the readySince columns a log line naming them and the
// gauge that counts them, since every status write keeps each Instance's
// Ready time in its own entry until a start finds the columns. The other
// CRDs, installed at this release's schema, must not be named. Once the
// current CRD is applied the same places must say that the CRD is current.
func TestStartupUnderACRDThatLacksAFieldOfThisRelease(t *testing.T) {
	root := repoRoot(t)
	crds := generatedCRDs(t, root)
	kind := reflect.TypeOf(v1beta1.InferenceReplica{}).Name()
	var lagging, current *apiextensionsv1.CustomResourceDefinition
	var otherCRDs []string
	for _, crd := range crds {
		if crd.Spec.Names.Kind == kind {
			current = crd.DeepCopy()
			lagging = crd
			continue
		}
		otherCRDs = append(otherCRDs, crd.Name)
	}
	require.NotNil(t, lagging, "no generated CRD serves %s", kind)
	field := "status." + dropFirstStatusField(t, lagging)
	columns := dropReadySinceColumns(t, lagging)

	env := ometesting.ForBusyHosts(&envtest.Environment{CRDs: crds})
	cfg, err := env.Start()
	require.NoError(t, err, "envtest did not start; KUBEBUILDER_ASSETS names the control plane binaries")
	t.Cleanup(func() { _ = env.Stop() })

	user, err := env.AddUser(envtest.User{Name: "ome-manager", Groups: []string{"system:masters"}}, nil)
	require.NoError(t, err)
	kubeconfig, err := user.KubeConfig()
	require.NoError(t, err)
	kubeconfigPath := filepath.Join(t.TempDir(), "kubeconfig")
	require.NoError(t, os.WriteFile(kubeconfigPath, kubeconfig, 0o600))
	t.Setenv("KUBECONFIG", kubeconfigPath)

	clientset, err := kubernetes.NewForConfig(cfg)
	require.NoError(t, err)
	ctx := context.Background()
	_, err = clientset.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: constants.OMENamespace}}, metav1.CreateOptions{})
	require.NoError(t, err)
	_, err = clientset.CoreV1().ConfigMaps(constants.OMENamespace).Create(ctx, startupConfigMap(t, root), metav1.CreateOptions{})
	require.NoError(t, err)
	hostname, err := os.Hostname()
	require.NoError(t, err)
	managerPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: hostname, Namespace: constants.OMENamespace},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "manager", Image: "manager"}}},
	}
	_, err = clientset.CoreV1().Pods(constants.OMENamespace).Create(ctx, managerPod, metav1.CreateOptions{})
	require.NoError(t, err)

	logs := &lockedLog{}
	ctrl.SetLogger(zap.New(zap.WriteTo(logs)))
	options := DefaultOptions()
	options.metricsAddr = "0"
	options.probeAddr = fmt.Sprintf("127.0.0.1:%d", freePort(t))
	options.webhookPort = freePort(t)
	options.enableWebhook = false
	options.enableLeaderElection = false

	ran := make(chan bool, 1)
	go func() { ran <- (&startup{options: options}).run() }()
	stopped := false
	stopManager := func() {
		if stopped {
			return
		}
		stopped = true
		require.NoError(t, syscall.Kill(os.Getpid(), syscall.SIGTERM))
		select {
		case ok := <-ran:
			require.True(t, ok, "start-up did not go on to the end; log:\n%s", logs.String())
		case <-time.After(startupTestTimeout):
			t.Fatalf("the manager did not stop on SIGTERM; log:\n%s", logs.String())
		}
	}
	t.Cleanup(stopManager)

	require.Eventually(t, func() bool {
		select {
		case ok := <-ran:
			t.Fatalf("start-up ended early with %v; log:\n%s", ok, logs.String())
		default:
		}
		response, err := http.Get("http://" + options.probeAddr + "/healthz")
		if err != nil {
			return false
		}
		response.Body.Close()
		return response.StatusCode == http.StatusOK
	}, startupTestTimeout, startupTestPoll, "the manager did not come up; log:\n%s", logs.String())

	require.Eventually(t, func() bool {
		log := logs.String()
		return strings.Contains(log, lagging.Name) && strings.Contains(log, field) && len(eventsNaming(t, clientset, lagging.Name, field)) > 0
	}, startupTestTimeout, startupTestPoll,
		"the manager runs under a CRD that lacks %s and neither its log nor an event in %s names %s and the field; log:\n%s",
		field, constants.OMENamespace, lagging.Name, logs.String())
	for _, column := range columns {
		require.Contains(t, logs.String(), column, "the manager runs under a CRD that lacks the readySince column %s and its log does not name it; log:\n%s", column, logs.String())
	}
	missing, present := gaugeValue(t, "ome_omenative_ir_status_readysince_columns_missing")
	require.True(t, present, "the gauge of missing readySince columns is not registered")
	require.Equal(t, float64(len(columns)), missing, "the gauge of missing readySince columns")
	for _, event := range eventsNaming(t, clientset, lagging.Name, field) {
		t.Logf("the manager said, on %s %s: %s %s", event.InvolvedObject.Kind, event.InvolvedObject.Name, event.Reason, event.Message)
		require.Equal(t, corev1.EventTypeWarning, event.Type, "event %s/%s: %s", event.Reason, event.InvolvedObject.Name, event.Message)
		require.Equal(t, hostname, event.InvolvedObject.Name, "the event is not on the manager Pod: %+v", event.InvolvedObject)
	}
	for _, other := range otherCRDs {
		require.Empty(t, eventsNaming(t, clientset, other), "a CRD at this release's schema is named in an event")
	}

	extensions, err := apiextensionsclient.NewForConfig(cfg)
	require.NoError(t, err)
	installed, err := extensions.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, lagging.Name, metav1.GetOptions{})
	require.NoError(t, err)
	installed.Spec = current.Spec
	_, err = extensions.ApiextensionsV1().CustomResourceDefinitions().Update(ctx, installed, metav1.UpdateOptions{})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, event := range eventsNaming(t, clientset, lagging.Name) {
			if event.Type == corev1.EventTypeNormal && !strings.Contains(event.Message, field) {
				t.Logf("the manager said, on %s %s: %s %s", event.InvolvedObject.Kind, event.InvolvedObject.Name, event.Reason, event.Message)
				return true
			}
		}
		return false
	}, startupTestTimeout, startupTestPoll,
		"the current CRD was applied and no event in %s says %s is current; log:\n%s", constants.OMENamespace, lagging.Name, logs.String())

	stopManager()
	for _, other := range otherCRDs {
		require.Empty(t, eventsNaming(t, clientset, other), "a CRD at this release's schema is named in an event")
	}
}
