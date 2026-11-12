// Package integration exercises the OMEGangPack plugin inside a real
// kube-scheduler against an envtest API server — the authoritative proof that the
// choose/assign/enforce/gate loop actually schedules, covering the informer/New/
// snapshot glue that unit tests fake. Mirrors sigs.k8s.io/scheduler-plugins'
// test/integration harness.
package integration

import (
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"
	"k8s.io/klog/v2/textlogger"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// globalKubeConfig is the envtest API server connection shared by the tests.
var globalKubeConfig *rest.Config

// discardedPods counts the scheduler log line for a pod its queue dropped as
// tracked twice. Every scheduler and plugin log line passes through it.
var discardedPods = &logCounter{out: os.Stderr, pattern: []byte(discardedPodLogLine)}

func TestMain(m *testing.M) {
	klog.SetLoggerWithOptions(textlogger.NewLogger(textlogger.NewConfig(textlogger.Output(discardedPods))), klog.ContextualLogger(true))

	crdPath, err := podGroupCRDPath()
	if err != nil {
		log.Fatalf("locate PodGroup CRD: %v", err)
	}

	testEnv := &envtest.Environment{
		// The scheduler-plugins PodGroup CRD, sourced from the go.mod module (not
		// vendored) — go.mod is the single source of truth for its version.
		CRDDirectoryPaths: []string{crdPath},
	}
	// Disable admission that would taint our fake (kubelet-less) nodes unschedulable.
	apiServerArgs := testEnv.ControlPlane.GetAPIServer().Configure()
	apiServerArgs.Append("disable-admission-plugins", "TaintNodesByCondition", "Priority")
	apiServerArgs.Append("runtime-config", "api/all=true")
	// Widen etcd's raft timing so a busy host does not turn a slow proposal
	// into "request timed out"; the single member still elects itself at once.
	testEnv.ControlPlane.Etcd = &envtest.Etcd{}
	etcdArgs := testEnv.ControlPlane.Etcd.Configure()
	etcdArgs.Append("heartbeat-interval", "500")
	etcdArgs.Append("election-timeout", "5000")

	cfg, err := testEnv.Start()
	if err != nil {
		log.Fatalf("envtest start: %v", err)
	}
	globalKubeConfig = cfg

	code := m.Run()
	_ = testEnv.Stop()
	os.Exit(code)
}

// podGroupCRDPath resolves the scheduler-plugins PodGroup CRD YAML from the
// module resolved by go.mod, so the version installed always matches the pinned
// dependency and nothing is vendored into the tree (see OME's dep-crds policy).
func podGroupCRDPath() (string, error) {
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "sigs.k8s.io/scheduler-plugins").Output()
	if err != nil {
		return "", err
	}
	dir := strings.TrimSpace(string(out))
	return filepath.Join(dir, "config", "crd", "bases", "scheduling.x-k8s.io_podgroups.yaml"), nil
}
