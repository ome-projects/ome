package guard

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

func TestKustomizeDispatchJournalLifecycle(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("set KUBEBUILDER_ASSETS to test real Kustomize lifecycle")
	}
	if _, err := exec.LookPath("kubectl"); err != nil {
		t.Skip("kubectl unavailable")
	}
	env := &envtest.Environment{BinaryAssetsDirectory: assets}
	cfg, err := env.Start()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := env.Stop(); err != nil {
			t.Error(err)
		}
	})
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	kubeconfig := clientcmdapi.Config{
		Clusters:       map[string]*clientcmdapi.Cluster{"test": {Server: cfg.Host, CertificateAuthorityData: cfg.CAData}},
		AuthInfos:      map[string]*clientcmdapi.AuthInfo{"test": {ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData}},
		Contexts:       map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test", Namespace: "ome"}},
		CurrentContext: "test",
	}
	raw, err := clientcmd.Write(kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) error {
		// An explicit test-owned kubeconfig prevents use of the operator's cluster.
		cmd := exec.CommandContext(ctx, "kubectl", append([]string{"--kubeconfig", path}, args...)...)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("kubectl %v: %w\n%s", args, err, out)
		}
		return nil
	}
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ome"}}); err != nil {
		t.Fatal(err)
	}
	const base = "../../../config/alfred"
	const bootstrap = base + "/dispatch-state.yaml"
	if err := run("create", "-f", bootstrap); err != nil {
		t.Fatal(err)
	}
	key := client.ObjectKey{Namespace: "ome", Name: "alfred-dispatch-state"}
	var cm corev1.ConfigMap
	if err := admin.Get(ctx, key, &cm); err != nil {
		t.Fatal(err)
	}
	// Preserve exact bytes, not just equivalent JSON: this is runtime-owned intent.
	const state = `{"version":"v1", "entries":[{"uuid":"12345678-1234-4234-8234-123456789abc","workload":{"Namespace":"prod","Name":"svc"},"workloadUID":"service-uid","irName":"svc-engine","irUID":"replica-uid","component":"engine","instance":0,"fromNode":"source","payload":"{\"schemaVersion\":\"v1\",\"component\":\"engine\",\"instance\":0,\"from_node\":\"source\",\"requested_at\":\"2026-01-01T12:00:00Z\",\"requested_by\":\"alfred\"}","sourceFingerprint":"retained-source-fingerprint","createdAt":"2026-01-01T12:00:00Z","phase":"prepared"}]}`
	cm.Data = map[string]string{"state.json": state, "operator-note": "preserve unrelated text\n"}
	cm.BinaryData = map[string][]byte{"operator-bytes": {0, 1, 255}}
	if err := admin.Update(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	want := cm.DeepCopy()
	check := func(stage string) {
		t.Helper()
		var got corev1.ConfigMap
		if err := admin.Get(ctx, key, &got); err != nil {
			t.Fatalf("%s: journal unavailable: %v", stage, err)
		}
		if got.UID != want.UID || got.DeletionTimestamp != nil || !reflect.DeepEqual(got.Data, want.Data) || !reflect.DeepEqual(got.BinaryData, want.BinaryData) {
			t.Fatalf("%s: journal replaced, deleted, or rewritten: UID=%s want=%s data=%v binary=%v", stage, got.UID, want.UID, got.Data, got.BinaryData)
		}
	}
	for _, mode := range []string{"client", "server-force-conflicts"} {
		for repeat := range 2 {
			args := []string{"apply", "-k", base}
			if mode == "server-force-conflicts" {
				args = append(args, "--server-side", "--force-conflicts")
			}
			if err := run(args...); err != nil {
				t.Fatal(err)
			}
			check(fmt.Sprintf("%s apply %d", mode, repeat+1))
		}
	}
	if err := run("create", "-f", bootstrap); err == nil || !strings.Contains(err.Error(), "AlreadyExists") {
		t.Fatalf("duplicate bootstrap must refuse to overwrite: %v", err)
	}
	check("duplicate bootstrap")
	// Envtest has no deployment/controller cleanup; do not wait for dependants.
	if err := run("delete", "-k", base, "--wait=false"); err != nil {
		t.Fatal(err)
	}
	check("uninstall")
	if err := run("apply", "-k", base, "--server-side", "--force-conflicts"); err != nil {
		t.Fatal(err)
	}
	check("reinstall")
}
