package guard

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	yamlutil "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/yaml"
)

func rendered(t *testing.T, command string, args ...string) []*unstructured.Unstructured {
	t.Helper()
	if _, err := exec.LookPath(command); err != nil {
		t.Skip(command + " unavailable")
	}
	out, err := exec.Command(command, args...).CombinedOutput()
	if err != nil {
		t.Fatalf("%s: %v\n%s", command, err, out)
	}
	decoder := yamlutil.NewYAMLOrJSONDecoder(bytes.NewReader(out), 4096)
	var objs []*unstructured.Unstructured
	for {
		o := &unstructured.Unstructured{}
		if err := decoder.Decode(o); err == io.EOF {
			break
		} else if err != nil {
			t.Fatal(err)
		}
		if o.GetKind() != "" {
			objs = append(objs, o)
		}
	}
	return objs
}

func TestShippedGuardMatchesRuntime(t *testing.T) {
	for _, source := range []string{"kustomize", "helm"} {
		t.Run(source, func(t *testing.T) {
			var objects []*unstructured.Unstructured
			if source == "kustomize" {
				objects = rendered(t, "kubectl", "kustomize", "../../../config/alfred")
			} else {
				objects = rendered(t, "helm", "template", "alfred", "../../../charts/ome-alfred", "--namespace", "ome", "--set", "migration.apiVersion=v1", "--set", "simulation.configMapName=worker")
			}
			var p admissionv1.ValidatingAdmissionPolicy
			var b admissionv1.ValidatingAdmissionPolicyBinding
			var state corev1.ConfigMap
			var role rbacv1.Role
			var cluster rbacv1.ClusterRole
			var deploy appsv1.Deployment
			for _, o := range objects {
				var target any
				switch o.GetKind() {
				case "ValidatingAdmissionPolicy":
					target = &p
				case "ValidatingAdmissionPolicyBinding":
					target = &b
				case "ConfigMap":
					if o.GetName() == "alfred-dispatch-state" {
						target = &state
					}
				case "Role":
					target = &role
				case "ClusterRole":
					target = &cluster
				case "Deployment":
					target = &deploy
				}
				if target != nil {
					raw, _ := o.MarshalJSON()
					if err := yaml.Unmarshal(raw, target); err != nil {
						t.Fatal(err)
					}
				}
			}
			if p.Name == "" || b.Name == "" {
				t.Fatal("shipped guard missing")
			}
			p.Generation = 1
			p.Status.ObservedGeneration = 1
			p.Status.TypeChecking = &admissionv1.TypeChecking{}
			scheme := runtime.NewScheme()
			_ = admissionv1.AddToScheme(scheme)
			g := Guard{Reader: fake.NewClientBuilder().WithScheme(scheme).WithObjects(&p, &b).Build(), Namespace: "ome", ServiceAccount: "ome-alfred"}
			if err := g.Check(context.Background()); err != nil {
				t.Fatal(err)
			}
			if source == "kustomize" {
				if state.Name != "" {
					t.Fatal("recurring Kustomize resources must not own the durable dispatch journal")
				}
				// Bootstrap is independently create-only, never part of apply/delete.
				raw, err := os.ReadFile("../../../config/alfred/dispatch-state.yaml")
				if err != nil {
					t.Fatal(err)
				}
				if err := yaml.UnmarshalStrict(raw, &state); err != nil {
					t.Fatal(err)
				}
				if state.APIVersion != "v1" || state.Kind != "ConfigMap" || state.Name != "alfred-dispatch-state" || len(state.OwnerReferences) != 0 {
					t.Fatalf("invalid bootstrap journal identity: %#v", state.ObjectMeta)
				}
			}
			if state.Namespace != "ome" || state.Data["state.json"] != `{"version":"v1","entries":[]}` {
				t.Fatalf("invalid precreated state: %#v", state.Data)
			}
			stateWrite := false
			for _, rule := range role.Rules {
				for _, name := range rule.ResourceNames {
					if name == "alfred-dispatch-state" {
						stateWrite = true
						if len(rule.Resources) != 1 || rule.Resources[0] != "configmaps" {
							t.Fatal("state grant not configmaps")
						}
					}
				}
			}
			if !stateWrite {
				t.Fatal("state persistence permission missing")
			}
			guardRead := false
			for _, rule := range cluster.Rules {
				for _, group := range rule.APIGroups {
					if group == "admissionregistration.k8s.io" {
						guardRead = true
						if len(rule.Verbs) != 1 || rule.Verbs[0] != "get" || len(rule.ResourceNames) != 1 || rule.ResourceNames[0] != PolicyName {
							t.Fatalf("overbroad guard RBAC: %#v", rule)
						}
					}
				}
			}
			if !guardRead {
				t.Fatal("guard read permission missing")
			}
			args := deploy.Spec.Template.Spec.Containers[0].Args
			flags := map[string]bool{}
			for _, a := range args {
				flags[a] = true
			}
			if flags["--migration-api-version=v1"] != (source == "helm") {
				t.Fatalf("incorrect opt-in args: %v", args)
			}
			if source == "helm" {
				for _, a := range []string{"--migration-service-account=ome-alfred", "--migration-ack-timeout=2m", "--migration-failure-backoff=5m"} {
					if !flags[a] {
						t.Fatalf("missing %s", a)
					}
				}
			}
		})
	}
}

func TestChartMigrationOptIn(t *testing.T) {
	objects := rendered(t, "helm", "template", "alfred", "../../../charts/ome-alfred")
	for _, o := range objects {
		if o.GetKind() == "ValidatingAdmissionPolicy" || o.GetKind() == "ValidatingAdmissionPolicyBinding" || o.GetName() == "alfred-dispatch-state" {
			t.Fatal("default chart enabled migration artifacts")
		}
	}
	for _, args := range [][]string{{"--set", "migration.apiVersion=v2", "--set", "simulation.configMapName=worker"}, {"--set", "migration.apiVersion=v1"}} {
		out, err := exec.Command("helm", append([]string{"template", "alfred", "../../../charts/ome-alfred"}, args...)...).CombinedOutput()
		if err == nil {
			t.Fatalf("invalid opt-in accepted: %s", out)
		}
	}
}

// Helm honors keep when an upgrade removes a resource or the release is
// uninstalled. Losing this annotation would erase unresolved dispatch history
// when migration opt-in is turned off.
func TestChartRetainsDispatchStateOnDisableAndUninstall(t *testing.T) {
	objects := rendered(t, "helm", "template", "alfred", "../../../charts/ome-alfred", "--namespace", "ome", "--set", "migration.apiVersion=v1", "--set", "simulation.configMapName=worker")
	found := false
	for _, object := range objects {
		if object.GetKind() == "ConfigMap" && object.GetName() == "alfred-dispatch-state" {
			found = true
			if object.GetAnnotations()["helm.sh/resource-policy"] != "keep" {
				t.Fatal("dispatch journal must survive Helm disable/uninstall with resource-policy keep")
			}
		}
	}
	if !found {
		t.Fatal("opt-in chart did not provision the dispatch journal")
	}
}

func TestHelmDispatchJournalLifecycle(t *testing.T) {
	assets := os.Getenv("KUBEBUILDER_ASSETS")
	if assets == "" {
		t.Skip("set KUBEBUILDER_ASSETS to test real Helm lifecycle")
	}
	if _, err := exec.LookPath("helm"); err != nil {
		t.Skip("helm unavailable")
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
	_ = corev1.AddToScheme(scheme)
	admin, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		t.Fatal(err)
	}
	kubeconfig := clientcmdapi.Config{Clusters: map[string]*clientcmdapi.Cluster{"test": {Server: cfg.Host, CertificateAuthorityData: cfg.CAData}}, AuthInfos: map[string]*clientcmdapi.AuthInfo{"test": {ClientCertificateData: cfg.CertData, ClientKeyData: cfg.KeyData}}, Contexts: map[string]*clientcmdapi.Context{"test": {Cluster: "test", AuthInfo: "test", Namespace: "ome"}}, CurrentContext: "test"}
	raw, err := clientcmd.Write(kubeconfig)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	run := func(args ...string) error {
		cmd := exec.Command("helm", args...)
		cmd.Env = append(os.Environ(), "KUBECONFIG="+path)
		out, err := cmd.CombinedOutput()
		if err != nil {
			return fmt.Errorf("helm %v: %w\n%s", args, err, out)
		}
		return nil
	}
	ctx := context.Background()
	key := client.ObjectKey{Namespace: "ome", Name: "alfred-dispatch-state"}
	if err := admin.Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "ome"}}); err != nil {
		t.Fatal(err)
	}
	if err := run("install", "journal-test", "../../../charts/ome-alfred", "--namespace", "ome", "--set", "migration.apiVersion=v1", "--set", "simulation.configMapName=worker"); err != nil {
		t.Fatal(err)
	}
	const state = `{"version":"v1","entries":[{"uuid":"12345678-1234-1234-1234-123456789abc","phase":"prepared"}]}`
	var cm corev1.ConfigMap
	if err := admin.Get(ctx, key, &cm); err != nil {
		t.Fatal(err)
	}
	cm.Data = map[string]string{"state.json": state, "operator-note": "retain this too"}
	if err := admin.Update(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	wantUID := cm.UID
	check := func(wantState string) {
		t.Helper()
		var got corev1.ConfigMap
		if err := admin.Get(ctx, key, &got); err != nil {
			t.Fatal(err)
		}
		if got.UID != wantUID || got.Data["state.json"] != wantState || got.Data["operator-note"] != "retain this too" {
			t.Fatalf("journal replaced or rewritten: UID=%s data=%v", got.UID, got.Data)
		}
	}
	if err := run("upgrade", "journal-test", "../../../charts/ome-alfred", "--namespace", "ome", "--reuse-values", "--set", "migration.apiVersion="); err != nil {
		t.Fatal(err)
	}
	check(state)
	if err := run("upgrade", "journal-test", "../../../charts/ome-alfred", "--namespace", "ome", "--reuse-values", "--set", "migration.apiVersion=v1"); err != nil {
		t.Fatal(err)
	}
	check(state)
	if err := run("uninstall", "journal-test", "--namespace", "ome"); err != nil {
		t.Fatal(err)
	}
	check(state)
	if err := run("install", "journal-test", "../../../charts/ome-alfred", "--namespace", "ome", "--set", "migration.apiVersion=v1", "--set", "simulation.configMapName=worker"); err != nil {
		t.Fatal(err)
	}
	check(state)
	// Existing corruption must remain visible to the strict runtime decoder;
	// chart upgrades must never silently turn it into an empty journal.
	if err := admin.Get(ctx, key, &cm); err != nil {
		t.Fatal(err)
	}
	cm.Data["state.json"] = "corrupt-existing-state"
	if err := admin.Update(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	if err := run("upgrade", "journal-test", "../../../charts/ome-alfred", "--namespace", "ome", "--reuse-values"); err != nil {
		t.Fatal(err)
	}
	check("corrupt-existing-state")
	if err := admin.Get(ctx, key, &cm); err != nil {
		t.Fatal(err)
	}
	delete(cm.Data, "state.json")
	if err := admin.Update(ctx, &cm); err != nil {
		t.Fatal(err)
	}
	if err := run("upgrade", "journal-test", "../../../charts/ome-alfred", "--namespace", "ome", "--reuse-values"); err == nil {
		t.Fatal("existing journal without state.json must fail upgrade")
	}
}
