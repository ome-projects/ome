package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
)

// The rules of the runner's inputs and of its loader, held through the two
// entry points every tree has: run and loadEnvironment.

const (
	lockImage    = "repo/ome@sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	lockRevision = "0123456789abcdef0123456789abcdef01234567"
	// lockRefusal opens the one answer to any incomplete or malformed input.
	lockRefusal = "required flags include loopback"
	// lockLoaderReached is the loader's answer to an input root without CRDs:
	// a run that reports it has accepted its inputs.
	lockLoaderReached = "expected the five resource CRDs and InferenceReplica, got 0"
)

var (
	lockPresets    = []string{"/mutate-ome-io-v1beta1-servingruntime-preset", "/mutate-ome-io-v1beta1-clusterservingruntime-preset"}
	lockValidators = []string{"/validate-ome-io-v1beta1-inferenceservice", "/validate-ome-io-v1beta1-servingruntime", "/validate-ome-io-v1beta1-clusterservingruntime", "/validate-ome-io-v1beta1-basemodel", "/validate-ome-io-v1beta1-clusterbasemodel"}
)

// lockCRD is one CustomResourceDefinition document of the given group.
func lockCRD(i int, group string) *unstructured.Unstructured {
	return &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "apiextensions.k8s.io/v1", "kind": "CustomResourceDefinition",
		"metadata": map[string]any{"name": fmt.Sprintf("kind%d.%s", i, group)},
		"spec":     map[string]any{"group": group, "names": map[string]any{"kind": fmt.Sprintf("Kind%d", i)}},
	}}
}

// lockGroups is ome CRD groups of ome.io followed by the other groups given.
func lockGroups(ome int, other ...string) []string {
	out := make([]string, 0, ome+len(other))
	for i := 0; i < ome; i++ {
		out = append(out, "ome.io")
	}
	return append(out, other...)
}

func lockMutating(name string, paths ...string) *unstructured.Unstructured {
	config := admissionv1.MutatingWebhookConfiguration{TypeMeta: metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "MutatingWebhookConfiguration"}, ObjectMeta: metav1.ObjectMeta{Name: name}}
	for i := range paths {
		path, policy, none := paths[i], admissionv1.Fail, admissionv1.SideEffectClassNone
		config.Webhooks = append(config.Webhooks, admissionv1.MutatingWebhook{Name: fmt.Sprintf("%s-%d.ome.io", name, i), ClientConfig: admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Name: "ome", Namespace: "ome", Path: &path}}, FailurePolicy: &policy, SideEffects: &none, AdmissionReviewVersions: []string{"v1"}})
	}
	obj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&config)
	return &unstructured.Unstructured{Object: obj}
}

func lockValidating(name string, paths ...string) *unstructured.Unstructured {
	config := admissionv1.ValidatingWebhookConfiguration{TypeMeta: metav1.TypeMeta{APIVersion: "admissionregistration.k8s.io/v1", Kind: "ValidatingWebhookConfiguration"}, ObjectMeta: metav1.ObjectMeta{Name: name}}
	for i := range paths {
		path := paths[i]
		config.Webhooks = append(config.Webhooks, admissionv1.ValidatingWebhook{Name: fmt.Sprintf("%s-%d.ome.io", name, i), ClientConfig: admissionv1.WebhookClientConfig{Service: &admissionv1.ServiceReference{Name: "ome", Namespace: "ome", Path: &path}}, AdmissionReviewVersions: []string{"v1"}})
	}
	obj, _ := runtime.DefaultUnstructuredConverter.ToUnstructured(&config)
	return &unstructured.Unstructured{Object: obj}
}

// lockCanonicalHooks is the webhook manifest the runner expects: one mutating
// configuration with both preset hooks, then one validating configuration per
// supported kind.
func lockCanonicalHooks() []*unstructured.Unstructured {
	hooks := []*unstructured.Unstructured{lockMutating("presets", lockPresets...)}
	for i, path := range lockValidators {
		hooks = append(hooks, lockValidating(fmt.Sprintf("v%d", i), path))
	}
	return hooks
}

// lockWith is base followed by extra, in a fresh slice.
func lockWith(base []*unstructured.Unstructured, extra ...*unstructured.Unstructured) []*unstructured.Unstructured {
	return append(append([]*unstructured.Unstructured{}, base...), extra...)
}

// lockRoot writes an input root with one CRD file per group and the webhook
// documents given, and loads it for the role.
func lockRoot(t *testing.T, role string, groups []string, hooks ...*unstructured.Unstructured) (*envtest.Environment, error) {
	t.Helper()
	root := t.TempDir()
	for i, group := range groups {
		writeTestObjects(t, filepath.Join(root, "config", "crd", "full", fmt.Sprintf("crd%d.yaml", i)), lockCRD(i, group))
	}
	writeTestObjects(t, filepath.Join(root, "config", "webhook", "manifests.yaml"), hooks...)
	return loadEnvironment(root, "", "127.0.0.1", time.Minute, role)
}

func TestLoadEnvironmentCountsTheOMEGroupOnly(t *testing.T) {
	canonical := lockCanonicalHooks()
	cases := []struct {
		name   string
		groups []string
		crds   int
		err    string
	}{
		{"five ome.io CRDs beside one of another group", lockGroups(5, "other.io"), 0, "expected the five resource CRDs and InferenceReplica, got 5"},
		{"exactly six ome.io CRDs", lockGroups(6), 6, ""},
		{"seven ome.io CRDs beside one of another group", lockGroups(7, "other.io"), 7, ""},
		{"an empty root", nil, 0, lockLoaderReached},
	}
	for _, c := range cases {
		env, err := lockRoot(t, "", c.groups, canonical...)
		if c.err != "" {
			if err == nil || err.Error() != c.err {
				t.Errorf("%s: %v, want %q", c.name, err, c.err)
			}
			continue
		}
		if err != nil || len(env.CRDs) != c.crds {
			t.Errorf("%s: %d CRDs, %v, want %d", c.name, len(env.CRDs), err, c.crds)
		}
	}
}

func TestLoadEnvironmentKeepsPresetsOnceAndCanonicalValidators(t *testing.T) {
	six, canonical := lockGroups(6), lockCanonicalHooks()
	env, err := lockRoot(t, "", six, canonical...)
	if err != nil || len(env.WebhookInstallOptions.MutatingWebhooks) != 1 || len(env.WebhookInstallOptions.MutatingWebhooks[0].Webhooks) != 2 || len(env.WebhookInstallOptions.ValidatingWebhooks) != 5 {
		t.Fatalf("the canonical manifests: %v", err)
	}
	extra := lockWith(canonical,
		lockValidating("other", "/validate-other", "/validate-ome-io-v1beta1-inferencereplica", "/validate-ome-io-v1beta1-InferenceService"),
		lockMutating("pods", "/mutate-ome-io-v1beta1-pod"))
	env, err = lockRoot(t, "", six, extra...)
	if err != nil || len(env.WebhookInstallOptions.MutatingWebhooks) != 1 || len(env.WebhookInstallOptions.ValidatingWebhooks) != 5 {
		t.Fatalf("configurations of other hooks are dropped, not counted: %v", err)
	}
	if env, err = lockRoot(t, "member", six, canonical...); err != nil || len(env.WebhookInstallOptions.ValidatingWebhooks) != 5 {
		t.Fatalf("a member keeps five validators: %v", err)
	}
	cp, err := lockRoot(t, "control-plane", six, canonical...)
	if err != nil || len(cp.WebhookInstallOptions.ValidatingWebhooks) != 4 {
		t.Fatalf("a control plane keeps four validators: %v", err)
	}
	for _, config := range cp.WebhookInstallOptions.ValidatingWebhooks {
		for _, hook := range config.Webhooks {
			if *hook.ClientConfig.Service.Path == lockValidators[0] {
				t.Fatal("a control plane kept the InferenceService validator")
			}
		}
	}
	refusals := []struct {
		name  string
		hooks []*unstructured.Unstructured
		err   string
	}{
		{"a sixth canonical validator configuration", lockWith(canonical, lockValidating("again", lockValidators[0])), "expected 5 canonical webhook configurations, got 6"},
		{"a missing validator configuration", canonical[:5], "expected 5 canonical webhook configurations, got 4"},
		{"a preset mutator named twice", lockWith(canonical, lockMutating("presets-again", lockPresets[1])), "duplicate runtime preset webhook: " + lockPresets[1]},
		{"one preset mutator missing", lockWith([]*unstructured.Unstructured{lockMutating("presets", lockPresets[0])}, canonical[1:]...), "missing runtime preset webhook: " + lockPresets[1]},
	}
	for _, c := range refusals {
		if _, err := lockRoot(t, "", six, c.hooks...); err == nil || err.Error() != c.err {
			t.Errorf("%s: %v, want %q", c.name, err, c.err)
		}
	}
}

// lockInputs writes a plan and an images file under dir and returns the
// complete flag set that reaches the loader.
func lockInputs(t *testing.T, dir, revision, namespace, image string) map[string]string {
	t.Helper()
	plan := fmt.Sprintf(`{"omeRevision":%q,"managerNamespace":%q,"cluster":"a","managerRole":""}`, revision, namespace)
	if err := os.WriteFile(filepath.Join(dir, "plan.json"), []byte(plan), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "images.json"), []byte(fmt.Sprintf(`{"a":%q}`, image)), 0o600); err != nil {
		t.Fatal(err)
	}
	return map[string]string{
		"input-directory": dir, "images": filepath.Join(dir, "images.json"), "assets": "/assets", "report": filepath.Join(dir, "report.json"),
		"manager-command": "/bin/manager", "identity-name": "lock", "loopback-host": "127.0.0.1", "timeout": "1m", "identity-ttl": "2m",
		"manager-metrics-address": ":8080", "manager-health-address": ":8081", "proxy-header-timeout": "1s", "readiness-poll-interval": "1s",
	}
}

// lockRun runs the command with the flags given, on a flag set of its own.
func lockRun(t *testing.T, flags map[string]string) error {
	t.Helper()
	names := make([]string, 0, len(flags))
	for name := range flags {
		names = append(names, name)
	}
	sort.Strings(names)
	args := []string{"admission-runner"}
	for _, name := range names {
		args = append(args, "-"+name+"="+flags[name])
	}
	savedArgs, savedFlags := os.Args, flag.CommandLine
	defer func() { os.Args, flag.CommandLine = savedArgs, savedFlags }()
	os.Args, flag.CommandLine = args, flag.NewFlagSet(args[0], flag.ContinueOnError)
	return run()
}

func TestRunRefusesIncompleteInputs(t *testing.T) {
	complete := func() map[string]string { return lockInputs(t, t.TempDir(), lockRevision, "ome", lockImage) }
	if err := lockRun(t, complete()); err == nil || err.Error() != lockLoaderReached {
		t.Fatalf("a complete set of inputs did not reach the loader: %v", err)
	}
	edits := map[string]func(f map[string]string){
		"assets": func(f map[string]string) { f["assets"] = "" }, "report": func(f map[string]string) { f["report"] = "" },
		"manager command": func(f map[string]string) { f["manager-command"] = "" }, "identity name": func(f map[string]string) { f["identity-name"] = "" },
		"metrics address": func(f map[string]string) { f["manager-metrics-address"] = "" }, "health address": func(f map[string]string) { f["manager-health-address"] = "" },
		"readiness interval": func(f map[string]string) { f["readiness-poll-interval"] = "0" }, "header timeout": func(f map[string]string) { f["proxy-header-timeout"] = "0" },
		"timeout":                                func(f map[string]string) { f["timeout"] = "0" },
		"identity lifetime equal to the timeout": func(f map[string]string) { f["identity-ttl"] = f["timeout"] },
		"identity lifetime below the timeout":    func(f map[string]string) { f["identity-ttl"] = "30s" },
		"IPv6 loopback host":                     func(f map[string]string) { f["loopback-host"] = "::1" },
		"non-loopback host":                      func(f map[string]string) { f["loopback-host"] = "10.0.0.1" },
		"host name":                              func(f map[string]string) { f["loopback-host"] = "localhost" },
		"empty host":                             func(f map[string]string) { f["loopback-host"] = "" },
	}
	for name, edit := range edits {
		f := complete()
		edit(f)
		if err := lockRun(t, f); err == nil || !strings.HasPrefix(err.Error(), lockRefusal) {
			t.Errorf("%s: %v, want the refusal", name, err)
		}
	}
	for name, in := range map[string][3]string{
		"empty namespace":          {lockRevision, "", lockImage},
		"image without a digest":   {lockRevision, "ome", "repo/ome:v1"},
		"image with a short hash":  {lockRevision, "ome", "repo/ome@sha256:abc"},
		"no image for the cluster": {lockRevision, "ome", ""},
		"short revision":           {"abc", "ome", lockImage},
		"revision not hex":         {"g123456789", "ome", lockImage},
		"upper-case revision":      {strings.ToUpper(lockRevision), "ome", lockImage},
	} {
		if err := lockRun(t, lockInputs(t, t.TempDir(), in[0], in[1], in[2])); err == nil || !strings.HasPrefix(err.Error(), lockRefusal) {
			t.Errorf("%s: %v, want the refusal", name, err)
		}
	}
	f := complete()
	f["loopback-host"] = "127.0.0.2"
	if err := lockRun(t, f); err == nil || err.Error() != lockLoaderReached {
		t.Errorf("a second IPv4 loopback address: %v, want the loader reached", err)
	}
	if err := lockRun(t, lockInputs(t, t.TempDir(), lockRevision[:8], "ome", lockImage)); err == nil || err.Error() != lockLoaderReached {
		t.Errorf("an eight-character revision: %v, want the loader reached", err)
	}
	f = complete()
	f["require-embedded-revision"] = "true"
	if err := lockRun(t, f); err == nil || err.Error() != "runner has no embedded release revision" {
		t.Errorf("a test binary asked for a release revision: %v", err)
	}
	f = complete()
	f["input-directory"] = ""
	if err := lockRun(t, f); err == nil || !strings.Contains(err.Error(), "plan.json") {
		t.Errorf("an empty input directory: %v, want the plan named", err)
	}
	f = complete()
	f["images"] = filepath.Join(t.TempDir(), "absent.json")
	if err := lockRun(t, f); err == nil || !strings.Contains(err.Error(), "absent.json") {
		t.Errorf("an absent images file: %v, want the file named", err)
	}
	f = complete()
	if err := os.WriteFile(f["images"], []byte("{bad"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := lockRun(t, f); err == nil || strings.HasPrefix(err.Error(), lockRefusal) || err.Error() == lockLoaderReached {
		t.Errorf("a malformed images file: %v, want a decoding error", err)
	}
}

func TestRunReportsAManagerThatExitsBeforeItListens(t *testing.T) {
	dir := t.TempDir()
	flags := lockInputs(t, dir, lockRevision, "ome", lockImage)
	if err := os.Symlink(filepath.Join(testRepository(t), "config"), filepath.Join(dir, "config")); err != nil {
		t.Fatal(err)
	}
	writeTestObjects(t, filepath.Join(dir, "configmap.yaml"), &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "v1", "kind": "ConfigMap", "metadata": map[string]any{"name": "lock"}}})
	flags["assets"], flags["manager-command"], flags["timeout"], flags["identity-ttl"] = os.Getenv("KUBEBUILDER_ASSETS"), "/bin/false", "3m", "10m"
	err := lockRun(t, flags)
	if err == nil || !strings.HasPrefix(err.Error(), "prebuilt manager exited: ") || !strings.HasSuffix(err.Error(), "; see manager.log") {
		t.Fatalf("a manager that exited before it listened: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, "manager.log")); err != nil {
		t.Fatalf("manager.log was not written beside the report: %v", err)
	}
}
