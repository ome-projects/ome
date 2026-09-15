package admin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"
	"k8s.io/client-go/kubernetes/fake"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/printers"
)

const fixtureCycle = `{"timestamp":"2000-01-01T00:00:00Z","mode":"recommend-only","recommendations":[{"workload":"prod/chat","component":"engine","instance":0,"policy":"defragmentation","reason":"Fragmentation","outcome":"advisory","advisoryReason":"RawDeploymentMigrationUnsupported","requestUUID":"SECRET","scheduling":{"token":"SECRET"}}]}`

func fixtures(ns string) []runtime.Object {
	return []runtime.Object{
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "alfred-config", Namespace: ns}, Data: map[string]string{"config.yaml": "schemaVersion: 1"}},
		&corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: "alfred-recommendations", Namespace: ns, Annotations: map[string]string{"password": "SECRET"}}, Data: map[string]string{"last-cycle.json": fixtureCycle, "node.private": "SECRET"}, BinaryData: map[string][]byte{"token": []byte("SECRET")}},
	}
}

func TestFormatsAndSafeSourceDisplay(t *testing.T) {
	var structured []map[string]interface{}
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		t.Run(format, func(t *testing.T) {
			client := fake.NewSimpleClientset(fixtures("ome")...)
			var out bytes.Buffer
			cmd := NewCmd(factory.Static{Kube: client, NS: "workloads"}, genericiooptions.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}})
			cmd.SetArgs([]string{"recommendations", "-o", format})
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			text := out.String()
			for _, bad := range []string{"SECRET", "requestUUID", "token", "password", "\x1b"} {
				if strings.Contains(text, bad) {
					t.Fatalf("leaked %s: %s", bad, text)
				}
			}
			if format == "table" || format == "wide" {
				for _, literal := range []string{"SUBJECT", "EVIDENCE", "DETAIL", "Reported", "Stale", "prod/chat/engine#0", "advisory", "Unverifiable", "Not authorization", "Convergence unverified", "Source namespace", "ome", "alfred-config", "alfred-recommendations"} {
					if !strings.Contains(text, literal) {
						t.Errorf("missing %q: %s", literal, text)
					}
				}
				if format == "table" {
					for _, line := range strings.Split(text, "\n") {
						if printers.CellDisplayWidth(line) > 80 {
							t.Errorf("line exceeds 80: %s", line)
						}
					}
				} else if !strings.Contains(text, "RawDeploymentMigrationUnsupported") || !strings.Contains(text, "last-known-good") {
					t.Fatalf("wide lacks expanded details: %s", text)
				}
			} else {
				for _, literal := range []string{"cli.ome.io/v1alpha1", "AlfredRecommendationsReport", "SelectedConfigMap", "AlfredReportedCycle", "RawDeploymentMigrationUnsupported", "Unverifiable", "last-cycle.json"} {
					if !strings.Contains(text, literal) {
						t.Errorf("missing %q: %s", literal, text)
					}
				}
				var value map[string]interface{}
				data := out.Bytes()
				if format == "yaml" {
					var err error
					data, err = yaml.YAMLToJSON(data)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := json.Unmarshal(data, &value); err != nil {
					t.Fatal(err)
				}
				delete(value, "collectedAt")
				for _, source := range value["sources"].([]interface{}) {
					delete(source.(map[string]interface{}), "collectedAt")
				}
				structured = append(structured, value)
			}
		})
	}
	if len(structured) != 2 || !reflect.DeepEqual(structured[0], structured[1]) {
		t.Fatalf("JSON/YAML semantic mismatch: %v", structured)
	}
}

func TestValidationBeforeAnyClientOrIO(t *testing.T) {
	for _, args := range [][]string{
		{"recommendations", "extra"}, {"recommendations", "-o", "SECRET"},
		{"recommendations", "--ome-namespace", ""}, {"recommendations", "--ome-namespace", "bad\nSECRET"},
		{"recommendations", "--alfred-namespace", ""}, {"recommendations", "--alfred-namespace", "../secret"},
		{"recommendations", "--alfred-config-name", ""}, {"recommendations", "--alfred-config-name", "../secret"},
		{"recommendations", "--alfred-config-key", ""}, {"recommendations", "--alfred-config-key", "../secret"},
	} {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			client := fake.NewSimpleClientset()
			var out bytes.Buffer
			cmd := NewCmd(factory.Static{Kube: client}, genericiooptions.IOStreams{Out: &out, ErrOut: &bytes.Buffer{}})
			cmd.SilenceErrors = true
			cmd.SilenceUsage = true
			cmd.SetArgs(args)
			err := cmd.Execute()
			if err == nil || len(client.Actions()) != 0 || out.Len() != 0 || exitcode.FromError(err) != 1 {
				t.Fatalf("validation = %v; actions %v; out %s", err, client.Actions(), &out)
			}
			if strings.Contains(err.Error(), "SECRET") {
				t.Fatalf("error leaked: %v", err)
			}
		})
	}
}

func TestNamespacesAndUnavailableOutcomes(t *testing.T) {
	for _, tc := range []struct {
		name, namespace string
		flags           []string
		disabled        bool
	}{
		{name: "default", namespace: "ome"},
		{name: "ome default", namespace: "control", flags: []string{"--ome-namespace", "control"}},
		{name: "alfred override", namespace: "caretaker", flags: []string{"--ome-namespace", "control", "--alfred-namespace", "caretaker"}},
		{name: "disabled", namespace: "ome", disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects := fixtures(tc.namespace)
			if tc.disabled {
				objects[0].(*corev1.ConfigMap).Data["config.yaml"] = "schemaVersion: 1\nrecommendationsConfigMapEnabled: false"
			}
			client := fake.NewSimpleClientset(objects...)
			var out bytes.Buffer
			cmd := NewCmd(factory.Static{Kube: client, NS: "workloads"}, genericiooptions.IOStreams{Out: &out})
			cmd.SetArgs(append([]string{"recommendations", "-o", "json"}, tc.flags...))
			if err := cmd.Execute(); err != nil {
				t.Fatal(err)
			}
			want := 2
			if tc.disabled {
				want = 1
			}
			if len(client.Actions()) != want {
				t.Fatalf("actions %v", client.Actions())
			}
			for _, action := range client.Actions() {
				if action.GetVerb() != "get" || action.GetNamespace() != tc.namespace || action.GetResource().Resource != "configmaps" {
					t.Fatalf("action %v", action)
				}
			}
			if tc.disabled && !strings.Contains(out.String(), `"state": "Disabled"`) {
				t.Fatalf("disabled = %s", &out)
			}
		})
	}
	var out bytes.Buffer
	cmd := NewCmd(factory.Static{Kube: fake.NewSimpleClientset()}, genericiooptions.IOStreams{Out: &out})
	cmd.SetArgs([]string{"recommendations", "-o", "json"})
	if err := cmd.Execute(); err != nil || !strings.Contains(out.String(), `"configState": "NotFound"`) {
		t.Fatalf("unavailable = %v %s", err, &out)
	}
}

type errorWriter struct{}

func (errorWriter) Write([]byte) (int, error) { return 0, errors.New("output unavailable") }

func TestErrorsCancellationAndHelp(t *testing.T) {
	cmd := NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"recommendations"})
	if err := cmd.Execute(); err == nil || err.Error() != "Kubernetes client unavailable" {
		t.Fatalf("client error %v", err)
	}
	for _, format := range []string{"table", "wide", "json", "yaml"} {
		cmd = NewCmd(factory.Static{Kube: fake.NewSimpleClientset(fixtures("ome")...)}, genericiooptions.IOStreams{Out: errorWriter{}, ErrOut: &bytes.Buffer{}})
		cmd.SetArgs([]string{"recommendations", "-o", format})
		if err := cmd.Execute(); err == nil {
			t.Fatalf("%s writer failure ignored", format)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd = NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: &bytes.Buffer{}, ErrOut: &bytes.Buffer{}})
	cmd.SetArgs([]string{"recommendations"})
	if err := cmd.ExecuteContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation = %v", err)
	}
	var out bytes.Buffer
	cmd = NewCmd(factory.Static{}, genericiooptions.IOStreams{Out: &out})
	cmd.SetOut(&out)
	cmd.SetArgs([]string{"recommendations", "--help"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	for _, literal := range []string{"read-only", "--alfred-namespace", "--ome-namespace", "64 KiB", "256 KiB", "800", "200-row", "10-second", "unverifiable"} {
		if !strings.Contains(out.String(), literal) {
			t.Errorf("help lacks %s", literal)
		}
	}
}
