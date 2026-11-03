package main

import (
	"reflect"
	"runtime"
	"strings"
	"testing"
)

func TestWebhookOnlyRejectsControllerLeadership(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options Options
		valid   bool
	}{
		{"disabled server", Options{webhookOnly: true}, false},
		{"controller leadership", Options{webhookOnly: true, enableWebhook: true, enableLeaderElection: true}, false},
		{"admission only", Options{webhookOnly: true, enableWebhook: true}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := (&startup{options: tc.options}).validateWebhookOnlyOptions(); got != tc.valid {
				t.Fatalf("valid=%v, want %v", got, tc.valid)
			}
		})
	}
}

func TestWebhookOnlyStartupRegistersAdmissionWithoutControllers(t *testing.T) {
	s := &startup{}
	var names []string
	for _, step := range s.webhookOnlySteps() {
		names = append(names, runtime.FuncForPC(reflect.ValueOf(step).Pointer()).Name())
	}
	joined := strings.Join(names, "\n")
	for _, forbidden := range []string{"setupInferenceServiceController", "setupInferenceReplicaController", "setupMultiClusterControllers", "startEventBroadcaster", "addControllerIdentityCheck"} {
		if strings.Contains(joined, forbidden) {
			t.Fatalf("webhook-only startup registers %s", forbidden)
		}
	}
	for _, required := range []string{"loadDeployConfig", "loadIngressConfig", "buildRuntimeSelector", "registerModelWebhooks", "registerPodAndRuntimeWebhooks", "registerInferenceServiceWebhook"} {
		if !strings.Contains(joined, required) {
			t.Fatalf("webhook-only startup omits %s", required)
		}
	}
}

func TestWebhookOnlyFlagsAndCertificateDirectory(t *testing.T) {
	withArgs(t, "manager", "--webhook", "--webhook-only", "--webhook-cert-dir=/tmp/test-certificates")
	options := GetOptions()
	if !options.webhookOnly || options.webhookCertDir != "/tmp/test-certificates" {
		t.Fatal("webhook-only flags not applied")
	}
}
