package constants

import (
	"strings"
	"testing"

	"k8s.io/apimachinery/pkg/util/validation"
)

func TestLWSNameTruncates(t *testing.T) {
	tests := []struct {
		name              string
		componentName     string
		expectedTruncated bool
	}{
		{
			name:              "short component name",
			componentName:     "my-model-engine",
			expectedTruncated: false,
		},
		{
			name:              "actual long engine component name",
			componentName:     "amaaaaaabgjpxjqamiuior4qamufon2clgneukbxomingadlfcsgq67sicoa-engine",
			expectedTruncated: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const (
				maxLabelValueLength        = 63
				maxLWSNameLength           = 35
				lwsNamePrefix              = "lws-"
				workerStatefulSetSuffix    = "-0"
				statefulSetRevisionHash    = "75965d8c9f"
				kueueLeaderWorkerSetPrefix = "leaderworkerset-"
				kueueWorkloadHashSuffix    = "-ece5c"
			)

			got := LWSName(tt.componentName)
			workerStatefulSetName := got + workerStatefulSetSuffix
			revisionLabelValue := workerStatefulSetName + "-" + statefulSetRevisionHash
			kueueWorkloadLabelValue := kueueLeaderWorkerSetPrefix + got + workerStatefulSetSuffix + kueueWorkloadHashSuffix
			expected := TruncateNameWithPrefix(tt.componentName, lwsNamePrefix, maxLWSNameLength)

			if len(got) > maxLWSNameLength {
				t.Fatalf("LWSName length = %d, want <= %d: %q", len(got), maxLWSNameLength, got)
			}
			if !strings.HasPrefix(got, lwsNamePrefix) {
				t.Fatalf("LWSName = %q, want prefix %q", got, lwsNamePrefix)
			}
			if tt.expectedTruncated && got == lwsNamePrefix+tt.componentName {
				t.Fatalf("LWSName = %q, want truncated name", got)
			}
			if len(revisionLabelValue) > maxLabelValueLength {
				t.Fatalf("StatefulSet revision label value length = %d, want <= %d: %q", len(revisionLabelValue), maxLabelValueLength, revisionLabelValue)
			}
			if len(kueueWorkloadLabelValue) > maxLabelValueLength {
				t.Fatalf("Kueue workload label value length = %d, want <= %d: %q", len(kueueWorkloadLabelValue), maxLabelValueLength, kueueWorkloadLabelValue)
			}
			if got != expected {
				t.Fatalf("LWSName = %q, want %q", got, expected)
			}
		})
	}
}

// ServiceHostname carries the configured cluster domain and, without one,
// renders the namespaced short name rather than a suffix of its own.
func TestServiceHostname(t *testing.T) {
	cases := []struct{ name, namespace, clusterDomain, want string }{
		{"svc-engine", "team-a", "cluster.local", "svc-engine.team-a.svc.cluster.local"},
		{"svc-engine", "team-a", "example.org", "svc-engine.team-a.svc.example.org"},
		{"svc-engine", "team-a", "", "svc-engine.team-a"},
	}
	for _, tc := range cases {
		if got := ServiceHostname(tc.name, tc.namespace, tc.clusterDomain); got != tc.want {
			t.Errorf("ServiceHostname(%q, %q, %q) = %q, want %q", tc.name, tc.namespace, tc.clusterDomain, got, tc.want)
		}
	}
}

// A service's model ConfigMap is named after the whole service name, so two
// services never share one, and the name stays within the length Kubernetes
// allows a ConfigMap name.
func TestModelConfigNameCarriesTheWholeServiceName(t *testing.T) {
	const prefix = "modelconfig-"
	longest := validation.DNS1123SubdomainMaxLength - len(prefix)
	for _, service := range []string{
		"chat",
		"llama-70b-instruct-chat-blue",
		strings.Repeat("a", longest-1),
		strings.Repeat("a", longest),
	} {
		if got, want := ModelConfigName(service), prefix+service; got != want {
			t.Errorf("ModelConfigName of a %d-character name = %q, want %q", len(service), got, want)
		}
	}
	blue, green := ModelConfigName("blue-llama-70b-instruct-chat-v2"), ModelConfigName("green-llama-70b-instruct-chat-v2")
	if blue == green {
		t.Fatalf("two services whose names share a long tail got one ConfigMap: %q", blue)
	}
	over := ModelConfigName(strings.Repeat("a", longest+1))
	if errs := validation.IsDNS1123Subdomain(over); len(errs) > 0 {
		t.Fatalf("ModelConfigName of a name over the limit = %q: %v", over, errs)
	}
	if !strings.HasPrefix(over, prefix) || over == ModelConfigName(strings.Repeat("a", longest)) {
		t.Fatalf("ModelConfigName of a name over the limit = %q, want the prefix and a name of its own", over)
	}
}
