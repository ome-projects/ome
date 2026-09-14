package scheduling

import (
	"reflect"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestSelectResolvesExactSchedulerProfile(t *testing.T) {
	profiles := Config{Profiles: map[string]Profile{
		corev1.DefaultSchedulerName: {
			Backend: "kube-v131", SchedulerVersion: "v1.31.2", ConfigurationID: "sha256:default",
		},
		"custom-gang": {
			Backend: "custom-gang-v030", SchedulerVersion: "v0.30.1", ConfigurationID: "sha256:gang", GangScheduling: true,
		},
	}}

	tests := []struct {
		name        string
		templates   []corev1.PodTemplateSpec
		requireGang bool
		wantName    string
		wantBackend string
		wantStatus  SelectionStatus
		wantReason  SelectionReason
	}{
		{
			name:      "omitted name uses Kubernetes default",
			templates: []corev1.PodTemplateSpec{{ObjectMeta: metav1.ObjectMeta{Name: "replacement"}}},
			wantName:  corev1.DefaultSchedulerName, wantBackend: "kube-v131",
			wantStatus: SelectionReady, wantReason: SelectionReasonProfileSelected,
		},
		{
			name:        "template scheduler selects custom profile",
			templates:   []corev1.PodTemplateSpec{{Spec: corev1.PodSpec{SchedulerName: "custom-gang"}}},
			requireGang: true, wantName: "custom-gang", wantBackend: "custom-gang-v030",
			wantStatus: SelectionReady, wantReason: SelectionReasonProfileSelected,
		},
		{
			name: "mixed schedulers",
			templates: []corev1.PodTemplateSpec{
				{Spec: corev1.PodSpec{SchedulerName: "custom-gang"}},
				{Spec: corev1.PodSpec{SchedulerName: corev1.DefaultSchedulerName}},
			},
			wantName: "custom-gang", wantStatus: SelectionUnavailable, wantReason: SelectionReasonMixedSchedulers,
		},
		{
			name:      "unknown scheduler",
			templates: []corev1.PodTemplateSpec{{Spec: corev1.PodSpec{SchedulerName: "other-scheduler"}}},
			wantName:  "other-scheduler", wantStatus: SelectionUnavailable, wantReason: SelectionReasonProfileNotConfigured,
		},
		{
			name:       "missing templates",
			wantStatus: SelectionUnavailable, wantReason: SelectionReasonNoTemplates,
		},
		{
			name:      "already bound template",
			templates: []corev1.PodTemplateSpec{{Spec: corev1.PodSpec{NodeName: "gpu-a"}}},
			wantName:  corev1.DefaultSchedulerName, wantStatus: SelectionUnavailable, wantReason: SelectionReasonTemplateBound,
		},
		{
			name:      "gang unsupported",
			templates: []corev1.PodTemplateSpec{{}}, requireGang: true,
			wantName: corev1.DefaultSchedulerName, wantBackend: "kube-v131",
			wantStatus: SelectionUnsupported, wantReason: SelectionReasonGangUnsupported,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var before []corev1.PodTemplateSpec
			if tc.templates != nil {
				before = make([]corev1.PodTemplateSpec, len(tc.templates))
			}
			for i := range tc.templates {
				before[i] = *tc.templates[i].DeepCopy()
			}

			got := Select(profiles, tc.templates, tc.requireGang)

			if got.SchedulerName != tc.wantName || got.Status != tc.wantStatus || got.Reason != tc.wantReason {
				t.Fatalf("Select() = name %q status %q reason %q, want %q %q %q", got.SchedulerName, got.Status, got.Reason, tc.wantName, tc.wantStatus, tc.wantReason)
			}
			if got.Profile.Backend != tc.wantBackend {
				t.Fatalf("Select() backend = %q, want %q", got.Profile.Backend, tc.wantBackend)
			}
			if !reflect.DeepEqual(tc.templates, before) {
				t.Fatalf("Select mutated templates: got %#v, want %#v", tc.templates, before)
			}
		})
	}
}

func TestConfigValidateRejectsInvalidProfiles(t *testing.T) {
	valid := Profile{Backend: "kube-v131", SchedulerVersion: "v1.31.2", ConfigurationID: "sha256:config"}
	tests := []struct {
		name    string
		config  Config
		wantErr string
	}{
		{name: "empty scheduler name", config: Config{Profiles: map[string]Profile{"": valid}}, wantErr: "scheduler name"},
		{name: "invalid scheduler name", config: Config{Profiles: map[string]Profile{"Not_A_DNS_Name": valid}}, wantErr: "scheduler name"},
		{name: "padded scheduler name", config: Config{Profiles: map[string]Profile{" custom-gang ": valid}}, wantErr: "scheduler name"},
		{name: "blank backend", config: Config{Profiles: map[string]Profile{"custom-gang": {SchedulerVersion: "v1", ConfigurationID: "config"}}}, wantErr: "backend"},
		{name: "padded backend", config: Config{Profiles: map[string]Profile{"custom-gang": {Backend: " simulator ", SchedulerVersion: "v1", ConfigurationID: "config"}}}, wantErr: "backend"},
		{name: "blank scheduler version", config: Config{Profiles: map[string]Profile{"custom-gang": {Backend: "simulator", ConfigurationID: "config"}}}, wantErr: "schedulerVersion"},
		{name: "padded configuration id", config: Config{Profiles: map[string]Profile{"custom-gang": {Backend: "simulator", SchedulerVersion: "v1", ConfigurationID: " config "}}}, wantErr: "configurationID"},
	}

	if err := (Config{}).Validate(); err != nil {
		t.Fatalf("empty configuration should be valid: %v", err)
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.config.Validate()
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("Validate() error = %v, want mention of %q", err, tc.wantErr)
			}
		})
	}
}

func TestSelectTreatsUnvalidatedProfileAsUnavailable(t *testing.T) {
	got := Select(Config{Profiles: map[string]Profile{
		corev1.DefaultSchedulerName: {SchedulerVersion: "v1", ConfigurationID: "config"},
	}}, []corev1.PodTemplateSpec{{}}, false)
	if got.Status != SelectionUnavailable || got.Reason != SelectionReasonProfileInvalid {
		t.Fatalf("Select() = status %q reason %q, want unavailable invalid profile", got.Status, got.Reason)
	}
}
