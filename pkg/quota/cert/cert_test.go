package cert

import (
	"context"
	"strings"
	"testing"
	"time"

	"k8s.io/client-go/rest"
)

func complete() Options {
	return Options{
		Namespace:         "ome",
		SecretName:        "quota-webhook-cert",
		ServiceName:       "quota-webhook",
		WebhookConfigName: "quota.ome.io",
		CertDir:           "/tmp/k8s-webhook-server/serving-certs",
		CAName:            "quota-ca",
		CAOrganization:    "ome",
	}
}

// The certificate covers the fully qualified Service name only under the
// cluster domain the chart passes; without one it covers the name the
// apiserver dials and nothing the binary made up.
func TestRotatorCoversTheFullyQualifiedNameOnlyWithAClusterDomain(t *testing.T) {
	withDomain := complete()
	withDomain.ClusterDomain = "cluster.local"
	r := newRotator(withDomain, "test", make(chan struct{}))
	if r.DNSName != "quota-webhook.ome.svc" {
		t.Fatalf("DNSName = %q, want quota-webhook.ome.svc", r.DNSName)
	}
	if len(r.ExtraDNSNames) != 1 || r.ExtraDNSNames[0] != "quota-webhook.ome.svc.cluster.local" {
		t.Fatalf("ExtraDNSNames = %v, want [quota-webhook.ome.svc.cluster.local]", r.ExtraDNSNames)
	}
	r = newRotator(complete(), "test", make(chan struct{}))
	if r.DNSName != "quota-webhook.ome.svc" || len(r.ExtraDNSNames) != 0 {
		t.Fatalf("without a cluster domain: DNSName = %q ExtraDNSNames = %v, want quota-webhook.ome.svc and none", r.DNSName, r.ExtraDNSNames)
	}
}

// Every name is supplied by the chart, so a missing one is a wiring bug that has
// to surface at startup: the rotator would otherwise generate a cert nobody can
// find, or inject a caBundle into nothing, and the webhook would just hang
// unready with no explanation.
func TestOptionsValidate(t *testing.T) {
	tests := []struct {
		name    string
		mutate  func(*Options)
		wantErr string
	}{
		{
			name:   "a fully specified set is accepted",
			mutate: func(*Options) {},
		},
		{
			name:    "namespace is required",
			mutate:  func(o *Options) { o.Namespace = "" },
			wantErr: "namespace is required",
		},
		{
			name:    "secret name is required",
			mutate:  func(o *Options) { o.SecretName = "" },
			wantErr: "secret name is required",
		},
		{
			name:    "service name is required",
			mutate:  func(o *Options) { o.ServiceName = "" },
			wantErr: "service name is required",
		},
		{
			name:    "webhook config name is required",
			mutate:  func(o *Options) { o.WebhookConfigName = "" },
			wantErr: "webhook config name is required",
		},
		{
			name:    "cert directory is required",
			mutate:  func(o *Options) { o.CertDir = "" },
			wantErr: "cert directory is required",
		},
		{
			name:    "CA name is required",
			mutate:  func(o *Options) { o.CAName = "" },
			wantErr: "CA name is required",
		},
		{
			name:    "CA organization is required",
			mutate:  func(o *Options) { o.CAOrganization = "" },
			wantErr: "CA organization is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			opts := complete()
			tc.mutate(&opts)

			err := opts.validate()
			switch {
			case tc.wantErr == "" && err != nil:
				t.Fatalf("validate() = %v, want no error", err)
			case tc.wantErr != "" && err == nil:
				t.Fatalf("validate() = nil, want an error naming %q", tc.wantErr)
			case tc.wantErr != "" && !strings.Contains(err.Error(), tc.wantErr):
				t.Errorf("validate() = %q, want it to name %q", err, tc.wantErr)
			}
		})
	}
}

// Both entry points must reject before they touch anything. Half-registering a
// rotator leaves the manager with a Runnable that can never succeed, and a
// bootstrap pass that got as far as building a manager would hold the health
// probe port open on the way out.
func TestEntryPointsRejectIncompleteOptions(t *testing.T) {
	opts := complete()
	opts.SecretName = ""

	// The nil manager and nil rest.Config are safe precisely because validation
	// runs first; if that ordering ever changes, these panic rather than passing
	// quietly.
	if err := Manage(nil, opts); err == nil {
		t.Error("Manage() = nil, want an error for the missing secret name")
	}
	if err := Bootstrap(context.Background(), nil, opts, ""); err == nil {
		t.Error("Bootstrap() = nil, want an error for the missing secret name")
	}
}

// Bootstrap blocks the whole startup path, so a bootstrap that cannot succeed
// has to come back as an error. The readiness channel is closed by the rotator
// and by nothing else, so waiting on it alone would leave a process whose
// rotator has already given up — an apiserver it cannot reach, a Secret the
// chart never created — hanging unready forever instead of exiting and letting
// the pod restart.
func TestBootstrapReportsAGivenUpRotator(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	returned := make(chan error, 1)
	go func() {
		// Nothing is listening on port 1, so the rotator fails its first read
		// and stops the manager under it.
		returned <- Bootstrap(ctx, &rest.Config{Host: "127.0.0.1:1"}, complete(), "")
	}()

	select {
	case err := <-returned:
		if err == nil {
			t.Fatal("Bootstrap() = nil, want the rotator's failure to surface")
		}
	case <-ctx.Done():
		t.Fatal("Bootstrap() blocked on a readiness channel its rotator will never close")
	}
}
