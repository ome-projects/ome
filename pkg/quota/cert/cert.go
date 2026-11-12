// Package cert self-manages the webhook serving certificate.
//
// The component generates its own CA and serving cert, stores them in a Secret,
// and injects the caBundle into its own ValidatingWebhookConfiguration — so the
// admission path works out of the box with no cert-manager in the cluster and no
// Issuer or Certificate in the chart. Rotation is handled on a timer thereafter.
// This mirrors what Kueue does by default, which matters because the quota plane
// is usually installed alongside Kueue and an operator should not need a second
// cert story for it.
//
// Delivery to the webhook server is via the kubelet, not this process: the
// Secret is mounted at CertDir, so a generated cert becomes servable only once
// the kubelet re-projects it. That projection is the slow step, and it is why
// certificate setup is a phase of its own — Bootstrap runs it on a throwaway
// manager before the real one exists, and Manage then keeps it fresh.
//
// The trade is a real one and worth naming: self-injection requires write access
// to the ValidatingWebhookConfiguration this component owns. Options scopes that
// as narrowly as the library allows — one named webhook config, no mutating
// webhooks — but list/watch on the kind cannot be narrowed, because the rotator
// builds an unfiltered informer over it.
package cert

import (
	"context"
	"errors"
	"fmt"

	rotator "github.com/open-policy-agent/cert-controller/pkg/rotator"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/ome/pkg/constants"
)

// Both rotators run in one process, and controller-runtime's controller-name
// registry is process-global — a shared name is a hard registration error, not
// two controllers sharing a metric.
const (
	bootstrapControllerName = "cert-rotator-bootstrap"
	rotationControllerName  = "cert-rotator"
)

// Options names the objects the rotator touches. Every one is supplied by the
// caller rather than defaulted here: the chart derives them from the release
// name, so a compiled-in default would silently disagree with any release not
// called what this package guessed.
type Options struct {
	// Namespace holds the Secret and the webhook Service. Required — the
	// rotator cannot build a Secret key without it.
	Namespace string
	// SecretName is the Secret the CA and serving cert are stored in. The chart
	// must create it empty: the rotator updates, and never creates, so a missing
	// Secret is a permanent startup failure rather than something it recovers
	// from.
	SecretName string
	// ServiceName is the webhook Service, used to derive the cert's DNS name.
	ServiceName string
	// WebhookConfigName is the ValidatingWebhookConfiguration to inject the
	// caBundle into.
	WebhookConfigName string
	// CertDir is where the webhook server reads the serving cert. Nothing in
	// this process writes it: the rotator writes the Secret and the kubelet
	// projects it back down, so this must be the Secret's mount path.
	CertDir string
	// CAName and CAOrganization identify the generated CA in its subject.
	CAName         string
	CAOrganization string
	// ClusterDomain is the cluster's DNS domain ("cluster.local" on most
	// clusters). The certificate then also covers the fully qualified
	// "<service>.<namespace>.svc.<ClusterDomain>" name. Empty covers only
	// "<service>.<namespace>.svc", the name the apiserver dials; nothing in
	// the code supplies a default.
	ClusterDomain string
}

func (o Options) validate() error {
	for _, f := range []struct{ name, value string }{
		{"namespace", o.Namespace},
		{"secret name", o.SecretName},
		{"service name", o.ServiceName},
		{"webhook config name", o.WebhookConfigName},
		{"cert directory", o.CertDir},
		{"CA name", o.CAName},
		{"CA organization", o.CAOrganization},
	} {
		if f.value == "" {
			return fmt.Errorf("internal cert management: %s is required", f.name)
		}
	}
	return nil
}

// newRotator builds the rotator both phases share, so the bootstrap pass and the
// long-lived one cannot drift on DNS names, key usages or readiness semantics.
func newRotator(opts Options, controllerName string, certsReady chan struct{}) *rotator.CertRotator {
	return &rotator.CertRotator{
		SecretKey: types.NamespacedName{
			Namespace: opts.Namespace,
			Name:      opts.SecretName,
		},
		CertDir:        opts.CertDir,
		CAName:         opts.CAName,
		CAOrganization: opts.CAOrganization,
		DNSName:        fmt.Sprintf("%s.%s.svc", opts.ServiceName, opts.Namespace),
		ExtraDNSNames:  extraDNSNames(opts),
		IsReady:        certsReady,
		ControllerName: controllerName,
		Webhooks: []rotator.WebhookInfo{{
			Type: rotator.Validating,
			Name: opts.WebhookConfigName,
		}},
		// Hold the caBundle injection until the serving cert is readable at
		// CertDir: a caBundle published before the kubelet has projected the
		// matching key advertises a CA this pod cannot yet present a cert for,
		// so the apiserver dials and fails the handshake. The hold does not
		// make the Secret single-writer: the library's Secret reconciler
		// refreshes it a second time whenever its informer still trails the
		// startup pass's write, loses the optimistic lock, logs it and retries
		// until the informer catches up. Bootstrap ends with one usable
		// certificate either way.
		EnableReadinessCheck: true,
		// Every replica serves admission, so every replica needs the cert on
		// disk — gating rotation on leadership would leave the followers unable
		// to answer the apiserver.
		RequireLeaderElection: false,
	}
}

// extraDNSNames is the fully qualified Service name the certificate covers
// besides "<service>.<namespace>.svc": none without a cluster domain.
func extraDNSNames(opts Options) []string {
	if opts.ClusterDomain == "" {
		return nil
	}
	return []string{constants.ServiceHostname(opts.ServiceName, opts.Namespace, opts.ClusterDomain)}
}

// Bootstrap generates the serving cert and injects the caBundle, and blocks
// until both have happened.
//
// It runs on a throwaway manager of its own, before the component's real manager
// is built, and that ordering is the whole point. The rotator is a manager
// Runnable, so hosting it on the manager that also runs the controllers starts
// those controllers against a webhook that is not yet serving — and this
// component's own writes go through that webhook under failurePolicy=Fail, so
// the first seconds become a burst of refused writes and retry noise. One extra
// apiserver connection at startup removes the window.
//
// healthProbeAddr is bound for the duration so the liveness probe answers while
// the certificate is being generated; pass "" to leave it unbound. Either way
// the listener is released before this returns, because the real manager binds
// the same address and controller-runtime opens it at construction time.
func Bootstrap(ctx context.Context, restConfig *rest.Config, opts Options, healthProbeAddr string) error {
	if err := opts.validate(); err != nil {
		return err
	}
	log := ctrl.Log.WithName("cert-bootstrap")

	mgr, err := ctrl.NewManager(restConfig, ctrl.Options{
		// No metrics: the real manager owns that endpoint, and binding it twice
		// in one process fails on the second bind. No leader election either —
		// every replica serves admission, so every replica needs its own cert.
		Metrics:                metricsserver.Options{BindAddress: "0"},
		HealthProbeBindAddress: healthProbeAddr,
	})
	if err != nil {
		return fmt.Errorf("internal cert management: building the bootstrap manager: %w", err)
	}
	if healthProbeAddr != "" {
		if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
			return fmt.Errorf("internal cert management: bootstrap health check: %w", err)
		}
	}

	certsReady := make(chan struct{})
	if err := rotator.AddRotator(mgr, newRotator(opts, bootstrapControllerName, certsReady)); err != nil {
		return fmt.Errorf("internal cert management: %w", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var runErr error
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		runErr = mgr.Start(runCtx)
	}()

	log.Info("generating the webhook certificate and injecting the caBundle",
		"secret", opts.SecretName, "webhookConfig", opts.WebhookConfigName)
	select {
	case <-certsReady:
	case <-stopped:
		// The rotator gave up: a Secret the chart never created, or a cert that
		// never reached CertDir. Report it rather than waiting on a channel
		// nothing will close now, so the pod crashloops instead of hanging
		// unready with no explanation.
		if runErr == nil {
			runErr = errors.New("the bootstrap manager stopped before the certificate was ready")
		}
		return fmt.Errorf("internal cert management: %w", runErr)
	case <-ctx.Done():
		return ctx.Err()
	}

	cancel()
	<-stopped
	log.Info("webhook certificate ready")
	return nil
}

// Manage registers the long-lived rotator with the component's real manager.
//
// Bootstrap has already produced a usable certificate by the time this runs.
// What this adds is the periodic rotation check and the watch that restores the
// caBundle after a chart re-apply resets it to the placeholder.
func Manage(mgr ctrl.Manager, opts Options) error {
	if err := opts.validate(); err != nil {
		return err
	}
	// The rotator closes this once it has confirmed what Bootstrap already
	// established, so nothing waits on it; it exists because the library closes
	// the channel unconditionally.
	return rotator.AddRotator(mgr, newRotator(opts, rotationControllerName, make(chan struct{})))
}
