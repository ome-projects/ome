// Command ome-quota-manager is the OME fleet-quota control plane (OEP-0024): it
// assembles the AcceleratorQuota tree, reports each node's position and
// condition, and — in a later increment — renders the tree into Kueue or
// projects per-cluster shares onto members.
//
// It is a separate binary from ome-manager on purpose. Rendering a tree needs
// cluster-wide write on Kueue's ClusterQueue and Cohort, and that grant should
// not ride the ServiceAccount that also runs the InferenceService reconcilers
// and the fail-closed pod mutating webhook. A separate Deployment keeps the
// blast radius of the quota plane to the quota plane, and lets an operator run,
// scale, and roll it independently of serving.
package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"go.uber.org/zap/zapcore"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
	kueuev1beta2 "sigs.k8s.io/kueue/apis/kueue/v1beta2"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/acceleratorquota"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workloadcluster"
	"sigs.k8s.io/ome/pkg/leaderelection"
	kueuebackend "sigs.k8s.io/ome/pkg/quota/backend/kueue"
	quotacert "sigs.k8s.io/ome/pkg/quota/cert"
	"sigs.k8s.io/ome/pkg/quota/tree"
	quotawebhook "sigs.k8s.io/ome/pkg/webhook/admission/acceleratorquota"
)

// transportWakeBuffer is how many transport changes may queue before one is
// dropped. A fleet reconnecting at once is the burst worth absorbing; past that
// a dropped signal costs one resync interval, and the pass it would have
// triggered rebuilds the whole tree regardless.
const transportWakeBuffer = 16

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(v1beta1.AddToScheme(scheme))
	// Unconditional, unlike ome-manager's probe table: Kueue is optional for
	// serving, but this whole binary is opt-in, so registering the types costs
	// nothing when they are unused. Reads go through the uncached reader, so a
	// cluster without the CRDs installed gets a no-match error the caller
	// handles rather than an informer that never syncs.
	utilruntime.Must(kueuev1beta2.AddToScheme(scheme))
}

// options are all supplied by the chart. Every knob is a flag rather than a
// ConfigMap: this binary has a handful of deploy-time decisions and no hot-tunable
// ones, so a flag avoids a ConfigMap read, the RBAC to do it, and a parse layer
// that could disagree with what the chart rendered.
type options struct {
	mode                 string
	metricsAddr          string
	probeAddr            string
	enableLeaderElection bool
	leaderElectionID     string
	// leaderElectionTiming is supplied by the chart, because the margin a leader
	// needs depends on the apiserver latency of the cluster.
	leaderElectionTiming leaderelection.Timing
	// maxTreeDepth bounds a node's distance from the root, in edges. Zero
	// disables the check rather than assuming a bound.
	maxTreeDepth int
	// resyncInterval re-runs the tree checks on an idle plane, so a violation
	// only a clock can clear is noticed without an edit. Zero leaves reconciles
	// purely event-driven.
	resyncInterval time.Duration
	// acceleratorResources are the resource-name suffixes that count as
	// accelerator capacity. Empty disables capacity derivation, and with it the
	// Node and ResourceFlavor reads it needs.
	acceleratorResources string
	// capacityHysteresisPercent damps the derived high-water mark. Zero means no
	// damping rather than a default band.
	capacityHysteresisPercent int
	coverResources            string
	fieldManager              string
	// enableWebhook serves the admission validator. It is separable from the
	// controller because the two fail differently: a webhook outage with
	// failurePolicy=Fail blocks writes, so an operator debugging a wedged
	// cluster needs to be able to run the controller without it.
	enableWebhook  bool
	webhookPort    int
	webhookCertDir string
	// internalCertManagement generates and rotates the webhook serving cert in
	// process, and injects the caBundle into this component's own
	// ValidatingWebhookConfiguration. On by default so the admission path works
	// with no cert-manager in the cluster; turn it off to have cert-manager (or
	// anything else) supply the cert instead.
	internalCertManagement bool
	certNamespace          string
	certSecretName         string
	webhookServiceName     string
	projectionOrigin       string
	projectionFieldManager string
	defaultDistribution    string
	allowExecCredentials   bool
	execCredentialCommands string
	webhookConfigName      string
	caName                 string
	caOrganization         string
	zapOpts                zap.Options
}

func main() {
	opts := options{
		metricsAddr:            ":8080",
		probeAddr:              ":8081",
		enableLeaderElection:   true,
		leaderElectionID:       "ome-quota-manager-leader-lock",
		enableWebhook:          true,
		webhookPort:            9443,
		webhookCertDir:         "/tmp/k8s-webhook-server/serving-certs",
		internalCertManagement: true,
		zapOpts:                zap.Options{TimeEncoder: zapcore.ISO8601TimeEncoder},
	}

	flag.StringVar(&opts.mode, "mode", opts.mode,
		"Which half of the quota plane to run. \"workload\" renders the local AcceleratorQuota "+
			"tree into Kueue on this serving cluster; \"management\" holds the authored fleet tree "+
			"and projects per-cluster shares onto members. Required — a manager runs exactly one "+
			"mode, because the two write different halves of the reserved root's status and "+
			"combining them is a last-writer-wins race.")
	flag.StringVar(&opts.metricsAddr, "metrics-bind-address", opts.metricsAddr,
		"The address the metrics endpoint binds to.")
	flag.StringVar(&opts.probeAddr, "health-probe-addr", opts.probeAddr,
		"The address the health/readiness probe endpoint binds to.")
	flag.BoolVar(&opts.enableLeaderElection, "leader-elect", opts.enableLeaderElection,
		"Elect a leader so exactly one replica reconciles. Keep this on whenever replicas > 1: "+
			"two active instances would race each other's status writes on every node.")
	flag.StringVar(&opts.leaderElectionID, "leader-election-id", opts.leaderElectionID,
		"Name of the leader-election lock.")
	opts.leaderElectionTiming.BindFlags(flag.CommandLine)
	flag.IntVar(&opts.maxTreeDepth, "max-tree-depth", opts.maxTreeDepth,
		"Greatest permitted distance from the root, in edges (the root is 0, a top-tier grouping "+
			"is 1). 0 disables the depth check rather than assuming a bound.")
	flag.StringVar(&opts.acceleratorResources, "accelerator-resources", opts.acceleratorResources,
		"Comma-separated extended resource names that count as accelerator capacity, written in full, "+
			"e.g. \"google.com/tpu,nvidia.com/gpu\". Exact names rather than a pattern, because this list "+
			"decides what every budget is measured against. A vendor left off contributes no capacity, "+
			"which surfaces as CapacityExceeded on any budget written against it rather than passing "+
			"silently. Empty disables capacity derivation entirely: without it nothing distinguishes a "+
			"node's accelerators from its cpu, and there is no safe guess. Workload mode only — a "+
			"management-mode manager has no local silicon to measure.")
	flag.IntVar(&opts.capacityHysteresisPercent, "capacity-hysteresis-percent", opts.capacityHysteresisPercent,
		"How far observed capacity must fall below the recorded high-water mark before the mark follows "+
			"it down. Budget checks read the mark, not the instantaneous value, because capacity dips for "+
			"reasons unrelated to entitlement — a drain, a reboot, a device plugin restarting — and "+
			"following those down would mark budgets Degraded every time a rack was patched. Growth is "+
			"always believed immediately. 0 disables damping.")
	flag.StringVar(&opts.coverResources, "cover-resources", opts.coverResources,
		"Comma-separated resource=quantity pairs every rendered ClusterQueue funds alongside its "+
			"accelerator budget, e.g. \"cpu=1k,memory=1Ti\". Kueue refuses to admit a workload "+
			"requesting a resource its queue does not cover, and every serving pod requests cpu and "+
			"memory, so a queue budgeted only for accelerators admits nothing and reports the reason "+
			"nowhere. These are a ceiling high enough not to be a budget, which is why they are "+
			"configured rather than authored on the CR. Empty disables materialization.")
	flag.StringVar(&opts.fieldManager, "field-manager", opts.fieldManager,
		"Field manager that owns the Kueue objects this process applies, and the value of the "+
			"managed-by label it selects them by. Two managers pointed at one cluster must not share "+
			"it, or each will treat the other's objects as its own. Empty disables materialization.")
	flag.StringVar(&opts.projectionOrigin, "projection-origin", opts.projectionOrigin,
		"Identity this management plane stamps on every AcceleratorQuota it projects onto a member, "+
			"and the value its sweep selects on. It is what tells a copy from a node an admin "+
			"authored locally, so two planes projecting onto one member must not share it or each "+
			"will reap the other's work. No compiled-in default: an unmarked copy is indistinguishable "+
			"from a local node and could never be swept. Empty disables projection. Management mode only.")
	flag.StringVar(&opts.projectionFieldManager, "projection-field-manager", opts.projectionFieldManager,
		"Field manager owning the fields this plane applies on a member. Separate from --field-manager, "+
			"which owns Kueue objects on a workload cluster: the two write different objects on "+
			"different clusters, and sharing a name would make one plane's apply look like the other's. "+
			"Empty disables projection. Management mode only.")
	flag.StringVar(&opts.defaultDistribution, "default-distribution-policy", opts.defaultDistribution,
		"Fleet-wide fallback split for a budget whose node declares no distribution: \"Explicit\" takes "+
			"the admin's perCluster shares, \"Proportional\" divides by each member's reported capacity. "+
			"Empty is not a policy — a budget with nowhere left to fall back to is reported unresolved "+
			"rather than split by a guess. Management mode only.")
	flag.BoolVar(&opts.allowExecCredentials, "allow-exec-credentials", opts.allowExecCredentials,
		"Permit a member's kubeconfig to authenticate through an exec credential plugin. Off by "+
			"default: an exec plugin runs a binary from this process, so it is opt-in and the binary "+
			"must be present in this image. The alternative is a bearer token or client certificate "+
			"inline in the kubeconfig. Management mode only.")
	flag.StringVar(&opts.execCredentialCommands, "exec-credential-allowed-commands", opts.execCredentialCommands,
		"Comma-separated plugin basenames an exec credential may invoke, e.g. "+
			"\"aws,gke-gcloud-auth-plugin,kubelogin\". Empty allows none even with exec enabled, so the "+
			"allowlist is a second deliberate step rather than a consequence of the first.")
	flag.DurationVar(&opts.resyncInterval, "resync-interval", opts.resyncInterval,
		"Re-run the tree checks on an otherwise-idle plane, so a violation only a clock can clear "+
			"is noticed without an edit. 0 leaves reconciles purely event-driven.")
	flag.BoolVar(&opts.enableWebhook, "webhook", opts.enableWebhook,
		"Serve the AcceleratorQuota validating webhook. On by default: it is what turns a "+
			"tree-breaking write into a rejected kubectl apply instead of a Degraded condition "+
			"minutes later. Disable only to run the controller while the webhook's certs or "+
			"Service are broken, since the ValidatingWebhookConfiguration is failurePolicy=Fail.")
	flag.IntVar(&opts.webhookPort, "webhook-port", opts.webhookPort,
		"Port the webhook server binds to.")
	flag.StringVar(&opts.webhookCertDir, "webhook-cert-dir", opts.webhookCertDir,
		"Directory holding tls.crt and tls.key for the webhook server. With internal cert "+
			"management this must be where the cert Secret is mounted: the process writes the "+
			"Secret, and the kubelet projects it back down to this path.")
	flag.BoolVar(&opts.internalCertManagement, "internal-cert-management", opts.internalCertManagement,
		"Generate and rotate the webhook serving cert in process, and inject the caBundle into "+
			"this component's own ValidatingWebhookConfiguration. On by default so admission works "+
			"out of the box with no cert-manager in the cluster. Set false to have cert-manager "+
			"supply the cert, in which case the chart must mount it and annotate the webhook config.")
	// The object names below are derived from the Helm release name, so there is
	// no sensible compiled-in default: one would silently disagree with any
	// release not called what the binary guessed.
	flag.StringVar(&opts.certNamespace, "cert-namespace", opts.certNamespace,
		"Namespace holding the webhook Secret and Service. Required with internal cert management; "+
			"the chart supplies it from the downward API.")
	flag.StringVar(&opts.certSecretName, "cert-secret-name", opts.certSecretName,
		"Secret the generated CA and serving cert are stored in. The chart must create it empty — "+
			"the rotator updates it and never creates it.")
	flag.StringVar(&opts.webhookServiceName, "webhook-service-name", opts.webhookServiceName,
		"Webhook Service name, used to derive the certificate's DNS name.")
	flag.StringVar(&opts.webhookConfigName, "webhook-config-name", opts.webhookConfigName,
		"ValidatingWebhookConfiguration to inject the generated caBundle into.")
	flag.StringVar(&opts.caName, "ca-name", opts.caName,
		"Common name of the generated CA.")
	flag.StringVar(&opts.caOrganization, "ca-organization", opts.caOrganization,
		"Organization of the generated CA.")
	opts.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts.zapOpts)))

	if err := opts.leaderElectionTiming.Validate(); err != nil {
		setupLog.Error(err, "bad flag")
		os.Exit(1)
	}

	mode := acceleratorquota.Mode(opts.mode)
	switch mode {
	case acceleratorquota.ModeWorkload, acceleratorquota.ModeManagement:
	case "":
		setupLog.Error(fmt.Errorf("--mode is required (want %q or %q)",
			acceleratorquota.ModeWorkload, acceleratorquota.ModeManagement), "bad flag")
		os.Exit(1)
	default:
		setupLog.Error(fmt.Errorf("invalid --mode %q (want %q or %q)",
			opts.mode, acceleratorquota.ModeWorkload, acceleratorquota.ModeManagement), "bad flag")
		os.Exit(1)
	}

	ctx := ctrl.SetupSignalHandler()
	kubeConfig := ctrl.GetConfigOrDie()

	certOpts := quotacert.Options{
		Namespace:         opts.certNamespace,
		SecretName:        opts.certSecretName,
		ServiceName:       opts.webhookServiceName,
		WebhookConfigName: opts.webhookConfigName,
		CertDir:           opts.webhookCertDir,
		CAName:            opts.caName,
		CAOrganization:    opts.caOrganization,
	}
	internalCerts := opts.enableWebhook && opts.internalCertManagement
	if internalCerts {
		setupLog.Info("Setting up internal cert management",
			"secret", opts.certSecretName, "service", opts.webhookServiceName,
			"webhookConfig", opts.webhookConfigName)
		// Before the real manager, not on it. The controllers that manager runs
		// write through this component's own fail-closed webhook, so starting
		// them against a webhook whose certificate does not exist yet turns the
		// first seconds into refused writes. This blocks until the cert is on
		// disk and the caBundle is injected.
		if err := quotacert.Bootstrap(ctx, kubeConfig, certOpts, opts.probeAddr); err != nil {
			setupLog.Error(err, "unable to set up internal cert management")
			os.Exit(1)
		}
	}

	mgrOpts := ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: opts.metricsAddr},
		HealthProbeBindAddress: opts.probeAddr,
		LeaderElection:         opts.enableLeaderElection,
		LeaderElectionID:       opts.leaderElectionID,
	}
	opts.leaderElectionTiming.Apply(&mgrOpts)
	if opts.enableWebhook {
		mgrOpts.WebhookServer = webhook.NewServer(webhook.Options{
			Port:    opts.webhookPort,
			CertDir: opts.webhookCertDir,
		})
	}
	mgr, err := ctrl.NewManager(kubeConfig, mgrOpts)
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}
	if internalCerts {
		// Ongoing rotation, and the watch that puts the caBundle back after a
		// chart re-apply resets it to the placeholder.
		if err := quotacert.Manage(mgr, certOpts); err != nil {
			setupLog.Error(err, "unable to set up certificate rotation")
			os.Exit(1)
		}
	}

	setupLog.Info("Setting up AcceleratorQuota controller",
		"mode", mode, "rootName", v1beta1.AcceleratorQuotaRootName,
		"maxTreeDepth", opts.maxTreeDepth, "resyncInterval", opts.resyncInterval)

	r := &acceleratorquota.Reconciler{
		Client:    mgr.GetClient(),
		Scheme:    mgr.GetScheme(),
		Log:       ctrl.Log.WithName("controllers").WithName("AcceleratorQuota"),
		APIReader: mgr.GetAPIReader(),
		Options: tree.Options{
			// Fixed, not configured: the name is part of the CR contract, so a
			// tree authored against one plane reads the same on every other.
			RootName: v1beta1.AcceleratorQuotaRootName,
			MaxDepth: opts.maxTreeDepth,
		},
		ResyncInterval: opts.resyncInterval,
		Mode:           mode,
	}
	// Workload mode only. A management-mode manager holds the authored fleet
	// tree; the nodes it would measure belong to members it reaches over a
	// remote client, and their capacity arrives as status read back from each
	// member's own root rather than as something this process samples.
	if mode == acceleratorquota.ModeWorkload {
		r.Capacity = acceleratorquota.CapacityOptions{
			Resources:         splitAndTrim(opts.acceleratorResources),
			HysteresisPercent: int32(opts.capacityHysteresisPercent),
		}
		if r.Capacity.Enabled() {
			setupLog.Info("Deriving cluster capacity onto the reserved root",
				"resources", r.Capacity.Resources,
				"hysteresisPercent", r.Capacity.HysteresisPercent)
		}

		cover, err := parseCoverResources(opts.coverResources)
		if err != nil {
			setupLog.Error(err, "invalid --cover-resources")
			os.Exit(1)
		}
		backendOpts := kueuebackend.Options{FieldManager: opts.fieldManager, CoverResources: cover}
		if err := backendOpts.Validate(); err != nil {
			setupLog.Error(err, "invalid materialization configuration")
			os.Exit(1)
		}
		if backendOpts.Enabled() {
			r.Materialize = acceleratorquota.MaterializeOptions{
				Backend: &kueuebackend.Backend{
					Writer:  mgr.GetClient(),
					Reader:  mgr.GetAPIReader(),
					Options: backendOpts,
				},
			}
			setupLog.Info("Materializing the quota tree into Kueue",
				"fieldManager", backendOpts.FieldManager,
				"coverResources", opts.coverResources)
		}
	}
	var setupOpts []acceleratorquota.Option
	if mode == acceleratorquota.ModeManagement {
		// The management plane's own transport. Not ome-manager's: a Manager is
		// an in-process struct with no remote surface, so sharing one would mean
		// running this controller inside ome-manager and handing that process
		// the grants a separate binary exists to keep off it.
		clusters := workloadcluster.NewManager(mgr.GetScheme())
		// Cache AcceleratorQuota on every member, and do it before the Connector
		// runs: SetCacheOptions only affects clients built after the call, so a
		// member connected first would keep a never-caching client and its
		// events would never arrive.
		//
		// UNFILTERED, deliberately, where placement scopes its cache to the
		// objects it owns. One selector covers every cached kind, and this plane
		// reads two disjoint sets off a member: its own origin-labelled
		// projections, and that member's reserved root, which the member creates
		// bare and unlabelled. An origin-scoped cache would hide the root, and
		// the capacity read would start returning NotFound -- which the projector
		// treats as "starting up" and skips, silently emptying the basis. The
		// kind is a handful of objects per cluster, so caching all of it is
		// cheaper than the selector that would go wrong.
		clusters.SetCacheOptions(workloadcluster.CacheOptions{
			CachedKinds: sets.New(v1beta1.SchemeGroupVersion.WithKind("AcceleratorQuota").GroupKind()),
		})
		clusters.SetExecCredentialPolicy(workloadcluster.ExecCredentialPolicy{
			Allowed:         opts.allowExecCredentials,
			AllowedCommands: splitAndTrim(opts.execCredentialCommands),
		})
		if err := mgr.Add(clusters); err != nil {
			setupLog.Error(err, "unable to add the cluster transport")
			os.Exit(1)
		}

		// Follows the registry and never writes it. WorkloadCluster status has
		// one writer, in ome-manager, whose grace and backoff state is in memory
		// rather than leased, so a second one would flap the condition between
		// two processes and lose the argument silently.
		// Buffered because the send is non-blocking: the transport must not wait
		// on a projector that may be mid-pass against the very member that just
		// changed. Depth covers a fleet reconnecting at once; past that a
		// dropped signal costs one resync interval, and the pass it would have
		// triggered rebuilds the whole tree anyway.
		transportChanged := make(chan event.GenericEvent, transportWakeBuffer)
		connector := &acceleratorquota.Connector{
			Client:   mgr.GetClient(),
			Log:      ctrl.Log.WithName("AcceleratorQuotaConnector"),
			Clusters: clusters,
			Changed:  transportChanged,
			Root:     v1beta1.AcceleratorQuotaRootName,
		}
		if err := connector.SetupWithManager(mgr); err != nil {
			setupLog.Error(err, "unable to create the cluster connector")
			os.Exit(1)
		}

		// Every AcceleratorQuota change on a member wakes the hub: a projection's
		// status carries the consumption figures the fleet view is made of, and
		// the member's own root carries the capacity a proportional split
		// divides by. Both move without anything on the hub changing.
		funnel := workloadcluster.NewStatusFunnel(clusters, workloadcluster.FunnelConfig{
			NewList:   func() client.ObjectList { return &v1beta1.AcceleratorQuotaList{} },
			NewObject: func() client.Object { return &v1beta1.AcceleratorQuota{} },
			// Resolved to the root rather than to the object that moved. The pass
			// rebuilds the whole tree whatever woke it, so naming one key lets a
			// burst of members reporting at once collapse into a single request.
			Resolve: func(client.Object) (types.NamespacedName, bool) {
				return types.NamespacedName{Name: v1beta1.AcceleratorQuotaRootName}, true
			},
		})
		if err := mgr.Add(funnel); err != nil {
			setupLog.Error(err, "unable to add the member status funnel")
			os.Exit(1)
		}

		setupOpts = append(setupOpts,
			acceleratorquota.WithTransportEvents(transportChanged),
			acceleratorquota.WithMemberEvents(funnel.Events()),
		)

		r.Project = acceleratorquota.ProjectOptions{
			Clusters:      clusters,
			Origin:        opts.projectionOrigin,
			FieldManager:  opts.projectionFieldManager,
			DefaultPolicy: v1beta1.AcceleratorQuotaDistributionPolicy(opts.defaultDistribution),
		}
		if r.Project.Enabled() {
			setupLog.Info("Projecting the quota tree onto workload clusters",
				"origin", r.Project.Origin,
				"fieldManager", r.Project.FieldManager,
				"defaultDistributionPolicy", r.Project.DefaultPolicy)
		} else {
			// Absent config disables rather than assuming, as everywhere else
			// here: without an origin a copy could not be told from a local
			// node, and nothing could ever sweep it.
			setupLog.Info("Projection is off: set --projection-origin and --projection-field-manager to enable it")
		}
	}

	if err := r.SetupWithManager(mgr, setupOpts...); err != nil {
		setupLog.Error(err, "unable to create the AcceleratorQuota controller")
		os.Exit(1)
	}

	readyz := healthz.Ping
	if opts.enableWebhook {
		setupLog.Info("Registering AcceleratorQuota validating webhook")
		// Registered before the manager starts, so the webhook server joins the
		// runnable group controller-runtime brings up ahead of the controllers.
		// The same Options the controller uses: admission and reconcile must
		// agree on what a valid tree is, or a write the webhook admits goes
		// straight to Degraded.
		mgr.GetWebhookServer().Register("/validate-ome-io-v1beta1-acceleratorquota", &webhook.Admission{
			Handler: &quotawebhook.Validator{
				Client:  mgr.GetAPIReader(),
				Decoder: admission.NewDecoder(mgr.GetScheme()),
				Options: r.Options,
			},
		})
		// Ready means the apiserver can actually reach this replica: the webhook
		// Service routes only to ready pods, and the configuration is
		// failurePolicy=Fail, so reporting ready before the listener is up turns
		// the first quota writes into TLS errors instead of decisions.
		readyz = func(req *http.Request) error {
			return mgr.GetWebhookServer().StartedChecker()(req)
		}
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up the health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", readyz); err != nil {
		setupLog.Error(err, "unable to set up the readiness check")
		os.Exit(1)
	}

	setupLog.Info("Starting ome-quota-manager", "mode", mode)
	if err := mgr.Start(ctx); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

// parseCoverResources reads "cpu=1k,memory=1Ti" into quantities. An empty list
// is not an error: it is how an operator says materialization is off.
func parseCoverResources(csv string) (map[corev1.ResourceName]resource.Quantity, error) {
	pairs := splitAndTrim(csv)
	if len(pairs) == 0 {
		return nil, nil
	}
	out := make(map[corev1.ResourceName]resource.Quantity, len(pairs))
	for _, pair := range pairs {
		name, value, ok := strings.Cut(pair, "=")
		if !ok {
			return nil, fmt.Errorf("cover resource %q is not name=quantity", pair)
		}
		name = strings.TrimSpace(name)
		qty, err := resource.ParseQuantity(strings.TrimSpace(value))
		if err != nil {
			return nil, fmt.Errorf("cover resource %q: %w", name, err)
		}
		out[corev1.ResourceName(name)] = qty
	}
	return out, nil
}

// splitAndTrim turns a comma-separated flag into a slice, dropping blanks so a
// trailing comma or a quoted empty string disables derivation rather than
// configuring a suffix that matches every resource name.
func splitAndTrim(csv string) []string {
	var out []string
	for _, part := range strings.Split(csv, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}
