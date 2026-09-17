// Package statusrepair is the operator-side, non-resident break-glass repair
// of one InferenceReplica's stored per-Instance representation. It reads the
// live object directly, without decoding the payload it is about to replace,
// and hands an independently validated DenseV1 replacement to the single
// status writer's repair entry point. It is dry-run unless asked to apply.
package statusrepair

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferencereplica"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/transitionpreflight"
)

// Options names the object to repair and the replacement to install.
type Options struct {
	// InventoryPath is the operator's fleet inventory; it supplies the
	// cluster's kubeconfig context, the manager configuration location, and
	// the omenativeStatus block the cluster is expected to run.
	InventoryPath string
	// Cluster is the inventory cluster name.
	Cluster string
	// Namespace and Name identify the InferenceReplica.
	Namespace string
	Name      string
	// ReplacementPath is the YAML or JSON document whose only key is
	// instanceStatuses, the complete DenseV1 row set to install.
	ReplacementPath string
	// Apply performs the write; without it the command only reports what it
	// would write.
	Apply bool
}

// Clients are the direct clients the repair uses against one cluster.
type Clients struct {
	// Kube reads the manager configuration ConfigMap.
	Kube kubernetes.Interface
	// Client is an uncached controller-runtime client for the live read and
	// the single status write.
	Client client.Client
}

// Connector opens the clients for one inventory cluster.
type Connector func(ctx context.Context, cluster transitionpreflight.Cluster) (*Clients, error)

// Result is the repair report.
type Result struct {
	InventoryDigest string
	Cluster         transitionpreflight.Cluster
	Namespace       string
	Name            string
	Config          controllerconfig.OMENativeStatusConfig
	Outcome         inferencereplica.InstanceStatusRepairOutcome
}

// ConnectWithKubeconfig opens the clients from the explicit kubeconfig path
// and context the inventory names; the ambient kubeconfig and its current
// context never select a cluster.
func ConnectWithKubeconfig(_ context.Context, cluster transitionpreflight.Cluster) (*Clients, error) {
	loading := &clientcmd.ClientConfigLoadingRules{ExplicitPath: cluster.Kubeconfig}
	overrides := &clientcmd.ConfigOverrides{CurrentContext: cluster.Context}
	restConfig, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(loading, overrides).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("load kubeconfig %s context %q: %w", cluster.Kubeconfig, cluster.Context, err)
	}
	kube, err := kubernetes.NewForConfig(restConfig)
	if err != nil {
		return nil, fmt.Errorf("build Kubernetes client: %w", err)
	}
	scheme := runtime.NewScheme()
	if err := v1beta1.AddToScheme(scheme); err != nil {
		return nil, fmt.Errorf("register OME API types: %w", err)
	}
	direct, err := client.New(restConfig, client.Options{Scheme: scheme})
	if err != nil {
		return nil, fmt.Errorf("build OME client: %w", err)
	}
	return &Clients{Kube: kube, Client: direct}, nil
}

// LoadReplacement reads the replacement document strictly: unknown and
// duplicate keys are errors. The writer boundary decides whether what parsed
// is acceptable.
func LoadReplacement(path string) (*v1beta1.InferenceReplicaStatus, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read replacement: %w", err)
	}
	replacement := &v1beta1.InferenceReplicaStatus{}
	if err := yaml.UnmarshalStrict(data, replacement); err != nil {
		return nil, fmt.Errorf("parse replacement: %w", err)
	}
	return replacement, nil
}

// Run performs one repair, dry-run unless opts.Apply is set, and returns the
// report. Every refusal is an error; a dry run never writes.
func Run(ctx context.Context, opts Options, connect Connector) (*Result, error) {
	inventory, digest, err := transitionpreflight.LoadInventory(opts.InventoryPath)
	if err != nil {
		return nil, err
	}
	cluster, err := findCluster(inventory, opts.Cluster)
	if err != nil {
		return nil, err
	}
	replacement, err := LoadReplacement(opts.ReplacementPath)
	if err != nil {
		return nil, err
	}
	clients, err := connect(ctx, cluster)
	if err != nil {
		return nil, fmt.Errorf("connect to cluster %s: %w", cluster.Name, err)
	}
	config, err := readStatusConfig(ctx, clients.Kube, inventory)
	if err != nil {
		return nil, err
	}

	// The live read is raw on purpose: the payload being replaced may be
	// exactly what the codec refuses, and only the resourceVersion and the
	// status fields outside the per-Instance representation are used.
	live := &v1beta1.InferenceReplica{}
	if err := clients.Client.Get(ctx, client.ObjectKey{Namespace: opts.Namespace, Name: opts.Name}, live); err != nil {
		return nil, fmt.Errorf("read InferenceReplica %s/%s: %w", opts.Namespace, opts.Name, err)
	}
	outcome, err := inferencereplica.RepairInstanceStatus(ctx, clients.Client, config.InstanceStatusEncoding, config.DecodeBound(), inferencereplica.InstanceStatusRepair{
		Live:            live,
		ResourceVersion: live.ResourceVersion,
		Replacement:     replacement,
	}, opts.Apply)
	if err != nil {
		return nil, err
	}
	return &Result{
		InventoryDigest: digest,
		Cluster:         cluster,
		Namespace:       opts.Namespace,
		Name:            opts.Name,
		Config:          *config,
		Outcome:         *outcome,
	}, nil
}

func findCluster(inventory *transitionpreflight.Inventory, name string) (transitionpreflight.Cluster, error) {
	for _, cluster := range inventory.Clusters {
		if cluster.Name == name {
			return cluster, nil
		}
	}
	return transitionpreflight.Cluster{}, fmt.Errorf("cluster %q is not in the inventory", name)
}

// readStatusConfig reads the cluster's omenativeStatus block with the
// manager's own strict parser and requires it to match the inventory, so the
// repair selects the representation with the target and bound the cluster's
// manager runs with.
func readStatusConfig(ctx context.Context, kube kubernetes.Interface, inventory *transitionpreflight.Inventory) (*controllerconfig.OMENativeStatusConfig, error) {
	configMap, err := kube.CoreV1().ConfigMaps(inventory.Manager.Namespace).Get(ctx, inventory.Manager.ConfigMap, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("read ConfigMap %s/%s: %w", inventory.Manager.Namespace, inventory.Manager.ConfigMap, err)
	}
	config, err := controllerconfig.ParseOMENativeStatusConfig(configMap)
	if err != nil {
		return nil, err
	}
	observed := transitionpreflight.ExpectedStatusConfig{InstanceStatusEncoding: config.InstanceStatusEncoding, MaxDecodedInstances: config.MaxDecodedInstances}
	expected := inventory.OMENativeStatus
	if observed.InstanceStatusEncoding != expected.InstanceStatusEncoding || observed.Bound() != expected.Bound() ||
		(observed.MaxDecodedInstances == nil) != (expected.MaxDecodedInstances == nil) {
		return nil, fmt.Errorf("cluster omenativeStatus is %s, inventory expects %s; align the inventory with the running configuration before repairing", observed, expected)
	}
	return config, nil
}

// WriteText renders the report for a terminal.
func (r *Result) WriteText(w io.Writer) error {
	mode := "dry run: nothing was written"
	if r.Outcome.Applied {
		mode = "applied"
	}
	stored := fmt.Sprintf("%s, %d bytes", r.Outcome.StoredEncoding, r.Outcome.StoredBytes)
	if r.Outcome.StoredRows >= 0 {
		stored = fmt.Sprintf("%s, %d rows, %d bytes", r.Outcome.StoredEncoding, r.Outcome.StoredRows, r.Outcome.StoredBytes)
	}
	verb := "would write"
	if r.Outcome.Applied {
		verb = "wrote"
	}
	lines := []string{
		fmt.Sprintf("InferenceReplica status repair (%s)", mode),
		fmt.Sprintf("  inventory digest:  %s", r.InventoryDigest),
		fmt.Sprintf("  cluster:           %s (context %s)", r.Cluster.Name, r.Cluster.Context),
		fmt.Sprintf("  object:            %s/%s", r.Namespace, r.Name),
		fmt.Sprintf("  target:            %s (maxDecodedInstances %s)", r.Config.InstanceStatusEncoding, boundString(r.Config)),
		fmt.Sprintf("  stored:            %s", stored),
		fmt.Sprintf("  replacement:       %d rows (DenseV1)", r.Outcome.ReplacementRows),
		fmt.Sprintf("  %s:%s %s, %d bytes (status %d -> %d bytes)", verb, spaces(len("would write")-len(verb)), r.Outcome.SelectedEncoding, r.Outcome.SelectedBytes, r.Outcome.StoredBytes, r.Outcome.SelectedBytes),
	}
	if r.Outcome.Applied {
		lines = append(lines, fmt.Sprintf("  resourceVersion:   %s", r.Outcome.ResourceVersion))
	} else {
		lines = append(lines, fmt.Sprintf("  precondition:      resourceVersion %s", r.Outcome.ResourceVersion), "Re-run with --apply to write.")
	}
	for _, line := range lines {
		if _, err := fmt.Fprintln(w, line); err != nil {
			return err
		}
	}
	return nil
}

func boundString(config controllerconfig.OMENativeStatusConfig) string {
	if config.MaxDecodedInstances == nil {
		return "unset"
	}
	return fmt.Sprintf("%d", *config.MaxDecodedInstances)
}

func spaces(n int) string {
	if n <= 0 {
		return ""
	}
	return fmt.Sprintf("%*s", n, "")
}

// IsRefusal reports whether err is a repair refusal rather than a transport
// or usage failure.
func IsRefusal(err error) bool {
	return errors.Is(err, inferencereplica.ErrInstanceStatusRepairRefused)
}
