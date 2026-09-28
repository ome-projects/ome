package acceleratorquota

import (
	"context"
	"fmt"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
)

// defaultKubeConfigKey is the Secret data key a WorkloadCluster's kubeConfig
// source defaults to. Named here because a nil source carries no default and
// the API's own default only applies to objects the apiserver has defaulted.
const defaultKubeConfigKey = "kubeconfig"

// clusterRegistry is the transport this connector drives: the subset of
// workloadcluster.Manager it needs, declared locally so the quota plane depends
// on the behaviour rather than on the type.
type clusterRegistry interface {
	ConnectFor(ctx context.Context, name string, uid types.UID, kubeconfig []byte) error
	Disconnect(name string)
	Connected() []string
}

// Connector keeps a cross-cluster transport in step with the WorkloadCluster
// registry. It is the management plane's half of multi-cluster quota: something
// has to hold a client for each member before anything can be projected onto
// one.
//
// It READS the registry and never writes it — no status, no conditions, no
// finalizer. That restraint is the whole design. WorkloadCluster status has
// exactly one writer, the registry's own reconciler in ome-manager, and that
// writer keeps its grace and backoff state in memory rather than behind a lease.
// A second process reporting its own probe results would flap the condition
// between them, churn LastTransitionTime, and lose the argument silently:
// conflicts on that path requeue without surfacing. Leader election is no help
// either, since the two binaries hold independent locks.
//
// So the registry decides what is reachable and this follows. Ready=True
// connects, anything else disconnects. Deferring to the owner also means the
// grace period is applied once, by the process that measured the failure —
// mirroring the condition inherits that damping for free, where a second prober
// would fight it.
type Connector struct {
	client.Client
	Log logr.Logger

	// Clusters is the transport whose connections track the registry. The quota
	// plane builds its own rather than sharing ome-manager's: a Manager is an
	// in-process struct with no remote surface, so "sharing" one would mean
	// running this controller inside ome-manager and handing that process the
	// Kueue write grants a separate binary exists to keep off it.
	Clusters clusterRegistry

	// Changed is signalled when the set of connected members moves. The
	// projector holds every split while any registered member is missing from
	// the basis, and nothing else would tell it the fleet was whole again --
	// its own watches see AcceleratorQuota edits and its resync tick, neither of
	// which fires when a member comes back. Without this a recovered fleet waits
	// out the resync interval before a correct split appears.
	//
	// Nil leaves the projector on its resync alone, which is what the workload
	// mode and the unit tests want.
	Changed chan<- event.GenericEvent

	// Root is the object the signal names. The projector reconciles the whole
	// tree from any node, so one key is enough and the root is the one node
	// guaranteed to exist.
	Root string
}

// notify wakes the projector, without blocking on it. A dropped signal costs a
// resync interval, and blocking the transport's reconcile to guarantee delivery
// would be the worse trade: the queue is drained by a controller that may be
// mid-pass against the very member that just changed.
func (c *Connector) notify() {
	if c.Changed == nil || c.Root == "" {
		return
	}
	evt := event.GenericEvent{Object: &v1beta1.AcceleratorQuota{
		ObjectMeta: metav1.ObjectMeta{Name: c.Root},
	}}
	select {
	case c.Changed <- evt:
	default:
		c.Log.V(1).Info("projector wake-up dropped; the resync tick still covers it")
	}
}

// membership is the connected set, for comparing one pass against the next.
func (c *Connector) membership() sets.Set[string] {
	return sets.New(c.Clusters.Connected()...)
}

// Reconcile mirrors one WorkloadCluster's reachability onto the transport.
func (c *Connector) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := c.Log.WithValues("workloadcluster", req.Name)

	// Connect is content-keyed and every pass calls it, so "did we call it" says
	// nothing. Only a change in who is reachable is worth waking the projector
	// for; otherwise every member's health probe would re-run the whole fleet.
	before := c.membership()
	defer func() {
		if !before.Equal(c.membership()) {
			c.notify()
		}
	}()

	var wc v1beta1.WorkloadCluster
	if err := c.Get(ctx, req.NamespacedName, &wc); err != nil {
		if apierrors.IsNotFound(err) {
			// Deregistered. Dropping the client releases its informers and its
			// share of the per-cluster rate limit; keeping it would leave this
			// process watching a cluster nobody has claimed since.
			c.Clusters.Disconnect(req.Name)
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, fmt.Errorf("reading WorkloadCluster %s: %w", req.Name, err)
	}

	if !wc.DeletionTimestamp.IsZero() || !ready(&wc) {
		c.Clusters.Disconnect(wc.Name)
		return ctrl.Result{}, nil
	}

	kubeconfig, err := c.kubeconfigFor(ctx, &wc)
	if err != nil {
		// Reported by logging rather than by a condition, because the condition
		// belongs to the registry. A cluster the registry calls Ready whose
		// credential this process cannot read is a misconfiguration of THIS
		// plane's access, and the requeue is what retries it.
		return ctrl.Result{}, err
	}
	if kubeconfig == nil {
		// A connection source this plane cannot resolve. Not an error to retry:
		// nothing about it will change on the next pass.
		log.V(1).Info("cluster has no kubeconfig source this plane can use; leaving it unconnected")
		c.Clusters.Disconnect(wc.Name)
		return ctrl.Result{}, nil
	}

	// Registry identity and credential content both identify the connection.
	if err := c.Clusters.ConnectFor(ctx, wc.Name, wc.UID, kubeconfig); err != nil {
		return ctrl.Result{}, fmt.Errorf("connecting to %s: %w", wc.Name, err)
	}
	return ctrl.Result{}, nil
}

// kubeconfigFor resolves the credential the registry recorded. A nil return
// with no error means the source is one this plane does not support.
func (c *Connector) kubeconfigFor(ctx context.Context, wc *v1beta1.WorkloadCluster) ([]byte, error) {
	src := wc.Spec.ClusterSource.KubeConfig
	if src == nil {
		// clusterProfileRef resolves through a credentials provider rather than
		// a Secret, and the registry does not support it yet either.
		return nil, nil
	}

	key := src.Key
	if key == "" {
		key = defaultKubeConfigKey
	}
	var secret corev1.Secret
	nn := types.NamespacedName{Name: src.SecretRef.Name, Namespace: src.SecretRef.Namespace}
	if err := c.Get(ctx, nn, &secret); err != nil {
		return nil, fmt.Errorf("reading kubeconfig Secret %s/%s: %w", nn.Namespace, nn.Name, err)
	}
	raw, ok := secret.Data[key]
	if !ok || len(raw) == 0 {
		return nil, fmt.Errorf("kubeconfig Secret %s/%s has no data at key %q", nn.Namespace, nn.Name, key)
	}
	return raw, nil
}

func ready(wc *v1beta1.WorkloadCluster) bool {
	return apimeta.IsStatusConditionTrue(wc.Status.Conditions, v1beta1.WorkloadClusterReady)
}

// SetupWithManager registers the connector.
//
// Watching WorkloadCluster alone: the Secret behind a cluster is read on every
// pass, so a rotated credential is picked up whenever the registry re-reconciles
// it — which it does, because the registry reads that same Secret to decide
// whether the cluster is still Ready. Watching Secrets here as well would mean
// a second cluster-wide Secret informer for no reachability this one lacks.
func (c *Connector) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1beta1.WorkloadCluster{}).
		Named("acceleratorquota-connector").
		Complete(c)
}
