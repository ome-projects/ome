package inferencereplica

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/go-logr/logr"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/clock"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/audit"
)

const capabilityPublishInterval = 10 * time.Second

// CacheSyncer is the cache synchronization surface the capability publisher
// needs before it may advertise a working executor.
type CacheSyncer interface {
	WaitForCacheSync(context.Context) bool
}

// CapabilityPublisher renews the InferenceReplica executor capability Lease.
// The manager starts it only for the elected replica after the controller has
// been registered successfully.
type CapabilityPublisher struct {
	apiReader    client.Reader
	client       client.Client
	cache        CacheSyncer
	readiness    <-chan struct{}
	namespace    string
	holder       string
	log          logr.Logger
	clock        clock.WithTicker
	retryBackoff wait.Backoff
	interval     time.Duration
}

var _ manager.Runnable = (*CapabilityPublisher)(nil)
var _ manager.LeaderElectionRunnable = (*CapabilityPublisher)(nil)

// NewCapabilityPublisher constructs the elected InferenceReplica executor
// heartbeat. apiReader must bypass the manager cache so every retry rebases on
// the apiserver's latest Lease. readiness must be the channel installed on the
// matching Reconciler; Start rejects a nil channel, while PublishOnce remains a
// directly callable write operation that does not consult startup readiness.
func NewCapabilityPublisher(
	apiReader client.Reader,
	writer client.Client,
	cache CacheSyncer,
	readiness <-chan struct{},
	namespace string,
	holder string,
) *CapabilityPublisher {
	return &CapabilityPublisher{
		apiReader:    apiReader,
		client:       writer,
		cache:        cache,
		readiness:    readiness,
		namespace:    namespace,
		holder:       holder,
		log:          ctrl.Log.WithName("InferenceReplicaCapability"),
		clock:        clock.RealClock{},
		retryBackoff: retry.DefaultRetry,
		interval:     capabilityPublishInterval,
	}
}

// NeedLeaderElection prevents a standby manager from advertising executor
// availability or taking over the capability holder.
func (*CapabilityPublisher) NeedLeaderElection() bool { return true }

// Start waits until the IR controller has processed its startup sentinel, then
// waits for the shared controller cache, publishes immediately, and renews
// periodically until the elected runnable is cancelled. Processing the
// sentinel proves the controller has finished synchronizing every event source
// and started its workers. Publish errors are logged and retried on the next
// heartbeat rather than stopping the manager.
func (p *CapabilityPublisher) Start(ctx context.Context) error {
	if p.cache == nil || p.apiReader == nil || p.client == nil || p.readiness == nil {
		return fmt.Errorf("inferencereplica capability publisher is not fully wired")
	}
	if err := p.validateIdentity(); err != nil {
		return err
	}
	select {
	case <-p.readiness:
	case <-ctx.Done():
		return nil
	}
	if !p.cache.WaitForCacheSync(ctx) {
		return nil
	}

	p.publishAndLog(ctx)
	ticker := p.clock.NewTicker(p.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C():
			p.publishAndLog(ctx)
		}
	}
}

func (p *CapabilityPublisher) publishAndLog(ctx context.Context) {
	if err := p.PublishOnce(ctx); err != nil && ctx.Err() == nil {
		p.log.Error(err, "Failed to publish InferenceReplica executor capability")
	}
}

// PublishOnce creates or renews the capability Lease. Each retry begins with a
// fresh uncached read, preserving unrelated metadata while enforcing only the
// holder, renewal timestamp, and supported migration schema.
func (p *CapabilityPublisher) PublishOnce(ctx context.Context) error {
	if p.apiReader == nil || p.client == nil {
		return fmt.Errorf("inferencereplica capability publisher is not fully wired")
	}
	if err := p.validateIdentity(); err != nil {
		return err
	}
	key := client.ObjectKey{Namespace: p.namespace, Name: constants.OMENativeExecutorCapabilityLeaseName}
	return retry.OnError(p.retryBackoff, capabilityWriteRetryable, func() error {
		lease := &coordinationv1.Lease{}
		err := p.apiReader.Get(ctx, key, lease)
		if apierrors.IsNotFound(err) {
			lease = &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Namespace: key.Namespace, Name: key.Name}}
			p.applyCapability(lease)
			return p.client.Create(ctx, lease)
		}
		if err != nil {
			return err
		}
		p.applyCapability(lease)
		return p.client.Update(ctx, lease)
	})
}

func (p *CapabilityPublisher) validateIdentity() error {
	if strings.TrimSpace(p.namespace) == "" {
		return fmt.Errorf("inferencereplica capability publisher namespace is required")
	}
	if strings.TrimSpace(p.holder) == "" {
		return fmt.Errorf("inferencereplica capability publisher holder is required")
	}
	return nil
}

func capabilityWriteRetryable(err error) bool {
	return apierrors.IsConflict(err) || apierrors.IsAlreadyExists(err)
}

func (p *CapabilityPublisher) applyCapability(lease *coordinationv1.Lease) {
	if lease.Annotations == nil {
		lease.Annotations = make(map[string]string)
	}
	lease.Annotations[constants.OMENativeExecutorCapabilitySchemaAnnotationKey] = audit.SchemaV1
	holder := p.holder
	renewed := metav1.NewMicroTime(p.clock.Now())
	lease.Spec = coordinationv1.LeaseSpec{
		HolderIdentity: &holder,
		RenewTime:      &renewed,
	}
}
