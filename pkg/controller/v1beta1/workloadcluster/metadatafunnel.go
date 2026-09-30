package workloadcluster

import (
	"context"
	"fmt"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// watchMetadataCluster relists on reconnect or an expired watch so changes
// between the snapshot and stream are covered by the snapshot resource version.
func (f *StatusFunnel) watchMetadataCluster(ctx context.Context, cluster string) {
	backoff := f.mgr.ReconnectBackoff()
	var failed uint
	for ctx.Err() == nil {
		if delay := backoff.retryAfter(failed); delay > 0 {
			select {
			case <-ctx.Done():
				return
			case <-time.After(delay):
			}
		}
		established, err := f.metadataRound(ctx, cluster, int(failed)+1)
		if established {
			failed = 0
		}
		if err != nil {
			failed++
			ctrl.LoggerFrom(ctx).V(2).Info("metadata funnel reconnecting", "cluster", cluster, "error", err.Error())
		} else {
			failed = 0
		}
	}
}

func (f *StatusFunnel) metadataRound(ctx context.Context, cluster string, attempt int) (bool, error) {
	cl, generation, connected := f.mgr.clientForGeneration(cluster)
	if !connected {
		return false, fmt.Errorf("cluster is disconnected")
	}
	snapshot, ok := f.cfg.NewList().(*metav1.PartialObjectMetadataList)
	if !ok {
		return false, fmt.Errorf("metadata funnel requires a metadata list")
	}
	listCtx, cancel := context.WithTimeout(ctx, f.mgr.ReconnectBackoff().establishWaitTime(attempt))
	err := cl.List(listCtx, snapshot, f.watchListOpts()...)
	cancel()
	if err != nil {
		return false, err
	}
	if snapshot.ResourceVersion == "" {
		return false, fmt.Errorf("metadata snapshot has no resource version")
	}
	if _, current, ok := f.mgr.clientForGeneration(cluster); !ok || current != generation {
		return false, fmt.Errorf("cluster connection changed during metadata list")
	}
	opts := append(f.watchListOpts(), &client.ListOptions{Raw: &metav1.ListOptions{ResourceVersion: snapshot.ResourceVersion}})
	stream, err := establishWatch(ctx, cl, f.cfg.NewList(), f.mgr.ReconnectBackoff().establishWaitTime(attempt), opts...)
	if err != nil {
		return false, err
	}
	defer stream.Stop()
	for i := range snapshot.Items {
		f.push(ctx, &snapshot.Items[i])
	}
	ticker := time.NewTicker(f.resyncInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return true, nil
		case <-ticker.C:
			if _, current, ok := f.mgr.clientForGeneration(cluster); !ok || current != generation {
				return true, fmt.Errorf("cluster connection changed during metadata watch")
			}
		case ev, ok := <-stream.ResultChan():
			if !ok {
				return true, fmt.Errorf("metadata watch closed")
			}
			switch ev.Type {
			case watch.Added, watch.Modified, watch.Deleted:
				f.push(ctx, ev.Object)
			case watch.Error:
				return true, fmt.Errorf("metadata watch: %w", apierrors.FromObject(ev.Object))
			}
		}
	}
}
