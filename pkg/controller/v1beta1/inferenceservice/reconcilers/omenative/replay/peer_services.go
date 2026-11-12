package replay

import (
	"context"
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

// peerServicesStage names the replica controller's stand-in in the trace:
// the per-revision Services it creates ahead of a pod are written under it.
const peerServicesStage = "replica"

// applyPeerServicesCreated runs the replica controller's create of a peer's
// per-revision Services for the named member, or for every member: each
// serving peer's roll target, read as the peer will publish it, gets its
// routing and headless Service when they are absent, and one already there
// is left as found. The writes are traced under the replica stage, so a
// pass that removes them shows beside them.
func applyPeerServicesCreated(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	members, err := d.membersNamed(ev)
	if err != nil {
		return "", err
	}
	isvc, err := d.liveService(ctx)
	if err != nil {
		return "", err
	}
	d.stage = peerServicesStage
	defer func() { d.stage = "" }()
	var created, kept int
	var peers []string
	for _, m := range members {
		for _, peer := range coordination.ServingPeers(isvc, m.component) {
			hash, c, k, err := d.createPeerRevisionServices(ctx, isvc, peer)
			if err != nil {
				return "", fmt.Errorf("replay: %s: %s pass creating %s services: %w", ev.Key(), m.component, peer, err)
			}
			created, kept = created+c, kept+k
			peers = append(peers, fmt.Sprintf("%s:%s@%s", m.component, peer, hash))
		}
	}
	return fmt.Sprintf("peers=[%s] created=%d kept=%d", strings.Join(peers, " "), created, kept), nil
}

// membersNamed resolves the members an event names: the one component it
// carries, or every member in Component order.
func (d *driver) membersNamed(ev TimelineEvent) ([]*member, error) {
	if ev.Args.Component != "" {
		m, err := d.member(ev.Args.Component)
		if err != nil {
			return nil, err
		}
		return []*member{m}, nil
	}
	members := make([]*member, 0, len(d.members))
	for _, c := range d.components() {
		members = append(members, d.members[c])
	}
	return members, nil
}

// createPeerRevisionServices creates the two per-revision Services of one
// peer's roll target when they are absent, as the replica controller does
// for the pods it renders, and reports the target's hash with how many of
// the two it created and how many it found.
func (d *driver) createPeerRevisionServices(ctx context.Context, isvc *v1beta1.InferenceService, peer v1beta1.ComponentType) (string, int, int, error) {
	pm, ok := d.members[peer]
	if !ok {
		return "", 0, 0, fmt.Errorf("no member drives %s", peer)
	}
	ir, err := d.replica(ctx, peer)
	if err != nil {
		return "", 0, 0, err
	}
	image, err := d.publishedRollTargetImage(ctx, pm, ir)
	if err != nil {
		return "", 0, 0, err
	}
	name, err := d.revisionName(ctx, pm, image)
	if err != nil {
		return "", 0, 0, err
	}
	hash := query.RevisionFromName(name).Hash()
	names := coordination.PerRevisionServiceNames(irprojector.RoleReplicaPrefix(isvc, peer), peer, hash)
	before, err := d.servicesPresent(ctx, names)
	if err != nil {
		return "", 0, 0, err
	}
	if err := coordination.CreatePerRevisionServicesIfAbsent(ctx, d.cluster.cli, isvc, peer, hash,
		coordination.RoutingSelectorForRunners(ir.Spec.Runners), coordination.ServingPortsFromRunners(ir.Spec.Runners), nil); err != nil {
		return "", 0, 0, err
	}
	after, err := d.servicesPresent(ctx, names)
	if err != nil {
		return "", 0, 0, err
	}
	return hash, after - before, before, nil
}

// publishedRollTargetImage is the image of the revision a member's next
// publication names as its roll target: the pushed image once the
// projection carries it, else the member's target, with the retained
// rollback target winning as the replica controller rolls.
func (d *driver) publishedRollTargetImage(ctx context.Context, m *member, ir *v1beta1.InferenceReplica) (string, error) {
	target := m.target
	if m.pendingTarget != "" && projectedImage(ir) == m.pendingTarget {
		target = m.pendingTarget
	}
	return d.rollTargetImageFrom(ctx, m, ir, target)
}

// servicesPresent counts how many of a revision's two Services exist.
func (d *driver) servicesPresent(ctx context.Context, names coordination.PerRevisionService) (int, error) {
	n := 0
	for _, name := range []string{names.RoutingName, names.HeadlessName} {
		err := d.cluster.cli.Get(ctx, client.ObjectKey{Namespace: d.opts.Namespace, Name: name}, &corev1.Service{})
		switch {
		case err == nil:
			n++
		case !apierrors.IsNotFound(err):
			return 0, fmt.Errorf("replay: read service %s: %w", name, err)
		}
	}
	return n, nil
}
