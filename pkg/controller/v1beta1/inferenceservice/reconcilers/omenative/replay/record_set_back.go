package replay

import (
	"context"
	"fmt"
)

// applyRecordSetBack writes a Component's rolled-out record back to the
// revision recorded before it, through the status subresource alone: the
// write a status restored from a backup or patched by hand leaves. No
// generation moves, so nothing tells the controller to look, and the tick's
// pass finds the record as written. A Component with no earlier revision on
// record has nothing to go back to, and the event is refused.
func applyRecordSetBack(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	m, err := requireMember(d, ev)
	if err != nil {
		return "", err
	}
	isvc, err := d.liveService(ctx)
	if err != nil {
		return "", err
	}
	entry := isvc.Status.Components[m.component]
	latest, previous := entry.LatestRolledoutRevision, entry.PreviousRolledoutRevision
	if previous == "" || previous == latest {
		return "", fmt.Errorf("replay: %s: %s records no earlier rolled-out revision to go back to (latest %q)", ev.Key(), m.component, latest)
	}
	entry.LatestRolledoutRevision = previous
	isvc.Status.Components[m.component] = entry
	if err := d.cluster.staging(func() error { return d.cluster.cli.Status().Update(ctx, isvc) }); err != nil {
		return "", fmt.Errorf("replay: write the %s status: %w", m.component, err)
	}
	return fmt.Sprintf("component=%s latest=%s→%s", m.component, latest, previous), nil
}
