package replay

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/runtimerevision"
)

// defaultRuntimeName is the fixture name of the ClusterServingRuntime a
// scenario declares without naming it, like the service name.
const defaultRuntimeName = "runtime-a"

// runtimeFixture is the driver's copy of the ServingRuntime the service
// names: the image every piece renders on the live object, the image every
// piece renders on the revision recorded for the service, and whether the
// service follows the live runtime (spec.runtime.autoSync) or the recorded
// revision. A Component that declares no image of its own renders its
// piece, so an edit of that piece moves the Component's template without a
// write of the service's spec.
type runtimeFixture struct {
	name     string
	autoSync *bool
	live     map[v1beta1.ComponentType]string
	recorded map[v1beta1.ComponentType]string
	// syncs counts the ome.io/runtime-sync values the scenario wrote, for a
	// sync event that names none.
	syncs int
}

func newRuntimeFixture(state *RuntimeState) *runtimeFixture {
	r := &runtimeFixture{
		name:     state.Name,
		autoSync: state.AutoSync,
		live:     map[v1beta1.ComponentType]string{},
		recorded: map[v1beta1.ComponentType]string{},
	}
	if r.name == "" {
		r.name = defaultRuntimeName
	}
	for _, c := range v1beta1.RoleTypes() {
		r.live[c] = state.Image
		r.recorded[c] = state.Image
	}
	return r
}

// follows reports a service that renders the live runtime on every pass:
// spec.runtime.autoSync unset or true.
func (r *runtimeFixture) follows() bool {
	return r.autoSync == nil || *r.autoSync
}

// rendered is the image a piece renders for the service: the live one when
// the service follows the live runtime, else the recorded revision's.
func (r *runtimeFixture) rendered(c v1beta1.ComponentType) string {
	if r.follows() {
		return r.live[c]
	}
	return r.recorded[c]
}

// drifted reports a live runtime that differs from the recorded revision
// while the service renders the recorded one.
func (r *runtimeFixture) drifted() bool {
	if r.follows() {
		return false
	}
	for c, image := range r.live {
		if r.recorded[c] != image {
			return true
		}
	}
	return false
}

// spec is the runtime spec the engine's hasher reads for one set of piece
// images: one container per piece, named by the piece.
func runtimeSpecOf(images map[v1beta1.ComponentType]string) *v1beta1.ServingRuntimeSpec {
	spec := &v1beta1.ServingRuntimeSpec{}
	for _, c := range v1beta1.RoleTypes() {
		spec.Containers = append(spec.Containers, corev1.Container{Name: string(c), Image: images[c]})
	}
	return spec
}

// hash is the engine's own short hash of the runtime spec carrying the
// given piece images, the form a recorded revision's name ends with.
func runtimeHash(images map[v1beta1.ComponentType]string) (string, error) {
	_, short, err := runtimerevision.Hash(runtimeSpecOf(images))
	if err != nil {
		return "", fmt.Errorf("replay: hash the runtime spec: %w", err)
	}
	return short, nil
}

// recordedRevision names the ControllerRevision the service renders while it
// does not follow the live runtime, as the engine names it.
func (r *runtimeFixture) recordedRevision() (string, error) {
	short, err := runtimeHash(r.recorded)
	if err != nil {
		return "", err
	}
	return runtimerevision.Name(runtimerevision.KindClusterServingRuntime, "", r.name, short), nil
}

// identity renders the recorded revision and the live hash for a trace
// line: the two the controller compares for a recorded runtime revision.
func (r *runtimeFixture) identity() (string, error) {
	live, err := runtimeHash(r.live)
	if err != nil {
		return "", err
	}
	if r.follows() {
		return "live=" + live, nil
	}
	recorded, err := r.recordedRevision()
	if err != nil {
		return "", err
	}
	return "recorded=" + recorded + " live=" + live, nil
}

// edit rewrites the named pieces of one image set and returns the pieces
// whose image moved.
func editRuntime(images map[v1beta1.ComponentType]string, pieces []v1beta1.ComponentType, image string) []v1beta1.ComponentType {
	var moved []v1beta1.ComponentType
	for _, c := range pieces {
		if images[c] == image {
			continue
		}
		images[c] = image
		moved = append(moved, c)
	}
	return moved
}

// requireRuntime is the runtime a runtime event edits; a service that
// names none has nothing the event could reach.
func requireRuntime(d *driver, ev TimelineEvent) (*runtimeFixture, error) {
	if d.runtime == nil {
		return nil, fmt.Errorf("replay: %s: the service names no runtime; declare initial.service.runtime", ev.Key())
	}
	return d.runtime, nil
}

// runtimePieces is the pieces a runtime event edits: the one the event
// names, else every piece of the runtime.
func runtimePieces(ev TimelineEvent) ([]v1beta1.ComponentType, error) {
	if ev.Args.Component == "" {
		return v1beta1.RoleTypes(), nil
	}
	c := v1beta1.ComponentType(ev.Args.Component)
	if !knownComponent(ev.Args.Component) {
		return nil, fmt.Errorf("replay: %s: component %q is not engine, decoder or router", ev.Key(), ev.Args.Component)
	}
	return []v1beta1.ComponentType{c}, nil
}

// piecesValue renders the pieces an event edits: the one it names, or all.
func piecesValue(ev TimelineEvent) string {
	if ev.Args.Component == "" {
		return "all"
	}
	return ev.Args.Component
}

func componentList(components []v1beta1.ComponentType) string {
	names := make([]string, 0, len(components))
	for _, c := range components {
		names = append(names, string(c))
	}
	sort.Strings(names)
	return "[" + strings.Join(names, " ") + "]"
}

// rendersRuntime reports a Component whose template is the runtime's
// piece: the service declares no image of its own for it.
func (d *driver) rendersRuntime(c v1beta1.ComponentType) bool {
	cs, ok := d.service.Components[string(c)]
	return ok && cs.Image == "" && d.runtime != nil
}

// followRuntime points every Component that renders the runtime at the
// image the runtime now renders for it. The projection carries the new
// template on the next pass; the member records it as its update revision
// on the publication after that, as the replica controller mints a
// revision from the spec it is given and not before.
func (d *driver) followRuntime() []v1beta1.ComponentType {
	var moved []v1beta1.ComponentType
	for _, c := range d.components() {
		if !d.rendersRuntime(c) {
			continue
		}
		m := d.members[c]
		image := d.runtime.rendered(c)
		if m.image == image {
			continue
		}
		m.image = image
		m.pendingTarget = image
		moved = append(moved, c)
	}
	return moved
}

// applyTargetChangeOrRuntimeEdit is replica.targetBumped: the spec variant
// is a push of the service's own template, the runtime variant an edit of
// the runtime the service follows.
func applyTargetChangeOrRuntimeEdit(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	if ev.Variant == "runtime" {
		return applyRuntimeEdit(ctx, d, ev)
	}
	return applyTargetChange(ctx, d, ev)
}

// applyRuntimeEdit edits the live runtime of a service that follows it: the
// named piece, or every piece, renders the new image, so every Component
// that renders an edited piece gets a new template with no write of the
// service's spec and no move of its generation.
func applyRuntimeEdit(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	r, err := requireRuntime(d, ev)
	if err != nil {
		return "", err
	}
	image, err := requireImage(ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Protocol != "" {
		return "", fmt.Errorf("replay: %s: a pairing protocol is spec.rollout.pairingProtocol, a field of the service; stage it with spec.revision", ev.Key())
	}
	if !r.follows() {
		return "", fmt.Errorf("replay: %s: the service renders a recorded revision of %s (spec.runtime.autoSync false); a live edit is runtime.drifted", ev.Key(), r.name)
	}
	pieces, err := runtimePieces(ev)
	if err != nil {
		return "", err
	}
	if moved := editRuntime(r.live, pieces, image); len(moved) == 0 {
		return "", fmt.Errorf("replay: %s: %s already renders %s on %s; an edit that changes no template is a resync", ev.Key(), r.name, image, componentList(pieces))
	}
	templates := d.followRuntime()
	isvc, err := d.liveService(ctx)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("image=%s pieces=%s templates=%s generation=%d", image, piecesValue(ev), componentList(templates), isvc.Generation), nil
}

// applyRuntimeDrift edits the live runtime of a service that renders a
// recorded revision of it: the live hash moves off the recorded one and
// no template changes. While the two differ the driver runs no pass, as
// the controller ends its pass before the rollout stages.
func applyRuntimeDrift(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	r, err := requireRuntime(d, ev)
	if err != nil {
		return "", err
	}
	image, err := requireImage(ev)
	if err != nil {
		return "", err
	}
	if r.follows() {
		return "", fmt.Errorf("replay: %s: the service follows the live runtime %s (spec.runtime.autoSync true); a live edit rolls it: stage replica.targetBumped[runtime]", ev.Key(), r.name)
	}
	pieces, err := runtimePieces(ev)
	if err != nil {
		return "", err
	}
	if moved := editRuntime(r.live, pieces, image); len(moved) == 0 {
		return "", fmt.Errorf("replay: %s: the live %s already renders %s on %s; an edit that moves no hash is a resync", ev.Key(), r.name, image, componentList(pieces))
	}
	identity, err := r.identity()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("image=%s pieces=%s %s", image, piecesValue(ev), identity), nil
}

// applyRuntimeSync is the operator accepting a drift: a new
// ome.io/runtime-sync value advances the recorded revision to the live
// spec, or spec.runtime.autoSync is set true and the service follows the
// live runtime from then on. Either way the Components that render the
// runtime get the live template.
func applyRuntimeSync(ctx context.Context, d *driver, ev TimelineEvent) (string, error) {
	r, err := requireRuntime(d, ev)
	if err != nil {
		return "", err
	}
	if !r.drifted() {
		return "", fmt.Errorf("replay: %s: the live %s matches the revision the service renders; there is nothing to sync", ev.Key(), r.name)
	}
	var detail string
	switch ev.Variant {
	case "annotation":
		value := ev.Args.Value
		if value == "" {
			r.syncs++
			value = strconv.Itoa(r.syncs)
		}
		if err := d.setAnnotation(ctx, constants.RuntimeSyncAnnotationKey, value); err != nil {
			return "", err
		}
		for c, image := range r.live {
			r.recorded[c] = image
		}
		recorded, err := r.recordedRevision()
		if err != nil {
			return "", err
		}
		detail = fmt.Sprintf("%s=%s recorded=%s", constants.RuntimeSyncAnnotationKey, value, recorded)
	case "autoSync":
		follows := true
		r.autoSync = &follows
		generation, err := d.writeSpec(ctx)
		if err != nil {
			return "", err
		}
		detail = fmt.Sprintf("spec.runtime.autoSync=true generation=%d", generation)
	default:
		return "", fmt.Errorf("replay: %s: the variant is annotation or autoSync", ev.Key())
	}
	return detail + " templates=" + componentList(d.followRuntime()), nil
}

// applyRuntimeEditRefused is an edit of the runtime the admission webhook
// refused: the live runtime keeps its spec, nothing is rendered again and
// nothing reaches the service. The reason is the webhook's answer, a claim
// the scenario makes.
func applyRuntimeEditRefused(_ context.Context, d *driver, ev TimelineEvent) (string, error) {
	r, err := requireRuntime(d, ev)
	if err != nil {
		return "", err
	}
	image, err := requireImage(ev)
	if err != nil {
		return "", err
	}
	if ev.Args.Reason == "" {
		return "", fmt.Errorf("replay: %s needs the reason the webhook refused the edit with", ev.Key())
	}
	if _, err := runtimePieces(ev); err != nil {
		return "", err
	}
	identity, err := r.identity()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("image=%s pieces=%s reason=%s %s", image, piecesValue(ev), ev.Args.Reason, identity), nil
}

// skipDriftedPass stands in for the controller's pass over a service whose
// live runtime differs from the recorded runtime revision it renders: the
// controller writes RuntimeDrifted and ends the pass before the run layer,
// the projection, the canary dispatch and the coordination pass run, so
// none of them runs here either and the tick records why.
func (d *driver) skipDriftedPass() error {
	identity, err := d.runtime.identity()
	if err != nil {
		return err
	}
	d.tickRun = RunView{}
	d.trace.line("pass skipped reason=%s %s", constants.RuntimeDriftedConditionType, identity)
	return nil
}
