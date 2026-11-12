package coordination

import (
	"context"
	"errors"
	"fmt"
	"sort"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/irprojector"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/service"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// prefixFor is the name prefix of the replica serving role c: the service
// name for a projected role, the replica name for a referenced one. The
// role's pods carry it in the ome.io/inferenceservice label, and its
// per-revision Services and ControllerRevisions derive their names from it,
// so every per-revision object coordination names or selects is keyed on it.
func prefixFor(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	return irprojector.RoleReplicaPrefix(isvc, c)
}

// servingPortName is the container-port NAME a runtime uses to mark its
// customer-traffic listener. It is a Kubernetes naming convention, not a
// behavioral value: it selects among the ports a runtime already declares
// and never contributes a port number of its own. A runner that declares
// no port under this name falls through to its first declared port.
const servingPortName = "http"

// ErrNoServingPort reports that a Component's effective serving template has
// no container port, so no per-revision routing Service can be built. A
// ClusterIP Service must publish at least one port, and publishing a port
// the runner does not listen on produces a Service whose EndpointSlice
// reads ready while every request is dropped — pod readiness is a property
// of the pod, not of the published port. Callers skip the routing Service
// instead of inventing a number.
var ErrNoServingPort = errors.New("component serving template declares no container port")

// isvcGroupVersionKind identifies the InferenceService owner reference
// on each per-revision Service. Declared locally so this subpackage
// stays importable from either the top-level reconciler or its caller
// without closing an import cycle.
var isvcGroupVersionKind = v1beta1.SchemeGroupVersion.WithKind("InferenceService")

// PerRevisionService describes the routing + headless pair OMENative creates
// per (component, revisionHash). Both share the revision selector; routing may
// add leader and ordinal filters. Names start with the Component's replica
// prefix (prefixFor), which is the InferenceService name for a projected
// role and the replica name for a referenced one.
type PerRevisionService struct {
	// RoutingName is the ClusterIP Service name —
	// `<prefix>-<component>-rev-<hash>`. Used as the backend target by
	// the HTTPRoute weighted-backendRef consumer. Empty when
	// EnsurePerRevisionServices skipped the routing Service because the
	// Component's serving template declares no container port.
	RoutingName string

	// HeadlessName is the headless Service name —
	// `<prefix>-<component>-rev-<hash>-headless`. Used for pod-level
	// DNS within the revision (multi-node tensor-parallel groups need
	// per-pod addressability among one revision's pods).
	HeadlessName string
}

// RevisionRoutingSelector controls the optional pod filters on one revision's
// routing Service. PodOrdinal is applied only when LeaderOnly is also true.
type RevisionRoutingSelector struct {
	LeaderOnly bool
	PodOrdinal bool
}

// PerRevisionServiceName returns the per-revision Service name for one
// (component, revisionHash) pair. Format: `<prefix>-<component>-rev-<hash>`,
// bounded to the DNS1035 label limit; prefix is the Component's replica
// prefix (prefixFor). Delegates to the workload/query copy so the
// coordination-created name and the dispatch-path lookup (workload/ops)
// are always byte-identical.
func PerRevisionServiceName(prefix string, component v1beta1.ComponentType, revisionHash string) string {
	return query.PerRevisionServiceName(prefix, workload.ComponentType(component), revisionHash)
}

// ControllerRevisionName is the name of the ControllerRevision a
// Component's replica mints for one revision hash, `<prefix>-<component>-<hash>`;
// prefix is the Component's replica prefix (prefixFor). Delegates to the
// workload/query composer the replica controller names its revisions with.
func ControllerRevisionName(prefix string, component v1beta1.ComponentType, revisionHash string) string {
	return query.ControllerRevisionName(prefix, workload.ComponentType(component), revisionHash)
}

// PerRevisionHeadlessServiceName returns the per-revision headless
// Service name. Format: `<prefix>-<component>-rev-<hash>-headless`,
// bounded to the DNS1035 label limit.
func PerRevisionHeadlessServiceName(prefix string, component v1beta1.ComponentType, revisionHash string) string {
	return query.PerRevisionHeadlessServiceName(prefix, workload.ComponentType(component), revisionHash)
}

// PerRevisionServiceNames returns the routing + headless names for a
// (component, revisionHash) pair.
func PerRevisionServiceNames(prefix string, component v1beta1.ComponentType, revisionHash string) PerRevisionService {
	return PerRevisionService{
		RoutingName:  PerRevisionServiceName(prefix, component, revisionHash),
		HeadlessName: PerRevisionHeadlessServiceName(prefix, component, revisionHash),
	}
}

// SortedRevisionHashes returns the revision hashes of a per-revision pod-count
// map in ascending order. Every writer of per-revision Services walks the
// revisions in this order, so a pass is repeatable: two passes over the same
// revisions issue the same writes, and a write that fails leaves the same
// revision waiting for the next pass.
func SortedRevisionHashes(byHash map[string]int32) []string {
	if len(byHash) == 0 {
		return nil
	}
	out := make([]string, 0, len(byHash))
	for hash := range byHash {
		out = append(out, hash)
	}
	sort.Strings(out)
	return out
}

// checkPerRevisionInputs is the guard both builders start with: a Service
// needs an owner to name it under and a revision to select.
func checkPerRevisionInputs(isvc *v1beta1.InferenceService, revisionHash string) error {
	if isvc == nil {
		return fmt.Errorf("nil InferenceService")
	}
	if revisionHash == "" {
		return fmt.Errorf("empty revisionHash")
	}
	return nil
}

// perRevisionServiceObject is what the two Services of a revision share: a
// ClusterIP Service in the InferenceService's namespace, labelled with the
// revision's label set and owned by the InferenceService, so a deleted
// service takes its per-revision Services with it. The caller sets what
// tells the two apart: the selector, the ports and the ClusterIP.
func perRevisionServiceObject(isvc *v1beta1.InferenceService, name string, labels map[string]string) *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: isvc.Namespace,
			Labels:    labels,
			OwnerReferences: []metav1.OwnerReference{
				*metav1.NewControllerRef(isvc, isvcGroupVersionKind),
			},
		},
		Spec: corev1.ServiceSpec{Type: corev1.ServiceTypeClusterIP},
	}
}

// BuildPerRevisionRoutingService produces the ClusterIP per-revision
// Service for one (component, revisionHash). Pure compute; idempotent.
//
// `runnerPorts` are the effective serving container ports; the published
// port is resolved from them (see
// perRevisionRoutingPorts). Returns ErrNoServingPort when the set is
// empty — the Service is not buildable without a port the pods listen on.
//
// `appProtocols` maps Service port names to the appProtocol the Component
// declares for them. Each published port gets the value declared for its own
// name, by the stable Service's rule (service.AppProtocolForPort), or none.
//
// `routing` may restrict the selector to leader pods and, when supported by
// the observed revision, leader ordinal 0. Revisions without ordinal labels
// retain a reachable leader-only selector.
//
// The routing Service's label map intentionally excludes optional runner and
// ordinal filters. Labels describe the Service; the Selector gates pod membership.
func BuildPerRevisionRoutingService(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, routing RevisionRoutingSelector, runnerPorts []corev1.ContainerPort, appProtocols map[string]string) (*corev1.Service, error) {
	if err := checkPerRevisionInputs(isvc, revisionHash); err != nil {
		return nil, err
	}
	ports, err := perRevisionRoutingPorts(runnerPorts)
	if err != nil {
		return nil, err
	}
	for i := range ports {
		ports[i].AppProtocol = service.AppProtocolForPort(appProtocols, ports[i].Name)
	}
	prefix := prefixFor(isvc, component)
	labels := perRevisionServiceSelector(prefix, component, revisionHash)
	selector := labels
	if routing.LeaderOnly {
		selector = query.LeaderRoutingSelector(labels, routing.PodOrdinal)
	}
	svc := perRevisionServiceObject(isvc, PerRevisionServiceName(prefix, component, revisionHash), labels)
	svc.Spec.Selector = selector
	svc.Spec.Ports = ports
	return svc, nil
}

// perRevisionRoutingPorts maps a Component's effective serving container ports
// onto the single ServicePort the routing Service publishes. HTTPRoute
// backendRef consumers route to that port, so it must be the one the
// runner listens on. Runtimes routinely declare several ports (serving,
// distributed-init rendezvous, PD bootstrap, metrics); the customer-traffic
// Service publishes only the serving one — the port named `http`, else the
// first declared port.
//
// TargetPort follows the container port's NAME when it has one. Kubernetes
// resolves a named targetPort per endpoint pod, so a revision whose pods
// were rendered from an older spec still receives traffic on the port that
// revision actually opened; a numeric targetPort would tie every revision
// to the current spec's number. Unnamed ports fall back to the number.
//
// ClusterIP Services require at least one port (k8s validation), so an
// empty runner-port set yields ErrNoServingPort. The headless variant
// takes no ports at all — peer DNS resolves pod IPs directly.
func perRevisionRoutingPorts(runnerPorts []corev1.ContainerPort) ([]corev1.ServicePort, error) {
	serving, ok := servingContainerPort(runnerPorts)
	if !ok {
		return nil, ErrNoServingPort
	}
	protocol := serving.Protocol
	if protocol == "" {
		protocol = corev1.ProtocolTCP
	}
	target := intstr.FromInt32(serving.ContainerPort)
	if serving.Name != "" {
		target = intstr.FromString(serving.Name)
	}
	return []corev1.ServicePort{{
		Name:       serving.Name,
		Port:       serving.ContainerPort,
		TargetPort: target,
		Protocol:   protocol,
	}}, nil
}

// servingContainerPort picks the customer-traffic port out of a runner's
// declared container ports: the one named `http`, else the first declared.
// Reports false when nothing is declared.
func servingContainerPort(ports []corev1.ContainerPort) (corev1.ContainerPort, bool) {
	if len(ports) == 0 {
		return corev1.ContainerPort{}, false
	}
	for _, p := range ports {
		if p.Name == servingPortName {
			return p, true
		}
	}
	return ports[0], true
}

// BuildPerRevisionHeadlessService produces the headless per-revision
// Service. Like the routing variant but with ClusterIP=None and
// PublishNotReadyAddresses=true so peer DNS resolves during gang init.
//
// Headless Service intentionally selects ALL pods of the revision —
// including workers — regardless of multi-pod-ness. Peer discovery
// for distributed init (NCCL all-gather, vLLM ray-init, etc.)
// requires every pod to be addressable via per-pod DNS. Only the
// per-revision routing Service adds leader and ordinal filters.
func BuildPerRevisionHeadlessService(isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string) (*corev1.Service, error) {
	if err := checkPerRevisionInputs(isvc, revisionHash); err != nil {
		return nil, err
	}
	prefix := prefixFor(isvc, component)
	selector := perRevisionServiceSelector(prefix, component, revisionHash)
	svc := perRevisionServiceObject(isvc, PerRevisionHeadlessServiceName(prefix, component, revisionHash), selector)
	svc.Spec.ClusterIP = corev1.ClusterIPNone
	svc.Spec.Selector = selector
	svc.Spec.PublishNotReadyAddresses = true
	return svc, nil
}

// perRevisionServiceSelector is the label set per-revision Services
// use as their .spec.selector. The inferenceservice label carries the
// Component's replica prefix, which is what its pods are labelled with;
// the revision-hash label narrows the selector to one ControllerRevision's
// pods; the OMENative-managed label keeps the selector from picking up
// legacy pods on the same Component.
func perRevisionServiceSelector(prefix string, component v1beta1.ComponentType, revisionHash string) map[string]string {
	return map[string]string{
		constants.InferenceServicePodLabelKey: prefix,
		constants.OMEComponentLabel:           string(component),
		query.LabelRevisionHash:               revisionHash,
		query.LabelManagedBy:                  query.ManagedByOMENative,
	}
}

// serviceBuilder renders one of the two Services of a revision from the
// InferenceService, the Component and the revision hash.
type serviceBuilder func(*v1beta1.InferenceService, v1beta1.ComponentType, string) (*corev1.Service, error)

// perRevisionServiceWrite is one Service of a revision's pair as a writer
// handles it: its name, how to render the desired object, and whether it is
// the headless one, whose ClusterIP is None and immutable once created.
type perRevisionServiceWrite struct {
	isvc         *v1beta1.InferenceService
	component    v1beta1.ComponentType
	revisionHash string
	name         string
	build        serviceBuilder
	headless     bool
}

// desired renders the Service as it should be.
func (w perRevisionServiceWrite) desired() (*corev1.Service, error) {
	return w.build(w.isvc, w.component, w.revisionHash)
}

// perRevisionPair is the routing and headless Service of one (component,
// revisionHash) as a writer handles them. The routing Service carries the
// revision's pairing protocol as a metadata label, never in the selector:
// pod selection stays purely revision-hash based, and the label lets
// routing consumers pair engine and decoder Services without decoding
// revisions. Both writers resolve the pair the same way and differ only in
// how they write each Service.
type perRevisionPair struct {
	routing  perRevisionServiceWrite
	headless perRevisionServiceWrite
}

// resolvePerRevisionPair names the pair under the Component's replica
// prefix and reads the revision's pairing protocol from its
// ControllerRevision. A missing ControllerRevision degrades to no label
// (pairs with anything); the ensure path corrects the label on every pass.
func resolvePerRevisionPair(ctx context.Context, reads client.Reader, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, routing RevisionRoutingSelector, runnerPorts []corev1.ContainerPort, appProtocols map[string]string) (perRevisionPair, error) {
	prefix := prefixFor(isvc, component)
	protocol, _, err := PairingProtocolForRevision(ctx, reads, isvc.Namespace, prefix, component, revisionHash)
	if err != nil {
		return perRevisionPair{}, err
	}
	names := PerRevisionServiceNames(prefix, component, revisionHash)
	return perRevisionPair{
		routing: perRevisionServiceWrite{
			isvc: isvc, component: component, revisionHash: revisionHash,
			name: names.RoutingName, build: perRevisionRoutingBuilder(protocol, routing, runnerPorts, appProtocols),
		},
		headless: perRevisionServiceWrite{
			isvc: isvc, component: component, revisionHash: revisionHash,
			name: names.HeadlessName, build: BuildPerRevisionHeadlessService, headless: true,
		},
	}, nil
}

// EnsurePerRevisionServices ensures both the routing and headless
// Services exist for one (component, revisionHash). Idempotent and
// drift-correcting on the selector / labels / owner reference. Returns
// the names so the coordination layer can record them on
// Status.Components.<c>.Traffic[].
//
// `routing` controls the optional leader and ordinal filters. The headless
// Service is unaffected because workers must stay discoverable for peer DNS.
//
// `runnerPorts` are the Component's effective serving container ports. When
// they are empty the routing Service is skipped and logged rather than
// published on a guessed port: a routing Service on a port nothing listens
// on is indistinguishable from a healthy one (its EndpointSlice still reads
// ready) and silently drops every request. The headless Service is still
// ensured — peer DNS for distributed init does not depend on the serving
// port. The reconcile does not fail: coordination is a producer layer, and
// one Component's missing port declaration must not hold up the rest of the
// InferenceService.
//
// `appProtocols` is applied as in BuildPerRevisionRoutingService. Every call
// rewrites the routing Service's ports, so an existing Service gains or drops
// an appProtocol when the argument changes.
func EnsurePerRevisionServices(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, routing RevisionRoutingSelector, runnerPorts []corev1.ContainerPort, appProtocols map[string]string) (PerRevisionService, error) {
	out := PerRevisionServiceNames(prefixFor(isvc, component), component, revisionHash)
	if c == nil {
		return out, fmt.Errorf("EnsurePerRevisionServices: nil client")
	}
	pair, err := resolvePerRevisionPair(ctx, c, isvc, component, revisionHash, routing, runnerPorts, appProtocols)
	if err != nil {
		return out, err
	}
	switch err := ensureService(ctx, c, pair.routing); {
	case errors.Is(err, ErrNoServingPort):
		log.FromContext(ctx).Info("Skipping per-revision routing Service: component serving template declares no container port",
			"component", component, "revisionHash", revisionHash, "service", out.RoutingName)
		out.RoutingName = ""
	case err != nil:
		return out, err
	}
	if err := ensureService(ctx, c, pair.headless); err != nil {
		return out, err
	}
	return out, nil
}

// CreatePerRevisionServicesIfAbsent creates the routing + headless pair for
// one (component, revisionHash) when they do not exist yet and leaves
// existing Services untouched. It is the create-before-pods path for a peer
// Component's target revision: a pod whose per-revision peer endpoint names
// this Service must find it resolvable when it starts. Drift correction
// stays with EnsurePerRevisionServices, which derives the selector from the
// revision's observed pods; this path derives it from the peer's declared
// Runners (RoutingSelectorForRunners) and never overwrites, so the two
// writers cannot fight. A missing serving port skips the routing Service
// the same way the ensure path does. `appProtocols` is applied as in
// BuildPerRevisionRoutingService. A Service created here without an
// appProtocol keeps none until EnsurePerRevisionServices rewrites its ports.
func CreatePerRevisionServicesIfAbsent(ctx context.Context, c client.Client, isvc *v1beta1.InferenceService, component v1beta1.ComponentType, revisionHash string, routing RevisionRoutingSelector, runnerPorts []corev1.ContainerPort, appProtocols map[string]string) error {
	if c == nil {
		return fmt.Errorf("CreatePerRevisionServicesIfAbsent: nil client")
	}
	if isvc == nil || revisionHash == "" {
		return nil
	}
	pair, err := resolvePerRevisionPair(ctx, c, isvc, component, revisionHash, routing, runnerPorts, appProtocols)
	if err != nil {
		return err
	}
	if err := createServiceIfAbsent(ctx, c, pair.routing); err != nil && !errors.Is(err, ErrNoServingPort) {
		return err
	}
	return createServiceIfAbsent(ctx, c, pair.headless)
}

// perRevisionRoutingBuilder returns the routing-Service builder for one
// revision with the revision's pairing protocol written as a metadata label.
// The label never enters the selector: the builder may alias Labels and
// Spec.Selector, so the label map is replaced rather than mutated.
func perRevisionRoutingBuilder(protocol string, routing RevisionRoutingSelector, runnerPorts []corev1.ContainerPort, appProtocols map[string]string) serviceBuilder {
	return func(i *v1beta1.InferenceService, comp v1beta1.ComponentType, hash string) (*corev1.Service, error) {
		svc, err := BuildPerRevisionRoutingService(i, comp, hash, routing, runnerPorts, appProtocols)
		if err != nil || svc == nil {
			return svc, err
		}
		if protocol != "" {
			lbls := make(map[string]string, len(svc.Labels)+1)
			for k, v := range svc.Labels {
				lbls[k] = v
			}
			lbls[query.LabelPairingProtocol] = protocol
			svc.Labels = lbls
		}
		return svc, nil
	}
}

// RoutingSelectorForRunners derives the routing selector for a revision
// that has no observed pods yet from the Component's declared Runners. It
// matches what observing the revision's pods yields once they exist: a
// leader/worker shape narrows the routing Service to the leader pod at
// ordinal 0 (every rendered pod carries the ordinal label); a single
// default Runner keeps the broad selector.
func RoutingSelectorForRunners(runners []v1beta1.Runner) RevisionRoutingSelector {
	for i := range runners {
		if runners[i].Name == v1beta1.RunnerNameLeader {
			return RevisionRoutingSelector{LeaderOnly: true, PodOrdinal: true}
		}
	}
	return RevisionRoutingSelector{}
}

// ServingPortsFromRunners returns the container ports of the Runner
// template that serves a Component's traffic — the leader template for a
// multi-pod shape, else the default one — in container order. The routing
// Service publishes the port named `http` from this set, else the first.
func ServingPortsFromRunners(runners []v1beta1.Runner) []corev1.ContainerPort {
	var serving *v1beta1.Runner
	for i := range runners {
		switch runners[i].Name {
		case v1beta1.RunnerNameLeader:
			serving = &runners[i]
		case v1beta1.RunnerNameDefault:
			if serving == nil {
				serving = &runners[i]
			}
		}
	}
	if serving == nil {
		return nil
	}
	var ports []corev1.ContainerPort
	for _, ctr := range serving.Template.Spec.Containers {
		ports = append(ports, ctr.Ports...)
	}
	return ports
}

// createServiceIfAbsent creates the Service only when no Service of that
// name exists; an existing one is left exactly as found.
func createServiceIfAbsent(ctx context.Context, c client.Client, w perRevisionServiceWrite) error {
	existing := &corev1.Service{}
	err := c.Get(ctx, client.ObjectKey{Namespace: w.isvc.Namespace, Name: w.name}, existing)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return fmt.Errorf("get per-revision service %s/%s: %w", w.isvc.Namespace, w.name, err)
	}
	desired, err := w.desired()
	if err != nil {
		if errors.Is(err, ErrNoServingPort) {
			return err
		}
		return fmt.Errorf("build per-revision service %s: %w", w.name, err)
	}
	if err := c.Create(ctx, desired); err != nil && !apierrors.IsAlreadyExists(err) {
		return fmt.Errorf("create per-revision service %s/%s: %w", w.isvc.Namespace, w.name, err)
	}
	return nil
}

// ensureService creates the Service or patches an existing one back to the
// desired shape. The ClusterIP and the type are set only at create: a
// headless Service's ClusterIP is None and immutable after, so the patch
// never tries to change it.
func ensureService(ctx context.Context, c client.Client, w perRevisionServiceWrite) error {
	desired, err := w.desired()
	if err != nil {
		return fmt.Errorf("build per-revision service %s: %w", w.name, err)
	}
	target := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      desired.Name,
			Namespace: desired.Namespace,
		},
	}
	_, err = controllerutil.CreateOrUpdate(ctx, c, target, func() error {
		if target.CreationTimestamp.IsZero() {
			if w.headless {
				target.Spec.ClusterIP = corev1.ClusterIPNone
			}
			target.Spec.Type = desired.Spec.Type
		}
		target.Spec.PublishNotReadyAddresses = desired.Spec.PublishNotReadyAddresses
		target.Spec.Selector = desired.Spec.Selector
		target.Spec.Ports = desired.Spec.Ports
		target.Labels = desired.Labels
		target.OwnerReferences = desired.OwnerReferences
		return nil
	})
	if err != nil {
		return fmt.Errorf("CreateOrUpdate per-revision service %s/%s: %w", target.Namespace, target.Name, err)
	}
	return nil
}

// isNamespaceTerminating reports whether err is the apiserver's rejection of a
// create because the target namespace is being deleted. Nothing new can be
// created in a terminating namespace, and the ISVC is going away with it, so
// coordination treats this as a benign stop rather than a retryable error.
func isNamespaceTerminating(err error) bool {
	return apierrors.HasStatusCause(err, corev1.NamespaceTerminatingCause)
}

// GCPerRevisionServices deletes both per-revision Services for one
// (component, revisionHash) under the Component's replica prefix. Called
// when the revision is past retention AND no live pod carries the hash.
// NotFound is treated as success — the caller may invoke GC on stale
// entries.
func GCPerRevisionServices(ctx context.Context, c client.Client, namespace, prefix string, component v1beta1.ComponentType, revisionHash string) error {
	if c == nil {
		return fmt.Errorf("GCPerRevisionServices: nil client")
	}
	names := PerRevisionServiceNames(prefix, component, revisionHash)
	for _, name := range []string{names.RoutingName, names.HeadlessName} {
		svc := &corev1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name:      name,
				Namespace: namespace,
			},
		}
		if err := c.Delete(ctx, svc); err != nil && !apierrors.IsNotFound(err) {
			return fmt.Errorf("delete per-revision service %s/%s: %w", namespace, name, err)
		}
	}
	return nil
}
