// Command slice-provider stands in for GKE's TPU slice provider on the
// harness's virtual TPU nodes. It places each accelerator.gke.io Slice that
// OME creates on a wholly free partition of its topology, using the slice
// scheduler's best fit and skipping cordoned hosts. It then labels the
// partition's nodes with the Slice name and topology, records the partition
// on the Slice, and reports it ACTIVE. A placement interrupted by a failed
// write resumes on the next pass with the same partition. Once a Slice is
// gone it removes those labels. It never creates, deletes or changes any
// other object.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/tpuslice/gke"
)

const (
	acceleratorLabel = "cloud.google.com/gke-tpu-accelerator"
	topologyLabel    = "cloud.google.com/gke-tpu-topology"
	sliceLabel       = "cloud.google.com/gke-tpu-slice"
	idPrefix         = "cloud.google.com/gke-tpu-partition-"
	idSuffix         = "-id"
	stateSuffix      = "-state"
	healthyState     = "HEALTHY"
	activeState      = "ACTIVE"
)

func main() {
	kubeconfig := flag.String("kubeconfig", "", "path to the harness kubeconfig")
	kubeContext := flag.String("context", "", "kubeconfig context")
	selector := flag.String("node-selector", "alfred-e2e/virtual-tpu=true", "label selector of the TPU nodes this provider serves")
	interval := flag.Duration("interval", 500*time.Millisecond, "reconcile interval")
	flag.Parse()

	sel, err := labels.Parse(*selector)
	if err != nil {
		fail(err)
	}
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		&clientcmd.ClientConfigLoadingRules{ExplicitPath: *kubeconfig},
		&clientcmd.ConfigOverrides{CurrentContext: *kubeContext}).ClientConfig()
	if err != nil {
		fail(err)
	}
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		fail(err)
	}
	c, err := client.New(cfg, client.Options{Scheme: scheme})
	if err != nil {
		fail(err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	p := &provider{c: c, selector: sel}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		if err := p.reconcile(ctx); err != nil && ctx.Err() == nil {
			fmt.Fprintf(os.Stderr, "%s reconcile: %v\n", time.Now().UTC().Format(time.RFC3339), err)
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}

type provider struct {
	c        client.Client
	selector labels.Selector
}

func (p *provider) reconcile(ctx context.Context) error {
	nodes := &corev1.NodeList{}
	if err := p.c.List(ctx, nodes, client.MatchingLabelsSelector{Selector: p.selector}); err != nil {
		return fmt.Errorf("list nodes: %w", err)
	}
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(gke.GroupVersion.WithKind(gke.Kind + "List"))
	if err := p.c.List(ctx, list); err != nil {
		return fmt.Errorf("list slices: %w", err)
	}
	live := map[string]bool{}
	var pending, inactive []*unstructured.Unstructured
	for i := range list.Items {
		u := &list.Items[i]
		s, err := gke.Parse(u)
		if err != nil {
			return err
		}
		if s.Terminating {
			continue
		}
		live[s.Name] = true
		switch {
		case len(s.PartitionIDs) == 0:
			pending = append(pending, u)
		case s.State != activeState:
			inactive = append(inactive, u)
		}
	}
	// Release first, so a partition freed by a deleted Slice is available
	// to the Slices placed in this pass.
	for i := range nodes.Items {
		n := &nodes.Items[i]
		if name := n.Labels[sliceLabel]; name != "" && !live[name] {
			if err := p.label(ctx, n, nil); err != nil {
				return err
			}
			fmt.Printf("%s released %s from %s\n", time.Now().UTC().Format(time.RFC3339), n.Name, name)
		}
	}
	sort.Slice(pending, func(i, j int) bool {
		a, b := pending[i].GetCreationTimestamp(), pending[j].GetCreationTimestamp()
		if !a.Equal(&b) {
			return a.Before(&b)
		}
		return pending[i].GetName() < pending[j].GetName()
	})
	// A Slice that holds a partition but whose activation failed is
	// activated on that partition.
	for _, u := range inactive {
		s, err := gke.Parse(u)
		if err != nil {
			return err
		}
		if err := p.activate(ctx, u, s.PartitionIDs[0]); err != nil {
			return err
		}
		fmt.Printf("%s activated %s on %s\n", time.Now().UTC().Format(time.RFC3339), s.Name, s.PartitionIDs[0])
	}
	for _, u := range pending {
		if err := p.place(ctx, u, nodes.Items); err != nil {
			return err
		}
	}
	return nil
}

// place assigns u a partition, labels its nodes, then reports u ACTIVE. Pods
// are created only once u is ACTIVE, so the nodes carry the label by then.
func (p *provider) place(ctx context.Context, u *unstructured.Unstructured, nodes []corev1.Node) error {
	s, err := gke.Parse(u)
	if err != nil {
		return err
	}
	var hosts []host
	byName := map[string]*corev1.Node{}
	for i := range nodes {
		n := &nodes[i]
		if n.Labels[acceleratorLabel] != s.Type {
			continue
		}
		byName[n.Name] = n
		hosts = append(hosts, hostOf(n))
	}
	id, members, ok := reserved(hosts, s.Name, s.Topology)
	if !ok {
		id, members, ok = choose(hosts, s.Topology)
	}
	if !ok {
		return nil
	}
	for _, name := range members {
		n := byName[name]
		if n.Labels[sliceLabel] == s.Name {
			continue
		}
		if err := p.label(ctx, n, map[string]string{sliceLabel: s.Name, topologyLabel: s.Topology}); err != nil {
			return err
		}
		n.Labels[sliceLabel] = s.Name
	}
	spec := map[string]any{"spec": map[string]any{"partitionIds": []string{id}}}
	if err := p.mergePatch(ctx, u, spec, false); err != nil {
		return fmt.Errorf("assign slice %s: %w", s.Name, err)
	}
	if err := p.activate(ctx, u, id); err != nil {
		return err
	}
	fmt.Printf("%s placed %s (%s) on %s: %s\n", time.Now().UTC().Format(time.RFC3339),
		s.Name, s.Topology, id, strings.Join(members, ","))
	return nil
}

// activate reports u ACTIVE on partition id.
func (p *provider) activate(ctx context.Context, u *unstructured.Unstructured, id string) error {
	status := map[string]any{"status": map[string]any{"conditions": []map[string]any{{
		"type":               "Ready",
		"status":             string(metav1.ConditionTrue),
		"reason":             activeState,
		"message":            "placed on partition " + id,
		"lastTransitionTime": time.Now().UTC().Format(time.RFC3339),
	}}}}
	if err := p.mergePatch(ctx, u, status, true); err != nil {
		return fmt.Errorf("activate slice %s: %w", u.GetName(), err)
	}
	return nil
}

func hostOf(n *corev1.Node) host {
	h := host{name: n.Name, ids: map[string]string{}, healthy: map[string]bool{}}
	for k, v := range n.Labels {
		if !strings.HasPrefix(k, idPrefix) || !strings.HasSuffix(k, idSuffix) {
			continue
		}
		shape := strings.TrimSuffix(strings.TrimPrefix(k, idPrefix), idSuffix)
		if chips(shape) == 0 || v == "" {
			continue
		}
		h.ids[shape] = v
		h.healthy[shape] = n.Labels[idPrefix+shape+stateSuffix] == healthyState
	}
	h.slice = n.Labels[sliceLabel]
	h.free = h.slice == "" && !cordoned(n)
	return h
}

func cordoned(n *corev1.Node) bool {
	if n.Spec.Unschedulable {
		return true
	}
	for _, t := range n.Spec.Taints {
		if t.Key == corev1.TaintNodeUnschedulable {
			return true
		}
	}
	return false
}

// label sets set on n, or removes the slice and topology labels when set is
// nil, guarded by n's resourceVersion.
func (p *provider) label(ctx context.Context, n *corev1.Node, set map[string]string) error {
	values := map[string]any{sliceLabel: nil, topologyLabel: nil}
	for k, v := range set {
		values[k] = v
	}
	patch, err := json.Marshal(map[string]any{"metadata": map[string]any{
		"resourceVersion": n.ResourceVersion,
		"labels":          values,
	}})
	if err != nil {
		return err
	}
	if err := p.c.Patch(ctx, n, client.RawPatch(types.MergePatchType, patch)); err != nil {
		return fmt.Errorf("label node %s: %w", n.Name, err)
	}
	return nil
}

func (p *provider) mergePatch(ctx context.Context, u *unstructured.Unstructured, body map[string]any, status bool) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	patch := client.RawPatch(types.MergePatchType, raw)
	if status {
		return p.c.Status().Patch(ctx, u, patch)
	}
	return p.c.Patch(ctx, u, patch)
}
