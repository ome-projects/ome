package transitionpreflight

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/openapi"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus/schemapreflight"
)

// ReplicaLister lists InferenceReplicas across every namespace one page at a
// time. The generated typed client satisfies it; every call is a direct,
// uncached API read.
type ReplicaLister interface {
	List(ctx context.Context, opts metav1.ListOptions) (*v1beta1.InferenceReplicaList, error)
}

// ClusterClients are the read-only clients the preflight uses against one
// cluster.
type ClusterClients struct {
	// Kube reads the manager Deployment and the configuration ConfigMap.
	Kube kubernetes.Interface
	// OpenAPI is the cluster's OpenAPI v3 discovery client for the schema
	// preflight.
	OpenAPI openapi.Client
	// Replicas pages through every InferenceReplica in the cluster.
	Replicas ReplicaLister
}

// Connector opens the read-only clients for one inventory cluster.
type Connector func(ctx context.Context, cluster Cluster) (*ClusterClients, error)

// Run evaluates every cluster in the inventory and returns the report. It
// performs reads only; a failure in one cluster never stops the others.
func Run(ctx context.Context, inventory *Inventory, digest string, connect Connector) *Report {
	report := &Report{
		InventoryDigest: digest,
		Expected: Expectation{
			ManagerImage:    inventory.ManagerImage,
			OMENativeStatus: inventory.OMENativeStatus,
		},
		Go: true,
	}
	for _, cluster := range inventory.Clusters {
		clusterReport := checkCluster(ctx, inventory, cluster, connect)
		if len(clusterReport.Reasons) != 0 {
			report.Go = false
		}
		report.Clusters = append(report.Clusters, clusterReport)
	}
	return report
}

func checkCluster(ctx context.Context, inventory *Inventory, cluster Cluster, connect Connector) ClusterReport {
	report := ClusterReport{Name: cluster.Name, Context: cluster.Context}
	clients, err := connect(ctx, cluster)
	if err != nil {
		report.fail(ReasonUnreachable, "connect: %v", err)
		return report
	}
	report.Reachable = true
	report.Manager = checkManager(ctx, clients.Kube, inventory)
	report.Config = checkConfig(ctx, clients.Kube, inventory)
	report.Schema = checkSchema(clients.OpenAPI)
	report.Replicas = checkReplicas(ctx, clients.Replicas, inventory)
	report.deriveReasons(inventory)
	return report
}

// checkManager reads the manager Deployment and compares the manager
// container's image with the expected reference.
func checkManager(ctx context.Context, kube kubernetes.Interface, inventory *Inventory) ManagerReport {
	report := ManagerReport{ExpectedImage: inventory.ManagerImage}
	deployment, err := kube.AppsV1().Deployments(inventory.Manager.Namespace).Get(ctx, inventory.Manager.Deployment, metav1.GetOptions{})
	if err != nil {
		report.Error = fmt.Sprintf("read Deployment %s/%s: %v", inventory.Manager.Namespace, inventory.Manager.Deployment, err)
		report.transport = isTransportError(err)
		return report
	}
	report.Found = true
	if deployment.Spec.Replicas != nil {
		report.Replicas = *deployment.Spec.Replicas
	}
	report.ReadyReplicas = deployment.Status.ReadyReplicas
	for _, container := range deployment.Spec.Template.Spec.Containers {
		if container.Name != inventory.Manager.Container {
			continue
		}
		report.Image = container.Image
		report.ImageMatches = container.Image == inventory.ManagerImage
		return report
	}
	report.Error = fmt.Sprintf("Deployment %s/%s has no container named %q", inventory.Manager.Namespace, inventory.Manager.Deployment, inventory.Manager.Container)
	return report
}

// checkConfig reads the configuration ConfigMap and parses its
// omenativeStatus block with the manager's own strict parser.
func checkConfig(ctx context.Context, kube kubernetes.Interface, inventory *Inventory) ConfigReport {
	report := ConfigReport{Expected: inventory.OMENativeStatus}
	configMap, err := kube.CoreV1().ConfigMaps(inventory.Manager.Namespace).Get(ctx, inventory.Manager.ConfigMap, metav1.GetOptions{})
	if err != nil {
		report.Error = fmt.Sprintf("read ConfigMap %s/%s: %v", inventory.Manager.Namespace, inventory.Manager.ConfigMap, err)
		report.transport = isTransportError(err)
		return report
	}
	report.Found = true
	parsed, err := controllerconfig.ParseOMENativeStatusConfig(configMap)
	if err != nil {
		report.Error = err.Error()
		return report
	}
	report.Observed = &ExpectedStatusConfig{InstanceStatusEncoding: parsed.InstanceStatusEncoding, MaxDecodedInstances: parsed.MaxDecodedInstances}
	report.Matches = report.Observed.InstanceStatusEncoding == inventory.OMENativeStatus.InstanceStatusEncoding &&
		report.Observed.Bound() == inventory.OMENativeStatus.Bound() &&
		(report.Observed.MaxDecodedInstances == nil) == (inventory.OMENativeStatus.MaxDecodedInstances == nil)
	return report
}

// checkSchema runs the manager's startup schema preflight through the
// cluster's discovery endpoint.
func checkSchema(client openapi.Client) SchemaReport {
	err := schemapreflight.Verify(client)
	if err == nil {
		return SchemaReport{OK: true}
	}
	return SchemaReport{Error: err.Error(), stale: errors.Is(err, schemapreflight.ErrSchemaIncompatible)}
}

// checkReplicas pages through every InferenceReplica and classifies its
// stored representation with the codec. ColumnarV2 payloads are decoded
// under the inventory bound exactly as a manager would decode them, so an
// object a manager would refuse is reported here first.
func checkReplicas(ctx context.Context, lister ReplicaLister, inventory *Inventory) ReplicaReport {
	report := ReplicaReport{}
	bound := inventory.OMENativeStatus.Bound()
	var continueToken string
	for {
		page, err := lister.List(ctx, metav1.ListOptions{Limit: inventory.PageSize, Continue: continueToken})
		if err != nil {
			report.FailedPage = &PageFailure{Page: report.Pages + 1, Error: err.Error()}
			return report
		}
		report.Pages++
		for i := range page.Items {
			report.classify(&page.Items[i], bound)
		}
		continueToken = page.Continue
		if continueToken == "" {
			return report
		}
	}
}

func (r *ReplicaReport) classify(ir *v1beta1.InferenceReplica, bound uint64) {
	r.Objects++
	key := ir.Namespace + "/" + ir.Name
	encoding, err := irstatus.ObservedEncoding(&ir.Status)
	if err != nil {
		r.Undecodable = append(r.Undecodable, ObjectProblem{Object: key, Reason: codecReason(err)})
		return
	}
	if encoding == irstatus.EncodingDenseV1 {
		r.DenseV1++
		return
	}
	r.ColumnarV2 = append(r.ColumnarV2, key)
	rows, _, err := irstatus.DecodeStatus(&ir.Status, bound)
	if err != nil {
		reason := codecReason(err)
		if reason == string(irstatus.ErrorReasonCardinalityLimit) {
			r.AboveBound = append(r.AboveBound, key)
			return
		}
		r.Undecodable = append(r.Undecodable, ObjectProblem{Object: key, Reason: reason})
		return
	}
	if len(rows) > r.LargestColumnarV2Rows {
		r.LargestColumnarV2Rows = len(rows)
	}
}

// codecReason is the bounded fixed-catalog reason of a codec failure; a
// non-codec failure keeps its message.
func codecReason(err error) string {
	if reason, ok := irstatus.ErrorReasonOf(err); ok {
		return string(reason)
	}
	return err.Error()
}

// isTransportError reports whether err came from the connection rather than
// from the API server's own response.
func isTransportError(err error) bool {
	var status *apierrors.StatusError
	return !errors.As(err, &status)
}

// deriveReasons turns the per-check results into the no-go reasons the
// report lists.
func (c *ClusterReport) deriveReasons(inventory *Inventory) {
	switch {
	case c.Manager.Error != "" && c.Manager.transport:
		c.fail(ReasonUnreachable, "%s", c.Manager.Error)
	case c.Manager.Error != "":
		c.fail(ReasonUnexpectedImage, "%s", c.Manager.Error)
	case !c.Manager.ImageMatches:
		c.fail(ReasonUnexpectedImage, "manager container %q runs %q, expected %q", inventory.Manager.Container, c.Manager.Image, inventory.ManagerImage)
	}

	switch {
	case c.Config.Error != "" && c.Config.transport:
		c.fail(ReasonUnreachable, "%s", c.Config.Error)
	case c.Config.Error != "":
		c.fail(ReasonConfigMismatch, "%s", c.Config.Error)
	case !c.Config.Matches:
		c.fail(ReasonConfigMismatch, "omenativeStatus is %s, expected %s", c.Config.Observed, inventory.OMENativeStatus)
	}

	switch {
	case c.Schema.OK:
	case c.Schema.stale:
		c.fail(ReasonStaleSchema, "%s", c.Schema.Error)
	default:
		c.fail(ReasonUnreachable, "%s", c.Schema.Error)
	}

	if c.Replicas.FailedPage != nil {
		c.fail(ReasonFailedPage, "InferenceReplica list page %d failed: %s", c.Replicas.FailedPage.Page, c.Replicas.FailedPage.Error)
	}
	if inventory.OMENativeStatus.InstanceStatusEncoding == irstatus.EncodingDenseV1 && len(c.Replicas.ColumnarV2) != 0 {
		c.fail(ReasonColumnarV2Present, "%d InferenceReplica objects are stored as %s while the target is %s: %s",
			len(c.Replicas.ColumnarV2), irstatus.EncodingColumnarV2, irstatus.EncodingDenseV1, strings.Join(c.Replicas.ColumnarV2, ", "))
	}
	if len(c.Replicas.AboveBound) != 0 {
		c.fail(ReasonAboveBound, "%d %s objects exceed maxDecodedInstances (%s): %s",
			len(c.Replicas.AboveBound), irstatus.EncodingColumnarV2, boundString(inventory.OMENativeStatus), strings.Join(c.Replicas.AboveBound, ", "))
	}
	if len(c.Replicas.Undecodable) != 0 {
		c.fail(ReasonUndecodable, "%d InferenceReplica objects cannot be decoded: %s", len(c.Replicas.Undecodable), c.Replicas.Undecodable)
	}
	sort.SliceStable(c.Reasons, func(i, j int) bool { return c.Reasons[i].Kind < c.Reasons[j].Kind })
}

func boundString(expected ExpectedStatusConfig) string {
	if expected.MaxDecodedInstances == nil {
		return "none configured"
	}
	return fmt.Sprintf("%d", *expected.MaxDecodedInstances)
}

func (c *ClusterReport) fail(kind ReasonKind, format string, args ...any) {
	c.Reasons = append(c.Reasons, Reason{Kind: kind, Detail: fmt.Sprintf(format, args...)})
}
