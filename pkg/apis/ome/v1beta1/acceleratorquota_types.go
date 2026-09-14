/*
AcceleratorQuota is the fleet capacity-budget CRD. One CR is one node of a
single-rooted quota tree: a Cohort node groups other nodes, a ClusterQueue node
is a leaf that is itself a tenant and carries an accelerator budget per
(resource, flavor) pair, which is how Kueue keys quota. spec.parentRef is the
edge to the parent; the tree is the graph of those edges, rooted at the single
reserved parent-less node.

The type lives in two planes and means something slightly different in each. On
a management plane the tree is admin-authored and spec.budgets[].nominal is the
FLEET-wide allowance; the controller computes each cluster's share and projects a
per-cluster copy of the tree onto every workload cluster. On a workload cluster
the tree is normally projected rather than authored: nominal is already resolved
to THAT cluster's share, and the local controller renders it into stock Kueue
objects. A single-cluster install authors the tree locally instead, and the same
controller both resolves and renders it.

Access model: on each plane the admin (or, for a projection, the projector
ServiceAccount) is the sole writer of spec, and that plane's quota controller is
the sole writer of status. A projected copy carries AcceleratorQuotaOriginLabel;
a validating webhook rejects hand-edits to it.
*/

package v1beta1

import (
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// AcceleratorQuotaRootName is the reserved name of the single parent-less node
// every other node descends from. On a management plane its spec is the
// admin-authored fleet total; on a workload cluster the local controller
// maintains it and reports that cluster's derived capacity in status only.
const AcceleratorQuotaRootName = "root"

// AcceleratorQuotaMaxNameLength caps a node's metadata.name. A node name is
// copied verbatim into the AcceleratorQuotaNodeLabel value on every Kueue object
// it materializes, and a label value may not exceed 63 characters — so a name
// long enough to be a valid DNS subdomain would admit fine and then fail to
// materialize. The same cap applies to spec.priorityTier, which is stamped into
// the Kueue priority-class label.
const AcceleratorQuotaMaxNameLength = 63

// Condition types reported on AcceleratorQuota.status.conditions.
const (
	// AcceleratorQuotaReady is True when the node's position in the tree is
	// valid and its materialization matches the resolved budget.
	AcceleratorQuotaReady = "Ready"

	// AcceleratorQuotaDegraded is True when a computed invariant the controller
	// re-checks at reconcile is violated. The last-good materialization is
	// frozen while it is True; nothing is deleted.
	AcceleratorQuotaDegraded = "Degraded"

	// AcceleratorQuotaMaterialized is True when every object this node owns on
	// the current plane matches the resolved budget: projected AcceleratorQuotas
	// on a management plane, Kueue objects on a workload cluster.
	AcceleratorQuotaMaterialized = "Materialized"
)

// Reasons stamped on the conditions above.
const (
	// AcceleratorQuotaReasonAdmitted marks a node whose tree checks pass and
	// whose materialization is current.
	AcceleratorQuotaReasonAdmitted = "Admitted"

	// AcceleratorQuotaReasonParentMissing marks a node whose parentRef does not
	// resolve to an existing node.
	AcceleratorQuotaReasonParentMissing = "ParentMissing"

	// AcceleratorQuotaReasonContainmentViolated marks a parent whose nominal for
	// some (resource, flavor) pair is below the sum of its children's, a state
	// concurrent admissions can reach without either write being individually
	// invalid.
	AcceleratorQuotaReasonContainmentViolated = "ContainmentViolated"

	// AcceleratorQuotaReasonCapacityExceeded marks a node whose budget exceeds
	// observed capacity beyond the configured hysteresis band. The comparison
	// uses the capacity high-water mark, so capacity that shrinks — a drain, a
	// cordon, a rolling node upgrade — never raises it.
	AcceleratorQuotaReasonCapacityExceeded = "CapacityExceeded"

	// AcceleratorQuotaReasonFlavorMissing marks a budget naming a ResourceFlavor
	// that does not exist on a target cluster. A ClusterQueue referencing a
	// missing flavor is inactive, so the budget is not materialized there.
	AcceleratorQuotaReasonFlavorMissing = "FlavorMissing"

	// AcceleratorQuotaReasonShareUnresolved marks a budget whose effective
	// distribution policy cannot be resolved, or an Explicit budget whose
	// perCluster entries do not sum to nominal.
	AcceleratorQuotaReasonShareUnresolved = "ShareUnresolved"

	// AcceleratorQuotaReasonClusterUnreachable marks a node whose projection to
	// at least one workload cluster could not be applied.
	AcceleratorQuotaReasonClusterUnreachable = "ClusterUnreachable"

	// AcceleratorQuotaReasonFrozen marks a node whose materialization is held at
	// its last-good state because a computed invariant is violated.
	AcceleratorQuotaReasonFrozen = "Frozen"

	// AcceleratorQuotaReasonMaterializationFailed marks a node whose objects
	// could not be written. It is the transient case — an API error, a
	// throttled write — and clears on the next successful pass.
	AcceleratorQuotaReasonMaterializationFailed = "MaterializationFailed"

	// AcceleratorQuotaReasonObjectConflict marks a node whose object name is
	// already taken by one this manager does not own. Adoption is refused
	// rather than performed, so the existing object is left untouched and the
	// node materializes nothing. Unlike a failed write this does not clear on
	// its own: an operator has to remove the object or rename the node.
	AcceleratorQuotaReasonObjectConflict = "ObjectConflict"

	// AcceleratorQuotaReasonParentCycle marks a node whose parentRef chain loops.
	// Distinct from ParentMissing because the remedy differs: a missing parent is
	// created, a cycle is broken.
	AcceleratorQuotaReasonParentCycle = "ParentCycle"

	// AcceleratorQuotaReasonUnreachable marks a node that is structurally sound
	// itself but cannot be placed in the tree, because an ancestor is missing or
	// looping. It has no position, so it must not materialize: a ClusterQueue
	// whose cohort was never created is silently rehomed by Kueue onto an
	// implicit default cohort.
	AcceleratorQuotaReasonUnreachable = "Unreachable"

	// AcceleratorQuotaReasonNodeKindInvalid marks a node whose structure
	// contradicts its declared role — a leaf named as another node's parent, a
	// leaf with no budget, or a grouping carrying leaf-only fields.
	AcceleratorQuotaReasonNodeKindInvalid = "NodeKindInvalid"

	// AcceleratorQuotaReasonDepthExceeded marks a node further from the root than
	// the configured maximum.
	AcceleratorQuotaReasonDepthExceeded = "DepthExceeded"

	// AcceleratorQuotaReasonDuplicateNode marks a node name that appears twice in
	// one assembled tree. Cluster-scoped names are unique per apiserver, so this
	// only arises when a caller assembles a set by hand.
	AcceleratorQuotaReasonDuplicateNode = "DuplicateNode"
)

// Projection and ownership marks. The origin label identifies a copy a
// management plane wrote onto a workload cluster. The managed-by and node labels
// go on the Kueue objects a node materializes, and are how those objects are
// found: they carry no OwnerReference back to the CR.
//
// That is a choice, not a constraint — a cluster-scoped owner may own a
// cluster-scoped dependent. Garbage collection fires only on deletion of the
// owner, and switching the mode off deletes no AcceleratorQuota, so an
// OwnerReference would do nothing in the case that matters while making
// "disable the mode" look like it might reap. Deletion is reaped by the
// finalizer instead, which is the one path that removes these objects.
const (
	// AcceleratorQuotaOriginLabel is present on a projected copy and absent on
	// an authored one. Its value is the management plane's configured identity,
	// so a workload cluster can tell two planes apart.
	AcceleratorQuotaOriginLabel = "ome.io/quota-origin"

	// AcceleratorQuotaOriginUIDAnnotation carries the source CR's UID, so a
	// projection left behind by a deleted-and-recreated source is detectable and
	// reaped rather than adopted.
	AcceleratorQuotaOriginUIDAnnotation = "ome.io/quota-origin-uid"

	// AcceleratorQuotaSourceGenerationAnnotation carries the source CR's
	// metadata.generation at projection time. The receiving controller echoes it
	// into status.sourceGeneration once it has materialized the projection,
	// which is what makes remote progress comparable: metadata.generation is an
	// object-local counter, so a projection's own observedGeneration says
	// nothing about which source revision it reflects.
	AcceleratorQuotaSourceGenerationAnnotation = "ome.io/quota-source-generation"

	// AcceleratorQuotaClusterAnnotation names the WorkloadCluster a projection
	// was written for, so the copy is self-describing on the receiving cluster.
	AcceleratorQuotaClusterAnnotation = "ome.io/quota-cluster"

	// AcceleratorQuotaManagedByLabel marks a Kueue object as materialized by
	// OME. Objects without it are never written or garbage-collected, so
	// hand-provisioned queues coexist.
	AcceleratorQuotaManagedByLabel = "ome.io/quota-managed-by"

	// AcceleratorQuotaNodeLabel names the AcceleratorQuota a materialized Kueue
	// object belongs to. Kueue object names come from the node's CR name, never
	// from its path, so re-parenting is an in-place update. Carrying the name as
	// a label value is why a node's name is capped at
	// AcceleratorQuotaMaxNameLength rather than the 253 a DNS subdomain allows.
	AcceleratorQuotaNodeLabel = "ome.io/accelerator-quota"

	// AcceleratorQuotaFinalizer gates deletion until the objects a node owns on
	// this plane are reaped.
	AcceleratorQuotaFinalizer = "ome.io/accelerator-quota"
)

// AcceleratorQuotaRole declares what a node is, rather than leaving it inferred
// from whether children happen to exist. A grouping created before its first
// child is still a Cohort, and adding a child can never silently convert a leaf.
// +kubebuilder:validation:Enum=Cohort;ClusterQueue
type AcceleratorQuotaRole string

const (
	// AcceleratorQuotaRoleCohort is an internal grouping node. It takes
	// children, binds no namespaces, and materializes as a Kueue Cohort with no
	// resourceGroups: pure topology that contributes no quota of its own while
	// still pooling what its children lend.
	AcceleratorQuotaRoleCohort AcceleratorQuotaRole = "Cohort"

	// AcceleratorQuotaRoleClusterQueue is a leaf. It carries the budget, binds
	// namespaces, takes no children, and materializes as a Kueue ClusterQueue
	// plus one LocalQueue per bound namespace.
	AcceleratorQuotaRoleClusterQueue AcceleratorQuotaRole = "ClusterQueue"
)

// AcceleratorQuotaDistributionPolicy selects how a leaf's fleet-wide nominal is
// split into per-cluster shares.
// +kubebuilder:validation:Enum=Explicit;Proportional
type AcceleratorQuotaDistributionPolicy string

const (
	// AcceleratorQuotaDistributionExplicit takes perCluster verbatim. The
	// entries MUST sum to nominal.
	AcceleratorQuotaDistributionExplicit AcceleratorQuotaDistributionPolicy = "Explicit"

	// AcceleratorQuotaDistributionProportional splits nominal in proportion to a
	// snapshot of each cluster's allocatable capacity of the flavor, taken at
	// reconcile. The snapshot is held between reconciles, so shares do not drift
	// as capacity moves.
	AcceleratorQuotaDistributionProportional AcceleratorQuotaDistributionPolicy = "Proportional"
)

// AcceleratorQuota is one node of the fleet quota tree. Cluster-scoped: it is
// fleet-level capacity policy, not a namespaced workload.
//
// The name-length rule is deliberately at the root rather than under spec:
// removecrdvalidation replaces only the spec and status schemas, so a root rule
// is the one structural check that also holds in the minimal CRD variant.
//
// +kubebuilder:validation:XValidation:rule="size(self.metadata.name) <= 63",message="an AcceleratorQuota name may not exceed 63 characters; it is copied into a label value on every Kueue object the node materializes"
// +k8s:openapi-gen=true
// +genclient
// +genclient:nonNamespaced
// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,path=acceleratorquotas,shortName=aq,singular=acceleratorquota
// +kubebuilder:printcolumn:name="Role",type=string,JSONPath=`.spec.role`
// +kubebuilder:printcolumn:name="Parent",type=string,JSONPath=`.spec.parentRef.name`
// Column names and order follow the kubectl-qt plugin, so a node reads the same
// either way, and the budget every node carries is in the default view rather
// than behind -o wide. Only status.budgets[0] fits a row; the plugin emits one
// per (resource, flavor) pair. Observed capacity is deliberately not a column:
// the root alone reports it, so in a flat list it would be blank on every other
// row. Read it off the root directly, or through the plugin, which has the tree
// to put it in.
// +kubebuilder:printcolumn:name="Resource",type=string,JSONPath=`.status.budgets[0].resourceName`
// +kubebuilder:printcolumn:name="Flavor",type=string,JSONPath=`.status.budgets[0].resourceFlavor`
// +kubebuilder:printcolumn:name="Nominal",type=string,JSONPath=`.status.budgets[0].nominal`
// +kubebuilder:printcolumn:name="Admitted",type=string,JSONPath=`.status.budgets[0].admitted`
// +kubebuilder:printcolumn:name="Borrowed",type=string,priority=1,JSONPath=`.status.budgets[0].borrowed`
// +kubebuilder:printcolumn:name="Ready",type=string,JSONPath=`.status.conditions[?(@.type=="Ready")].status`
// +kubebuilder:printcolumn:name="Degraded",type=string,JSONPath=`.status.conditions[?(@.type=="Degraded")].status`
// +kubebuilder:printcolumn:name="Age",type=date,JSONPath=`.metadata.creationTimestamp`
type AcceleratorQuota struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   AcceleratorQuotaSpec   `json:"spec,omitempty"`
	Status AcceleratorQuotaStatus `json:"status,omitempty"`
}

// AcceleratorQuotaList contains a list of AcceleratorQuota nodes. Listing the
// whole set is how the tree is assembled: the edges live in the CRs, not in a
// separate document.
//
// +k8s:openapi-gen=true
// +kubebuilder:object:root=true
type AcceleratorQuotaList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []AcceleratorQuota `json:"items"`
}

// AcceleratorQuotaSpec is one node's declared position in the tree and, for a
// leaf, its budget.
//
// The rules below are same-object shape checks only. Everything needing the rest
// of the tree — parent resolution, cycles, depth, containment, fleet-wide
// namespace uniqueness, role transitions — is enforced by the validating webhook
// at admission and re-checked by the controller at reconcile, which is the
// authority. Structural checks here are also stripped from the minimal CRD
// variant, so they are convenience, never the durable tier.
//
// +kubebuilder:validation:XValidation:rule="self.role != 'ClusterQueue' || has(self.parentRef)",message="a ClusterQueue node must set parentRef; only the reserved root node is parent-less"
// +kubebuilder:validation:XValidation:rule="self.role != 'Cohort' || (!has(self.priorityTier) && !has(self.distribution))",message="a Cohort node must not set priorityTier or distribution; no workload binds to a grouping"
// +kubebuilder:validation:XValidation:rule="self.role != 'Cohort' || !has(self.budgets) || self.budgets.all(b, !has(b.policy) && !has(b.borrowingLimit) && !has(b.lendingLimit) && !has(b.perCluster))",message="a Cohort node's budgets carry only resourceName, resourceFlavor, and nominal"
// +kubebuilder:validation:XValidation:rule="self.role != 'ClusterQueue' || (has(self.budgets) && size(self.budgets) > 0)",message="a ClusterQueue node must carry at least one budget"
// +kubebuilder:validation:XValidation:rule="!has(self.budgets) || self.budgets.all(b, !(has(b.policy) ? b.policy == 'Explicit' : (has(self.distribution) && has(self.distribution.policy) && self.distribution.policy == 'Explicit')) || has(b.perCluster))",message="a budget whose effective distribution policy is Explicit must set perCluster"
// +kubebuilder:validation:XValidation:rule="!has(self.budgets) || self.budgets.all(b, !(has(b.policy) ? b.policy == 'Proportional' : (has(self.distribution) && has(self.distribution.policy) && self.distribution.policy == 'Proportional')) || !has(b.perCluster))",message="a budget whose effective distribution policy is Proportional must not set perCluster; the split is computed"
type AcceleratorQuotaSpec struct {
	// Role declares whether this node groups other nodes or is a leaf carrying
	// the budget. A node is one or the other, never both. Changing it is
	// admitted only under the drained-leaf / childless-grouping guard, because
	// the change deletes the underlying Kueue object.
	// +required
	Role AcceleratorQuotaRole `json:"role"`

	// ParentRef is the edge to this node's parent. Every node sets it except the
	// single reserved root. It is mutable: re-parenting updates the materialized
	// ClusterQueue's cohort in place, so the queue, its LocalQueues, and its
	// admitted workloads all survive.
	// +optional
	ParentRef *AcceleratorQuotaParentRef `json:"parentRef,omitempty"`

	// PriorityTier names the WorkloadPriorityClass stamped on this leaf's
	// workloads. It is a default, not a partition: a workload declaring its own
	// priority keeps it, and the leaf's ClusterQueue admits mixed priorities.
	// OME references the class and does not create it. Capped at
	// AcceleratorQuotaMaxNameLength because it is stamped into a label value.
	// +optional
	// +kubebuilder:validation:MaxLength=63
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	PriorityTier string `json:"priorityTier,omitempty"`

	// Distribution is this leaf's default split policy, overridable per budget
	// entry. Unset means the operator-level default from the quota config
	// applies; absent that too, the node reports Degraded rather than guessing.
	// +optional
	Distribution *AcceleratorQuotaDistribution `json:"distribution,omitempty"`

	// Budgets is the per-flavor allowance, one entry per ResourceFlavor.
	//
	// On a leaf this is the budget: the fleet-wide allowance where the tree is
	// authored, that cluster's resolved share on a projected copy. On a Cohort
	// node it is an authoring guardrail only — the parent number that
	// parent >= sum(children) is checked against. A grouping materializes as a
	// Kueue Cohort with no resourceGroups, so a Cohort budget is never quota
	// anyone can consume.
	//
	// Capped at the number of entries one Kueue resourceGroup accepts.
	// +optional
	// +listType=map
	// +listMapKey=resourceName
	// +listMapKey=resourceFlavor
	// +kubebuilder:validation:MaxItems=16
	Budgets []AcceleratorBudget `json:"budgets,omitempty"`
}

// AcceleratorQuotaParentRef names the parent node by CR name. Nodes are
// cluster-scoped, so the name is globally unique and needs no namespace.
type AcceleratorQuotaParentRef struct {
	// Name is the parent AcceleratorQuota's metadata.name.
	// +required
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	Name string `json:"name"`
}

// AcceleratorQuotaDistribution holds a node's default split policy. A struct
// rather than a bare field so future policy inputs attach to the policy they
// configure.
type AcceleratorQuotaDistribution struct {
	// Policy is the default split for every budget on this node that does not
	// override it.
	// +optional
	Policy AcceleratorQuotaDistributionPolicy `json:"policy,omitempty"`
}

// AcceleratorBudget is one node's allowance of one resource on one
// ResourceFlavor. Kueue quota is keyed by that pair, so both halves are needed:
// a flavor names a hardware class by node labels and says nothing about which
// resource the count applies to.
//
// Accelerators are the only budgeted dimension: cpu and memory are covered on
// the materialized ClusterQueue so Kueue admission never blocks on them, but
// they are not authored here.
type AcceleratorBudget struct {
	// ResourceName is the Kubernetes resource this allowance counts, such as
	// nvidia.com/gpu or google.com/tpu. It becomes the resource entry in the
	// materialized ClusterQueue's resourceGroup. Which names are budgetable is
	// operator config; the API accepts any qualified resource name.
	// +required
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*(/[A-Za-z0-9]([-A-Za-z0-9_.]*[A-Za-z0-9])?)?$`
	ResourceName string `json:"resourceName"`

	// ResourceFlavor names an existing Kueue ResourceFlavor. OME references
	// flavors and does not own node labeling; a flavor missing on a target
	// cluster makes that cluster's ClusterQueue inactive, so the budget is not
	// materialized there.
	// +required
	// +kubebuilder:validation:MaxLength=253
	// +kubebuilder:validation:Pattern=`^[a-z0-9]([-a-z0-9]*[a-z0-9])?(\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*$`
	ResourceFlavor string `json:"resourceFlavor"`

	// Policy overrides the node's default split for this flavor only, so a leaf
	// can be Explicit on a reserved flavor and Proportional on a fungible one.
	// Leaves only.
	// +optional
	Policy AcceleratorQuotaDistributionPolicy `json:"policy,omitempty"`

	// Nominal is the allowance for this flavor, in the plane's own terms: the
	// fleet-wide total where the tree is authored, this cluster's resolved share
	// on a projected copy. It becomes the ClusterQueue's nominalQuota.
	// +required
	Nominal resource.Quantity `json:"nominal"`

	// BorrowingLimit is how far above its share this leaf may burst by borrowing
	// idle sibling capacity, passed through to Kueue. Unset, this leaf does not
	// borrow: nominal is its ceiling, and the materialized queue carries an
	// explicit limit of zero to say so, because the enforcement backend reads an
	// absent limit as unbounded.
	//
	// Borrowing is cluster-local, so setting it turns nominal from a ceiling
	// into a floor plus overage, and the relaxed ceiling is reported in status.
	//
	// WARNING: overage is not currently reclaimable. This plane sets no
	// preemption policy, so a borrower holds what it took until its own work
	// finishes -- a leaf that bursts can leave a sibling waiting for the share
	// that sibling was guaranteed. Leaves only.
	// +optional
	BorrowingLimit *resource.Quantity `json:"borrowingLimit,omitempty"`

	// LendingLimit is how much of this leaf's share siblings may borrow while it
	// is idle, passed through to Kueue. It MUST NOT exceed the share Kueue sees.
	// Leaves only.
	// +optional
	LendingLimit *resource.Quantity `json:"lendingLimit,omitempty"`

	// PerCluster is the admin-authored split, required when the effective policy
	// is Explicit and rejected otherwise. Entries MUST sum to Nominal. It is
	// absent on a projected copy, which carries one already-resolved share
	// rather than a fleet split.
	// +optional
	// +listType=map
	// +listMapKey=cluster
	PerCluster []AcceleratorClusterShare `json:"perCluster,omitempty"`
}

// AcceleratorClusterShare is one cluster's slice of a flavor's budget.
type AcceleratorClusterShare struct {
	// Cluster is the WorkloadCluster's name.
	// +required
	// +kubebuilder:validation:MaxLength=253
	Cluster string `json:"cluster"`

	// Nominal is that cluster's share, and becomes the nominalQuota of the
	// ClusterQueue materialized there.
	// +required
	Nominal resource.Quantity `json:"nominal"`
}

// AcceleratorQuotaStatus is the observed state of one node. The quota controller
// owning the plane is its sole writer.
type AcceleratorQuotaStatus struct {
	// ObservedGeneration is the metadata.generation the most recent status flush
	// reflects.
	// +optional
	ObservedGeneration int64 `json:"observedGeneration,omitempty"`

	// Parent echoes the resolved parent's name, so a node's edge is readable
	// from status alone once the controller has confirmed it resolves.
	// +optional
	Parent string `json:"parent,omitempty"`

	// SourceGeneration is the source CR's generation this node's materialization
	// reflects, echoed from the projection annotation once the local controller
	// has materialized it. Set only on a projected copy: it is what lets the
	// management plane compare remote progress against what it projected, which
	// object-local generations cannot express.
	// +optional
	SourceGeneration int64 `json:"sourceGeneration,omitempty"`

	// Budgets reports, per resource and flavor, the resolved allowance and the
	// usage rolled up from the materialized ClusterQueues.
	// +optional
	// +listType=map
	// +listMapKey=resourceName
	// +listMapKey=resourceFlavor
	Budgets []AcceleratorBudgetStatus `json:"budgets,omitempty"`

	// Capacity is the observed accelerator capacity behind this tree, reported
	// on the root node only. On a management plane it is the fleet view broken
	// down per cluster; on a workload cluster it is that cluster's own derived
	// capacity, which is the only thing the local controller writes on its root.
	// +optional
	// +listType=map
	// +listMapKey=resourceName
	// +listMapKey=resourceFlavor
	Capacity []AcceleratorCapacityStatus `json:"capacity,omitempty"`

	// Clusters reports per-cluster projection state on a management plane: which
	// generation each workload cluster has, and why one is behind. Empty on a
	// workload cluster, which projects nothing.
	// +optional
	// +listType=map
	// +listMapKey=cluster
	Clusters []AcceleratorQuotaClusterStatus `json:"clusters,omitempty"`

	// Materialization records what this node last wrote successfully and whether
	// that output is currently frozen. A frozen node keeps serving its last-good
	// materialization; nothing is deleted while an invariant is violated.
	// +optional
	Materialization *AcceleratorQuotaMaterialization `json:"materialization,omitempty"`

	// Conditions reports Ready, Degraded, and Materialized.
	// +optional
	// +listType=map
	// +listMapKey=type
	// +patchStrategy=merge
	// +patchMergeKey=type
	Conditions []metav1.Condition `json:"conditions,omitempty" patchStrategy:"merge" patchMergeKey:"type"`
}

// AcceleratorBudgetStatus is one (resource, flavor) pair's resolved allowance
// and observed use.
type AcceleratorBudgetStatus struct {
	// ResourceName is the resource these numbers describe.
	// +required
	ResourceName string `json:"resourceName"`

	// ResourceFlavor is the flavor these numbers describe.
	// +required
	ResourceFlavor string `json:"resourceFlavor"`

	// Nominal is the resolved allowance for this plane: the fleet total where
	// the tree is authored, this cluster's share on a projection.
	// +optional
	Nominal resource.Quantity `json:"nominal,omitempty"`

	// Admitted is the quantity of this flavor currently admitted against the
	// materialized queues, including anything borrowed.
	// +optional
	Admitted resource.Quantity `json:"admitted,omitempty"`

	// Reserved is the quantity held by workloads carrying a quota reservation,
	// admitted or not. It is never below Admitted, and the gap between them is
	// work that owns the chips but has not started — a tenant sitting at its
	// ceiling with Admitted low reads as idle unless this is read too.
	// +optional
	Reserved resource.Quantity `json:"reserved,omitempty"`

	// Borrowed is the admitted quantity above the nominal share, taken from idle
	// siblings and reclaimable on contention.
	// +optional
	Borrowed resource.Quantity `json:"borrowed,omitempty"`

	// PerCluster breaks the numbers above down by cluster. Populated where the
	// tree is authored; empty on a projected copy, which knows only itself.
	// +optional
	// +listType=map
	// +listMapKey=cluster
	PerCluster []AcceleratorClusterBudgetStatus `json:"perCluster,omitempty"`
}

// AcceleratorClusterBudgetStatus is one cluster's slice of a flavor's status.
type AcceleratorClusterBudgetStatus struct {
	// Cluster is the WorkloadCluster's name.
	// +required
	Cluster string `json:"cluster"`

	// Nominal is the share assigned to this cluster.
	// +optional
	Nominal resource.Quantity `json:"nominal,omitempty"`

	// Admitted is the quantity admitted on this cluster, including borrowed.
	// +optional
	Admitted resource.Quantity `json:"admitted,omitempty"`

	// Reserved is the quantity reserved on this cluster, admitted or not.
	// +optional
	Reserved resource.Quantity `json:"reserved,omitempty"`

	// Borrowed is the admitted quantity above this cluster's share.
	// +optional
	Borrowed resource.Quantity `json:"borrowed,omitempty"`
}

// AcceleratorCapacityStatus is the observed physical capacity of one flavor, the
// denominator an admin reads a budget against. It is derived, never authored.
type AcceleratorCapacityStatus struct {
	// ResourceName is the resource these numbers describe.
	// +required
	ResourceName string `json:"resourceName"`

	// ResourceFlavor is the flavor these numbers describe.
	// +required
	ResourceFlavor string `json:"resourceFlavor"`

	// Allocatable is the currently schedulable quantity: the flavor's resource
	// summed over Ready, uncordoned nodes matching the flavor's node labels.
	// Nodes that are NotReady or cordoned remain physical inventory but
	// contribute no schedulable capacity.
	// +optional
	Allocatable resource.Quantity `json:"allocatable,omitempty"`

	// HighWaterMark is the greatest Allocatable observed for this flavor. Budget
	// checks compare against it rather than the instantaneous value, so capacity
	// that shrinks — a drain, a cordon, a device-plugin restart — never marks a
	// node Degraded. It is lowered only once the observed value stays below it
	// by more than the configured hysteresis band.
	// +optional
	HighWaterMark resource.Quantity `json:"highWaterMark,omitempty"`

	// ObservedAt is when Allocatable was last sampled.
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`

	// PerCluster breaks capacity down by cluster on a management plane. Empty on
	// a workload cluster, whose root reports only its own capacity.
	// +optional
	// +listType=map
	// +listMapKey=cluster
	PerCluster []AcceleratorClusterCapacityStatus `json:"perCluster,omitempty"`
}

// AcceleratorClusterCapacityStatus is one cluster's contribution to a flavor's
// observed capacity.
type AcceleratorClusterCapacityStatus struct {
	// Cluster is the WorkloadCluster's name.
	// +required
	Cluster string `json:"cluster"`

	// Allocatable is that cluster's schedulable quantity of the flavor.
	// +optional
	Allocatable resource.Quantity `json:"allocatable,omitempty"`

	// HighWaterMark is the greatest Allocatable observed on this cluster.
	// +optional
	HighWaterMark resource.Quantity `json:"highWaterMark,omitempty"`

	// ObservedAt is when this cluster's value was last sampled. A stale stamp
	// distinguishes "capacity dropped" from "the cluster stopped reporting".
	// +optional
	ObservedAt *metav1.Time `json:"observedAt,omitempty"`
}

// AcceleratorQuotaClusterStatus is one workload cluster's projection state,
// reported where the tree is authored.
type AcceleratorQuotaClusterStatus struct {
	// Cluster is the WorkloadCluster's name.
	// +required
	Cluster string `json:"cluster"`

	// AppliedGeneration is the source generation this cluster's projection last
	// carried. Behind metadata.generation means the update has not landed.
	// +optional
	AppliedGeneration int64 `json:"appliedGeneration,omitempty"`

	// AppliedTime is when the projection was last applied successfully.
	// +optional
	AppliedTime *metav1.Time `json:"appliedTime,omitempty"`

	// MaterializedGeneration is the source generation this cluster has actually
	// materialized, read back from the projection's status.sourceGeneration.
	// Both it and AppliedGeneration count source revisions, so the pair is
	// comparable: equal means caught up, lower means the remote controller has
	// not yet rendered what was projected.
	// +optional
	MaterializedGeneration int64 `json:"materializedGeneration,omitempty"`

	// Message explains why this cluster is behind.
	// +optional
	Message string `json:"message,omitempty"`
}

// AcceleratorQuotaMaterialization is the freeze bookkeeping that makes last-good
// behavior durable across controller restarts.
type AcceleratorQuotaMaterialization struct {
	// Frozen reports that output is held at its last-good state because a
	// computed invariant is violated. While frozen the controller writes no new
	// projections or Kueue objects for this node, and deletes none; only
	// explicit deletion of the CR reaps them.
	// +optional
	Frozen bool `json:"frozen,omitempty"`

	// FrozenAt is when the freeze began.
	// +optional
	FrozenAt *metav1.Time `json:"frozenAt,omitempty"`

	// Reason is the condition reason that caused the freeze, retained so the
	// cause survives a later condition update.
	// +optional
	Reason string `json:"reason,omitempty"`

	// LastAppliedGeneration is the spec generation the frozen output was built
	// from, which is the state a reader is currently looking at.
	// +optional
	LastAppliedGeneration int64 `json:"lastAppliedGeneration,omitempty"`

	// LastAppliedTime is when that output was written.
	// +optional
	LastAppliedTime *metav1.Time `json:"lastAppliedTime,omitempty"`
}

func init() {
	SchemeBuilder.Register(&AcceleratorQuota{}, &AcceleratorQuotaList{})
}
