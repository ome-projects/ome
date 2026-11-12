package irprojector

import (
	"testing"

	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/v1beta1convert"
)

// desiredSpecCase is one row of TestApplyDesiredSpec: the parent service, the
// replica the spec is written onto (a fresh one, or a live one carrying what
// an earlier pass, an operator or another controller left on it), the input,
// and what the replica must carry after the write.
type desiredSpecCase struct {
	name string
	// service edits the parent before the input is built from it.
	service func(isvc *v1beta1.InferenceService)
	// live edits the replica the spec is written onto; nil hands a fresh one.
	// A live replica carries a resource version, as one read from the store
	// does.
	live func(ir *v1beta1.InferenceReplica)
	// params edits the input of the first write.
	params func(p *Params)
	check  func(s *desiredSpec)
}

// desiredSpec is what a row's check reads and drives: the parent, the replica
// after the first write, and the input that write received.
type desiredSpec struct {
	*gomega.WithT
	isvc  *v1beta1.InferenceService
	ir    *v1beta1.InferenceReplica
	input Params
}

// again writes the spec onto the same replica once more, from the parent as
// it is now and the input edited by edit.
func (s *desiredSpec) again(edit func(p *Params)) *v1beta1.InferenceReplica {
	p := s.input
	p.ISVC = s.isvc
	if edit != nil {
		edit(&p)
	}
	applyDesiredSpec(s.ir, p, s.ir.Name)
	return s.ir
}

// countPartition is a canary resolver that holds every Instance of the count
// it is read at.
func countPartition(replicas int32) *int32 {
	return &replicas
}

// liveReplica marks the replica as one read from the store.
func liveReplica(ir *v1beta1.InferenceReplica) {
	ir.ResourceVersion = "12"
}

func runDesiredSpecCase(t *testing.T, row desiredSpecCase) {
	t.Helper()
	isvc := baselineISVC("llama", "prod")
	isvc.Generation = 4
	if row.service != nil {
		row.service(isvc)
	}
	p := minimalParams(t, isvc, nil)
	if row.params != nil {
		row.params(&p)
	}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: InferenceReplicaName(isvc.Name, p.Component), Namespace: isvc.Namespace}}
	if row.live != nil {
		liveReplica(ir)
		row.live(ir)
	}
	applyDesiredSpec(ir, p, ir.Name)
	row.check(&desiredSpec{WithT: gomega.NewWithT(t), isvc: isvc, ir: ir, input: p})
}

// desiredSpecCases hold what one write of the projected spec leaves on a
// replica, field by field: what is taken from the parent service and the
// merged component, what is kept from the live replica, what is replaced, and
// in which order the fields that depend on each other are written.
var desiredSpecCases = []desiredSpecCase{
	{
		name: "a fresh replica carries the controller-write key and the parent generation",
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Annotations).To(gomega.HaveKeyWithValue(constants.InferenceReplicaControllerWriteAnnotationKey, constants.InferenceReplicaControllerWriteAnnotationVal))
			s.Expect(s.ir.Annotations).To(gomega.HaveKeyWithValue(constants.InferenceReplicaParentGenerationAnnotationKey, "4"))
			s.Expect(s.ir.Annotations).NotTo(gomega.HaveKey(constants.RevisionExcludedAnnotationKeysAnnotationKey))
		},
	},
	{
		name: "annotations another owner wrote are kept and a label added by hand is replaced",
		live: func(ir *v1beta1.InferenceReplica) {
			ir.Annotations = map[string]string{"ome.io/release-held-revision": "rev-1", constants.InferenceReplicaParentGenerationAnnotationKey: "3"}
			ir.Labels = map[string]string{"by-hand": "yes", "managed-by": "someone"}
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Annotations).To(gomega.HaveKeyWithValue("ome.io/release-held-revision", "rev-1"))
			s.Expect(s.ir.Annotations).To(gomega.HaveKeyWithValue(constants.InferenceReplicaParentGenerationAnnotationKey, "4"))
			s.Expect(s.ir.Labels).To(gomega.Equal(s.input.ObjectMeta.Labels))
			s.ir.Labels["later"] = "edit"
			s.Expect(s.input.ObjectMeta.Labels).NotTo(gomega.HaveKey("later"), "the replica's labels are a copy of the input's")
		},
	},
	{
		name: "the service annotation keys the component does not declare are recorded in order",
		service: func(isvc *v1beta1.InferenceService) {
			isvc.Annotations = map[string]string{"team/z": "1", "team/a": "2", "team/m": "3"}
			isvc.Spec.Engine.Annotations = map[string]string{"team/m": "3"}
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Annotations).To(gomega.HaveKeyWithValue(constants.RevisionExcludedAnnotationKeysAnnotationKey, "team/a,team/z"))
		},
	},
	{
		name: "a service with no annotations left clears the record of excluded keys",
		live: func(ir *v1beta1.InferenceReplica) {
			ir.Annotations = map[string]string{constants.RevisionExcludedAnnotationKeysAnnotationKey: "team/a"}
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Annotations).NotTo(gomega.HaveKey(constants.RevisionExcludedAnnotationKeysAnnotationKey))
		},
	},
	{
		name: "the owner reference is the service as controller, replacing one edited by hand",
		live: func(ir *v1beta1.InferenceReplica) {
			ir.OwnerReferences = []metav1.OwnerReference{{APIVersion: "v1", Kind: "ConfigMap", Name: "other", UID: "other-uid"}}
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.OwnerReferences).To(gomega.HaveLen(1))
			owner := s.ir.OwnerReferences[0]
			s.Expect(owner.Kind).To(gomega.Equal("InferenceService"))
			s.Expect(owner.Name).To(gomega.Equal(s.isvc.Name))
			s.Expect(owner.UID).To(gomega.Equal(s.isvc.UID))
			s.Expect(owner.Controller).To(gomega.HaveValue(gomega.BeTrue()))
		},
	},
	{
		name: "parentRef, component, the placement policy and the placement limit come from the input",
		params: func(p *Params) {
			p.Component = v1beta1.DecoderComponent
			p.placementExecution = &v1beta1.PlacementExecutionPolicy{PlanID: "plan-1", Revision: 2, SourceUID: "source", ClusterUID: "cluster"}
			p.placementReplicaLimit = ptr.To(int32(3))
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.ParentRef).To(gomega.Equal(&v1beta1.ParentReference{Name: "llama"}))
			s.Expect(s.ir.Spec.Component).To(gomega.Equal(v1beta1.DecoderComponent))
			s.Expect(s.ir.Spec.PlacementExecution).To(gomega.Equal(s.input.placementExecution))
			s.Expect(s.ir.Spec.PlacementExecution).NotTo(gomega.BeIdenticalTo(s.input.placementExecution), "the policy is a copy")
			s.Expect(s.ir.Spec.PlacementReplicaLimit).To(gomega.HaveValue(gomega.Equal(int32(3))))
		},
	},
	{
		name: "a fresh replica's count is the component minimum",
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Replicas).To(gomega.HaveValue(gomega.Equal(int32(2))))
		},
	},
	{
		name: "a live replica an autoscaler drives keeps its count",
		live: func(ir *v1beta1.InferenceReplica) { ir.Spec.Replicas = ptr.To(int32(5)) },
		params: func(p *Params) {
			p.ResolvedAutoscaler = hpaAutoscalerBlock()
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Replicas).To(gomega.HaveValue(gomega.Equal(int32(5))))
		},
	},
	{
		name: "a live replica nothing else scales takes the component minimum",
		live: func(ir *v1beta1.InferenceReplica) { ir.Spec.Replicas = ptr.To(int32(7)) },
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Replicas).To(gomega.HaveValue(gomega.Equal(int32(2))))
		},
	},
	{
		name: "the runners, a copy of the lifecycle and minReadySeconds come from the merged component",
		service: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Engine.Lifecycle.MinReadySeconds = ptr.To(int32(30))
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Runners).To(gomega.Equal(runnersFromParams(s.input)))
			s.Expect(s.ir.Spec.Runners).To(gomega.HaveLen(1))
			s.Expect(s.ir.Spec.Runners[0].Name).To(gomega.Equal(v1beta1.RunnerNameDefault))
			s.Expect(s.ir.Spec.Lifecycle).To(gomega.Equal(s.input.ComponentExt.Lifecycle))
			s.Expect(s.ir.Spec.Lifecycle).NotTo(gomega.BeIdenticalTo(s.input.ComponentExt.Lifecycle), "the lifecycle is a copy")
			s.Expect(s.ir.Spec.MinReadySeconds).To(gomega.Equal(int32(30)))
		},
	},
	{
		name:   "a multi-pod component writes a leader of one and a worker runner of its size",
		params: threeWorkers,
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Runners).To(gomega.HaveLen(2))
			s.Expect(s.ir.Spec.Runners[0].Name).To(gomega.Equal(v1beta1.RunnerNameLeader))
			s.Expect(s.ir.Spec.Runners[0].Size).To(gomega.Equal(int32(1)))
			s.Expect(s.ir.Spec.Runners[1].Name).To(gomega.Equal(v1beta1.RunnerNameWorker))
			s.Expect(s.ir.Spec.Runners[1].Size).To(gomega.Equal(int32(3)))
		},
	},
	{
		name: "a paused service writes paused with no pause mode",
		service: func(isvc *v1beta1.InferenceService) {
			isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: "true"}
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Paused).To(gomega.BeTrue())
			s.Expect(s.ir.Spec.PauseMode).To(gomega.BeEmpty())
		},
	},
	{
		name: "a service paused at full stop writes paused with the full-stop mode",
		service: func(isvc *v1beta1.InferenceService) {
			isvc.Annotations = map[string]string{constants.PausedRolloutAnnotation: constants.PausedRolloutFreezeValue}
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Paused).To(gomega.BeTrue())
			s.Expect(s.ir.Spec.PauseMode).To(gomega.Equal(v1beta1.PauseModeFreeze))
		},
	},
	{
		name: "a service no longer paused clears both the pause and its mode on a live replica",
		live: func(ir *v1beta1.InferenceReplica) {
			ir.Spec.Paused = true
			ir.Spec.PauseMode = v1beta1.PauseModeFreeze
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Paused).To(gomega.BeFalse())
			s.Expect(s.ir.Spec.PauseMode).To(gomega.BeEmpty())
		},
	},
	{
		name: "the revision history limit follows the service annotation",
		service: func(isvc *v1beta1.InferenceService) {
			isvc.Annotations = map[string]string{constants.RevisionHistoryLimitAnnotation: "3"}
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.RevisionHistoryLimit).To(gomega.HaveValue(gomega.Equal(int32(3))))
		},
	},
	{
		name: "a malformed revision history limit writes none over a live value",
		service: func(isvc *v1beta1.InferenceService) {
			isvc.Annotations = map[string]string{constants.RevisionHistoryLimitAnnotation: "many"}
		},
		live: func(ir *v1beta1.InferenceReplica) { ir.Spec.RevisionHistoryLimit = ptr.To(int32(5)) },
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.RevisionHistoryLimit).To(gomega.BeNil())
		},
	},
	{
		name: "the topology key, the spread policy and the spread key are copied as given",
		params: func(p *Params) {
			p.TopologyKey = ptr.To("topology.kubernetes.io/rack")
			p.TopologySpread = ptr.To(v1beta1.TopologySpreadRequired)
			p.TopologySpreadKey = ptr.To("topology.kubernetes.io/zone")
		},
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.TopologyKey).To(gomega.HaveValue(gomega.Equal("topology.kubernetes.io/rack")))
			s.Expect(s.ir.Spec.TopologySpread).To(gomega.HaveValue(gomega.Equal(v1beta1.TopologySpreadRequired)))
			s.Expect(s.ir.Spec.TopologySpreadKey).To(gomega.HaveValue(gomega.Equal("topology.kubernetes.io/zone")))
			cleared := s.again(func(p *Params) { p.TopologyKey, p.TopologySpread, p.TopologySpreadKey = nil, nil, nil })
			s.Expect(cleared.Spec.TopologyKey).To(gomega.BeNil())
			s.Expect(cleared.Spec.TopologySpread).To(gomega.BeNil())
			s.Expect(cleared.Spec.TopologySpreadKey).To(gomega.BeNil())
		},
	},
	{
		name:   "the pacing partition is the resolver's answer at the count written in the same pass",
		live:   func(ir *v1beta1.InferenceReplica) { ir.Spec.Replicas = ptr.To(int32(7)) },
		params: func(p *Params) { p.PacingPartition = countPartition },
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Replicas).To(gomega.HaveValue(gomega.Equal(int32(2))))
			s.Expect(s.ir.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(2))), "the count written this pass, not the live 7")
		},
	},
	{
		name:   "a partition above the count is written as the count",
		params: func(p *Params) { p.PacingPartition = fixedPartition(5) },
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(2))))
			one := s.again(func(p *Params) { p.PacingPartition = fixedPartition(1) })
			s.Expect(one.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(1))))
		},
	},
	{
		name: "the live rollback target is kept while a canary governs and dropped with the block when none does",
		live: func(ir *v1beta1.InferenceReplica) {
			ir.Spec.Pacing = &v1beta1.InferenceReplicaPacing{RollbackToRevision: ptr.To("llama-engine-stable")}
		},
		params: func(p *Params) { p.PacingPartition = fixedPartition(0) },
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Pacing.Partition).To(gomega.HaveValue(gomega.Equal(int32(0))))
			s.Expect(s.ir.Spec.Pacing.RollbackToRevision).To(gomega.HaveValue(gomega.Equal("llama-engine-stable")))
			released := s.again(func(p *Params) { p.PacingPartition = nil })
			s.Expect(released.Spec.Pacing).To(gomega.BeNil())
		},
	},
	{
		name: "the pairing protocol is written on the roles that pair and cleared on the router",
		service: func(isvc *v1beta1.InferenceService) {
			isvc.Spec.Rollout = &v1beta1.RolloutSpec{PairingProtocol: ptr.To("kv-transfer-v2")}
		},
		live: func(ir *v1beta1.InferenceReplica) { ir.Spec.PairingProtocol = ptr.To("kv-transfer-v1") },
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.PairingProtocol).To(gomega.HaveValue(gomega.Equal("kv-transfer-v2")))
			decoder := s.again(func(p *Params) { p.Component = v1beta1.DecoderComponent })
			s.Expect(decoder.Spec.PairingProtocol).To(gomega.HaveValue(gomega.Equal("kv-transfer-v2")))
			router := s.again(func(p *Params) { p.Component = v1beta1.RouterComponent })
			s.Expect(router.Spec.PairingProtocol).To(gomega.BeNil())
		},
	},
	{
		name: "a service with no pairing protocol clears a live one",
		live: func(ir *v1beta1.InferenceReplica) { ir.Spec.PairingProtocol = ptr.To("kv-transfer-v1") },
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.PairingProtocol).To(gomega.BeNil())
		},
	},
	{
		name:   "the autoscaler block is replaced whole by a copy",
		live:   func(ir *v1beta1.InferenceReplica) { ir.Spec.Autoscaler = autoscalerBlockA() },
		params: func(p *Params) { p.ResolvedAutoscaler = autoscalerBlockB() },
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Autoscaler).To(gomega.Equal(autoscalerBlockB()))
			s.Expect(s.ir.Spec.Autoscaler).NotTo(gomega.BeIdenticalTo(s.input.ResolvedAutoscaler), "the block is a copy")
			cleared := s.again(func(p *Params) { p.ResolvedAutoscaler = nil })
			s.Expect(cleared.Spec.Autoscaler).To(gomega.BeNil())
		},
	},
	{
		name:   "a policy hold keeps the live autoscaler block",
		live:   func(ir *v1beta1.InferenceReplica) { ir.Spec.Autoscaler = autoscalerBlockA() },
		params: func(p *Params) { p.ResolvedAutoscaler, p.PreserveAutoscaler = autoscalerBlockB(), true },
		check: func(s *desiredSpec) {
			s.Expect(s.ir.Spec.Autoscaler).To(gomega.Equal(autoscalerBlockA()))
		},
	},
	{
		name: "the template hash is taken over the runners, the topology key, the pairing protocol and the excluded keys written in the same pass",
		service: func(isvc *v1beta1.InferenceService) {
			isvc.Annotations = map[string]string{"team/a": "2"}
			isvc.Spec.Rollout = &v1beta1.RolloutSpec{PairingProtocol: ptr.To("kv-transfer-v2")}
		},
		params: func(p *Params) { p.TopologyKey = ptr.To("topology.kubernetes.io/rack") },
		check: func(s *desiredSpec) {
			hash := s.ir.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]
			s.Expect(hash).NotTo(gomega.BeEmpty())
			s.Expect(hash).To(gomega.Equal(v1beta1convert.TemplateHash(s.ir, s.ir.Spec.Runners)), "the hash of the replica as written")
			router := s.again(func(p *Params) { p.Component = v1beta1.RouterComponent })
			s.Expect(router.Annotations[constants.InferenceReplicaTemplateHashAnnotationKey]).NotTo(gomega.Equal(hash), "no pairing protocol on the router changes the hash")
		},
	},
}

// TestApplyDesiredSpec runs every row of desiredSpecCases.
func TestApplyDesiredSpec(t *testing.T) {
	for _, row := range desiredSpecCases {
		t.Run(row.name, func(t *testing.T) { runDesiredSpecCase(t, row) })
	}
}
