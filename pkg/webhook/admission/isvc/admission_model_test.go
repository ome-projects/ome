package isvc

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"

	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/controllerconfig"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/reconcilers/omenative/coordination"
	"sigs.k8s.io/ome/pkg/validation"
)

// This file drives the production InferenceService webhook (ValidateCreate
// and ValidateUpdate, the rule list over pkg/validation) over the writes
// an operator makes to a prefill/decode service. The validator is built the
// way cmd/manager builds it, with the chart's operator configuration (a
// ratio tolerance of 5, one replica, SurgeThenDrain fills), and twice more
// with a changed configuration, the state a restarted manager is in after
// the ConfigMap moved under a stored object.
//
// Every configuration (a reference shape, pods per Instance, routers
// beside the pair, a strategy per side at ten percent, a rollout group of
// each kind with or without a ratio rule, a pairing protocol, a partition,
// a scaling policy, and the moment admission can see: idle, a run open in
// status, or paused) is written once as a create. On the stored object
// every operator write is then tried one at a time: the object written back
// unchanged, the verbs (pause, resume, roll back, promote), a push, a scale
// of one side, both or the ratio, a partition set, raised, lowered or
// cleared, a protocol change, a scaling policy added, edited or removed, a
// group or a ratio rule added or removed, a strategy change, an autoscaler,
// a backend change. Seeded random runs chain admitted writes.
//
// At every state the model checks that the unchanged object is admitted,
// that every verb is admitted, that a refusal names a reason the write's
// own fields are read by and carries a reason token, that an admitted write
// can be undone by writing the stored object back, that the ways out a
// ratio refusal names are themselves admitted, and that a first drain the
// drain feasibility rule refuses is one the engine's own ratio band
// (coordination.EvaluateSurge) refuses too. OME_ADMISSION_HUNT=full widens
// the configurations and the random runs; OME_ADMISSION_HUNT_ONLY=<text>
// keeps the configurations whose name carries the text.

const (
	admissionModelFullEnv = "OME_ADMISSION_HUNT"
	admissionModelOnlyEnv = "OME_ADMISSION_HUNT_ONLY"

	admissionModelImageV1 = "runtime-image:v1"
	admissionModelImageV2 = "runtime-image:v2"
	// admissionModelHash is a well-formed canary revision hash for the
	// promote and resume verbs.
	admissionModelHash = "abcdef0123"
)

// admissionModelOperatorConfig is what the manager loads once at start and
// hands the validator: the tolerance a ratio group runs under when it names
// none, the replica count a Component without replica fields is sized at,
// and the update strategy a Component without one rolls by.
type admissionModelOperatorConfig struct {
	name        string
	tolerance   *int32
	minReplicas *int
	fills       *controllerconfig.UpdateStrategyDefaultsConfig
}

func (c admissionModelOperatorConfig) validator() *InferenceServiceValidator {
	return &InferenceServiceValidator{
		DefaultRatioTolerancePercent: c.tolerance,
		DefaultMinReplicas:           c.minReplicas,
		DefaultUpdateStrategy:        c.fills,
	}
}

// admissionModelSurgeFills is the chart's deploy.updateStrategy block: every
// Component rolls SurgeThenDrain when it declares no strategy.
func admissionModelSurgeFills() *controllerconfig.UpdateStrategyDefaultsConfig {
	surge := &controllerconfig.ComponentUpdateStrategyDefaults{Type: string(v1beta1.UpdateStrategySurgeThenDrain)}
	return &controllerconfig.UpdateStrategyDefaultsConfig{Router: surge, Engine: surge, Decoder: surge, Encoder: surge}
}

var (
	// admissionModelChartConfig is the chart's values as the manager loads
	// them: coordination.defaultRatioTolerancePercent 5,
	// deploy.replicas.defaultMinReplicas 1, SurgeThenDrain fills.
	admissionModelChartConfig = admissionModelOperatorConfig{name: "chart", tolerance: ptr.To[int32](5), minReplicas: ptr.To(1), fills: admissionModelSurgeFills()}
	// admissionModelWideConfig is the chart with a tolerance of 20: the
	// operator configuration a stored object may have been written under
	// before the tolerance was narrowed.
	admissionModelWideConfig = admissionModelOperatorConfig{name: "tolerance-20", tolerance: ptr.To[int32](20), minReplicas: ptr.To(1), fills: admissionModelSurgeFills()}
	// admissionModelUnfilledConfig is the chart with no strategy fill: a
	// Component without a strategy then rolls as the engine reads an unset
	// one, SurgeThenDrain.
	admissionModelUnfilledConfig = admissionModelOperatorConfig{name: "no-fill", tolerance: ptr.To[int32](5), minReplicas: ptr.To(1)}
)

// admissionModelGroupKind is the rollout group a configuration declares
// over the engine and the decoder.
type admissionModelGroupKind int

const (
	admissionModelNoGroup admissionModelGroupKind = iota
	admissionModelRollingGroup
	admissionModelRatioGroup
	admissionModelBlueGreenGroup
	admissionModelCanaryGroup
)

func (k admissionModelGroupKind) String() string {
	return [...]string{"ungrouped", "rolling", "ratio", "bluegreen", "canary"}[k]
}

// admissionModelMoment is what admission can see of a roll: nothing (idle),
// a run open in status, or the pause annotation on the object.
type admissionModelMoment int

const (
	admissionModelIdle admissionModelMoment = iota
	admissionModelRunOpen
	admissionModelPaused
)

func (m admissionModelMoment) String() string {
	return [...]string{"idle", "run-open", "paused"}[m]
}

// admissionModelConfiguration is one stored object: the service as the
// operator wrote it, before any later write.
type admissionModelConfiguration struct {
	prefill, decode int
	// pods is the pods per Instance on both sides: one is a single pod, more
	// is a leader with pods-1 workers.
	pods    int
	routers int
	// engineStrategy and decoderStrategy are the declared update strategy
	// types; empty leaves the field unset for the operator fill.
	engineStrategy, decoderStrategy v1beta1.UpdateStrategyType
	group                           admissionModelGroupKind
	// tolerance is the ratio group's own tolerance; nil leaves it to the
	// operator configuration.
	tolerance *int32
	protocol  string
	// partitionOn names the side that holds its first two Instances back,
	// empty for none.
	partitionOn v1beta1.ComponentType
	policy      bool
	moment      admissionModelMoment
	// under names the operator configuration the object was written under;
	// empty is the chart's. The other configurations are what a restarted
	// manager may load instead.
	under string
}

// admissionModelOperatorConfigs lists the operator configurations by name.
var admissionModelOperatorConfigs = []admissionModelOperatorConfig{admissionModelChartConfig, admissionModelWideConfig, admissionModelUnfilledConfig}

// writtenUnder is the operator configuration the object was written under,
// and the others a restarted manager may load.
func (c admissionModelConfiguration) writtenUnder() (admissionModelOperatorConfig, []admissionModelOperatorConfig) {
	under := admissionModelChartConfig
	var others []admissionModelOperatorConfig
	for _, config := range admissionModelOperatorConfigs {
		if config.name == c.under {
			under = config
		}
	}
	for _, config := range admissionModelOperatorConfigs {
		if config.name != under.name {
			others = append(others, config)
		}
	}
	return under, others
}

func (c admissionModelConfiguration) String() string {
	strategy := func(s v1beta1.UpdateStrategyType) string {
		if s == "" {
			return "unset"
		}
		return string(s)
	}
	parts := []string{
		fmt.Sprintf("%d:%d", c.prefill, c.decode), fmt.Sprintf("pods%d", c.pods), fmt.Sprintf("routers%d", c.routers),
		strategy(c.engineStrategy) + "+" + strategy(c.decoderStrategy), c.group.String(),
	}
	if c.tolerance != nil {
		parts = append(parts, fmt.Sprintf("tol%d", *c.tolerance))
	}
	if c.protocol != "" {
		parts = append(parts, "protocol")
	}
	if c.partitionOn != "" {
		parts = append(parts, "partition-"+string(c.partitionOn))
	}
	if c.policy {
		parts = append(parts, "policy")
	}
	parts = append(parts, c.moment.String())
	if c.under != "" {
		parts = append(parts, "under-"+c.under)
	}
	return strings.Join(parts, " ")
}

// admissionModelRunner is a runner block carrying image.
func admissionModelRunner(image string) *v1beta1.RunnerSpec {
	return &v1beta1.RunnerSpec{Container: v1.Container{Image: image}}
}

// admissionModelLifecycle is a Component's lifecycle at a ten percent budget
// under the strategy: maxSurge for a surge, maxUnavailable for the rest. An
// empty strategy leaves the lifecycle unset.
func admissionModelLifecycle(strategy v1beta1.UpdateStrategyType) *v1beta1.LifecycleSpec {
	if strategy == "" {
		return nil
	}
	budget := &v1beta1.RollingUpdate{}
	if strategy == v1beta1.UpdateStrategySurgeThenDrain {
		budget.MaxSurge = ptr.To(intstr.FromString("10%"))
	} else {
		budget.MaxUnavailable = ptr.To(intstr.FromString("10%"))
	}
	return &v1beta1.LifecycleSpec{UpdateStrategy: &v1beta1.UpdateStrategy{Type: strategy, RollingUpdate: budget}}
}

// admissionModelExtension is the common block of a Component: OMENative,
// sized at replicas, under the strategy.
func admissionModelExtension(replicas int, strategy v1beta1.UpdateStrategyType) v1beta1.ComponentExtensionSpec {
	return v1beta1.ComponentExtensionSpec{
		Annotations: map[string]string{constants.DeploymentMode: string(constants.OMENative)},
		MinReplicas: ptr.To(replicas), MaxReplicas: replicas,
		Lifecycle: admissionModelLifecycle(strategy),
	}
}

// admissionModelGang fills a Component's pod shape: a single runner, or a
// leader with pods-1 workers, every pod carrying the image.
func admissionModelGang(pods int, image string) (runner *v1beta1.RunnerSpec, leader *v1beta1.LeaderSpec, worker *v1beta1.WorkerSpec) {
	if pods <= 1 {
		return admissionModelRunner(image), nil, nil
	}
	return nil, &v1beta1.LeaderSpec{Runner: admissionModelRunner(image)}, &v1beta1.WorkerSpec{Size: ptr.To(pods - 1), Runner: admissionModelRunner(image)}
}

// admissionModelGroup is the configuration's rollout group over the engine
// and the decoder, nil for none.
func (c admissionModelConfiguration) rolloutGroup() *v1beta1.RolloutGroup {
	g := &v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}}
	switch c.group {
	case admissionModelNoGroup:
		return nil
	case admissionModelRollingGroup:
		g.RollingUpdate = &v1beta1.GroupRollingUpdate{}
	case admissionModelRatioGroup:
		g.RollingUpdate = &v1beta1.GroupRollingUpdate{}
		g.MaintainRatio = &v1beta1.MaintainRatio{Tolerance: c.tolerance}
	case admissionModelBlueGreenGroup:
		g.BlueGreen = &v1beta1.GroupBlueGreen{}
	case admissionModelCanaryGroup:
		g.Canary = admissionModelCanaryPlan()
	}
	return g
}

// admissionModelCanaryPlan is a three-step canary: a tenth, a half, all.
func admissionModelCanaryPlan() *v1beta1.GroupCanary {
	return &v1beta1.GroupCanary{Steps: []v1beta1.RolloutGroupStep{
		{Capacity: intstr.FromString("10%"), Traffic: 10},
		{Capacity: intstr.FromString("50%"), Traffic: 50},
		{Capacity: intstr.FromString("100%"), Traffic: 100},
	}}
}

// admissionModelPolicy is a proportional scaling policy anchored on the
// engine with the decoder following at the configuration's ratio.
func (c admissionModelConfiguration) scalingPolicy() *v1beta1.ScalingPolicy {
	ratio := resource.MustParse(strconv.FormatFloat(float64(c.decode)/float64(c.prefill), 'f', 3, 64))
	return &v1beta1.ScalingPolicy{Mode: v1beta1.ScalingProportional, Proportional: &v1beta1.ProportionalPolicy{
		Anchor: v1beta1.EngineComponent, Ratios: map[v1beta1.ComponentType]resource.Quantity{v1beta1.DecoderComponent: ratio},
	}}
}

// build is the service as the operator writes it: a named runtime that is
// not kept in sync (so admission resolves no runtime), an OMENative engine
// and decoder at the shape, routers beside them under SurgeThenDrain, and
// the configuration's group, protocol, partition, policy and moment.
func (c admissionModelConfiguration) build() *v1beta1.InferenceService {
	isvc := readyPolicyISVC(nil)
	isvc.Name = "model-service"
	engine := &v1beta1.EngineSpec{ComponentExtensionSpec: admissionModelExtension(c.prefill, c.engineStrategy)}
	engine.Runner, engine.Leader, engine.Worker = admissionModelGang(c.pods, admissionModelImageV1)
	decoder := &v1beta1.DecoderSpec{ComponentExtensionSpec: admissionModelExtension(c.decode, c.decoderStrategy)}
	decoder.Runner, decoder.Leader, decoder.Worker = admissionModelGang(c.pods, admissionModelImageV1)
	isvc.Spec.Engine, isvc.Spec.Decoder = engine, decoder
	if c.routers > 0 {
		isvc.Spec.Router = &v1beta1.RouterSpec{
			ComponentExtensionSpec: admissionModelExtension(c.routers, v1beta1.UpdateStrategySurgeThenDrain),
			Runner:                 admissionModelRunner(admissionModelImageV1),
		}
	}
	if g := c.rolloutGroup(); g != nil || c.protocol != "" {
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{}
		if g != nil {
			isvc.Spec.Rollout.Groups = []v1beta1.RolloutGroup{*g}
		}
		if c.protocol != "" {
			isvc.Spec.Rollout.PairingProtocol = ptr.To(c.protocol)
		}
	}
	if c.partitionOn != "" {
		admissionModelSetPartition(isvc, c.partitionOn, ptr.To[int32](2))
	}
	if c.policy {
		isvc.Spec.ScalingPolicy = c.scalingPolicy()
	}
	switch c.moment {
	case admissionModelRunOpen:
		g := c.rolloutGroup()
		if g == nil {
			g = &v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.EngineComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}}
		}
		isvc.Status.Rollout = &v1beta1.RolloutStatus{ActiveRun: &v1beta1.RolloutRun{
			RunID: "run-1", OpenedAt: metav1.Now(),
			Plan: v1beta1.RolloutRunPlan{Groups: []v1beta1.RolloutRunGroup{{Source: v1beta1.RolloutPlanSourceInline, Group: *g}}},
		}}
	case admissionModelPaused:
		admissionModelAnnotate(isvc, constants.PausedRolloutAnnotation, "true")
	}
	return isvc
}

// admissionModelAnnotate sets one annotation on the object.
func admissionModelAnnotate(isvc *v1beta1.InferenceService, key, value string) {
	if isvc.Annotations == nil {
		isvc.Annotations = map[string]string{}
	}
	isvc.Annotations[key] = value
}

// admissionModelUnannotate removes the annotations and reports whether any
// was present.
func admissionModelUnannotate(isvc *v1beta1.InferenceService, keys ...string) bool {
	present := false
	for _, key := range keys {
		if _, ok := isvc.Annotations[key]; ok {
			present = true
			delete(isvc.Annotations, key)
		}
	}
	return present
}

// admissionModelExtensionOf is the common block of a declared Component,
// nil for one the spec does not declare.
func admissionModelExtensionOf(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) *v1beta1.ComponentExtensionSpec {
	switch c {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine != nil {
			return &isvc.Spec.Engine.ComponentExtensionSpec
		}
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder != nil {
			return &isvc.Spec.Decoder.ComponentExtensionSpec
		}
	case v1beta1.RouterComponent:
		if isvc.Spec.Router != nil {
			return &isvc.Spec.Router.ComponentExtensionSpec
		}
	}
	return nil
}

// admissionModelReplicas is the count a side is written at.
func admissionModelReplicas(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) int {
	ext := admissionModelExtensionOf(isvc, c)
	if ext == nil || ext.MinReplicas == nil {
		return 0
	}
	return *ext.MinReplicas
}

// admissionModelSetReplicas writes a side's count on both replica fields,
// the way the configurations are written.
func admissionModelSetReplicas(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, n int) bool {
	ext := admissionModelExtensionOf(isvc, c)
	if ext == nil {
		return false
	}
	ext.MinReplicas, ext.MaxReplicas = ptr.To(n), n
	return true
}

// admissionModelRollingUpdate is a side's rollingUpdate block, created when
// create is set.
func admissionModelRollingUpdate(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, create bool) *v1beta1.RollingUpdate {
	ext := admissionModelExtensionOf(isvc, c)
	if ext == nil {
		return nil
	}
	if ext.Lifecycle == nil || ext.Lifecycle.UpdateStrategy == nil || ext.Lifecycle.UpdateStrategy.RollingUpdate == nil {
		if !create {
			return nil
		}
		if ext.Lifecycle == nil {
			ext.Lifecycle = &v1beta1.LifecycleSpec{}
		}
		if ext.Lifecycle.UpdateStrategy == nil {
			ext.Lifecycle.UpdateStrategy = &v1beta1.UpdateStrategy{}
		}
		ext.Lifecycle.UpdateStrategy.RollingUpdate = &v1beta1.RollingUpdate{}
	}
	return ext.Lifecycle.UpdateStrategy.RollingUpdate
}

// admissionModelPartition is a side's partition, nil for none.
func admissionModelPartition(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) *int32 {
	ru := admissionModelRollingUpdate(isvc, c, false)
	if ru == nil {
		return nil
	}
	return ru.Partition
}

// admissionModelSetPartition writes a side's partition; nil clears it.
func admissionModelSetPartition(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, p *int32) bool {
	ru := admissionModelRollingUpdate(isvc, c, p != nil)
	if ru == nil {
		return false
	}
	ru.Partition = p
	return true
}

// admissionModelSetStrategy writes a side's update strategy at ten percent,
// keeping its partition; an empty strategy unsets the lifecycle block.
func admissionModelSetStrategy(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, strategy v1beta1.UpdateStrategyType) bool {
	ext := admissionModelExtensionOf(isvc, c)
	if ext == nil {
		return false
	}
	partition := admissionModelPartition(isvc, c)
	ext.Lifecycle = admissionModelLifecycle(strategy)
	if partition != nil {
		admissionModelSetPartition(isvc, c, partition)
	}
	return true
}

// admissionModelStrategy is a side's declared strategy type, empty when unset.
func admissionModelStrategy(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) v1beta1.UpdateStrategyType {
	ext := admissionModelExtensionOf(isvc, c)
	if ext == nil || ext.Lifecycle == nil || ext.Lifecycle.UpdateStrategy == nil {
		return ""
	}
	return ext.Lifecycle.UpdateStrategy.Type
}

// admissionModelSetImage writes the image on every pod of a side.
func admissionModelSetImage(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, image string) bool {
	var runner *v1beta1.RunnerSpec
	var leader *v1beta1.LeaderSpec
	var worker *v1beta1.WorkerSpec
	switch c {
	case v1beta1.EngineComponent:
		if isvc.Spec.Engine == nil {
			return false
		}
		runner, leader, worker = isvc.Spec.Engine.Runner, isvc.Spec.Engine.Leader, isvc.Spec.Engine.Worker
	case v1beta1.DecoderComponent:
		if isvc.Spec.Decoder == nil {
			return false
		}
		runner, leader, worker = isvc.Spec.Decoder.Runner, isvc.Spec.Decoder.Leader, isvc.Spec.Decoder.Worker
	case v1beta1.RouterComponent:
		if isvc.Spec.Router == nil {
			return false
		}
		runner = isvc.Spec.Router.Runner
	}
	if runner != nil {
		runner.Image = image
	}
	if leader != nil && leader.Runner != nil {
		leader.Runner.Image = image
	}
	if worker != nil && worker.Runner != nil {
		worker.Runner.Image = image
	}
	return true
}

// admissionModelWorkers is the worker block of the engine, nil on a single pod.
func admissionModelWorkers(isvc *v1beta1.InferenceService) *v1beta1.WorkerSpec {
	if isvc.Spec.Engine == nil {
		return nil
	}
	return isvc.Spec.Engine.Worker
}

// admissionModelGroupOf is the first rollout group, nil for none.
func admissionModelGroupOf(isvc *v1beta1.InferenceService) *v1beta1.RolloutGroup {
	if isvc.Spec.Rollout == nil || len(isvc.Spec.Rollout.Groups) == 0 {
		return nil
	}
	return &isvc.Spec.Rollout.Groups[0]
}

// admissionModelSetGroups replaces the rollout groups, keeping the protocol;
// with none left and no protocol the rollout block goes.
func admissionModelSetGroups(isvc *v1beta1.InferenceService, groups ...v1beta1.RolloutGroup) {
	if isvc.Spec.Rollout == nil {
		isvc.Spec.Rollout = &v1beta1.RolloutSpec{}
	}
	isvc.Spec.Rollout.Groups = groups
	if len(groups) == 0 && isvc.Spec.Rollout.PairingProtocol == nil {
		isvc.Spec.Rollout = nil
	}
}

// admissionModelGroupKindOf reads the kind a group declares.
func admissionModelGroupKindOf(g *v1beta1.RolloutGroup) v1beta1.RolloutProgressionKind {
	return g.DeclaredProgression()
}

// admissionModelMakeGroup rewrites a group's kind, keeping its members and
// ratio rule.
func admissionModelMakeGroup(g *v1beta1.RolloutGroup, kind v1beta1.RolloutProgressionKind) {
	g.Canary, g.BlueGreen, g.RollingUpdate = nil, nil, nil
	switch kind {
	case v1beta1.RolloutProgressionCanary:
		g.Canary = admissionModelCanaryPlan()
	case v1beta1.RolloutProgressionBlueGreen:
		g.BlueGreen = &v1beta1.GroupBlueGreen{}
	case v1beta1.RolloutProgressionRollingUpdate:
		g.RollingUpdate = &v1beta1.GroupRollingUpdate{}
	}
}

// admissionModelHasMember reports whether a group names the Component.
func admissionModelHasMember(g *v1beta1.RolloutGroup, c v1beta1.ComponentType) bool {
	for _, m := range g.Components {
		if m == c {
			return true
		}
	}
	return false
}

// admissionModelMode is a side's declared backend annotation.
func admissionModelMode(isvc *v1beta1.InferenceService, c v1beta1.ComponentType) string {
	if ext := admissionModelExtensionOf(isvc, c); ext != nil {
		return ext.Annotations[constants.DeploymentMode]
	}
	return ""
}

// admissionModelSetMode writes a side's backend annotation.
func admissionModelSetMode(isvc *v1beta1.InferenceService, c v1beta1.ComponentType, mode constants.DeploymentModeType) bool {
	ext := admissionModelExtensionOf(isvc, c)
	if ext == nil {
		return false
	}
	if ext.Annotations == nil {
		ext.Annotations = map[string]string{}
	}
	ext.Annotations[constants.DeploymentMode] = string(mode)
	return true
}

// admissionModelWrite is one write an operator makes to a stored object.
type admissionModelWrite struct {
	name string
	// apply edits the object and reports whether the write applies to it (a
	// partition cannot be cleared where none is set).
	apply func(isvc *v1beta1.InferenceService) bool
	// owns lists the reason tokens, and message fragments, of the rules that
	// read the fields this write changes; a refusal outside them is a
	// refusal for an untouched field.
	owns []string
	// verb marks the operator verbs, which must always be admitted on a
	// stored object, and the unchanged rewrite.
	verb bool
}

// The rules each family of fields is read by, named by the reason token
// their refusal carries, or by a fragment of the text when it carries none.
var (
	admissionModelReplicaRules = []string{"minReplicas", "maxReplicas", "InvalidScaleToZero", validation.ReasonRatioDrainInfeasible}
	admissionModelRolloutRules = []string{
		validation.ReasonCanaryInvalid, validation.ReasonCanaryRequiresOMENative, validation.ReasonMultipleCanaryGroups,
		validation.ReasonMaintainRatioUnpaired, validation.ReasonRatioToleranceOutOfRange, validation.ReasonZeroBudgetPacingUnstartable,
		validation.ReasonOrderNotHonored, validation.ReasonGroupOrderingNotHonored,
		validation.ReasonRatioDrainInfeasible, validation.ReasonRouteProviderUnsupported,
		validation.ReasonInvalidRollingUpdatePercent, validation.ReasonInvalidRollingUpdateInteger,
		validation.ReasonCoordinationRequiresOMENative,
		validation.ReasonInvalidComponentInCoordinationGroup, validation.ReasonDuplicateComponentInCoordinationGroups,
		validation.ReasonInvalidCoordinationOrder, validation.ReasonRolloutPolicyRefInvalid, validation.ReasonRolloutPolicyRefUnsupported,
		validation.ReasonRolloutPlanTooLarge,
	}
	admissionModelProtocolRules = []string{validation.ReasonPairingProtocolChangeUncoordinated, validation.ReasonPairingProtocolChangeWithEncoder, validation.ReasonEncoderAddedWithPairingProtocol}
	admissionModelPolicyRules   = []string{"spec.scalingPolicy", "Proportional scaling", "follower", validation.ReasonScalingModeNotImplemented}
	admissionModelStrategyRules = []string{validation.ReasonRatioDrainInfeasible, validation.ReasonRollingUpdateZeroBudget, validation.ReasonInvalidRollingUpdatePercent, validation.ReasonInvalidRollingUpdateInteger, validation.ReasonInvalidMinReadySeconds}
	admissionModelScalerRules   = []string{
		validation.ReasonAutoscalerClassUnknown, validation.ReasonKedaTriggersRequired, validation.ReasonHPAMetricMalformed,
		validation.ReasonKedaIdleBelowMin, validation.ReasonKedaHPANameInvalid, validation.ReasonAutoscalerAnnotationConflict,
		"follower autoscaler", "Proportional scaling",
	}
	admissionModelBackendRules = []string{
		ReasonDeploymentModeChangeUnderOpenRun, "InvalidDeploymentModeCombination", validation.ReasonCoordinationRequiresOMENative,
		validation.ReasonCanaryRequiresOMENative, "Proportional scaling requires OMENative", "readyPolicy",
	}
	admissionModelReadyPolicyRules = []string{"readyPolicy"}
	admissionModelGangRules        = []string{validation.ReasonWorkerSizeMustBePositive, validation.ReasonInvalidLeaderWorkerPairing, validation.ReasonLeaderRequiresWorker, validation.ReasonWorkerRequiresLeader}
)

// admissionModelHalf is half a count, at least one.
func admissionModelHalf(n int) int {
	if n/2 < 1 {
		return 1
	}
	return n / 2
}

// admissionModelScale is a write that moves one side's count by delta.
func admissionModelScale(name string, c v1beta1.ComponentType, delta int) admissionModelWrite {
	return admissionModelWrite{name: name, owns: admissionModelReplicaRules, apply: func(isvc *v1beta1.InferenceService) bool {
		n := admissionModelReplicas(isvc, c) + delta
		return n >= 1 && admissionModelSetReplicas(isvc, c, n)
	}}
}

// admissionModelPartitionWrites are the partition moves on one side.
func admissionModelPartitionWrites(c v1beta1.ComponentType) []admissionModelWrite {
	side := string(c)
	return []admissionModelWrite{
		{name: "set a partition of two on the " + side, apply: func(isvc *v1beta1.InferenceService) bool {
			return admissionModelReplicas(isvc, c) > 2 && admissionModelSetPartition(isvc, c, ptr.To[int32](2))
		}},
		{name: "raise the " + side + " partition to half", apply: func(isvc *v1beta1.InferenceService) bool {
			if admissionModelPartition(isvc, c) == nil {
				return false
			}
			return admissionModelSetPartition(isvc, c, ptr.To(int32(admissionModelHalf(admissionModelReplicas(isvc, c)))))
		}},
		{name: "raise the " + side + " partition to the whole side", apply: func(isvc *v1beta1.InferenceService) bool {
			return admissionModelPartition(isvc, c) != nil && admissionModelSetPartition(isvc, c, ptr.To(int32(admissionModelReplicas(isvc, c))))
		}},
		{name: "lower the " + side + " partition to one", apply: func(isvc *v1beta1.InferenceService) bool {
			p := admissionModelPartition(isvc, c)
			return p != nil && *p > 1 && admissionModelSetPartition(isvc, c, ptr.To[int32](1))
		}},
		{name: "clear the " + side + " partition", apply: func(isvc *v1beta1.InferenceService) bool {
			return admissionModelPartition(isvc, c) != nil && admissionModelSetPartition(isvc, c, nil)
		}},
	}
}

// admissionModelStrategyWrites are the strategy changes on one side.
func admissionModelStrategyWrites(c v1beta1.ComponentType, strategies ...v1beta1.UpdateStrategyType) []admissionModelWrite {
	var out []admissionModelWrite
	for _, s := range strategies {
		s := s
		name := fmt.Sprintf("give the %s the %s strategy", c, s)
		if s == "" {
			name = fmt.Sprintf("unset the %s strategy", c)
		}
		out = append(out, admissionModelWrite{name: name, owns: admissionModelStrategyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return admissionModelStrategy(isvc, c) != s && admissionModelSetStrategy(isvc, c, s)
		}})
	}
	return out
}

// admissionModelGroupWrites are the writes on the rollout group and its
// ratio rule.
func admissionModelGroupWrites() []admissionModelWrite {
	pair := []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent}
	withGroup := func(name string, edit func(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool) admissionModelWrite {
		return admissionModelWrite{name: name, owns: admissionModelRolloutRules, apply: func(isvc *v1beta1.InferenceService) bool {
			g := admissionModelGroupOf(isvc)
			return g != nil && edit(isvc, g)
		}}
	}
	return []admissionModelWrite{
		{name: "add a rollingUpdate group over the pair", owns: admissionModelRolloutRules, apply: func(isvc *v1beta1.InferenceService) bool {
			if admissionModelGroupOf(isvc) != nil {
				return false
			}
			admissionModelSetGroups(isvc, v1beta1.RolloutGroup{Components: pair, RollingUpdate: &v1beta1.GroupRollingUpdate{}})
			return true
		}},
		{name: "add a canary group over the pair", owns: admissionModelRolloutRules, apply: func(isvc *v1beta1.InferenceService) bool {
			if admissionModelGroupOf(isvc) != nil {
				return false
			}
			admissionModelSetGroups(isvc, v1beta1.RolloutGroup{Components: pair, Canary: admissionModelCanaryPlan()})
			return true
		}},
		withGroup("remove the group", func(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			admissionModelSetGroups(isvc)
			return true
		}),
		withGroup("make the group blueGreen", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if admissionModelGroupKindOf(g) == v1beta1.RolloutProgressionBlueGreen {
				return false
			}
			admissionModelMakeGroup(g, v1beta1.RolloutProgressionBlueGreen)
			return true
		}),
		withGroup("make the group rollingUpdate", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if admissionModelGroupKindOf(g) == v1beta1.RolloutProgressionRollingUpdate {
				return false
			}
			admissionModelMakeGroup(g, v1beta1.RolloutProgressionRollingUpdate)
			return true
		}),
		withGroup("make the group a canary", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if admissionModelGroupKindOf(g) == v1beta1.RolloutProgressionCanary {
				return false
			}
			admissionModelMakeGroup(g, v1beta1.RolloutProgressionCanary)
			return true
		}),
		withGroup("add the routers to the group", func(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if isvc.Spec.Router == nil || admissionModelHasMember(g, v1beta1.RouterComponent) {
				return false
			}
			g.Components = append(g.Components, v1beta1.RouterComponent)
			return true
		}),
		withGroup("take the routers out of the group", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if !admissionModelHasMember(g, v1beta1.RouterComponent) {
				return false
			}
			g.Components = pair
			return true
		}),
		withGroup("add a second group for the routers", func(isvc *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if isvc.Spec.Router == nil || admissionModelHasMember(g, v1beta1.RouterComponent) || len(isvc.Spec.Rollout.Groups) > 1 {
				return false
			}
			isvc.Spec.Rollout.Groups = append(isvc.Spec.Rollout.Groups, v1beta1.RolloutGroup{Components: []v1beta1.ComponentType{v1beta1.RouterComponent}, BlueGreen: &v1beta1.GroupBlueGreen{}})
			return true
		}),
		withGroup("add the ratio rule", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if g.MaintainRatio != nil {
				return false
			}
			g.MaintainRatio = &v1beta1.MaintainRatio{}
			return true
		}),
		withGroup("remove the ratio rule", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if g.MaintainRatio == nil {
				return false
			}
			g.MaintainRatio = nil
			return true
		}),
		withGroup("widen the tolerance to 20", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if g.MaintainRatio == nil {
				return false
			}
			g.MaintainRatio.Tolerance = ptr.To[int32](20)
			return true
		}),
		withGroup("narrow the tolerance to 5", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if g.MaintainRatio == nil {
				return false
			}
			g.MaintainRatio.Tolerance = ptr.To[int32](5)
			return true
		}),
		withGroup("set the tolerance to zero", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if g.MaintainRatio == nil {
				return false
			}
			g.MaintainRatio.Tolerance = ptr.To[int32](0)
			return true
		}),
		withGroup("set the tolerance to 101", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if g.MaintainRatio == nil {
				return false
			}
			g.MaintainRatio.Tolerance = ptr.To[int32](101)
			return true
		}),
		withGroup("leave the tolerance to the operator configuration", func(_ *v1beta1.InferenceService, g *v1beta1.RolloutGroup) bool {
			if g.MaintainRatio == nil || g.MaintainRatio.Tolerance == nil {
				return false
			}
			g.MaintainRatio.Tolerance = nil
			return true
		}),
	}
}

// admissionModelWrites lists every write tried on a stored object, built
// from its counts so a half step is half of what it holds.
func admissionModelWrites(stored *v1beta1.InferenceService) []admissionModelWrite {
	engine, decoder := v1beta1.EngineComponent, v1beta1.DecoderComponent
	prefill, decode := admissionModelReplicas(stored, engine), admissionModelReplicas(stored, decoder)
	verb := func(name string, apply func(isvc *v1beta1.InferenceService) bool) admissionModelWrite {
		return admissionModelWrite{name: name, apply: apply, verb: true}
	}
	annotate := func(key, value string) func(isvc *v1beta1.InferenceService) bool {
		return func(isvc *v1beta1.InferenceService) bool {
			if isvc.Annotations[key] == value {
				return false
			}
			admissionModelAnnotate(isvc, key, value)
			return true
		}
	}
	push := func(name string, sides ...v1beta1.ComponentType) admissionModelWrite {
		return admissionModelWrite{name: name, apply: func(isvc *v1beta1.InferenceService) bool {
			applied := false
			for _, c := range sides {
				applied = admissionModelSetImage(isvc, c, admissionModelImageV2) || applied
			}
			return applied
		}}
	}
	writes := []admissionModelWrite{
		verb("write it back unchanged", func(*v1beta1.InferenceService) bool { return true }),
		verb("pause", annotate(constants.PausedRolloutAnnotation, "true")),
		verb("resume", func(isvc *v1beta1.InferenceService) bool {
			return admissionModelUnannotate(isvc, constants.PausedRolloutAnnotation)
		}),
		verb("roll back", annotate(constants.RolloutRollbackAnnotation, "true")),
		verb("promote the canary", annotate(constants.RolloutPromoteAnnotation, admissionModelHash)),
		verb("resume the canary", annotate(constants.RolloutResumeAnnotation, admissionModelHash)),
		verb("clear the verbs", func(isvc *v1beta1.InferenceService) bool {
			return admissionModelUnannotate(isvc, constants.RolloutRollbackAnnotation, constants.RolloutPromoteAnnotation, constants.RolloutResumeAnnotation)
		}),
		push("push both sides", engine, decoder),
		push("push the engine", engine),
		push("push the decoder", decoder),
		push("push the routers", v1beta1.RouterComponent),
		{name: "add a worker to every gang", apply: func(isvc *v1beta1.InferenceService) bool {
			w := admissionModelWorkers(isvc)
			if w == nil || w.Size == nil {
				return false
			}
			w.Size = ptr.To(*w.Size + 1)
			if isvc.Spec.Decoder != nil && isvc.Spec.Decoder.Worker != nil && isvc.Spec.Decoder.Worker.Size != nil {
				isvc.Spec.Decoder.Worker.Size = ptr.To(*isvc.Spec.Decoder.Worker.Size + 1)
			}
			return true
		}},
		{name: "set the engine worker size to zero", owns: admissionModelGangRules, apply: func(isvc *v1beta1.InferenceService) bool {
			w := admissionModelWorkers(isvc)
			if w == nil {
				return false
			}
			w.Size = ptr.To(0)
			return true
		}},
		admissionModelScale("scale the engine up by one", engine, 1),
		admissionModelScale("scale the engine down by one", engine, -1),
		admissionModelScale("scale the engine up by half", engine, admissionModelHalf(prefill)),
		admissionModelScale("scale the engine down by half", engine, -admissionModelHalf(prefill)),
		admissionModelScale("scale the decoder up by one", decoder, 1),
		admissionModelScale("scale the decoder down by one", decoder, -1),
		admissionModelScale("scale the decoder up by half", decoder, admissionModelHalf(decode)),
		admissionModelScale("scale the decoder down by half", decoder, -admissionModelHalf(decode)),
		{name: "scale both sides up by one", owns: admissionModelReplicaRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return admissionModelSetReplicas(isvc, engine, prefill+1) && admissionModelSetReplicas(isvc, decoder, decode+1)
		}},
		{name: "scale both sides down by one", owns: admissionModelReplicaRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return prefill > 1 && decode > 1 && admissionModelSetReplicas(isvc, engine, prefill-1) && admissionModelSetReplicas(isvc, decoder, decode-1)
		}},
		{name: "change the ratio: engine up by half, decoder down by half", owns: admissionModelReplicaRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return decode > 1 && admissionModelSetReplicas(isvc, engine, prefill+admissionModelHalf(prefill)) && admissionModelSetReplicas(isvc, decoder, decode-admissionModelHalf(decode))
		}},
		{name: "scale the decoder to one", owns: admissionModelReplicaRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return decode != 1 && admissionModelSetReplicas(isvc, decoder, 1)
		}},
		{name: "scale the engine to one", owns: admissionModelReplicaRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return prefill != 1 && admissionModelSetReplicas(isvc, engine, 1)
		}},
		{name: "scale the decoder to zero", owns: admissionModelReplicaRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return admissionModelSetReplicas(isvc, decoder, 0)
		}},
		{name: "set the pairing protocol to v2", owns: admissionModelProtocolRules, apply: func(isvc *v1beta1.InferenceService) bool {
			if isvc.Spec.Rollout == nil {
				isvc.Spec.Rollout = &v1beta1.RolloutSpec{}
			}
			isvc.Spec.Rollout.PairingProtocol = ptr.To("v2")
			return true
		}},
		{name: "clear the pairing protocol", owns: admissionModelProtocolRules, apply: func(isvc *v1beta1.InferenceService) bool {
			if isvc.Spec.Rollout == nil || isvc.Spec.Rollout.PairingProtocol == nil {
				return false
			}
			isvc.Spec.Rollout.PairingProtocol = nil
			if len(isvc.Spec.Rollout.Groups) == 0 {
				isvc.Spec.Rollout = nil
			}
			return true
		}},
		{name: "add a scaling policy", owns: admissionModelPolicyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			if isvc.Spec.ScalingPolicy != nil {
				return false
			}
			isvc.Spec.ScalingPolicy = admissionModelConfiguration{prefill: prefill, decode: decode}.scalingPolicy()
			return true
		}},
		{name: "remove the scaling policy", owns: admissionModelPolicyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			if isvc.Spec.ScalingPolicy == nil {
				return false
			}
			isvc.Spec.ScalingPolicy = nil
			return true
		}},
		{name: "double the follower ratio", owns: admissionModelPolicyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			p := isvc.Spec.ScalingPolicy
			if p == nil || p.Proportional == nil {
				return false
			}
			ratio := p.Proportional.Ratios[decoder]
			ratio.Add(ratio)
			p.Proportional.Ratios[decoder] = ratio
			return true
		}},
		{name: "anchor the policy on the decoder", owns: admissionModelPolicyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			p := isvc.Spec.ScalingPolicy
			if p == nil || p.Proportional == nil || p.Proportional.Anchor == decoder {
				return false
			}
			p.Proportional.Anchor = decoder
			p.Proportional.Ratios = map[v1beta1.ComponentType]resource.Quantity{engine: resource.MustParse("1.5")}
			return true
		}},
		{name: "let the routers follow the policy", owns: admissionModelPolicyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			p := isvc.Spec.ScalingPolicy
			if p == nil || p.Proportional == nil || isvc.Spec.Router == nil {
				return false
			}
			p.Proportional.Ratios[v1beta1.RouterComponent] = resource.MustParse("0.1")
			return true
		}},
		{name: "let an undeclared encoder follow the policy", owns: admissionModelPolicyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			p := isvc.Spec.ScalingPolicy
			if p == nil || p.Proportional == nil {
				return false
			}
			p.Proportional.Ratios[v1beta1.EncoderComponent] = resource.MustParse("0.1")
			return true
		}},
		{name: "zero both budgets on the decoder", owns: admissionModelStrategyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			ru := admissionModelRollingUpdate(isvc, decoder, true)
			if ru == nil {
				return false
			}
			ru.MaxSurge, ru.MaxUnavailable = ptr.To(intstr.FromInt(0)), ptr.To(intstr.FromInt(0))
			return true
		}},
		{name: "add an HPA autoscaler to the decoder", owns: admissionModelScalerRules, apply: func(isvc *v1beta1.InferenceService) bool {
			ext := admissionModelExtensionOf(isvc, decoder)
			if ext == nil || ext.Autoscaler != nil {
				return false
			}
			ext.Autoscaler = &v1beta1.ComponentAutoscaler{Class: v1beta1.AutoscalerHPA}
			return true
		}},
		{name: "remove the decoder autoscaler", owns: admissionModelScalerRules, apply: func(isvc *v1beta1.InferenceService) bool {
			ext := admissionModelExtensionOf(isvc, decoder)
			if ext == nil || ext.Autoscaler == nil {
				return false
			}
			ext.Autoscaler = nil
			return true
		}},
		{name: "move the decoder to RawDeployment", owns: admissionModelBackendRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return admissionModelMode(isvc, decoder) != string(constants.RawDeployment) && admissionModelSetMode(isvc, decoder, constants.RawDeployment)
		}},
		{name: "move both sides to RawDeployment", owns: admissionModelBackendRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return admissionModelSetMode(isvc, engine, constants.RawDeployment) && admissionModelSetMode(isvc, decoder, constants.RawDeployment)
		}},
		{name: "move the decoder back to OMENative", owns: admissionModelBackendRules, apply: func(isvc *v1beta1.InferenceService) bool {
			return admissionModelMode(isvc, decoder) != string(constants.OMENative) && admissionModelSetMode(isvc, decoder, constants.OMENative)
		}},
		{name: "set readyPolicy None on the engine", owns: admissionModelReadyPolicyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			ext := admissionModelExtensionOf(isvc, engine)
			if ext.Lifecycle == nil {
				ext.Lifecycle = &v1beta1.LifecycleSpec{}
			}
			if ext.Lifecycle.ReadyPolicy != nil {
				return false
			}
			ext.Lifecycle.ReadyPolicy = ptr.To(v1beta1.InstanceReadyPolicyNone)
			return true
		}},
		{name: "clear the engine readyPolicy", owns: admissionModelReadyPolicyRules, apply: func(isvc *v1beta1.InferenceService) bool {
			ext := admissionModelExtensionOf(isvc, engine)
			if ext.Lifecycle == nil || ext.Lifecycle.ReadyPolicy == nil {
				return false
			}
			ext.Lifecycle.ReadyPolicy = nil
			return true
		}},
	}
	writes = append(writes, admissionModelPartitionWrites(decoder)...)
	writes = append(writes, admissionModelPartitionWrites(engine)...)
	writes = append(writes, admissionModelStrategyWrites(engine, v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible, "")...)
	writes = append(writes, admissionModelStrategyWrites(decoder, v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, "")...)
	writes = append(writes, admissionModelGroupWrites()...)
	return writes
}

// admissionModelFinding is the first state a class was seen in.
type admissionModelFinding struct {
	class, message, write, state string
}

// admissionModelWorld drives one configuration: the validator of the
// operator configuration it was written under, and the validators of the
// configurations a restarted manager may load instead.
type admissionModelWorld struct {
	t         *testing.T
	cfg       admissionModelConfiguration
	config    admissionModelOperatorConfig
	v         *InferenceServiceValidator
	restarted []admissionModelOperatorConfig
	counts    map[string]int
	findings  map[string]admissionModelFinding
	seen      map[string]struct{}
	probes    int
	// write names the write under way, for the record of a finding.
	write string
}

func newAdmissionModelWorld(t *testing.T, cfg admissionModelConfiguration, config admissionModelOperatorConfig, restarted ...admissionModelOperatorConfig) *admissionModelWorld {
	return &admissionModelWorld{t: t, cfg: cfg, config: config, v: config.validator(), restarted: restarted,
		counts: map[string]int{}, findings: map[string]admissionModelFinding{}, seen: map[string]struct{}{}}
}

// note records a state in its class: the count, and the first one with the
// write and the picture.
func (w *admissionModelWorld) note(class string, isvc *v1beta1.InferenceService, format string, args ...any) {
	w.counts[class]++
	if _, ok := w.findings[class]; ok {
		return
	}
	w.findings[class] = admissionModelFinding{class: class, message: fmt.Sprintf(format, args...), write: w.write, state: admissionModelPicture(isvc)}
}

// admissionModelPicture is the object in one line: each side's count,
// strategy and backend, the group, the protocol, the policy, the verbs.
func admissionModelPicture(isvc *v1beta1.InferenceService) string {
	var parts []string
	for _, c := range []v1beta1.ComponentType{v1beta1.EngineComponent, v1beta1.DecoderComponent, v1beta1.RouterComponent} {
		ext := admissionModelExtensionOf(isvc, c)
		if ext == nil {
			continue
		}
		side := fmt.Sprintf("%s %d", c, admissionModelReplicas(isvc, c))
		if s := admissionModelStrategy(isvc, c); s != "" {
			side += " " + string(s)
		}
		if p := admissionModelPartition(isvc, c); p != nil {
			side += fmt.Sprintf(" partition %d", *p)
		}
		if m := admissionModelMode(isvc, c); m != string(constants.OMENative) {
			side += " " + m
		}
		if ext.Autoscaler != nil {
			side += " autoscaler " + string(ext.Autoscaler.Class)
		}
		parts = append(parts, side)
	}
	if w := admissionModelWorkers(isvc); w != nil && w.Size != nil {
		parts = append(parts, fmt.Sprintf("%d workers", *w.Size))
	}
	for i, g := range isvc.Spec.GetRolloutGroups() {
		desc := fmt.Sprintf("group %d %s %v", i, g.DeclaredProgression(), g.Components)
		if g.MaintainRatio != nil {
			desc += " ratio"
			if g.MaintainRatio.Tolerance != nil {
				desc += fmt.Sprintf(" tol %d", *g.MaintainRatio.Tolerance)
			}
		}
		parts = append(parts, desc)
	}
	if p := isvc.Spec.RolloutPairingProtocol(); p != "" {
		parts = append(parts, "protocol "+p)
	}
	if p := isvc.Spec.ScalingPolicy; p != nil && p.Proportional != nil {
		parts = append(parts, fmt.Sprintf("policy anchor %s ratios %v", p.Proportional.Anchor, p.Proportional.Ratios))
	}
	if v1beta1.RolloutRunActive(isvc) {
		parts = append(parts, "run open")
	}
	var verbs []string
	for key, value := range isvc.Annotations {
		if strings.HasPrefix(key, "ome.io/rollout-") {
			verbs = append(verbs, strings.TrimPrefix(key, "ome.io/")+"="+value)
		}
	}
	sort.Strings(verbs)
	parts = append(parts, verbs...)
	return strings.Join(parts, "; ")
}

// admissionModelKey identifies a state by its spec and annotations.
func admissionModelKey(isvc *v1beta1.InferenceService) string {
	raw, _ := json.Marshal(struct {
		Spec        v1beta1.InferenceServiceSpec
		Annotations map[string]string
	}{isvc.Spec, isvc.Annotations})
	sum := sha256.Sum256(raw)
	return fmt.Sprintf("%x", sum[:12])
}

// seeState records a state and reports whether it is new.
func (w *admissionModelWorld) seeState(isvc *v1beta1.InferenceService) bool {
	key := admissionModelKey(isvc)
	if _, ok := w.seen[key]; ok {
		return false
	}
	w.seen[key] = struct{}{}
	return true
}

func (w *admissionModelWorld) create(isvc *v1beta1.InferenceService) error {
	w.probes++
	_, err := w.v.ValidateCreate(context.Background(), isvc)
	return err
}

func (w *admissionModelWorld) update(v *InferenceServiceValidator, stored, next *v1beta1.InferenceService) error {
	w.probes++
	_, err := v.ValidateUpdate(context.Background(), stored, next)
	return err
}

var (
	admissionModelTrailingReason = regexp.MustCompile(`\(([A-Za-z]+)\)\s*$`)
	admissionModelLeadingReason  = regexp.MustCompile(`^([A-Za-z]+): `)
	admissionModelToleranceWay   = regexp.MustCompile(`widen maintainRatio\.tolerance to at least (\d+)`)
	admissionModelSurgeWay       = regexp.MustCompile(`give the (engine|decoder|router|encoder) the SurgeThenDrain update strategy`)
	admissionModelStepWay        = regexp.MustCompile(`give the (engine|decoder)(?: and the (engine|decoder))? a SurgeThenDrain or RecreatePod update strategy`)
	admissionModelReplicaWay     = regexp.MustCompile(`add (engine|decoder|router|encoder) replicas`)
	admissionModelDrainMember    = regexp.MustCompile(`the (engine|decoder|router|encoder) rolls drain-first`)
)

// admissionModelReason is the reason token a refusal carries: the
// parenthesised word that ends it, or the word that opens it before a colon.
func admissionModelReason(err error) string {
	msg := err.Error()
	if m := admissionModelTrailingReason.FindStringSubmatch(msg); m != nil {
		return m[1]
	}
	if m := admissionModelLeadingReason.FindStringSubmatch(msg); m != nil {
		return m[1]
	}
	return ""
}

// admissionModelOwned reports whether the refusal comes from a rule that
// reads a field the write changed.
func admissionModelOwned(write admissionModelWrite, err error) bool {
	msg := err.Error()
	for _, rule := range write.owns {
		if strings.Contains(msg, rule) {
			return true
		}
	}
	return false
}

// checkWrite applies one write to the stored object and asks the webhook.
// An admitted write must be undoable: the stored object written back over
// it is admitted. A refused write is classified by refusal.
func (w *admissionModelWorld) checkWrite(stored *v1beta1.InferenceService, write admissionModelWrite) (*v1beta1.InferenceService, bool) {
	next := stored.DeepCopy()
	if !write.apply(next) {
		return nil, false
	}
	w.write = write.name
	err := w.update(w.v, stored, next)
	if err == nil {
		if undo := w.update(w.v, next, stored.DeepCopy()); undo != nil {
			w.note("no-way-back", next, "after %q the stored object written back is refused: %v", write.name, undo)
		}
		return next, true
	}
	w.refusal(stored, next, write, err)
	return nil, false
}

// refusal classifies one refused write: a verb refused, a refusal for a
// field the write did not touch, a refusal without a reason token, and for
// a ratio refusal the ways out it names and the engine's own reading.
func (w *admissionModelWorld) refusal(stored, next *v1beta1.InferenceService, write admissionModelWrite, err error) {
	token := admissionModelReason(err)
	if token == "" {
		w.note("refusal-without-a-reason", next, "%q: %v", write.name, err)
		w.counts["no-reason:"+write.name]++
	}
	switch {
	case write.verb:
		w.note("verb-refused", next, "%q is refused on a stored object: %v", write.name, err)
	case !admissionModelOwned(write, err):
		w.note("refused-for-an-untouched-field", next, "%q is refused by a rule about a field it did not touch: %v", write.name, err)
	}
	if token == validation.ReasonRatioDrainInfeasible {
		w.waysOut(stored, next, err)
		w.bandAgrees(next, err)
	}
}

// waysOut tries every way out a ratio refusal names, each on top of the
// refused write; a named way out that is refused too is a finding. stored
// is nil for a refused create.
func (w *admissionModelWorld) waysOut(stored, refused *v1beta1.InferenceService, refusal error) {
	msg := refusal.Error()
	if m := admissionModelToleranceWay.FindStringSubmatch(msg); m != nil {
		n, _ := strconv.Atoi(m[1])
		try := refused.DeepCopy()
		if g := admissionModelRatioGroupOf(try); g != nil {
			g.MaintainRatio.Tolerance = ptr.To(int32(n))
			w.expectWayOut(stored, try, "widen the tolerance to "+m[1], refusal)
		}
	}
	for _, m := range admissionModelSurgeWay.FindAllStringSubmatch(msg, -1) {
		try := refused.DeepCopy()
		admissionModelSetStrategy(try, v1beta1.ComponentType(m[1]), v1beta1.UpdateStrategySurgeThenDrain)
		w.expectWayOut(stored, try, "give the "+m[1]+" SurgeThenDrain", refusal)
	}
	if m := admissionModelStepWay.FindStringSubmatch(msg); m != nil {
		for _, strategy := range []v1beta1.UpdateStrategyType{v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyRecreatePod} {
			try := refused.DeepCopy()
			for _, side := range m[1:] {
				if side != "" {
					admissionModelSetStrategy(try, v1beta1.ComponentType(side), strategy)
				}
			}
			w.expectWayOut(stored, try, fmt.Sprintf("hand the pacing to the batch step with %s", strategy), refusal)
		}
	}
	for _, m := range admissionModelReplicaWay.FindAllStringSubmatch(msg, -1) {
		side := v1beta1.ComponentType(m[1])
		have := admissionModelReplicas(refused, side)
		limit := 4*have + 4
		if g := admissionModelRatioGroupOf(refused); g != nil {
			for _, c := range g.Components {
				if peer := 2*admissionModelReplicas(refused, c) + 2; peer > limit {
					limit = peer
				}
			}
		}
		admitted := false
		for n := have + 1; n <= limit && !admitted; n++ {
			try := refused.DeepCopy()
			admissionModelSetReplicas(try, side, n)
			admitted = w.attempt(stored, try) == nil
		}
		if !admitted {
			w.note("named-way-out-refused", refused, "the refusal names %q as a way out, and no count up to %d is admitted: %v", "add "+m[1]+" replicas", limit, refusal)
		}
	}
}

// attempt asks the webhook for try as an update of stored, or as a create
// when stored is nil.
func (w *admissionModelWorld) attempt(stored, try *v1beta1.InferenceService) error {
	if stored == nil {
		return w.create(try)
	}
	return w.update(w.v, stored, try)
}

func (w *admissionModelWorld) expectWayOut(stored, try *v1beta1.InferenceService, way string, refusal error) {
	if err := w.attempt(stored, try); err != nil {
		w.note("named-way-out-refused", try, "the refusal names %q as a way out, and that write is refused too: %v (the refusal: %v)", way, err, refusal)
	}
}

// admissionModelRatioGroupOf is the first group with a ratio rule.
func admissionModelRatioGroupOf(isvc *v1beta1.InferenceService) *v1beta1.RolloutGroup {
	if isvc.Spec.Rollout == nil {
		return nil
	}
	for i := range isvc.Spec.Rollout.Groups {
		if isvc.Spec.Rollout.Groups[i].MaintainRatio != nil {
			return &isvc.Spec.Rollout.Groups[i]
		}
	}
	return nil
}

// bandAgrees asks the engine's own ratio band the question the drain
// feasibility rule answered: the first drain of the member the refusal
// names, from a group whose every Instance serves. A drain the band admits
// and admission refuses is a finding.
func (w *admissionModelWorld) bandAgrees(refused *v1beta1.InferenceService, refusal error) {
	m := admissionModelDrainMember.FindStringSubmatch(refusal.Error())
	g := admissionModelRatioGroupOf(refused)
	if m == nil || g == nil {
		return
	}
	member := v1beta1.ComponentType(m[1])
	tolerance := g.MaintainRatio.Tolerance
	if tolerance == nil {
		tolerance = w.config.tolerance
	}
	if tolerance == nil {
		return
	}
	counts := map[v1beta1.ComponentType]int32{}
	none := map[v1beta1.ComponentType]int32{}
	for _, c := range g.Components {
		if n := admissionModelReplicas(refused, c); n > 0 {
			counts[c], none[c] = int32(n), 0
		}
	}
	state := coordination.RatioState{Original: counts, Serving: counts, Desired: counts, NewPods: none,
		InFlightSurge: map[v1beta1.ComponentType]int32{}, InFlightUnavail: map[v1beta1.ComponentType]int32{}}
	pacing := v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced, RatioTolerancePercent: tolerance}
	decision := coordination.EvaluateSurge(pacing, state, member, -1)
	if decision.AllowedSurgeDelta != 0 {
		w.note("refused-drain-the-band-admits", refused, "admission refuses the %s's first drain while the engine's ratio band admits it (%q, outside the tolerance: %v): %v",
			member, decision.Reason, decision.OutsideTolerance, refusal)
	}
}

// checkVerbs asks validator v every verb on the stored object and records a
// refusal in class.
func (w *admissionModelWorld) checkVerbs(v *InferenceServiceValidator, stored *v1beta1.InferenceService, class, under string) {
	for _, write := range admissionModelWrites(stored) {
		if !write.verb {
			continue
		}
		next := stored.DeepCopy()
		if !write.apply(next) {
			continue
		}
		w.write = write.name
		if err := w.update(v, stored, next); err != nil {
			w.note(class, next, "%q is refused %s: %v", write.name, under, err)
		}
	}
}

// checkRestarted is the manager restarted with a changed ConfigMap: the
// verbs on the stored object under each other operator configuration.
func (w *admissionModelWorld) checkRestarted(stored *v1beta1.InferenceService) {
	for _, c := range w.restarted {
		w.checkVerbs(c.validator(), stored, "verb-refused-after-a-config-change", "once the manager restarts with the "+c.name+" configuration")
	}
}

// checkStored tries every write on one stored object.
func (w *admissionModelWorld) checkStored(stored *v1beta1.InferenceService) {
	for _, write := range admissionModelWrites(stored) {
		if next, ok := w.checkWrite(stored, write); ok {
			w.seeState(next)
		}
	}
	w.checkRestarted(stored)
}

// explore writes the configuration as a create, then tries every write on
// it and plays seeded random runs of admitted writes.
func (w *admissionModelWorld) explore(seed int64, runs, depth int) {
	stored := w.cfg.build()
	w.write = "create"
	if err := w.create(stored); err != nil {
		token := admissionModelReason(err)
		if token == "" {
			w.note("refusal-without-a-reason", stored, "create: %v", err)
		}
		w.counts["create-refused:"+token]++
		if token == validation.ReasonRatioDrainInfeasible {
			w.waysOut(nil, stored, err)
			w.bandAgrees(stored, err)
		}
		// The same object written by an older manager, before the rule that
		// refuses it: the verbs must still be admitted on it.
		w.checkVerbs(w.v, stored, "verb-refused-on-an-object-stored-before-its-rule", "on an object stored before the rule that refuses it")
		return
	}
	w.seeState(stored)
	w.checkStored(stored)
	w.randomRuns(stored, seed, runs, depth)
}

// randomRuns chains admitted writes from the stored object under a fixed
// seed; every new state is checked for its verbs and the restart.
func (w *admissionModelWorld) randomRuns(stored *v1beta1.InferenceService, seed int64, runs, depth int) {
	rng := rand.New(rand.NewSource(seed))
	for r := 0; r < runs; r++ {
		current := stored
		for d := 0; d < depth; d++ {
			writes := admissionModelWrites(current)
			write := writes[rng.Intn(len(writes))]
			if write.verb {
				continue
			}
			next, ok := w.checkWrite(current, write)
			if !ok {
				continue
			}
			if w.seeState(next) {
				w.checkVerbs(w.v, next, "verb-refused", "on a stored object")
				w.checkRestarted(next)
			}
			current = next
		}
	}
}

// admissionModelShapes are the reference prefill:decode shapes.
var admissionModelShapes = [][2]int{{12, 8}, {15, 15}, {16, 8}, {20, 10}, {24, 8}, {30, 10}, {12, 4}}

// admissionModelSettings is everything but the shape, the pods and the
// routers of a configuration.
type admissionModelSettings struct {
	engine, decoder v1beta1.UpdateStrategyType
	group           admissionModelGroupKind
	tolerance       *int32
	protocol        string
	partitionOn     v1beta1.ComponentType
	policy          bool
	moment          admissionModelMoment
	under           string
}

// admissionModelReferenceSettings are the settings every run explores on
// every shape: the reference pairs, each group kind, the in-place engine
// the ratio rule reads, the unset strategies the operator fill decides, a
// protocol, a partition, a policy, a run open and a pause.
var admissionModelReferenceSettings = []admissionModelSettings{
	{engine: v1beta1.UpdateStrategyRecreatePod, decoder: v1beta1.UpdateStrategyRecreatePod, group: admissionModelRatioGroup},
	{engine: v1beta1.UpdateStrategySurgeThenDrain, decoder: v1beta1.UpdateStrategyRecreatePod, group: admissionModelRatioGroup},
	{engine: v1beta1.UpdateStrategySurgeThenDrain, decoder: v1beta1.UpdateStrategySurgeThenDrain, group: admissionModelRatioGroup, protocol: "v1"},
	{engine: v1beta1.UpdateStrategyInPlaceIfPossible, decoder: v1beta1.UpdateStrategyRecreatePod, group: admissionModelRatioGroup},
	{engine: v1beta1.UpdateStrategyInPlaceIfPossible, decoder: v1beta1.UpdateStrategyRecreatePod, group: admissionModelRatioGroup, tolerance: ptr.To[int32](20)},
	{group: admissionModelRatioGroup},
	{engine: v1beta1.UpdateStrategyRecreatePod, decoder: v1beta1.UpdateStrategyRecreatePod, group: admissionModelRollingGroup, protocol: "v1"},
	{engine: v1beta1.UpdateStrategySurgeThenDrain, decoder: v1beta1.UpdateStrategySurgeThenDrain, group: admissionModelBlueGreenGroup, protocol: "v1"},
	{engine: v1beta1.UpdateStrategySurgeThenDrain, decoder: v1beta1.UpdateStrategySurgeThenDrain, group: admissionModelCanaryGroup},
	{engine: v1beta1.UpdateStrategyRecreatePod, decoder: v1beta1.UpdateStrategyRecreatePod},
	{engine: v1beta1.UpdateStrategyRecreatePod, decoder: v1beta1.UpdateStrategyRecreatePod, group: admissionModelRatioGroup, partitionOn: v1beta1.DecoderComponent, policy: true},
	{engine: v1beta1.UpdateStrategySurgeThenDrain, decoder: v1beta1.UpdateStrategySurgeThenDrain, group: admissionModelRollingGroup, policy: true, moment: admissionModelRunOpen},
	{engine: v1beta1.UpdateStrategyRecreatePod, decoder: v1beta1.UpdateStrategyRecreatePod, group: admissionModelRatioGroup, moment: admissionModelPaused},
	{engine: v1beta1.UpdateStrategyInPlaceIfPossible, decoder: v1beta1.UpdateStrategyRecreatePod, group: admissionModelRatioGroup, under: admissionModelWideConfig.name},
	{engine: v1beta1.UpdateStrategyRecreatePod, decoder: v1beta1.UpdateStrategyRecreatePod, group: admissionModelRatioGroup, under: admissionModelWideConfig.name},
	{group: admissionModelRatioGroup, under: admissionModelUnfilledConfig.name},
}

func admissionModelConfigurationOf(shape [2]int, pods, routers int, s admissionModelSettings) admissionModelConfiguration {
	return admissionModelConfiguration{prefill: shape[0], decode: shape[1], pods: pods, routers: routers,
		engineStrategy: s.engine, decoderStrategy: s.decoder, group: s.group, tolerance: s.tolerance,
		protocol: s.protocol, partitionOn: s.partitionOn, policy: s.policy, moment: s.moment, under: s.under}
}

// admissionModelReferenceConfigs are the bounded run's configurations: every
// shape, single pods and gangs of four, with and without routers, under the
// reference settings.
func admissionModelReferenceConfigs() []admissionModelConfiguration {
	var out []admissionModelConfiguration
	for _, shape := range admissionModelShapes {
		for _, pods := range []int{1, 4} {
			for _, routers := range []int{0, 2} {
				for _, s := range admissionModelReferenceSettings {
					out = append(out, admissionModelConfigurationOf(shape, pods, routers, s))
				}
			}
		}
	}
	return out
}

// admissionModelAllConfigs is the full run: the whole product of the axes.
func admissionModelAllConfigs() []admissionModelConfiguration {
	var out []admissionModelConfiguration
	engines := []v1beta1.UpdateStrategyType{v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, v1beta1.UpdateStrategyInPlaceIfPossible, ""}
	decoders := []v1beta1.UpdateStrategyType{v1beta1.UpdateStrategyRecreatePod, v1beta1.UpdateStrategySurgeThenDrain, ""}
	groups := []admissionModelGroupKind{admissionModelNoGroup, admissionModelRollingGroup, admissionModelRatioGroup, admissionModelBlueGreenGroup, admissionModelCanaryGroup}
	partitions := []v1beta1.ComponentType{"", v1beta1.DecoderComponent, v1beta1.EngineComponent}
	moments := []admissionModelMoment{admissionModelIdle, admissionModelRunOpen, admissionModelPaused}
	for _, shape := range admissionModelShapes {
		for _, pods := range []int{1, 2, 4, 8} {
			for _, routers := range []int{0, 2} {
				for _, engine := range engines {
					for _, decoder := range decoders {
						for _, group := range groups {
							tolerances := []*int32{nil}
							unders := []string{""}
							if group == admissionModelRatioGroup {
								tolerances = append(tolerances, ptr.To[int32](20))
								unders = append(unders, admissionModelWideConfig.name, admissionModelUnfilledConfig.name)
							}
							for _, tolerance := range tolerances {
								for _, under := range unders {
									for _, protocol := range []string{"", "v1"} {
										for _, partition := range partitions {
											for _, policy := range []bool{false, true} {
												for _, moment := range moments {
													out = append(out, admissionModelConfigurationOf(shape, pods, routers, admissionModelSettings{
														engine: engine, decoder: decoder, group: group, tolerance: tolerance, under: under,
														protocol: protocol, partitionOn: partition, policy: policy, moment: moment}))
												}
											}
										}
									}
								}
							}
						}
					}
				}
			}
		}
	}
	return out
}

// admissionModelKnownClasses are the finding classes the model currently
// reports, each with the behaviour behind it. A finding in a known class is
// reported and does not fail the run; a change of behaviour removes its entry and the test
// that holds the behaviour.
var admissionModelKnownClasses = map[string]string{
	"refused-drain-the-band-admits":                    "the drain feasibility rule refuses a drain-first member whose first drain lies outside twice the tolerance and says the rollout would stop for good; the engine's band admits that first drain from a fully serving group and raises the outside-tolerance warning event instead (open question; held by TestAdmission_TheDrainFeasibilityRuleRefusesAFirstDrainTheBandAdmits)",
	"verb-refused-after-a-config-change":               "the drain feasibility rule reads the whole stored spec on every write, so once the configured tolerance narrows under a stored object every write to it is refused, the pause and the roll back included, until one write also changes the tolerance or a strategy (open question; held by TestAdmission_AStoredObjectRefusesItsPauseOnceTheToleranceNarrows)",
	"verb-refused-on-an-object-stored-before-its-rule": "the same rule on an object an older manager admitted: every write is refused until one also changes the tolerance or a strategy (the same open question)",
	"named-way-out-refused":                            "a ratio refusal at a tolerance of zero names adding replicas as a way out, and no count fits a band of zero width (fix pending; held by TestAdmission_ARatioRefusalAtToleranceZeroNamesAddingReplicasAsAWayOut)",
	"refusal-without-a-reason":                         "the scaling policy, readyPolicy and backend refusals carry no reason token",
}

// admissionModelExpectedEnd reports a class that is an end of the walk, not
// a finding: a configuration the webhook refuses at create, by token.
func admissionModelExpectedEnd(class string) bool {
	return strings.HasPrefix(class, "create-refused:") || strings.HasPrefix(class, "no-reason:")
}

type admissionModelExample struct {
	cfg     admissionModelConfiguration
	finding admissionModelFinding
}

// admissionModelResult is what one configuration's exploration found.
type admissionModelResult struct {
	cfg            admissionModelConfiguration
	states, probes int
	counts         map[string]int
	findings       map[string]admissionModelFinding
}

// admissionModelExplore runs the model over the configurations, one subtest
// per shape, pod count and router count in parallel, and reports every
// class, failing on the classes that are neither expected ends nor known.
func admissionModelExplore(t *testing.T) {
	t.Helper()
	full := os.Getenv(admissionModelFullEnv) == "full"
	only := os.Getenv(admissionModelOnlyEnv)
	runs, depth := 1, 12
	configs := admissionModelReferenceConfigs()
	if full {
		runs, depth = 4, 24
		configs = admissionModelAllConfigs()
	}
	if only != "" {
		var kept []admissionModelConfiguration
		for _, cfg := range configs {
			if strings.Contains(cfg.String(), only) {
				kept = append(kept, cfg)
			}
		}
		configs = kept
	}
	start := time.Now()
	lanes := map[string][]int{}
	var laneNames []string
	for i, cfg := range configs {
		lane := fmt.Sprintf("%d:%d pods%d routers%d", cfg.prefill, cfg.decode, cfg.pods, cfg.routers)
		if _, ok := lanes[lane]; !ok {
			laneNames = append(laneNames, lane)
		}
		lanes[lane] = append(lanes[lane], i)
	}
	results := make([]admissionModelResult, len(configs))
	t.Run("configurations", func(t *testing.T) {
		for _, lane := range laneNames {
			indices := lanes[lane]
			t.Run(strings.ReplaceAll(lane, " ", "_"), func(t *testing.T) {
				t.Parallel()
				began := time.Now()
				var states, probes int
				for _, i := range indices {
					under, others := configs[i].writtenUnder()
					w := newAdmissionModelWorld(t, configs[i], under, others...)
					w.explore(1, runs, depth)
					results[i] = admissionModelResult{cfg: configs[i], states: len(w.seen), probes: w.probes, counts: w.counts, findings: w.findings}
					states += len(w.seen)
					probes += w.probes
				}
				t.Logf("%s: %d configurations, %d states, %d probes, %s", lane, len(indices), states, probes, time.Since(began).Round(time.Millisecond))
			})
		}
	})
	totals := map[string]int{}
	configurations := map[string]int{}
	examples := map[string][]admissionModelExample{}
	var states, probes int
	for _, res := range results {
		states += res.states
		probes += res.probes
		for class, c := range res.counts {
			totals[class] += c
		}
		for class, f := range res.findings {
			configurations[class]++
			if len(examples[class]) < 4 {
				examples[class] = append(examples[class], admissionModelExample{cfg: res.cfg, finding: f})
			}
		}
	}
	classes := make([]string, 0, len(totals))
	for class := range totals {
		classes = append(classes, class)
	}
	sort.Strings(classes)
	t.Logf("%d configurations, %d states, %d probes, %s", len(configs), states, probes, time.Since(start).Round(time.Second))
	for _, class := range classes {
		t.Logf("%s: %d states in %d configurations", class, totals[class], configurations[class])
		for _, ex := range examples[class] {
			t.Logf("  [%s]\n    write: %s\n    %s\n    state: %s", ex.cfg, ex.finding.write, ex.finding.message, ex.finding.state)
		}
		switch {
		case admissionModelExpectedEnd(class):
		case admissionModelKnownClasses[class] != "":
			t.Logf("  known: %s", admissionModelKnownClasses[class])
		default:
			t.Errorf("%s: %d states (first: %s: %s)", class, totals[class], examples[class][0].cfg, examples[class][0].finding.message)
		}
	}
}

// TestAdmission_EveryOperatorWriteIsAdmittedOrRefusedForItsOwnField
// explores every configuration: on every stored object the unchanged
// rewrite and the verbs are admitted, every other write is admitted or
// refused by a rule about a field it changed with a reason token, an
// admitted write can be undone, the ways out a ratio refusal names are
// admitted, and a first drain admission refuses is one the engine's own
// band refuses too, before and after the manager restarts with a changed
// ConfigMap.
func TestAdmission_EveryOperatorWriteIsAdmittedOrRefusedForItsOwnField(t *testing.T) {
	admissionModelExplore(t)
}

// admissionModelFirstDrain is the engine's own answer to the first drain of
// member from a group whose every Instance serves, at the tolerance.
func admissionModelFirstDrain(counts map[v1beta1.ComponentType]int32, member v1beta1.ComponentType, tolerance int32) coordination.RatioDecision {
	none := map[v1beta1.ComponentType]int32{}
	for c := range counts {
		none[c] = 0
	}
	state := coordination.RatioState{Original: counts, Serving: counts, Desired: counts, NewPods: none,
		InFlightSurge: map[v1beta1.ComponentType]int32{}, InFlightUnavail: map[v1beta1.ComponentType]int32{}}
	pacing := v1beta1.CoordinationPacing{Type: v1beta1.CoordinationPacingRatioBalanced, RatioTolerancePercent: ptr.To(tolerance)}
	return coordination.EvaluateSurge(pacing, state, member, -1)
}

// admissionModelInPlacePair is 12 prefill patched in place beside 8 decode
// under RecreatePod at ten percent, one rollingUpdate group with the ratio
// rule at the operator's tolerance: the shape the drain feasibility rule
// reads, since the engine does not step.
func admissionModelInPlacePair(tolerance *int32, under string) admissionModelConfiguration {
	return admissionModelConfiguration{prefill: 12, decode: 8, pods: 1,
		engineStrategy: v1beta1.UpdateStrategyInPlaceIfPossible, decoderStrategy: v1beta1.UpdateStrategyRecreatePod,
		group: admissionModelRatioGroup, tolerance: tolerance, under: under}
}

// TestAdmission_TheDrainFeasibilityRuleRefusesAFirstDrainTheBandAdmits
// holds the picture the model found: at the chart's tolerance of 5,
// admission refuses the in-place pair, saying the decoder's first drain
// could never be admitted and the rollout would stop for good. The engine's
// own band admits that drain (7 of 8 serving beside 12, 14 points off the
// ratio) and raises the outside-tolerance warning event. When the two
// agree, this test fails, and the known class and this lock go with it.
func TestAdmission_TheDrainFeasibilityRuleRefusesAFirstDrainTheBandAdmits(t *testing.T) {
	cfg := admissionModelInPlacePair(nil, "")
	w := newAdmissionModelWorld(t, cfg, admissionModelChartConfig)
	err := w.create(cfg.build())
	require.Error(t, err)
	require.Contains(t, err.Error(), validation.ReasonRatioDrainInfeasible)
	require.Contains(t, err.Error(), "could never be admitted")
	decision := admissionModelFirstDrain(map[v1beta1.ComponentType]int32{v1beta1.EngineComponent: 12, v1beta1.DecoderComponent: 8}, v1beta1.DecoderComponent, 5)
	if decision.AllowedSurgeDelta == 0 {
		t.Fatalf("the picture moved: the band refuses the first drain too (%s); remove the known class and this lock", decision.Reason)
	}
	if !decision.OutsideTolerance {
		t.Fatalf("the picture moved: the band admits the first drain inside the tolerance (%s); if admission admits the pair now, remove the known class and this lock", decision.Reason)
	}
	t.Logf("admission: %v", err)
	t.Logf("the band: %s (outside the tolerance: %v)", decision.Reason, decision.OutsideTolerance)
}

// TestAdmission_AStoredObjectRefusesItsPauseOnceTheToleranceNarrows holds
// the picture: the in-place pair written while the configured tolerance was
// 20, admitted; the manager restarts with the chart's 5; the operator's
// pause, and the roll back, are refused by the drain feasibility rule,
// which reads the whole stored spec on every write. When a verb is
// admitted on such an object, this test fails, and the known classes and
// this lock go with it.
func TestAdmission_AStoredObjectRefusesItsPauseOnceTheToleranceNarrows(t *testing.T) {
	cfg := admissionModelInPlacePair(nil, admissionModelWideConfig.name)
	under, _ := cfg.writtenUnder()
	w := newAdmissionModelWorld(t, cfg, under)
	stored := cfg.build()
	require.NoError(t, w.create(stored))
	narrowed := admissionModelChartConfig.validator()
	for key, value := range map[string]string{constants.PausedRolloutAnnotation: "true", constants.RolloutRollbackAnnotation: "true"} {
		next := stored.DeepCopy()
		admissionModelAnnotate(next, key, value)
		_, err := narrowed.ValidateUpdate(context.Background(), stored, next)
		if err == nil {
			t.Fatalf("the picture moved: %s is admitted under the narrowed tolerance; remove the known classes and this lock", key)
		}
		require.Contains(t, err.Error(), validation.ReasonRatioDrainInfeasible)
		t.Logf("%s=%s: %v", key, value, err)
	}
}

// TestAdmission_ARatioRefusalAtToleranceZeroNamesAddingReplicasAsAWayOut
// holds the picture: the in-place pair at a tolerance of zero is refused,
// and the refusal names adding decoder replicas as a way out, which no
// count takes: a band of zero width admits no drain. When the refusal
// stops naming it, this test fails, and the known class and this lock go
// with it.
func TestAdmission_ARatioRefusalAtToleranceZeroNamesAddingReplicasAsAWayOut(t *testing.T) {
	cfg := admissionModelInPlacePair(ptr.To[int32](0), "")
	w := newAdmissionModelWorld(t, cfg, admissionModelChartConfig)
	err := w.create(cfg.build())
	require.Error(t, err)
	if !strings.Contains(err.Error(), "add decoder replicas") {
		t.Fatalf("the picture moved: the refusal no longer names adding replicas as a way out; remove the known class and this lock: %v", err)
	}
	for n := 9; n <= 64; n++ {
		try := cfg.build()
		admissionModelSetReplicas(try, v1beta1.DecoderComponent, n)
		if w.create(try) == nil {
			t.Fatalf("the picture moved: %d decoders are admitted at a tolerance of zero", n)
		}
	}
	t.Logf("%v", err)
}
