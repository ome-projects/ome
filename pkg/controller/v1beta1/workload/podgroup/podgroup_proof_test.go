package podgroup

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// The pictures below are one gang of a leader and two workers, as the
// renderer labels them, and what its pods prove about the gang's topology
// key: the key the generated worker-to-leader terms name, none, or two,
// which no PodGroup can describe.
const (
	proofOwner           = "svc"
	proofComponent       = workload.ComponentEngine
	proofGang      int32 = 1
	proofOtherGang int32 = 7
	proofKey             = "topology.example.com/rack"
	proofOtherKey        = "topology.example.com/zone"
	proofThirdKey        = "topology.example.com/host"
)

// proofPod is one pod of a gang as the renderer labels it, bound to a node.
func proofPod(runner string, index int32) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("%s-%s-%d-%s", proofOwner, proofComponent, index, runner),
			Labels: map[string]string{
				constants.InferenceServicePodLabelKey: proofOwner,
				constants.OMEComponentLabel:           string(proofComponent),
				query.LabelManagedBy:                  query.ManagedByOMENative,
				query.LabelInstanceIdx:                fmt.Sprintf("%d", index),
				query.LabelRunner:                     runner,
				query.LabelPodGroup:                   query.PodGroupName(proofOwner, proofComponent, index),
			},
		},
		Spec: corev1.PodSpec{NodeName: "node-a"},
	}
}

// leaderTerm is the required affinity term the renderer writes on a
// worker: follow the leader of the Instance index on key.
func leaderTerm(index string, key string) corev1.PodAffinityTerm {
	return corev1.PodAffinityTerm{
		TopologyKey: key,
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
			constants.InferenceServicePodLabelKey: proofOwner,
			constants.OMEComponentLabel:           string(proofComponent),
			query.LabelInstanceIdx:                index,
			query.LabelRunner:                     workload.RunnerLeader,
		}},
	}
}

// withTerms gives a pod the required affinity terms.
func withTerms(pod *corev1.Pod, terms ...corev1.PodAffinityTerm) *corev1.Pod {
	pod.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: terms}}
	return pod
}

// proofWorker is a worker of the gang index following its leader on key.
func proofWorker(index int32, key string) *corev1.Pod {
	return withTerms(proofPod(workload.RunnerWorker, index), leaderTerm(fmt.Sprintf("%d", index), key))
}

// handWrittenTerm is a term an operator wrote on the pod template, which
// names something other than the gang's leader.
func handWrittenTerm() corev1.PodAffinityTerm {
	return corev1.PodAffinityTerm{TopologyKey: "kubernetes.io/hostname",
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "cache"}}}
}

// proofPicture is one picture of the gang's pods and what they prove for
// the Instance asked about: the key, whether one is proven, or the error.
type proofPicture struct {
	name string
	pods []*corev1.Pod
	// index is the Instance asked about; zero means the gang's own.
	index   int32
	key     string
	proven  bool
	wantErr string
}

// conflictOf is the error that names every key the pods disagree on,
// sorted.
func conflictOf(keys ...string) string {
	return fmt.Sprintf("conflicting live topology keys: %v", keys)
}

// provenPictures are the pictures in which the pods prove the gang's key.
func provenPictures() []proofPicture {
	leader := proofPod(workload.RunnerLeader, proofGang)
	worker := proofWorker(proofGang, proofKey)
	unbound := proofWorker(proofGang, proofKey)
	unbound.Spec.NodeName = ""
	deleted := proofWorker(proofGang, proofKey)
	now := metav1.Now()
	deleted.DeletionTimestamp = &now
	far := proofWorker(3999, proofKey)
	return []proofPicture{
		{name: "two workers following the leader on the key", pods: []*corev1.Pod{leader, worker, proofWorker(proofGang, proofKey)}, key: proofKey, proven: true},
		{name: "one worker following the leader, the other not yet in the list", pods: []*corev1.Pod{leader, worker}, key: proofKey, proven: true},
		{name: "a worker with no node yet", pods: []*corev1.Pod{leader, unbound}, key: proofKey, proven: true},
		{name: "a worker being deleted", pods: []*corev1.Pod{leader, deleted}, key: proofKey, proven: true},
		{name: "the same worker listed twice", pods: []*corev1.Pod{leader, worker, worker}, key: proofKey, proven: true},
		{name: "a nil entry beside a worker", pods: []*corev1.Pod{nil, worker}, key: proofKey, proven: true},
		{name: "a hand-written term beside the generated one", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), handWrittenTerm(), leaderTerm("1", proofKey))}, key: proofKey, proven: true},
		{name: "one worker with the generated term twice", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), leaderTerm("1", proofKey), leaderTerm("1", proofKey))}, key: proofKey, proven: true},
		{name: "a worker following the other gang's leader beside one following ours", pods: []*corev1.Pod{proofWorker(proofOtherGang, proofOtherKey), worker}, key: proofKey, proven: true},
		{name: "the gang at index 3999", pods: []*corev1.Pod{far}, index: 3999, key: proofKey, proven: true},
	}
}

// unprovenPictures are the pictures in which the pods prove nothing: the
// gang has no worker with the generated term that follows its own leader.
func unprovenPictures() []proofPicture {
	leader := proofPod(workload.RunnerLeader, proofGang)
	noIndex := leaderTerm("1", proofKey)
	delete(noIndex.LabelSelector.MatchLabels, query.LabelInstanceIdx)
	workerSelected := leaderTerm("1", proofKey)
	workerSelected.LabelSelector.MatchLabels[query.LabelRunner] = workload.RunnerWorker
	otherOwner := leaderTerm("1", proofKey)
	otherOwner.LabelSelector.MatchLabels[constants.InferenceServicePodLabelKey] = "other"
	otherComponent := leaderTerm("1", proofKey)
	otherComponent.LabelSelector.MatchLabels[constants.OMEComponentLabel] = string(workload.ComponentDecoder)
	noSelector := leaderTerm("1", proofKey)
	noSelector.LabelSelector = nil
	unlabelled := proofWorker(proofGang, proofKey)
	delete(unlabelled.Labels, query.LabelRunner)
	return []proofPicture{
		{name: "no pod at all", pods: nil},
		{name: "a leader alone", pods: []*corev1.Pod{leader}},
		{name: "a worker with no affinity", pods: []*corev1.Pod{leader, proofPod(workload.RunnerWorker, proofGang)}},
		{name: "a worker following another Instance's leader", pods: []*corev1.Pod{leader, proofWorker(proofOtherGang, proofKey)}},
		{name: "an Instance past the plan asked about", pods: []*corev1.Pod{leader, proofWorker(proofGang, proofKey)}, index: 5},
		{name: "the term's index spelled with a leading zero", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), leaderTerm("01", proofKey))}},
		{name: "a term with no topology key", pods: []*corev1.Pod{proofWorker(proofGang, "")}},
		{name: "a hand-written term alone", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), handWrittenTerm())}},
		{name: "the leader carrying the term", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerLeader, proofGang), leaderTerm("1", proofKey))}},
		{name: "a pod without the runner label carrying the term", pods: []*corev1.Pod{unlabelled}},
		{name: "a term selecting a worker, not the leader", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), workerSelected)}},
		{name: "a term whose selector names no index", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), noIndex)}},
		{name: "a term with no selector", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), noSelector)}},
		{name: "a term naming another owner's leader", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), otherOwner)}},
		{name: "a term naming another Component's leader", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), otherComponent)}},
		{name: "a nil entry alone", pods: []*corev1.Pod{nil}},
	}
}

// conflictingPictures are the pictures in which the pods name two or more
// keys: the error names every one of them, sorted.
func conflictingPictures() []proofPicture {
	return []proofPicture{
		{name: "two workers on two keys", pods: []*corev1.Pod{proofWorker(proofGang, proofKey), proofWorker(proofGang, proofOtherKey)}, wantErr: conflictOf(proofKey, proofOtherKey)},
		{name: "two workers on two keys, the other key first", pods: []*corev1.Pod{proofWorker(proofGang, proofOtherKey), proofWorker(proofGang, proofKey)}, wantErr: conflictOf(proofKey, proofOtherKey)},
		{name: "three workers on three keys, listed from the last", pods: []*corev1.Pod{proofWorker(proofGang, proofOtherKey), proofWorker(proofGang, proofKey), proofWorker(proofGang, proofThirdKey)}, wantErr: conflictOf(proofThirdKey, proofKey, proofOtherKey)},
		{name: "one worker with two terms on two keys", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofGang), leaderTerm("1", proofKey), leaderTerm("1", proofOtherKey))}, wantErr: conflictOf(proofKey, proofOtherKey)},
		{name: "two keys among four workers, two on each", pods: []*corev1.Pod{proofWorker(proofGang, proofKey), proofWorker(proofGang, proofKey), proofWorker(proofGang, proofOtherKey), proofWorker(proofGang, proofOtherKey)}, wantErr: conflictOf(proofKey, proofOtherKey)},
	}
}

func (p proofPicture) instance() int32 {
	if p.index != 0 {
		return p.index
	}
	return proofGang
}

// TestTopologyKeyProvenByPods_OnEveryPicture holds what the pods of one
// gang prove: the key the generated worker-to-leader terms name when they
// agree, nothing when no worker follows the gang's own leader, and the
// error naming every key when they disagree; and what
// EffectiveTopologyKeyForPods makes of it for a gang with a configured key
// and for one without.
func TestTopologyKeyProvenByPods_OnEveryPicture(t *testing.T) {
	var pictures []proofPicture
	pictures = append(pictures, provenPictures()...)
	pictures = append(pictures, unprovenPictures()...)
	pictures = append(pictures, conflictingPictures()...)
	for _, p := range pictures {
		t.Run(p.name, func(t *testing.T) {
			key, proven, err := TopologyKeyProvenByPods(proofOwner, proofComponent, p.instance(), p.pods)
			if p.wantErr != "" {
				if err == nil || err.Error() != p.wantErr {
					t.Fatalf("error %v, want %q", err, p.wantErr)
				}
				if key != "" || proven {
					t.Fatalf("key %q proven=%t beside the error, want none", key, proven)
				}
				requireEffectiveKeyRefused(t, p, p.wantErr)
				return
			}
			if err != nil {
				t.Fatalf("error %v, want none", err)
			}
			if key != p.key || proven != p.proven {
				t.Fatalf("key %q proven=%t, want key %q proven=%t", key, proven, p.key, p.proven)
			}
			requireEffectiveKey(t, p)
		})
	}
}

// requireEffectiveKey holds EffectiveTopologyKeyForPods on a picture the
// proof does not refuse: a proven key is the gang's whatever the plan asks
// for; with nothing proven a gang with no pod takes the plan's key, a gang
// with pods under a configured key is refused, and one under no key keeps
// none.
func requireEffectiveKey(t *testing.T, p proofPicture) {
	t.Helper()
	index := p.instance()
	configured, err := EffectiveTopologyKeyForPods(proofOtherKey, proofOwner, proofComponent, index, p.pods)
	unset, unsetErr := EffectiveTopologyKeyForPods("", proofOwner, proofComponent, index, p.pods)
	switch {
	case p.proven:
		if err != nil || configured != p.key || unsetErr != nil || unset != p.key {
			t.Fatalf("effective key under a configured key %q (%v) and under none %q (%v), want the proven %q", configured, err, unset, unsetErr, p.key)
		}
	case len(p.pods) == 0:
		if err != nil || configured != proofOtherKey || unsetErr != nil || unset != "" {
			t.Fatalf("effective key of a gang with no pod: %q (%v) under a configured key, %q (%v) under none; want the plan's", configured, err, unset, unsetErr)
		}
	default:
		want := fmt.Sprintf("cannot prove active topology for %s instance %d from %d live pod(s)", proofComponent, index, len(p.pods))
		if err == nil || err.Error() != want {
			t.Fatalf("effective key under a configured key: %q, %v; want the refusal %q", configured, err, want)
		}
		if unsetErr != nil || unset != "" {
			t.Fatalf("effective key under no key: %q, %v; want none", unset, unsetErr)
		}
	}
}

// requireEffectiveKeyRefused holds that EffectiveTopologyKeyForPods carries
// the proof's own error whatever the plan asks for.
func requireEffectiveKeyRefused(t *testing.T, p proofPicture, wantErr string) {
	t.Helper()
	for _, desired := range []string{proofOtherKey, ""} {
		key, err := EffectiveTopologyKeyForPods(desired, proofOwner, proofComponent, p.instance(), p.pods)
		if err == nil || err.Error() != wantErr || key != "" {
			t.Fatalf("effective key under %q: %q, %v; want the error %q", desired, key, err, wantErr)
		}
	}
}

// TestGeneratedTopologyKeyFromPods_OnEveryPicture holds the proof over the
// pods of a whole Component, where each worker's term must follow the
// leader of the Instance the worker's own index label names.
func TestGeneratedTopologyKeyFromPods_OnEveryPicture(t *testing.T) {
	noIndexLabel := proofWorker(proofGang, proofKey)
	delete(noIndexLabel.Labels, query.LabelInstanceIdx)
	noIndexAnywhere := withTerms(proofPod(workload.RunnerWorker, proofGang), leaderTerm("", proofKey))
	delete(noIndexAnywhere.Labels, query.LabelInstanceIdx)
	strayIndex := proofWorker(proofGang, proofKey)
	strayIndex.Labels[query.LabelInstanceIdx] = "x"
	pictures := []proofPicture{
		{name: "workers of two gangs, each following its own leader on the key", pods: []*corev1.Pod{proofWorker(proofGang, proofKey), proofWorker(proofOtherGang, proofKey)}, key: proofKey, proven: true},
		{name: "a worker following the other gang's leader", pods: []*corev1.Pod{proofWorker(proofGang, proofKey), withTerms(proofPod(workload.RunnerWorker, proofOtherGang), leaderTerm("1", proofKey))}, key: proofKey, proven: true},
		{name: "only a worker following the other gang's leader", pods: []*corev1.Pod{withTerms(proofPod(workload.RunnerWorker, proofOtherGang), leaderTerm("1", proofKey))}},
		{name: "a worker with no index label", pods: []*corev1.Pod{noIndexLabel}},
		{name: "a worker with no index label whose term names no index", pods: []*corev1.Pod{noIndexAnywhere}},
		{name: "a worker whose index label is not a number", pods: []*corev1.Pod{strayIndex}},
		{name: "two gangs on two keys", pods: []*corev1.Pod{proofWorker(proofGang, proofKey), proofWorker(proofOtherGang, proofOtherKey)}, wantErr: conflictOf(proofKey, proofOtherKey)},
		{name: "no pod at all", pods: nil},
	}
	for _, p := range pictures {
		t.Run(p.name, func(t *testing.T) {
			key, proven, err := GeneratedTopologyKeyFromPods(proofOwner, proofComponent, p.pods)
			if p.wantErr != "" {
				if err == nil || err.Error() != p.wantErr {
					t.Fatalf("error %v, want %q", err, p.wantErr)
				}
				return
			}
			if err != nil || key != p.key || proven != p.proven {
				t.Fatalf("key %q proven=%t err=%v, want key %q proven=%t", key, proven, err, p.key, p.proven)
			}
		})
	}
}

// The proof as it was written, kept here whole so the differential below
// does not share a line with the production walk: the keys the generated
// terms name collected in a set and sorted into a list, the Instance index
// formatted as text, the terms of a worker read off its affinity.

func workerTermsAsWritten(pod *corev1.Pod) []corev1.PodAffinityTerm {
	if pod == nil || pod.Labels[query.LabelRunner] != workload.RunnerWorker ||
		pod.Spec.Affinity == nil || pod.Spec.Affinity.PodAffinity == nil {
		return nil
	}
	return pod.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution
}

func selectsLeaderAsWritten(selector *metav1.LabelSelector, ownerName string, component workload.ComponentType, index string) bool {
	if selector == nil {
		return false
	}
	labels := selector.MatchLabels
	return labels[constants.InferenceServicePodLabelKey] == ownerName &&
		labels[constants.OMEComponentLabel] == string(component) &&
		labels[query.LabelInstanceIdx] == index &&
		labels[query.LabelRunner] == workload.RunnerLeader
}

func topologyKeyThroughASortedSet(ownerName string, component workload.ComponentType, pods []*corev1.Pod, leaderIndex func(*corev1.Pod) string) (string, bool, error) {
	keys := make(map[string]struct{})
	for _, pod := range pods {
		terms := workerTermsAsWritten(pod)
		if len(terms) == 0 {
			continue
		}
		index := leaderIndex(pod)
		if index == "" {
			continue
		}
		for i := range terms {
			if terms[i].TopologyKey != "" && selectsLeaderAsWritten(terms[i].LabelSelector, ownerName, component, index) {
				keys[terms[i].TopologyKey] = struct{}{}
			}
		}
	}
	sorted := slices.Sorted(maps.Keys(keys))
	switch len(sorted) {
	case 0:
		return "", false, nil
	case 1:
		return sorted[0], true, nil
	}
	return "", false, fmt.Errorf("conflicting live topology keys: %v", sorted)
}

func instanceKeyThroughASortedSet(ownerName string, component workload.ComponentType, instanceIdx int32, pods []*corev1.Pod) (string, bool, error) {
	wantIndex := fmt.Sprintf("%d", instanceIdx)
	return topologyKeyThroughASortedSet(ownerName, component, pods, func(*corev1.Pod) string { return wantIndex })
}

func componentKeyThroughASortedSet(ownerName string, component workload.ComponentType, pods []*corev1.Pod) (string, bool, error) {
	return topologyKeyThroughASortedSet(ownerName, component, pods, func(pod *corev1.Pod) string {
		return pod.Labels[query.LabelInstanceIdx]
	})
}

func effectiveKeyThroughASortedSet(desiredKey, ownerName string, component workload.ComponentType, instanceIdx int32, pods []*corev1.Pod) (string, error) {
	if len(pods) == 0 {
		return desiredKey, nil
	}
	key, ok, err := instanceKeyThroughASortedSet(ownerName, component, instanceIdx, pods)
	if err != nil {
		return "", err
	}
	if ok {
		return key, nil
	}
	if desiredKey != "" {
		return "", fmt.Errorf("cannot prove active topology for %s instance %d from %d live pod(s)", component, instanceIdx, len(pods))
	}
	return "", nil
}

// proofDraw is one random picture of a gang's pods and the Instance asked
// about: pods of the gang and of others, leaders and workers, with and
// without a node, some being deleted, some listed twice, nil entries, and
// terms that follow the right leader, another Instance's, an index spelled
// with a leading zero, a worker, another owner or Component, nothing, or
// that carry no key.
type proofDraw struct {
	asked int32
	pods  []*corev1.Pod
	// pastThePlan is an asked Instance no pod belongs to; duplicates and
	// nils count the entries listed twice and the nil entries.
	pastThePlan      bool
	duplicates, nils int
}

var proofDrawKeys = []string{"", proofKey, proofKey, proofKey, proofKey, proofKey, proofOtherKey, proofThirdKey}

func drawProofPods(r *rand.Rand) proofDraw {
	gang := pick(r, int32(0), 1, 7, 99, 100, 256, 1725, 3999, -1)
	draw := proofDraw{asked: gang}
	if r.IntN(8) == 0 {
		draw.asked, draw.pastThePlan = gang+5, true
	}
	for n := r.IntN(7); n > 0; n-- {
		if r.IntN(12) == 0 {
			draw.pods = append(draw.pods, nil)
			draw.nils++
			continue
		}
		if len(draw.pods) > 0 && r.IntN(6) == 0 {
			draw.pods = append(draw.pods, draw.pods[r.IntN(len(draw.pods))])
			draw.duplicates++
			continue
		}
		draw.pods = append(draw.pods, drawProofPod(r, gang))
	}
	return draw
}

func drawProofPod(r *rand.Rand, gang int32) *corev1.Pod {
	index := pick(r, gang, gang, gang, gang, gang+1, -1)
	pod := proofPod(pick(r, workload.RunnerWorker, workload.RunnerWorker, workload.RunnerWorker, workload.RunnerLeader, "", "other"), index)
	switch r.IntN(10) {
	case 0:
		pod.Labels[query.LabelInstanceIdx] = fmt.Sprintf("0%d", index)
	case 1:
		delete(pod.Labels, query.LabelInstanceIdx)
	case 2:
		pod.Labels[query.LabelInstanceIdx] = "x"
	}
	if pod.Labels[query.LabelRunner] == "" {
		delete(pod.Labels, query.LabelRunner)
	}
	if r.IntN(4) == 0 {
		pod.Spec.NodeName = ""
	}
	if r.IntN(6) == 0 {
		now := metav1.Now()
		pod.DeletionTimestamp = &now
	}
	if r.IntN(5) == 0 {
		return pod
	}
	var terms []corev1.PodAffinityTerm
	for n := 1 + r.IntN(2); n > 0; n-- {
		terms = append(terms, drawProofTerm(r, gang))
	}
	return withTerms(pod, terms...)
}

func drawProofTerm(r *rand.Rand, gang int32) corev1.PodAffinityTerm {
	term := leaderTerm(fmt.Sprintf("%d", gang), pick(r, proofDrawKeys...))
	switch r.IntN(14) {
	case 0:
		term.LabelSelector.MatchLabels[query.LabelInstanceIdx] = fmt.Sprintf("0%d", gang)
	case 1:
		term.LabelSelector.MatchLabels[query.LabelInstanceIdx] = fmt.Sprintf("%d", gang+1)
	case 2:
		delete(term.LabelSelector.MatchLabels, query.LabelInstanceIdx)
	case 3:
		term.LabelSelector.MatchLabels[query.LabelRunner] = workload.RunnerWorker
	case 4:
		term.LabelSelector.MatchLabels[constants.InferenceServicePodLabelKey] = "other"
	case 5:
		term.LabelSelector.MatchLabels[constants.OMEComponentLabel] = string(workload.ComponentDecoder)
	case 6:
		term.LabelSelector = nil
	case 7:
		term = handWrittenTerm()
	case 8:
		term.LabelSelector.MatchLabels = nil
	}
	return term
}

// sameAnswer holds two readings of the proof to the same key, the same
// proven flag and the same error text.
func sameAnswer(t *testing.T, what string, draw proofDraw, key, keyWas string, proven, provenWas bool, err, errWas error) {
	t.Helper()
	if key != keyWas || proven != provenWas || (err == nil) != (errWas == nil) || (err != nil && err.Error() != errWas.Error()) {
		t.Fatalf("%s over %s: key %q proven=%t err=%v; through the sorted set key %q proven=%t err=%v",
			what, describeProofDraw(draw), key, proven, err, keyWas, provenWas, errWas)
	}
}

func describeProofDraw(draw proofDraw) string {
	parts := make([]string, 0, len(draw.pods))
	for _, pod := range draw.pods {
		if pod == nil {
			parts = append(parts, "nil")
			continue
		}
		parts = append(parts, fmt.Sprintf("%v/%v", pod.Labels, pod.Spec.Affinity))
	}
	return fmt.Sprintf("instance %d, pods [%s]", draw.asked, strings.Join(parts, "; "))
}

// TestTopologyKeyProvenByPods_AgreesWithTheSortedSetOnEveryDraw draws
// 300,000 pictures of a gang's pods and holds the three readings of the
// proof, for one Instance, for a whole Component and the effective key
// under a configured key and under none, to the answers of the proof as
// it was written through a set and a sorted list: the same key, the same
// proven flag, the same error text.
func TestTopologyKeyProvenByPods_AgreesWithTheSortedSetOnEveryDraw(t *testing.T) {
	r := rand.New(rand.NewPCG(24, 1725))
	var proven, unproven, conflicts, pastThePlan, duplicates, nils, unbound, deleted int
	for i := 0; i < 300000; i++ {
		draw := drawProofPods(r)
		key, ok, err := TopologyKeyProvenByPods(proofOwner, proofComponent, draw.asked, draw.pods)
		keyWas, okWas, errWas := instanceKeyThroughASortedSet(proofOwner, proofComponent, draw.asked, draw.pods)
		sameAnswer(t, "the Instance's proof", draw, key, keyWas, ok, okWas, err, errWas)
		key, ok, err = GeneratedTopologyKeyFromPods(proofOwner, proofComponent, draw.pods)
		keyWas, okWas, errWas = componentKeyThroughASortedSet(proofOwner, proofComponent, draw.pods)
		sameAnswer(t, "the Component's proof", draw, key, keyWas, ok, okWas, err, errWas)
		for _, desired := range []string{"", proofOtherKey} {
			key, err = EffectiveTopologyKeyForPods(desired, proofOwner, proofComponent, draw.asked, draw.pods)
			keyWas, errWas = effectiveKeyThroughASortedSet(desired, proofOwner, proofComponent, draw.asked, draw.pods)
			sameAnswer(t, "the effective key under "+strconv.Quote(desired), draw, key, keyWas, false, false, err, errWas)
		}
		switch {
		case errWas != nil:
			conflicts++
		case okWas:
			proven++
		default:
			unproven++
		}
		if draw.pastThePlan {
			pastThePlan++
		}
		duplicates += draw.duplicates
		nils += draw.nils
		for _, pod := range draw.pods {
			if pod == nil {
				continue
			}
			if pod.Spec.NodeName == "" {
				unbound++
			}
			if pod.DeletionTimestamp != nil {
				deleted++
			}
		}
	}
	t.Logf("300000 draws: %d proven, %d unproven, %d conflicts; %d asked past the plan; %d entries listed twice, %d nil entries, %d pods with no node, %d being deleted",
		proven, unproven, conflicts, pastThePlan, duplicates, nils, unbound, deleted)
	for name, n := range map[string]int{"proven": proven, "unproven": unproven, "conflicts": conflicts, "past the plan": pastThePlan, "duplicates": duplicates, "nils": nils, "no node": unbound, "deleted": deleted} {
		if n == 0 {
			t.Errorf("the draws never reached a picture that is %s", name)
		}
	}
}

// TestTopologyKeyProvenByPods_AllocatesNothing: proving the key of a gang
// of eight whose terms agree costs no allocation, for one Instance, for a
// Component and as the effective key, whatever the index spells.
func TestTopologyKeyProvenByPods_AllocatesNothing(t *testing.T) {
	for _, index := range []int32{0, 7, 100, 1725, 3999} {
		pods := gangPodsOnKey(proofOwner, proofComponent, index, proofKey, 7)
		readings := map[string]func(){
			"the Instance's proof": func() {
				if key, ok, err := TopologyKeyProvenByPods(proofOwner, proofComponent, index, pods); err != nil || !ok || key != proofKey {
					t.Fatalf("index %d: key %q ok=%t err=%v", index, key, ok, err)
				}
			},
			"the Component's proof": func() {
				if key, ok, err := GeneratedTopologyKeyFromPods(proofOwner, proofComponent, pods); err != nil || !ok || key != proofKey {
					t.Fatalf("index %d: key %q ok=%t err=%v", index, key, ok, err)
				}
			},
			"the effective key": func() {
				if key, err := EffectiveTopologyKeyForPods(proofOtherKey, proofOwner, proofComponent, index, pods); err != nil || key != proofKey {
					t.Fatalf("index %d: key %q err=%v", index, key, err)
				}
			},
		}
		for name, reading := range readings {
			if allocs := testing.AllocsPerRun(100, reading); allocs != 0 {
				t.Errorf("index %d: %s allocated %.0f times per call; want none", index, name, allocs)
			}
		}
	}
}
