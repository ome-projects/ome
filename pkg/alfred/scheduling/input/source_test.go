package input

import (
	"context"
	"encoding/json"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	codec "sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func TestBuildRequestSelectsColumnarSourceWithoutChangingCapture(t *testing.T) {
	// Catches the source resolver ignoring compact logical rows.
	objects, source := validSingleSourceObjects()
	ir := sourceIR(objects)
	columns, err := codec.EncodeColumns(ir.Status.InstanceStatuses, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatuses = nil
	ir.Status.InstanceStatusEncoding = &marker
	ir.Status.InstanceStatusColumns = columns
	snap := captureSourceFixture(t, objects)
	before, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	request, err := BuildRequest(snap, source, testProfiles(false), "compact-source", captureTime.Add(time.Second), time.Minute)
	if err != nil || len(request.SourcePods) != 1 || len(request.ReplacementPods) != 1 {
		t.Fatalf("compact source request = %+v, %v", request, err)
	}
	after, err := json.Marshal(snap)
	if err != nil || !reflect.DeepEqual(after, before) {
		t.Fatal("BuildRequest changed the lossless capture")
	}
}

func TestSyntheticIdentityAvoidsColumnarStatusOnlyIndex(t *testing.T) {
	// Catches a synthetic identity colliding with a compact status-only member.
	_, source := validSingleSourceObjects()
	members := []podMember{{pod: *readySourcePod("source", "source-uid", "source-a", v1beta1.RunnerNameDefault, 0, "default-scheduler"), incarnation: 7}}
	snap := &Snapshot{ID: "fixed-snapshot", InferenceReplicas: []v1beta1.InferenceReplica{{
		ObjectMeta: metav1.ObjectMeta{Namespace: source.Namespace},
		Spec:       v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: source.InferenceService}, Component: source.Component},
	}}}
	first, err := newSyntheticIdentity(snap, source, members, "request")
	if err != nil {
		t.Fatal(err)
	}
	index, err := strconv.ParseInt(first.index, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := codec.EncodeColumns([]v1beta1.OMENativeInstanceStatus{{Index: int32(index), Phase: v1beta1.OMENativeInstancePending}}, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	snap.InferenceReplicas[0].Status.InstanceStatusEncoding = &marker
	snap.InferenceReplicas[0].Status.InstanceStatusColumns = columns
	second, err := newSyntheticIdentity(snap, source, members, "request")
	if err != nil || second.index == first.index {
		t.Fatalf("synthetic index collided with compact status-only member: first=%q second=%q err=%v", first.index, second.index, err)
	}
}

const (
	testISVCUID = types.UID("isvc-uid")
	testIRUID   = types.UID("ir-uid")
)

func TestBuildRequestPreservesSnapshotTimeAcrossJSON(t *testing.T) {
	for _, execution := range []bool{false, true} {
		name := "prediction"
		if execution {
			name = "execution"
		}
		t.Run(name, func(t *testing.T) {
			objects, source := validSingleSourceObjects()
			for _, object := range objects {
				if node, ok := object.(*corev1.Node); ok {
					node.Labels[corev1.LabelHostname] = node.Name
				}
			}
			started := time.Date(2026, 9, 14, 5, 0, 0, 123456789, time.FixedZone("capture", -7*60*60))
			completed := time.Date(2026, 9, 14, 5, 0, 0, 987654321, started.Location())
			calls := 0
			snap, err := Capture(context.Background(), captureReader(t, objects...), func() time.Time {
				calls++
				if calls == 1 {
					return started
				}
				return completed
			})
			if err != nil {
				t.Fatal(err)
			}
			before, err := json.Marshal(snap)
			if err != nil {
				t.Fatal(err)
			}
			var request scheduling.Request
			if execution {
				request, err = BuildExecutionRequest(snap, source, testProfiles(false), "fractional-clock", []string{"target-a"}, completed.Add(time.Second), time.Minute)
			} else {
				request, err = BuildRequest(snap, source, testProfiles(false), "fractional-clock", completed.Add(time.Second), time.Minute)
			}
			if err != nil {
				t.Fatal(err)
			}
			requestJSON, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			var wireRequest scheduling.Request
			if err := json.Unmarshal(requestJSON, &wireRequest); err != nil {
				t.Fatal(err)
			}
			pod := wireRequest.ReplacementPods[0]
			response := scheduling.Result{
				SchemaVersion: wireRequest.SchemaVersion, RequestID: wireRequest.RequestID,
				SnapshotID: wireRequest.SnapshotID, SnapshotTime: wireRequest.SnapshotTime, Profile: wireRequest.Profile,
				Decision: scheduling.DecisionFeasible, Reason: scheduling.SimulationReasonPlacementFound,
				Placements: []scheduling.Placement{{Pod: scheduling.PodIdentity{Namespace: pod.Namespace, Name: pod.Name, UID: pod.UID}, NodeName: "target-a"}},
			}
			responseJSON, err := json.Marshal(response)
			if err != nil {
				t.Fatal(err)
			}
			var result scheduling.Result
			if err := json.Unmarshal(responseJSON, &result); err != nil {
				t.Fatal(err)
			}
			for _, validate := range []struct {
				name string
				fn   func(scheduling.Request, scheduling.Result) error
			}{{"ValidateResponse", scheduling.ValidateResponse}, {"ValidateResult", scheduling.ValidateResult}} {
				if err := validate.fn(request, result); err != nil {
					t.Fatalf("%s rejected JSON-roundtripped feasible result: %v", validate.name, err)
				}
				for _, seconds := range []int{-1, 1} {
					changed := result
					changed.SnapshotTime = metav1.NewTime(time.Date(2026, 9, 14, 12, 0, seconds, 0, time.UTC))
					if err := validate.fn(request, changed); err == nil || !strings.Contains(err.Error(), "snapshot time") {
						t.Fatalf("%s accepted changed timestamp second %d: %v", validate.name, seconds, err)
					}
				}
			}
			want := time.Date(2026, 9, 14, 12, 0, 0, 0, time.UTC)
			if !request.SnapshotTime.Time.Equal(want) || request.SnapshotTime.Time.Location() != time.UTC || !wireRequest.SnapshotTime.Time.Equal(want) {
				t.Fatalf("request timestamp = %v, wire timestamp = %v, want %v", request.SnapshotTime.Time, wireRequest.SnapshotTime.Time, want)
			}
			after, err := json.Marshal(snap)
			if err != nil {
				t.Fatal(err)
			}
			if string(before) != string(after) || !snap.StartedAt.Equal(started) || !snap.CompletedAt.Equal(completed) || request.SnapshotID != snap.ID {
				t.Fatal("request construction changed full-precision snapshot identity or times")
			}
			if err := snap.Validate(started.Add(time.Second), time.Second); err != nil {
				t.Fatalf("full-precision freshness boundary rejected: %v", err)
			}
			if err := snap.Validate(started.Add(time.Second+time.Nanosecond), time.Second); err == nil {
				t.Fatal("full-precision freshness boundary was weakened")
			}
		})
	}
}

func TestBuildRequestClonesLivePodWithoutLosingSchedulingInputs(t *testing.T) {
	objects, source := validSingleSourceObjects()
	snap := captureSourceFixture(t, objects)
	before, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}

	request, err := BuildRequest(snap, source, testProfiles(false), "request-one", captureTime.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatalf("BuildRequest() error = %v", err)
	}
	if request.SchemaVersion != scheduling.SimulationSchemaV1 || request.RequestID != "request-one" ||
		request.SnapshotID != snap.ID || !request.SnapshotTime.Time.Equal(snap.CompletedAt) {
		t.Fatalf("request envelope = %+v", request)
	}
	if request.Profile != (scheduling.ProfileIdentity{
		SchedulerName: "default-scheduler", Backend: "kube-scheduler", SchedulerVersion: "v1.35.8", ConfigurationID: "default-config",
	}) {
		t.Fatalf("profile identity = %+v", request.Profile)
	}
	if request.RequireGang || !reflect.DeepEqual(request.ExcludedNodes, []string{"source-a"}) ||
		len(request.SourcePods) != 1 || len(request.ReplacementPods) != 1 {
		t.Fatalf("request membership = %+v", request)
	}

	sourcePod, replacement := request.SourcePods[0], request.ReplacementPods[0]
	if sourcePod.Name != "svc-engine-2-default-0" || sourcePod.UID != "source-pod-uid" || sourcePod.Spec.NodeName != "source-a" {
		t.Fatalf("source pod = %+v", sourcePod)
	}
	if replacement.Name == sourcePod.Name || replacement.UID == sourcePod.UID || replacement.Name == "" || replacement.UID == "" ||
		replacement.Spec.NodeName != "" || !reflect.DeepEqual(replacement.Status, corev1.PodStatus{}) ||
		replacement.ResourceVersion != "" || !replacement.CreationTimestamp.IsZero() {
		t.Fatalf("replacement identity/server state was not normalized: %+v", replacement)
	}
	if !reflect.DeepEqual(replacement.Spec.NodeSelector, sourcePod.Spec.NodeSelector) ||
		!reflect.DeepEqual(replacement.Spec.Tolerations, sourcePod.Spec.Tolerations) ||
		!reflect.DeepEqual(replacement.Spec.Containers[0].Resources, sourcePod.Spec.Containers[0].Resources) ||
		replacement.Spec.SchedulerName != sourcePod.Spec.SchedulerName {
		t.Fatalf("replacement lost scheduling inputs: source=%+v replacement=%+v", sourcePod.Spec, replacement.Spec)
	}
	if replacement.Labels[labelInstanceIndex] == sourcePod.Labels[labelInstanceIndex] ||
		replacement.Labels[labelInstanceIncarnation] == sourcePod.Labels[labelInstanceIncarnation] {
		t.Fatalf("replacement reused private instance identity: %v", replacement.Labels)
	}
	for _, key := range []string{labelInferenceService, labelComponent, labelManagedBy, labelRunner, labelPodOrdinal, labelRevisionHash} {
		if replacement.Labels[key] != sourcePod.Labels[key] {
			t.Fatalf("cohort label %q changed: source=%q replacement=%q", key, sourcePod.Labels[key], replacement.Labels[key])
		}
	}
	after, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != string(before) {
		t.Fatal("BuildRequest mutated its snapshot")
	}
	if err := snap.Validate(captureTime.Add(time.Second), time.Minute); err != nil {
		t.Fatalf("BuildRequest invalidated snapshot: %v", err)
	}
}

func TestBuildRequestClonesCompleteGangAndRetargetsPrivateIdentity(t *testing.T) {
	objects, source := validGangSourceObjects()
	snap := captureSourceFixture(t, objects)
	request, err := BuildRequest(snap, source, testProfiles(true), "gang-request", captureTime.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatalf("BuildRequest() error = %v", err)
	}
	if !request.RequireGang || len(request.SourcePods) != 2 || len(request.ReplacementPods) != 2 {
		t.Fatalf("gang membership = %+v", request)
	}
	if !reflect.DeepEqual(request.ExcludedNodes, []string{"source-a", "source-b"}) {
		t.Fatalf("excluded source nodes = %v", request.ExcludedNodes)
	}

	newGroup := request.ReplacementPods[0].Labels[labelPodGroup]
	if newGroup == "" || newGroup == "svc-engine-2" || request.ReplacementPods[1].Labels[labelPodGroup] != newGroup {
		t.Fatalf("replacement PodGroup labels = %q/%q", newGroup, request.ReplacementPods[1].Labels[labelPodGroup])
	}
	if request.ReplacementPods[0].Labels[labelInstanceIndex] == "2" ||
		request.ReplacementPods[0].Labels[labelInstanceIndex] != request.ReplacementPods[1].Labels[labelInstanceIndex] {
		t.Fatalf("replacement instance labels = %q/%q", request.ReplacementPods[0].Labels[labelInstanceIndex], request.ReplacementPods[1].Labels[labelInstanceIndex])
	}
	worker := podByRunner(t, request.ReplacementPods, string(v1beta1.RunnerNameWorker))
	term := worker.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
	if term.LabelSelector.MatchLabels[labelInstanceIndex] != worker.Labels[labelInstanceIndex] ||
		term.LabelSelector.MatchLabels[labelRunner] != string(v1beta1.RunnerNameLeader) {
		t.Fatalf("worker affinity did not follow synthetic leader: %+v", term)
	}

	foundSource, foundReplacement := false, false
	for _, extension := range request.ClusterObjects {
		var meta struct {
			APIVersion string `json:"apiVersion"`
			Kind       string `json:"kind"`
			Metadata   struct {
				Name string `json:"name"`
				UID  string `json:"uid"`
			} `json:"metadata"`
			Status any `json:"status"`
		}
		if err := json.Unmarshal(extension.Raw, &meta); err != nil {
			t.Fatal(err)
		}
		if meta.APIVersion != "scheduling.x-k8s.io/v1alpha1" || meta.Kind != "PodGroup" {
			continue
		}
		switch meta.Metadata.Name {
		case "svc-engine-2":
			foundSource = meta.Metadata.UID == "source-group-uid" && meta.Status != nil
		case newGroup:
			foundReplacement = meta.Metadata.UID != "" && meta.Metadata.UID != "source-group-uid" && meta.Status == nil
		}
	}
	if !foundSource || !foundReplacement {
		t.Fatalf("source/replacement PodGroups not preserved and cloned: source=%v replacement=%v", foundSource, foundReplacement)
	}
}

func TestBuildRequestPreservesSameProfilePendingCompetition(t *testing.T) {
	for _, gang := range []bool{false, true} {
		name := "default-scheduler"
		if gang {
			name = "ome-scheduler"
		}
		t.Run(name, func(t *testing.T) {
			objects, source := validSingleSourceObjects()
			if gang {
				objects, source = validGangSourceObjects()
			}
			pending := &corev1.Pod{
				TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
				ObjectMeta: captureMeta("pending-competitor"),
				Spec:       sourcePodSpec(name),
				Status:     corev1.PodStatus{Phase: corev1.PodPending},
			}
			pending.Spec.Priority = ptr.To(int32(200))
			objects = append(objects, pending)
			snap := captureSourceFixture(t, objects)
			request, err := BuildRequest(snap, source, testProfiles(gang), "pending-competition", captureTime.Add(time.Second), time.Minute)
			if err != nil {
				t.Fatalf("same-profile standalone pending competitor rejected: %v", err)
			}
			foundPending, foundSource := false, false
			for _, raw := range request.ClusterObjects {
				var pod corev1.Pod
				if err := json.Unmarshal(raw.Raw, &pod); err != nil {
					t.Fatal(err)
				}
				if pod.Kind != "Pod" {
					continue
				}
				if pod.UID == pending.UID {
					foundPending = reflect.DeepEqual(pod, *pending)
				}
				if pod.Name == "svc-engine-2-default-0" || pod.Name == "svc-engine-2-leader-0" {
					foundSource = pod.Spec.NodeName == "source-a" && pod.Status.Phase == corev1.PodRunning
				}
			}
			if !foundPending || !foundSource {
				t.Fatalf("request lost pending competition or bound source occupancy: pending=%v source=%v", foundPending, foundSource)
			}
			for _, pod := range append(request.SourcePods, request.ReplacementPods...) {
				if pod.UID == pending.UID {
					t.Fatal("pending competitor became a requested replacement or source")
				}
			}
			if request.RequireGang != gang {
				t.Fatal("pending competitor changed replacement gang membership")
			}
			if err := snap.Validate(captureTime.Add(time.Second), time.Minute); err != nil {
				t.Fatalf("request construction mutated the captured snapshot: %v", err)
			}
		})
	}
}

func TestBuildRequestRejectsUnsupportedPendingCompetition(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*corev1.Pod)
	}{
		{"mixed scheduler profiles", func(pod *corev1.Pod) { pod.Spec.SchedulerName = "other-scheduler" }},
		{"pending gang", func(pod *corev1.Pod) { pod.Labels = map[string]string{labelPodGroup: "other-gang"} }},
		{"persistent storage", func(pod *corev1.Pod) {
			pod.Spec.Volumes = []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "model"}}}}
		}},
		{"DRA", func(pod *corev1.Pod) {
			pod.Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimName: ptr.To("gpu-claim")}}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			objects, source := validSingleSourceObjects()
			pending := &corev1.Pod{ObjectMeta: captureMeta("pending-competitor"), Spec: sourcePodSpec("default-scheduler"), Status: corev1.PodStatus{Phase: corev1.PodPending}}
			tc.mutate(pending)
			objects = append(objects, pending)
			profiles := testProfiles(false)
			profiles.Profiles["other-scheduler"] = scheduling.Profile{Backend: "other", SchedulerVersion: "v1.35.8", ConfigurationID: "other-config"}
			request, err := BuildRequest(captureSourceFixture(t, objects), source, profiles, "unsupported-competition", captureTime.Add(time.Second), time.Minute)
			if err == nil || len(request.ReplacementPods) != 0 {
				t.Fatalf("unsupported pending competition returned a usable request: %+v, %v", request, err)
			}
		})
	}
}

func TestSyntheticInstanceIdentityAvoidsLiveCohortIndexes(t *testing.T) {
	_, source := validSingleSourceObjects()
	members := []podMember{{pod: *readySourcePod("source", "source-uid", "source-a", v1beta1.RunnerNameDefault, 0, "default-scheduler"), incarnation: 7}}
	snap := &Snapshot{ID: "fixed-snapshot", InferenceReplicas: []v1beta1.InferenceReplica{{
		ObjectMeta: metav1.ObjectMeta{Namespace: source.Namespace},
		Spec:       v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: source.InferenceService}, Component: source.Component},
		Status:     v1beta1.InferenceReplicaStatus{InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: source.Instance}}},
	}}}
	first, err := newSyntheticIdentity(snap, source, members, "request")
	if err != nil {
		t.Fatal(err)
	}
	index, err := strconv.ParseInt(first.index, 10, 32)
	if err != nil {
		t.Fatal(err)
	}
	snap.InferenceReplicas[0].Status.InstanceStatuses = append(snap.InferenceReplicas[0].Status.InstanceStatuses, v1beta1.OMENativeInstanceStatus{Index: int32(index)})
	second, err := newSyntheticIdentity(snap, source, members, "request")
	if err != nil {
		t.Fatal(err)
	}
	if second.index == first.index || second.index == "2" {
		t.Fatalf("synthetic index collided with live cohort: first=%q second=%q", first.index, second.index)
	}
}

func TestBuildRequestRejectsAmbiguousPrivateSelectorScope(t *testing.T) {
	for _, kind := range []string{"label", "expression", "match-key", "mismatch-key", "scoped-negative", "incarnation", "spread", "spread-match-key"} {
		t.Run(kind, func(t *testing.T) {
			objects, source := validSingleSourceObjects()
			pod := sourcePods(objects)[0]
			term := corev1.PodAffinityTerm{TopologyKey: "topology.kubernetes.io/zone", LabelSelector: &metav1.LabelSelector{}}
			switch kind {
			case "label", "spread":
				term.LabelSelector.MatchLabels = map[string]string{labelInstanceIndex: "2"}
			case "expression":
				term.LabelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: labelInstanceIndex, Operator: metav1.LabelSelectorOpIn, Values: []string{"2"}}}
			case "match-key":
				term.MatchLabelKeys = []string{labelInstanceIndex}
			case "mismatch-key":
				term.MismatchLabelKeys = []string{labelInstanceIndex}
			case "scoped-negative":
				term.LabelSelector.MatchLabels = map[string]string{labelInferenceService: "svc", labelComponent: "engine"}
				term.LabelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: labelInstanceIndex, Operator: metav1.LabelSelectorOpNotIn, Values: []string{"2"}}}
			case "spread-match-key":
				term.MatchLabelKeys = []string{labelInstanceIndex}
			case "incarnation":
				term.LabelSelector.MatchLabels = map[string]string{labelInferenceService: "svc", labelComponent: "engine", labelInstanceIncarnation: "7"}
			}
			if kind == "spread" || kind == "spread-match-key" {
				pod.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: term.TopologyKey, WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: term.LabelSelector, MatchLabelKeys: term.MatchLabelKeys}}
			} else {
				pod.Spec.Affinity = &corev1.Affinity{PodAntiAffinity: &corev1.PodAntiAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{term}}}
			}
			// A same-namespace Pod can share an index or incarnation without
			// belonging to the source cohort. Rewriting a broad selector must
			// not silently remove this Pod from its scheduling constraints.
			other := readySourcePod("other-source", "other-source-uid", "target-a", v1beta1.RunnerNameDefault, 0, "default-scheduler")
			other.Labels[labelInferenceService] = "other-service"
			other.OwnerReferences = nil
			if kind == "incarnation" {
				other.Labels[labelInferenceService] = "svc"
				other.Labels[labelInstanceIndex] = "3"
			}
			objects = append(objects, other)
			snap := captureSourceFixture(t, objects)
			_, err := BuildRequest(snap, source, testProfiles(false), "ambiguous", captureTime.Add(time.Second), time.Minute)
			if err == nil || !strings.Contains(err.Error(), "identity") {
				t.Fatalf("BuildRequest() error = %v, want ambiguous identity rejection", err)
			}
		})
	}
}

func TestBuildRequestRetargetsScopedPrivateSelectors(t *testing.T) {
	for _, kind := range []string{"label", "expression", "match-key", "spread-match-key", "pod-group", "group-match-key"} {
		t.Run(kind, func(t *testing.T) {
			objects, source := validGangSourceObjects()
			leader := sourcePods(objects)[0]
			term := corev1.PodAffinityTerm{
				TopologyKey: "topology.kubernetes.io/zone",
				LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
					labelInferenceService: "svc", labelComponent: "engine",
				}},
			}
			switch kind {
			case "label":
				term.LabelSelector.MatchLabels[labelInstanceIndex] = "2"
			case "expression":
				term.LabelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: labelInstanceIndex, Operator: metav1.LabelSelectorOpIn, Values: []string{"2"}}}
			case "match-key", "spread-match-key":
				term.MatchLabelKeys = []string{labelInstanceIndex}
			case "pod-group":
				term.LabelSelector.MatchLabels = map[string]string{labelPodGroup: "svc-engine-2"}
			case "group-match-key":
				term.LabelSelector.MatchLabels = nil
				term.MatchLabelKeys = []string{labelPodGroup}
			}
			if kind == "spread-match-key" {
				leader.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: term.TopologyKey, WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: term.LabelSelector, MatchLabelKeys: term.MatchLabelKeys}}
			} else {
				leader.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{term}}}
			}
			snap := captureSourceFixture(t, objects)
			request, err := BuildRequest(snap, source, testProfiles(true), "scoped", captureTime.Add(time.Second), time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			replacement := podByRunner(t, request.ReplacementPods, string(v1beta1.RunnerNameLeader))
			if kind == "spread-match-key" {
				if !reflect.DeepEqual(replacement.Spec.TopologySpreadConstraints[0].MatchLabelKeys, term.MatchLabelKeys) {
					t.Fatal("dynamic topology spread key changed")
				}
				return
			}
			got := replacement.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0]
			switch kind {
			case "label":
				if got.LabelSelector.MatchLabels[labelInstanceIndex] != replacement.Labels[labelInstanceIndex] {
					t.Fatal("static index selector did not follow replacement")
				}
			case "expression":
				if got.LabelSelector.MatchExpressions[0].Values[0] != replacement.Labels[labelInstanceIndex] {
					t.Fatal("index expression did not follow replacement")
				}
			case "pod-group":
				if got.LabelSelector.MatchLabels[labelPodGroup] != replacement.Labels[labelPodGroup] {
					t.Fatal("group selector did not follow replacement")
				}
			default:
				if !reflect.DeepEqual(got.MatchLabelKeys, term.MatchLabelKeys) {
					t.Fatal("dynamic affinity key changed")
				}
			}
		})
	}
}

func TestBuildRequestRejectsUntrustworthySourceState(t *testing.T) {
	tests := []struct {
		name    string
		gang    bool
		mutate  func([]client.Object)
		profile func(bool) scheduling.Config
		want    string
	}{
		{name: "stale ISVC observation", mutate: func(objects []client.Object) {
			sourceISVC(objects).Status.Components[v1beta1.EngineComponent].Lifecycle.ObservedGeneration--
		}, want: "InferenceService observation"},
		{name: "stale IR observation", mutate: func(objects []client.Object) { sourceIR(objects).Status.ObservedGeneration-- }, want: "InferenceReplica observation"},
		{name: "IR owner UID mismatch", mutate: func(objects []client.Object) { sourceIR(objects).OwnerReferences[0].UID = "other-isvc" }, want: "owner"},
		{name: "pod owner UID mismatch", mutate: func(objects []client.Object) { sourcePods(objects)[0].OwnerReferences[0].UID = "other-ir" }, want: "owner"},
		{name: "revision mismatch", mutate: func(objects []client.Object) { sourcePods(objects)[0].Labels[labelRevisionHash] = "other" }, want: "revision"},
		{name: "Ready with target revision", mutate: func(objects []client.Object) {
			ir := sourceIR(objects)
			ir.Status.InstanceStatuses[0].TargetRevision = ir.Status.InstanceStatuses[0].RunningRevision
		}, want: "revision"},
		{name: "reused ordinal", gang: true, mutate: func(objects []client.Object) {
			sourcePods(objects)[1].Labels[labelRunner] = sourcePods(objects)[0].Labels[labelRunner]
		}, want: "member"},
		{name: "missing gang member", gang: true, mutate: func(objects []client.Object) { removeLastSourcePod(objects) }, want: "complete"},
		{name: "unknown profile", profile: func(bool) scheduling.Config { return scheduling.Config{} }, want: "profile"},
		{name: "ambiguous private selector", gang: true, mutate: func(objects []client.Object) {
			worker := sourcePods(objects)[1]
			worker.Spec.Affinity.PodAffinity.RequiredDuringSchedulingIgnoredDuringExecution[0].LabelSelector.MatchExpressions = []metav1.LabelSelectorRequirement{{Key: labelInstanceIndex, Operator: metav1.LabelSelectorOpIn, Values: []string{"2", "3"}}}
		}, want: "identity"},
		{name: "changed topology", gang: true, mutate: func(objects []client.Object) {
			key := "topology.example/rack"
			sourceIR(objects).Spec.TopologyKey = &key
		}, want: "topology"},
		{name: "removed topology", gang: true, mutate: func(objects []client.Object) {
			sourceIR(objects).Spec.TopologyKey = nil
		}, want: "topology"},
		{name: "pinned runner template", mutate: func(objects []client.Object) {
			sourceIR(objects).Spec.Runners[0].Template.Spec.NodeName = "source-a"
		}, want: "pinned"},
		{name: "active migration", mutate: func(objects []client.Object) {
			sourceIR(objects).Status.Migrations = []v1beta1.MigrationStatus{{SourceInstance: 2, Phase: v1beta1.MigrationPhaseAccepted}}
		}, want: "active migration"},
		{name: "gang profile unsupported", gang: true, profile: func(bool) scheduling.Config {
			return scheduling.Config{Profiles: map[string]scheduling.Profile{"ome-scheduler": {Backend: "ome", SchedulerVersion: "v1", ConfigurationID: "config"}}}
		}, want: "GangUnsupported"},
		{name: "paused", mutate: func(objects []client.Object) { sourceIR(objects).Spec.Paused = true }, want: "paused"},
		{name: "deleting source", mutate: func(objects []client.Object) {
			isvc := sourceISVC(objects)
			isvc.Finalizers = []string{"test/finalizer"}
			at := metav1.NewTime(captureTime)
			isvc.DeletionTimestamp = &at
		}, want: "deleting"},
		{name: "persistent storage", mutate: func(objects []client.Object) {
			sourcePods(objects)[0].Spec.Volumes = []corev1.Volume{{Name: "model", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "model"}}}}
		}, want: "storage"},
		{name: "DRA", mutate: func(objects []client.Object) {
			claim := "gpu-claim"
			sourcePods(objects)[0].Spec.ResourceClaims = []corev1.PodResourceClaim{{Name: "gpu", ResourceClaimName: &claim}}
		}, want: "resource claim"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			objects, source := validSingleSourceObjects()
			if tc.gang {
				objects, source = validGangSourceObjects()
			}
			sourcePodsSlice = objects
			if tc.mutate != nil {
				tc.mutate(objects)
				objects = sourcePodsSlice
			}
			profile := testProfiles(tc.gang)
			if tc.profile != nil {
				profile = tc.profile(tc.gang)
			}
			snap := captureSourceFixture(t, objects)
			_, err := BuildRequest(snap, source, profile, "request", captureTime.Add(time.Second), time.Minute)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("BuildRequest() error = %v, want substring %q", err, tc.want)
			}
		})
	}
}

// sourcePodsSlice lets the table's structural deletion cases replace the slice
// while its ordinary mutators remain small. Tests run serially.
var sourcePodsSlice []client.Object

func removeLastSourcePod(objects []client.Object) {
	pods := sourcePods(objects)
	remove := pods[len(pods)-1]
	result := make([]client.Object, 0, len(objects)-1)
	for _, object := range objects {
		if object != remove {
			result = append(result, object)
		}
	}
	sourcePodsSlice = result
}

func captureSourceFixture(t *testing.T, objects []client.Object) *Snapshot {
	t.Helper()
	snap, err := Capture(context.Background(), captureReader(t, objects...), func() time.Time { return captureTime })
	if err != nil {
		t.Fatalf("Capture() error = %v", err)
	}
	return snap
}

func testProfiles(gang bool) scheduling.Config {
	name := "default-scheduler"
	profile := scheduling.Profile{Backend: "kube-scheduler", SchedulerVersion: "v1.35.8", ConfigurationID: "default-config"}
	if gang {
		name = "ome-scheduler"
		profile = scheduling.Profile{Backend: "ome-scheduler", SchedulerVersion: "v1.35.8", ConfigurationID: "ome-config", GangScheduling: true}
	}
	return scheduling.Config{Profiles: map[string]scheduling.Profile{name: profile}}
}

// validSingleSourceObjects is shared with package-level integration tests. It
// returns a complete public-API source suitable for Capture then BuildRequest.
func validSingleSourceObjects() ([]client.Object, Source) {
	objects, source := baseSourceObjects([]v1beta1.Runner{{
		Name: v1beta1.RunnerNameDefault, Size: 1,
		Template: corev1.PodTemplateSpec{Spec: sourcePodSpec("default-scheduler")},
	}}, 1)
	pod := readySourcePod("svc-engine-2-default-0", "source-pod-uid", "source-a", v1beta1.RunnerNameDefault, 0, "default-scheduler")
	return append(objects, pod), source
}

// validGangSourceObjects is shared with package-level integration tests. It
// includes the observed PodGroup and generated worker-to-leader affinity.
func validGangSourceObjects() ([]client.Object, Source) {
	topology := "topology.kubernetes.io/zone"
	objects, source := baseSourceObjects([]v1beta1.Runner{
		{Name: v1beta1.RunnerNameLeader, Size: 1, Template: corev1.PodTemplateSpec{Spec: sourcePodSpec("ome-scheduler")}},
		{Name: v1beta1.RunnerNameWorker, Size: 1, Template: corev1.PodTemplateSpec{Spec: sourcePodSpec("ome-scheduler")}},
	}, 2)
	sourceIR(objects).Spec.TopologyKey = &topology
	leader := readySourcePod("svc-engine-2-leader-0", "leader-pod-uid", "source-a", v1beta1.RunnerNameLeader, 0, "ome-scheduler")
	worker := readySourcePod("svc-engine-2-worker-0", "worker-pod-uid", "source-b", v1beta1.RunnerNameWorker, 0, "ome-scheduler")
	for _, pod := range []*corev1.Pod{leader, worker} {
		pod.Labels[labelPodGroup] = "svc-engine-2"
	}
	worker.Spec.Affinity = &corev1.Affinity{PodAffinity: &corev1.PodAffinity{RequiredDuringSchedulingIgnoredDuringExecution: []corev1.PodAffinityTerm{{
		TopologyKey: topology,
		LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{
			labelInferenceService: "svc", labelComponent: "engine", labelInstanceIndex: "2", labelRunner: "leader",
		}},
	}}}}
	group := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "scheduling.x-k8s.io/v1alpha1",
		"kind":       "PodGroup",
		"metadata": map[string]any{
			"namespace": "team", "name": "svc-engine-2", "uid": "source-group-uid", "resourceVersion": "12",
			"annotations":     map[string]any{annotationTopologyKey: topology},
			"labels":          map[string]any{labelInferenceService: "svc", labelComponent: "engine", labelManagedBy: managedByOMENative, labelInstanceIndex: "2"},
			"ownerReferences": []any{map[string]any{"apiVersion": v1beta1.SchemeGroupVersion.String(), "kind": "InferenceReplica", "name": "svc-engine", "uid": string(testIRUID), "controller": true}},
		},
		"spec":   map[string]any{"minMember": int64(2), "scheduleTimeoutSeconds": int64(30)},
		"status": map[string]any{"phase": "Running", "running": int64(2)},
	}}
	return append(objects, leader, worker, group), source
}

func baseSourceObjects(runners []v1beta1.Runner, count int32) ([]client.Object, Source) {
	lifecycle := &v1beta1.LifecycleStatus{ObservedGeneration: 5, CurrentRevision: "svc-engine-rev-a", UpdateRevision: "svc-engine-rev-a"}
	isvc := &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Namespace: "team", Name: "svc", UID: testISVCUID, Generation: 5, ResourceVersion: "12"},
		Spec:       v1beta1.InferenceServiceSpec{Engine: &v1beta1.EngineSpec{}},
		Status: v1beta1.InferenceServiceStatus{Components: map[v1beta1.ComponentType]v1beta1.ComponentStatusSpec{
			v1beta1.EngineComponent: {Lifecycle: lifecycle, RolloutPhase: v1beta1.RolloutPhaseStable},
		}},
	}
	ir := &v1beta1.InferenceReplica{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "team", Name: "svc-engine", UID: testIRUID, Generation: 3, ResourceVersion: "12",
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceService", Name: "svc", UID: testISVCUID, Controller: ptr.To(true)}},
		},
		Spec: v1beta1.InferenceReplicaSpec{ParentRef: v1beta1.ParentReference{Name: "svc"}, Component: v1beta1.EngineComponent, Runners: runners},
		Status: v1beta1.InferenceReplicaStatus{
			ObservedGeneration: 3, Replicas: 1, ReadyReplicas: 1, ServingReplicas: 1, AvailableReplicas: 1,
			CurrentRevision: "svc-engine-rev-a", UpdateRevision: "svc-engine-rev-a",
			InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{
				Index: 2, Incarnation: 7, Phase: v1beta1.OMENativeInstanceReady,
				RunningRevision: "svc-engine-rev-a", PodCount: count,
				ServingPodCount: count, AvailablePodCount: count, Admitted: true,
			}},
		},
	}
	nodes := []client.Object{
		&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "team", UID: "team-uid", ResourceVersion: "12"}},
		readyNode("source-a", "zone-a"), readyNode("source-b", "zone-a"),
		readyNode("target-a", "zone-b"), readyNode("target-b", "zone-b"),
	}
	return append(nodes, isvc, ir), Source{Namespace: "team", InferenceService: "svc", Component: v1beta1.EngineComponent, Instance: 2, FromNode: "source-a"}
}

func readyNode(name, zone string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name, UID: types.UID(name + "-uid"), ResourceVersion: "12", Labels: map[string]string{"topology.kubernetes.io/zone": zone}},
		Status:     corev1.NodeStatus{Capacity: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("64Gi")}, Allocatable: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("16"), corev1.ResourceMemory: resource.MustParse("64Gi")}, Conditions: []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}},
	}
}

func sourcePodSpec(scheduler string) corev1.PodSpec {
	priority := int32(100)
	return corev1.PodSpec{
		SchedulerName: scheduler, Priority: &priority, PriorityClassName: "important",
		NodeSelector: map[string]string{"accelerator": "gpu"},
		Tolerations:  []corev1.Toleration{{Key: "gpu", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoSchedule}},
		Containers:   []corev1.Container{{Name: "model", Image: "example.invalid/model:v1", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}, Limits: corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("2"), corev1.ResourceMemory: resource.MustParse("4Gi")}}}},
	}
}

func readySourcePod(name string, uid types.UID, node string, runner v1beta1.RunnerName, ordinal int32, scheduler string) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "team", Name: name, UID: uid, ResourceVersion: "12", CreationTimestamp: metav1.NewTime(captureTime.Add(-time.Hour)),
			Labels: map[string]string{
				labelInferenceService: "svc", labelComponent: "engine", labelManagedBy: managedByOMENative,
				labelInstanceIndex: "2", labelInstanceIncarnation: "7", labelRunner: string(runner),
				labelPodOrdinal: string(rune('0' + ordinal)), labelRevisionHash: "a",
			},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: v1beta1.SchemeGroupVersion.String(), Kind: "InferenceReplica", Name: "svc-engine", UID: testIRUID, Controller: ptr.To(true)}},
		},
		Spec:   sourcePodSpec(scheduler),
		Status: corev1.PodStatus{Phase: corev1.PodRunning, Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}}},
	}
	pod.Spec.NodeName = node
	return pod
}

func sourceISVC(objects []client.Object) *v1beta1.InferenceService {
	for _, object := range objects {
		if typed, ok := object.(*v1beta1.InferenceService); ok {
			return typed
		}
	}
	panic("source ISVC missing")
}

func sourceIR(objects []client.Object) *v1beta1.InferenceReplica {
	for _, object := range objects {
		if typed, ok := object.(*v1beta1.InferenceReplica); ok {
			return typed
		}
	}
	panic("source IR missing")
}

func sourcePods(objects []client.Object) []*corev1.Pod {
	var pods []*corev1.Pod
	for _, object := range objects {
		if typed, ok := object.(*corev1.Pod); ok {
			pods = append(pods, typed)
		}
	}
	return pods
}

func podByRunner(t *testing.T, pods []corev1.Pod, runner string) corev1.Pod {
	t.Helper()
	for _, pod := range pods {
		if pod.Labels[labelRunner] == runner {
			return pod
		}
	}
	t.Fatalf("runner %q missing from %v", runner, pods)
	return corev1.Pod{}
}
