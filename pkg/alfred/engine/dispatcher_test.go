package engine

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/policy/defrag"
	"sigs.k8s.io/ome/pkg/alfred/scheduling"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
	v1beta1 "sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
	codec "sigs.k8s.io/ome/pkg/controller/v1beta1/irstatus"
)

func TestDispatchFingerprintUsesLogicalSourceAcrossEncodings(t *testing.T) {
	// Catches hash parity that ignores the row entirely or hashes only dense storage.
	owner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{UID: "owner", Generation: 2}}
	ir := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{UID: "replica", Generation: 3}, Status: v1beta1.InferenceReplicaStatus{
		CurrentRevision: "rev", InstanceStatuses: []v1beta1.OMENativeInstanceStatus{{Index: 7, Phase: v1beta1.OMENativeInstanceReady,
			Incarnation: 2, RunningRevision: "rev", PodCount: 1, ServingPodCount: 1, AvailablePodCount: 1, Admitted: true}},
	}}
	dense, err := dispatchSourceFingerprint(owner, ir, nil, 7)
	if err != nil {
		t.Fatal(err)
	}
	columns, err := codec.EncodeColumns(ir.Status.InstanceStatuses, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatuses = nil
	ir.Status.InstanceStatusEncoding = &marker
	ir.Status.InstanceStatusColumns = columns
	compact, err := dispatchSourceFingerprint(owner, ir, nil, 7)
	if err != nil {
		t.Fatal(err)
	}
	if dense != compact {
		t.Fatalf("equivalent logical source changed fingerprint: dense=%s compact=%s", dense, compact)
	}
	ir.Status.InstanceStatusColumns.RunningRevisions = nil
	changed, err := dispatchSourceFingerprint(owner, ir, nil, 7)
	if err != nil {
		t.Fatal(err)
	}
	if changed == dense {
		t.Fatal("changed logical source retained fingerprint")
	}
	unknown := v1beta1.InstanceStatusEncoding("Future")
	ir.Status.InstanceStatusEncoding = &unknown
	if fingerprint, err := dispatchSourceFingerprint(owner, ir, nil, 7); err == nil || fingerprint != "" {
		t.Fatalf("undecodable source fingerprint = %q, %v; want error", fingerprint, err)
	}
}

func TestDispatcherSubmitsColumnarSourceWithDenseObservation(t *testing.T) {
	// Catches a representation-only change blocking otherwise safe dispatch.
	d, cl, observed, candidate, cfg, arbiter := dispatchFixture(t, false)
	var ir v1beta1.InferenceReplica
	if err := cl.Client.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a-engine"}, &ir); err != nil {
		t.Fatal(err)
	}
	columns, err := codec.EncodeColumns(ir.Status.InstanceStatuses, 10_000)
	if err != nil {
		t.Fatal(err)
	}
	marker := v1beta1.InstanceStatusEncodingColumnarV2
	ir.Status.InstanceStatuses = nil
	ir.Status.InstanceStatusEncoding = &marker
	ir.Status.InstanceStatusColumns = columns
	if err := cl.Client.Update(context.Background(), &ir); err != nil {
		t.Fatal(err)
	}
	_, decisions := d.Execute(context.Background(), observed, []policy.Candidate{candidate}, cfg, arbiter)
	got := decisionFor(t, decisions, "prod/a")
	if got.DispatchStatus != "submitted" || cl.patches != 1 {
		t.Fatalf("compact source not dispatched once: %+v patches=%d", got, cl.patches)
	}
}

type dispatchClient struct {
	client.Client
	patches         int
	journalWrites   int
	failJournalAt   int
	uncertain       bool
	failBeforeApply bool
	beforePatch     func()
}

func (c *dispatchClient) Update(ctx context.Context, o client.Object, opts ...client.UpdateOption) error {
	if cm, ok := o.(*corev1.ConfigMap); ok && cm.Name == "alfred-dispatch-state" {
		c.journalWrites++
		if c.journalWrites == c.failJournalAt {
			return errors.New("journal unavailable")
		}
	} else {
		return errors.New("unexpected update target")
	}
	return c.Client.Update(ctx, o, opts...)
}
func (c *dispatchClient) Patch(ctx context.Context, o client.Object, patch client.Patch, opts ...client.PatchOption) error {
	if _, ok := o.(*v1beta1.InferenceService); !ok {
		return errors.New("unexpected mutation target")
	}
	c.patches++
	var cm corev1.ConfigMap
	if err := c.Client.Get(ctx, types.NamespacedName{Namespace: "ome", Name: "alfred-dispatch-state"}, &cm); err != nil {
		return err
	}
	if !strings.Contains(cm.Data["state.json"], `"lastAttempt"`) {
		return errors.New("write happened without persisted attempt")
	}
	if c.failBeforeApply {
		return errors.New("connection failed before apply")
	}
	if c.beforePatch != nil {
		c.beforePatch()
	}
	if err := c.Client.Patch(ctx, o, patch, opts...); err != nil {
		return err
	}
	if c.uncertain {
		return errors.New("response lost after applied patch")
	}
	return nil
}

func dispatchFixture(t *testing.T, gang bool) (*Dispatcher, *dispatchClient, *snapshot.ClusterSnapshot, policy.Candidate, *config.Config, *Arbiter) {
	t.Helper()
	snap, r, c := predictionScenario(t, gang)
	cl := &dispatchClient{Client: r.Reader.(client.Client)}
	if err := cl.Client.Create(context.Background(), &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Namespace: "ome", Name: "alfred-dispatch-state"}, Data: map[string]string{"state.json": `{"version":"v1","entries":[]}`}}); err != nil {
		t.Fatal(err)
	}
	c.Executable, c.AdvisoryReason, c.FootprintGPUs = true, "", 1
	c.HintTargetNodes = []string{"target"}
	c.PlacementTargetNodes = []string{"target", "target-worker"}
	cfg, err := config.Load([]byte(schedulingProfilesYAML + "mode: execute\n"))
	if err != nil {
		t.Fatal(err)
	}
	if gang {
		cfg.Scheduling.Profiles["ome-scheduler"] = cfg.Scheduling.Profiles["custom-gang"]
	}
	d := &Dispatcher{Reader: cl.Client, Client: cl, Namespace: "ome", Now: func() time.Time { return testNow },
		Guard: func(context.Context) error { return nil }, Options: DispatchOptions{APIVersion: "v1", AcknowledgementTimeout: 2 * time.Minute, FailureBackoff: 5 * time.Minute},
		Policies: []policy.Policy{&stubPolicy{out: []policy.Candidate{c}}}, Simulator: simulationFunc(func(_ context.Context, r scheduling.Request) (scheduling.Result, error) {
			return feasiblePrediction(r), nil
		})}
	return d, cl, snap, c, cfg, &Arbiter{Ledger: NewLedger()}
}

func TestDispatcherSubmitsWholeInstanceWithWriteAheadJournal(t *testing.T) {
	for _, gang := range []bool{false, true} {
		t.Run(map[bool]string{false: "single", true: "gang"}[gang], func(t *testing.T) {
			d, cl, snap, c, cfg, a := dispatchFixture(t, gang)
			d.Simulator = simulationFunc(func(_ context.Context, r scheduling.Request) (scheduling.Result, error) {
				want := 1
				if gang {
					want = 2
				}
				if len(r.SourcePods) != want || len(r.ReplacementPods) != want || len(r.ExcludedNodes) != 1 || r.ExcludedNodes[0] != "source" {
					t.Fatalf("not actual whole-instance request: %+v", r)
				}
				return feasiblePrediction(r), nil
			})
			_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
			got := decisionFor(t, decisions, "prod/a")
			if got.DispatchStatus != "submitted" || got.RequestUUID == "" || cl.patches != 1 {
				t.Fatalf("not submitted exactly once: %+v patches=%d", got, cl.patches)
			}
			_, j, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
			if err != nil {
				t.Fatal(err)
			}
			if len(j.Entries) != 1 || j.Entries[0].Phase != "submitted" || j.Entries[0].LastAttempt == nil {
				t.Fatalf("bad durable state: %+v", j)
			}
			// Simulate leader replacement; the same observed source must not generate a new UUID.
			d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, &Arbiter{Ledger: NewLedger()})
			if cl.patches != 1 {
				t.Fatalf("duplicate request after restart: %d", cl.patches)
			}
		})
	}
}

func TestDispatcherUnchangedEmptyJournalDoesNotWrite(t *testing.T) {
	for _, mode := range []string{"idle", "recommend-only", "guard-withheld", "opt-in-disabled"} {
		t.Run(mode, func(t *testing.T) {
			d, cl, observed, c, cfg, arbiter := dispatchFixture(t, false)
			candidates := []policy.Candidate{c}
			switch mode {
			case "idle":
				candidates = nil
			case "recommend-only":
				cfg.Mode = config.ModeRecommendOnly
			case "guard-withheld":
				d.Guard = func(context.Context) error { return errors.New("guard unavailable") }
			case "opt-in-disabled":
				d.Options.APIVersion = ""
			}
			before, _, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
			if err != nil {
				t.Fatal(err)
			}
			for range 2 {
				d.Execute(context.Background(), observed, candidates, cfg, arbiter)
			}
			after, _, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
			if err != nil {
				t.Fatal(err)
			}
			if cl.journalWrites != 0 || cl.patches != 0 || before.ResourceVersion != after.ResourceVersion || before.Data["state.json"] != after.Data["state.json"] {
				t.Fatalf("unchanged cycle rewrote empty state: updates=%d patches=%d resourceVersions=%s/%s", cl.journalWrites, cl.patches, before.ResourceVersion, after.ResourceVersion)
			}
		})
	}
}

func TestDispatcherSubmitsRealDefragmentationCandidate(t *testing.T) {
	d, cl, _, _, cfg, arbiter := dispatchFixture(t, false)
	var ir v1beta1.InferenceReplica
	if err := cl.Client.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a-engine"}, &ir); err != nil {
		t.Fatal(err)
	}
	ir.Status.InstanceStatuses[0].TargetRevision = ""
	ir.Status.Replicas, ir.Status.ReadyReplicas, ir.Status.ServingReplicas = 1, 1, 1
	ir.Status.AvailableReplicas, ir.Status.UpdatedReplicas, ir.Status.UpdatedReadyReplicas = 1, 1, 1
	if err := cl.Client.Update(context.Background(), &ir); err != nil {
		t.Fatal(err)
	}
	// Moving the one-GPU source into this one-GPU hole frees an entire node.
	occupant := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "other-occupant", UID: "other-occupant"},
		Spec: corev1.PodSpec{NodeName: "target", SchedulerName: "default-scheduler", Containers: []corev1.Container{{Name: "other", Image: "example.invalid/other:v1",
			Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{"nvidia.com/gpu": resource.MustParse("7")}}}}},
		Status: corev1.PodStatus{Phase: corev1.PodRunning}}
	if err := cl.Client.Create(context.Background(), occupant); err != nil {
		t.Fatal(err)
	}
	*cfg.Policies.Defragmentation.FragmentationThreshold = 0.1
	d.Policies = []policy.Policy{&defrag.Policy{}}
	observed, err := d.freshObservation(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	candidates := d.Policies[0].Evaluate(observed, cfg)
	if len(candidates) != 1 || !candidates[0].Executable {
		t.Fatalf("fixture must produce a real executable candidate: %+v", candidates)
	}
	_, decisions := d.Execute(context.Background(), observed, candidates, cfg, arbiter)
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" || cl.patches != 1 {
		t.Fatalf("real policy candidate was not dispatched: %+v patches=%d", got, cl.patches)
	}
}

func TestDispatcherWithholdsUnprovenPriorMigrationAffinityBeforeWorker(t *testing.T) {
	for _, member := range []string{"source-pod", "source-worker"} {
		t.Run(member, func(t *testing.T) {
			d, cl, _, c, cfg, arbiter := dispatchFixture(t, member == "source-worker")
			var pod corev1.Pod
			if err := cl.Client.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: member}, &pod); err != nil {
				t.Fatal(err)
			}
			if pod.Spec.Affinity == nil {
				pod.Spec.Affinity = &corev1.Affinity{}
			}
			pod.Spec.Affinity.NodeAffinity = &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{{MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpNotIn, Values: []string{"earlier-source"},
				}}}}},
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{Weight: 50, Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{
					Key: corev1.LabelHostname, Operator: corev1.NodeSelectorOpIn, Values: []string{"source", "earlier-alternate"},
				}}}}},
			}
			if err := cl.Client.Update(context.Background(), &pod); err != nil {
				t.Fatal(err)
			}
			observed, err := d.freshObservation(context.Background(), cfg)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			d.Simulator = simulationFunc(func(_ context.Context, r scheduling.Request) (scheduling.Result, error) {
				calls++
				return feasiblePrediction(r), nil
			})
			_, decisions := d.Execute(context.Background(), observed, []policy.Candidate{c}, cfg, arbiter)
			if got := decisionFor(t, decisions, "prod/a"); got.DispatchReason != "SourceUnsupported" || calls != 0 || cl.patches != 0 {
				t.Fatalf("unproven live affinity reached worker or dispatch: %+v calls=%d patches=%d", got, calls, cl.patches)
			}
		})
	}
}

func TestDispatcherRetainsLongCooldownAcrossUnrelatedDispatchAndRestart(t *testing.T) {
	for _, window := range []string{"node", "workload", "override", "full-journal"} {
		t.Run(window, func(t *testing.T) {
			d, cl, snap, c, cfg, arbiter := dispatchFixture(t, false)
			switch window {
			case "node", "full-journal":
				cfg.PerNodeCooldownMinutes = 120
			case "workload":
				cfg.PerWorkloadCooldownMinutes = 120
			case "override":
				owner := &v1beta1.InferenceService{ObjectMeta: metav1.ObjectMeta{Namespace: "prod", Name: "earlier", UID: "earlier-owner", Annotations: map[string]string{constants.AlfredCooldownMinutesAnnotationKey: "120"}}}
				if err := cl.Client.Create(context.Background(), owner); err != nil {
					t.Fatal(err)
				}
			}
			cm, j, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
			if err != nil {
				t.Fatal(err)
			}
			earlier := cand("prod/earlier", "earlier-source", "earlier-target")
			entry, err := newDispatchEntry(earlier, "earlier-owner", "earlier-engine", "earlier-ir", "fingerprint", testNow.Add(-100*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			completed := testNow.Add(-90 * time.Minute)
			entry.Phase, entry.CompletedAt, entry.Targets = dispatchCompleted, &completed, []string{"earlier-target"}
			j.Entries = append(j.Entries, entry)
			if window == "full-journal" {
				for len(j.Entries) < dispatchMaxEntries {
					next, err := newDispatchEntry(earlier, "earlier-owner", "earlier-engine", "earlier-ir", "fingerprint", entry.CreatedAt)
					if err != nil {
						t.Fatal(err)
					}
					next.Phase, next.CompletedAt = dispatchCompleted, &completed
					j.Entries = append(j.Entries, next)
				}
			}
			if err := saveDispatchJournal(context.Background(), cl.Client, cm, j); err != nil {
				t.Fatal(err)
			}
			_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, arbiter)
			if window == "full-journal" {
				if got := decisionFor(t, decisions, "prod/a"); got.DispatchReason != "JournalUnavailable" || cl.patches != 0 {
					t.Fatalf("needed history discarded to make room for a dispatch: %+v patches=%d", got, cl.patches)
				}
				_, restored, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
				if err != nil {
					t.Fatal(err)
				}
				if len(restored.Entries) != dispatchMaxEntries || restored.Entries[0].UUID != entry.UUID {
					t.Fatalf("full journal was mutated on failed append: %+v", restored.Entries)
				}
				return
			}
			if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" {
				t.Fatalf("unrelated dispatch failed: %+v", got)
			}
			// A fresh process reconstructs cooldowns exclusively from persisted state.
			_, restored, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
			if err != nil {
				t.Fatal(err)
			}
			if len(restored.Entries) != 2 || restored.Entries[0].UUID != entry.UUID {
				t.Fatalf("still-needed cooldown history was pruned: %+v", restored.Entries)
			}
			if !journalNodeCooling(restored, "earlier-target", 2*time.Hour, testNow, nil) {
				t.Fatal("restart lost node cooldown history")
			}
		})
	}
}

func TestDispatcherFailsClosedBeforeAnyWorkloadWrite(t *testing.T) {
	for _, name := range []string{"opt-in", "recommend-only", "guard", "journal", "stale", "advisory", "infeasible", "source-changed", "config-changed"} {
		t.Run(name, func(t *testing.T) {
			d, cl, snap, c, cfg, a := dispatchFixture(t, false)
			switch name {
			case "opt-in":
				d.Options.APIVersion = ""
			case "recommend-only":
				cfg.Mode = config.ModeRecommendOnly
			case "guard":
				d.Guard = func(context.Context) error { return errors.New("guard absent") }
			case "journal":
				cl.failJournalAt = 1
			case "stale":
				snap.Timestamp = testNow.Add(-time.Minute)
			case "advisory":
				c.Executable = false
			case "infeasible":
				d.Simulator = simulationFunc(func(_ context.Context, r scheduling.Request) (scheduling.Result, error) {
					return scheduling.Result{}, errors.New("no placement")
				})
			case "source-changed":
				d.Simulator = simulationFunc(func(_ context.Context, r scheduling.Request) (scheduling.Result, error) {
					var pod corev1.Pod
					if err := cl.Client.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "source-pod"}, &pod); err != nil {
						t.Fatal(err)
					}
					pod.Spec.Containers[0].Image = "new-image"
					if err := cl.Client.Update(context.Background(), &pod); err != nil {
						t.Fatal(err)
					}
					return feasiblePrediction(r), nil
				})
			case "config-changed":
				d.Store = config.NewStore()
				if _, err := d.Store.Update([]byte(schedulingProfilesYAML + "mode: execute\n")); err != nil {
					t.Fatal(err)
				}
				cfg = d.Store.Get()
				d.Simulator = simulationFunc(func(_ context.Context, r scheduling.Request) (scheduling.Result, error) {
					if _, err := d.Store.Update([]byte(schedulingProfilesYAML + "mode: recommend-only\n")); err != nil {
						t.Fatal(err)
					}
					return feasiblePrediction(r), nil
				})
			}
			d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
			if cl.patches != 0 {
				t.Fatalf("unsafe patch on %s", name)
			}
		})
	}
}

func TestDispatcherRejectsUnrepresentableConfiguredCooldowns(t *testing.T) {
	for _, field := range []string{"node", "workload", "health", "placement"} {
		for _, value := range []int{-1, int(math.MaxInt64/int64(time.Minute)) + 1} {
			t.Run(fmt.Sprintf("%s/%d", field, value), func(t *testing.T) {
				d, cl, snap, c, cfg, arbiter := dispatchFixture(t, false)
				switch field {
				case "node":
					cfg.PerNodeCooldownMinutes = value
				case "workload":
					cfg.PerWorkloadCooldownMinutes = value
				case "health":
					cfg.Policies.NodeHealth.HealthCooldownFloorMinutes = value
				case "placement":
					cfg.RecentPlacementCooldownMinutes = value
				}
				_, decisions := d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, arbiter)
				if got := decisionFor(t, decisions, "prod/a"); got.DispatchReason != "InvalidCooldown" || cl.patches != 0 {
					t.Fatalf("unsafe duration reached dispatch: %+v patches=%d", got, cl.patches)
				}
				_, journal, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
				if err != nil || len(journal.Entries) != 0 {
					t.Fatalf("invalid cooldown created a durable intent: journal=%+v err=%v", journal, err)
				}
			})
		}
	}
}

func TestDispatcherRetryKeepsExactUUIDAndPayload(t *testing.T) {
	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	cl.failBeforeApply = true
	d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
	_, before, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
	if err != nil {
		t.Fatal(err)
	}
	if len(before.Entries) != 1 {
		t.Fatalf("missing durable intent: %+v", before)
	}
	cl.failBeforeApply = false
	_, decisions := d.Execute(context.Background(), snap, nil, cfg, &Arbiter{Ledger: NewLedger()})
	var owner v1beta1.InferenceService
	if err := cl.Client.Get(context.Background(), c.Workload, &owner); err != nil {
		t.Fatal(err)
	}
	e := before.Entries[0]
	if owner.Annotations[migrationRequestPrefix+e.UUID] != e.Payload || cl.patches != 2 {
		t.Fatalf("retry changed intent: %+v patches=%d", owner.Annotations, cl.patches)
	}
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "submitted" || got.RequestUUID != e.UUID {
		t.Fatalf("retry result: %+v", got)
	}
}

func TestDispatcherAcknowledgementHonorsAPITimestampPrecision(t *testing.T) {
	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	now := testNow.Add(100 * time.Millisecond)
	d.Now = func() time.Time { return now }
	d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
	_, j, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Entries) != 1 {
		t.Fatalf("no request: %+v", j)
	}
	e := j.Entries[0]
	var ir v1beta1.InferenceReplica
	if err := cl.Client.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a-engine"}, &ir); err != nil {
		t.Fatal(err)
	}
	ir.Status.Migrations = []v1beta1.MigrationStatus{{RequestUUID: e.UUID, Trigger: v1beta1.MigrationTriggerManual, SourceInstance: 0, FromNode: "source", Phase: v1beta1.MigrationPhaseAccepted, StartedAt: metav1.NewTime(testNow), Deadline: metav1.NewTime(testNow.Add(time.Hour))}}
	if err := cl.Client.Update(context.Background(), &ir); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	_, decisions := d.Execute(context.Background(), snap, nil, cfg, a)
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "acknowledged" {
		t.Fatalf("valid second-precision acknowledgement lost: %+v", got)
	}
	_, j, err = loadDispatchJournal(context.Background(), cl.Client, "ome")
	if err != nil {
		t.Fatal(err)
	}
	if j.Entries[0].AcknowledgedAt == nil {
		t.Fatal("acknowledgement not durable")
	}
}

func TestDispatcherUncertainWriteStaysPendingAndLateStatusResolves(t *testing.T) {
	d, cl, snap, c, cfg, a := dispatchFixture(t, false)
	cl.uncertain = true
	d.Execute(context.Background(), snap, []policy.Candidate{c}, cfg, a)
	_, j, err := loadDispatchJournal(context.Background(), cl.Client, "ome")
	if err != nil {
		t.Fatal(err)
	}
	if len(j.Entries) != 1 || cl.patches != 1 {
		t.Fatalf("missing uncertain intent: %+v patches=%d", j, cl.patches)
	}
	id := j.Entries[0].UUID
	d.Now = func() time.Time { return testNow.Add(3 * time.Minute) }
	_, decisions := d.Execute(context.Background(), snap, nil, cfg, a)
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "stalled" || got.RequestUUID != id {
		t.Fatalf("timeout treated as cancellation: %+v", got)
	}
	var ir v1beta1.InferenceReplica
	if err := cl.Client.Get(context.Background(), types.NamespacedName{Namespace: "prod", Name: "a-engine"}, &ir); err != nil {
		t.Fatal(err)
	}
	done := metav1.NewTime(testNow.Add(3 * time.Minute))
	success := true
	ir.Status.Migrations = []v1beta1.MigrationStatus{{RequestUUID: id, Trigger: v1beta1.MigrationTriggerManual, SourceInstance: 0, FromNode: "source", Phase: v1beta1.MigrationPhaseCompleted, StartedAt: metav1.NewTime(testNow), Deadline: metav1.NewTime(testNow.Add(time.Hour)), CompletedAt: &done, Succeeded: &success}}
	if err := cl.Client.Update(context.Background(), &ir); err != nil {
		t.Fatal(err)
	}
	_, decisions = d.Execute(context.Background(), snap, nil, cfg, a)
	if got := decisionFor(t, decisions, "prod/a"); got.DispatchStatus != "completed" {
		t.Fatalf("late terminal status not reconciled: %+v", got)
	}
	if cl.patches != 1 {
		t.Fatalf("new UUID submitted after uncertain write: %d", cl.patches)
	}
}
