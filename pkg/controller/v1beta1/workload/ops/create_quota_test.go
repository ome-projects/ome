package ops

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
	workload "sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

// podCountQuota is a namespace's ResourceQuota on the pod count as
// admission enforces it for a test: the pods already charged, the limit,
// and the creates the test admitted so far. A create past the limit is
// refused in the apiserver's words.
type podCountQuota struct {
	name     string
	used     int
	hard     int
	admitted int
	attempts int
	deletes  int
}

func (q *podCountQuota) object(namespace string) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: q.name, Namespace: namespace},
		Status: corev1.ResourceQuotaStatus{
			Hard: corev1.ResourceList{corev1.ResourcePods: *resource.NewQuantity(int64(q.hard), resource.DecimalSI)},
			Used: corev1.ResourceList{corev1.ResourcePods: *resource.NewQuantity(int64(q.used), resource.DecimalSI)},
		},
	}
}

func (q *podCountQuota) refusal(podName string) error {
	return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, podName, fmt.Errorf(
		"exceeded quota: %s, requested: pods=1, used: pods=%d, limited: pods=%d", q.name, q.used+q.admitted, q.hard))
}

// enforce puts the quota in front of every pod create and counts the pod
// deletes, so a test can tell a set that was never created from one that
// was created and retired.
func (q *podCountQuota) enforce(t *testing.T, c client.Client) client.Client {
	t.Helper()
	base, ok := c.(client.WithWatch)
	if !ok {
		t.Fatalf("fixture client %T does not implement client.WithWatch", c)
	}
	return interceptor.NewClient(base, interceptor.Funcs{
		Create: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.CreateOption) error {
			pod, isPod := obj.(*corev1.Pod)
			if !isPod {
				return cl.Create(ctx, obj, opts...)
			}
			q.attempts++
			if q.used+q.admitted+1 > q.hard {
				return q.refusal(pod.Name)
			}
			if err := cl.Create(ctx, obj, opts...); err != nil {
				return err
			}
			q.admitted++
			return nil
		},
		Delete: func(ctx context.Context, cl client.WithWatch, obj client.Object, opts ...client.DeleteOption) error {
			if _, isPod := obj.(*corev1.Pod); isPod {
				q.deletes++
			}
			return cl.Delete(ctx, obj, opts...)
		},
	})
}

// gangOfFourPlan is one engine Instance that is a gang of a leader and
// three workers under the given strategy; every size a test reads comes
// from this plan.
func gangOfFourPlan(strategy workload.UpdateStrategyType) workload.ComponentPlan {
	plan := legacyMultiPodComponentPlan(strategy)
	plan.Instances[0].Runners[1].Size = 3
	return plan
}

func quotaEvents(recorder *record.FakeRecorder) []string {
	var events []string
	for {
		select {
		case e := <-recorder.Events:
			if strings.Contains(e, string(workload.EventReasonInstanceQuotaBlocked)) {
				events = append(events, e)
			}
		default:
			return events
		}
	}
}

func surgeMembers(t *testing.T, c client.Client, input workload.ReconcileInput, idx int32) []*corev1.Pod {
	t.Helper()
	pods, err := query.LiveListPodsForInstance(context.Background(), c, input.Key.Namespace, input.Key.OwnerName, input.Key.Component, idx)
	if err != nil {
		t.Fatalf("list the replacement's members: %v", err)
	}
	return pods
}

// TestGangSurgeCreate_QuotaShortOfTheGang_CreatesNoMember: a gang of four
// surges a replacement of four pods under a quota with room for three. The
// create pass reads the quota first and creates nothing: no member binds or
// waits at the gate while the gang is short, the refusal is recorded on the
// replacement's row and the wait names the gang's size. When the quota is
// raised, every member is created on the same pass.
func TestGangSurgeCreate_QuotaShortOfTheGang_CreatesNoMember(t *testing.T) {
	input, store, surgeIndex, revision := gangSurgeRejectionFixture(t)
	plan := gangOfFourPlan(workload.UpdateStrategySurgeThenDrain)
	members := int(plan.Instances[0].TotalPods())
	quota := &podCountQuota{name: "compute-resources", used: members, hard: 2*members - 1}
	base := legacyNewFakeClient(t, quota.object(input.Key.Namespace))
	recorder := record.NewFakeRecorder(16)
	deps := legacyTestDeps(quota.enforce(t, base))
	deps.Recorder = recorder
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v (a quota short of the gang is a wait, not an error)", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while the quota is short of the gang")
	}
	if got := surgeMembers(t, base, input, surgeIndex); len(got) != 0 {
		t.Fatalf("replacement members after the short pass: got %d want 0 (a gang that cannot start whole holds nothing)", len(got))
	}
	if quota.attempts != 0 {
		t.Errorf("pod creates attempted under a quota short of the gang: got %d want 0", quota.attempts)
	}
	replacementRow := store.statuses[surgeIndex]
	if !workload.OperationCapacityRefused(replacementRow.Operation) {
		t.Fatalf("replacement row Operation: got %+v want the quota refusal recorded", replacementRow.Operation)
	}
	events := quotaEvents(recorder)
	if len(events) != 1 {
		t.Fatalf("InstanceQuotaBlocked events: got %d want 1", len(events))
	}
	want := fmt.Sprintf("waiting on capacity for all %d members: quota %s leaves room for %d pods, the %d members need %d",
		members, quota.name, quota.hard-quota.used, members, members)
	if !strings.Contains(events[0], want) {
		t.Errorf("event: got %q want it to carry %q", events[0], want)
	}

	// The operator raises the quota by the one pod the gang was short.
	raised := quota.object(input.Key.Namespace)
	raised.Status.Hard[corev1.ResourcePods] = *resource.NewQuantity(int64(2*members), resource.DecimalSI)
	if err := base.Update(context.Background(), raised); err != nil {
		t.Fatalf("raise the quota: %v", err)
	}
	quota.hard = 2 * members
	legacyResetExpectations(t)
	input.ObservedState.InstanceStatuses = []workload.InstanceStatus{
		cloneTerminalStatus(store.statuses[gangSurgeSourceIndex]), cloneTerminalStatus(replacementRow),
	}
	if _, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target); err != nil {
		t.Fatalf("gangSurgeUpdate (room for the gang): %v", err)
	}
	if got := surgeMembers(t, base, input, surgeIndex); len(got) != members {
		t.Fatalf("replacement members once the room exists: got %d want %d on one pass", len(got), members)
	}
	if quota.deletes != 0 {
		t.Errorf("pod deletes: got %d want 0 (nothing was created to retire)", quota.deletes)
	}
	if workload.OperationCapacityRefused(store.statuses[surgeIndex].Operation) {
		t.Errorf("replacement row still records the refusal after every member was created")
	}
}

// TestGangSurgeCreate_QuotaRefusesMidSet_RetiresTheMembersCreated: the
// quota the create pass cannot read (none is listed) refuses the fourth
// member of a replacement gang after admitting three. The three admitted
// members are deleted on the same pass, the refusal is recorded and the
// wait names the gang's size and the members retired.
func TestGangSurgeCreate_QuotaRefusesMidSet_RetiresTheMembersCreated(t *testing.T) {
	input, store, surgeIndex, revision := gangSurgeRejectionFixture(t)
	plan := gangOfFourPlan(workload.UpdateStrategySurgeThenDrain)
	members := int(plan.Instances[0].TotalPods())
	quota := &podCountQuota{name: "compute-resources", used: members, hard: 2*members - 1}
	base := legacyNewFakeClient(t)
	recorder := record.NewFakeRecorder(16)
	deps := legacyTestDeps(quota.enforce(t, base))
	deps.Recorder = recorder
	target := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: revision}}

	done, err := gangSurgeUpdate(context.Background(), deps, input, plan, plan.Instances[0], target)
	if err != nil {
		t.Fatalf("gangSurgeUpdate: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("gangSurgeUpdate reported done while blocked on quota")
	}
	if quota.attempts != members {
		t.Errorf("pod creates attempted: got %d want %d (every member asked, the last refused)", quota.attempts, members)
	}
	if quota.deletes != members-1 {
		t.Errorf("pod deletes: got %d want %d (the members admitted are retired)", quota.deletes, members-1)
	}
	if got := surgeMembers(t, base, input, surgeIndex); len(got) != 0 {
		t.Fatalf("replacement members after the refused pass: got %d want 0 (a gang that cannot start whole holds nothing)", len(got))
	}
	if !workload.OperationCapacityRefused(store.statuses[surgeIndex].Operation) {
		t.Fatalf("replacement row Operation: got %+v want the quota refusal recorded", store.statuses[surgeIndex].Operation)
	}
	events := quotaEvents(recorder)
	if len(events) != 1 {
		t.Fatalf("InstanceQuotaBlocked events: got %d want 1", len(events))
	}
	for _, want := range []string{
		fmt.Sprintf("waiting on capacity for all %d members:", members),
		"exceeded quota",
		fmt.Sprintf("the %d members admitted are retired", members-1),
	} {
		if !strings.Contains(events[0], want) {
			t.Errorf("event: got %q want it to carry %q", events[0], want)
		}
	}
}

// TestCreate_GangUnderAQuotaShortOfTheGang_CreatesNoMemberUntilTheRoomExists:
// a gang of a leader and a worker starts for the first time in a namespace
// whose quota admits one pod. The create pass reads the quota before any
// member exists and creates nothing: the row opens its attempt, records the
// refusal and the wait names both members, so a placement race reading the
// Instance's admission sees nothing admitted. Once the quota has room for
// two, one pass creates the leader and the worker together and retires
// nothing.
func TestCreate_GangUnderAQuotaShortOfTheGang_CreatesNoMemberUntilTheRoomExists(t *testing.T) {
	input, _, target, _, base, isvc := createGateFixture(t, time.Now())
	plan := legacyMultiPodComponentPlan(workload.UpdateStrategyRecreatePod)
	members := int(plan.Instances[0].TotalPods())
	quota := &podCountQuota{name: "pod-count", hard: members - 1}
	if err := base.Create(context.Background(), quota.object(isvc.Namespace)); err != nil {
		t.Fatalf("seed the quota: %v", err)
	}
	recorder := record.NewFakeRecorder(16)
	deps := legacyTestDeps(quota.enforce(t, base))
	deps.Recorder = recorder

	if _, err := Create(context.Background(), deps, *input, plan, target); err != nil {
		t.Fatalf("Create: %v (a quota short of the gang is a wait, not an error)", err)
	}
	if got := createGatePods(t, base, isvc.Namespace); len(got) != 0 {
		t.Fatalf("pods after the short pass: got %d want 0 (a gang that cannot start whole holds nothing)", len(got))
	}
	if quota.attempts != 0 {
		t.Errorf("pod creates attempted under a quota short of the gang: got %d want 0", quota.attempts)
	}
	rows := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)
	if len(rows) != 1 || rows[0].Phase != v1beta1.OMENativeInstanceCreating || rows[0].Operation == nil {
		t.Fatalf("rows after the short pass: got %+v want one Creating row with its operation", rows)
	}
	if !workload.OperationCapacityRefused(legacyFromV1beta1Op(rows[0].Operation)) {
		t.Fatalf("row Operation: got %+v want the quota refusal recorded", rows[0].Operation)
	}
	events := quotaEvents(recorder)
	if len(events) != 1 {
		t.Fatalf("InstanceQuotaBlocked events: got %d want 1", len(events))
	}
	want := fmt.Sprintf("waiting on capacity for all %d members: quota %s leaves room for %d pods, the %d members need %d",
		members, quota.name, quota.hard, members, members)
	if !strings.Contains(events[0], want) {
		t.Errorf("event: got %q want it to carry %q", events[0], want)
	}

	// The operator raises the quota by the one pod the gang was short.
	raised := quota.object(isvc.Namespace)
	raised.Status.Hard[corev1.ResourcePods] = *resource.NewQuantity(int64(members), resource.DecimalSI)
	if err := base.Update(context.Background(), raised); err != nil {
		t.Fatalf("raise the quota: %v", err)
	}
	quota.hard = members
	legacyResetExpectations(t)
	next := legacyTestInput(isvc, base, workload.ComponentEngine)
	next.Clock = input.Clock
	if _, err := Create(context.Background(), deps, next, plan, target); err != nil {
		t.Fatalf("Create (room for the gang): %v", err)
	}
	if got := createGatePods(t, base, isvc.Namespace); len(got) != members {
		t.Fatalf("pods once the room exists: got %d want %d on one pass", len(got), members)
	}
	if quota.deletes != 0 {
		t.Errorf("pod deletes: got %d want 0 (nothing was created to retire)", quota.deletes)
	}
	rows = legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)
	if len(rows) != 1 || rows[0].Operation == nil || workload.OperationCapacityRefused(legacyFromV1beta1Op(rows[0].Operation)) {
		t.Errorf("row after every member was created: got %+v want the attempt open with no refusal recorded", rows)
	}
}

// TestSurgeCreate_SinglePod_QuotaRefused_ReadsNoRoomAndRetiresNothing: a
// single-pod Instance keeps the member-by-member create. Its surge reads
// no quota before the create, the refusal is recorded as before and
// nothing is deleted.
func TestSurgeCreate_SinglePod_QuotaRefused_ReadsNoRoomAndRetiresNothing(t *testing.T) {
	legacyResetExpectations(t)
	isvc, ir := surgeISVCReady("llama-70b", "prod", 1)
	base := legacyNewFakeClient(t, isvc, ir)
	tcr := makeCR(t, base, isvc, "llama-70b-engine-rev-abc12345")
	sourcePod := surgePodAtOrdinal(isvc, 0, 1, 0, true, true)
	if err := base.Create(context.Background(), sourcePod); err != nil {
		t.Fatalf("seed source pod: %v", err)
	}
	quota := &podCountQuota{name: "compute-resources", used: 1, hard: 1}
	quotaReads := 0
	c := interceptor.NewClient(quota.enforce(t, base).(client.WithWatch), interceptor.Funcs{
		List: func(ctx context.Context, cl client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if _, isQuota := list.(*corev1.ResourceQuotaList); isQuota {
				quotaReads++
			}
			return cl.List(ctx, list, opts...)
		},
	})
	input := legacyTestInput(isvc, base, workload.ComponentEngine)
	plan := surgePlan()

	done, err := surgeUpdate(context.Background(), legacyTestDeps(c), input, plan, plan.Instances[0], tcr,
		[]*corev1.Pod{sourcePod})
	if err != nil {
		t.Fatalf("surgeUpdate: %v (a quota refusal is a wait, not an error)", err)
	}
	if done {
		t.Fatal("surgeUpdate reported done while blocked on quota")
	}
	if quotaReads != 0 {
		t.Errorf("quota reads before a single pod's create: got %d want 0", quotaReads)
	}
	if quota.attempts != 1 || quota.deletes != 0 {
		t.Errorf("pod creates/deletes: got %d/%d want 1/0", quota.attempts, quota.deletes)
	}
	s := legacyInstanceStatusesOnIR(base, isvc, workload.ComponentEngine)[0]
	if s.Operation == nil || !workload.OperationCapacityRefused(legacyFromV1beta1Op(s.Operation)) {
		t.Fatalf("Operation: got %+v want the quota refusal recorded", s.Operation)
	}
}

func quantity(value string) resource.Quantity { return resource.MustParse(value) }

func podWithRequests(requests corev1.ResourceList) *corev1.Pod {
	return &corev1.Pod{Spec: corev1.PodSpec{Containers: []corev1.Container{{
		Name:      "runner",
		Resources: corev1.ResourceRequirements{Requests: requests, Limits: requests},
	}}}}
}

func quotaWith(name string, hard, used corev1.ResourceList) *corev1.ResourceQuota {
	return &corev1.ResourceQuota{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns"},
		Status:     corev1.ResourceQuotaStatus{Hard: hard, Used: used},
	}
}

// quotaCase is one reading of a quota against the gang of four: the quota,
// and the shortfall it must report, empty when the set has room.
type quotaCase struct {
	name  string
	quota *corev1.ResourceQuota
	short string
}

// scopedQuota is a quota on the pod count that applies only within its scopes.
func scopedQuota(name, pods string, scopes ...corev1.ResourceQuotaScope) *corev1.ResourceQuota {
	q := quotaWith(name, corev1.ResourceList{corev1.ResourcePods: quantity(pods)}, nil)
	q.Spec.Scopes = scopes
	return q
}

// quotaResourceCases read the room by resource name: pod counts, requests
// and limits under the names a quota uses, usage never charged, usage over
// the limit, and limits not yet enforced.
func quotaResourceCases() []quotaCase {
	return []quotaCase{
		{
			name:  "pod count short by one",
			quota: quotaWith("compute", corev1.ResourceList{corev1.ResourcePods: quantity("7")}, corev1.ResourceList{corev1.ResourcePods: quantity("4")}),
			short: "quota compute leaves room for 3 pods, the 4 members need 4",
		},
		{
			name:  "pod count with room for the set",
			quota: quotaWith("compute", corev1.ResourceList{corev1.ResourcePods: quantity("8")}, corev1.ResourceList{corev1.ResourcePods: quantity("4")}),
		},
		{
			name:  "object count key",
			quota: quotaWith("objects", corev1.ResourceList{"count/pods": quantity("5")}, corev1.ResourceList{"count/pods": quantity("4")}),
			short: "quota objects leaves room for 1 count/pods, the 4 members need 4",
		},
		{
			name:  "extended resource requests",
			quota: quotaWith("gpus", corev1.ResourceList{"requests.nvidia.com/gpu": quantity("64")}, corev1.ResourceList{"requests.nvidia.com/gpu": quantity("40")}),
			short: "quota gpus leaves room for 24 requests.nvidia.com/gpu, the 4 members need 32",
		},
		{
			name:  "bare cpu and its limits",
			quota: quotaWith("cpu", corev1.ResourceList{corev1.ResourceCPU: quantity("8"), corev1.ResourceLimitsCPU: quantity("7")}, corev1.ResourceList{corev1.ResourceCPU: quantity("0"), corev1.ResourceLimitsCPU: quantity("0")}),
			short: "quota cpu leaves room for 7 limits.cpu, the 4 members need 8",
		},
		{
			name:  "usage never charged reads as zero",
			quota: quotaWith("fresh", corev1.ResourceList{corev1.ResourcePods: quantity("4")}, nil),
		},
		{
			name:  "over-charged quota leaves no room",
			quota: quotaWith("over", corev1.ResourceList{corev1.ResourcePods: quantity("4")}, corev1.ResourceList{corev1.ResourcePods: quantity("6")}),
			short: "quota over leaves room for 0 pods, the 4 members need 4",
		},
		{
			name:  "limits not yet enforced",
			quota: quotaWith("pending", nil, nil),
		},
	}
}

// quotaScopeCases read the scopes that take a pod out of a quota or leave
// the quota to admission.
func quotaScopeCases() []quotaCase {
	high := quotaWith("high", corev1.ResourceList{corev1.ResourcePods: quantity("0")}, nil)
	high.Spec.ScopeSelector = &corev1.ScopeSelector{MatchExpressions: []corev1.ScopedResourceSelectorRequirement{{
		ScopeName: corev1.ResourceQuotaScopePriorityClass, Operator: corev1.ScopeSelectorOpIn, Values: []string{"high"},
	}}}
	return []quotaCase{
		{
			name:  "best-effort scope leaves a pod with requests out",
			quota: scopedQuota("besteffort", "0", corev1.ResourceQuotaScopeBestEffort),
		},
		{
			name:  "not-best-effort scope counts a pod with requests",
			quota: scopedQuota("burstable", "2", corev1.ResourceQuotaScopeNotBestEffort),
			short: "quota burstable leaves room for 2 pods, the 4 members need 4",
		},
		{
			name:  "priority class selector names another class",
			quota: high,
		},
		{
			name:  "a scope the reading does not evaluate is admission's",
			quota: scopedQuota("vac", "0", corev1.ResourceQuotaScope("VolumeAttributesClass")),
		},
	}
}

// TestQuotaShortOfSet reads one quota against a set of pods the way
// admission charges it: pod counts, requests and limits by name, the room
// the enforced limit leaves, and the scopes that take a pod out of the
// quota or leave the quota to admission.
func TestQuotaShortOfSet(t *testing.T) {
	gpu := corev1.ResourceName("nvidia.com/gpu")
	member := podWithRequests(corev1.ResourceList{corev1.ResourceCPU: quantity("2"), gpu: quantity("8")})
	gang := []*corev1.Pod{member, member, member, member}
	for _, tt := range append(quotaResourceCases(), quotaScopeCases()...) {
		t.Run(tt.name, func(t *testing.T) {
			short := quotaShortOfSet(tt.quota, gang)
			switch {
			case tt.short == "" && short != nil:
				t.Fatalf("got short %q want room for the set", short)
			case tt.short != "" && short == nil:
				t.Fatalf("got room for the set want short %q", tt.short)
			case tt.short != "" && short.String() != tt.short:
				t.Fatalf("got %q want %q", short, tt.short)
			}
		})
	}
}

// TestQuotaRoomForSet_LeavesUnreadableQuotasToAdmission: a manager without
// leave to list quotas creates as before and lets admission answer; any
// other failure to read is the pass's error.
func TestQuotaRoomForSet_LeavesUnreadableQuotasToAdmission(t *testing.T) {
	member := podWithRequests(corev1.ResourceList{corev1.ResourceCPU: quantity("1")})
	refusing := func(err error) client.Reader {
		return interceptor.NewClient(fake.NewClientBuilder().Build(), interceptor.Funcs{
			List: func(context.Context, client.WithWatch, client.ObjectList, ...client.ListOption) error { return err },
		})
	}
	forbidden := apierrors.NewForbidden(schema.GroupResource{Resource: "resourcequotas"}, "", errors.New("no leave to list"))
	short, err := quotaRoomForSet(context.Background(), refusing(forbidden), "ns", []*corev1.Pod{member, member})
	if err != nil || short != nil {
		t.Fatalf("forbidden list: got short=%v err=%v want room left to admission", short, err)
	}
	outage := apierrors.NewServiceUnavailable("apiserver down")
	if _, err := quotaRoomForSet(context.Background(), refusing(outage), "ns", []*corev1.Pod{member, member}); err == nil {
		t.Fatal("apiserver outage: got nil error want the read's failure")
	}
}
