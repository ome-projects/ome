package status

import (
	"context"
	"errors"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/types"
)

func TestFinalizeAndRemoveInstance_StrongOrderingAndGuards(t *testing.T) {
	const namespace, owner = "prod", "model"
	index := int32(3)
	expected := terminalStatusFixture(index)
	tests := []struct {
		name              string
		prepare           func(*terminalMutationStore, *types.InstanceStatus)
		afterFinalize     func(*terminalMutationStore)
		finalizeErr       error
		finalizePending   bool
		wantComplete      bool
		wantErr           bool
		wantStatusPresent bool
		wantFinalizes     int
		wantWrites        int
		wantForgotten     bool
	}{
		{
			name:              "exact owner finalizes before committed removal",
			wantComplete:      true,
			wantStatusPresent: false,
			wantFinalizes:     1,
			wantWrites:        1,
			wantForgotten:     true,
		},
		{
			name:              "resource finalization failure retains row",
			finalizeErr:       errors.New("podgroup delete failed"),
			wantErr:           true,
			wantStatusPresent: true,
			wantFinalizes:     1,
		},
		{
			name:              "resource deletion in progress retains row",
			finalizePending:   true,
			wantStatusPresent: true,
			wantFinalizes:     1,
		},
		{
			name: "lifecycle drift during finalization aborts removal",
			afterFinalize: func(store *terminalMutationStore) {
				row := store.statuses[index]
				row.Operation.ID = "new-owner"
				store.statuses[index] = row
			},
			wantStatusPresent: true,
			wantFinalizes:     1,
		},
		{
			name: "owner replacement during finalization aborts removal",
			afterFinalize: func(store *terminalMutationStore) {
				store.ownerUID = "owner-b"
			},
			wantStatusPresent: true,
			wantFinalizes:     1,
		},
		{
			name: "concurrent row removal is idempotent",
			afterFinalize: func(store *terminalMutationStore) {
				delete(store.statuses, index)
			},
			wantComplete:      true,
			wantStatusPresent: false,
			wantFinalizes:     1,
			wantForgotten:     true,
		},
		{
			name: "changed operation aborts guarded removal",
			prepare: func(store *terminalMutationStore, _ *types.InstanceStatus) {
				row := store.statuses[index]
				row.Operation.ID = "new-owner"
				store.statuses[index] = row
			},
			wantStatusPresent: true,
		},
		{
			name: "row write failure retains row and expectations",
			prepare: func(store *terminalMutationStore, _ *types.InstanceStatus) {
				store.applyErr = errors.New("row write failed")
			},
			wantErr:           true,
			wantStatusPresent: true,
			wantFinalizes:     1,
		},
		{
			name: "owner recreation aborts guarded removal",
			prepare: func(store *terminalMutationStore, _ *types.InstanceStatus) {
				store.ownerUID = "owner-b"
			},
			wantStatusPresent: true,
		},
		{
			name: "owner disappearance stops lifecycle tail",
			prepare: func(store *terminalMutationStore, _ *types.InstanceStatus) {
				store.readErr = types.ErrStatusOwnerGone
			},
			wantStatusPresent: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := []string{}
			store := &terminalMutationStore{
				ownerUID: "owner-a",
				statuses: map[int32]types.InstanceStatus{index: cloneTerminalStatus(expected)},
				log:      &log,
			}
			expectations := types.NewExpectations()
			expectations.ExpectDeletes(namespace, owner, types.ComponentEngine, index, 1)
			finalizes := 0
			input := types.ReconcileInput{
				OwnerObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
				Key:         types.Key{Namespace: namespace, OwnerName: owner, Component: types.ComponentEngine},
				FinalizeInstanceResources: func(context.Context, int32) (bool, error) {
					finalizes++
					log = append(log, "finalize")
					if test.afterFinalize != nil {
						test.afterFinalize(store)
					}
					return test.finalizeErr == nil && !test.finalizePending, test.finalizeErr
				},
				ApplyInstanceMutationsWithRetryBlock: store.apply,
			}
			if test.prepare != nil {
				test.prepare(store, &expected)
			}

			complete, err := FinalizeAndRemove(context.Background(), types.Deps{Expectations: expectations}, input, index, &expected)
			if (err != nil) != test.wantErr || complete != test.wantComplete {
				t.Fatalf("result: complete=%v err=%v", complete, err)
			}
			_, present := store.statuses[index]
			if present != test.wantStatusPresent || finalizes != test.wantFinalizes || store.writes != test.wantWrites {
				t.Fatalf("state: present=%v finalizes=%d writes=%d log=%v", present, finalizes, store.writes, log)
			}
			forgotten := expectations.Satisfied(namespace, owner, types.ComponentEngine, index)
			if forgotten != test.wantForgotten {
				t.Fatalf("expectations forgotten=%v want %v", forgotten, test.wantForgotten)
			}
			if finalizes > 0 && (len(log) < 2 || log[0] != "status" || log[1] != "finalize") {
				t.Fatalf("effect order=%v want status preflight before finalizer", log)
			}
			if store.applyCall > 1 && (len(log) < 3 || log[2] != "status") {
				t.Fatalf("effect order=%v want status removal after finalizer", log)
			}
		})
	}
}

func TestFinalizeAndRemoveInstance_RetriesAfterStatusFailure(t *testing.T) {
	expected := terminalStatusFixture(3)
	store := &terminalMutationStore{
		ownerUID: "owner-a",
		statuses: map[int32]types.InstanceStatus{3: cloneTerminalStatus(expected)},
		applyErr: errors.New("transient row failure"),
	}
	finalizes := 0
	input := types.ReconcileInput{
		OwnerObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		Key:         types.Key{Namespace: "prod", OwnerName: "model", Component: types.ComponentEngine},
		FinalizeInstanceResources: func(context.Context, int32) (bool, error) {
			finalizes++
			return true, nil
		},
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}

	complete, err := FinalizeAndRemove(context.Background(), types.Deps{}, input, 3, &expected)
	if err == nil || complete {
		t.Fatalf("first attempt: complete=%v err=%v", complete, err)
	}
	store.applyErr = nil
	complete, err = FinalizeAndRemove(context.Background(), types.Deps{}, input, 3, &expected)
	if err != nil || !complete {
		t.Fatalf("retry: complete=%v err=%v", complete, err)
	}
	if finalizes != 2 || store.writes != 1 {
		t.Fatalf("retry state: finalizes=%d writes=%d", finalizes, store.writes)
	}
}

func TestFinalizeAndRemoveInstance_ConfirmedAbsenceIsIdempotent(t *testing.T) {
	const namespace, owner = "prod", "model"
	index := int32(3)
	expected := terminalStatusFixture(index)
	store := &terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{}}
	expectations := types.NewExpectations()
	expectations.ExpectDeletes(namespace, owner, types.ComponentEngine, index, 1)
	finalizes := 0
	input := types.ReconcileInput{
		OwnerObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
		Key:         types.Key{Namespace: namespace, OwnerName: owner, Component: types.ComponentEngine},
		FinalizeInstanceResources: func(context.Context, int32) (bool, error) {
			finalizes++
			return true, nil
		},
		ApplyInstanceMutationsWithRetryBlock: store.apply,
	}

	complete, err := FinalizeAndRemove(context.Background(), types.Deps{Expectations: expectations}, input, index, &expected)
	if err != nil || !complete {
		t.Fatalf("confirmed absence: complete=%v err=%v", complete, err)
	}
	if finalizes != 0 || store.writes != 0 || store.applyCall != 1 {
		t.Fatalf("idempotent absence: finalizes=%d writes=%d applyCalls=%d", finalizes, store.writes, store.applyCall)
	}
	if !expectations.Satisfied(namespace, owner, types.ComponentEngine, index) {
		t.Fatal("authoritatively absent row did not forget expectations")
	}
}

func TestFinalizeAndRemoveInstance_AbsentIdentityGuardsResidualFinalization(t *testing.T) {
	const namespace, owner = "prod", "model"
	index := int32(3)
	tests := []struct {
		name          string
		prepare       func(*terminalMutationStore)
		finalizeErr   error
		wantComplete  bool
		wantErr       bool
		wantFinalizes int
		wantApply     int
		wantForgotten bool
	}{
		{
			name:          "authoritative absence finalizes residual resources",
			wantComplete:  true,
			wantFinalizes: 1,
			wantApply:     2,
			wantForgotten: true,
		},
		{
			name: "live row blocks finalization",
			prepare: func(store *terminalMutationStore) {
				store.statuses[index] = terminalStatusFixture(index)
			},
			wantApply: 1,
		},
		{
			name: "owner recreation blocks finalization",
			prepare: func(store *terminalMutationStore) {
				store.ownerUID = "owner-b"
			},
			wantApply: 1,
		},
		{
			name:          "finalization failure remains retryable",
			finalizeErr:   errors.New("podgroup delete failed"),
			wantErr:       true,
			wantFinalizes: 1,
			wantApply:     1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			log := []string{}
			store := &terminalMutationStore{
				ownerUID: "owner-a",
				statuses: map[int32]types.InstanceStatus{},
				log:      &log,
			}
			if test.prepare != nil {
				test.prepare(store)
			}
			expectations := types.NewExpectations()
			expectations.ExpectDeletes(namespace, owner, types.ComponentEngine, index, 1)
			finalizes := 0
			input := types.ReconcileInput{
				OwnerObject: &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
				Key:         types.Key{Namespace: namespace, OwnerName: owner, Component: types.ComponentEngine},
				FinalizeInstanceResources: func(context.Context, int32) (bool, error) {
					finalizes++
					log = append(log, "finalize")
					return test.finalizeErr == nil, test.finalizeErr
				},
				ApplyInstanceMutationsWithRetryBlock: store.apply,
			}

			complete, err := FinalizeAndRemove(
				context.Background(),
				types.Deps{Expectations: expectations},
				input,
				index,
				nil,
			)
			if complete != test.wantComplete || (err != nil) != test.wantErr {
				t.Fatalf("result: complete=%v err=%v", complete, err)
			}
			if finalizes != test.wantFinalizes || store.applyCall != test.wantApply || store.writes != 0 {
				t.Fatalf("effects: finalizes=%d applyCalls=%d writes=%d log=%v", finalizes, store.applyCall, store.writes, log)
			}
			forgotten := expectations.Satisfied(namespace, owner, types.ComponentEngine, index)
			if forgotten != test.wantForgotten {
				t.Fatalf("expectations forgotten=%v want %v", forgotten, test.wantForgotten)
			}
			if finalizes > 0 && (len(log) < 2 || log[0] != "status" || log[1] != "finalize") {
				t.Fatalf("effect order=%v want authoritative status read before finalization", log)
			}
		})
	}
}

func TestFinalizeAndRemoveInstance_LegacyRemovalWithoutPerInstanceResources(t *testing.T) {
	const namespace, owner = "prod", "model"
	index := int32(3)
	expected := terminalStatusFixture(index)
	tests := []struct {
		name          string
		removed       bool
		removeErr     error
		configure     bool
		wantComplete  bool
		wantErr       bool
		wantForgotten bool
		wantCalls     int
	}{
		{
			name:          "committed removal completes",
			removed:       true,
			configure:     true,
			wantComplete:  true,
			wantForgotten: true,
			wantCalls:     1,
		},
		{
			name:          "already absent completes",
			configure:     true,
			wantComplete:  true,
			wantForgotten: true,
			wantCalls:     1,
		},
		{
			name:      "removal failure retries",
			removeErr: errors.New("row update failed"),
			configure: true,
			wantErr:   true,
			wantCalls: 1,
		},
		{
			name:    "missing removal adapter fails",
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expectations := types.NewExpectations()
			expectations.ExpectDeletes(namespace, owner, types.ComponentEngine, index, 1)
			calls := 0
			input := types.ReconcileInput{
				Key: types.Key{Namespace: namespace, OwnerName: owner, Component: types.ComponentEngine},
			}
			if test.configure {
				input.RemoveInstance = func(context.Context, int32) (bool, error) {
					calls++
					return test.removed, test.removeErr
				}
			}

			complete, err := FinalizeAndRemove(
				context.Background(),
				types.Deps{Expectations: expectations},
				input,
				index,
				&expected,
			)
			if complete != test.wantComplete || (err != nil) != test.wantErr {
				t.Fatalf("result: complete=%v err=%v", complete, err)
			}
			if calls != test.wantCalls {
				t.Fatalf("RemoveInstance calls=%d want %d", calls, test.wantCalls)
			}
			forgotten := expectations.Satisfied(namespace, owner, types.ComponentEngine, index)
			if forgotten != test.wantForgotten {
				t.Fatalf("expectations forgotten=%v want %v", forgotten, test.wantForgotten)
			}
		})
	}
}

func TestTerminalLifecycle_FailsClosedWithoutStrongOwnerIdentity(t *testing.T) {
	expected := terminalStatusFixture(3)
	tests := []struct {
		name  string
		input types.ReconcileInput
	}{
		{
			name: "missing owner",
			input: types.ReconcileInput{
				ApplyInstanceMutationsWithRetryBlock: (&terminalMutationStore{ownerUID: "owner-a", statuses: map[int32]types.InstanceStatus{3: expected}}).apply,
			},
		},
		{
			name: "empty owner UID",
			input: types.ReconcileInput{
				OwnerObject:                          &corev1.ConfigMap{},
				ApplyInstanceMutationsWithRetryBlock: (&terminalMutationStore{statuses: map[int32]types.InstanceStatus{3: expected}}).apply,
			},
		},
		{
			name: "missing strong adapter",
			input: types.ReconcileInput{
				OwnerObject:               &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{UID: "owner-a"}},
				ApplyInstanceMutations:    func(context.Context, []types.InstanceMutation) error { return nil },
				FinalizeInstanceResources: func(context.Context, int32) (bool, error) { return true, nil },
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := StampStep(context.Background(), test.input, &expected, types.UpdateStepSurgeDrain, true); err == nil {
				t.Fatal("terminal marker accepted an unverifiable owner or adapter")
			}
			if test.input.FinalizeInstanceResources == nil {
				test.input.FinalizeInstanceResources = func(context.Context, int32) (bool, error) { return true, nil }
			}
			if _, err := FinalizeAndRemove(context.Background(), types.Deps{}, test.input, 3, &expected); err == nil {
				t.Fatal("terminal finalization accepted an unverifiable owner or adapter")
			}
		})
	}
}
