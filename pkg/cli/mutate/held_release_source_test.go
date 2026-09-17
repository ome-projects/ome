package mutate

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/printers"
	"sigs.k8s.io/ome/pkg/constants"
)

type heldTypedListReply struct {
	omeclient.OmeV1beta1Interface
	list *v1beta1.InferenceReplicaList
}

func (r heldTypedListReply) InferenceReplicas(ns string) omeclient.InferenceReplicaInterface {
	return heldIRListReply{InferenceReplicaInterface: r.OmeV1beta1Interface.InferenceReplicas(ns), list: r.list}
}

type heldIRListReply struct {
	omeclient.InferenceReplicaInterface
	list *v1beta1.InferenceReplicaList
}

func (r heldIRListReply) List(context.Context, metav1.ListOptions) (*v1beta1.InferenceReplicaList, error) {
	return r.list, nil
}

func TestHeldReleaseRelationshipAndBoundedIdentityRefusals(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.InferenceReplica)
	}{
		{"wrong kind", func(ir *v1beta1.InferenceReplica) { ir.Kind = "Other" }},
		{"wrong API", func(ir *v1beta1.InferenceReplica) { ir.APIVersion = "other.io/v1" }},
		{"wrong parent", func(ir *v1beta1.InferenceReplica) { ir.Spec.ParentRef.Name = "other" }},
		{"wrong namespace", func(ir *v1beta1.InferenceReplica) { ir.Namespace = "other" }},
		{"missing UID", func(ir *v1beta1.InferenceReplica) { ir.UID = "" }},
		{"unsafe RV", func(ir *v1beta1.InferenceReplica) { ir.ResourceVersion = "PRIVATE\nRV" }},
		{"zero generation", func(ir *v1beta1.InferenceReplica) { ir.Generation = 0 }},
		{"deleting", func(ir *v1beta1.InferenceReplica) { tm := metav1.NewTime(testNow); ir.DeletionTimestamp = &tm }},
		{"owner UID", func(ir *v1beta1.InferenceReplica) { ir.OwnerReferences[0].UID = "old-uid" }},
		{"owner name", func(ir *v1beta1.InferenceReplica) { ir.OwnerReferences[0].Name = "other" }},
		{"owner kind", func(ir *v1beta1.InferenceReplica) { ir.OwnerReferences[0].Kind = "Other" }},
		{"owner API", func(ir *v1beta1.InferenceReplica) { ir.OwnerReferences[0].APIVersion = "other.io/v1" }},
		{"non-controller", func(ir *v1beta1.InferenceReplica) { f := false; ir.OwnerReferences[0].Controller = &f }},
		{"two controllers", func(ir *v1beta1.InferenceReplica) {
			ir.OwnerReferences = append(ir.OwnerReferences, ir.OwnerReferences[0])
		}},
		{"unknown component", func(ir *v1beta1.InferenceReplica) { ir.Spec.Component = "PRIVATE_COMPONENT" }},
		{"placement finalizer", func(ir *v1beta1.InferenceReplica) { ir.Finalizers = []string{"ome.io/placement"} }},
		{"labels bound", func(ir *v1beta1.InferenceReplica) {
			for n := 0; n < 256; n++ {
				ir.Labels[fmt.Sprint(n)] = "x"
			}
		}},
		{"owners bound", func(ir *v1beta1.InferenceReplica) {
			for n := 0; n < 16; n++ {
				ir.OwnerReferences = append(ir.OwnerReferences, metav1.OwnerReference{})
			}
		}},
		{"conditions bound", func(ir *v1beta1.InferenceReplica) { ir.Status.Conditions = make([]metav1.Condition, 65) }},
		{"blocks bound", func(ir *v1beta1.InferenceReplica) { ir.Status.RetryBlocks = make([]v1beta1.RetryBlock, 65) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir := heldReplica(v)
			tc.change(ir)
			err := validateHeldReplicaIdentity(ir, v)
			require.Error(t, err)
			require.NotContains(t, err.Error(), "PRIVATE")
		})
	}
	for _, stamp := range []string{"", "+7", "-7", "07", "6", "8", "9223372036854775808"} {
		v, _ := nativeTarget(t)
		ir := heldReplica(v)
		ir.Annotations[constants.InferenceReplicaParentGenerationAnnotationKey] = stamp
		require.Error(t, validateHeldReplicaIdentity(ir, v))
	}
}

func TestHeldReleaseRefusesInvalidColumnarStatus(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1beta1.InferenceReplica)
	}{
		{name: "unknown encoding", edit: func(ir *v1beta1.InferenceReplica) {
			unknown := v1beta1.InstanceStatusEncoding("PrivateV3")
			ir.Status.InstanceStatusEncoding = &unknown
		}},
		{name: "missing columns", edit: func(ir *v1beta1.InferenceReplica) { ir.Status.InstanceStatusColumns = nil }},
		{name: "mixed dense and columnar", edit: func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
		}},
		{name: "incomplete phase coverage", edit: func(ir *v1beta1.InferenceReplica) { ir.Status.InstanceStatusColumns.Members = "0-1" }},
		{name: "over row limit", edit: func(ir *v1beta1.InferenceReplica) {
			ir.Status.InstanceStatusColumns.Members = "0-2048"
			ir.Status.InstanceStatusColumns.Phases[0].Indexes = "0-2048"
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			parent, _ := nativeTarget(t)
			ir := heldReplica(parent)
			ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
			storeColumnarReplica(t, ir)
			tc.edit(ir)
			before := ir.DeepCopy()
			evidence, err := CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), parent, "engine", testClock)
			require.Error(t, err)
			require.Empty(t, evidence.items)
			require.NotContains(t, err.Error(), "PrivateV3")
			require.Equal(t, before, ir)
		})
	}
}

func TestHeldReleaseKeepsValidColumnarRawEvidence(t *testing.T) {
	parent, state := nativeTarget(t)
	ir := heldReplica(parent)
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 0, Phase: v1beta1.OMENativeInstanceReady}}
	storeColumnarReplica(t, ir)
	stored := ir.DeepCopy()
	evidence, err := CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), parent, "engine", testClock)
	require.NoError(t, err)
	require.Len(t, evidence.items, 1)
	require.Equal(t, stored.Status, evidence.items[0].Status)
	plan, err := PrepareHeldRelease(parent, state, evidence, "engine", "aaaaaaaa", testClock)
	require.NoError(t, err)
	require.Equal(t, "81", plan.Target().ResourceVersion)
	require.Equal(t, stored, ir)
}

func TestHeldReleaseCollectNeverAcceptsUncheckedTailOrContinuation(t *testing.T) {
	for _, scenario := range []string{"two pages", "duplicate tail", "duplicate UID", "mismatched selector", "oversized page", "truncated", "loop token", "wrong list GVK", "nil list", "private list error", "private exact error", "exact changed", "aggregate bytes"} {
		t.Run(scenario, func(t *testing.T) {
			v, state := nativeTarget(t)
			ir := heldReplica(v)
			client := omefake.NewSimpleClientset(ir)
			pages := 0
			client.PrependReactor("list", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
				pages++
				opts := action.(ktesting.ListAction).GetListRestrictions()
				require.Equal(t, "ome.io/inferenceservice=chat", opts.Labels.String())
				list := &v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir}}
				switch scenario {
				case "two pages":
					if pages == 1 {
						list.Continue = "next"
					} else {
						list.Items = nil
					}
				case "duplicate tail":
					if pages == 1 {
						list.Continue = "next"
					} else {
						other := ir.DeepCopy()
						other.Name = "second-engine"
						other.UID = "second-uid"
						list.Items = []v1beta1.InferenceReplica{*other}
					}
				case "duplicate UID":
					other := ir.DeepCopy()
					other.Name = "decoder"
					other.Spec.Component = v1beta1.DecoderComponent
					list.Items = append(list.Items, *other)
				case "mismatched selector":
					list.Items[0] = *ir.DeepCopy()
					list.Items[0].Labels[constants.InferenceServiceLabel] = "other"
				case "oversized page":
					list.Items = make([]v1beta1.InferenceReplica, 17)
				case "truncated":
					list.Continue = fmt.Sprint(pages)
				case "loop token":
					list.Continue = "next"
				case "wrong list GVK":
					list.Kind = "Other"
				case "nil list":
					return true, nil, nil
				case "private list error":
					return true, nil, errors.New("PRIVATE_UPSTREAM")
				case "aggregate bytes":
					other := ir.DeepCopy()
					other.Name = "decoder"
					other.UID = "uid-decoder"
					other.Spec.Component = v1beta1.DecoderComponent
					list.Items[0] = *ir.DeepCopy()
					list.Items[0].Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{NodesOccupied: []string{strings.Repeat("x", 550000)}}}
					other.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{NodesOccupied: []string{strings.Repeat("x", 550000)}}}
					list.Items = append(list.Items, *other)
				}
				return true, list, nil
			})
			if scenario == "private exact error" || scenario == "exact changed" {
				client.PrependReactor("get", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
					if scenario == "private exact error" {
						return true, nil, errors.New("PRIVATE_GET")
					}
					copy := ir.DeepCopy()
					copy.ResourceVersion = "82"
					return true, copy, nil
				})
			}
			var typed omeclient.OmeV1beta1Interface = client.OmeV1beta1()
			if scenario == "wrong list GVK" {
				typed = heldTypedListReply{OmeV1beta1Interface: typed, list: &v1beta1.InferenceReplicaList{TypeMeta: metav1.TypeMeta{Kind: "Other"}, Items: []v1beta1.InferenceReplica{*ir}}}
			}
			evidence, err := CollectHeldReleaseEvidence(context.Background(), typed, v, "engine", testClock)
			if scenario == "two pages" {
				require.NoError(t, err)
				_, err = PrepareHeldRelease(v, state, evidence, "engine", "aaaaaaaa", testClock)
				require.NoError(t, err)
				require.Equal(t, 2, evidence.pages)
			} else {
				require.Error(t, err)
				require.NotContains(t, err.Error(), "PRIVATE")
			}
			require.LessOrEqual(t, pages, 2)
		})
	}
}

func TestHeldReleaseLongParentScanAndDefensiveCopies(t *testing.T) {
	v, state := nativeTarget(t)
	name := strings.Repeat("a", 64)
	v.Name = name
	// A fresh state is required for changed parent identity, so exercise source
	// admission and wrapping here without pretending the old state is bound.
	ir := heldReplica(v)
	delete(ir.Labels, constants.InferenceServiceLabel)
	ir.Status.RetryBlocks[0].TargetRevision = name + "-engine-aaaaaaaa"
	unrelated := &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{Name: "unrelated", Namespace: v.Namespace}}
	client := omefake.NewSimpleClientset(ir, unrelated)
	evidence, err := CollectHeldReleaseEvidence(context.Background(), client.OmeV1beta1(), v, "engine", testClock)
	require.NoError(t, err)
	require.Len(t, evidence.items, 1)
	ir.Annotations[constants.InferenceReplicaControllerWriteAnnotationKey] = "false"
	v.ResourceVersion = "changed"
	require.Equal(t, "true", evidence.items[0].Annotations[constants.InferenceReplicaControllerWriteAnnotationKey])
	require.Equal(t, "42", evidence.parent.ResourceVersion)
	_, err = PrepareHeldRelease(v, state, evidence, "engine", "aaaaaaaa", testClock)
	require.Error(t, err)

	v, _ = nativeTarget(t)
	ir = heldReplica(v)
	v.UID = types.UID(strings.Repeat("u", 256))
	ir.OwnerReferences[0].UID = v.UID
	evidence, err = CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), v, "engine", testClock)
	require.NoError(t, err)
	// Use the original bound UID for planning; wrap a long saved context instead.
	v, state = nativeTarget(t)
	evidence, err = CollectHeldReleaseEvidence(context.Background(), omefake.NewSimpleClientset(heldReplica(v)).OmeV1beta1(), v, "engine", testClock)
	require.NoError(t, err)
	plan, err := PrepareHeldRelease(v, state, evidence, "engine", "aaaaaaaa", testClock)
	require.NoError(t, err)
	var preview bytes.Buffer
	require.NoError(t, plan.WritePreview(&preview, strings.Repeat("c", 256), "ome", "client"))
	for _, line := range strings.Split(preview.String(), "\n") {
		require.LessOrEqual(t, printers.CellDisplayWidth(line), 80)
	}
	require.Equal(t, "<HeldReleasePlan redacted>", fmt.Sprintf("%#v", plan))
	require.Equal(t, "<HeldReleaseEvidence redacted>", fmt.Sprintf("%v", evidence))
}

func TestHeldReleaseChecksContextAfterIgnoringTypedClient(t *testing.T) {
	for _, operation := range []string{"list", "get"} {
		v, _ := nativeTarget(t)
		ir := heldReplica(v)
		client := omefake.NewSimpleClientset(ir)
		ctx, cancel := context.WithCancel(context.Background())
		client.PrependReactor(operation, "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
			cancel()
			if operation == "list" {
				return true, &v1beta1.InferenceReplicaList{Items: []v1beta1.InferenceReplica{*ir}}, nil
			}
			return true, ir, nil
		})
		_, err := CollectHeldReleaseEvidence(ctx, client.OmeV1beta1(), v, "engine", testClock, time.Second)
		cancel()
		require.Error(t, err)
	}
}

func TestHeldReleaseWholeRetryBlockValidationAndRefresh(t *testing.T) {
	for _, tc := range []struct {
		name   string
		change func(*v1beta1.RetryBlock)
	}{
		{"cross component", func(b *v1beta1.RetryBlock) { b.TargetRevision = "chat-decoder-bbbbbbbb" }},
		{"wrong hash", func(b *v1beta1.RetryBlock) { b.TargetRevision = "chat-engine-BBBBBBBB" }},
		{"short scope", func(b *v1beta1.RetryBlock) { b.TargetRevision = "bbbbbbbb" }},
		{"negative attempts", func(b *v1beta1.RetryBlock) { b.AttemptsStarted = -1 }},
		{"zero failure", func(b *v1beta1.RetryBlock) { b.FirstFailureAt = &metav1.Time{} }},
		{"failure order", func(b *v1beta1.RetryBlock) {
			first, last := metav1.NewTime(testNow.Add(-time.Second)), metav1.NewTime(testNow.Add(-2*time.Second))
			b.FirstFailureAt, b.LastFailureAt = &first, &last
		}},
		{"backoff no deadline", func(b *v1beta1.RetryBlock) { b.State = v1beta1.RetryBlockBackoff }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir := heldReplica(v)
			b := v1beta1.RetryBlock{TargetRevision: "chat-engine-bbbbbbbb", State: v1beta1.RetryBlockHeld, AttemptsStarted: 1}
			tc.change(&b)
			ir.Status.RetryBlocks = append(ir.Status.RetryBlocks, b)
			require.Error(t, validateHeldBlocks(ir, v, testNow))
		})
	}
	v, state := nativeTarget(t)
	ir := heldReplica(v)
	client := omefake.NewSimpleClientset(ir)
	evidence, err := CollectHeldReleaseEvidence(context.Background(), client.OmeV1beta1(), v, "engine", testClock)
	require.NoError(t, err)
	plan, err := PrepareHeldRelease(v, state, evidence, "engine", "aaaaaaaa", testClock)
	require.NoError(t, err)
	for _, observed := range []int64{0, 6, 7, 8} {
		fresh := v.DeepCopy()
		fresh.Status.ObservedGeneration = observed
		require.True(t, plan.MatchesRefresh(fresh, state, evidence))
	}
	for _, scenario := range []string{"sibling added", "sibling changed", "sibling removed"} {
		fresh := HeldReleaseEvidence{parent: evidence.parent.DeepCopy(), items: append([]v1beta1.InferenceReplica{}, evidence.items...), selected: evidence.selected}
		other := ir.DeepCopy()
		other.Name = "decoder"
		other.UID = "uid-decoder"
		other.Spec.Component = v1beta1.DecoderComponent
		switch scenario {
		case "sibling added":
			fresh.items = append(fresh.items, *other)
		case "sibling changed":
			fresh.items[0].ResourceVersion = "83"
		case "sibling removed":
			fresh.items = nil
		}
		require.False(t, plan.MatchesRefresh(v, state, fresh))
	}
	for _, revision := range []string{"bbbbbbbb", "chat-decoder-aaaaaaaa", "aaaaaaa", "aaaaaaaaa"} {
		_, err = PrepareHeldRelease(v, state, evidence, "engine", revision, testClock)
		require.Error(t, err)
	}
}
