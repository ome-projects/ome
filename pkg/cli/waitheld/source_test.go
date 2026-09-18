package waitheld

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	ktesting "k8s.io/client-go/testing"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	omeclient "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/constants"
)

type fakeReplicaReader struct{ client omeclient.OmeV1beta1Interface }

func (r fakeReplicaReader) GetInferenceReplica(ctx context.Context, namespace, name string, options metav1.GetOptions) (*v1beta1.InferenceReplica, error) {
	return r.client.InferenceReplicas(namespace).Get(ctx, name, options)
}

func newTestSource(client omeclient.OmeV1beta1Interface, target Target) (*Source, error) {
	return NewSource(client, fakeReplicaReader{client: client}, target)
}

func TestSourceExactNameGETMatchesWithManyUnrelatedIRsAndNoLIST(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks = nil
	objects := []runtime.Object{evidence.Parent, evidence.Replica}
	for i := 0; i < 40; i++ {
		objects = append(objects, &v1beta1.InferenceReplica{ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("unrelated-%02d", i), Namespace: target.Namespace,
			UID: types.UID(fmt.Sprintf("unrelated-uid-%02d", i)),
		}})
	}
	client := omefake.NewSimpleClientset(objects...)
	client.PrependReactor("list", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		t.Fatal("exact target read must not LIST sibling IRs")
		return true, nil, nil
	})
	gets := 0
	client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		gets++
		if action.(ktesting.GetAction).GetName() != target.IRName {
			t.Fatalf("wrong exact IR name: %q", action.(ktesting.GetAction).GetName())
		}
		return false, nil, nil
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || gets != 1 || !snapshot.Value.Matched ||
		snapshot.UID != evidence.Parent.UID || snapshot.ResourceVersion != evidence.Parent.ResourceVersion {
		t.Fatalf("exact current target should match without sibling LIST: %+v, gets=%d, err=%v", snapshot, gets, err)
	}
}

func TestSourceAcceptsActionResultNoncanonicalIRName(t *testing.T) {
	target, evidence := heldFixture()
	target.IRName = "custom-engine"
	evidence.Replica.Name = target.IRName
	evidence.Replica.Status.RetryBlocks = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	gets := 0
	client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		gets++
		if got := action.(ktesting.GetAction).GetName(); got != target.IRName {
			t.Fatalf("wait must GET the action's exact IR name, got %q", got)
		}
		return false, nil, nil
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || gets != 1 || !snapshot.Value.Matched || snapshot.Value.InferenceReplica != target.IRName {
		t.Fatalf("same-UID noncanonical action target must match through exact GET: %+v, gets=%d, err=%v", snapshot, gets, err)
	}
}

func TestSourceNoncanonicalActionTargetStillRejectsReplacementUID(t *testing.T) {
	target, evidence := heldFixture()
	target.IRName = "custom-engine"
	evidence.Replica.Name = target.IRName
	evidence.Replica.Status.RetryBlocks = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
		if got := action.(ktesting.GetAction).GetName(); got != target.IRName {
			t.Fatalf("wait must GET the action's exact IR name, got %q", got)
		}
		replacement := evidence.Replica.DeepCopy()
		replacement.UID = "replacement"
		return true, replacement, nil
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || snapshot.Value.Matched || snapshot.Value.Reason != ReasonReplicaReplaced {
		t.Fatalf("same-name replacement cannot inherit released state: %+v, %v", snapshot, err)
	}
}

func TestSourceRedactsCredentialShapedNoncanonicalIRName(t *testing.T) {
	target, evidence := heldFixture()
	secret := "sk-proj-0123456789abcdefghijklmnopqrst"
	target.IRName = secret
	evidence.Replica.Name = secret
	evidence.Replica.Status.RetryBlocks = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || !snapshot.Value.Matched {
		t.Fatalf("credential-shaped exact name remains operable: %+v, %v", snapshot, err)
	}
	raw, err := json.Marshal(snapshot.Value)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), secret) || strings.Contains(fmt.Sprintf("%+v", snapshot.Value), secret) {
		t.Fatalf("exported observation leaked credential-shaped exact name: %s", raw)
	}
}

func TestSourceOtherParentClaimantDoesNotVetoExactTarget(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks = nil
	hidden := evidence.Replica.DeepCopy()
	hidden.Name, hidden.UID, hidden.ResourceVersion = "hidden-engine", "hidden-uid", "32"
	hidden.Labels[constants.InferenceServiceLabel] = "other"
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica, hidden)
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || !snapshot.Value.Matched {
		t.Fatalf("noncanonical claimant is not the action's exact name+UID target: %+v, %v", snapshot, err)
	}
}

func TestSourceExactUIDReplacementCannotMatchEmptyBlocks(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	client.PrependReactor("get", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		replacement := evidence.Replica.DeepCopy()
		replacement.UID = "replacement"
		return true, replacement, nil
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || snapshot.Value.Matched || snapshot.Value.Reason != ReasonReplicaReplaced {
		t.Fatalf("replacement IR cannot inherit unheld observation: %+v, %v", snapshot, err)
	}
}

func TestSourceChildNotFoundIsUnmetNotParentNotFound(t *testing.T) {
	target, evidence := heldFixture()
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	client.PrependReactor("get", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "inferencereplicas"}, target.IRName)
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || snapshot.Value.Matched || snapshot.Value.Reason != ReasonReplicaMissing ||
		snapshot.UID != evidence.Parent.UID {
		t.Fatalf("child 404 must be typed unmet parent snapshot: %+v, %v", snapshot, err)
	}
}

func TestSourceParentRefreshDeletionIsPromptlyTerminal(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	reads := 0
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		reads++
		parent := evidence.Parent.DeepCopy()
		if reads == 2 {
			now := metav1.Now()
			parent.DeletionTimestamp = &now
			parent.ResourceVersion = "18"
		}
		return true, parent, nil
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || reads != 2 || !snapshot.Deleting || snapshot.Value.Matched ||
		snapshot.Value.Reason != ReasonDeleting {
		t.Fatalf("refreshed parent deletion should be terminal now: %+v, reads=%d, err=%v", snapshot, reads, err)
	}
}

func TestSourceParentRefreshGenerationChangeStaysPartial(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	reads := 0
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		reads++
		parent := evidence.Parent.DeepCopy()
		if reads == 2 {
			parent.ResourceVersion, parent.Generation = "18", 8
		}
		return true, parent, nil
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || snapshot.Value.Matched || snapshot.Value.Reason != ReasonSourceIncomplete {
		t.Fatalf("parent changed during scan must be partial: %+v, %v", snapshot, err)
	}
}

func TestSourceParentRefreshNotFoundIsDeletingNotUnheld(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	reads := 0
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		reads++
		if reads == 2 {
			return true, nil, apierrors.NewNotFound(schema.GroupResource{Group: "ome.io", Resource: "inferenceservices"}, target.ParentName)
		}
		return true, evidence.Parent.DeepCopy(), nil
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || reads != 2 || !snapshot.Deleting || snapshot.Value.Matched ||
		snapshot.Value.Reason != ReasonDeleting || snapshot.UID != evidence.Parent.UID {
		t.Fatalf("parent lost during acquisition must terminate without an unheld match: %+v, reads=%d, err=%v", snapshot, reads, err)
	}
}

func TestSourceParentRefreshWrongUIDStaysPartial(t *testing.T) {
	target, evidence := heldFixture()
	evidence.Replica.Status.RetryBlocks = nil
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	reads := 0
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		reads++
		parent := evidence.Parent.DeepCopy()
		if reads == 2 {
			parent.UID = "replacement-parent-uid"
		}
		return true, parent, nil
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := source.Get(context.Background())
	if err != nil || reads != 2 || snapshot.Deleting || snapshot.Value.Matched ||
		snapshot.Value.Reason != ReasonSourceIncomplete || snapshot.Value.Validity != ValidityPartial ||
		snapshot.UID != evidence.Parent.UID {
		t.Fatalf("different parent UID cannot promote old-generation child evidence: %+v, reads=%d, err=%v", snapshot, reads, err)
	}
}

func TestSourceRejectsUnsafeExactParentResponses(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1beta1.InferenceService) *v1beta1.InferenceService
	}{
		{"nil", func(*v1beta1.InferenceService) *v1beta1.InferenceService { return nil }},
		{"wrong namespace", func(parent *v1beta1.InferenceService) *v1beta1.InferenceService {
			parent.Namespace = "other"
			return parent
		}},
		{"oversized private payload", func(parent *v1beta1.InferenceService) *v1beta1.InferenceService {
			parent.Annotations = map[string]string{"private": strings.Repeat("x", 1<<20)}
			return parent
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, evidence := heldFixture()
			client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
			client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
				return true, tc.edit(evidence.Parent.DeepCopy()), nil
			})
			source, err := newTestSource(client.OmeV1beta1(), target)
			if err != nil {
				t.Fatal(err)
			}
			_, err = source.Get(context.Background())
			if !errors.Is(err, ErrUnsafeResponse) {
				t.Fatalf("unsafe exact parent response must fail closed, got %v", err)
			}
		})
	}
}

func TestSourceRejectsUnsafeExactReplicaResponses(t *testing.T) {
	for _, tc := range []struct {
		name       string
		edit       func(*v1beta1.InferenceReplica) *v1beta1.InferenceReplica
		wantErr    error
		wantReason Reason
	}{
		{"nil", func(*v1beta1.InferenceReplica) *v1beta1.InferenceReplica { return nil }, ErrUnsafeResponse, ""},
		{"oversized private payload", func(ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
			ir.Annotations["private"] = strings.Repeat("x", 1<<20)
			return ir
		}, ErrUnsafeResponse, ""},
		{"wrong exact name", func(ir *v1beta1.InferenceReplica) *v1beta1.InferenceReplica {
			ir.Name = "other-engine"
			return ir
		}, nil, ReasonSourceInvalid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target, evidence := heldFixture()
			evidence.Replica.Status.RetryBlocks = nil
			client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
			client.PrependReactor("get", "inferencereplicas", func(action ktesting.Action) (bool, runtime.Object, error) {
				if got := action.(ktesting.GetAction).GetName(); got != target.IRName {
					t.Fatalf("wrong exact IR requested: %q", got)
				}
				return true, tc.edit(evidence.Replica.DeepCopy()), nil
			})
			source, err := newTestSource(client.OmeV1beta1(), target)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := source.Get(context.Background())
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) || snapshot.Value.Matched {
					t.Fatalf("unsafe exact IR response must fail closed: %+v, %v", snapshot, err)
				}
				return
			}
			if err != nil || snapshot.Value.Matched || snapshot.Value.Reason != tc.wantReason {
				t.Fatalf("malformed exact IR response cannot match: %+v, %v", snapshot, err)
			}
		})
	}
}

func TestSourceCancellationDuringExactReads(t *testing.T) {
	for _, phase := range []string{"before first read", "during replica read", "during parent refresh"} {
		t.Run(phase, func(t *testing.T) {
			target, evidence := heldFixture()
			evidence.Replica.Status.RetryBlocks = nil
			client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if phase == "before first read" {
				cancel()
			}
			if phase == "during replica read" {
				client.PrependReactor("get", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
					cancel()
					return true, evidence.Replica.DeepCopy(), nil
				})
			}
			if phase == "during parent refresh" {
				reads := 0
				client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
					reads++
					if reads == 2 {
						cancel()
					}
					return true, evidence.Parent.DeepCopy(), nil
				})
			}
			source, err := newTestSource(client.OmeV1beta1(), target)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := source.Get(ctx)
			if !errors.Is(err, context.Canceled) || snapshot.Value.Matched {
				t.Fatalf("cancellation must not become unheld evidence: %+v, %v", snapshot, err)
			}
		})
	}
}

func TestSourceWatchAndDecodeAreUnsupported(t *testing.T) {
	target, evidence := heldFixture()
	source, err := newTestSource(omefake.NewSimpleClientset(evidence.Parent, evidence.Replica).OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	watcher, err := source.Watch(context.Background(), "17")
	if watcher != nil || !errors.Is(err, ErrWatchUnsupported) {
		t.Fatalf("watch must not imply parent events cover child state: watcher=%v, err=%v", watcher, err)
	}
	snapshot, err := source.Decode(evidence.Replica)
	if !errors.Is(err, ErrWatchUnsupported) || snapshot.Value.Matched {
		t.Fatalf("decode cannot promote unobserved watch data: %+v, %v", snapshot, err)
	}
}

func TestSourceSanitizesCredentialShapedExactIdentity(t *testing.T) {
	for _, scenario := range []string{"sk parent", "ghp UID"} {
		t.Run(scenario, func(t *testing.T) {
			target, evidence := heldFixture()
			evidence.Replica.Status.RetryBlocks = nil
			secret := ""
			if scenario == "sk parent" {
				secret = "sk-proj-0123456789abcdefghijklmnopqrst"
				target.ParentName = secret
				target.IRName = secret + "-engine"
				target.Revision = target.IRName + "-aaaaaaaa"
				evidence.Parent.Name = secret
				evidence.Replica.Name = target.IRName
				evidence.Replica.Spec.ParentRef.Name = secret
				evidence.Replica.Labels[constants.InferenceServiceLabel] = secret
				evidence.Replica.OwnerReferences[0].Name = secret
			} else {
				secret = "ghp_0123456789abcdefghijklmnopqrst"
				target.IRUID = secret
				evidence.Replica.UID = types.UID(secret)
			}
			client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
			source, err := newTestSource(client.OmeV1beta1(), target)
			if err != nil {
				t.Fatal(err)
			}
			snapshot, err := source.Get(context.Background())
			if err != nil || !snapshot.Value.Matched {
				t.Fatalf("valid secret-shaped identity remains operable: %+v, %v", snapshot, err)
			}
			raw, err := json.Marshal(snapshot.Value)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(raw), secret) || strings.Contains(fmt.Sprintf("%+v", snapshot.Value), secret) {
				t.Fatalf("exported observation leaked credential-shaped identity: %s", raw)
			}
		})
	}
}

func TestSourceParentNotFoundClassificationAndPrivateErrors(t *testing.T) {
	target, evidence := heldFixture()
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	client.PrependReactor("get", "inferenceservices", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, &apierrors.StatusError{ErrStatus: metav1.Status{Reason: metav1.StatusReasonNotFound, Message: "PRIVATE_SERVER_MESSAGE", Code: 404}}
	})
	source, err := newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Get(context.Background())
	if !apierrors.IsNotFound(err) || strings.Contains(err.Error(), "PRIVATE_SERVER_MESSAGE") {
		t.Fatalf("parent 404 should remain classified but sanitized: %v", err)
	}

	client = omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	client.PrependReactor("get", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("PRIVATE_CHILD_ERROR")
	})
	source, err = newTestSource(client.OmeV1beta1(), target)
	if err != nil {
		t.Fatal(err)
	}
	_, err = source.Get(context.Background())
	if err == nil || strings.Contains(err.Error(), "PRIVATE_CHILD_ERROR") {
		t.Fatalf("child API error must be private: %v", err)
	}
}

func TestSourceRequiresValidatedActionTargetBeforeReads(t *testing.T) {
	target, evidence := heldFixture()
	target.IRName = ""
	client := omefake.NewSimpleClientset(evidence.Parent, evidence.Replica)
	if _, err := newTestSource(client.OmeV1beta1(), target); err == nil {
		t.Fatal("missing action-result name must be rejected before API access")
	}
}
