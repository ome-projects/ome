package types

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/tools/record"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func eventPod(name string) client.Object {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "ns", Name: name}}
}

func TestRecordNormalAndWarning_EmitOnceWithTheTypedReason(t *testing.T) {
	rec := record.NewFakeRecorder(4)
	target := eventPod("target")

	RecordNormal(rec, target, EventReasonInstanceReady, "ready %d", 3)
	RecordWarning(rec, target, EventReasonInstanceRejected, "rejected %s", "quota")

	normal := <-rec.Events
	warning := <-rec.Events
	if want := "Normal InstanceReady ready 3"; normal != want {
		t.Fatalf("normal event = %q, want %q", normal, want)
	}
	if want := "Warning InstanceRejected rejected quota"; warning != want {
		t.Fatalf("warning event = %q, want %q", warning, want)
	}
}

func TestRecordNormalAndWarning_NoRecorderOrNoTargetIsANoOp(t *testing.T) {
	RecordNormal(nil, eventPod("target"), EventReasonInstanceReady, "ready")
	RecordWarning(nil, eventPod("target"), EventReasonInstanceRejected, "rejected")

	rec := record.NewFakeRecorder(2)
	RecordNormal(rec, nil, EventReasonInstanceReady, "ready")
	RecordWarning(rec, nil, EventReasonInstanceRejected, "rejected")
	select {
	case got := <-rec.Events:
		t.Fatalf("nil target emitted %q", got)
	default:
	}
}

func TestEventTarget_PrefersTheExplicitTarget(t *testing.T) {
	owner := eventPod("owner")
	explicit := eventPod("explicit")

	if got := EventTarget(ReconcileInput{OwnerObject: owner}); got != owner {
		t.Fatalf("no explicit target: got %v, want the owner", got)
	}
	if got := EventTarget(ReconcileInput{OwnerObject: owner, EventTarget: explicit}); got != explicit {
		t.Fatalf("explicit target: got %v, want the explicit target", got)
	}
	if got := EventTarget(ReconcileInput{}); got != nil {
		t.Fatalf("neither set: got %v, want nil", got)
	}
}

func TestInstanceKey_OneShapeForEveryEmitter(t *testing.T) {
	if got, want := InstanceKey(ComponentType("engine"), 2), "component=engine instance=2"; got != want {
		t.Fatalf("InstanceKey = %q, want %q", got, want)
	}
}
