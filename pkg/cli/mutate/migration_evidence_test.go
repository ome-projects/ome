package mutate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	kubefake "k8s.io/client-go/kubernetes/fake"
	ktesting "k8s.io/client-go/testing"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/exitcode"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/controller/v1beta1/workload/query"
)

func migrationSources(v *v1beta1.InferenceService) (*v1beta1.InferenceReplica, []corev1.Pod, *appsv1.ControllerRevision) {
	ir := replicaFor(v)
	replicas := int32(1)
	ir.Spec.Replicas = &replicas
	ir.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameDefault, Size: 1}}
	ir.Status.UpdateRevision = ir.Status.CurrentRevision
	ir.Status.InstanceStatuses = []v1beta1.OMENativeInstanceStatus{{Index: 3, Incarnation: 1, Phase: v1beta1.OMENativeInstanceReady, RunningRevision: ir.Status.CurrentRevision, NodesOccupied: []string{"forged-node"}}}
	controller := true
	owner := []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceReplica", Name: ir.Name, UID: ir.UID, Controller: &controller}}
	labels := map[string]string{constants.InferenceServicePodLabelKey: v.Name, constants.OMEComponentLabel: "engine", query.LabelManagedBy: query.ManagedByOMENative, query.LabelInstanceIdx: "3", query.LabelInstanceIncarnation: "1", query.LabelPodOrdinal: "0", query.LabelRunner: "default", query.LabelRevisionHash: "aaaaaaaa"}
	pod := corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "chat-engine-3-default-0", Namespace: v.Namespace, UID: "uid-pod", ResourceVersion: "71", OwnerReferences: owner, Labels: labels}, Spec: corev1.PodSpec{NodeName: "node-a"}}
	cr := &appsv1.ControllerRevision{ObjectMeta: metav1.ObjectMeta{Name: ir.Status.CurrentRevision, Namespace: v.Namespace, UID: "uid-cr", ResourceVersion: "61", OwnerReferences: owner, Labels: map[string]string{constants.InferenceServicePodLabelKey: v.Name, constants.OMEComponentLabel: "engine", query.LabelManagedBy: query.ManagedByOMENative}}, Data: runtime.RawExtension{Raw: []byte(`{"podSpec":{"containers":[{"name":"main","image":"busybox"}]}}`)}}
	return ir, []corev1.Pod{pod}, cr
}

func migrationKube(pods []corev1.Pod, cr *appsv1.ControllerRevision, extra ...runtime.Object) *kubefake.Clientset {
	objects := extra
	if cr != nil {
		objects = append(objects, cr)
	}
	for i := range pods {
		objects = append(objects, &pods[i])
	}
	return kubefake.NewClientset(objects...)
}

func TestMigrationSourceMembershipAndWholeBounds(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(*v1beta1.InferenceService, *v1beta1.InferenceReplica, *[]corev1.Pod, *appsv1.ControllerRevision)
		code int
	}{
		{"sparse Ready", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
		}, 0},
		{"nil replicas", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			ir.Spec.Replicas = nil
		}, 0},
		{"zero replicas", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			*ir.Spec.Replicas = 0
		}, 0},
		{"replica drop sheds sparse", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			s := ir.Status.InstanceStatuses[0]
			s.Index = 0
			ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, s)
		}, 3},
		{"negative replicas", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			*ir.Spec.Replicas = -1
		}, 3},
		{"source not running", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			ir.Status.InstanceStatuses[0].RunningRevision = ""
		}, 3},
		{"target revision conflict", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			ir.Status.InstanceStatuses[0].TargetRevision = "chat-engine-bbbbbbbb"
		}, 3},
		{"parent freeze", func(v *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			v.Annotations = map[string]string{constants.PausedRolloutAnnotation: "freeze"}
		}, 3},
		{"unknown pause actor faithful", func(v *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			v.Annotations = map[string]string{constants.PausedRolloutAnnotation: "unknown"}
		}, 0},
		{"promote mailbox", func(v *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			v.Annotations = map[string]string{constants.RolloutPromoteAnnotation: "anything"}
		}, 3},
		{"pending same target", func(v *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			v.Annotations = map[string]string{constants.MigrationRequestAnnotationPrefix + migrationTestID: migrationTestPayload}
		}, 3},
		{"pending different target", func(v *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			v.Annotations = map[string]string{constants.MigrationRequestAnnotationPrefix + migrationTestID: strings.Replace(migrationTestPayload, `"instance":3`, `"instance":9`, 1)}
		}, 0},
		{"terminal is not pending", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			r := validMigration(v1beta1.MigrationPhaseFailed)
			r.SourceInstance = 3
			ir.Status.Migrations = []v1beta1.MigrationStatus{r}
		}, 0},
		{"live selected migration", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			r := validMigration(v1beta1.MigrationPhaseAccepted)
			r.SourceInstance = 3
			ir.Status.Migrations = []v1beta1.MigrationStatus{r}
		}, 3},
		{"live different migration", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			ir.Status.Migrations = []v1beta1.MigrationStatus{validMigration(v1beta1.MigrationPhaseAccepted)}
		}, 0},
		{"missing Pod", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			*p = nil
		}, 3},
		{"old incarnation", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			(*p)[0].Labels[query.LabelInstanceIncarnation] = "0"
		}, 3},
		{"wrong hash", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			(*p)[0].Labels[query.LabelRevisionHash] = "bbbbbbbb"
		}, 3},
		{"noncanonical ordinal", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			(*p)[0].Labels[query.LabelPodOrdinal] = "00"
		}, 3},
		{"missing ordinal", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			delete((*p)[0].Labels, query.LabelPodOrdinal)
		}, 3},
		{"wrong owner", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			(*p)[0].OwnerReferences[0].UID = "replacement"
		}, 3},
		{"all deleting", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			stamp := metav1.NewTime(testNow)
			(*p)[0].DeletionTimestamp = &stamp
		}, 3},
		{"unscheduled", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			(*p)[0].Spec.NodeName = ""
		}, 3},
		{"Pod payload cap", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, p *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			(*p)[0].Annotations = map[string]string{"private": strings.Repeat("s", 1048577)}
		}, 1},
		{"whole IR cap", func(_ *v1beta1.InferenceService, ir *v1beta1.InferenceReplica, _ *[]corev1.Pod, _ *appsv1.ControllerRevision) {
			ir.Status.InstanceStatuses[0].NodesOccupied = []string{strings.Repeat("s", 1048577)}
		}, 1},
		{"CR owner", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *[]corev1.Pod, cr *appsv1.ControllerRevision) {
			cr.OwnerReferences[0].UID = "replacement"
		}, 3},
		{"CR raw malformed", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *[]corev1.Pod, cr *appsv1.ControllerRevision) {
			cr.Data.Raw = []byte(`{"podSpec":null}`)
		}, 1},
		{"CR leader affinity", func(_ *v1beta1.InferenceService, _ *v1beta1.InferenceReplica, _ *[]corev1.Pod, cr *appsv1.ControllerRevision) {
			cr.Data.Raw = []byte(`{"podSpec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["node-a"]}]}]}}}}}`)
		}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			tc.edit(v, ir, &pods, cr)
			o := MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3, RequestedBy: "kubectl-ome"}
			e, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), migrationKube(pods, cr), v, []string{"engine"}, o, testClock)
			if tc.code == 0 {
				require.NoError(t, err)
				require.True(t, e.complete)
				require.Equal(t, "node-a", e.fromNode)
				require.NotContains(t, e.nodes, "forged-node")
			} else {
				require.Error(t, err)
				require.Equal(t, tc.code, exitcode.FromError(err))
				require.False(t, e.complete)
			}
		})
	}
}

func TestMigrationGangCurrentCompleteNodesAndWorkerAffinity(t *testing.T) {
	for _, tc := range []struct {
		name, from              string
		missing, workerAffinity bool
		code                    int
	}{{"ambiguous", "", false, false, 3}, {"member explicit", "node-b", false, false, 0}, {"outside", "node-c", false, false, 3}, {"incomplete", "node-a", true, false, 3}, {"worker affinity", "node-a", false, true, 3}} {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			ir.Spec.Runners = []v1beta1.Runner{{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: 1}}
			pods[0].Name = "chat-engine-3-leader-0"
			pods[0].Labels[query.LabelRunner] = "leader"
			worker := *pods[0].DeepCopy()
			worker.Name = "chat-engine-3-worker-0"
			worker.UID = "uid-worker"
			worker.Labels[query.LabelRunner] = "worker"
			worker.Spec.NodeName = "node-b"
			if !tc.missing {
				pods = append(pods, worker)
			}
			cr.Data.Raw = []byte(`{"podSpec":{},"workerPodSpec":{}}`)
			if tc.workerAffinity {
				cr.Data.Raw = []byte(`{"podSpec":{},"workerPodSpec":{"affinity":{"nodeAffinity":{"requiredDuringSchedulingIgnoredDuringExecution":{"nodeSelectorTerms":[{"matchExpressions":[{"key":"kubernetes.io/hostname","operator":"In","values":["node-a"]}]}]}}}}}`)
			}
			o := MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3, FromNode: tc.from}
			e, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), migrationKube(pods, cr), v, []string{"engine"}, o, testClock)
			require.Equal(t, tc.code, exitcode.FromError(err))
			if tc.code == 0 {
				require.NoError(t, err)
				require.Equal(t, []string{"node-a", "node-b"}, e.nodes)
			}
		})
	}
}

func TestMigrationRecheckSelectedSourceNeverReplans(t *testing.T) {
	for _, resource := range []string{"none", "inferencereplicas", "pods", "controllerrevisions"} {
		t.Run(resource, func(t *testing.T) {
			v, state := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			ome := omefake.NewSimpleClientset(ir)
			kube := migrationKube(pods, cr)
			o := MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3}
			e, err := CollectMigrationEvidence(context.Background(), ome.OmeV1beta1(), kube, v, []string{"engine"}, o, testClock)
			require.NoError(t, err)
			p, err := PrepareMigration(v, state, e, o, func() (string, error) { return migrationTestID, nil }, testClock)
			require.NoError(t, err)
			if resource == "inferencereplicas" {
				ome.PrependReactor("get", resource, func(ktesting.Action) (bool, runtime.Object, error) {
					changed := ir.DeepCopy()
					changed.ResourceVersion = "82"
					return true, changed, nil
				})
			}
			if resource == "pods" {
				kube.PrependReactor("list", resource, func(ktesting.Action) (bool, runtime.Object, error) {
					changed := pods[0].DeepCopy()
					changed.ResourceVersion = "72"
					return true, &corev1.PodList{Items: []corev1.Pod{*changed}}, nil
				})
			}
			if resource == "controllerrevisions" {
				kube.PrependReactor("get", resource, func(ktesting.Action) (bool, runtime.Object, error) {
					changed := cr.DeepCopy()
					changed.ResourceVersion = "62"
					return true, changed, nil
				})
			}
			err = RecheckMigration(context.Background(), ome.OmeV1beta1(), kube, v, p, testClock)
			if resource == "none" {
				require.NoError(t, err)
			} else {
				require.Equal(t, 3, exitcode.FromError(err))
			}
			require.Equal(t, migrationTestID, p.RequestID())
		})
	}
}

func TestMigrationLookupAuditRequiredAndLossySources(t *testing.T) {
	for _, tc := range []struct {
		name                                           string
		pending, audit, forbidden, wrongOwner, corrupt bool
		code                                           int
	}{{"pending exact", true, false, false, false, false, 0}, {"unseen", false, false, false, false, false, 3}, {"audit lossy", false, true, false, false, false, 3}, {"audit wrong owner", true, true, false, true, false, 1}, {"audit corrupt", true, true, false, false, true, 1}, {"required forbidden", true, false, true, false, false, 1}} {
		t.Run(tc.name, func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			if tc.pending {
				v.Annotations = map[string]string{constants.MigrationRequestAnnotationPrefix + migrationTestID: migrationTestPayload}
			}
			var extra []runtime.Object
			if tc.audit {
				controller := true
				cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: v.Name + "-ome-migration-audit", Namespace: v.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: v.Name, UID: v.UID, Controller: &controller}}}, Data: map[string]string{"history.json": fmt.Sprintf(`{"entries":[{"requestUUID":%q,"component":"engine","sourceInstance":3,"fromNode":"node-a","phase":"Completed"}]}`, migrationTestID)}}
				if tc.wrongOwner {
					cm.OwnerReferences[0].UID = "wrong"
				}
				if tc.corrupt {
					cm.Data["history.json"] = `{bad`
				}
				extra = append(extra, cm)
			}
			kube := migrationKube(pods, cr, extra...)
			if tc.forbidden {
				kube.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "configmaps"}, "private", errors.New("secret"))
				})
			}
			o := MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3, RequestedBy: "kubectl-ome", RequestID: migrationTestID}
			e, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), kube, v, []string{"engine"}, o, testClock)
			if err == nil {
				r, ok := e.pending[migrationTestID]
				if !ok || r.FromNode != "node-a" {
					err = migrationConflict()
				}
			}
			require.Equal(t, tc.code, exitcode.FromError(err))
			if err != nil {
				require.NotContains(t, err.Error(), "secret")
			}
		})
	}
}

func TestMigrationPodCollectionCapBeforePositiveProof(t *testing.T) {
	v, _ := nativeTarget(t)
	ir, pods, cr := migrationSources(v)
	for i := 1; i < 129; i++ {
		p := *pods[0].DeepCopy()
		p.Name = fmt.Sprintf("extra-%d", i)
		p.UID = types.UID(p.Name)
		pods = append(pods, p)
	}
	_, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), migrationKube(pods, cr), v, []string{"engine"}, MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3}, testClock)
	require.ErrorIs(t, err, ErrBounds)
	raw, _ := json.Marshal(map[string]any{"a": strings.Repeat("a", 1048577)})
	require.False(t, boundedPrivatePayload(raw))
}

func TestMigrationRequiredSourcesAPIErrorsAndVanishing(t *testing.T) {
	for _, resource := range []string{"pods", "controllerrevisions", "inferencereplicas", "missing revision"} {
		t.Run(resource, func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			ome := omefake.NewSimpleClientset(ir)
			kube := migrationKube(pods, cr)
			failure := func(ktesting.Action) (bool, runtime.Object, error) {
				return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: resource}, "private", errors.New("secret"))
			}
			if resource == "inferencereplicas" {
				ome.PrependReactor("list", resource, failure)
			} else if resource == "missing revision" {
				kube.PrependReactor("get", "controllerrevisions", func(ktesting.Action) (bool, runtime.Object, error) {
					return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "controllerrevisions"}, "private")
				})
			} else {
				kube.PrependReactor("*", resource, failure)
			}
			_, err := CollectMigrationEvidence(context.Background(), ome.OmeV1beta1(), kube, v, []string{"engine"}, MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3}, testClock)
			require.Error(t, err)
			want := 1
			if resource == "missing revision" {
				want = 3
			}
			require.Equal(t, want, exitcode.FromError(err))
			require.NotContains(t, err.Error(), "secret")
		})
	}
}

func TestMigrationRecheckRequiredErrorsAndBounds(t *testing.T) {
	for _, kind := range []string{"missing IR", "forbidden IR", "stale IR", "IR cap", "canceled", "invalid plan", "existing lookup"} {
		t.Run(kind, func(t *testing.T) {
			v, state := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			ome := omefake.NewSimpleClientset(ir)
			kube := migrationKube(pods, cr)
			o := MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3}
			e, err := CollectMigrationEvidence(context.Background(), ome.OmeV1beta1(), kube, v, []string{"engine"}, o, testClock)
			require.NoError(t, err)
			p, err := PrepareMigration(v, state, e, o, func() (string, error) { return migrationTestID, nil }, nil)
			require.NoError(t, err)
			ctx := context.Background()
			if kind == "invalid plan" {
				p = MigrationPlan{}
			} else if kind == "existing lookup" {
				p.existing = true
			} else if kind == "canceled" {
				c, cancel := context.WithCancel(ctx)
				cancel()
				ctx = c
			} else {
				ome.PrependReactor("get", "inferencereplicas", func(ktesting.Action) (bool, runtime.Object, error) {
					switch kind {
					case "missing IR":
						return true, nil, apierrors.NewNotFound(schema.GroupResource{Resource: "inferencereplicas"}, "private")
					case "forbidden IR":
						return true, nil, errors.New("private secret")
					case "stale IR":
						r := ir.DeepCopy()
						r.Status.ObservedGeneration = 0
						return true, r, nil
					default:
						r := ir.DeepCopy()
						r.Status.InstanceStatuses = make([]v1beta1.OMENativeInstanceStatus, 2049)
						return true, r, nil
					}
				})
			}
			err = RecheckMigration(ctx, ome.OmeV1beta1(), kube, v, p, nil)
			want := 3
			if kind == "forbidden IR" || kind == "IR cap" || kind == "canceled" {
				want = 1
			}
			if kind == "existing lookup" {
				want = 0
			}
			require.Equal(t, want, exitcode.FromError(err))
		})
	}
}

func TestMigrationRunnerShapeAndInvalidDependencies(t *testing.T) {
	for _, runners := range [][]v1beta1.Runner{nil, {{Name: v1beta1.RunnerNameDefault, Size: 2}}, {{Name: v1beta1.RunnerNameLeader, Size: 2}, {Name: v1beta1.RunnerNameWorker, Size: 1}}, {{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: 0}}, {{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameWorker, Size: 128}}, {{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: v1beta1.RunnerNameLeader, Size: 1}}, {{Name: v1beta1.RunnerNameLeader, Size: 1}, {Name: "private", Size: 1}}} {
		require.False(t, validMigrationRunners(&v1beta1.InferenceReplica{Spec: v1beta1.InferenceReplicaSpec{Runners: runners}}))
	}
	v, _ := nativeTarget(t)
	_, err := CollectMigrationEvidence(nil, nil, nil, v, []string{"engine"}, MigrationOptions{Component: v1beta1.EngineComponent}, nil) //nolint:staticcheck // Deliberately test the nil-context refusal boundary.
	require.Error(t, err)
	_, err = CollectMigrationEvidence(context.Background(), nil, nil, nil, nil, MigrationOptions{}, nil)
	require.Error(t, err)
}

func TestMigrationAuditWholeBoundsAndCrossSourceIdentity(t *testing.T) {
	for _, kind := range []string{"oversized", "too many", "invalid row", "invalid hint", "cross identity", "new unreadable"} {
		t.Run(kind, func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			controller := true
			v.Annotations = map[string]string{constants.MigrationRequestAnnotationPrefix + migrationTestID: migrationTestPayload}
			entry := migrationAuditEntry{ID: migrationTestID, Component: "engine", Index: new(int32), FromNode: "node-a", Phase: "Completed"}
			*entry.Index = 3
			entries := []migrationAuditEntry{entry}
			switch kind {
			case "too many":
				entries = make([]migrationAuditEntry, 801)
			case "invalid row":
				entries[0].Index = nil
			case "invalid hint":
				entries[0].Hints = []string{"private secret"}
			case "cross identity":
				entries[0].FromNode = "node-b"
			}
			raw, _ := json.Marshal(struct {
				Entries []migrationAuditEntry `json:"entries"`
			}{entries})
			if kind == "oversized" {
				raw = []byte(strings.Repeat("s", 1048577))
			}
			cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: v.Name + "-ome-migration-audit", Namespace: v.Namespace, OwnerReferences: []metav1.OwnerReference{{APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: v.Name, UID: v.UID, Controller: &controller}}}, Data: map[string]string{"history.json": string(raw)}}
			kube := migrationKube(pods, cr, cm)
			o := MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3, RequestedBy: "kubectl-ome", RequestID: migrationTestID}
			if kind == "new unreadable" {
				o.RequestID = ""
				delete(v.Annotations, constants.MigrationRequestAnnotationPrefix+migrationTestID)
				kube.PrependReactor("get", "configmaps", func(ktesting.Action) (bool, runtime.Object, error) { return true, nil, errors.New("private secret") })
			}
			e, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), kube, v, []string{"engine"}, o, testClock)
			if kind == "cross identity" {
				require.NoError(t, err)
				require.True(t, e.conflicting[migrationTestID])
			} else if kind == "new unreadable" {
				require.NoError(t, err)
				require.Contains(t, strings.Join(e.warnings, " "), "unavailable")
			} else {
				require.Error(t, err)
				require.Equal(t, 1, exitcode.FromError(err))
			}
		})
	}
}

func TestMigrationIncomingUpdateSurgeAndUnrelatedFailedRepair(t *testing.T) {
	for _, kind := range []string{"incoming update", "failed update cleanup", "unrelated failed create", "unknown policy", "Surge policy"} {
		t.Run(kind, func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			if kind == "unknown policy" || kind == "Surge policy" {
				mode := v1beta1.MigrationPolicyMode("Unknown")
				if kind == "Surge policy" {
					mode = v1beta1.MigrationPolicyModeSurge
				}
				ir.Spec.Lifecycle = &v1beta1.LifecycleSpec{MigrationPolicy: &v1beta1.MigrationPolicy{Mode: mode}}
			} else {
				other := v1beta1.OMENativeInstanceStatus{Index: 0, Phase: v1beta1.OMENativeInstanceUpdating, Operation: &v1beta1.InstanceOperation{ID: "update-0-1", Type: v1beta1.InstanceOperationUpdate, Step: "SurgeDrain", StartedAt: metav1.NewTime(testNow.Add(-2e9)), LastProgressAt: metav1.NewTime(testNow.Add(-1e9))}}
				surge := int32(3)
				other.Operation.SurgeIndex = &surge
				if kind == "failed update cleanup" {
					other.Phase = v1beta1.OMENativeInstanceFailed
				}
				if kind == "unrelated failed create" {
					other.Phase = v1beta1.OMENativeInstanceFailed
					other.Operation.Type = v1beta1.InstanceOperationCreate
					other.Operation.SurgeIndex = nil
				}
				ir.Status.InstanceStatuses = append(ir.Status.InstanceStatuses, other)
			}
			_, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), migrationKube(pods, cr), v, []string{"engine"}, MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3}, testClock)
			want := 3
			if kind == "unrelated failed create" || kind == "Surge policy" {
				want = 0
			}
			require.Equal(t, want, exitcode.FromError(err))
		})
	}
}

func TestMigrationTerminalsAndDeletingAlternatePodAreNotPending(t *testing.T) {
	for _, phase := range []v1beta1.MigrationPhase{v1beta1.MigrationPhaseCompleted, v1beta1.MigrationPhaseFailed, v1beta1.MigrationPhaseRelocated} {
		t.Run(string(phase), func(t *testing.T) {
			v, _ := nativeTarget(t)
			ir, pods, cr := migrationSources(v)
			r := validMigration(phase)
			r.SourceInstance = 3
			ir.Status.Migrations = []v1beta1.MigrationStatus{r}
			old := *pods[0].DeepCopy()
			old.Name = "chat-engine-3-default-1"
			old.UID = "old-pod"
			old.Labels[query.LabelPodOrdinal] = "1"
			old.Labels[query.LabelInstanceIncarnation] = "0"
			old.Spec.NodeName = "old-node"
			stamp := metav1.NewTime(testNow)
			old.DeletionTimestamp = &stamp
			pods = append(pods, old)
			e, err := CollectMigrationEvidence(context.Background(), omefake.NewSimpleClientset(ir).OmeV1beta1(), migrationKube(pods, cr), v, []string{"engine"}, MigrationOptions{Component: v1beta1.EngineComponent, Instance: 3}, testClock)
			require.NoError(t, err)
			require.Equal(t, []string{"node-a"}, e.nodes)
		})
	}
}
