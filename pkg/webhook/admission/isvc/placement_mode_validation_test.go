package isvc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/runtimeselector"
	"sigs.k8s.io/ome/pkg/utils/testing/fakeclient"
	"sigs.k8s.io/ome/pkg/validation"
)

// placementModeFixedText opens every refusal of a placement mode change.
const placementModeFixedText = "spec.placement.mode is fixed for the life of the service"

// noPlacementBlockWord is how the refusal names a service without a
// spec.placement block.
const noPlacementBlockWord = "unset (no spec.placement block)"

// placementISVC is a service with the given placement block (nil for a
// service that lives on this cluster alone) and a runtime name, which is all
// the unrelated rules read.
func placementISVC(placement *v1beta1.PlacementSpec) *v1beta1.InferenceService {
	return &v1beta1.InferenceService{
		ObjectMeta: metav1.ObjectMeta{Name: "test-isvc", Namespace: "default"},
		Spec: v1beta1.InferenceServiceSpec{
			Runtime:   &v1beta1.ServingRuntimeRef{Name: "test-runtime"},
			Placement: placement,
		},
	}
}

// placedIn is a valid placement block for one mode; the two Split modes
// carry the fleet count they distribute.
func placedIn(mode v1beta1.PlacementMode) *v1beta1.PlacementSpec {
	p := &v1beta1.PlacementSpec{Policy: v1beta1.PlacementPolicyClusterAffinity, Mode: mode}
	if mode == v1beta1.PlacementModeSplit || mode == v1beta1.PlacementModeSplitByCapacity {
		p.Split = &v1beta1.SplitSpec{Replicas: ptr.To[int32](2)}
	}
	return p
}

func placementValidator(t *testing.T) *InferenceServiceValidator {
	t.Helper()
	c := fakeclient.New(t)
	return &InferenceServiceValidator{Client: c, Reader: c, RuntimeSelector: runtimeselector.New(c)}
}

// placementValues is every value a stored service's spec.placement.mode can
// hold, the missing block among them, with the word the refusal uses for it.
var placementValues = []struct {
	word      string
	placement *v1beta1.PlacementSpec
}{
	{word: noPlacementBlockWord},
	{word: "Single", placement: placedIn(v1beta1.PlacementModeSingle)},
	{word: "All", placement: placedIn(v1beta1.PlacementModeAll)},
	{word: "Split", placement: placedIn(v1beta1.PlacementModeSplit)},
	{word: "SplitByCapacity", placement: placedIn(v1beta1.PlacementModeSplitByCapacity)},
}

// TestInferenceService_PlacementModeChangeIsRefused: a running service keeps
// the placement mode it was created with. For every ordered pair of the four
// modes, and for the placement block added to a service created without one
// or removed from one that has it, the update is refused; the refusal names
// the mode the service has, the one the update asked for, and its reason
// code. The same value on both sides is admitted.
func TestInferenceService_PlacementModeChangeIsRefused(t *testing.T) {
	for _, from := range placementValues {
		for _, to := range placementValues {
			t.Run(from.word+" to "+to.word, func(t *testing.T) {
				_, err := placementValidator(t).ValidateUpdate(context.Background(), placementISVC(from.placement), placementISVC(to.placement))
				if from.word == to.word {
					require.NoError(t, err, "the mode is kept")
					return
				}
				require.Error(t, err, "the mode changes")
				assert.Contains(t, err.Error(), placementModeFixedText)
				assert.Contains(t, err.Error(), "from "+from.word+" to "+to.word)
				assert.Contains(t, err.Error(), validation.ReasonPlacementModeImmutable)
			})
		}
	}
}

// TestInferenceService_PlacementModeKeptAdmitsOtherEdits: with the mode kept,
// the rest of the spec stays editable, the placement block included.
func TestInferenceService_PlacementModeKeptAdmitsOtherEdits(t *testing.T) {
	modes := []v1beta1.PlacementMode{"", v1beta1.PlacementModeSingle, v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity}
	for _, mode := range modes {
		name := string(mode)
		if mode == "" {
			name = "no placement block"
		}
		t.Run(name, func(t *testing.T) {
			var placement *v1beta1.PlacementSpec
			if mode != "" {
				placement = placedIn(mode)
			}
			oldIsvc := placementISVC(placement)
			newIsvc := oldIsvc.DeepCopy()
			newIsvc.Spec.Runtime = &v1beta1.ServingRuntimeRef{Name: "another-runtime"}
			if p := newIsvc.Spec.Placement; p != nil {
				p.ClusterAffinity = []v1beta1.ClusterAffinityTerm{{MatchFields: []v1beta1.ClusterSelectorRequirement{
					{Key: "metadata.name", Operator: corev1.NodeSelectorOpIn, Values: []string{"member-a"}},
				}}}
				if p.Split != nil {
					p.Split.Replicas = ptr.To[int32](3)
				}
			}
			_, err := placementValidator(t).ValidateUpdate(context.Background(), oldIsvc, newIsvc)
			require.NoError(t, err)
		})
	}
}

// TestInferenceService_PlacementModeCreateAdmitsEveryMode: the mode is the
// operator's choice at create.
func TestInferenceService_PlacementModeCreateAdmitsEveryMode(t *testing.T) {
	for _, mode := range []v1beta1.PlacementMode{v1beta1.PlacementModeSingle, v1beta1.PlacementModeAll, v1beta1.PlacementModeSplit, v1beta1.PlacementModeSplitByCapacity} {
		t.Run(string(mode), func(t *testing.T) {
			_, err := placementValidator(t).ValidateCreate(context.Background(), placementISVC(placedIn(mode)))
			require.NoError(t, err)
		})
	}
}
