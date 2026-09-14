package get

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	versioned "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	typedome "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

func autoscalerPolicyFixture(name, namespace string) *v1beta1.AutoscalerPolicy {
	return &v1beta1.AutoscalerPolicy{
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Generation: 3,
			Labels:     map[string]string{"tier": "gold"},
		},
		Spec: v1beta1.AutoscalerPolicySpec{Class: v1beta1.AutoscalerHPA},
		Status: v1beta1.AutoscalerPolicyStatus{
			ObservedGeneration: 3,
			PortableDigest:     "ap1:1234567890ab",
			AttachedComponents: 0,
			Conditions: []metav1.Condition{
				{
					Type:               v1beta1.AutoscalerPolicyReadyCondition,
					Status:             metav1.ConditionTrue,
					ObservedGeneration: 3,
					Reason:             v1beta1.AutoscalerPolicyReasonTemplatesValid,
				},
				{
					Type:               v1beta1.AutoscalerPolicyInUseCondition,
					Status:             metav1.ConditionFalse,
					ObservedGeneration: 3,
					Reason:             v1beta1.AutoscalerPolicyReasonNoConsumers,
				},
			},
		},
	}
}

func autoscalerPolicyColumnValues(t *testing.T, policy runtime.Object) map[string]string {
	t.Helper()
	entry, err := resolve("ap")
	require.NoError(t, err)
	values := make(map[string]string, len(entry.Columns))
	for _, column := range entry.Columns {
		values[column.Name] = column.Extract(policy)
	}
	return values
}

// TestAutoscalerPolicyColumnsCurrentStatus protects the operator-facing
// inventory contract, including the meaningful current value zero for an
// unattached policy.
func TestAutoscalerPolicyColumnsCurrentStatus(t *testing.T) {
	values := autoscalerPolicyColumnValues(t, autoscalerPolicyFixture("cpu-policy", "team-a"))

	assert.Equal(t, "cpu-policy", values["NAME"])
	assert.Equal(t, "HPA", values["CLASS"])
	assert.Equal(t, "True", values["READY"])
	assert.Equal(t, "0", values["ATTACHED"])
	assert.Equal(t, "ap1:1234567890ab", values["DIGEST"])
	assert.Equal(t, v1beta1.AutoscalerPolicyReasonTemplatesValid, values["REASON"])
	assert.Equal(t, "False", values["IN-USE"])
	assert.Equal(t, "Current", values["STATUS-FRESHNESS"])
}

func TestAutoscalerPolicyColumnsHideStaleAndUnobservedStatus(t *testing.T) {
	for _, test := range []struct {
		name      string
		observed  int64
		freshness string
	}{
		{name: "stale", observed: 2, freshness: "Stale"},
		{name: "unobserved", observed: 0, freshness: "Unobserved"},
	} {
		t.Run(test.name, func(t *testing.T) {
			policy := autoscalerPolicyFixture(test.name, "team-a")
			policy.Status.ObservedGeneration = test.observed
			policy.Status.AttachedComponents = 4
			values := autoscalerPolicyColumnValues(t, policy)

			assert.Equal(t, "Unknown", values["READY"])
			assert.Equal(t, "-", values["ATTACHED"])
			assert.Equal(t, "-", values["DIGEST"])
			assert.Equal(t, "-", values["REASON"])
			assert.Equal(t, "Unknown", values["IN-USE"])
			assert.Equal(t, test.freshness, values["STATUS-FRESHNESS"])
		})
	}
}

func TestAutoscalerPolicyColumnsTreatZeroValueStatusAsUnobserved(t *testing.T) {
	values := autoscalerPolicyColumnValues(t, &v1beta1.AutoscalerPolicy{})

	assert.Equal(t, "Unknown", values["READY"])
	assert.Equal(t, "-", values["ATTACHED"])
	assert.Equal(t, "-", values["DIGEST"])
	assert.Equal(t, "-", values["REASON"])
	assert.Equal(t, "Unknown", values["IN-USE"])
	assert.Equal(t, "Unobserved", values["STATUS-FRESHNESS"])
}

// A current status envelope does not make an individually stale condition
// trustworthy. Scalar status remains visible, while only the stale condition
// and its reason fail closed.
func TestAutoscalerPolicyColumnsGateEachConditionGeneration(t *testing.T) {
	policy := autoscalerPolicyFixture("mixed-freshness", "team-a")
	policy.Status.Conditions[0].ObservedGeneration = 2
	values := autoscalerPolicyColumnValues(t, policy)

	assert.Equal(t, "Unknown", values["READY"])
	assert.Equal(t, "-", values["REASON"])
	assert.Equal(t, "False", values["IN-USE"])
	assert.Equal(t, "0", values["ATTACHED"])
	assert.Equal(t, "ap1:1234567890ab", values["DIGEST"])
	assert.Equal(t, "Current", values["STATUS-FRESHNESS"])
}

func TestAutoscalerPolicyColumnsWrongTypeAreSafe(t *testing.T) {
	values := autoscalerPolicyColumnValues(t, &v1beta1.InferenceService{})
	for name, value := range values {
		assert.Equal(t, "?", value, name)
	}
}

func TestGetAutoscalerPolicyDefaultWideAndAllNamespaces(t *testing.T) {
	teamA := autoscalerPolicyFixture("cpu-policy", "team-a")
	teamB := autoscalerPolicyFixture("cpu-policy", "team-b")
	client := omefake.NewSimpleClientset(teamA, teamB)
	f := factory.Static{OME: client, NS: "team-a"}

	defaultOutput, err := execute(t, f, "ap")
	require.NoError(t, err)
	defaultLines := strings.Split(strings.TrimSpace(defaultOutput), "\n")
	require.Len(t, defaultLines, 2)
	assert.Equal(t, []string{"NAME", "CLASS", "READY", "ATTACHED", "AGE"}, strings.Fields(defaultLines[0]))
	assert.Contains(t, defaultLines[1], "cpu-policy")
	assert.NotContains(t, defaultOutput, "DIGEST")

	wideOutput, err := execute(t, f, "autoscalerpolicy", "-o", "wide")
	require.NoError(t, err)
	wideLines := strings.Split(strings.TrimSpace(wideOutput), "\n")
	require.Len(t, wideLines, 2)
	assert.Equal(t,
		[]string{"NAME", "CLASS", "READY", "ATTACHED", "AGE", "DIGEST", "REASON", "IN-USE", "STATUS-FRESHNESS"},
		strings.Fields(wideLines[0]),
	)
	assert.Contains(t, wideOutput, "ap1:1234567890ab")
	assert.Contains(t, wideOutput, v1beta1.AutoscalerPolicyReasonTemplatesValid)

	allNamespacesOutput, err := execute(t, f, "autoscalerpolicies", "-A")
	require.NoError(t, err)
	assert.Equal(t, 2, strings.Count(allNamespacesOutput, "cpu-policy"), allNamespacesOutput)
	assert.Equal(t, []string{"NAMESPACE", "NAME", "CLASS", "READY", "ATTACHED", "AGE"},
		strings.Fields(strings.Split(strings.TrimSpace(allNamespacesOutput), "\n")[0]))
	assert.Contains(t, allNamespacesOutput, "team-a")
	assert.Contains(t, allNamespacesOutput, "team-b")
}

func TestGetAutoscalerPolicySelectorAndMachineFormats(t *testing.T) {
	selected := autoscalerPolicyFixture("selected", "team-a")
	unselected := autoscalerPolicyFixture("unselected", "team-a")
	unselected.Labels = map[string]string{"tier": "silver"}
	f := factory.Static{OME: omefake.NewSimpleClientset(selected, unselected), NS: "team-a"}

	selectedOutput, err := execute(t, f, "ap", "-l", "tier=gold")
	require.NoError(t, err)
	assert.Contains(t, selectedOutput, "selected")
	assert.NotContains(t, selectedOutput, "unselected")

	jsonOutput, err := execute(t, f, "ap", "selected", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, jsonOutput, `"name": "selected"`)
	assert.Contains(t, jsonOutput, `"class": "HPA"`)

	yamlOutput, err := execute(t, f, "autoscalerpolicy", "selected", "-o", "yaml")
	require.NoError(t, err)
	assert.Contains(t, yamlOutput, "name: selected")
	assert.Contains(t, yamlOutput, "class: HPA")
}

type recordingAutoscalerPolicyClient struct {
	typedome.AutoscalerPolicyInterface
	requests []metav1.ListOptions
}

func (client *recordingAutoscalerPolicyClient) List(
	_ context.Context,
	options metav1.ListOptions,
) (*v1beta1.AutoscalerPolicyList, error) {
	client.requests = append(client.requests, options)
	switch options.Continue {
	case "":
		return &v1beta1.AutoscalerPolicyList{
			ListMeta: metav1.ListMeta{Continue: "second-page"},
			Items:    []v1beta1.AutoscalerPolicy{*autoscalerPolicyFixture("first", "team-a")},
		}, nil
	case "second-page":
		return &v1beta1.AutoscalerPolicyList{
			Items: []v1beta1.AutoscalerPolicy{*autoscalerPolicyFixture("second", "team-a")},
		}, nil
	default:
		return nil, fmt.Errorf("unexpected continuation token %q", options.Continue)
	}
}

type recordingAutoscalerPolicyV1beta1 struct {
	typedome.OmeV1beta1Interface
	client     *recordingAutoscalerPolicyClient
	namespaces []string
}

func (client *recordingAutoscalerPolicyV1beta1) AutoscalerPolicies(namespace string) typedome.AutoscalerPolicyInterface {
	client.namespaces = append(client.namespaces, namespace)
	return client.client
}

type recordingAutoscalerPolicyClientset struct {
	versioned.Interface
	client *recordingAutoscalerPolicyV1beta1
}

func (client *recordingAutoscalerPolicyClientset) OmeV1beta1() typedome.OmeV1beta1Interface {
	return client.client
}

// TestGetAutoscalerPolicyUsesBoundedTypedPaging catches regressions that
// either bypass the generated OME client or silently lose selectors and
// continuation tokens while paging.
func TestGetAutoscalerPolicyUsesBoundedTypedPaging(t *testing.T) {
	policies := &recordingAutoscalerPolicyClient{}
	v1Client := &recordingAutoscalerPolicyV1beta1{client: policies}
	clientset := &recordingAutoscalerPolicyClientset{client: v1Client}
	f := factory.Static{OME: clientset, NS: "team-a"}

	output, err := execute(t, f, "ap", "-l", "tier=gold")
	require.NoError(t, err)
	assert.Contains(t, output, "first")
	assert.Contains(t, output, "second")
	assert.Equal(t, []string{"team-a"}, v1Client.namespaces)
	require.Len(t, policies.requests, 2)
	assert.Equal(t, int64(paging.ChunkSize), policies.requests[0].Limit)
	assert.Empty(t, policies.requests[0].Continue)
	assert.Equal(t, "tier=gold", policies.requests[0].LabelSelector)
	assert.Equal(t, int64(paging.ChunkSize), policies.requests[1].Limit)
	assert.Equal(t, "second-page", policies.requests[1].Continue)
	assert.Equal(t, "tier=gold", policies.requests[1].LabelSelector)
}
