package get

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/cli-runtime/pkg/genericiooptions"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	"sigs.k8s.io/ome/pkg/cli/factory"
	"sigs.k8s.io/ome/pkg/cli/paging"
	versioned "sigs.k8s.io/ome/pkg/client/clientset/versioned"
	omefake "sigs.k8s.io/ome/pkg/client/clientset/versioned/fake"
	typedome "sigs.k8s.io/ome/pkg/client/clientset/versioned/typed/ome/v1beta1"
)

type trafficMapTerminalBuffer struct {
	bytes.Buffer
}

func (*trafficMapTerminalBuffer) TerminalWidth() (int, bool) {
	return 80, true
}

func trafficMapFixture(name, namespace string) *v1beta1.TrafficMap {
	return &v1beta1.TrafficMap{
		TypeMeta: metav1.TypeMeta{APIVersion: "ome.io/v1beta1", Kind: "TrafficMap"},
		ObjectMeta: metav1.ObjectMeta{
			Name:       name,
			Namespace:  namespace,
			Generation: 4,
			Labels:     map[string]string{"tier": "gold"},
		},
		Spec: v1beta1.TrafficMapSpec{
			Service: name,
			Mode:    v1beta1.PlacementModeSplit,
			Entries: []v1beta1.TrafficMapEntry{
				{Cluster: "gpu-a", Weight: 80, Healthy: true},
				{Cluster: "gpu-b", Weight: 0, Healthy: false},
			},
			ObservedISVCGeneration: 7,
		},
		Status: v1beta1.TrafficMapStatus{
			Published:                    true,
			ObservedTrafficMapGeneration: 4,
			GatewayRef: &v1beta1.TrafficMapGatewayRef{
				Group: "gateway.networking.k8s.io", Kind: "HTTPRoute", Name: name,
			},
			Conditions: []metav1.Condition{
				{Type: v1beta1.TrafficMapRoutable, Status: metav1.ConditionTrue, Reason: v1beta1.TrafficMapReasonRoutable, ObservedGeneration: 4},
				{Type: v1beta1.TrafficMapPublished, Status: metav1.ConditionTrue, Reason: "Published", ObservedGeneration: 4},
				{Type: v1beta1.TrafficMapOverrideActive, Status: metav1.ConditionFalse, Reason: v1beta1.TrafficMapReasonNoOverrides, ObservedGeneration: 4},
			},
		},
	}
}

func trafficMapColumnValues(t *testing.T, object runtime.Object) map[string]string {
	t.Helper()
	resource, err := resolve("tm")
	require.NoError(t, err)
	values := make(map[string]string, len(resource.Columns))
	for _, column := range resource.Columns {
		values[column.Name] = column.Extract(object)
	}
	return values
}

// Removing any alias or changing TrafficMap to cluster scope breaks the
// public resource-discovery contract this test exercises.
func TestResolveTrafficMapCanonicalAliasesAndScope(t *testing.T) {
	for _, name := range []string{"trafficmaps", "trafficmap", "tm", "tmap", "TMAP"} {
		resource, err := resolve(name)
		require.NoError(t, err, name)
		assert.Equal(t, "trafficmaps", resource.Canonical, name)
		assert.True(t, resource.Namespaced, name)
	}
}

// Returning stale conditions as current would make an old routing decision
// look live. These assertions pin both the compact and wide evidence model.
func TestTrafficMapColumnsExposeCurrentRoutingEvidence(t *testing.T) {
	values := trafficMapColumnValues(t, trafficMapFixture("checkout", "team-a"))

	assert.Equal(t, "checkout", values["NAME"])
	assert.Equal(t, "Split", values["MODE"])
	assert.Equal(t, "2", values["TARGETS"])
	assert.Equal(t, "True", values["ROUTABLE"])
	assert.Equal(t, "True", values["PUBLISHED"])
	assert.Equal(t, "checkout", values["SERVICE"])
	assert.Equal(t, "1", values["ACTIVE"])
	assert.Equal(t, "1", values["HEALTHY"])
	assert.Equal(t, "False", values["OVERRIDE"])
	assert.Equal(t, v1beta1.TrafficMapReasonNoOverrides, values["OVERRIDE-REASON"])
	assert.Equal(t, v1beta1.TrafficMapReasonRoutable, values["REASON"])
	assert.Equal(t, "gateway.networking.k8s.io/HTTPRoute:team-a/checkout", values["GATEWAY"])
	assert.Equal(t, "Current", values["PUBLISHER-FRESHNESS"])
}

func TestTrafficMapColumnsFailClosedForStaleStatus(t *testing.T) {
	trafficMap := trafficMapFixture("checkout", "team-a")
	trafficMap.Generation = 5
	values := trafficMapColumnValues(t, trafficMap)

	assert.Equal(t, "Unknown", values["ROUTABLE"])
	assert.Equal(t, "Unknown", values["PUBLISHED"])
	assert.Equal(t, "Unknown", values["OVERRIDE"])
	assert.Equal(t, "-", values["OVERRIDE-REASON"])
	assert.Equal(t, "-", values["REASON"])
	assert.Equal(t, "Stale", values["PUBLISHER-FRESHNESS"])
}

func TestTrafficMapOverrideReasonUsesOnlyCurrentCondition(t *testing.T) {
	tests := []struct {
		name       string
		status     metav1.ConditionStatus
		reason     string
		generation int64
		wantStatus string
		wantReason string
	}{
		{
			name: "inactive", status: metav1.ConditionFalse,
			reason: v1beta1.TrafficMapReasonNoOverrides, generation: 4,
			wantStatus: "False", wantReason: v1beta1.TrafficMapReasonNoOverrides,
		},
		{
			name: "pending", status: metav1.ConditionFalse,
			reason: v1beta1.TrafficMapReasonOverridesPending, generation: 4,
			wantStatus: "False", wantReason: v1beta1.TrafficMapReasonOverridesPending,
		},
		{
			name: "applied", status: metav1.ConditionTrue,
			reason: v1beta1.TrafficMapReasonOverridesApplied, generation: 4,
			wantStatus: "True", wantReason: v1beta1.TrafficMapReasonOverridesApplied,
		},
		{
			name: "stale", status: metav1.ConditionTrue,
			reason: v1beta1.TrafficMapReasonOverridesApplied, generation: 3,
			wantStatus: "Unknown", wantReason: "-",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			trafficMap := trafficMapFixture("checkout", "team-a")
			for index := range trafficMap.Status.Conditions {
				condition := &trafficMap.Status.Conditions[index]
				if condition.Type == v1beta1.TrafficMapOverrideActive {
					condition.Status = test.status
					condition.Reason = test.reason
					condition.ObservedGeneration = test.generation
				}
			}

			values := trafficMapColumnValues(t, trafficMap)
			assert.Equal(t, test.wantStatus, values["OVERRIDE"])
			assert.Equal(t, test.wantReason, values["OVERRIDE-REASON"])
		})
	}
}

func TestTrafficMapColumnsAreWrongTypeSafe(t *testing.T) {
	values := trafficMapColumnValues(t, &v1beta1.InferenceService{})
	for name, value := range values {
		assert.Equal(t, "?", value, name)
	}
}

// Adding operational evidence to the default table would regress the
// terminal-width constraint; wide is the explicit opt-in for that detail.
func TestGetTrafficMapCompactAndWideOutput(t *testing.T) {
	f := factory.Static{OME: omefake.NewSimpleClientset(trafficMapFixture("checkout", "team-a")), NS: "team-a"}

	var compactOutput trafficMapTerminalBuffer
	var compactErrors bytes.Buffer
	command := NewCmd(f, genericiooptions.IOStreams{Out: &compactOutput, ErrOut: &compactErrors})
	command.SetArgs([]string{"trafficmaps"})
	err := command.Execute()
	require.NoError(t, err)
	assert.Empty(t, compactErrors.String())
	compact := compactOutput.String()
	compactLines := strings.Split(strings.TrimSpace(compact), "\n")
	require.Len(t, compactLines, 2)
	assert.Equal(t, []string{"NAME", "MODE", "TARGETS", "ROUTABLE", "PUBLISHED", "AGE"}, strings.Fields(compactLines[0]))
	for _, line := range compactLines {
		assert.LessOrEqual(t, len([]rune(line)), 80, line)
	}
	assert.NotContains(t, compact, "PUBLISHER-FRESHNESS")
	assert.NotContains(t, compact, "gateway.networking.k8s.io")

	wide, err := execute(t, f, "tm", "-o", "wide")
	require.NoError(t, err)
	for _, header := range []string{"SERVICE", "ACTIVE", "HEALTHY", "OVERRIDE", "OVERRIDE-REASON", "REASON", "GATEWAY", "PUBLISHER-FRESHNESS"} {
		assert.Contains(t, strings.Fields(strings.Split(strings.TrimSpace(wide), "\n")[0]), header)
	}
	assert.Contains(t, wide, "gateway.networking.k8s.io/HTTPRoute:team-a/checkout")
}

func TestGetTrafficMapNamespaceSelectorAndMachineFormats(t *testing.T) {
	teamA := trafficMapFixture("checkout", "team-a")
	teamB := trafficMapFixture("checkout", "team-b")
	teamB.Labels = map[string]string{"tier": "silver"}
	f := factory.Static{OME: omefake.NewSimpleClientset(teamA, teamB), NS: "team-a"}

	currentNamespace, err := execute(t, f, "tmap", "-l", "tier=gold")
	require.NoError(t, err)
	assert.Equal(t, 1, strings.Count(currentNamespace, "checkout"), currentNamespace)
	assert.NotContains(t, strings.Fields(strings.Split(strings.TrimSpace(currentNamespace), "\n")[0]), "NAMESPACE")

	allNamespaces, err := execute(t, f, "trafficmap", "-A")
	require.NoError(t, err)
	assert.Equal(t, []string{"NAMESPACE", "NAME", "MODE", "TARGETS", "ROUTABLE", "PUBLISHED", "AGE"}, strings.Fields(strings.Split(strings.TrimSpace(allNamespaces), "\n")[0]))
	assert.Equal(t, 2, strings.Count(allNamespaces, "checkout"), allNamespaces)
	assert.Contains(t, allNamespaces, "team-a")
	assert.Contains(t, allNamespaces, "team-b")

	jsonOutput, err := execute(t, f, "tm", "checkout", "-o", "json")
	require.NoError(t, err)
	assert.Contains(t, jsonOutput, `"kind": "TrafficMap"`)
	assert.Contains(t, jsonOutput, `"mode": "Split"`)
	assert.Contains(t, jsonOutput, `"observedTrafficMapGeneration": 4`)

	yamlOutput, err := execute(t, f, "trafficmaps", "checkout", "-o", "yaml")
	require.NoError(t, err)
	assert.Contains(t, yamlOutput, "kind: TrafficMap")
	assert.Contains(t, yamlOutput, "mode: Split")
	assert.Contains(t, yamlOutput, "observedTrafficMapGeneration: 4")
}

type trafficMapGetCall struct {
	namespace string
	name      string
}

type recordingTrafficMapClient struct {
	typedome.TrafficMapInterface
	namespace string
	listCalls []metav1.ListOptions
	getCalls  []trafficMapGetCall
	list      func(metav1.ListOptions) (*v1beta1.TrafficMapList, error)
	get       func(string) (*v1beta1.TrafficMap, error)
}

func (client *recordingTrafficMapClient) List(_ context.Context, options metav1.ListOptions) (*v1beta1.TrafficMapList, error) {
	client.listCalls = append(client.listCalls, options)
	if client.list == nil {
		return nil, errors.New("unexpected TrafficMap list")
	}
	return client.list(options)
}

func (client *recordingTrafficMapClient) Get(_ context.Context, name string, _ metav1.GetOptions) (*v1beta1.TrafficMap, error) {
	if client.get == nil {
		return nil, errors.New("unexpected TrafficMap get")
	}
	client.getCalls = append(client.getCalls, trafficMapGetCall{namespace: client.namespace, name: name})
	result, err := client.get(name)
	return result, err
}

type recordingTrafficMapV1beta1 struct {
	typedome.OmeV1beta1Interface
	client     *recordingTrafficMapClient
	namespaces []string
}

func (client *recordingTrafficMapV1beta1) TrafficMaps(namespace string) typedome.TrafficMapInterface {
	client.namespaces = append(client.namespaces, namespace)
	client.client.namespace = namespace
	return client.client
}

type recordingTrafficMapClientset struct {
	versioned.Interface
	client *recordingTrafficMapV1beta1
}

func (client *recordingTrafficMapClientset) OmeV1beta1() typedome.OmeV1beta1Interface {
	return client.client
}

func trafficMapFactory(client *recordingTrafficMapClient, namespace string) (factory.Factory, *recordingTrafficMapV1beta1) {
	v1Client := &recordingTrafficMapV1beta1{client: client}
	return factory.Static{OME: &recordingTrafficMapClientset{client: v1Client}, NS: namespace}, v1Client
}

// Replacing named GET with a list-and-filter would make the command more
// expensive and weaken Kubernetes authorization and not-found semantics.
func TestGetTrafficMapNameUsesExactTypedGet(t *testing.T) {
	client := &recordingTrafficMapClient{}
	client.get = func(name string) (*v1beta1.TrafficMap, error) {
		return trafficMapFixture(name, "team-a"), nil
	}
	f, v1Client := trafficMapFactory(client, "team-a")

	output, err := execute(t, f, "tm", "checkout")
	require.NoError(t, err)
	assert.Contains(t, output, "checkout")
	assert.Equal(t, []string{"team-a"}, v1Client.namespaces)
	assert.Equal(t, []trafficMapGetCall{{namespace: "team-a", name: "checkout"}}, client.getCalls)
	assert.Empty(t, client.listCalls)
}

// Keeping the stale prefix after an expired continue token would mix two API
// snapshots. The retained output must come only from the single clean retry.
func TestGetTrafficMapListRestartsExpiredSnapshotOnce(t *testing.T) {
	client := &recordingTrafficMapClient{}
	call := 0
	client.list = func(options metav1.ListOptions) (*v1beta1.TrafficMapList, error) {
		call++
		switch call {
		case 1:
			return &v1beta1.TrafficMapList{ListMeta: metav1.ListMeta{Continue: "old-token"}, Items: []v1beta1.TrafficMap{*trafficMapFixture("stale-prefix", "team-a")}}, nil
		case 2:
			return nil, apierrors.NewResourceExpired("private continuation detail")
		case 3:
			return &v1beta1.TrafficMapList{ListMeta: metav1.ListMeta{Continue: "new-token"}, Items: []v1beta1.TrafficMap{*trafficMapFixture("fresh-a", "team-a")}}, nil
		case 4:
			return &v1beta1.TrafficMapList{Items: []v1beta1.TrafficMap{*trafficMapFixture("fresh-b", "team-a")}}, nil
		default:
			return nil, fmt.Errorf("unexpected request %d", call)
		}
	}
	f, v1Client := trafficMapFactory(client, "team-a")

	output, err := execute(t, f, "trafficmaps", "-l", "tier=gold")
	require.NoError(t, err)
	assert.NotContains(t, output, "stale-prefix")
	assert.Contains(t, output, "fresh-a")
	assert.Contains(t, output, "fresh-b")
	assert.Equal(t, []string{"team-a"}, v1Client.namespaces)
	require.Len(t, client.listCalls, 4)
	assert.Equal(t, []string{"", "old-token", "", "new-token"}, []string{
		client.listCalls[0].Continue,
		client.listCalls[1].Continue,
		client.listCalls[2].Continue,
		client.listCalls[3].Continue,
	})
	for _, options := range client.listCalls {
		assert.Equal(t, int64(paging.ChunkSize), options.Limit)
		assert.Equal(t, "tier=gold", options.LabelSelector)
	}
}

func TestGetTrafficMapNamedErrorIsPrivacySafe(t *testing.T) {
	secret := "xoxb-traffic-map-private-token"
	client := &recordingTrafficMapClient{}
	client.get = func(string) (*v1beta1.TrafficMap, error) {
		return nil, apierrors.NewForbidden(
			v1beta1.Resource("trafficmaps"),
			"checkout",
			errors.New("publisher rejected Bearer "+secret),
		)
	}
	f, _ := trafficMapFactory(client, "team-a")

	stdout, stderr, err := executeSeparate(t, f, "tm", "checkout", "-o", "json")
	require.Error(t, err)
	assert.Equal(t, "get trafficmaps team-a/checkout: Forbidden", err.Error())
	assert.Empty(t, stdout)
	assert.Empty(t, stderr)
	assert.True(t, apierrors.IsForbidden(err))
	assert.NotContains(t, err.Error(), secret)
}
