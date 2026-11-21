package coordination

import (
	"reflect"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/ptr"
)

// serviceWithEveryOwnedLeafSet is a Service with a value in every leaf the
// writer owns: labels, one owner reference with both flags, a selector, one
// port with every field set, and the publish flag.
func serviceWithEveryOwnedLeafSet() *corev1.Service {
	return &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "fleet-engine-rev-hash1", Namespace: "prod",
			Labels: map[string]string{"component": "engine", "ome.io/revision-hash": "hash1"},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion: "ome.io/v1beta1", Kind: "InferenceService", Name: "fleet", UID: "fleet-uid",
				Controller: ptr.To(true), BlockOwnerDeletion: ptr.To(true),
			}},
		},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"component": "engine", "ome.io/runner": "leader"},
			Ports: []corev1.ServicePort{{
				Name: "http", Protocol: corev1.ProtocolTCP, AppProtocol: ptr.To("kubernetes.io/h2c"),
				Port: 8000, TargetPort: intstr.FromString("http"), NodePort: 31000,
			}},
			PublishNotReadyAddresses: true,
		},
	}
}

// ownedLeafChange changes one leaf of the owned fields of a Service.
type ownedLeafChange struct {
	path   string
	change func(*corev1.Service)
}

// ownedLeafChanges are the changes to a Service with every owned leaf set:
// each map emptied, grown, changed in one value and shrunk; each list
// emptied and grown; each scalar and pointer of an owner reference and of a
// port moved, cleared or filled; the flag flipped.
func ownedLeafChanges() []ownedLeafChange {
	return []ownedLeafChange{
		{"labels=nil", func(s *corev1.Service) { s.Labels = nil }},
		{"labels+1", func(s *corev1.Service) { s.Labels["added"] = "" }},
		{"labels[component]", func(s *corev1.Service) { s.Labels["component"] = "decoder" }},
		{"labels-component", func(s *corev1.Service) { delete(s.Labels, "component") }},
		{"ownerReferences=nil", func(s *corev1.Service) { s.OwnerReferences = nil }},
		{"ownerReferences+1", func(s *corev1.Service) { s.OwnerReferences = append(s.OwnerReferences, metav1.OwnerReference{}) }},
		{"ownerReferences[0].apiVersion", func(s *corev1.Service) { s.OwnerReferences[0].APIVersion += "x" }},
		{"ownerReferences[0].kind", func(s *corev1.Service) { s.OwnerReferences[0].Kind += "x" }},
		{"ownerReferences[0].name", func(s *corev1.Service) { s.OwnerReferences[0].Name += "x" }},
		{"ownerReferences[0].uid", func(s *corev1.Service) { s.OwnerReferences[0].UID += "x" }},
		{"ownerReferences[0].controller=nil", func(s *corev1.Service) { s.OwnerReferences[0].Controller = nil }},
		{"ownerReferences[0].controller=false", func(s *corev1.Service) { s.OwnerReferences[0].Controller = ptr.To(false) }},
		{"ownerReferences[0].blockOwnerDeletion=nil", func(s *corev1.Service) { s.OwnerReferences[0].BlockOwnerDeletion = nil }},
		{"ownerReferences[0].blockOwnerDeletion=false", func(s *corev1.Service) { s.OwnerReferences[0].BlockOwnerDeletion = ptr.To(false) }},
		{"selector=nil", func(s *corev1.Service) { s.Spec.Selector = nil }},
		{"selector+1", func(s *corev1.Service) { s.Spec.Selector["added"] = "" }},
		{"selector[component]", func(s *corev1.Service) { s.Spec.Selector["component"] = "decoder" }},
		{"selector-component", func(s *corev1.Service) { delete(s.Spec.Selector, "component") }},
		{"ports=nil", func(s *corev1.Service) { s.Spec.Ports = nil }},
		{"ports+1", func(s *corev1.Service) { s.Spec.Ports = append(s.Spec.Ports, corev1.ServicePort{}) }},
		{"ports[0].name", func(s *corev1.Service) { s.Spec.Ports[0].Name += "x" }},
		{"ports[0].protocol", func(s *corev1.Service) { s.Spec.Ports[0].Protocol = corev1.ProtocolUDP }},
		{"ports[0].appProtocol=nil", func(s *corev1.Service) { s.Spec.Ports[0].AppProtocol = nil }},
		{"ports[0].appProtocol", func(s *corev1.Service) { s.Spec.Ports[0].AppProtocol = ptr.To("http") }},
		{"ports[0].port", func(s *corev1.Service) { s.Spec.Ports[0].Port++ }},
		{"ports[0].targetPort.type", func(s *corev1.Service) { s.Spec.Ports[0].TargetPort = intstr.FromInt32(8000) }},
		{"ports[0].targetPort.strVal", func(s *corev1.Service) { s.Spec.Ports[0].TargetPort = intstr.FromString("web") }},
		{"ports[0].targetPort.intVal", func(s *corev1.Service) {
			s.Spec.Ports[0].TargetPort = intstr.FromInt32(8000)
		}},
		{"ports[0].nodePort", func(s *corev1.Service) { s.Spec.Ports[0].NodePort++ }},
		{"publishNotReadyAddresses", func(s *corev1.Service) { s.Spec.PublishNotReadyAddresses = false }},
	}
}

// The compare answers as the semantic compare of the whole object answers
// on every leaf of the owned fields: a Service that differs in one leaf
// reads as different, and a Service reads as equal to its own copy. The
// leaves are listed by hand, so the field counts of the types they belong
// to are held: a field added to a port, an owner reference, the target port
// or the owned set fails this test until the compare reads it and the list
// changes it.
func TestPerRevisionServiceFieldsEqual_AgreesWithTheSemanticCompareOnEveryOwnedLeaf(t *testing.T) {
	for name, want := range map[string]struct {
		typ    reflect.Type
		fields int
	}{
		"a port":             {reflect.TypeOf(corev1.ServicePort{}), 6},
		"an owner reference": {reflect.TypeOf(metav1.OwnerReference{}), 6},
		"a target port":      {reflect.TypeOf(intstr.IntOrString{}), 3},
		"the owned fields":   {reflect.TypeOf(perRevisionServiceFields{}), 5},
	} {
		if got := want.typ.NumField(); got != want.fields {
			t.Fatalf("%s has %d fields, this test knows %d; read the new field in the compare and change it here", name, got, want.fields)
		}
	}
	base := serviceWithEveryOwnedLeafSet()
	for _, leaf := range ownedLeafChanges() {
		changed := base.DeepCopy()
		leaf.change(changed)
		if equality.Semantic.DeepEqual(base, changed) {
			t.Errorf("%s: the semantic compare reads the changed Service as equal; the change is not one", leaf.path)
		}
		if perRevisionServiceFieldsOf(base).equal(perRevisionServiceFieldsOf(changed)) {
			t.Errorf("%s: equal() = true, the semantic compare says false", leaf.path)
		}
		if !perRevisionServiceFieldsOf(changed).equal(perRevisionServiceFieldsOf(changed.DeepCopy())) {
			t.Errorf("%s: the changed Service and its copy read as different", leaf.path)
		}
	}
	if !perRevisionServiceFieldsOf(base).equal(perRevisionServiceFieldsOf(base.DeepCopy())) {
		t.Errorf("a Service and its copy read as different")
	}
}

// A change outside the owned fields leaves the compare at equal, as the
// writer's update leaves those fields: the annotations, the cluster IP,
// the type, the apiserver's defaults, the status and the identity.
func TestPerRevisionServiceFieldsEqual_ReadsOnlyTheOwnedFields(t *testing.T) {
	base := serviceWithEveryOwnedLeafSet()
	for name, change := range map[string]func(*corev1.Service){
		"an annotation":        func(s *corev1.Service) { s.Annotations = map[string]string{"example.com/note": "kept"} },
		"the cluster IP":       func(s *corev1.Service) { s.Spec.ClusterIP, s.Spec.ClusterIPs = "10.0.0.9", []string{"10.0.0.9"} },
		"the type":             func(s *corev1.Service) { s.Spec.Type = corev1.ServiceTypeNodePort },
		"the session affinity": func(s *corev1.Service) { s.Spec.SessionAffinity = corev1.ServiceAffinityClientIP },
		"the status": func(s *corev1.Service) {
			s.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: "203.0.113.7"}}
		},
		"the resource version": func(s *corev1.Service) { s.ResourceVersion = "8" },
	} {
		changed := base.DeepCopy()
		change(changed)
		if !perRevisionServiceFieldsOf(base).equal(perRevisionServiceFieldsOf(changed)) {
			t.Errorf("%s changed: equal() = false over a field the writer does not own", name)
		}
	}
}

// The semantic compare reads a nil map and an empty one as the same, and a
// nil list and an empty one; an app protocol set on one side alone is a
// difference. The compare agrees on each.
func TestPerRevisionServiceFieldsEqual_ReadsNilAndEmptyAsEqual(t *testing.T) {
	left, right := serviceWithEveryOwnedLeafSet(), serviceWithEveryOwnedLeafSet()
	left.Labels, right.Labels = nil, map[string]string{}
	left.Spec.Selector, right.Spec.Selector = map[string]string{}, nil
	left.OwnerReferences, right.OwnerReferences = []metav1.OwnerReference{}, nil
	left.Spec.Ports, right.Spec.Ports = nil, []corev1.ServicePort{}
	if !equality.Semantic.DeepEqual(left, right) {
		t.Fatalf("the semantic compare reads the two Services as different; the fixture is wrong")
	}
	if !perRevisionServiceFieldsOf(left).equal(perRevisionServiceFieldsOf(right)) {
		t.Errorf("equal() = false over nil against empty maps and lists; the semantic compare says true")
	}
	left, right = serviceWithEveryOwnedLeafSet(), serviceWithEveryOwnedLeafSet()
	right.Spec.Ports[0].AppProtocol = ptr.To("")
	if equality.Semantic.DeepEqual(left, right) || perRevisionServiceFieldsOf(left).equal(perRevisionServiceFieldsOf(right)) {
		t.Errorf("an empty app protocol and an absent one read as equal")
	}
}
