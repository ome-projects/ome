package v1alpha1_test

import (
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"

	"sigs.k8s.io/ome/pkg/cli/report"
	"sigs.k8s.io/ome/pkg/cli/report/v1alpha1"
)

func TestNewEnvelopeUsesVersionedTypedDefaultsAndInjectedTime(t *testing.T) {
	location := time.FixedZone("test", -7*60*60)
	now := time.Date(2026, time.August, 31, 11, 45, 0, 0, location)

	got := v1alpha1.NewEnvelope(
		"InstanceReport",
		v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		diagnosticContent{},
		fixedClock{now: now},
	)

	assert.Equal(t, v1alpha1.APIVersion, got.APIVersion)
	assert.Equal(t, "InstanceReport", got.Kind)
	assert.Equal(t, v1alpha1.Metadata{Namespace: "prod", Name: "chat"}, got.Metadata)
	assert.Equal(t, "2026-08-31T18:45:00Z", got.CollectedAt.Format(time.RFC3339))
	assert.NotNil(t, got.Sources)
	assert.NotNil(t, got.Warnings)
	assert.NotNil(t, got.Content.Rows)
}

func TestEnvelopeCanonicalReturnsSortedCopy(t *testing.T) {
	collectedAt := time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC)
	reportValue := v1alpha1.Envelope[diagnosticContent]{
		APIVersion:  "incorrect.example/v9",
		Kind:        "InstanceReport",
		Metadata:    v1alpha1.Metadata{Namespace: "prod", Name: "chat"},
		CollectedAt: collectedAt,
		Sources: []v1alpha1.SourceReference{
			{Kind: "Pod", Namespace: "prod", Name: "z", Evidence: v1alpha1.EvidenceObserved},
			{Kind: "InferenceService", Namespace: "prod", Name: "chat", Evidence: v1alpha1.EvidenceReported},
			{Kind: "Pod", Namespace: "prod", Name: "a", Evidence: v1alpha1.EvidenceObserved},
		},
		Warnings: []v1alpha1.Warning{
			{Code: v1alpha1.WarningTruncated, Message: "later rows omitted"},
			{Code: v1alpha1.WarningPartialData, Message: "events forbidden"},
		},
		Content: diagnosticContent{Rows: []diagnosticRow{{Name: "z"}, {Name: "a"}}},
	}

	got := reportValue.Canonical()

	assert.Equal(t, v1alpha1.APIVersion, got.APIVersion)
	assert.Equal(t, []string{"InferenceService/chat", "Pod/a", "Pod/z"}, sourceNames(got.Sources))
	assert.Equal(t, []v1alpha1.WarningCode{v1alpha1.WarningPartialData, v1alpha1.WarningTruncated}, warningCodes(got.Warnings))
	assert.Equal(t, []diagnosticRow{{Name: "a"}, {Name: "z"}}, got.Content.Rows)
	for _, source := range got.Sources {
		assert.Equal(t, collectedAt, source.CollectedAt)
	}

	assert.Equal(t, []string{"Pod/z", "InferenceService/chat", "Pod/a"}, sourceNames(reportValue.Sources))
	assert.Equal(t, []diagnosticRow{{Name: "z"}, {Name: "a"}}, reportValue.Content.Rows)
}

func TestEnvelopeCanonicalNormalizesNilCollections(t *testing.T) {
	reportValue := v1alpha1.Envelope[diagnosticContent]{
		Kind:    "InstanceReport",
		Content: diagnosticContent{},
	}

	got := reportValue.Canonical()

	assert.NotNil(t, got.Sources)
	assert.NotNil(t, got.Warnings)
	assert.NotNil(t, got.Content.Rows)
}

func TestEnvelopeCanonicalOrdersEverySourceIdentityField(t *testing.T) {
	base := v1alpha1.SourceReference{
		Kind:              "Pod",
		Namespace:         "prod",
		Name:              "chat",
		UID:               "uid-b",
		Generation:        2,
		ResourceVersion:   "2",
		Evidence:          v1alpha1.EvidenceObserved,
		CollectedAt:       time.Date(2026, time.August, 31, 18, 1, 0, 0, time.UTC),
		UnavailableReason: v1alpha1.UnavailableNotFound,
	}
	tests := []struct {
		name  string
		early v1alpha1.SourceReference
		late  v1alpha1.SourceReference
	}{
		{name: "kind", early: withSource(base, func(s *v1alpha1.SourceReference) { s.Kind = "InferenceService" }), late: base},
		{name: "namespace", early: withSource(base, func(s *v1alpha1.SourceReference) { s.Namespace = "a" }), late: base},
		{name: "name", early: withSource(base, func(s *v1alpha1.SourceReference) { s.Name = "a" }), late: base},
		{name: "uid", early: withSource(base, func(s *v1alpha1.SourceReference) { s.UID = "uid-a" }), late: base},
		{name: "generation", early: withSource(base, func(s *v1alpha1.SourceReference) { s.Generation = 1 }), late: base},
		{name: "resource version", early: withSource(base, func(s *v1alpha1.SourceReference) { s.ResourceVersion = "1" }), late: base},
		{name: "evidence", early: withSource(base, func(s *v1alpha1.SourceReference) { s.Evidence = v1alpha1.EvidenceDeclared }), late: base},
		{name: "collected at", early: withSource(base, func(s *v1alpha1.SourceReference) { s.CollectedAt = s.CollectedAt.Add(-time.Minute) }), late: base},
		{name: "unavailable reason", early: withSource(base, func(s *v1alpha1.SourceReference) { s.UnavailableReason = v1alpha1.UnavailableForbidden }), late: base},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reportValue := v1alpha1.Envelope[diagnosticContent]{
				Kind:        "InstanceReport",
				CollectedAt: base.CollectedAt,
				Sources:     []v1alpha1.SourceReference{tt.late, tt.early},
				Content:     diagnosticContent{},
			}

			got := reportValue.Canonical()

			require.Len(t, got.Sources, 2)
			assert.Equal(t, tt.early, got.Sources[0])
			assert.Equal(t, tt.late, got.Sources[1])
		})
	}
}

func TestEnvelopeCanonicalNormalizesSourceTimezoneAndWarningMessageOrder(t *testing.T) {
	location := time.FixedZone("test", 2*60*60)
	sourceTime := time.Date(2026, time.August, 31, 20, 0, 0, 0, location)
	reportValue := v1alpha1.Envelope[diagnosticContent]{
		Kind: "InstanceReport",
		Sources: []v1alpha1.SourceReference{
			{Kind: "Pod", Name: "chat", CollectedAt: sourceTime},
		},
		Warnings: []v1alpha1.Warning{
			{Code: v1alpha1.WarningPartialData, Message: "z"},
			{Code: v1alpha1.WarningPartialData, Message: "a"},
		},
		Content: diagnosticContent{},
	}

	got := reportValue.Canonical()

	assert.Equal(t, time.UTC, got.Sources[0].CollectedAt.Location())
	assert.Equal(t, []string{"a", "z"}, []string{got.Warnings[0].Message, got.Warnings[1].Message})
	assert.Equal(t, "test", reportValue.Sources[0].CollectedAt.Location().String())
}

func TestEnvelopeTableUsesTypedContent(t *testing.T) {
	reportValue := v1alpha1.Envelope[diagnosticContent]{
		Content: diagnosticContent{Rows: []diagnosticRow{{Name: "chat"}}},
	}

	assert.Equal(t, report.Table{Headers: []string{"NAME"}, Rows: [][]string{{"chat"}}}, reportValue.Table())
}

func TestClockImplementations(t *testing.T) {
	want := time.Date(2026, time.August, 31, 18, 0, 0, 0, time.UTC)
	clock := v1alpha1.ClockFunc(func() time.Time { return want })
	assert.Equal(t, want, clock.Now())

	before := time.Now()
	got := (v1alpha1.SystemClock{}).Now()
	after := time.Now()
	assert.False(t, got.Before(before))
	assert.False(t, got.After(after))
}

func TestConstructorsAcceptDefaultSystemClock(t *testing.T) {
	before := time.Now().UTC()
	envelope := v1alpha1.NewEnvelope("InstanceReport", v1alpha1.Metadata{}, diagnosticContent{}, nil)
	action := v1alpha1.NewActionResult("pause", v1alpha1.ActionTarget{}, v1alpha1.DryRunNone, nil)
	after := time.Now().UTC()

	for _, got := range []time.Time{envelope.CollectedAt, action.CollectedAt} {
		assert.False(t, got.Before(before))
		assert.False(t, got.After(after))
	}
}

func TestV1Alpha1EnvelopeSchemaHasNoUnstructuredOrSecretBearingFields(t *testing.T) {
	assertTypedSchema(t, reflect.TypeOf(v1alpha1.Envelope[diagnosticContent]{}), map[reflect.Type]bool{})
}

func TestSchemaGuardRecognizesForbiddenNamesAndTypes(t *testing.T) {
	typ := reflect.TypeOf(struct {
		Environment []string                  `json:"safeName"`
		Selector    *corev1.SecretKeySelector `json:"selector"`
	}{})

	assert.True(t, isForbiddenSchemaField(typ.Field(0)))
	assert.True(t, isForbiddenSchemaField(typ.Field(1)))
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

type diagnosticContent struct {
	Rows []diagnosticRow `json:"rows"`
}

func (c diagnosticContent) Canonical() diagnosticContent {
	result := c
	result.Rows = append([]diagnosticRow{}, c.Rows...)
	sort.Slice(result.Rows, func(i, j int) bool {
		return result.Rows[i].Name < result.Rows[j].Name
	})
	return result
}

func (c diagnosticContent) Table() report.Table {
	table := report.Table{Headers: []string{"NAME"}, Rows: make([][]string, 0, len(c.Rows))}
	for _, row := range c.Rows {
		table.Rows = append(table.Rows, []string{row.Name})
	}
	return table
}

type diagnosticRow struct {
	Name string `json:"name"`
}

func sourceNames(sources []v1alpha1.SourceReference) []string {
	result := make([]string, 0, len(sources))
	for _, source := range sources {
		result = append(result, source.Kind+"/"+source.Name)
	}
	return result
}

func warningCodes(warnings []v1alpha1.Warning) []v1alpha1.WarningCode {
	result := make([]v1alpha1.WarningCode, 0, len(warnings))
	for _, warning := range warnings {
		result = append(result, warning.Code)
	}
	return result
}

func withSource(source v1alpha1.SourceReference, change func(*v1alpha1.SourceReference)) v1alpha1.SourceReference {
	change(&source)
	return source
}

func assertTypedSchema(t *testing.T, typ reflect.Type, seen map[reflect.Type]bool) {
	t.Helper()
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if typ.PkgPath() == "time" || seen[typ] {
		return
	}
	seen[typ] = true

	require.False(t, isForbiddenSchemaType(typ), "schema contains forbidden type %s", typ)
	require.NotEqual(t, reflect.Interface, typ.Kind(), "schema contains interface field at %s", typ)
	require.NotEqual(t, reflect.Map, typ.Kind(), "schema contains map field at %s", typ)
	if typ.Kind() != reflect.Struct {
		return
	}
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		assert.False(t, isForbiddenSchemaField(field), "schema contains forbidden field %s.%s", typ, field.Name)
		assertTypedSchema(t, field.Type, seen)
	}
}

func isForbiddenSchemaField(field reflect.StructField) bool {
	jsonName := strings.Split(field.Tag.Get("json"), ",")[0]
	// The scale source is a typed fixed provenance label, not a payload spec.
	if field.Name == "SpecSource" && jsonName == "specSource" && field.Type == reflect.TypeOf(v1alpha1.ScaleSpecSource("")) {
		return false
	}
	name := strings.ToLower(field.Name + " " + jsonName)
	for _, fragment := range []string{
		"annotation", "detail", "environment", "envfrom", "header",
		"secret", "spec", "template",
	} {
		if strings.Contains(name, fragment) {
			return true
		}
	}
	return isForbiddenSchemaType(field.Type)
}

func isForbiddenSchemaType(typ reflect.Type) bool {
	for typ.Kind() == reflect.Pointer || typ.Kind() == reflect.Slice || typ.Kind() == reflect.Array {
		typ = typ.Elem()
	}
	if !strings.HasPrefix(typ.PkgPath(), "k8s.io/") {
		return false
	}
	name := strings.ToLower(typ.Name())
	for _, fragment := range []string{"envvar", "rawextension", "secret", "spec", "template"} {
		if strings.Contains(name, fragment) {
			return true
		}
	}
	return false
}
