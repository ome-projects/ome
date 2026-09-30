package controllerconfig

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	authenticationv1 "k8s.io/api/authentication/v1"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"sigs.k8s.io/ome/pkg/constants"
)

// inferenceReplicaBlock is a ConfigMap holding only the given inferenceReplica
// block.
func inferenceReplicaBlock(raw string) *v1.ConfigMap {
	return &v1.ConfigMap{Data: map[string]string{InferenceReplicaConfigName: raw}}
}

func TestParseInferenceReplicaConfig(t *testing.T) {
	cfg, err := parseInferenceReplicaConfig(inferenceReplicaBlock(
		`{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager"],"groups":["system:masters"]}}`))
	require.NoError(t, err)
	id := cfg.Identity()
	require.True(t, id.Configured())
	assert.True(t, id.Matches(authenticationv1.UserInfo{Username: "system:serviceaccount:ome:ome-controller-manager"}), "username match")
	assert.True(t, id.Matches(authenticationv1.UserInfo{Username: "default", Groups: []string{"system:authenticated", "system:masters"}}), "group match")
	assert.False(t, id.Matches(authenticationv1.UserInfo{Username: "example-user", Groups: []string{"system:authenticated"}}), "unrelated user")
}

// An absent or blank key yields no config; an empty object or an empty
// controllerIdentity yields an unconfigured identity. Neither matches anyone.
func TestParseInferenceReplicaConfigUnconfigured(t *testing.T) {
	anyone := authenticationv1.UserInfo{Username: "example-user", Groups: []string{"system:authenticated"}}

	for name, cm := range map[string]*v1.ConfigMap{
		"absent key":          {},
		"whitespace-only key": inferenceReplicaBlock(" \n\t"),
	} {
		t.Run(name, func(t *testing.T) {
			cfg, err := parseInferenceReplicaConfig(cm)
			require.NoError(t, err)
			assert.Nil(t, cfg)
		})
	}
	for _, raw := range []string{`{}`, `{"controllerIdentity":{}}`, `{"controllerIdentity":{"usernames":[],"groups":[]}}`} {
		t.Run(raw, func(t *testing.T) {
			cfg, err := parseInferenceReplicaConfig(inferenceReplicaBlock(raw))
			require.NoError(t, err)
			require.NotNil(t, cfg)
			assert.False(t, cfg.Identity().Configured())
			assert.False(t, cfg.Identity().Matches(anyone))
		})
	}
	t.Run("nil config", func(t *testing.T) {
		var none *InferenceReplicaConfig
		assert.Nil(t, none.Identity())
		assert.False(t, none.Identity().Configured())
		assert.False(t, none.Identity().Matches(anyone))
	})
}

// A block the parser cannot read exactly as written is an error, never a
// silently narrower, wider or empty identity.
func TestParseInferenceReplicaConfigRejects(t *testing.T) {
	for _, tc := range []struct {
		name    string
		raw     string
		wantErr string
	}{
		{name: "top-level null", raw: `null`, wantErr: "expected a JSON object"},
		{name: "top-level list", raw: `["group-a"]`, wantErr: "expected a JSON object"},
		{name: "unknown top-level key", raw: `{"identity":{}}`, wantErr: `unknown field "identity"`},
		{name: "top-level key in another case", raw: `{"ControllerIdentity":{"groups":["group-a"]}}`, wantErr: `unknown field "ControllerIdentity"`},
		{name: "unknown identity key", raw: `{"controllerIdentity":{"user":"x"}}`, wantErr: `unknown field "user"`},
		{name: "duplicate top-level key", raw: `{"controllerIdentity":{"groups":["group-a"]},"controllerIdentity":{}}`, wantErr: `duplicate key "controllerIdentity"`},
		{name: "duplicate identity key", raw: `{"controllerIdentity":{"groups":["group-a"],"groups":[]}}`, wantErr: `duplicate key "groups"`},
		{name: "null identity", raw: `{"controllerIdentity":null}`, wantErr: "controllerIdentity: expected a JSON object"},
		{name: "identity not an object", raw: `{"controllerIdentity":["group-a"]}`, wantErr: "controllerIdentity: expected a JSON object"},
		{name: "usernames not a list", raw: `{"controllerIdentity":{"usernames":"example-user"}}`, wantErr: "cannot unmarshal"},
		{name: "trailing object", raw: `{"controllerIdentity":{"groups":["group-a"]}} {"controllerIdentity":{}}`, wantErr: "trailing data"},
		{name: "blank username", raw: `{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager"," "]}}`, wantErr: "controllerIdentity.usernames[1] is blank"},
		{name: "null username", raw: `{"controllerIdentity":{"usernames":[null]}}`, wantErr: "controllerIdentity.usernames[0] is blank"},
		{name: "empty group", raw: `{"controllerIdentity":{"groups":[""]}}`, wantErr: "controllerIdentity.groups[0] is blank"},
		{name: "username with surrounding whitespace", raw: `{"controllerIdentity":{"usernames":["system:serviceaccount:ome:ome-controller-manager"," system:serviceaccount:ome:custom"]}}`, wantErr: "controllerIdentity.usernames[1] has surrounding whitespace"},
		{name: "group with surrounding whitespace", raw: `{"controllerIdentity":{"groups":["group-a\t"]}}`, wantErr: "controllerIdentity.groups[0] has surrounding whitespace"},
		{name: "every authenticated caller", raw: `{"controllerIdentity":{"groups":["group-a","system:authenticated"]}}`, wantErr: `group "system:authenticated" names every caller`},
		{name: "every unauthenticated caller", raw: `{"controllerIdentity":{"groups":["system:unauthenticated"]}}`, wantErr: `group "system:unauthenticated" names every caller`},
		{name: "every service account", raw: `{"controllerIdentity":{"groups":["system:serviceaccounts"]}}`, wantErr: `group "system:serviceaccounts" names every caller`},
		{name: "the anonymous user", raw: `{"controllerIdentity":{"usernames":["system:anonymous"]}}`, wantErr: `username "system:anonymous" names every caller`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := parseInferenceReplicaConfig(inferenceReplicaBlock(tc.raw))
			require.ErrorContains(t, err, "invalid inferenceReplica config: ")
			assert.ErrorContains(t, err, tc.wantErr)
			assert.Nil(t, cfg)
		})
	}
}

// TestNewInferenceReplicaConfigCached verifies the loader the admission
// webhook calls per request: loads share one apiserver GET within the TTL, an
// edit applies after it, and a missing ConfigMap is an error.
func TestNewInferenceReplicaConfigCached(t *testing.T) {
	t.Run("hit: repeated loads share one GET within TTL", func(t *testing.T) {
		clientset, gets := countingClientset(t, map[string]string{
			InferenceReplicaConfigName: `{"controllerIdentity":{"groups":["group-a"]}}`,
		})
		fakeNow := time.Unix(0, 0)
		cache := &ConfigCache{ttl: 30 * time.Second, now: func() time.Time { return fakeNow }}

		cfg, err := NewInferenceReplicaConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, []string{"group-a"}, cfg.Identity().Groups)

		_, err = NewInferenceReplicaConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, int64(1), atomic.LoadInt64(gets), "both loads must share one apiserver GET")
	})

	t.Run("expiry picks up a ConfigMap edit without restart", func(t *testing.T) {
		clientset, _ := countingClientset(t, map[string]string{
			InferenceReplicaConfigName: `{"controllerIdentity":{"groups":["group-a"]}}`,
		})
		fakeNow := time.Unix(0, 0)
		cache := &ConfigCache{ttl: 30 * time.Second, now: func() time.Time { return fakeNow }}

		cfg, err := NewInferenceReplicaConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, []string{"group-a"}, cfg.Identity().Groups)

		_, err = clientset.CoreV1().ConfigMaps(constants.OMENamespace).Update(context.TODO(), &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name:      constants.InferenceServiceConfigMapName,
				Namespace: constants.OMENamespace,
			},
			Data: map[string]string{
				InferenceReplicaConfigName: `{"controllerIdentity":{"groups":["group-b"]}}`,
			},
		}, metav1.UpdateOptions{})
		require.NoError(t, err)

		// Within TTL the edit is not yet visible.
		cfg, err = NewInferenceReplicaConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, []string{"group-a"}, cfg.Identity().Groups)

		// After TTL the edit applies.
		fakeNow = fakeNow.Add(31 * time.Second)
		cfg, err = NewInferenceReplicaConfigCached(cache, clientset)
		require.NoError(t, err)
		assert.Equal(t, []string{"group-b"}, cfg.Identity().Groups)
	})

	t.Run("missing ConfigMap is an error, cached or not", func(t *testing.T) {
		clientset := fake.NewSimpleClientset() // no ConfigMap
		for name, cache := range map[string]*ConfigCache{
			"cached":   NewConfigCache(30 * time.Second),
			"uncached": nil,
		} {
			cfg, err := NewInferenceReplicaConfigCached(cache, clientset)
			assert.Error(t, err, name)
			assert.Nil(t, cfg, name)
		}
	})
}
