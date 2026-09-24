package replica

import (
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"sigs.k8s.io/ome/internal/ome-agent/replica/common"
	"sigs.k8s.io/ome/internal/ome-agent/replica/replicator"
	"sigs.k8s.io/ome/pkg/constants"
	"sigs.k8s.io/ome/pkg/ociobjectstore"
)

func TestRetryReusesOwnedUploadLockUntilCompletion(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.ArtifactUploadLockOwnerID = "operation-a"
	configureLockObject(t, agent, `{"ownerID":"operation-a"}`, "original-etag", http.StatusOK)
	lockPresent := true
	markerWritten := false
	started := time.Now().Add(-time.Hour)
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		return targetArtifactState{UploadLocked: lockPresent, UploadLockETag: "original-etag", UploadLockModifiedTime: &started}, nil
	}
	tryAcquireArtifactUploadLockFunc = uploadLockAlreadyExists
	sleepFunc = func(time.Duration) { t.Fatal("a retry must not wait for its own lock to expire") }
	replication := &fakeReplicator{onReplicate: func([]common.ReplicationObject) error {
		assert.True(t, lockPresent, "operation A must keep its lock while the retry uploads")
		return nil
	}}
	newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) { return replication, nil }
	uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
		assert.True(t, lockPresent)
		markerWritten = true
		return nil
	}
	var releasedETags []string
	releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, etag string) (bool, error) {
		assert.True(t, markerWritten, "do not delete and reacquire the lock between attempts")
		releasedETags = append(releasedETags, etag)
		lockPresent = false
		return true, nil
	}
	deleteStaleArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, _ string) (bool, error) {
		t.Fatal("a retry must not delete its owner's lock")
		return false, nil
	}

	require.NoError(t, agent.Start())
	assert.Len(t, replication.objects, 1)
	assert.True(t, markerWritten)
	assert.Equal(t, []string{"original-etag"}, releasedETags)
}

func TestOwnedUploadLockReleasedAfterTerminalError(t *testing.T) {
	for _, failure := range []string{"replication", "completion marker"} {
		t.Run(failure, func(t *testing.T) {
			agent, cleanup := newTestAgentForCompletionMarker(t)
			defer cleanup()
			agent.Config.ArtifactUploadLockOwnerID = "operation-a"
			configureLockObject(t, agent, `{"ownerID":"operation-a"}`, "original-etag", http.StatusOK)
			lockPresent := false
			lockCreates := 0
			tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, body string, _ ociobjectstore.ObjectURI) (string, bool, error) {
				if lockPresent {
					return "", false, nil
				}
				lockPresent = true
				lockCreates++
				if lockCreates == 1 {
					assert.JSONEq(t, `{"ownerID":"operation-a"}`, body)
					return "original-etag", true, nil
				}
				assert.JSONEq(t, `{"ownerID":"operation-b"}`, body)
				return "next-etag", true, nil
			}
			started := time.Now()
			targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
				return targetArtifactState{UploadLocked: lockPresent, UploadLockETag: "original-etag", UploadLockModifiedTime: &started}, nil
			}
			attempt := 1
			attemptError := errors.New("upload failed")
			replication := &fakeReplicator{onReplicate: func([]common.ReplicationObject) error {
				assert.True(t, lockPresent)
				if attempt == 1 && failure == "replication" {
					return attemptError
				}
				return nil
			}}
			newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) { return replication, nil }
			completed := false
			uploadCompletionMarkerFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) error {
				if attempt == 1 && failure == "completion marker" {
					return attemptError
				}
				completed = true
				return nil
			}
			var releasedETags []string
			releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, etag string) (bool, error) {
				if attempt == 2 {
					assert.True(t, completed, "release the next owner's lock after its upload completes")
				}
				releasedETags = append(releasedETags, etag)
				lockPresent = false
				return true, nil
			}
			sleepFunc = func(time.Duration) { t.Fatal("the next operation must not wait for a failed operation's lock") }

			// The consumer may not retry a returned error, so release the lock
			// to allow an independent operation to acquire it.
			require.ErrorIs(t, agent.Start(), attemptError)
			require.False(t, lockPresent, "do not leave the failed operation's lock until its 120-hour timeout")
			require.Equal(t, []string{"original-etag"}, releasedETags)

			next := &ReplicaAgent{Logger: agent.Logger, Config: agent.Config, ReplicationInput: agent.ReplicationInput}
			next.Config.ArtifactUploadLockOwnerID = "operation-b"
			attempt = 2
			require.NoError(t, next.Start())
			assert.True(t, completed)
			assert.Equal(t, 2, lockCreates, "the next operation must acquire its own lock")
			assert.Equal(t, []string{"original-etag", "next-etag"}, releasedETags)
		})
	}
}

func TestOwnedUploadLockDoesNotReuseOtherOrUnidentifiedOwners(t *testing.T) {
	for _, tt := range []struct {
		name   string
		body   string
		etag   string
		status int
	}{
		{name: "other owner", body: `{"ownerID":"operation-b"}`, etag: "other-etag"},
		{name: "legacy lock", body: constants.ArtifactUploadLockBody, etag: "etag"},
		{name: "missing owner ID", body: `{}`, etag: "etag"},
		{name: "empty owner ID", body: `{"ownerID":""}`, etag: "etag"},
		{name: "malformed record", body: `{`, etag: "etag"},
		{name: "oversized record", body: `{"ownerID":"operation-a"}` + strings.Repeat(" ", 8192), etag: "etag"},
		{name: "missing ETag", body: `{"ownerID":"operation-a"}`, etag: "absent"},
		{name: "empty ETag", body: `{"ownerID":"operation-a"}`},
		{name: "read failed", body: `{}`, status: http.StatusForbidden},
	} {
		t.Run(tt.name, func(t *testing.T) {
			agent, cleanup := newTestAgentForCompletionMarker(t)
			defer cleanup()
			agent.Config.ArtifactUploadLockOwnerID = "operation-a"
			configureLockObject(t, agent, tt.body, tt.etag, tt.status)
			tryAcquireArtifactUploadLockFunc = uploadLockAlreadyExists
			created := time.Now()
			current := created
			nowFunc = func() time.Time { return current }
			sleepFunc = func(d time.Duration) { current = current.Add(d) }
			targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
				if current.Sub(created) >= time.Minute {
					return targetArtifactState{}, errors.New("stop after confirming the lock is still held")
				}
				return targetArtifactState{UploadLocked: true, UploadLockModifiedTime: &created, UploadLockETag: "etag"}, nil
			}
			releaseArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI, _ string) (bool, error) {
				t.Fatal("must not delete another or unidentified owner's lock")
				return false, nil
			}

			lock, skip, err := agent.prepareTargetArtifactUpload()
			require.ErrorContains(t, err, "stop after confirming")
			assert.Nil(t, lock)
			assert.False(t, skip)
		})
	}
}

func TestOwnedUploadLockRejectsMissingContent(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.ArtifactUploadLockOwnerID = "operation-a"
	agent.Config.Target.OCIOSDataStore = newTargetArtifactStateDataStore("")
	agent.Config.Target.OCIOSDataStore.Client.HTTPClient = targetArtifactStateRequestFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Header: http.Header{
				"Content-Type": []string{"application/json"},
				"Etag":         []string{"original-etag"},
			},
			Request: request,
		}, nil
	})

	assert.Nil(t, agent.reuseOwnedUploadLock())
}

func TestOwnedUploadLockRetriesOwnerRead(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.ArtifactUploadLockOwnerID = "operation-a"
	configureLockObject(t, agent, `{"ownerID":"operation-a"}`, "original-etag", http.StatusOK)
	client := agent.Config.Target.OCIOSDataStore.Client
	successfulRead := client.HTTPClient
	reads := 0
	client.HTTPClient = targetArtifactStateRequestFunc(func(request *http.Request) (*http.Response, error) {
		reads++
		if reads == 1 {
			return &http.Response{StatusCode: http.StatusNotFound, Header: http.Header{}, Body: io.NopCloser(strings.NewReader("")), Request: request}, nil
		}
		return successfulRead.Do(request)
	})
	tryAcquireArtifactUploadLockFunc = uploadLockAlreadyExists
	created := time.Now()
	current := created
	nowFunc = func() time.Time { return current }
	sleepFunc = func(d time.Duration) { current = current.Add(d) }
	targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
		if current.Sub(created) >= time.Minute {
			return targetArtifactState{}, errors.New("owner read was not retried")
		}
		return targetArtifactState{UploadLocked: true, UploadLockModifiedTime: &created, UploadLockETag: "original-etag"}, nil
	}

	lock, skip, err := agent.prepareTargetArtifactUpload()
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "original-etag", lock.ETag)
	assert.False(t, skip)
	assert.Equal(t, 2, reads)
}

func TestAcquireArtifactUploadLockRecordsOnlyOwnerID(t *testing.T) {
	agent, cleanup := newTestAgentForCompletionMarker(t)
	defer cleanup()
	agent.Config.ArtifactUploadLockOwnerID = "operation-a"
	tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, body string, _ ociobjectstore.ObjectURI) (string, bool, error) {
		assert.JSONEq(t, `{"ownerID":"operation-a"}`, body)
		return "created-etag", true, nil
	}
	lock, err := agent.acquireTargetArtifactUploadLock()
	require.NoError(t, err)
	require.NotNil(t, lock)
	assert.Equal(t, "created-etag", lock.ETag)
}

func TestArtifactLockOwnerIDFromEnvironment(t *testing.T) {
	t.Setenv("OME_AGENT_ARTIFACT_UPLOAD_LOCK_OWNER_ID", "operation-a")
	v := viper.New()
	v.SetEnvPrefix("OME_AGENT")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()
	config, err := NewReplicaConfig(WithViper(v))
	require.NoError(t, err)
	assert.Equal(t, "operation-a", config.ArtifactUploadLockOwnerID)
	require.NoError(t, config.validateArtifactLockOwner())
}

func TestArtifactLockOwnerIDRejectsWhitespace(t *testing.T) {
	config := &Config{ArtifactUploadLockOwnerID: " operation-a "}
	require.ErrorContains(t, config.validateArtifactLockOwner(), "whitespace")
}

func TestArtifactLockOwnerIDSizeValidation(t *testing.T) {
	// The JSON object adds 14 bytes; each '<' is encoded as six bytes (\u003c).
	for _, tt := range []struct {
		name    string
		ownerID string
		tooLong bool
	}{
		{name: "unset"},
		{name: "job UID", ownerID: "2e6c7afc-133a-4325-a458-0e5da80c5a81"},
		{name: "4096 byte body", ownerID: strings.Repeat("a", 4082)},
		{name: "4097 byte body", ownerID: strings.Repeat("a", 4083), tooLong: true},
		{name: "4096 byte escaped body", ownerID: strings.Repeat("<", 680) + "aa"},
		{name: "4097 byte escaped body", ownerID: strings.Repeat("<", 680) + "aaa", tooLong: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			agent, cleanup := newTestAgentForCompletionMarker(t)
			defer cleanup()
			agent.Config.ArtifactUploadLockOwnerID = tt.ownerID
			agent.Config.Target.OCIOSDataStore = newTargetArtifactStateDataStore("")
			if tt.tooLong {
				require.ErrorContains(t, agent.Config.Validate(), "artifact_upload_lock_owner_id")
				tryAcquireArtifactUploadLockFunc = func(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
					t.Fatal("must reject oversized lock bodies before uploading")
					return "", false, nil
				}
				lock, err := agent.acquireTargetArtifactUploadLock()
				require.ErrorContains(t, err, "4096 bytes")
				assert.Nil(t, lock)
				return
			}

			require.NoError(t, agent.Config.Validate())
			body, err := agent.artifactUploadLockBody()
			require.NoError(t, err)
			if tt.ownerID == "" {
				assert.Equal(t, constants.ArtifactUploadLockBody, body)
				return
			}
			configureLockObject(t, agent, body, "original-etag", http.StatusOK)
			lock := agent.reuseOwnedUploadLock()
			require.NotNil(t, lock, "a retry must be able to reuse a lock created with a valid owner ID")
			assert.Equal(t, "original-etag", lock.ETag)
		})
	}
}

func TestCompletedArtifactTakesPrecedenceOverOwnedLockReuse(t *testing.T) {
	for _, tt := range []struct {
		name           string
		completeOnRead int
	}{
		{name: "already complete", completeOnRead: 1},
		{name: "complete when lock is reused", completeOnRead: 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			agent, cleanup := newTestAgentForCompletionMarker(t)
			defer cleanup()
			agent.Config.ArtifactUploadLockOwnerID = "operation-a"
			configureLockObject(t, agent, `{"ownerID":"operation-a"}`, "original-etag", http.StatusOK)
			tryAcquireArtifactUploadLockFunc = func(ds *ociobjectstore.OCIOSDataStore, body string, uri ociobjectstore.ObjectURI) (string, bool, error) {
				assert.NotEqual(t, 1, tt.completeOnRead, "a completed artifact needs no lock acquisition")
				return uploadLockAlreadyExists(ds, body, uri)
			}
			stateCalls := 0
			targetArtifactStateFunc = func(_ *ociobjectstore.OCIOSDataStore, _ ociobjectstore.ObjectURI) (targetArtifactState, error) {
				stateCalls++
				return targetArtifactState{Complete: stateCalls >= tt.completeOnRead, UploadLocked: true, UploadLockETag: "original-etag"}, nil
			}
			newReplicatorFunc = func(_ *ReplicaAgent) (replicator.Replicator, error) {
				t.Fatal("must reuse the completed artifact")
				return nil, errors.New("unexpected replication")
			}
			require.NoError(t, agent.Start())
		})
	}
}

func uploadLockAlreadyExists(_ *ociobjectstore.OCIOSDataStore, _ string, _ ociobjectstore.ObjectURI) (string, bool, error) {
	return "", false, nil
}

func configureLockObject(t *testing.T, agent *ReplicaAgent, body, etag string, statusCode int) {
	t.Helper()
	if statusCode == 0 {
		statusCode = http.StatusOK
	}
	agent.Config.Target.OCIOSDataStore = newTargetArtifactStateDataStore("")
	agent.Config.Target.OCIOSDataStore.Client.HTTPClient = targetArtifactStateRequestFunc(func(request *http.Request) (*http.Response, error) {
		assert.Equal(t, http.MethodGet, request.Method)
		assert.True(t, strings.HasSuffix(request.URL.Path, "/.ome-artifact-upload.lock"))
		headers := http.Header{"Content-Type": []string{"application/json"}}
		if etag != "absent" {
			headers.Set("Etag", etag)
		}
		return &http.Response{StatusCode: statusCode, Header: headers, Body: io.NopCloser(strings.NewReader(body)), Request: request}, nil
	})
}
