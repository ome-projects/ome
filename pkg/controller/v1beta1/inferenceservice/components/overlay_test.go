package components

import (
	"testing"

	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/ome/pkg/apis/ome/v1beta1"
	isvcutils "sigs.k8s.io/ome/pkg/controller/v1beta1/inferenceservice/utils"
	"sigs.k8s.io/ome/pkg/render"
)

func pvcOverlay(name, ns, pvcName, subPath string) isvcutils.ResolvedOverlay {
	uri := "pvc://" + ns + ":" + pvcName + "/" + subPath
	return isvcutils.ResolvedOverlay{
		Ref:  v1beta1.ModelOverlayRef{Name: name},
		Spec: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{StorageUri: &uri}},
	}
}

func shardedOverlay(name, uri string) isvcutils.ResolvedOverlay {
	dist := v1beta1.DistributionSharded
	return isvcutils.ResolvedOverlay{
		Ref: v1beta1.ModelOverlayRef{Name: name},
		Spec: &v1beta1.BaseModelSpec{
			Distribution: &dist,
			Storage:      &v1beta1.StorageSpec{StorageUri: &uri},
		},
	}
}

func perNodeOverlay(name, hostPath string) isvcutils.ResolvedOverlay {
	return isvcutils.ResolvedOverlay{
		Ref:  v1beta1.ModelOverlayRef{Name: name},
		Spec: &v1beta1.BaseModelSpec{Storage: &v1beta1.StorageSpec{Path: &hostPath}},
	}
}

func skippedOverlay(name, reason string) isvcutils.ResolvedOverlay {
	return isvcutils.ResolvedOverlay{
		Ref:        v1beta1.ModelOverlayRef{Name: name},
		SkipReason: reason,
	}
}

func TestAppendOverlayVolumes(t *testing.T) {
	overlays := []isvcutils.ResolvedOverlay{
		pvcOverlay("foo-pvc", "default", "data-pvc", "weights/v1"),
		shardedOverlay("foo-sharded", "s3://bucket/tree"),
		perNodeOverlay("foo-pernode", "/mnt/models/foo"),
		skippedOverlay("foo-gone", "not found"),
	}
	pod := &corev1.PodSpec{}
	render.AppendOverlayVolumes(nil, overlays, pod)

	assert.Len(t, pod.Volumes, 2, "expect PVC + PerNode volumes, no Sharded, no skipped")
	var sawPVC, sawHost bool
	for _, v := range pod.Volumes {
		switch v.Name {
		case "model-overlay-foo-pvc":
			assert.NotNil(t, v.PersistentVolumeClaim)
			assert.Equal(t, "data-pvc", v.PersistentVolumeClaim.ClaimName)
			assert.True(t, v.PersistentVolumeClaim.ReadOnly)
			sawPVC = true
		case "model-overlay-foo-pernode":
			assert.NotNil(t, v.HostPath)
			assert.Equal(t, "/mnt/models/foo", v.HostPath.Path)
			sawHost = true
		}
	}
	assert.True(t, sawPVC && sawHost)
}

func TestAppendOverlayVolumeMounts(t *testing.T) {
	overlays := []isvcutils.ResolvedOverlay{
		pvcOverlay("foo-pvc", "default", "data-pvc", "weights/v1"),
		shardedOverlay("foo-sharded", "s3://bucket/tree"),
		perNodeOverlay("foo-pernode", "/mnt/models/foo"),
	}
	c := &corev1.Container{}
	render.AppendOverlayVolumeMounts(nil, overlays, c)

	assert.Len(t, c.VolumeMounts, 2, "Sharded contributes no mount")

	for _, vm := range c.VolumeMounts {
		assert.True(t, vm.ReadOnly, "overlay mounts must be ReadOnly")
		switch vm.Name {
		case "model-overlay-foo-pvc":
			assert.Equal(t, "/opt/ml/model-overlays/foo-pvc", vm.MountPath)
			assert.Equal(t, "weights/v1", vm.SubPath)
		case "model-overlay-foo-pernode":
			assert.Equal(t, "/opt/ml/model-overlays/foo-pernode", vm.MountPath)
			assert.Empty(t, vm.SubPath, "PerNode has no SubPath; HostPath is the whole root")
		}
	}
}

func TestAppendOverlayEnvVars(t *testing.T) {
	s3URI := "s3://bucket/tree/weights"
	overlays := []isvcutils.ResolvedOverlay{
		pvcOverlay("foo-pvc", "default", "data-pvc", "weights/v1"),
		shardedOverlay("foo-sharded", s3URI),
		perNodeOverlay("foo-pernode", "/mnt/models/foo"),
		skippedOverlay("foo-gone", "not found"),
	}
	c := &corev1.Container{}
	render.AppendOverlayEnvVars(nil, overlays, c)

	envs := map[string]string{}
	for _, e := range c.Env {
		envs[e.Name] = e.Value
	}
	assert.Equal(t, "/opt/ml/model-overlays/foo-pvc", envs["OVERLAY_FOO_PVC_MODEL_PATH"])
	assert.Equal(t, s3URI, envs["OVERLAY_FOO_SHARDED_MODEL_PATH"],
		"Sharded overlay env is the storage URI verbatim — runner fetches via cluster_cache client")
	assert.Equal(t, "/opt/ml/model-overlays/foo-pernode", envs["OVERLAY_FOO_PERNODE_MODEL_PATH"])
	_, gone := envs["OVERLAY_FOO_GONE_MODEL_PATH"]
	assert.False(t, gone, "skipped overlay must not appear")
}

func TestAnyOverlayIsSharded(t *testing.T) {
	tests := []struct {
		name string
		in   []isvcutils.ResolvedOverlay
		want bool
	}{
		{"empty", nil, false},
		{"only PVC", []isvcutils.ResolvedOverlay{pvcOverlay("a", "ns", "pvc", "p")}, false},
		{"PVC + Sharded mixed", []isvcutils.ResolvedOverlay{
			pvcOverlay("a", "ns", "pvc", "p"),
			shardedOverlay("b", "s3://x"),
		}, true},
		{"skipped Sharded does not count", []isvcutils.ResolvedOverlay{
			skippedOverlay("a", "not found"),
		}, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, render.AnyOverlayIsSharded(tc.in))
		})
	}
}

func TestMountedOverlaySummary(t *testing.T) {
	overlays := []isvcutils.ResolvedOverlay{
		pvcOverlay("foo-pvc", "default", "data-pvc", "weights/v1"),
		shardedOverlay("foo-sharded", "s3://bucket/tree"),
		skippedOverlay("foo-gone", "not found"),
	}
	got := MountedOverlaySummary(overlays)
	assert.Len(t, got, 2, "skipped overlays excluded from MountedOverlays")

	byName := map[string]v1beta1.MountedOverlay{}
	for _, m := range got {
		byName[m.Name] = m
	}
	pvc := byName["foo-pvc"]
	assert.Equal(t, "OVERLAY_FOO_PVC_MODEL_PATH", pvc.EnvVar)
	assert.Equal(t, "/opt/ml/model-overlays/foo-pvc", pvc.MountPath)
	assert.Equal(t, string(v1beta1.DistributionPerNode), pvc.Distribution,
		"PVC is a storage backend, not a Distribution; defaults to PerNode")

	sharded := byName["foo-sharded"]
	assert.Equal(t, "OVERLAY_FOO_SHARDED_MODEL_PATH", sharded.EnvVar)
	assert.Empty(t, sharded.MountPath, "Sharded overlays have no on-disk path")
	assert.Equal(t, string(v1beta1.DistributionSharded), sharded.Distribution)
}

func TestSkippedOverlayReasons(t *testing.T) {
	overlays := []isvcutils.ResolvedOverlay{
		pvcOverlay("ok", "ns", "pvc", "p"),
		skippedOverlay("missing", "not found"),
		skippedOverlay("dead", "disabled"),
	}
	reasons := SkippedOverlayReasons(overlays)
	assert.Equal(t, []string{"not found", "disabled"}, reasons)
}

// AppendOverlayEnvVars must not clobber an env var the runtime already
// set with the same name. The wider contract (AppendEnvVarsIfNotExist)
// is exercised by base.go callers; this pins it for the overlay path.
func TestAppendOverlayEnvVars_PreservesExisting(t *testing.T) {
	c := &corev1.Container{
		Env: []corev1.EnvVar{{Name: "OVERLAY_FOO_MODEL_PATH", Value: "user-set"}},
	}
	render.AppendOverlayEnvVars(nil, []isvcutils.ResolvedOverlay{
		pvcOverlay("foo", "ns", "pvc", "p"),
	}, c)
	var got string
	for _, e := range c.Env {
		if e.Name == "OVERLAY_FOO_MODEL_PATH" {
			got = e.Value
		}
	}
	assert.Equal(t, "user-set", got, "user-set env wins over controller-injected")
}

// silence unused warning when the test file is the only consumer
var _ = ptr.To[string]
