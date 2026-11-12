package utils

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierr "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/discovery"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/ome/pkg/constants"
)

/* NOTE TO AUTHORS:
 *
 * Only you can prevent ... the proliferation of useless "utility" classes.
 * Please add functional style container operations sparingly and intentionally.
 */

// gvResourcesCache caches API resource discovery results keyed by groupVersion.
// It is read on every InferenceService reconcile (via IsCrdAvailable) while
// several controllers concurrently probe CRD availability at startup, so all
// access must go through gvResourcesCacheMu to avoid a fatal
// "concurrent map writes".
var (
	gvResourcesCacheMu sync.RWMutex
	gvResourcesCache   map[string]*metav1.APIResourceList
)

func Filter(origin map[string]string, predicate func(string) bool) map[string]string {
	result := make(map[string]string)
	for k, v := range origin {
		if predicate(k) {
			result[k] = v
		}
	}
	return result
}

func Union(maps ...map[string]string) map[string]string {
	result := make(map[string]string)
	for _, m := range maps {
		for k, v := range m {
			result[k] = v
		}
	}
	return result
}

func Includes(slice []string, value string) bool {
	for _, v := range slice {
		if v == value {
			return true
		}
	}
	return false
}

// FilterPodOnlyAnnotations returns a copy of annotations without the keys that
// start with an entry of constants.PodOnlyAnnotationPrefixes, which belong on a
// Pod and do nothing on a Service; nil stays nil.
func FilterPodOnlyAnnotations(annotations map[string]string) map[string]string {
	if annotations == nil {
		return nil
	}
	return Filter(annotations, func(key string) bool {
		return !IsPrefixSupported(key, constants.PodOnlyAnnotationPrefixes)
	})
}

func AppendVolumeIfNotExists(slice []v1.Volume, volume v1.Volume) []v1.Volume {
	for i := range slice {
		if slice[i].Name == volume.Name {
			return slice
		}
	}
	return append(slice, volume)
}

// PodRequestsAccelerator reports whether any container of spec, init containers
// included, names one of acceleratorResources in its Requests or Limits. An empty
// acceleratorResources matches nothing: there is no default resource name.
func PodRequestsAccelerator(spec *v1.PodSpec, acceleratorResources []string) bool {
	if spec == nil || len(acceleratorResources) == 0 {
		return false
	}
	holds := func(containers []v1.Container) bool {
		for i := range containers {
			r := &containers[i].Resources
			for _, name := range acceleratorResources {
				n := v1.ResourceName(name)
				if _, ok := r.Limits[n]; ok {
					return true
				}
				if _, ok := r.Requests[n]; ok {
					return true
				}
			}
		}
		return false
	}
	return holds(spec.Containers) || holds(spec.InitContainers)
}

// IsPrefixSupported Check if a given string contains one of the prefixes in the provided list.
func IsPrefixSupported(input string, prefixes []string) bool {
	for _, prefix := range prefixes {
		if strings.HasPrefix(input, prefix) {
			return true
		}
	}
	return false
}

// MergeEnvs copies each override's Value onto the base entry of the same Name,
// rewriting baseEnvs in place, and appends the overrides that have no base entry.
func MergeEnvs(baseEnvs []v1.EnvVar, overrideEnvs []v1.EnvVar) []v1.EnvVar {
	var extra []v1.EnvVar

	for _, override := range overrideEnvs {
		inBase := false

		for i, base := range baseEnvs {
			if override.Name == base.Name {
				inBase = true
				baseEnvs[i].Value = override.Value
				break
			}
		}

		if !inBase {
			extra = append(extra, override)
		}
	}

	return append(baseEnvs, extra...)
}

func AppendPortIfNotExists(slice []v1.ContainerPort, elems ...v1.ContainerPort) []v1.ContainerPort {
	for _, elem := range elems {
		isElemExists := false
		for _, item := range slice {
			if item.Name == elem.Name {
				isElemExists = true
				break
			}
		}
		if !isElemExists {
			slice = append(slice, elem)
		}
	}
	return slice
}

// IsCrdAvailable checks if a given CRD is present in the cluster by verifying the
// existence of its API.
func IsCrdAvailable(config *rest.Config, groupVersion, kind string) (bool, error) {
	gvResources, err := GetAvailableResourcesForApi(config, groupVersion)
	if err != nil {
		return false, err
	}

	found := false
	if gvResources != nil {
		for _, crd := range gvResources.APIResources {
			if crd.Kind == kind {
				found = true
				break
			}
		}
	}

	return found, nil
}

// GetAvailableResourcesForApi returns the list of discovered resources that belong
// to the API specified in groupVersion. The first query to a specific groupVersion will
// query the cluster API server to discover the available resources and the discovered
// resources will be cached and returned to subsequent invocations to prevent additional
// queries to the API server.
func GetAvailableResourcesForApi(config *rest.Config, groupVersion string) (*metav1.APIResourceList, error) {
	gvResourcesCacheMu.RLock()
	gvResources, ok := gvResourcesCache[groupVersion]
	gvResourcesCacheMu.RUnlock()

	if !ok {
		discoveryClient, newClientErr := discovery.NewDiscoveryClientForConfig(config)
		if newClientErr != nil {
			return nil, newClientErr
		}

		var getGvResourcesErr error
		gvResources, getGvResourcesErr = discoveryClient.ServerResourcesForGroupVersion(groupVersion)
		if getGvResourcesErr != nil && !apierr.IsNotFound(getGvResourcesErr) {
			return nil, getGvResourcesErr
		}

		SetAvailableResourcesForApi(groupVersion, gvResources)
	}

	return gvResources, nil
}

// SetAvailableResourcesForApi stores the value of resources argument in the global cache
// of discovered API resources. This function should never be called directly. It is exported
// for usage in tests.
func SetAvailableResourcesForApi(groupVersion string, resources *metav1.APIResourceList) {
	gvResourcesCacheMu.Lock()
	defer gvResourcesCacheMu.Unlock()

	if gvResourcesCache == nil {
		gvResourcesCache = make(map[string]*metav1.APIResourceList)
	}

	gvResourcesCache[groupVersion] = resources
}

func Retry(attempts int, sleep time.Duration, f func() error) (err error) {
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return
		}

		if i >= (attempts - 1) {
			break
		}

		time.Sleep(sleep)
	}

	return fmt.Errorf("after %d attempts, last error: %w", attempts, err)
}

// RemoveString Helper functions to remove string from a slice of strings.
func RemoveString(slice []string, s string) (result []string) {
	for _, item := range slice {
		if item == s {
			continue
		}
		result = append(result, item)
	}
	return
}

var blankRegex = regexp.MustCompile(`\s`)

// IsStringEmptyOrWithWhitespaces reports whether input is empty or contains any
// whitespace character (per the \s regex class). Name is misleading — kept for
// backward compatibility with callers that grep for the symbol.
func IsStringEmptyOrWithWhitespaces(input string) bool {
	return input == "" || blankRegex.MatchString(input)
}

// HasSymlinkPointingToDir recursively searches under searchDir to determine
// whether any symbolic link points to targetDir.
//
// The search traverses all subdirectories of searchDir (including children,
// grandchildren, and deeper descendants) using filepath.WalkDir. The targetDir
// itself is not evaluated as a candidate symlink, but its subtree is still
// traversed to allow detection of symlinks located under targetDir.
//
// Only symbolic links are inspected. Regular files and directories are ignored.
// Relative symlink targets are resolved against the symlink’s parent directory
// and normalized before comparison.
func HasSymlinkPointingToDir(searchDir, targetDir string) (bool, error) {
	searchDir, err := filepath.Abs(searchDir)
	if err != nil {
		return false, err
	}

	targetDir, err = filepath.Abs(targetDir)
	if err != nil {
		return false, err
	}
	var errFound = errors.New("symlink pointing to target found")

	err = filepath.WalkDir(searchDir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}

		// Skip evaluating targetDir itself, but DO NOT SkipDir
		if path == targetDir {
			return nil
		}

		// Only inspect symlinks
		if d.Type()&os.ModeSymlink == 0 {
			return nil
		}

		linkTarget, err := os.Readlink(path)
		if err != nil {
			return nil
		}

		// Resolve relative symlink
		if !filepath.IsAbs(linkTarget) {
			linkTarget = filepath.Join(filepath.Dir(path), linkTarget)
		}

		linkTarget = filepath.Clean(linkTarget)

		if linkTarget == targetDir {
			return errFound
		}

		return nil
	})

	if err != nil {
		if errors.Is(err, errFound) {
			return true, nil
		}
		return false, err
	}

	return false, nil
}

// ContainsString reports whether values contains target.
// If isCaseSensitive is true, comparison is case-sensitive; otherwise it is case-insensitive.
// Non-string elements in values are ignored.
func ContainsString(values []interface{}, target string, isCaseSensitive bool) bool {
	for _, v := range values {
		if s, ok := v.(string); ok {
			var result bool
			if isCaseSensitive {
				result = s == target
			} else {
				result = strings.EqualFold(s, target)
			}
			if result {
				return result
			}
		}
	}
	return false
}
