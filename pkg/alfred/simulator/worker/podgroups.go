package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/ome/alfred-simulator/protocol"
)

const podGroupLabel = "scheduling.x-k8s.io/pod-group"

// snapshotTransport has no dialer or delegate: only immutable PodGroup list/watch is legal.
type snapshotTransport struct {
	ctx    context.Context
	groups map[types.NamespacedName]*unstructured.Unstructured
}

func (t *snapshotTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Method != http.MethodGet || r.URL.Path != "/apis/scheduling.x-k8s.io/v1alpha1/podgroups" {
		return nil, fmt.Errorf("snapshot transport denies %s %s", r.Method, r.URL.Path)
	}
	if r.URL.Query().Get("watch") == "true" {
		reader, writer := io.Pipe()
		stop := context.AfterFunc(t.ctx, func() { _ = reader.Close() })
		stopRequest := context.AfterFunc(r.Context(), func() { _ = reader.Close() })
		go func() {
			defer stop()
			defer stopRequest()
			defer writer.Close()
			if r.URL.Query().Get("sendInitialEvents") == "true" {
				encoder := json.NewEncoder(writer)
				for _, group := range t.groups {
					copy := group.DeepCopy()
					copy.SetAPIVersion("scheduling.x-k8s.io/v1alpha1")
					copy.SetKind("PodGroup")
					copy.SetResourceVersion("1")
					if err := encoder.Encode(map[string]any{"type": "ADDED", "object": copy.Object}); err != nil {
						return
					}
				}
				bookmark := map[string]any{"apiVersion": "scheduling.x-k8s.io/v1alpha1", "kind": "PodGroup", "metadata": map[string]any{"resourceVersion": "1", "annotations": map[string]string{metav1.InitialEventsAnnotationKey: "true"}}}
				if err := encoder.Encode(map[string]any{"type": "BOOKMARK", "object": bookmark}); err != nil {
					return
				}
			}
			select {
			case <-r.Context().Done():
			case <-t.ctx.Done():
			}
		}()
		return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: reader, Request: r}, nil
	}
	items := make([]any, 0, len(t.groups))
	for _, g := range t.groups {
		copy := g.DeepCopy()
		copy.SetAPIVersion("scheduling.x-k8s.io/v1alpha1")
		copy.SetKind("PodGroup")
		copy.SetResourceVersion("1")
		items = append(items, copy.Object)
	}
	b, err := json.Marshal(map[string]any{"apiVersion": "scheduling.x-k8s.io/v1alpha1", "kind": "PodGroupList", "metadata": map[string]string{"resourceVersion": "1"}, "items": items})
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(bytes.NewReader(b)), Request: r}, nil
}

func validateGroups(r protocol.Request, s *protocol.Snapshot, gang bool) error {
	replacement := map[types.NamespacedName]int{}
	occupied := map[types.NamespacedName]int{}
	groupUIDs := map[types.UID]bool{}
	for key, group := range s.PodGroups {
		if group.GetUID() == "" || groupUIDs[group.GetUID()] {
			return fmt.Errorf("PodGroup %s must have a distinct immutable UID", key)
		}
		groupUIDs[group.GetUID()] = true
	}
	for _, p := range s.Pods {
		if name := p.Labels[podGroupLabel]; name != "" {
			occupied[types.NamespacedName{Namespace: p.Namespace, Name: name}]++
		}
	}
	for _, p := range r.ReplacementPods {
		if name := p.Labels[podGroupLabel]; name != "" {
			replacement[types.NamespacedName{Namespace: p.Namespace, Name: name}]++
		} else if r.RequireGang {
			return fmt.Errorf("RequireGang replacement lacks PodGroup")
		}
	}
	if (r.RequireGang || len(replacement) > 0) && !gang {
		return fmt.Errorf("replacement gangs require the complete OMEGangPack profile")
	}
	if r.RequireGang && len(replacement) != 1 {
		return fmt.Errorf("RequireGang requires one complete replacement PodGroup")
	}
	if len(replacement) > 0 {
		if len(replacement) != 1 {
			return fmt.Errorf("mixed replacement PodGroups are unsupported")
		}
		for _, count := range replacement {
			if count != len(r.ReplacementPods) {
				return fmt.Errorf("mixing gang and standalone replacements is unsupported")
			}
		}
	}
	for group, count := range replacement {
		if occupied[group] != 0 {
			return fmt.Errorf("replacement reuses occupied PodGroup %s", group)
		}
		pg := s.PodGroups[group]
		if pg == nil {
			return fmt.Errorf("replacement PodGroup %s missing", group)
		}
		min, ok, err := unstructured.NestedInt64(pg.Object, "spec", "minMember")
		if err != nil || !ok || min != int64(count) {
			return fmt.Errorf("incomplete replacement PodGroup %s", group)
		}
	}
	for group, count := range occupied {
		pg := s.PodGroups[group]
		if pg == nil {
			return fmt.Errorf("occupied PodGroup %s missing", group)
		}
		min, ok, err := unstructured.NestedInt64(pg.Object, "spec", "minMember")
		if err != nil || !ok || min != int64(count) {
			return fmt.Errorf("incomplete occupied PodGroup %s", group)
		}
	}
	for _, p := range s.Pods {
		schedulerName := strings.TrimSpace(p.Spec.SchedulerName)
		if schedulerName == "" {
			schedulerName = "default-scheduler"
		}
		if name := p.Labels[podGroupLabel]; name != "" && schedulerName != r.Profile.SchedulerName {
			return fmt.Errorf("occupied gang has mixed scheduler profiles")
		}
	}
	return nil
}
