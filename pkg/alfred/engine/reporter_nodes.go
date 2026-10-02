package engine

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"sigs.k8s.io/ome/pkg/alfred/config"
	"sigs.k8s.io/ome/pkg/alfred/policy"
	"sigs.k8s.io/ome/pkg/alfred/snapshot"
)

const (
	nodeRecordPrefix               = "node."
	hashedNodeRecordPrefix         = "node-hash."
	eventNodeRepairNeeded          = "NodeRepairNeeded"
	eventNodeDrainedForRepair      = "NodeDrainedForRepair"
	eventNodeMaintenanceRequested  = "NodeMaintenanceRequested"
	eventNodeDrainedForMaintenance = "NodeDrainedForMaintenance"
)

// Each reason owns an independent phase; clearing maintenance never clears an
// ongoing repair episode. Timestamps describe observations, not reporting time.
type nodeConditionRecord struct {
	Type               corev1.NodeConditionType `json:"type"`
	Status             corev1.ConditionStatus   `json:"status"`
	LastTransitionTime time.Time                `json:"lastTransitionTime"`
}

type nodeMaintenanceRecord struct {
	Requested bool     `json:"requested"`
	Triggers  []string `json:"triggers"`
}

type nodeRemediationRecord struct {
	NodeUID                types.UID                `json:"nodeUID"`
	State                  snapshot.NodeHealthState `json:"state"`
	Conditions             []nodeConditionRecord    `json:"conditions"`
	SuspectUntil           *time.Time               `json:"suspectUntil,omitempty"`
	Maintenance            nodeMaintenanceRecord    `json:"maintenance"`
	Workloads              []string                 `json:"workloads"`
	OMEGPUOccupantsPresent bool                     `json:"omeGpuOccupantsPresent"`
	ObservedAt             time.Time                `json:"observedAt"`
	SignaledAt             *time.Time               `json:"signaledAt,omitempty"`
	DrainedAt              *time.Time               `json:"drainedAt,omitempty"`
	MaintenanceRequestedAt *time.Time               `json:"maintenanceRequestedAt,omitempty"`
	MaintenanceDrainedAt   *time.Time               `json:"maintenanceDrainedAt,omitempty"`
}

func (r *Reporter) reconcileNodeRemediations(ctx context.Context, markers []*policy.NodeRemediation, cfg *config.Config, now time.Time, observed []time.Time) (map[string]nodeRemediationRecord, time.Time, bool) {
	var at time.Time
	if len(observed) > 0 {
		at = observed[0]
	} else if len(markers) > 0 && markers[0] != nil {
		at = markers[0].ObservedAt
	}
	if at.IsZero() || at.After(now) || at.Before(r.nodeObservedAt) {
		return nil, at, false
	}
	desired := make(map[string]*policy.NodeRemediation, len(markers))
	for _, m := range markers {
		if m == nil || m.Node == "" || m.NodeUID == "" || !m.ObservedAt.Equal(at) {
			return nil, at, false
		}
		if _, duplicate := desired[m.Node]; duplicate {
			return nil, at, false
		}
		if m.Health.State != snapshot.NodeHealthClear && m.Health.State != snapshot.NodeHealthUnhealthy && m.Health.State != snapshot.NodeHealthUnknown && m.Health.State != snapshot.NodeHealthSuspect {
			return nil, at, false
		}
		desired[m.Node] = m
	}
	if r.nodeRecords == nil {
		r.nodeRecords = map[string]nodeRemediationRecord{}
		r.nodeInitialized = map[string]struct{}{}
	}
	if !r.seedNodeRecords(ctx, desired, cfg, at) {
		return nil, at, false
	}
	// Re-reporting a cached successful snapshot retries persistence but cannot
	// create a second observation, clear an episode or refresh its timestamp.
	if at.Equal(r.nodeObservedAt) {
		return r.nodeRecords, at, true
	}
	for node, previous := range r.nodeRecords {
		if _, ok := desired[node]; !ok && at.After(previous.ObservedAt) {
			delete(r.nodeRecords, node)
		}
	}
	nodes := make([]string, 0, len(desired))
	for node := range desired {
		nodes = append(nodes, node)
	}
	sort.Strings(nodes)
	for _, node := range nodes {
		m := desired[node]
		previous := r.nodeRecords[node]
		if previous.NodeUID == m.NodeUID && !at.After(previous.ObservedAt) {
			continue
		}
		if m.Health.State == snapshot.NodeHealthClear && !m.Maintenance.Requested {
			delete(r.nodeRecords, node)
			continue
		}
		if previous.NodeUID != m.NodeUID {
			previous = nodeRemediationRecord{}
		}
		workloads := sortedUniqueStrings(m.Workloads)
		conditions := make([]nodeConditionRecord, len(m.Health.Conditions))
		for i, c := range m.Health.Conditions {
			conditions[i] = nodeConditionRecord(c)
		}
		record := nodeRemediationRecord{
			NodeUID: m.NodeUID, State: m.Health.State, Conditions: conditions,
			SuspectUntil: cloneTime(m.Health.SuspectUntil),
			Maintenance: nodeMaintenanceRecord{
				Requested: m.Maintenance.Requested,
				Triggers:  sortedUniqueStrings(m.Maintenance.Triggers),
			},
			Workloads: workloads, OMEGPUOccupantsPresent: m.OMEGPUOccupantsPresent || len(workloads) > 0,
			ObservedAt: at,
			SignaledAt: cloneTime(previous.SignaledAt), DrainedAt: cloneTime(previous.DrainedAt),
			MaintenanceRequestedAt: cloneTime(previous.MaintenanceRequestedAt),
			MaintenanceDrainedAt:   cloneTime(previous.MaintenanceDrainedAt),
		}
		if record.OMEGPUOccupantsPresent {
			record.DrainedAt = nil
			record.MaintenanceDrainedAt = nil
		}
		if record.State == snapshot.NodeHealthClear {
			record.SignaledAt = nil
			record.DrainedAt = nil
		}
		if record.State == snapshot.NodeHealthUnknown {
			record.DrainedAt = nil
		}
		if record.SignaledAt == nil && (record.State == snapshot.NodeHealthUnhealthy || record.State == snapshot.NodeHealthUnknown) {
			record.SignaledAt = cloneTime(&at)
			r.nodeEvent(node, m.NodeUID, eventNodeRepairNeeded, true)
		} else if record.SignaledAt != nil && record.State != snapshot.NodeHealthUnknown && record.DrainedAt == nil && !record.OMEGPUOccupantsPresent && at.After(*record.SignaledAt) {
			record.DrainedAt = cloneTime(&at)
			r.nodeEvent(node, m.NodeUID, eventNodeDrainedForRepair, false)
		}
		if !record.Maintenance.Requested {
			record.MaintenanceRequestedAt = nil
			record.MaintenanceDrainedAt = nil
		} else if record.MaintenanceRequestedAt == nil {
			record.MaintenanceRequestedAt = cloneTime(&at)
			r.nodeEvent(node, m.NodeUID, eventNodeMaintenanceRequested, false)
		} else if record.MaintenanceDrainedAt == nil && !record.OMEGPUOccupantsPresent && at.After(*record.MaintenanceRequestedAt) {
			record.MaintenanceDrainedAt = cloneTime(&at)
			r.nodeEvent(node, m.NodeUID, eventNodeDrainedForMaintenance, false)
		}
		r.nodeRecords[node] = record
	}
	r.nodeObservedAt = at
	return r.nodeRecords, at, true
}

func (r *Reporter) seedNodeRecords(ctx context.Context, desired map[string]*policy.NodeRemediation, cfg *config.Config, at time.Time) bool {
	var pending []string
	for node, m := range desired {
		if _, ok := r.nodeInitialized[node+"/"+string(m.NodeUID)]; !ok {
			pending = append(pending, node)
		}
	}
	if len(pending) == 0 {
		return true
	}
	var cm corev1.ConfigMap
	if *cfg.RecommendationsConfigMapEnabled {
		err := r.reader().Get(ctx, client.ObjectKey{Namespace: r.Namespace, Name: cfg.RecommendationsConfigMapName}, &cm)
		if err != nil && !apierrors.IsNotFound(err) {
			r.Log.Error(err, "seed node remediation records")
			return false
		}
	}
	for _, node := range pending {
		m := desired[node]
		var old nodeRemediationRecord
		if raw, ok := cm.Data[nodeRecordKey(node)]; ok && json.Unmarshal([]byte(raw), &old) == nil && old.NodeUID == m.NodeUID {
			if old.ObservedAt.After(at) {
				return false
			}
			seed := nodeRemediationRecord{NodeUID: m.NodeUID}
			healthValid := recordCoversHealth(old, m.Health)
			// Maintenance trigger names do not identify a transition. Only exact
			// snapshot replay can reuse maintenance phase after restart; otherwise
			// re-signal conservatively and require a new empty observation.
			maintenanceValid := old.ObservedAt.Equal(at) &&
				old.Maintenance.Requested == m.Maintenance.Requested &&
				reflect.DeepEqual(sortedUniqueStrings(old.Maintenance.Triggers), sortedUniqueStrings(m.Maintenance.Triggers)) &&
				validNodePhase(old.MaintenanceRequestedAt, old.MaintenanceDrainedAt, old.ObservedAt)
			if healthValid {
				seed.SignaledAt = cloneTime(old.SignaledAt)
				seed.DrainedAt = cloneTime(old.DrainedAt)
			}
			if maintenanceValid {
				seed.MaintenanceRequestedAt = cloneTime(old.MaintenanceRequestedAt)
				seed.MaintenanceDrainedAt = cloneTime(old.MaintenanceDrainedAt)
			}
			if (healthValid && (!m.Maintenance.Requested || maintenanceValid)) ||
				(maintenanceValid && m.Health.State == snapshot.NodeHealthClear) {
				seed = old
				if !healthValid {
					seed.SignaledAt = nil
					seed.DrainedAt = nil
				}
			}
			r.nodeRecords[node] = seed
		}
		r.nodeInitialized[node+"/"+string(m.NodeUID)] = struct{}{}
	}
	return true
}

func recordCoversHealth(old nodeRemediationRecord, current snapshot.NodeHealthObservation) bool {
	if old.ObservedAt.IsZero() || old.State != current.State || len(old.Conditions) == 0 || len(old.Conditions) != len(current.Conditions) || !validNodePhase(old.SignaledAt, old.DrainedAt, old.ObservedAt) {
		return false
	}
	conditions := map[corev1.NodeConditionType]snapshot.NodeConditionObservation{}
	for _, c := range old.Conditions {
		if !usableConditionEvidence(snapshot.NodeConditionObservation(c), old.ObservedAt) {
			return false
		}
		if _, exists := conditions[c.Type]; exists {
			return false
		}
		conditions[c.Type] = snapshot.NodeConditionObservation(c)
	}
	for _, c := range current.Conditions {
		previous, ok := conditions[c.Type]
		if !ok || !usableConditionEvidence(c, old.ObservedAt) || previous.Status != c.Status || !previous.LastTransitionTime.Equal(c.LastTransitionTime) {
			return false
		}
		delete(conditions, c.Type)
	}
	return len(conditions) == 0
}

func validNodePhase(signaled, drained *time.Time, observed time.Time) bool {
	if signaled == nil {
		return drained == nil
	}
	return !signaled.IsZero() && !signaled.After(observed) && (drained == nil || drained.After(*signaled) && !drained.After(observed))
}

func usableConditionEvidence(c snapshot.NodeConditionObservation, observed time.Time) bool {
	return c.Type != "" && !c.LastTransitionTime.IsZero() && !c.LastTransitionTime.After(observed) && (c.Status == corev1.ConditionTrue || c.Status == corev1.ConditionFalse || c.Status == corev1.ConditionUnknown)
}

func (r *Reporter) nodeEvent(node string, uid types.UID, reason string, warning bool) {
	message := "node %s has no observed OME GPU occupants; this does not establish whole-node maintenance safety"
	switch reason {
	case eventNodeRepairNeeded:
		message = "node %s has an observed health concern; repair investigation requested"
	case eventNodeMaintenanceRequested:
		message = "node %s has planned maintenance requested; no hardware damage is implied"
	}
	if reason == eventNodeMaintenanceRequested || reason == eventNodeDrainedForMaintenance {
		r.Metrics.NodeMaintenanceSignals.WithLabelValues(node, reason).Inc()
	} else {
		r.Metrics.NodeHealthSignals.WithLabelValues(node, reason).Inc()
	}
	typ := corev1.EventTypeNormal
	if warning {
		typ = corev1.EventTypeWarning
	}
	r.Recorder.Eventf(&corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: node, UID: uid}}, typ, reason, message, node)
}

func sortedUniqueStrings(values []string) []string {
	seen := map[string]bool{}
	result := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" && !seen[v] {
			seen[v] = true
			result = append(result, v)
		}
	}
	sort.Strings(result)
	return result
}
func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}
func nodeRecordKey(node string) string {
	key := nodeRecordPrefix + node
	if len(key) <= 253 {
		return key
	}
	digest := sha256.Sum256([]byte(node))
	suffix := fmt.Sprintf(".%x", digest[:])
	return hashedNodeRecordPrefix + node[:253-len(hashedNodeRecordPrefix)-len(suffix)] + suffix
}
func isNodeRecordKey(key string) bool {
	return strings.HasPrefix(key, nodeRecordPrefix) || strings.HasPrefix(key, hashedNodeRecordPrefix)
}

func writeNodeRecords(cm *corev1.ConfigMap, records map[string]nodeRemediationRecord, at time.Time) {
	wanted := make(map[string]nodeRemediationRecord, len(records))
	for node, record := range records {
		wanted[nodeRecordKey(node)] = record
	}
	for key, raw := range cm.Data {
		if !isNodeRecordKey(key) {
			continue
		}
		if _, keep := wanted[key]; keep {
			continue
		}
		var old nodeRemediationRecord
		if json.Unmarshal([]byte(raw), &old) == nil && !old.ObservedAt.IsZero() && at.After(old.ObservedAt) {
			delete(cm.Data, key)
		}
	}
	for key, record := range wanted {
		var old nodeRemediationRecord
		if json.Unmarshal([]byte(cm.Data[key]), &old) == nil && old.ObservedAt.After(record.ObservedAt) {
			continue
		}
		raw, err := json.Marshal(record)
		if err == nil {
			cm.Data[key] = string(raw)
		}
	}
}
