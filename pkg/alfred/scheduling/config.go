// Package scheduling defines Alfred's scheduler-profile selection and
// transport-independent simulation contract. It deliberately has no
// dependency on a scheduler implementation or controller package.
package scheduling

import (
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Config maps each scheduler name Alfred may simulate to its exact profile.
// An absent map or absent key means simulation is unavailable; there is no
// fallback profile.
type Config struct {
	Profiles map[string]Profile `json:"profiles,omitempty"`
}

// Profile identifies one configured scheduler simulation backend and the
// scheduler behavior it claims to reproduce. ConfigurationID is supplied by
// the operator and must identify the immutable combination of plugins,
// arguments, feature gates, and scheduler profile; Alfred never infers it.
type Profile struct {
	Backend          string `json:"backend"`
	SchedulerVersion string `json:"schedulerVersion"`
	ConfigurationID  string `json:"configurationID"`
	GangScheduling   bool   `json:"gangScheduling,omitempty"`
}

// SelectionStatus is a closed set of profile-selection outcomes.
type SelectionStatus string

const (
	SelectionReady       SelectionStatus = "Ready"
	SelectionUnavailable SelectionStatus = "Unavailable"
	SelectionUnsupported SelectionStatus = "Unsupported"
)

// SelectionReason explains a profile-selection outcome without accepting
// backend-provided free-form status.
type SelectionReason string

const (
	SelectionReasonProfileSelected      SelectionReason = "ProfileSelected"
	SelectionReasonNoTemplates          SelectionReason = "NoTemplates"
	SelectionReasonTemplateBound        SelectionReason = "TemplateBound"
	SelectionReasonMixedSchedulers      SelectionReason = "MixedSchedulers"
	SelectionReasonProfileNotConfigured SelectionReason = "ProfileNotConfigured"
	SelectionReasonProfileInvalid       SelectionReason = "ProfileInvalid"
	SelectionReasonGangUnsupported      SelectionReason = "GangUnsupported"
)

// Selection is the profile selected from the templates' actual schedulerName.
// A non-Ready selection must not be treated as a feasibility decision.
type Selection struct {
	SchedulerName string
	Profile       Profile
	Status        SelectionStatus
	Reason        SelectionReason
}

// ProfileIdentity returns the immutable identity to fence a simulation
// request and response, including the actual scheduler name selected from the
// templates.
func (s Selection) ProfileIdentity() ProfileIdentity {
	return ProfileIdentity{
		SchedulerName:    s.SchedulerName,
		Backend:          s.Profile.Backend,
		SchedulerVersion: s.Profile.SchedulerVersion,
		ConfigurationID:  s.Profile.ConfigurationID,
	}
}

// Validate rejects ambiguous map keys and profile identity values.
func (c Config) Validate() error {
	for schedulerName, profile := range c.Profiles {
		if err := validateSchedulerName(schedulerName); err != nil {
			return fmt.Errorf("scheduling profile scheduler name %q: %w", schedulerName, err)
		}
		if err := validateProfile(profile); err != nil {
			return fmt.Errorf("scheduling profile %q: %w", schedulerName, err)
		}
	}
	return nil
}

// Select resolves an exact scheduler profile for a set of Pod templates.
// Empty schedulerName values mean the Kubernetes default scheduler. Select
// never edits templates and never substitutes a configured scheduler for the
// templates' effective scheduler.
func Select(config Config, templates []corev1.PodTemplateSpec, requireGang bool) Selection {
	if len(templates) == 0 {
		return Selection{Status: SelectionUnavailable, Reason: SelectionReasonNoTemplates}
	}

	schedulerName := effectiveSchedulerName(templates[0].Spec.SchedulerName)
	for i := range templates {
		if effectiveSchedulerName(templates[i].Spec.SchedulerName) != schedulerName {
			return Selection{SchedulerName: schedulerName, Status: SelectionUnavailable, Reason: SelectionReasonMixedSchedulers}
		}
		if templates[i].Spec.NodeName != "" {
			return Selection{SchedulerName: schedulerName, Status: SelectionUnavailable, Reason: SelectionReasonTemplateBound}
		}
	}

	profile, found := config.Profiles[schedulerName]
	if !found {
		return Selection{SchedulerName: schedulerName, Status: SelectionUnavailable, Reason: SelectionReasonProfileNotConfigured}
	}
	selection := Selection{SchedulerName: schedulerName, Profile: profile}
	if validateSchedulerName(schedulerName) != nil || validateProfile(profile) != nil {
		selection.Status = SelectionUnavailable
		selection.Reason = SelectionReasonProfileInvalid
		return selection
	}
	if requireGang && !profile.GangScheduling {
		selection.Status = SelectionUnsupported
		selection.Reason = SelectionReasonGangUnsupported
		return selection
	}
	selection.Status = SelectionReady
	selection.Reason = SelectionReasonProfileSelected
	return selection
}

func effectiveSchedulerName(name string) string {
	if name == "" {
		return corev1.DefaultSchedulerName
	}
	return name
}

func validateSchedulerName(name string) error {
	if err := validateIdentifier("scheduler name", name); err != nil {
		return err
	}
	if problems := validation.IsDNS1123Subdomain(name); len(problems) > 0 {
		return fmt.Errorf("scheduler name must be a DNS subdomain: %s", strings.Join(problems, "; "))
	}
	return nil
}

func validateProfile(profile Profile) error {
	if err := validateIdentifier("backend", profile.Backend); err != nil {
		return err
	}
	if err := validateIdentifier("schedulerVersion", profile.SchedulerVersion); err != nil {
		return err
	}
	return validateIdentifier("configurationID", profile.ConfigurationID)
}

func validateIdentifier(name, value string) error {
	if value == "" {
		return fmt.Errorf("%s must not be blank", name)
	}
	if strings.TrimSpace(value) != value {
		return fmt.Errorf("%s must not have surrounding whitespace", name)
	}
	return nil
}
