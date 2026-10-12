package appstate

import (
	"context"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

// PreinstalledObservation is a candidate inspection, not an installation or
// public app state. tx also owns the caller's durable installation history read.
func (s *Service) PreinstalledObservation(ctx context.Context, tx pgx.Tx, user identity.IdentityContext, app string) (catalog.Observation, error) {
	unknown := unknownPreinstalled()
	if s == nil || s.Catalog == nil || tx == nil {
		return unknown, ErrUnavailable
	}
	if !validID(s.StationID) || user.StationID != s.StationID || !validID(user.OrganizationID) || !validID(user.UserID) || !slices.Contains(user.Roles, "admin") || !slices.Contains(user.Scopes, "app:read") {
		return unknown, identity.Denied
	}
	entry, exists := s.Catalog.Entry(app)
	if !exists {
		return unknown, nil
	}
	bindings, err := s.readBindings(ctx, tx, "")
	if err != nil {
		return unknown, err
	}
	return s.memory().preinstalledObservation(user.OrganizationID, entry, bindings, slices.Contains(user.Scopes, "app:manage"))
}

func unknownPreinstalled() catalog.Observation {
	return catalog.Observation{State: "unknown", Reason: "unavailable", ManagementStatus: "unknown", Images: []catalog.Image{}}
}

func (m *Memory) preinstalledObservation(organization string, entry catalog.Entry, bindings []Binding, canManage bool) (catalog.Observation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	unknown := unknownPreinstalled()
	snapshot, err := m.snapshotLocked(bindings)
	if err != nil {
		return unknown, err
	}
	var own []Binding
	for _, b := range bindings {
		if b.AppID != entry.AppID {
			continue
		}
		if b.OrganizationID != organization {
			return unknown, nil
		}
		own = append(own, b)
	}
	if len(own) > 1 {
		return unknown, nil
	}
	project := func(view MemoryView) catalog.Observation {
		if view.Runtime.Quality != "known" {
			return unknown
		}
		item := catalog.Item{Entry: entry, Deployment: catalogObservation(view)}
		item.ProjectManagement(m.station, organization)
		return item.Deployment
	}
	if len(own) == 1 {
		view := snapshot.installations[own[0].InstallationID]
		if view.Resource != nil {
			return project(view), nil
		}
		if view.Runtime.ReasonCode == "IDENTITY_CONFLICT" {
			return unknown, nil
		}
	}
	now := m.now()
	for _, collector := range m.collectors {
		if !slices.Contains(collector.config.Namespaces, entry.Namespace) {
			continue
		}
		object, exists := collector.objects[entry.Namespace+"/"+entry.WorkloadName]
		if !exists {
			observation := m.catalogPosition(entry, now)
			observation.ManagementStatus = "unknown"
			return observation, nil
		}
		// The explicit trusted position and current management permission grant
		// candidate visibility. Resource labels never grant ownership or access.
		if !canManage || object.Metadata == nil || object.Metadata.OwnershipMarked || object.StationID != "" || object.OrganizationID != "" || object.InstallationID != "" || object.AppID != "" || object.InstanceID != "" {
			return unknown, nil
		}
		fact := object.ResourceObservation
		if !now.Before(object.healthExpires) {
			fact.Health = "not_checked"
		}
		state, reason := evaluate(fact, slices.Contains(collector.config.HealthRequiredApps, entry.AppID))
		view := RuntimeView(now, &state, &reason, &object.received, &object.expires, false)
		if view.Quality != "known" {
			return unknown, nil
		}
		observation := catalogObservation(MemoryView{Runtime: view, Resource: &fact})
		observation.Ownership = nil
		observation.ManagementStatus = "unmanaged"
		return observation, nil
	}
	return unknown, nil
}
