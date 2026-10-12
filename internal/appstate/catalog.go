package appstate

import (
	"context"
	"slices"
	"sort"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

// CatalogSnapshot joins trusted metadata to the same memory projection used by
// the independent API. tx belongs to the caller's installation/operation read.
func (s *Service) CatalogSnapshot(ctx context.Context, tx pgx.Tx, organization string, entries []catalog.Entry) ([]catalog.SnapshotItem, map[string]MemoryView, error) {
	if s == nil || !validID(s.StationID) || !validID(organization) || tx == nil {
		return nil, nil, ErrUnavailable
	}
	bindings, err := s.readBindings(ctx, tx, "")
	if err != nil {
		return nil, nil, err
	}
	return s.memory().catalogSnapshot(organization, entries, bindings)
}

func (m *Memory) catalogSnapshot(organization string, entries []catalog.Entry, bindings []Binding) ([]catalog.SnapshotItem, map[string]MemoryView, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validID(organization) || len(entries) > 1000 {
		return nil, nil, ErrLimit
	}
	snapshot, err := m.snapshotLocked(bindings)
	if err != nil {
		return nil, nil, err
	}
	return m.catalogSnapshotLocked(organization, entries, bindings, snapshot)
}

// catalogSnapshotLocked only adapts metadata; snapshot owns state interpretation.
func (m *Memory) catalogSnapshotLocked(organization string, entries []catalog.Entry, bindings []Binding, snapshot memorySnapshot) ([]catalog.SnapshotItem, map[string]MemoryView, error) {
	views := map[string]MemoryView{}
	locations := map[string]MemoryView{}
	for _, b := range bindings {
		if b.OrganizationID != organization {
			continue
		}
		v := snapshot.installations[b.InstallationID]
		views[b.InstallationID] = v
		if v.Resource != nil {
			locations[v.Resource.Namespace+"/"+v.Resource.Name] = v
		}
	}
	now := m.now()
	items := make([]catalog.SnapshotItem, 0, len(entries)+len(locations))
	used := map[string]bool{}
	for _, e := range entries {
		e.Models, e.Images = slices.Clone(e.Models), slices.Clone(e.Images)
		key := e.Namespace + "/" + e.WorkloadName
		obs := m.catalogPosition(e, now)
		if v, ok := locations[key]; ok && v.Resource.AppID == e.AppID {
			obs = catalogObservation(v)
			used[key] = true
		} else if v, ok := snapshot.legacy[organization][e.AppID]; ok {
			obs = catalogObservation(v)
			// Historical observation visibility does not create ownership.
			obs.Ownership = nil
		}
		modelStatus := "unknown"
		if len(e.Models) == 0 {
			modelStatus = "not_required"
		}
		items = append(items, catalog.SnapshotItem{Item: catalog.Item{Entry: e, ModelStatus: modelStatus, Deployment: obs}, Registered: true})
	}
	for key, v := range locations {
		if used[key] {
			continue
		}
		r := v.Resource
		// The exact installation binding owns visibility; discovery labels do
		// not grant a release or a second installation candidate.
		e := catalog.Entry{AppID: r.AppID, Namespace: r.Namespace, WorkloadName: r.Name}
		items = append(items, catalog.SnapshotItem{Item: catalog.Item{Entry: e, ModelStatus: "unknown", Deployment: catalogObservation(v)}})
	}
	if len(items) > 1000 {
		return nil, nil, ErrLimit
	}
	sort.Slice(items, func(i, j int) bool {
		a, b := items[i], items[j]
		return a.AppID+"/"+a.Namespace+"/"+a.WorkloadName < b.AppID+"/"+b.Namespace+"/"+b.WorkloadName
	})
	return items, views, nil
}

// RegisteredCatalog serves the legacy public catalog without network discovery.
// Missing release metadata remains visible through the installation APIs, not
// through invented legacy catalog entries.
func (s *Service) RegisteredCatalog(ctx context.Context, user identity.IdentityContext) ([]catalog.Item, error) {
	if s == nil || user.StationID != s.StationID || !validID(user.StationID) || !validID(user.OrganizationID) || !validID(user.UserID) || !slices.Contains(user.Roles, "admin") || !slices.Contains(user.Scopes, "app:read") {
		return nil, identity.Denied
	}
	if s.Pool == nil || s.Catalog == nil {
		return nil, ErrUnavailable
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rollback(tx)
	bindings, err := s.readBindings(ctx, tx, "")
	if err != nil {
		return nil, err
	}
	items, err := s.memory().registeredCatalog(user.OrganizationID, s.Catalog.Entries(), bindings)
	if err != nil {
		return nil, err
	}
	if err = tx.Commit(ctx); err != nil {
		return nil, ErrUnavailable
	}
	return items, nil
}

func (m *Memory) registeredCatalog(organization string, entries []catalog.Entry, bindings []Binding) ([]catalog.Item, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !validID(organization) || len(entries) > 1000 {
		return nil, ErrLimit
	}
	snapshot, err := m.snapshotLocked(bindings)
	if err != nil {
		return nil, err
	}
	items, _, err := m.catalogSnapshotLocked(organization, entries, bindings, snapshot)
	if err != nil {
		return nil, err
	}
	byApp := map[string][]string{}
	for _, b := range bindings {
		if b.OrganizationID == organization {
			byApp[b.AppID] = append(byApp[b.AppID], b.InstallationID)
		}
	}
	out := make([]catalog.Item, 0, len(entries))
	for _, item := range items {
		if !item.Registered {
			continue
		}
		ids := byApp[item.AppID]
		if len(ids) == 1 {
			item.Deployment = catalogObservation(snapshot.installations[ids[0]])
		} else if len(ids) > 1 {
			item.Deployment = catalogObservation(MemoryView{})
		}
		item.ProjectManagement(m.station, organization)
		out = append(out, item.Item)
	}
	return out, nil
}

// catalogPosition requires m.mu. A trusted position grants no ownership;
// foreign/unbound/instance occupants expose only the existing unknown result.
func (m *Memory) catalogPosition(e catalog.Entry, now time.Time) catalog.Observation {
	obs := catalog.Observation{State: "unknown", Reason: "unavailable", Images: []catalog.Image{}}
	for _, c := range m.collectors {
		if !slices.Contains(c.config.Namespaces, e.Namespace) {
			continue
		}
		if _, occupied := c.objects[e.Namespace+"/"+e.WorkloadName]; occupied {
			obs.Reason = "unavailable"
		} else if !c.inventoryAt.IsZero() && now.Before(c.inventoryExpires) {
			obs.State, obs.Reason, obs.ObservedAt = "not_installed", "not_found", c.inventoryAt.UTC()
		}
	}
	return obs
}

func catalogObservation(v MemoryView) catalog.Observation {
	r := v.Resource
	// The embedded v1 catalog retains its closed vocabulary. Detailed reasons
	// belong to the independent state API and the installation service view.
	state, reason := catalogState(v.Runtime)
	o := catalog.Observation{State: state, Reason: reason, Images: []catalog.Image{}}
	if v.Runtime.ObservedAt != nil {
		o.ObservedAt = *v.Runtime.ObservedAt
	}
	if r == nil {
		return o
	}
	o.UID, o.Desired, o.Ready = r.UID, &r.Desired, &r.Ready
	o.Ownership = map[string]string{
		"station.verdantflare.com/station-id":      r.StationID,
		"station.verdantflare.com/organization-id": r.OrganizationID,
		"station.verdantflare.com/app-id":          r.AppID,
		"station.verdantflare.com/installation-id": r.InstallationID,
	}
	if r.Metadata != nil {
		o.Ownership["station.verdantflare.com/template-hash"] = r.Metadata.TemplateHash
		if r.Metadata.ImagesComplete {
			for _, image := range r.Metadata.Images {
				o.Images = append(o.Images, catalog.Image{Component: image.Component, Image: image.Image})
			}
		}
	}
	return o
}

func catalogState(runtime Runtime) (string, string) {
	switch runtime.State {
	case "absent":
		return "not_installed", "not_found"
	case "ready":
		return "ready", "replicas_ready"
	case "starting":
		return "starting", "reconciling"
	case "stopping":
		return "stopping", "scaling_down"
	case "stopped":
		return "stopped", "scaled_to_zero"
	case "degraded":
		return "degraded", "deployment_failed"
	default:
		return "unknown", "unavailable"
	}
}
