package appstate

import (
	"context"
	"slices"
	"sort"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
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
	allViews, _, err := m.viewsLocked(bindings)
	if err != nil {
		return nil, nil, err
	}
	views := map[string]MemoryView{}
	locations := map[string]MemoryView{}
	for _, b := range bindings {
		if b.OrganizationID != organization {
			continue
		}
		v := allViews[b.InstallationID]
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
		obs := catalog.Observation{State: "unknown", Reason: "NOT_SYNCED", Images: []catalog.Image{}}
		if v, ok := locations[key]; ok && v.Resource.AppID == e.AppID {
			obs = catalogObservation(v)
			used[key] = true
		} else {
			for _, c := range m.collectors {
				if !slices.Contains(c.config.Namespaces, e.Namespace) {
					continue
				}
				if _, occupied := c.objects[key]; occupied {
					// No organization identity is inferred from a catalog name.
					obs.Reason = "WORKLOAD_BINDING_MISSING"
				} else if !c.inventoryAt.IsZero() && now.Before(c.inventoryExpires) {
					obs.State, obs.Reason, obs.ObservedAt = "not_installed", "RESOURCE_ABSENT", c.inventoryAt
				}
			}
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

func catalogObservation(v MemoryView) catalog.Observation {
	r := v.Resource
	o := catalog.Observation{State: v.Runtime.State, Reason: v.Runtime.ReasonCode, Images: []catalog.Image{}}
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
