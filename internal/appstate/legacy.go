package appstate

import (
	"context"
	"os"
	"slices"
	"sort"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
)

// These are observations authorized by the existing Studio registry, never
// synthetic installations or workload ownership. Only Core's startup loads it.
type legacyBinding struct {
	BindingID       string `json:"bindingId"`
	AppID           string `json:"appId"`
	OrganizationID  string `json:"organizationId"`
	StationID       string `json:"stationId"`
	ObservationKey  string `json:"observationKey"`
	WorkloadUID     string `json:"workloadUID"`
	namespace, name string
}

func (s *Service) LoadLegacy(ctx context.Context, path string) error {
	if s == nil || !validID(s.StationID) || ctx.Err() != nil {
		return ErrUnavailable
	}
	var bindings []legacyBinding
	if path != "" {
		if s.Pool == nil {
			return ErrUnavailable
		}
		f, err := os.Open(path)
		if err != nil {
			return ErrUnavailable
		}
		defer f.Close()
		info, err := f.Stat()
		if err != nil || !info.Mode().IsRegular() || info.Size() > 1<<20 {
			return ErrStreamFrame
		}
		var input struct {
			Records []legacyBinding `json:"records"`
		}
		// Validate the entire existing closed contract before decoding the
		// subset Core owns. Route/adapter/backend fields never cause requests.
		if appcenter.Decode(f, "legacy", 1<<20, &input) != nil {
			return ErrStreamFrame
		}
		bindings, err = validateLegacy(s.StationID, input.Records, s.Catalog.Entries())
		if err != nil {
			return err
		}
		organizations := map[string]bool{}
		for _, b := range bindings {
			organizations[b.OrganizationID] = true
		}
		for organization := range organizations {
			var exists bool
			if s.Pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.organizations WHERE organization_id=$1)`, organization).Scan(&exists) != nil {
				return ErrUnavailable
			}
			if !exists {
				return ErrStreamFrame
			}
		}
	}
	m := s.memory()
	m.mu.Lock()
	defer m.mu.Unlock()
	m.legacy = slices.Clone(bindings)
	return nil
}

func validateLegacy(station string, bindings []legacyBinding, entries []catalog.Entry) ([]legacyBinding, error) {
	if !validID(station) || len(bindings) > 128 || len(entries) > 1000 {
		return nil, ErrStreamFrame
	}
	registry := map[string]catalog.Entry{}
	for _, entry := range entries {
		if _, duplicate := registry[entry.AppID]; duplicate {
			return nil, ErrStreamFrame
		}
		registry[entry.AppID] = entry
	}
	bindings = slices.Clone(bindings)
	seenIDs, seenApps, seenPositions, seenUIDs := map[string]bool{}, map[string]bool{}, map[string]bool{}, map[string]bool{}
	for i := range bindings {
		b := &bindings[i]
		e, exists := registry[b.AppID]
		matchedNS, matchedName := e.Namespace, e.WorkloadName
		if exists && b.ObservationKey != "obs:"+appcenter.Digest([]string{matchedNS, matchedName, b.WorkloadUID})[7:] {
			legacyCandidates := [][2]string{}
			switch e.AppID {
			case "image-mcp":
				legacyCandidates = append(legacyCandidates, [2]string{"verdantflare-app", "image-mcp-server"}, [2]string{"verdantflare-image", "image-mcp-server"})
			case "video-mcp":
				legacyCandidates = append(legacyCandidates, [2]string{"verdantflare-app", "video-mcp-server"}, [2]string{"verdantflare-video", "video-mcp-server"})
			case "blender":
				legacyCandidates = append(legacyCandidates, [2]string{"verdantflare-app", "blender-manager"}, [2]string{"verdantflare-app", "blender"}, [2]string{"verdantflare-blender", "blender"})
			}
			for _, cand := range legacyCandidates {
				if b.ObservationKey == "obs:"+appcenter.Digest([]string{cand[0], cand[1], b.WorkloadUID})[7:] {
					matchedNS, matchedName = cand[0], cand[1]
					break
				}
			}
		}
		key := matchedNS + "/" + matchedName
		if !exists || b.BindingID == "" || seenIDs[b.BindingID] || seenApps[b.AppID] || seenPositions[key] || seenUIDs[b.WorkloadUID] ||
			b.StationID != station || !validID(b.OrganizationID) || !validID(b.WorkloadUID) ||
			!dns(matchedNS) || !dns(matchedName) ||
			b.ObservationKey != "obs:"+appcenter.Digest([]string{matchedNS, matchedName, b.WorkloadUID})[7:] {
			return nil, ErrStreamFrame
		}
		b.namespace, b.name = matchedNS, matchedName
		seenIDs[b.BindingID], seenApps[b.AppID], seenPositions[key], seenUIDs[b.WorkloadUID] = true, true, true, true
	}
	sort.Slice(bindings, func(i, j int) bool { return bindings[i].AppID < bindings[j].AppID })
	return bindings, nil
}

type memorySnapshot struct {
	installations map[string]MemoryView
	legacy        map[string]map[string]MemoryView
	revisions     map[string]string
}

func (m *Memory) snapshotViews(bindings []Binding) (memorySnapshot, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.snapshotLocked(bindings)
}

// All projections share one lock and the caller's persistent identity snapshot.
func (m *Memory) snapshotLocked(bindings []Binding) (memorySnapshot, error) {
	views, revisions, err := m.viewsLocked(bindings)
	if err != nil {
		return memorySnapshot{}, err
	}
	out := memorySnapshot{views, map[string]map[string]MemoryView{}, revisions}
	installed := map[string]bool{}
	for _, b := range bindings {
		installed[b.AppID] = true
	}
	now := m.now()
	for _, b := range m.legacy {
		if installed[b.AppID] {
			continue
		}
		view := m.legacyView(b, now)
		if out.legacy[b.OrganizationID] == nil {
			out.legacy[b.OrganizationID] = map[string]MemoryView{}
		}
		out.legacy[b.OrganizationID][b.AppID] = view
	}
	for org, apps := range out.legacy {
		base := revisions[org]
		if base == "" {
			base = m.token(0)
		}
		tokens := map[string]string{}
		for app, view := range apps {
			tokens[app] = view.Revision
		}
		out.revisions[org] = m.boot + ":" + appcenter.Digest(struct {
			Base string
			Apps map[string]string
		}{base, tokens})[7:]
	}
	return out, nil
}

func (m *Memory) legacyView(b legacyBinding, now time.Time) MemoryView {
	v := MemoryView{Runtime: Runtime{State: "unknown", Quality: "unknown", ReasonCode: "NOT_SYNCED"}}
	metadata := ""
	for _, c := range m.collectors {
		if !slices.Contains(c.config.Namespaces, b.namespace) {
			continue
		}
		if o, exists := c.objects[b.namespace+"/"+b.name]; exists {
			if o.UID != b.WorkloadUID || o.StationID != "" || o.OrganizationID != "" || o.InstallationID != "" || o.InstanceID != "" || o.AppID != "" || o.Metadata == nil || o.Metadata.OwnershipMarked {
				v.Runtime.ReasonCode = "IDENTITY_CONFLICT"
				break
			}
			fact := o.ResourceObservation
			if now.Before(o.expires) && !now.Before(o.healthExpires) {
				fact.Health = "not_checked"
			}
			state, reason := evaluate(fact, slices.Contains(c.config.HealthRequiredApps, b.AppID))
			v.Runtime = RuntimeView(now, &state, &reason, &o.received, &o.expires, false)
			resource := sampled(o.ResourceObservation, o.received).ResourceObservation
			v.Resource = &resource
			metadata = o.metadataDigest
		} else if !c.inventoryAt.IsZero() {
			state, reason := "absent", "RESOURCE_ABSENT"
			v.Runtime = RuntimeView(now, &state, &reason, &c.inventoryAt, &c.inventoryExpires, false)
		}
		break
	}
	last := ""
	if v.Runtime.LastKnownState != nil {
		last = *v.Runtime.LastKnownState
	}
	v.Revision = m.boot + ":" + appcenter.Digest([]string{b.ObservationKey, v.Runtime.State, v.Runtime.Quality, v.Runtime.ReasonCode, last, metadata})[7:]
	return v
}
