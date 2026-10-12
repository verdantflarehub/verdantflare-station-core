package appstate

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func legacyFixture(t *testing.T, health bool) (*Memory, *StreamSession, legacyBinding, ResourceObservation, *time.Time) {
	t.Helper()
	m, stream, owner, o, now := memoryFixture(t, health)
	e := catalogEntry(o)
	b := legacyBinding{BindingID: "legacy-fixture", AppID: o.AppID, OrganizationID: owner.OrganizationID, StationID: m.station, WorkloadUID: o.UID, ObservationKey: "obs:" + appcenter.Digest([]string{o.Namespace, o.Name, o.UID})[7:]}
	var err error
	m.legacy, err = validateLegacy(m.station, []legacyBinding{b}, []catalog.Entry{e})
	if err != nil {
		t.Fatal(err)
	}
	o.AppID, o.StationID, o.OrganizationID, o.InstallationID = "", "", "", ""
	o.Metadata = metadataFixture()
	o.Metadata.TemplateHash = ""
	o.Metadata.OwnershipMarked = false
	return m, stream, m.legacy[0], o, now
}

func readLegacy(t *testing.T, m *Memory, b legacyBinding) MemoryView {
	t.Helper()
	v, err := m.snapshotViews(nil)
	if err != nil {
		t.Fatal(err)
	}
	return v.legacy[b.OrganizationID][b.AppID]
}

func TestLegacyConfigPinsScopeCatalogAndUID(t *testing.T) {
	m, _, b, o, _ := legacyFixture(t, false)
	e := catalogEntry(o)
	e.AppID = b.AppID
	for _, variant := range []string{"station", "organization", "uid", "observation", "catalog", "duplicate", "duplicate-app", "duplicate-position"} {
		t.Run(variant, func(t *testing.T) {
			candidate, entries := []legacyBinding{b}, []catalog.Entry{e}
			switch variant {
			case "station":
				candidate[0].StationID = uuid.NewString()
			case "organization":
				candidate[0].OrganizationID = "*"
			case "uid":
				candidate[0].WorkloadUID = uuid.NewString()
			case "observation":
				candidate[0].ObservationKey = "obs:incorrect"
			case "catalog":
				entries = nil
			case "duplicate":
				candidate = append(candidate, b)
			case "duplicate-app":
				entries = append(entries, e)
			case "duplicate-position":
				other := e
				other.AppID = "other-app"
				entries = append(entries, other)
				second := b
				second.AppID = other.AppID
				second.BindingID = "other"
				candidate = append(candidate, second)
			}
			if _, err := validateLegacy(m.station, candidate, entries); err == nil {
				t.Fatal("unsafe legacy association accepted")
			}
		})
	}
}

func TestLegacyObservationSharedProjectionAndIsolation(t *testing.T) {
	m, stream, b, o, now := legacyFixture(t, false)
	if v := readLegacy(t, m, b); v.Runtime.State != "unknown" || v.Resource != nil {
		t.Fatal("configuration invented runtime evidence")
	}
	memoryFull(t, stream, o)
	v := readLegacy(t, m, b)
	if v.Runtime.State != "ready" || v.Resource == nil {
		t.Fatal("authorized legacy state missing")
	}
	v.Resource.Metadata.Images[0].Image = "mutated"
	if readLegacy(t, m, b).Resource.Metadata.Images[0].Image == "mutated" {
		t.Fatal("historical view leaked mutable inventory")
	}
	e := catalogEntry(o)
	e.AppID = b.AppID
	items, _, err := m.catalogSnapshot(b.OrganizationID, []catalog.Entry{e}, nil)
	if err != nil || items[0].Deployment.State != "ready" || items[0].Deployment.UID != b.WorkloadUID || len(items[0].Deployment.Ownership) != 0 {
		t.Fatal("facts changed historical ownership", err)
	}
	u := identity.IdentityContext{UserID: uuid.NewString(), OrganizationID: b.OrganizationID, StationID: m.station, Roles: []string{"admin"}, Scopes: []string{"app:read", "app:manage"}}
	facts, err := appcenter.BuildState(u, "fixture", *now, items, nil, nil, map[string]appcenter.Service{})
	if err != nil || len(facts.Items) != 1 || facts.Items[0].Origin != "observation" || facts.Items[0].Installation != nil || facts.Items[0].CardKey != b.ObservationKey || len(facts.Items[0].Lifecycle.Candidates) != 0 {
		t.Fatal("legacy projection fabricated installation or management capability", err)
	}
	foreign, _, err := m.catalogSnapshot(uuid.NewString(), []catalog.Entry{e}, nil)
	if err != nil || foreign[0].Deployment.UID != "" || len(foreign[0].Deployment.Images) != 0 || foreign[0].Deployment.State != "unknown" {
		t.Fatal("legacy resource exposed to another organization", err)
	}
	*now = now.Add(10 * time.Second)
	if stream.Heartbeat() != nil || readLegacy(t, m, b).Revision != v.Revision {
		t.Fatal("heartbeat changed semantic revision")
	}
	*now = now.Add(31 * time.Second)
	if stale := readLegacy(t, m, b); stale.Runtime.State != "unknown" || stale.Runtime.ReasonCode != "OBSERVATION_EXPIRED" || stale.Revision == v.Revision {
		t.Fatal("legacy view did not expire without events")
	}
}

func TestLegacyObservationRejectsReplacementOwnershipAndInstallation(t *testing.T) {
	for _, variant := range []string{"uid", "organization", "instance", "marker", "old-frame", "installed"} {
		t.Run(variant, func(t *testing.T) {
			m, stream, b, o, _ := legacyFixture(t, false)
			memoryFull(t, stream, o)
			bindings := []Binding{}
			switch variant {
			case "uid":
				o.UID = uuid.NewString()
			case "organization":
				o.OrganizationID = uuid.NewString()
			case "instance":
				o.InstanceID = uuid.NewString()
			case "marker":
				o.Metadata.OwnershipMarked = true
			case "old-frame":
				o.Metadata = nil
			case "installed":
				bindings = append(bindings, Binding{uuid.NewString(), b.OrganizationID, b.AppID})
			}
			if stream.Upsert(o) != nil {
				t.Fatal("stream failed")
			}
			v, err := m.snapshotViews(bindings)
			if err != nil {
				t.Fatal(err)
			}
			if variant == "installed" {
				if len(v.legacy) != 0 || v.installations[bindings[0].InstallationID].Runtime.State != "unknown" {
					t.Fatal("legacy registry bypassed true installation binding")
				}
			} else if view := v.legacy[b.OrganizationID][b.AppID]; view.Resource != nil || view.Runtime.ReasonCode != "IDENTITY_CONFLICT" {
				t.Fatal("unsafe historical resource was reused")
			}
		})
	}
}

func TestLegacyObservationAbsenceRestartAndHealth(t *testing.T) {
	m, stream, b, o, now := legacyFixture(t, true)
	memoryFull(t, stream, o)
	if readLegacy(t, m, b).Runtime.ReasonCode != "HEALTH_NOT_OBSERVED" {
		t.Fatal("legacy view bypassed required health")
	}
	o.Health = "ready"
	if stream.Upsert(o) != nil || readLegacy(t, m, b).Runtime.State != "ready" {
		t.Fatal("valid health missing")
	}
	*now = now.Add(20 * time.Second)
	if stream.Heartbeat() != nil {
		t.Fatal("heartbeat failed")
	}
	*now = now.Add(11 * time.Second)
	if readLegacy(t, m, b).Runtime.State != "unknown" {
		t.Fatal("heartbeat extended HTTP health")
	}
	stream.Close()
	restarted, err := NewMemory(m.station, []Collector{{ID: "runtime", Namespaces: []string{"apps"}}})
	if err != nil {
		t.Fatal(err)
	}
	restarted.legacy = append([]legacyBinding{}, m.legacy...)
	if v := readLegacy(t, restarted, b); v.Runtime.State != "unknown" || v.Resource != nil {
		t.Fatal("restart reused runtime evidence")
	}
	connection, err := restarted.Open("runtime")
	if err != nil {
		t.Fatal(err)
	}
	memoryFull(t, connection)
	if readLegacy(t, restarted, b).Runtime.State != "absent" {
		t.Fatal("complete inventory failed to prove absence")
	}
}
