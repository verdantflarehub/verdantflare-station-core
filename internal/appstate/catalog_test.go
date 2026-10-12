package appstate

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func TestMemoryCatalogFactsSatisfyConsumerContract(t *testing.T) {
	for _, state := range []string{"ready", "starting", "stopping", "stopped", "degraded", "absent", "unknown"} {
		t.Run(state, func(t *testing.T) {
			_, _, b, o, now := memoryFixture(t, false)
			e := catalogEntry(o)
			e.Source = "deploys/apps/fixture.dp.yaml"
			o.Metadata = metadataFixture()
			v := MemoryView{Runtime: Runtime{State: state, ReasonCode: "OBSERVATION_EXPIRED", ObservedAt: now}, Resource: &o}
			item := catalog.SnapshotItem{Registered: true, Item: catalog.Item{Entry: e, ModelStatus: "not_required", Deployment: catalogObservation(v)}}
			item.Deployment.Ownership = nil
			user := identity.IdentityContext{UserID: uuid.Must(uuid.NewV7()).String(), OrganizationID: b.OrganizationID, StationID: o.StationID, Roles: []string{"admin"}, Scopes: []string{"app:read"}}
			facts, err := appcenter.BuildState(user, uuid.Must(uuid.NewV7()).String(), *now, []catalog.SnapshotItem{item}, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(facts)
			if err != nil {
				t.Fatal(err)
			}
			var decoded appcenter.Facts
			if appcenter.Decode(bytes.NewReader(raw), "facts", 4<<20, &decoded) != nil {
				t.Fatal("Core memory projection violates Studio's central facts schema")
			}
		})
	}
}

func catalogEntry(o ResourceObservation) catalog.Entry {
	return catalog.Entry{AppID: o.AppID, Namespace: o.Namespace, WorkloadName: o.Name, DisplayName: "Trusted display", GroupID: "system", Brand: "vf", Version: "1.0.0", Source: "deploys/apps/fixture.dp.yaml", Models: []string{}, Images: []catalog.Image{{Component: "server", Image: "example.invalid/declared:1"}}}
}

func TestMemoryCatalogFactsNonUTCClock(t *testing.T) {
	for _, offset := range []int{8 * 3600, -7 * 3600} {
		for _, mode := range []string{"observed", "absent", "expired"} {
			m, s, b, o, now := memoryFixture(t, false)
			*now = now.In(time.FixedZone("test", offset))
			o.Metadata = metadataFixture()
			observed := *now
			want := "ready"
			if mode == "absent" {
				memoryFull(t, s)
				want = "not_installed"
			} else {
				memoryFull(t, s, o)
			}
			if mode == "expired" {
				*now = now.Add(31 * time.Second)
				want = "unknown"
			}
			items, _, err := m.catalogSnapshot(b.OrganizationID, []catalog.Entry{catalogEntry(o)}, []Binding{b})
			if err != nil || len(items) != 1 || items[0].Deployment.State != want {
				t.Fatalf("offset=%d mode=%s projection failed: %v", offset, mode, err)
			}
			stamp := items[0].Deployment.ObservedAt
			if stamp.Location() != time.UTC || !stamp.Equal(observed) {
				t.Fatalf("offset=%d mode=%s timestamp changed instant or is not UTC: %v", offset, mode, stamp)
			}
			user := identity.IdentityContext{UserID: uuid.NewString(), OrganizationID: b.OrganizationID, StationID: o.StationID, Roles: []string{"admin"}, Scopes: []string{"app:read"}}
			facts, err := appcenter.BuildState(user, uuid.NewString(), *now, items, nil, nil, nil)
			if err != nil {
				t.Fatal(err)
			}
			raw, err := json.Marshal(facts)
			if err != nil {
				t.Fatal(err)
			}
			var decoded appcenter.Facts
			if err := appcenter.Decode(bytes.NewReader(raw), "facts", 4<<20, &decoded); err != nil {
				t.Fatalf("offset=%d mode=%s violates consumer facts schema: %v", offset, mode, err)
			}
		}
	}
}

func TestMemoryCatalogUsesScopedCoherentProjection(t *testing.T) {
	m, s, b, o, now := memoryFixture(t, false)
	o.Metadata = metadataFixture()
	e := catalogEntry(o)
	read := func(org string) ([]catalog.SnapshotItem, map[string]MemoryView) {
		t.Helper()
		items, views, err := m.catalogSnapshot(org, []catalog.Entry{e}, []Binding{b})
		if err != nil {
			t.Fatal(err)
		}
		user := identity.IdentityContext{UserID: uuid.Must(uuid.NewV7()).String(), OrganizationID: org, StationID: o.StationID, Roles: []string{"admin"}, Scopes: []string{"app:read"}}
		facts, err := appcenter.BuildState(user, uuid.Must(uuid.NewV7()).String(), *now, items, nil, nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(facts)
		var decoded appcenter.Facts
		if appcenter.Decode(bytes.NewReader(raw), "facts", 4<<20, &decoded) != nil {
			t.Fatal("memory lifecycle violated facts contract")
		}
		return items, views
	}
	items, views := read(b.OrganizationID)
	if len(items) != 1 || items[0].Deployment.State != "unknown" || views[b.InstallationID].Runtime.State != "unknown" {
		t.Fatal("startup lost catalog or fabricated observation")
	}
	memoryFull(t, s, o)
	items, views = read(b.OrganizationID)
	if len(items) != 1 || !items[0].Registered || items[0].DisplayName != e.DisplayName || items[0].Deployment.State != "ready" || views[b.InstallationID].Runtime.State != "ready" || items[0].Images[0].Image == items[0].Deployment.Images[0].Image {
		t.Fatal("trusted release metadata or runtime evidence was conflated")
	}
	items[0].Deployment.Images[0].Image = "mutated"
	views[b.InstallationID].Resource.Metadata.Images[0].Image = "mutated"
	items, _ = read(b.OrganizationID)
	if items[0].Deployment.Images[0].Image != "example.invalid/fixture:1" {
		t.Fatal("projection exposed mutable inventory")
	}
	items, views = read(uuid.NewString())
	if len(items) != 1 || len(views) != 0 || items[0].Deployment.UID != "" || items[0].Deployment.State != "unknown" || len(items[0].Deployment.Images) != 0 {
		t.Fatal("foreign observation leaked through registered catalog")
	}
	*now = now.Add(31 * time.Second)
	items, views = read(b.OrganizationID)
	if items[0].Deployment.State != "unknown" || views[b.InstallationID].Runtime.ReasonCode != "OBSERVATION_EXPIRED" || items[0].Deployment.ObservedAt.Equal(*now) {
		t.Fatal("read renewed stale evidence")
	}
}

func TestMemoryCatalogAbsenceNeedsFreshCompleteInventory(t *testing.T) {
	m, s, b, o, now := memoryFixture(t, false)
	e := catalogEntry(o)
	memoryFull(t, s)
	items, views, err := m.catalogSnapshot(b.OrganizationID, []catalog.Entry{e}, []Binding{b})
	if err != nil || items[0].Deployment.State != "not_installed" || views[b.InstallationID].Runtime.State != "unknown" {
		t.Fatal("catalog absence fabricated installed resource absence", err)
	}
	*now = now.Add(20 * time.Second)
	other := o
	other.Name = "different-resource"
	if s.Upsert(other) != nil {
		t.Fatal("upsert failed")
	}
	*now = now.Add(11 * time.Second)
	items, _, err = m.catalogSnapshot(b.OrganizationID, []catalog.Entry{e}, nil)
	if err != nil || items[0].Deployment.State != "unknown" {
		t.Fatal("single resource delta renewed missing-position proof")
	}
	if s.Heartbeat() != nil {
		t.Fatal("heartbeat failed")
	}
	items, _, err = m.catalogSnapshot(b.OrganizationID, []catalog.Entry{e}, nil)
	if err != nil || items[0].Deployment.State != "not_installed" {
		t.Fatal("healthy inventory did not renew catalog absence")
	}
}

func TestMemoryCatalogQuarantinesUnboundAndInstanceResources(t *testing.T) {
	for _, variant := range []string{"unowned", "foreign", "instance", "unknown-installation"} {
		t.Run(variant, func(t *testing.T) {
			m, s, b, o, _ := memoryFixture(t, false)
			e := catalogEntry(o)
			switch variant {
			case "unowned":
				o.StationID, o.OrganizationID, o.InstallationID = "", "", ""
			case "foreign":
				o.OrganizationID = uuid.NewString()
			case "instance":
				o.InstanceID = uuid.NewString()
			case "unknown-installation":
				o.InstallationID = uuid.NewString()
			}
			memoryFull(t, s, o)
			items, views, err := m.catalogSnapshot(b.OrganizationID, []catalog.Entry{e}, []Binding{b})
			if err != nil || len(items) != 1 || items[0].Deployment.UID != "" || items[0].Deployment.State != "unknown" || views[b.InstallationID].Runtime.State != "unknown" {
				t.Fatal("unbound resource exposed or marked absent", err)
			}
		})
	}
}
