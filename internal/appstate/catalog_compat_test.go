package appstate

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
)

func TestRegisteredCatalogInstallationPrecedence(t *testing.T) {
	m, stream, binding, object, now := memoryFixture(t, true)
	entry := catalogEntry(object)
	object.Name = "actual-management"
	object.Metadata = metadataFixture()
	object.Health = "ready"
	read := func(org string, bindings []Binding) catalog.Item {
		t.Helper()
		items, err := m.registeredCatalog(org, []catalog.Entry{entry}, bindings)
		if err != nil || len(items) != 1 {
			t.Fatal("registered metadata lost", err)
		}
		if items[0].WorkloadName != entry.WorkloadName || items[0].Version != entry.Version {
			t.Fatal("observation rewrote trusted release metadata")
		}
		return items[0]
	}
	if read(binding.OrganizationID, []Binding{binding}).Deployment.State != "unknown" {
		t.Fatal("startup invented readiness")
	}
	memoryFull(t, stream) // An empty catalog position cannot override an installation.
	if read(binding.OrganizationID, []Binding{binding}).Deployment.State != "unknown" {
		t.Fatal("missing installation evidence became not_installed")
	}
	memoryFull(t, stream, object)
	for _, health := range []string{"ready", "failed"} {
		object.Health = health
		if err := stream.Upsert(object); err != nil {
			t.Fatal(err)
		}
		item := read(binding.OrganizationID, []Binding{binding})
		view := memoryView(t, m, binding)
		if item.Deployment.State != view.Runtime.State || item.Deployment.UID != object.UID || item.Deployment.ManagementStatus != "managed" {
			t.Fatal("legacy state diverged from exact installation")
		}
	}
	*now = now.Add(31 * time.Second)
	item := read(binding.OrganizationID, []Binding{binding})
	if item.Deployment.State != "unknown" || item.Deployment.ObservedAt.Equal(*now) {
		t.Fatal("read renewed expired health or workload evidence")
	}
	other := binding
	other.InstallationID = uuid.NewString()
	item = read(binding.OrganizationID, []Binding{binding, other})
	if item.Deployment.State != "unknown" || item.Deployment.UID != "" || len(item.Deployment.Images) != 0 {
		t.Fatal("ambiguous installation selected an arbitrary resource")
	}
	items, err := m.registeredCatalog(binding.OrganizationID, nil, []Binding{binding})
	if err != nil || len(items) != 0 {
		t.Fatal("unregistered observation fabricated a release", err)
	}
}

func TestRegisteredCatalogIsolationAndLegacy(t *testing.T) {
	for _, variant := range []string{"foreign", "instance", "unbound", "wrong-installation"} {
		t.Run(variant, func(t *testing.T) {
			m, stream, b, object, _ := memoryFixture(t, false)
			e := catalogEntry(object)
			switch variant {
			case "foreign":
				object.OrganizationID = uuid.NewString()
			case "instance":
				object.InstanceID = uuid.NewString()
			case "unbound":
				object.InstallationID, object.OrganizationID, object.StationID = "", "", ""
			case "wrong-installation":
				object.InstallationID = uuid.NewString()
			}
			memoryFull(t, stream, object)
			for _, bindings := range [][]Binding{nil, {b}} {
				items, err := m.registeredCatalog(b.OrganizationID, []catalog.Entry{e}, bindings)
				if err != nil || len(items) != 1 || items[0].Deployment.State != "unknown" || items[0].Deployment.UID != "" || len(items[0].Deployment.Images) != 0 {
					t.Fatal("untrusted identity leaked through legacy catalog", err)
				}
			}
		})
	}
	m, stream, b, object, now := legacyFixture(t, false)
	e := catalogEntry(object)
	e.AppID = b.AppID
	memoryFull(t, stream, object)
	items, err := m.registeredCatalog(b.OrganizationID, []catalog.Entry{e}, nil)
	if err != nil || len(items) != 1 || items[0].Deployment.State != "ready" || items[0].Deployment.UID != b.WorkloadUID || items[0].Deployment.ManagementStatus != "unmanaged" {
		t.Fatal("controlled legacy projection changed", err)
	}
	foreign, err := m.registeredCatalog(uuid.NewString(), []catalog.Entry{e}, nil)
	if err != nil || foreign[0].Deployment.State != "unknown" || foreign[0].Deployment.UID != "" {
		t.Fatal("legacy visibility crossed organization")
	}
	*now = now.Add(31 * time.Second)
	items, err = m.registeredCatalog(b.OrganizationID, []catalog.Entry{e}, nil)
	if err != nil || items[0].Deployment.State != "unknown" {
		t.Fatal("expired legacy remained ready")
	}
}
