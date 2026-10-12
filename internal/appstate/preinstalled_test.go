package appstate

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
)

func TestPreinstalledMemoryCandidateLifecycle(t *testing.T) {
	m, stream, b, object, now := memoryFixture(t, false)
	entry := catalogEntry(object)
	read := func() string {
		t.Helper()
		v, err := m.preinstalledObservation(b.OrganizationID, entry, nil, true)
		if err != nil {
			t.Fatal(err)
		}
		return v.State
	}
	if read() != "unknown" {
		t.Fatal("startup manufactured absence")
	}
	memoryFull(t, stream)
	if read() != "not_installed" {
		t.Fatal("complete empty inventory ignored")
	}
	object.StationID, object.OrganizationID, object.InstallationID, object.AppID = "", "", "", ""
	object.Metadata = &ResourceMetadata{Images: []ResourceImage{{Component: "server", Image: "example.invalid/test:1.0.0"}}, ImagesComplete: true}
	if err := stream.Upsert(object); err != nil {
		t.Fatal(err)
	}
	v, err := m.preinstalledObservation(b.OrganizationID, entry, nil, true)
	if err != nil || v.State != "ready" || v.UID != object.UID || v.ManagementStatus != "unmanaged" || v.Ownership != nil {
		t.Fatal("candidate missing or granted ownership", v, err)
	}
	// Inventory cannot inject this unbound candidate into the public catalog.
	items, _, err := m.catalogSnapshot(b.OrganizationID, []catalog.Entry{entry}, nil)
	if err != nil || items[0].Deployment.State != "unknown" || items[0].Deployment.UID != "" {
		t.Fatal("candidate leaked to public catalog", err)
	}
	object.UID = uuid.NewString()
	if err := stream.Upsert(object); err != nil {
		t.Fatal(err)
	}
	replaced, err := m.preinstalledObservation(b.OrganizationID, entry, nil, true)
	if err != nil || replaced.UID == v.UID || replaced.UID != object.UID {
		t.Fatal("UID replacement hidden", err)
	}
	*now = now.Add(31 * time.Second)
	if read() != "unknown" {
		t.Fatal("stale candidate remained available")
	}
	stale, _ := m.preinstalledObservation(b.OrganizationID, entry, nil, true)
	if stale.UID != "" {
		t.Fatal("stale UID remained actionable")
	}
}

func TestPreinstalledMemoryCandidateIsolation(t *testing.T) {
	for _, mode := range []string{"read-only", "foreign-installation", "foreign-resource", "instance", "missing-metadata", "ownership-marked", "duplicate-installation", "unregistered-position"} {
		t.Run(mode, func(t *testing.T) {
			m, stream, b, object, _ := memoryFixture(t, false)
			entry := catalogEntry(object)
			object.StationID, object.OrganizationID, object.InstallationID, object.AppID = "", "", "", ""
			object.Metadata = &ResourceMetadata{Images: []ResourceImage{{Component: "server", Image: "example.invalid/test:1.0.0"}}, ImagesComplete: true}
			manage := true
			bindings := []Binding{}
			switch mode {
			case "read-only":
				manage = false
			case "foreign-installation":
				b.OrganizationID = uuid.NewString()
				bindings = append(bindings, b)
			case "foreign-resource":
				object.OrganizationID = uuid.NewString()
			case "instance":
				object.InstanceID = uuid.NewString()
			case "missing-metadata":
				object.Metadata = nil
			case "ownership-marked":
				object.Metadata.OwnershipMarked = true
			case "duplicate-installation":
				bindings = append(bindings, b, Binding{uuid.NewString(), b.OrganizationID, b.AppID})
			case "unregistered-position":
				entry.Namespace = "outside"
			}
			memoryFull(t, stream, object)
			org := b.OrganizationID
			if mode == "foreign-installation" {
				org = uuid.NewString()
			}
			view, err := m.preinstalledObservation(org, entry, bindings, manage)
			if err != nil || view.State != "unknown" || view.UID != "" {
				t.Fatal("candidate scope escaped", view, err)
			}
		})
	}
}

func TestPreinstalledMemoryManagedInstallationPriority(t *testing.T) {
	m, stream, b, object, now := memoryFixture(t, false)
	entry := catalogEntry(object)
	object.Name = "actual-installed-location"
	object.Metadata = metadataFixture()
	memoryFull(t, stream, object)
	view, err := m.preinstalledObservation(b.OrganizationID, entry, []Binding{b}, false)
	if err != nil || view.State != "ready" || view.ManagementStatus != "managed" || view.UID != object.UID {
		t.Fatal("exact installed identity lost", view, err)
	}
	*now = now.Add(31 * time.Second)
	view, err = m.preinstalledObservation(b.OrganizationID, entry, []Binding{b}, true)
	if err != nil || view.State != "unknown" || view.UID != "" {
		t.Fatal("expired installation fell back to absence", view, err)
	}
}
