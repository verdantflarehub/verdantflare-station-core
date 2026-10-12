package appstate

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/uuid"
)

func metadataFixture() *ResourceMetadata {
	return &ResourceMetadata{OwnershipMarked: true, TemplateHash: strings.Repeat("a", 64), ImagesComplete: true,
		Images:    []ResourceImage{{Component: "server", Image: "example.invalid/fixture:1"}},
		Discovery: ResourceDiscovery{Enabled: true, AppID: "fixture-app", GroupID: "system", Brand: "vf", Version: "1.0.0", DisplayName: "Fixture"}}
}

func TestMemoryMetadataCopyAndFrameValidation(t *testing.T) {
	m, s, b, o, _ := memoryFixture(t, false)
	o.Metadata = metadataFixture()
	validateCollector(t, "resource", o)
	memoryFull(t, s, o)
	before := memoryView(t, m, b).Revision
	o.Metadata.TemplateHash = "changed-after-snapshot"
	o.Metadata.Images[0].Image = "changed-after-snapshot"
	o.Metadata.Discovery.DisplayName = "changed-after-snapshot"
	stored := m.collectors["runtime"].objects[o.Namespace+"/"+o.Name].Metadata
	if stored.TemplateHash != strings.Repeat("a", 64) || stored.Images[0].Image != "example.invalid/fixture:1" || stored.Discovery.DisplayName != "Fixture" {
		t.Fatal("caller changed accepted inventory")
	}
	o.Metadata = metadataFixture()
	o.Metadata.Images[0].Image = "example.invalid/fixture:2"
	if err := s.Upsert(o); err != nil {
		t.Fatal(err)
	}
	o.Metadata.Images[0].Image = "changed-after-upsert"
	if got := m.collectors["runtime"].objects[o.Namespace+"/"+o.Name].Metadata.Images[0].Image; got != "example.invalid/fixture:2" {
		t.Fatal("caller changed accepted delta")
	}
	if memoryView(t, m, b).Runtime.State != "ready" {
		t.Fatal("presentation metadata changed runtime readiness")
	}
	changed := memoryView(t, m, b).Revision
	if changed == before {
		t.Fatal("image change did not advance volatile state revision")
	}
	if err := s.Heartbeat(); err != nil || memoryView(t, m, b).Revision != changed {
		t.Fatal("heartbeat changed metadata revision", err)
	}
	// A malformed nested object must not replace the previous complete inventory.
	raw, _ := json.Marshal(Upsert{SchemaVersion: 1, Kind: "upsert", Object: o})
	raw = []byte(strings.Replace(string(raw), `"imagesComplete":true,`, "", 1))
	if err := s.Receive(raw); err == nil {
		t.Fatal("missing nested required field accepted")
	}
	if got := m.collectors["runtime"].objects[o.Namespace+"/"+o.Name].Metadata.Images[0].Image; got != "example.invalid/fixture:2" {
		t.Fatal("invalid frame modified inventory")
	}
}

func TestMemoryMetadataBounds(t *testing.T) {
	cases := map[string]func(*ResourceMetadata){
		"template":       func(m *ResourceMetadata) { m.TemplateHash = strings.Repeat("a", 129) },
		"image":          func(m *ResourceMetadata) { m.Images[0].Image = strings.Repeat("x", 2049) },
		"duplicate":      func(m *ResourceMetadata) { m.Images = append(m.Images, m.Images[0]) },
		"partial":        func(m *ResourceMetadata) { m.ImagesComplete = false },
		"empty complete": func(m *ResourceMetadata) { m.Images = []ResourceImage{} },
		"null images":    func(m *ResourceMetadata) { m.Images = nil },
		"component":      func(m *ResourceMetadata) { m.Images[0].Component = "invalid/name" },
		"display bytes":  func(m *ResourceMetadata) { m.Discovery.DisplayName = strings.Repeat("界", 171) },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			_, s, _, o, _ := memoryFixture(t, false)
			o.Metadata = metadataFixture()
			mutate(o.Metadata)
			id := uuid.NewString()
			if s.Begin(id) != nil || s.Objects(id, []ResourceObservation{o}) == nil {
				t.Fatal("invalid metadata accepted")
			}
		})
	}
	// An older collector can omit metadata, but explicitly null is invalid.
	_, s, _, o, _ := memoryFixture(t, false)
	memoryFull(t, s, o)
	raw, _ := json.Marshal(Upsert{SchemaVersion: 1, Kind: "upsert", Object: o})
	raw = []byte(strings.Replace(string(raw), `"namespace":`, `"metadata":null,"namespace":`, 1))
	if s.Receive(raw) == nil {
		t.Fatal("null metadata accepted")
	}
}
