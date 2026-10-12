package appstate

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
)

func legacyRegistryBytes(t *testing.T, b legacyBinding) []byte {
	t.Helper()
	raw, err := json.Marshal(map[string]any{
		"schemaVersion": 1, "revision": "sha256:" + strings.Repeat("a", 64),
		"records": []map[string]any{{"bindingId": b.BindingID, "appId": b.AppID, "organizationId": b.OrganizationID, "stationId": b.StationID, "workloadUID": b.WorkloadUID, "observationKey": b.ObservationKey, "adapterId": "blender-host-v1", "backendRevision": "sha256:" + strings.Repeat("b", 64), "route": "/market?blender=instances"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func TestLegacyDatabaseConsistentStateFactsAndScopedHints(t *testing.T) {
	s, u, id := fixture(t)
	query(t, s, `UPDATE station.app_installations SET state='removed' WHERE installation_id=$1`, id)
	notificationCatalog(t, s)
	if err := s.SyncCatalog(t.Context(), []string{"fixture-app"}); err != nil {
		t.Fatal(err)
	}
	m, stream, _, o, now := memoryFixture(t, false)
	m.station = s.StationID
	s.Observations = m
	b := legacyBinding{BindingID: "legacy-fixture", AppID: "fixture-app", OrganizationID: u.OrganizationID, StationID: s.StationID, WorkloadUID: o.UID, ObservationKey: "obs:" + appcenter.Digest([]string{o.Namespace, o.Name, o.UID})[7:]}
	path := filepath.Join(t.TempDir(), "legacy.json")
	if os.WriteFile(path, legacyRegistryBytes(t, b), 0600) != nil {
		t.Fatal("registry write failed")
	}
	if err := s.LoadLegacy(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, s, u, "")
	if len(before.Items) != 1 || before.Items[0].Installation != nil || before.Items[0].Runtime.State != "unknown" {
		t.Fatal("registry fabricated installation or observation")
	}
	foreign := u
	foreign.OrganizationID = uuid.Must(uuid.NewV7()).String()
	query(t, s, `INSERT INTO station.organizations(organization_id,name) VALUES($1,'Legacy observer')`, foreign.OrganizationID)
	o.AppID, o.OrganizationID, o.StationID, o.InstallationID = "", "", "", ""
	o.Metadata = metadataFixture()
	o.Metadata.TemplateHash = ""
	o.Metadata.OwnershipMarked = false
	memoryFull(t, stream, o)
	ready := snapshot(t, s, u, "")
	if ready.Items[0].Runtime.State != "ready" || ready.Revision == before.Revision {
		t.Fatal("API did not project legacy evidence")
	}
	tx, err := s.Pool.BeginTx(t.Context(), pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	items, views, err := s.CatalogSnapshot(t.Context(), tx, u.OrganizationID, s.Catalog.Entries())
	rollback(tx)
	if err != nil || len(views) != 0 || items[0].Deployment.State != ready.Items[0].Runtime.State {
		t.Fatal("facts and independent state diverged", err)
	}
	facts, err := appcenter.BuildState(u, "fixture", *now, items, nil, nil, map[string]appcenter.Service{})
	if err != nil || len(facts.Items) != 1 || facts.Items[0].CardKey != b.ObservationKey || facts.Items[0].Installation != nil || facts.Items[0].Origin != "observation" || len(facts.Items[0].Lifecycle.Candidates) != 0 {
		t.Fatal("legacy output changed identity or management permissions", err)
	}
	if snapshot(t, s, foreign, "").Items[0].Runtime.State != "unknown" {
		t.Fatal("legacy state leaked to another organization")
	}
	persistent := persistentRevision(t, s, u.OrganizationID)
	beforeHints, err := s.NotificationRevisions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	o.Desired, o.Ready, o.Replicas, o.Updated, o.Available = 0, 0, 0, 0, 0
	if stream.Upsert(o) != nil {
		t.Fatal("stream failed")
	}
	afterHints, err := s.NotificationRevisions(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if afterHints[u.OrganizationID] == beforeHints[u.OrganizationID] || afterHints[foreign.OrganizationID] != beforeHints[foreign.OrganizationID] {
		t.Fatal("legacy hints did not preserve organization scope")
	}
	if got := snapshot(t, s, u, ""); got.Items[0].Runtime.State != "stopped" || got.Revision == ready.Revision {
		t.Fatal("legacy stop missing from API")
	}
	*now = now.Add(31 * time.Second)
	if snapshot(t, s, u, "").Items[0].Runtime.State != "unknown" || persistentRevision(t, s, u.OrganizationID) != persistent {
		t.Fatal("legacy expiry changed persistent state")
	}
	if err := s.LoadLegacy(t.Context(), ""); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(t, s, u, ""); got.Items[0].Runtime.ReasonCode != "NOT_SYNCED" {
		t.Fatal("withdrawn registry remained active")
	}
}

func TestLegacyDatabaseRegistryRejectsInvalidAndKeepsPriorConfig(t *testing.T) {
	s, u, _ := fixture(t)
	notificationCatalog(t, s)
	b := legacyBinding{BindingID: "legacy-fixture", AppID: "fixture-app", OrganizationID: u.OrganizationID, StationID: s.StationID, WorkloadUID: uuid.NewString()}
	b.ObservationKey = "obs:" + appcenter.Digest([]string{"apps", "fixture-app", b.WorkloadUID})[7:]
	path := filepath.Join(t.TempDir(), "legacy.json")
	write := func(raw []byte) {
		t.Helper()
		if os.WriteFile(path, raw, 0600) != nil {
			t.Fatal("registry write failed")
		}
	}
	write(legacyRegistryBytes(t, b))
	if err := s.LoadLegacy(t.Context(), path); err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"duplicate-field", "unknown-field", "unknown-org", "station", "missing-catalog", "wrong-position", "empty-scope"} {
		t.Run(variant, func(t *testing.T) {
			candidate := b
			switch variant {
			case "unknown-org":
				candidate.OrganizationID = uuid.NewString()
			case "station":
				candidate.StationID = uuid.NewString()
			case "missing-catalog":
				candidate.AppID = "missing-app"
			case "wrong-position":
				candidate.ObservationKey = "obs:" + strings.Repeat("0", 64)
			case "empty-scope":
				candidate.OrganizationID = ""
			}
			raw := legacyRegistryBytes(t, candidate)
			if variant == "duplicate-field" {
				raw = []byte(strings.Replace(string(raw), `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1))
			}
			if variant == "unknown-field" {
				raw = append([]byte(`{"unexpected":true,`), raw[1:]...)
			}
			write(raw)
			if s.LoadLegacy(t.Context(), path) == nil {
				t.Fatal("invalid registry accepted")
			}
			m := s.memory()
			m.mu.Lock()
			retained := len(m.legacy) == 1 && m.legacy[0].OrganizationID == b.OrganizationID
			m.mu.Unlock()
			if !retained {
				t.Fatal("invalid registry partially replaced previous config")
			}
		})
	}
}
