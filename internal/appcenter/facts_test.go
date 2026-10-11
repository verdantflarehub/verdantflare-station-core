package appcenter

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

var fixtureTime = time.Date(2026, 10, 12, 0, 0, 0, 0, time.UTC)

func actor() identity.IdentityContext {
	return identity.IdentityContext{UserID: "user", OrganizationID: "org", StationID: "station", Roles: []string{"admin"}, Scopes: []string{"app:read", "app:manage"}}
}
func installed(app string) Record {
	return Record{AppID: app, Installation: Installation{InstallationID: "installation", Revision: 2, AppVersion: "1.0.0", State: "installed"}}
}
func workload(app, name, installation string) catalog.SnapshotItem {
	return catalog.SnapshotItem{Registered: true, Item: catalog.Item{
		Entry: catalog.Entry{AppID: app, DisplayName: "Example", Namespace: "apps", WorkloadName: name, Version: "2.0.0", Models: []string{}, Images: []catalog.Image{}},
		Deployment: catalog.Observation{UID: name + "-uid", State: "ready", ObservedAt: fixtureTime, Images: []catalog.Image{}, Ownership: map[string]string{
			"station.verdantflare.com/station-id": "station", "station.verdantflare.com/organization-id": "org",
			"station.verdantflare.com/app-id": app, "station.verdantflare.com/template-hash": strings.Repeat("a", 64),
			"station.verdantflare.com/installation-id": installation,
		}},
	}}
}
func build(t *testing.T, items []catalog.SnapshotItem, installs []Record, ops []OperationRecord) Facts {
	t.Helper()
	f, err := Build(actor(), "request", fixtureTime, items, installs, ops)
	if err != nil {
		t.Fatal(err)
	}
	return f
}

func TestAppCenterInstallationAndObservationAreIndependent(t *testing.T) {
	for _, app := range []string{"comfyui", "blender", "new-app"} {
		t.Run(app, func(t *testing.T) {
			w := workload(app, "manager", "installation")
			f := build(t, []catalog.SnapshotItem{w}, []Record{installed(app)}, nil)
			if len(f.Items) != 1 {
				t.Fatalf("got %d cards", len(f.Items))
			}
			card := f.Items[0]
			if card.CardKey != "installation:installation" || card.Installation.AppVersion != "1.0.0" || card.Catalog.Version != "2.0.0" || card.Service.State != "ready" {
				t.Fatal("installation identity/version confused with candidate or missing runtime version annotation")
			}
			for _, c := range card.Lifecycle.Candidates {
				if c.Enabled {
					t.Fatal("unimplemented writer advertised")
				}
			}
			f = build(t, []catalog.SnapshotItem{w}, nil, nil)
			if f.Items[0].Installation != nil || f.Items[0].Origin != "observation" {
				t.Fatal("deployment manufactured an installation")
			}
			f = build(t, nil, []Record{installed(app)}, nil)
			if f.Items[0].Service.State != "unknown" || f.Items[0].Catalog != nil {
				t.Fatal("withdrawn catalog fabricated readiness or removed installation")
			}
		})
	}
}

func TestAppCenterDistinctWorkloadsAndConflicts(t *testing.T) {
	a, b := workload("app", "a", "installation"), workload("app", "b", "other")
	b.Registered = false
	f := build(t, []catalog.SnapshotItem{a, b}, []Record{installed("app")}, nil)
	if len(f.Items) != 2 || f.Items[0].Origin != "installation" || f.Items[1].Origin != "observation" {
		t.Fatal("distinct workload collapsed")
	}
	b.Deployment.Ownership["station.verdantflare.com/installation-id"] = "installation"
	f = build(t, []catalog.SnapshotItem{a, b}, []Record{installed("app")}, nil)
	if len(f.Items) != 1 || f.Items[0].Service.Quality != "conflict" {
		t.Fatal("ambiguous installation binding accepted")
	}
	// A registered workload not bound to this installation must also remain an observation.
	a.Deployment.Ownership["station.verdantflare.com/installation-id"] = "other"
	f = build(t, []catalog.SnapshotItem{a}, []Record{installed("app")}, nil)
	if len(f.Items) != 2 || f.Items[0].Service.State != "unknown" {
		t.Fatal("registered orphan hidden or adopted")
	}
}

func TestAppCenterForeignDataRedacted(t *testing.T) {
	w := workload("app", "manager", "installation")
	w.Deployment.Ownership["station.verdantflare.com/organization-id"] = "other-org"
	w.Deployment.UID = "private-workload"
	w.Deployment.Images = []catalog.Image{{Component: "private-component", Image: "private-image"}}
	for _, installs := range [][]Record{nil, {installed("app")}} {
		f := build(t, []catalog.SnapshotItem{w}, installs, nil)
		raw, _ := json.Marshal(f)
		for _, secret := range []string{"private-workload", "private-image", "private-component", "other-org"} {
			if strings.Contains(string(raw), secret) {
				t.Fatalf("leaked %s", secret)
			}
		}
		if f.Items[0].Service.Quality != "conflict" {
			t.Fatal("foreign binding appeared healthy")
		}
	}
	w.Registered = false
	if f := build(t, []catalog.SnapshotItem{w}, nil, nil); len(f.Items) != 0 {
		t.Fatal("foreign discovered object exposed")
	}
}

func TestAppCenterOperationPrecedenceAndRedaction(t *testing.T) {
	terminal := OperationRecord{AppID: "app", OperationID: "finished", UserID: "user", OrganizationID: "org", Protocol: "lifecycle", Status: "succeeded", Phase: "completed", UpdatedAt: fixtureTime}
	active := OperationRecord{AppID: "app", OperationID: "active", UserID: "user", OrganizationID: "org", Protocol: "transition", Status: "recovery_required", Phase: "recovery_required", UpdatedAt: fixtureTime.Add(time.Second)}
	i := installed("app")
	i.LastOperationID = Text("finished")
	f := build(t, nil, []Record{i}, []OperationRecord{terminal})
	if f.Items[0].Operation.Blocking || !f.Items[0].Operation.CanRead || f.Items[0].Installation.LastOperationID == nil {
		t.Fatal("terminal operation blocked or vanished")
	}
	for _, ops := range [][]OperationRecord{{terminal, active}, {active, terminal}} {
		f = build(t, nil, []Record{i}, ops)
		if !f.Items[0].Operation.Blocking || *f.Items[0].Operation.OperationID != "active" || f.Items[0].Installation.LastOperationID != nil {
			t.Fatal("active operation did not take precedence")
		}
	}
	active.UserID = "another-user"
	f = build(t, nil, []Record{i}, []OperationRecord{terminal, active})
	op := f.Items[0].Operation
	if !op.Blocking || op.CanRead || op.OperationID != nil || op.Protocol != nil || op.Status != nil || op.Phase != nil || op.UpdatedAt != nil {
		t.Fatal("foreign operation metadata leaked")
	}
	f = build(t, nil, []Record{i}, []OperationRecord{active, active})
	if f.Items[0].Operation.Quality != "conflict" || f.Items[0].Operation.CanRead {
		t.Fatal("multiple active operations accepted")
	}
}

func TestAppCenterStableRevisionAndScope(t *testing.T) {
	w := workload("app", "manager", "installation")
	f := build(t, []catalog.SnapshotItem{w}, []Record{installed("app")}, nil)
	w.Deployment.ObservedAt = fixtureTime.Add(time.Second)
	user := actor()
	user.Scopes = []string{"app:manage", "app:read"}
	g, err := Build(user, "next-request", fixtureTime.Add(time.Second), []catalog.SnapshotItem{w}, []Record{installed("app")}, nil)
	if err != nil || f.Revision != g.Revision {
		t.Fatal("request time or scope ordering invalidated unchanged facts")
	}
	user.PolicyVersion++
	g, _ = Build(user, "request", fixtureTime, []catalog.SnapshotItem{w}, []Record{installed("app")}, nil)
	if f.Revision == g.Revision || f.Scope.AuthorizationEpoch == g.Scope.AuthorizationEpoch {
		t.Fatal("policy change preserved binding")
	}
	w.Deployment.ObservedAt = time.Time{}
	f = build(t, []catalog.SnapshotItem{w}, []Record{installed("app")}, nil)
	if f.Items[0].Service.State != "unknown" {
		t.Fatal("untimestamped observation advertised ready")
	}
}

func TestAppCenterInvalidInputsFailClosed(t *testing.T) {
	u := actor()
	u.Roles = nil
	if _, err := Build(u, "request", fixtureTime, nil, nil, nil); !errors.Is(err, identity.Denied) {
		t.Fatal("missing permission accepted")
	}
	i := installed("app")
	if _, err := Build(actor(), "request", fixtureTime, nil, []Record{i, i}, nil); !errors.Is(err, ErrConflict) {
		t.Fatal("duplicate installation accepted")
	}
	if _, err := Build(actor(), "request", fixtureTime, nil, make([]Record, 1001), nil); !errors.Is(err, ErrLimit) {
		t.Fatal("oversized snapshot accepted")
	}
	f := build(t, nil, nil, nil)
	if f.Items == nil || len(f.Items) != 0 {
		t.Fatal("successful empty list must remain empty")
	}
}
