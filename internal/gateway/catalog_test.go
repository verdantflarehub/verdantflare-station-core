package gateway

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestSystemGroupCatalogQuery(t *testing.T) {
	path := filepath.Join(t.TempDir(), "catalog.json")
	entry := catalog.Entry{AppID: "project", DisplayName: "Project", GroupID: "system", Brand: "vf", Models: []string{}, Source: "deploys/app-project.yaml", Namespace: "verdantflare-station", WorkloadName: "app-project", Version: "0.1.0", Images: []catalog.Image{{Component: "app", Image: "example.invalid/project:0.1.0"}}}
	payload, err := json.Marshal([]catalog.Entry{entry})
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, payload, 0600); err != nil {
		t.Fatal(err)
	}
	apps, err := catalog.Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	p := testdb.New(t)
	station := uuid.NewString()
	if err := migrations.Apply(t.Context(), p, station); err != nil {
		t.Fatal(err)
	}
	s := &Server{Catalog: apps, AppStates: &appstate.Service{Pool: p, StationID: station, Catalog: apps}}
	admin := identity.IdentityContext{StationID: station, OrganizationID: uuid.NewString(), UserID: uuid.NewString(), Roles: []string{"admin"}, Scopes: []string{"app:read"}}
	for _, test := range []struct {
		name, target string
		user         identity.IdentityContext
		status       int
	}{
		{"system filter", "/catalog/apps?group_id=system", admin, http.StatusOK},
		{"system detail", "/catalog/apps/project", admin, http.StatusOK},
		{"unknown group", "/catalog/apps?group_id=unknown", admin, http.StatusBadRequest},
		{"duplicate group", "/catalog/apps?group_id=system&group_id=video", admin, http.StatusBadRequest},
		{"admin required", "/catalog/apps?group_id=system", identity.IdentityContext{Scopes: []string{"app:read"}}, http.StatusForbidden},
		{"read scope required", "/catalog/apps?group_id=system", identity.IdentityContext{Roles: []string{"admin"}}, http.StatusForbidden},
	} {
		t.Run(test.name, func(t *testing.T) {
			response := httptest.NewRecorder()
			s.serveCatalog(response, httptest.NewRequest(http.MethodGet, test.target, nil), context.Background(), "system-query", test.user)
			if response.Code != test.status {
				t.Fatalf("status %d, want %d", response.Code, test.status)
			}
			if test.name == "system filter" {
				var list catalog.List
				if json.Unmarshal(response.Body.Bytes(), &list) != nil || len(list.Items) != 1 || list.Items[0].AppID != "project" || list.Items[0].GroupID != "system" || list.Items[0].Deployment.State != "unknown" {
					t.Fatal("system filter response changed identity or invented installed state")
				}
			}
		})
	}
}

func TestCatalogHTTP(t *testing.T) {
	ctx := context.Background()
	p := testdb.New(t)
	id := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, p, id); err != nil {
		t.Fatal(err)
	}
	svc, err := identity.New(p, id, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := svc.Bootstrap(ctx, "catalog-test", identity.BootstrapRequest{Username: "admin", Password: secret(), OrganizationName: "Catalog"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	payload := `[{"app_id":"video-mcp-server","display_name":"Video MCP","group_id":"video","brand":"vf","models":[],"source":"deploys/video.yaml","namespace":"verdantflare-video","workload_name":"video-mcp-server","version":"1.0.0","images":[{"component":"server","image":"example.invalid/server:1.0.0"}]}]`
	if err = os.WriteFile(path, []byte(payload), 0600); err != nil {
		t.Fatal(err)
	}
	apps, err := catalog.Load(path, catalogNoProbe{t})
	if err != nil {
		t.Fatal(err)
	}
	memory, err := appstate.NewMemory(id, []appstate.Collector{{ID: "runtime", Namespaces: []string{"verdantflare-video"}}})
	if err != nil {
		t.Fatal(err)
	}
	states := &appstate.Service{Pool: p, StationID: id, Catalog: apps, Observations: memory}
	if err = states.SyncCatalog(ctx, apps.RegisteredAppIDs()); err != nil {
		t.Fatal(err)
	}
	s := &Server{Identity: svc, Catalog: apps, AppStates: states, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	request := func(path, token string, status int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s status %d want %d", path, w.Code, status)
		}
		if w.Header().Get("Cache-Control") != "no-store" {
			t.Fatal("catalog response must not be cached")
		}
		return w
	}
	request("/catalog/apps", "", 401)
	w := request("/catalog/apps", session.AccessToken, 200)
	var list catalog.List
	if json.Unmarshal(w.Body.Bytes(), &list) != nil || len(list.Items) == 0 || list.StationID != id {
		t.Fatal("invalid catalog response")
	}
	if list.Items[0].Deployment.State != "unknown" {
		t.Fatal("startup invented state")
	}
	request("/catalog/apps?group_id=video", session.AccessToken, 200)
	request("/catalog/apps?namespace=default", session.AccessToken, 400)
	request("/catalog/apps?group_id=image&group_id=video", session.AccessToken, 400)
	request("/catalog/apps/video-mcp-server", session.AccessToken, 200)
	request("/catalog/apps/not-registered", session.AccessToken, 404)
	installation := uuid.NewString()
	_, err = p.Exec(ctx, `INSERT INTO station.app_installations(installation_id,station_id,organization_id,created_by,app_id,app_version,state) VALUES($1,$2,$3,$4,'video-mcp-server','1.0.0','installed')`, installation, id, session.OrganizationID, session.UserID)
	if err != nil {
		t.Fatal(err)
	}
	stream, err := memory.Open("runtime")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	object := appstate.ResourceObservation{Namespace: "verdantflare-video", Name: "actual-management", UID: uuid.NewString(), ResourceVersion: "1", StationID: id, OrganizationID: session.OrganizationID, InstallationID: installation, AppID: "video-mcp-server", Generation: 1, ObservedGeneration: 1, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked"}
	syncID := uuid.NewString()
	if err = stream.Begin(syncID); err != nil {
		t.Fatal(err)
	}
	if err = stream.Objects(syncID, []appstate.ResourceObservation{object}); err != nil {
		t.Fatal(err)
	}
	if err = stream.End(syncID, 1); err != nil {
		t.Fatal(err)
	}
	for _, state := range []string{"ready", "stopped"} {
		if state == "stopped" {
			object.Desired, object.Replicas, object.Ready, object.Updated, object.Available = 0, 0, 0, 0, 0
			if err = stream.Upsert(object); err != nil {
				t.Fatal(err)
			}
		}
		var detail catalog.Detail
		if json.Unmarshal(request("/catalog/apps/video-mcp-server", session.AccessToken, 200).Body.Bytes(), &detail) != nil || detail.App.Deployment.State != state || detail.App.Deployment.UID != object.UID {
			t.Fatal("detail did not use exact installation memory")
		}
		if json.Unmarshal(request("/catalog/apps?group_id=video", session.AccessToken, 200).Body.Bytes(), &list) != nil || len(list.Items) != 1 || list.Items[0].Deployment.State != state {
			t.Fatal("list disagrees with detail")
		}
		var independent appstate.Snapshot
		if json.Unmarshal(request("/app-states/v1", session.AccessToken, 200).Body.Bytes(), &independent) != nil || len(independent.Items) != 1 || independent.Items[0].Runtime.State != state {
			t.Fatal("catalog disagrees with independent state API")
		}
	}
	s.AppStates = nil
	request("/catalog/apps", session.AccessToken, 503)
	request("/catalog/apps/not-registered", session.AccessToken, 503)
	s.AppStates = states
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	user := identity.IdentityContext{StationID: id, OrganizationID: session.OrganizationID, UserID: uuid.NewString(), Roles: []string{"admin"}, Scopes: []string{"app:read"}}
	if _, err = states.RegisteredCatalog(canceled, user); err == nil {
		t.Fatal("canceled database read returned successful catalog")
	}
	if err = svc.Logout(ctx, "catalog-test", session.AccessToken); err != nil {
		t.Fatal(err)
	}
	request("/catalog/apps", session.AccessToken, 401)
}

type catalogNoProbe struct{ t *testing.T }

func (r catalogNoProbe) Observe(context.Context, catalog.Entry) catalog.Observation {
	r.t.Error("legacy public read called Kubernetes Observe")
	return catalog.Observation{}
}

func (r catalogNoProbe) Discover(context.Context) ([]catalog.Item, error) {
	r.t.Error("legacy public read called Kubernetes Discover")
	return nil, nil
}
