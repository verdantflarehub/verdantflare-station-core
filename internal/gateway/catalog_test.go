package gateway

import (
	"context"
	"encoding/json"
	"github.com/google/uuid"
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
	s := &Server{Catalog: apps}
	admin := identity.IdentityContext{StationID: "test-station", OrganizationID: "test-organization", Roles: []string{"admin"}, Scopes: []string{"app:read"}}
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
	var reader catalog.Reader
	path := os.Getenv("STATION_TEST_CATALOG_FILE")
	if path == "" {
		path = filepath.Join(t.TempDir(), "catalog.json")
		payload := `[{"app_id":"video-mcp-server","display_name":"Video MCP","group_id":"video","brand":"vf","models":[],"source":"deploys/video.yaml","namespace":"verdantflare-video","workload_name":"video-mcp-server","version":"1.0.0","images":[{"component":"server","image":"example.invalid/server:1.0.0"}]}]`
		if err = os.WriteFile(path, []byte(payload), 0600); err != nil {
			t.Fatal(err)
		}
	} else {
		reader, err = catalog.NewKubernetes(os.Getenv("STATION_TEST_KUBECONFIG"))
		if err != nil {
			t.Fatal("live Kubernetes configuration failed")
		}
	}
	apps, err := catalog.Load(path, reader)
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Identity: svc, Catalog: apps, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	request := func(path, token string, status int) *httptest.ResponseRecorder {
		t.Helper()
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		if w.Code != status {
			t.Fatalf("%s status %d want %d", path, w.Code, status)
		}
		return w
	}
	request("/catalog/apps", "", 401)
	w := request("/catalog/apps", session.AccessToken, 200)
	var list catalog.List
	if json.Unmarshal(w.Body.Bytes(), &list) != nil || len(list.Items) == 0 || list.StationID != id {
		t.Fatal("invalid catalog response")
	}
	if reader != nil {
		for _, a := range list.Items {
			if a.Deployment.State == "unknown" {
				t.Fatalf("live observation failed for %s: %s", a.AppID, a.Deployment.Reason)
			}
		}
	}
	if path := os.Getenv("STATION_TEST_CATALOG_OUTPUT"); path != "" {
		if err = os.WriteFile(path, w.Body.Bytes(), 0600); err != nil {
			t.Fatal(err)
		}
	}
	request("/catalog/apps?group_id=video", session.AccessToken, 200)
	request("/catalog/apps?namespace=default", session.AccessToken, 400)
	request("/catalog/apps?group_id=image&group_id=video", session.AccessToken, 400)
	request("/catalog/apps/video-mcp-server", session.AccessToken, 200)
	request("/catalog/apps/not-registered", session.AccessToken, 404)
	if err = svc.Logout(ctx, "catalog-test", session.AccessToken); err != nil {
		t.Fatal(err)
	}
	request("/catalog/apps", session.AccessToken, 401)
}
