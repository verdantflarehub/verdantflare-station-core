package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func TestAppStateGatewayGuards(t *testing.T) {
	user := identity.IdentityContext{Roles: []string{"admin"}, Scopes: []string{"app:read"}}
	for _, tc := range []struct {
		path   string
		body   bool
		status int
	}{
		{"/app-states/v1", false, 503},
		{"/app-states/v1?organizationId=other", false, 400},
		{"/app-states/v1?", false, 400},
		{"/app-states/%761", false, 400},
		{"/app-states/v1", true, 400},
		{"/app-states/v1/installations/", false, 400},
		{"/app-states/v1/installations/not-a-uuid", false, 400},
	} {
		r := httptest.NewRequest("GET", tc.path, nil)
		if tc.body {
			r.Body = io.NopCloser(strings.NewReader("{}"))
			r.ContentLength = -1
		}
		w := httptest.NewRecorder()
		(&Server{}).serveAppStates(w, r, context.Background(), "request", user)
		if w.Code != tc.status || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("%s returned %d", tc.path, w.Code)
		}
	}
	w := httptest.NewRecorder()
	(&Server{}).serveAppStates(w, httptest.NewRequest("GET", "/app-states/v1", nil), context.Background(), "request", identity.IdentityContext{})
	if w.Code != 403 {
		t.Fatal("authorization bypass")
	}
}

func TestAppStateDatabaseAuthenticatedGateway(t *testing.T) {
	ctx := t.Context()
	pool := testdb.New(t)
	station := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, pool, station); err != nil {
		t.Fatal(err)
	}
	auth, err := identity.New(pool, station, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := auth.Bootstrap(ctx, "state-test", identity.BootstrapRequest{Username: "state-admin", Password: secret(), OrganizationName: "State test"})
	if err != nil {
		t.Fatal(err)
	}
	s := &Server{Identity: auth, AppStates: &appstate.Service{Pool: pool, StationID: station}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	if err = s.AppStates.SyncCatalog(ctx, []string{"uninstalled-app"}); err != nil {
		t.Fatal(err)
	}
	call := func(path, method, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, nil)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	for _, tc := range []struct {
		path, method, token string
		status              int
	}{
		{"/app-states/v1", "GET", "", 401},
		{"/app-states/v1", "POST", session.AccessToken, 405},
		{"/app-states/v1?stationId=other", "GET", session.AccessToken, 400},
		{"/app-states/v1/installations/" + uuid.NewString(), "GET", session.AccessToken, 404},
	} {
		if w := call(tc.path, tc.method, tc.token); w.Code != tc.status {
			t.Fatalf("%s status %d, want %d", tc.path, w.Code, tc.status)
		}
	}
	w := call("/app-states/v1", "GET", session.AccessToken)
	var out appstate.Snapshot
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(out.Items) != 1 || out.Scope.OrganizationID != session.OrganizationID || out.Items[0].Runtime.State != "unknown" || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("authenticated state route not connected", w.Code)
	}
	if err = auth.Logout(ctx, "state-logout", session.AccessToken); err != nil {
		t.Fatal(err)
	}
	if w = call("/app-states/v1", "GET", session.AccessToken); w.Code != 401 {
		t.Fatal("revoked session could read state")
	}
}
