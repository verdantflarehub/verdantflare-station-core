package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
)

func TestInstallationAccessExactBindingAndLiveRevocation(t *testing.T) {
	const station = "01900000-0000-7000-8000-000000000001"
	const user = "01900000-0000-7000-8000-000000000002"
	const org = "01900000-0000-7000-8000-000000000003"
	const installation = "01900000-0000-7000-8000-000000000004"
	token := strings.Repeat("i", 32)
	calls, reads := 0, 0
	allowed := true
	item := operations.Installation{InstallationID: installation, StationID: station, OrganizationID: org,
		AppID: "comfyui", AppVersion: "0.1.2", State: "installed", Revision: 3}
	access := &InstallationAccess{callers: map[[32]byte]string{sha256.Sum256([]byte("Bearer " + token)): "comfyui"},
		manager: func(ctx context.Context, u, o string, effect func(context.Context) error) error {
			calls++
			if u != user || o != org {
				t.Fatal("changed principal")
			}
			if !allowed {
				return identity.Denied
			}
			return effect(ctx)
		},
		installation: func(_ context.Context, actor identity.IdentityContext, id string) (operations.Installation, error) {
			reads++
			if actor.UserID != user || actor.OrganizationID != org || actor.StationID != station || id != installation {
				t.Fatal("changed installation scope")
			}
			return item, nil
		}}
	s := &Server{Identity: &identity.Service{StationID: station}, InstallationAccess: access,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	request := func() *http.Request {
		r := httptest.NewRequest("POST", "/internal/v1/app-installations/authorize", strings.NewReader(`{"installation_id":"`+installation+`","app_version":"0.1.2"}`))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-User-Id", user)
		r.Header.Set("X-Organization-Id", org)
		r.Header.Set("X-Station-Id", station)
		return r
	}
	for _, tc := range []struct {
		name   string
		status int
		mutate func(*http.Request)
	}{
		{"missing credential", 401, func(r *http.Request) { r.Header.Del("Authorization") }},
		{"duplicate credential", 401, func(r *http.Request) { r.Header.Add("Authorization", "Bearer "+token) }},
		{"duplicate principal", 400, func(r *http.Request) { r.Header.Add("X-User-Id", user) }},
		{"wrong station", 403, func(r *http.Request) { r.Header.Set("X-Station-Id", org) }},
		{"query", 400, func(r *http.Request) { r.URL.RawQuery = "other=1" }},
		{"duplicate body", 400, func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader(`{"installation_id":"` + installation + `","app_version":"0.1.2","app_version":"0.1.2"}`))
		}},
		{"unknown app claim", 400, func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader(`{"installation_id":"` + installation + `","app_version":"0.1.2","app_id":"comfyui"}`))
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := request()
			tc.mutate(r)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)
			if w.Code != tc.status || calls != 0 || reads != 0 {
				t.Fatalf("status=%d calls=%d reads=%d", w.Code, calls, reads)
			}
		})
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, request())
	var out map[string]any
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || out["installation_revision"] != float64(3) || len(out) != 9 {
		t.Fatalf("invalid authorized response: status %d", w.Code)
	}
	for _, change := range []func(){func() { item.AppID = "another-app" }, func() { item.AppVersion = "0.1.3" },
		func() { item.OrganizationID = user }, func() { item.StationID = org }, func() { item.State = "removed" }} {
		original := item
		change()
		w = httptest.NewRecorder()
		s.ServeHTTP(w, request())
		item = original
		if w.Code != 403 {
			t.Fatalf("binding change accepted: %d", w.Code)
		}
	}
	before := reads
	allowed = false
	w = httptest.NewRecorder()
	s.ServeHTTP(w, request())
	if w.Code != 403 || reads != before {
		t.Fatal("revocation reused old grant")
	}
}

func TestInstallationAccessRegistryRejectsAmbiguousCredentials(t *testing.T) {
	path := filepath.Join(t.TempDir(), "registry.json")
	service := &identity.Service{}
	ops := &operations.Service{}
	load := func(raw string) (*InstallationAccess, error) {
		if err := os.WriteFile(path, []byte(raw), 0600); err != nil {
			t.Fatal(err)
		}
		return LoadInstallationAccess(path, func(string) string { return strings.Repeat("x", 32) }, service, ops)
	}
	for _, raw := range []string{`[]`, `[{"app_id":"comfyui","token_env":"TOKEN"}]`,
		`[{"app_id":"comfyui","token_env":"STATION_APP_INSTALLATION_A_TOKEN","token":"literal"}]`,
		`[{"app_id":"comfyui","token_env":"STATION_APP_INSTALLATION_A_TOKEN"},{"app_id":"image","token_env":"STATION_APP_INSTALLATION_B_TOKEN"}]`,
		`[{"app_id":"comfyui","app_id":"image","token_env":"STATION_APP_INSTALLATION_A_TOKEN"}]`} {
		if _, err := load(raw); err == nil {
			t.Fatal("ambiguous registry accepted")
		}
	}
	access, err := load(`[{"app_id":"comfyui","token_env":"STATION_APP_INSTALLATION_A_TOKEN"}]`)
	if err != nil || len(access.callers) != 1 {
		t.Fatal("valid registry rejected")
	}
	if _, err = LoadInstallationAccess(path, func(string) string { return "" }, service, ops); err == nil {
		t.Fatal("missing credential accepted")
	}
}
