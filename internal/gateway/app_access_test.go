package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func TestAppAccessRejectsInvalidDelegationBeforeIdentity(t *testing.T) {
	const station = "01900000-0000-7000-8000-000000000001"
	const user = "01900000-0000-7000-8000-000000000002"
	const org = "01900000-0000-7000-8000-000000000003"
	calls := 0
	access := &AppAccess{networkToken: strings.Repeat("n", 32), resourcesToken: strings.Repeat("r", 32),
		manager: func(_ context.Context, u, o string, effect func(context.Context) error) error {
			calls++
			if u != user || o != org {
				t.Fatal("subject changed")
			}
			return effect(context.Background())
		}}
	s := &Server{Identity: &identity.Service{StationID: station}, AppAccess: access, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	request := func() *http.Request {
		r := httptest.NewRequest("POST", "/internal/v1/app-access", strings.NewReader(`{"app_id":"network"}`))
		r.Header.Set("Authorization", "Bearer "+access.networkToken)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-User-Id", user)
		r.Header.Set("X-Organization-Id", org)
		r.Header.Set("X-Station-Id", station)
		return r
	}
	for _, tc := range []struct {
		name   string
		status int
		modify func(*http.Request)
	}{
		{"user bearer", 401, func(r *http.Request) { r.Header.Set("Authorization", "Bearer user-session") }},
		{"wrong service", 401, func(r *http.Request) { r.Header.Set("Authorization", "Bearer "+access.resourcesToken) }},
		{"duplicate credential", 401, func(r *http.Request) { r.Header.Add("Authorization", r.Header.Get("Authorization")) }},
		{"duplicate subject", 400, func(r *http.Request) { r.Header.Add("X-User-Id", user) }},
		{"missing subject", 400, func(r *http.Request) { r.Header.Del("X-Organization-Id") }},
		{"wrong station", 403, func(r *http.Request) { r.Header.Set("X-Station-Id", org) }},
		{"query", 400, func(r *http.Request) { r.URL.RawQuery = "admin=true" }},
		{"compressed", 400, func(r *http.Request) { r.Header.Set("Content-Encoding", "gzip") }},
		{"unknown field", 400, func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader(`{"app_id":"network","admin":true}`)) }},
		{"duplicate field", 400, func(r *http.Request) {
			r.Body = io.NopCloser(strings.NewReader(`{"app_id":"network","app_id":"network"}`))
		}},
		{"wrong app", 400, func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader(`{"app_id":"resources"}`)) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := request()
			tc.modify(r)
			w := httptest.NewRecorder()
			s.ServeHTTP(w, r)
			if w.Code != tc.status || calls != 0 {
				t.Fatalf("status %d; identity calls %d", w.Code, calls)
			}
		})
	}
	w := httptest.NewRecorder()
	s.ServeHTTP(w, request())
	var body map[string]any
	if w.Code != 200 || calls != 1 || json.Unmarshal(w.Body.Bytes(), &body) != nil || body["user_id"] != user || body["app_id"] != "network" {
		t.Fatalf("authorization failed: status %d calls %d", w.Code, calls)
	}
	if _, ok := body["session_id"]; ok {
		t.Fatal("invented user session")
	}
	access.manager = func(context.Context, string, string, func(context.Context) error) error {
		calls++
		return identity.Denied
	}
	w = httptest.NewRecorder()
	s.ServeHTTP(w, request())
	if w.Code != 403 || calls != 2 {
		t.Fatal("revocation must be checked again on next request")
	}
}

func TestAppAccessConfiguration(t *testing.T) {
	svc := &identity.Service{}
	if v, err := NewAppAccess("", "", svc); err != nil || v != nil {
		t.Fatal("empty configuration must disable")
	}
	for _, pair := range [][2]string{{"short", ""}, {strings.Repeat("x", 32), strings.Repeat("x", 32)}, {strings.Repeat("x", 32) + "\n", ""}} {
		if _, err := NewAppAccess(pair[0], pair[1], svc); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}
