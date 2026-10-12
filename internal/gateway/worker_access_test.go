package gateway

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
)

func TestWorkerAccessSeparatesManagementVersionAndRechecksAuthority(t *testing.T) {
	const station = "01900000-0000-7000-8000-000000000001"
	const user = "01900000-0000-7000-8000-000000000002"
	const org = "01900000-0000-7000-8000-000000000003"
	const installation = "01900000-0000-7000-8000-000000000004"
	token := strings.Repeat("w", 40)
	item := operations.Installation{InstallationID: installation, StationID: station, OrganizationID: org, AppID: "comfyui", AppVersion: "0.1.4", State: "installed", Revision: 7}
	allowed, compatibilityCalls := true, 0
	var compatibilityError error
	a := &InstallationAccess{
		callers: map[[32]byte]string{sha256.Sum256([]byte("Bearer " + token)): "comfyui"},
		manager: func(ctx context.Context, u, o string, effect func(context.Context) error) error {
			if u != user || o != org || !allowed {
				return identity.Denied
			}
			return effect(ctx)
		},
		installation: func(_ context.Context, actor identity.IdentityContext, id string) (operations.Installation, error) {
			if id != installation || actor.StationID != station || actor.OrganizationID != org {
				t.Fatal("wrong scope")
			}
			return item, nil
		},
		worker: func(_ context.Context, got operations.Installation, version string) error {
			compatibilityCalls++
			if got != item || version != "0.1.3" {
				t.Fatal("worker proof changed")
			}
			return compatibilityError
		},
	}
	s := &Server{Identity: &identity.Service{StationID: station}, InstallationAccess: a, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	request := func(path, body string, mutate func(*http.Request)) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", "/internal/v1/app-installations/"+path, strings.NewReader(body))
		for key, value := range map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + token, "X-User-Id": user, "X-Organization-Id": org, "X-Station-Id": station} {
			r.Header.Set(key, value)
		}
		if mutate != nil {
			mutate(r)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	body := `{"installation_id":"` + installation + `","worker_version":"0.1.3"}`
	for range 2 {
		w := request("authorize-worker", body, nil)
		var out map[string]any
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(out) != 10 || out["worker_version"] != "0.1.3" || out["app_version"] != "0.1.4" || out["installation_revision"] != float64(7) {
			t.Fatal("invalid worker grant", w.Code)
		}
	}
	if compatibilityCalls != 2 {
		t.Fatal("cached authority")
	}
	before := compatibilityCalls
	for _, tc := range []struct {
		path, body string
		status     int
		mutate     func(*http.Request)
	}{
		{"authorize", `{"installation_id":"` + installation + `","app_version":"0.1.3"}`, 403, nil},
		{"authorize", body, 400, nil},
		{"authorize-worker", strings.Replace(body, "worker_version", "app_version", 1), 400, nil},
		{"authorize-worker", strings.TrimSuffix(body, "}") + `,"worker_version":"0.1.3"}`, 400, nil},
		{"authorize-worker", body, 401, func(r *http.Request) { r.Header.Del("Authorization") }},
		{"authorize-worker", body, 400, func(r *http.Request) { r.Header.Add("X-User-Id", user) }},
	} {
		if w := request(tc.path, tc.body, tc.mutate); w.Code != tc.status || compatibilityCalls != before {
			t.Fatal("invalid request reached compatibility", w.Code, tc.status)
		}
	}
	compatibilityError = operations.ErrTransitionRelease
	if w := request("authorize-worker", body, nil); w.Code != 503 {
		t.Fatal("missing material reused old grant", w.Code)
	}
	compatibilityError = identity.Denied
	if w := request("authorize-worker", body, nil); w.Code != 403 {
		t.Fatal("incompatible worker authorized", w.Code)
	}
	compatibilityError = nil
	before = compatibilityCalls
	allowed = false
	if w := request("authorize-worker", body, nil); w.Code != 403 || compatibilityCalls != before {
		t.Fatal("revoked membership authorized")
	}
	allowed = true
	item.State = "removed"
	if w := request("authorize-worker", body, nil); w.Code != 403 || compatibilityCalls != before {
		t.Fatal("removed installation authorized")
	}
}
