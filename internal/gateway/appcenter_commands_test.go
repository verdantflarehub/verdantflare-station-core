package gateway

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
)

func TestAppCenterCommandGatewayValidation(t *testing.T) {
	digest := "sha256:" + strings.Repeat("a", 64)
	input := operations.AppCenterCommand{SchemaVersion: 1, Command: operations.Command{RequestID: "trace", IdempotencyKey: "command-key", OrganizationID: "00000000-0000-7000-8000-000000000002", StationID: "00000000-0000-7000-8000-000000000003", AppID: "test-app", AppVersion: "1.0.0", Action: "install"}, Expected: operations.AppCenterExpected{AppVersion: "1.0.0", ReleaseDigest: &digest, PolicyRevision: digest}}
	raw, _ := json.Marshal(input)
	actor := identity.IdentityContext{Roles: []string{"admin"}, Scopes: []string{"app:read", "app:manage"}}
	for _, test := range []struct {
		name, target, media, body string
		want                      int
	}{
		{"unavailable", "/appcenter/v1/commands", "application/json", string(raw), 503},
		{"query", "/appcenter/v1/commands?x=1", "application/json", string(raw), 400},
		{"empty-query", "/appcenter/v1/commands?", "application/json", string(raw), 400},
		{"encoded", "/appcenter/v1/%63ommands", "application/json", string(raw), 400},
		{"wrong-media", "/appcenter/v1/commands", "text/plain", string(raw), 400},
		{"missing-media", "/appcenter/v1/commands", "", string(raw), 400},
		{"duplicate", "/appcenter/v1/commands", "application/json", strings.Replace(string(raw), `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1), 400},
		{"oversized", "/appcenter/v1/commands", "application/json", strings.Repeat(" ", 16<<10) + string(raw), 400},
	} {
		t.Run(test.name, func(t *testing.T) {
			r := httptest.NewRequest("POST", test.target, strings.NewReader(test.body))
			r.Header.Set("Content-Type", test.media)
			w := httptest.NewRecorder()
			(&Server{}).serveAppCenterCommand(w, r, context.Background(), "trace", actor)
			if w.Code != test.want || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal(w.Code)
			}
		})
	}
	actor.Scopes = []string{"app:read"}
	r := httptest.NewRequest("POST", "/appcenter/v1/commands", strings.NewReader(string(raw)))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	(&Server{}).serveAppCenterCommand(w, r, context.Background(), "trace", actor)
	if w.Code != 403 {
		t.Fatal("manage permission bypassed", w.Code)
	}
}
