package gateway

import (
	"context"
	"io"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func TestAppCenterFactsGatewayGuards(t *testing.T) {
	user := identity.IdentityContext{Roles: []string{"admin"}, Scopes: []string{"app:read"}}
	for _, tc := range []struct {
		name, target string
		body         bool
		status       int
	}{
		{"unavailable", "/appcenter/v1/facts", false, 503},
		{"query", "/appcenter/v1/facts?organization=other", false, 400},
		{"empty-query", "/appcenter/v1/facts?", false, 400},
		{"encoded", "/appcenter/v1/%66acts", false, 400},
		{"unframed-body", "/appcenter/v1/facts", true, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest("GET", tc.target, nil)
			if tc.body {
				r.Body = io.NopCloser(strings.NewReader("{}"))
				r.ContentLength = -1
			}
			w := httptest.NewRecorder()
			(&Server{}).serveAppCenter(w, r, context.Background(), "request", user)
			if w.Code != tc.status || w.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("status %d", w.Code)
			}
		})
	}
	w := httptest.NewRecorder()
	(&Server{}).serveAppCenter(w, httptest.NewRequest("GET", "/appcenter/v1/facts", nil), context.Background(), "request", identity.IdentityContext{})
	if w.Code != 403 {
		t.Fatal("authorization bypass")
	}
}
