package gateway

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
)

func TestTransitionGatewayRejectsMalformedRequestsBeforeDatabase(t *testing.T) {
	actor := identity.IdentityContext{Roles: []string{"admin"}, Scopes: []string{"app:read", "app:manage"}}
	command := operations.TransitionCommand{RequestID: "trace", IdempotencyKey: "test-key", InstallationID: uuid.Must(uuid.NewV7()).String(), ExpectedRevision: 1, SourceVersion: "1.0.0", TargetReleaseRef: "sample-1.0.1", Action: "upgrade"}
	for _, scenario := range []string{"duplicate key", "unknown key", "null", "multiple values", "oversized", "query", "encoded path", "compressed", "chunked", "content type", "duplicate content type", "duplicate auth", "duplicate trace", "get body", "no admin", "no read", "no manage"} {
		t.Run(scenario, func(t *testing.T) {
			raw, _ := json.Marshal(command)
			switch scenario {
			case "duplicate key":
				raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"action":"upgrade"}`)
			case "unknown key":
				raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"image":"untrusted"}`)
			case "null":
				raw = []byte(`{"action":null}`)
			case "multiple values":
				raw = append(raw, []byte(`{}`)...)
			case "oversized":
				raw = append([]byte(strings.Repeat(" ", 16<<10)), raw...)
			}
			r := httptest.NewRequest(http.MethodPost, transitionPath, strings.NewReader(string(raw)))
			r.Header.Set("Authorization", "Bearer test-only")
			r.Header.Set("Content-Type", "application/json")
			u := actor
			want := 400
			switch scenario {
			case "query":
				r.URL.RawQuery = "identity=other"
			case "encoded path":
				r.URL.RawPath = "/%61pp-installation-transitions"
			case "compressed":
				r.Header.Set("Content-Encoding", "gzip")
			case "chunked":
				r.TransferEncoding = []string{"chunked"}
			case "content type":
				r.Header.Set("Content-Type", "text/plain")
			case "duplicate content type":
				r.Header.Add("Content-Type", "application/json")
			case "duplicate auth":
				r.Header.Add("Authorization", "Bearer other")
			case "duplicate trace":
				r.Header.Add("X-Request-ID", "a")
				r.Header.Add("X-Request-ID", "b")
			case "get body":
				r.Method = http.MethodGet
				r.URL.Path = transitionPath + "/" + command.InstallationID
			case "no admin":
				u.Roles = nil
				want = 403
			case "no read":
				u.Scopes = []string{"app:manage"}
				want = 403
			case "no manage":
				u.Scopes = []string{"app:read"}
				want = 403
			}
			w := httptest.NewRecorder()
			(&Server{}).serveTransitions(w, r, r.Context(), "http-trace", u)
			if w.Code != want {
				t.Fatal("rejected at incorrect boundary", w.Code, want)
			}
		})
	}
}
