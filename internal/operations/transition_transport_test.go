package operations

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
)

type transitionRoundTrip func(*http.Request) (*http.Response, error)

func (f transitionRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestTransitionTransportConfigurationAndBoundResponse(t *testing.T) {
	token := strings.Repeat("test-token-", 4)
	if c, e := NewHTTPTransitionRuntime("", ""); c != nil || e != nil {
		t.Fatal("disabled configuration changed")
	}
	for _, origin := range []string{"", "file:///local", "http://user:pass@manager", "http://manager/path", "http://manager?x=1", "http://manager#fragment"} {
		if _, err := NewHTTPTransitionRuntime(origin, token); err == nil {
			t.Fatal("invalid origin accepted")
		}
	}
	client, err := NewHTTPTransitionRuntime("http://runtime.test:5053", token)
	if err != nil {
		t.Fatal(err)
	}
	if client.client.Transport.(*http.Transport).Proxy != nil || client.client.Timeout <= 0 {
		t.Fatal("unsafe client defaults")
	}
	step := TransitionStep{uuid.Must(uuid.NewV7()).String(), uuid.NewString(), "sha256:" + strings.Repeat("a", 64), "release_target"}
	for _, scenario := range []string{"releasing", "released", "wrong nonce", "wrong mode", "duplicate", "unknown", "503", "redirect", "transport error", "wrong content type"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			client.client.Transport = transitionRoundTrip(func(r *http.Request) (*http.Response, error) {
				calls++
				if r.Method != "POST" || r.URL.Path != transitionReconcilePath || r.Header.Get("Authorization") != "Bearer "+token {
					t.Fatal("wrong private request")
				}
				var actual TransitionStep
				if json.NewDecoder(r.Body).Decode(&actual) != nil || actual != step {
					t.Fatal("binding changed in transport")
				}
				out := TransitionProgress{step, "released"}
				status := 200
				switch scenario {
				case "releasing":
					out.Phase = "releasing"
				case "wrong nonce":
					out.RequestID = uuid.NewString()
				case "wrong mode":
					out.Mode = "release_source"
				case "503":
					status = 503
				case "redirect":
					status = 307
				case "transport error":
					return nil, errors.New("private URL and token must not escape")
				}
				raw, _ := json.Marshal(out)
				if scenario == "duplicate" {
					raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"phase":"released"}`)
				}
				if scenario == "unknown" {
					raw = []byte(strings.TrimSuffix(string(raw), "}") + `,"internal":"private"}`)
				}
				header := http.Header{"Content-Type": []string{"application/json"}}
				if scenario == "redirect" {
					header.Set("Location", "http://untrusted.invalid")
				}
				if scenario == "wrong content type" {
					header.Set("Content-Type", "text/plain")
				}
				return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(string(raw)))}, nil
			})
			out, err := client.ReconcileTransition(context.Background(), step)
			valid := scenario == "releasing" || scenario == "released"
			if (err == nil) != valid || calls != 1 {
				t.Fatal("unexpected response handling", err, calls)
			}
			if valid && (out.Phase != scenario || out.TransitionStep != step) {
				t.Fatal("lost response binding")
			}
			if err != nil && strings.Contains(err.Error(), "private") {
				t.Fatal("transport details leaked")
			}
		})
	}
}
