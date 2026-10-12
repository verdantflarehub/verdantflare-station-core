package appstate

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/google/uuid"
)

// This helper is launched as a separate process by the Runtime integration
// test on 5090. The regular Core suite explicitly excludes this entrypoint.
func TestCollectorBridge(t *testing.T) {
	path := os.Getenv("STATION_APP_STATE_BRIDGE_CONFIG")
	if path == "" {
		t.Skip("cross-process bridge entrypoint")
	}
	s, u, id := fixture(t)
	secretBytes := make([]byte, 32)
	if _, err := rand.Read(secretBytes); err != nil {
		t.Fatal("bridge credential generation failed")
	}
	token := hex.EncodeToString(secretBytes)
	h, err := LoadCollectors(collectorConfig(t), func(string) string { return token }, s, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/test/snapshot" {
			if r.Header.Get("Authorization") != "Bearer "+token {
				w.WriteHeader(401)
				return
			}
			v, err := s.Snapshot(r.Context(), u, id)
			if err != nil {
				w.WriteHeader(503)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(v)
			return
		}
		h.ServeHTTP(w, r)
	}))
	defer server.Close()
	resource := ResourceObservation{Namespace: "apps", Name: "fixture-app", UID: uuid.NewString(), ResourceVersion: "bridge:list", StationID: u.StationID, OrganizationID: u.OrganizationID, InstallationID: id, AppID: "fixture-app", Generation: 1, ObservedGeneration: 1, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked"}
	raw, _ := json.Marshal(struct {
		Origin, Token string
		Resource      ResourceObservation
	}{server.URL, token, resource})
	if os.WriteFile(path+".pending", raw, 0600) != nil || os.Rename(path+".pending", path) != nil {
		t.Fatal("bridge configuration unavailable")
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	v := snapshot(t, s, u, id)
	if v.Items[0].Runtime.State != "starting" || v.Items[0].Installation.State != "installed" {
		t.Fatal("cross-process observation not reflected")
	}
}
