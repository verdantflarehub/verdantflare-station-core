package catalog

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestCheckInstallGrantIsLiveAndFailClosed(t *testing.T) {
	status := http.StatusOK
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/control/station/v2/apps/sample/versions/1.0.0/install-grant/station-org" || r.Header.Get("Authorization") != "Bearer station-token" {
			t.Errorf("unexpected entitlement request: %s", r.URL.Path)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(status)
		if status == http.StatusOK {
			_ = json.NewEncoder(w).Encode(map[string]any{"appId": "sample", "version": "1.0.0", "allowed": true})
		}
	}))
	defer server.Close()
	svc := &Service{marketCfg: &MarketConfig{BaseURL: server.URL, Token: "station-token", HTTPClient: server.Client()}}
	allowed, err := svc.CheckInstallGrant(context.Background(), "sample", "1.0.0", "station-org")
	if err != nil || !allowed {
		t.Fatalf("expected live grant, got allowed=%v err=%v", allowed, err)
	}
	status = http.StatusForbidden
	allowed, err = svc.CheckInstallGrant(context.Background(), "sample", "1.0.0", "station-org")
	if err != nil || allowed {
		t.Fatalf("expected live denial, got allowed=%v err=%v", allowed, err)
	}
	status = http.StatusServiceUnavailable
	allowed, err = svc.CheckInstallGrant(context.Background(), "sample", "1.0.0", "station-org")
	if err == nil || allowed {
		t.Fatalf("expected unavailable error, got allowed=%v err=%v", allowed, err)
	}
}
