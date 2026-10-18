package gateway

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
)

func TestMigrateAppNamespaceEndpoint(t *testing.T) {
	stationID := uuid.Must(uuid.NewV7()).String()
	bootstrap := secret()
	mem, err := appstate.NewMemory(stationID, []appstate.Collector{
		{ID: "runtime", Namespaces: []string{"verdantflare-comfyui", "verdantflare-app"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	sess, err := mem.Open("runtime")
	if err != nil {
		t.Fatal(err)
	}
	orgID := uuid.Must(uuid.NewV7()).String()
	instID := uuid.Must(uuid.NewV7()).String()
	syncID := uuid.NewString()
	if err := sess.Begin(syncID); err != nil {
		t.Fatal(err)
	}
	if err := sess.Objects(syncID, []appstate.ResourceObservation{
		{
			Namespace: "verdantflare-comfyui", Name: "comfyui", UID: uuid.NewString(), ResourceVersion: "1",
			StationID: stationID, OrganizationID: orgID, InstallationID: instID, AppID: "comfyui",
			Generation: 1, ObservedGeneration: 1, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked",
		},
		{
			Namespace: "verdantflare-app", Name: "comfyui", UID: uuid.NewString(), ResourceVersion: "2",
			StationID: stationID, OrganizationID: orgID, InstallationID: instID, AppID: "comfyui",
			Generation: 1, ObservedGeneration: 1, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := sess.End(syncID, 2); err != nil {
		t.Fatal(err)
	}

	appStates := &appstate.Service{StationID: stationID, Observations: mem}
	ops := &operations.Service{StationID: stationID, AppStates: appStates}
	srv := &Server{
		AppStates:      appStates,
		Operations:     ops,
		BootstrapToken: bootstrap,
		Logger:         slog.New(slog.NewJSONHandler(io.Discard, nil)),
	}

	// 1. Unauthenticated / wrong token rejected with 403.
	unauthReq := httptest.NewRequest("POST", "/internal/v1/app-namespace/migrate", strings.NewReader(`{"app_id":"comfyui","source_namespace":"verdantflare-comfyui","target_namespace":"verdantflare-app"}`))
	unauthReq.Header.Set("Content-Type", "application/json")
	unauthW := httptest.NewRecorder()
	srv.ServeHTTP(unauthW, unauthReq)
	if unauthW.Code != 403 {
		t.Fatalf("expected 403 without bootstrap token, got %d", unauthW.Code)
	}

	// 2. Invalid payload rejected with 400.
	badReq := httptest.NewRequest("POST", "/internal/v1/app-namespace/migrate", strings.NewReader(`{"app_id":"INVALID_APP","source_namespace":"verdantflare-comfyui","target_namespace":"verdantflare-app"}`))
	badReq.Header.Set("Content-Type", "application/json")
	badReq.Header.Set("Authorization", "Bearer "+bootstrap)
	badW := httptest.NewRecorder()
	srv.ServeHTTP(badW, badReq)
	if badW.Code != 400 {
		t.Fatalf("expected 400 for invalid app_id, got %d", badW.Code)
	}

	// 3. Valid migration request succeeds and resets stale app bindings outside verdantflare-app.
	payload := `{
		"app_id": "comfyui",
		"installation_id": "` + instID + `",
		"source_namespace": "verdantflare-comfyui",
		"target_namespace": "verdantflare-app",
		"workspaces": [
			{
				"pvc_name": "comfyui-managed-a",
				"new_pvc_uid": "` + uuid.NewString() + `",
				"new_profile_hash": "sha256:76b8d4f8784899ea83bc546600cd13b848fda70c67709073f37f91041d759f75",
				"new_secret_uid": "` + uuid.NewString() + `"
			}
		]
	}`
	okReq := httptest.NewRequest("POST", "/internal/v1/app-namespace/migrate", strings.NewReader(payload))
	okReq.Header.Set("Content-Type", "application/json")
	okReq.Header.Set("Authorization", "Bearer "+bootstrap)
	okW := httptest.NewRecorder()
	srv.ServeHTTP(okW, okReq)
	if okW.Code != 200 {
		t.Fatalf("expected 200 for valid migration request, got %d: %s", okW.Code, okW.Body.String())
	}
	var resp struct {
		operations.NamespaceMigrationResult
		RequestID string `json:"request_id"`
	}
	if err := json.Unmarshal(okW.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.AppID != "comfyui" || resp.TargetNamespace != "verdantflare-app" || resp.RequestID == "" {
		t.Fatalf("unexpected migration response: %+v", resp)
	}

	views, _, err := mem.Views([]appstate.Binding{{InstallationID: instID, OrganizationID: orgID, AppID: "comfyui"}})
	if err != nil {
		t.Fatal(err)
	}
	if views[instID].Runtime.State != "ready" || views[instID].Resource == nil || views[instID].Resource.Namespace != "verdantflare-app" {
		t.Fatalf("expected binding reset to verdantflare-app ready state, got %+v", views[instID])
	}
}
