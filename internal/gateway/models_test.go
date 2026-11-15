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
	"github.com/verdantflarehub/verdantflare-station-core/internal/resources"
)

type stubModelStore struct {
	calls int
	last  resources.ModelListFilter
	page  resources.ModelListPage
	err   error
}

func (s *stubModelStore) ListModels(_ context.Context, filter resources.ModelListFilter) (resources.ModelListPage, error) {
	s.calls++
	s.last = filter
	return s.page, s.err
}

func TestResourcesModelsGatewayAdminEnforcementAndEvictPassthrough(t *testing.T) {
	const (
		stationID = "01900000-0000-7000-8000-000000000001"
		adminUser = "01900000-0000-7000-8000-000000000002"
		memberUsr = "01900000-0000-7000-8000-000000000099"
		orgID     = "01900000-0000-7000-8000-000000000003"
		shaReady  = "31e35c80fc4829d14f90153f4c74cd59c90b779f6afe05a74cd6120b893f7e5b"
		shaPinned = "aaaaaaaaaaaa29d14f90153f4c74cd59c90b779f6afe05a74cd6120b893f7e5b"
	)
	resToken := strings.Repeat("r", 32)
	runtimeToken := strings.Repeat("t", 32)

	authCalls := 0
	runtimeCalls := 0

	rtSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeCalls++
		if r.Header.Get("Authorization") != "Bearer "+runtimeToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"UNAUTHENTICATED"}`))
			return
		}
		var req runtimeModelEvictRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"code":"INVALID_ARGUMENT"}`))
			return
		}
		if req.StationID != stationID || req.OrganizationID != orgID || req.UserID != adminUser {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"code":"PERMISSION_DENIED"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/internal/v1/model-cache/models/" + shaPinned + "/evict":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"code":"MODEL_IN_USE","message":"pinned at /data/models/comfyui/cas/secret","active_pins":2,"reserved_pins":1}`))
		case "/internal/v1/model-cache/models/" + shaReady + "/evict":
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"status":"not_installed","model":{"catalog_code":"MDL-CKPT-SDXL-001","status":"not_installed","download_fence":4}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"code":"MODEL_NOT_FOUND"}`))
		}
	}))
	defer rtSrv.Close()

	rtControl, err := NewRuntimeControl(rtSrv.URL, runtimeToken)
	if err != nil {
		t.Fatalf("NewRuntimeControl: %v", err)
	}

	totalBytes := int64(3200) * 1024 * 1024 * 1024
	freeBytes := int64(1480) * 1024 * 1024 * 1024
	compliant := true
	store := &stubModelStore{
		page: resources.ModelListPage{
			SchemaVersion: 1,
			MountPath:     resources.ContainerModelsMountPath,
			Watermark: resources.StorageWatermark{
				FilesystemTotalBytes:  &totalBytes,
				FilesystemFreeBytes:   &freeBytes,
				ReservedHeadroomBytes: resources.DefaultReservedHeadroomBytes,
				MaxAutoPullModelBytes: resources.DefaultMaxAutoPullModelBytes,
				HeadroomCompliant:     &compliant,
			},
			Models: []resources.ModelCacheItem{
				{
					CatalogCode:      "MDL-CKPT-SDXL-001",
					ModelID:          "sdxl-base-1_0",
					DisplayName:      "SDXL Base 1.0",
					Category:         "CKPT",
					CategoryDir:      "checkpoints",
					Filename:         "sd_xl_base_1.0.safetensors",
					RelativeViewPath: "checkpoints/31e35c80fc48__sd_xl_base_1.0.safetensors",
					SHA256:           shaReady,
					SizeBytes:        6938078334,
					VRAMFootprintMiB: 6912,
					License:          "openrail++",
					LicenseTier:      "commercial_free",
					PaidAccess:       false,
					OriginKind:       "public",
					ApprovalState:    "approved",
					Status:           "ready",
					DownloadFence:    3,
					BytesDownloaded:  6938078334,
					PinScopes:        []string{},
				},
			},
			TotalCount: 1,
		},
	}

	access := &AppAccess{
		networkToken:   strings.Repeat("n", 32),
		resourcesToken: resToken,
		manager: func(ctx context.Context, user, org string, effect func(context.Context) error) error {
			authCalls++
			if org != orgID || user != adminUser {
				return identity.Denied
			}
			return effect(ctx)
		},
	}

	s := &Server{
		Identity:          &identity.Service{StationID: stationID},
		AppAccess:         access,
		ModelCacheControl: rtControl,
		ModelStore:        store,
		Logger:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}

	makeDelegatedReq := func(method, target, user string, body string) *http.Request {
		var reader io.Reader
		if body != "" {
			reader = strings.NewReader(body)
		}
		r := httptest.NewRequest(method, target, reader)
		r.Header.Set("Authorization", "Bearer "+resToken)
		r.Header.Set("X-User-Id", user)
		r.Header.Set("X-Organization-Id", orgID)
		r.Header.Set("X-Station-Id", stationID)
		if body != "" {
			r.Header.Set("Content-Type", "application/json")
		}
		return r
	}

	// 1. Disallowed query parameter (?admin=true) must fail 400 BEFORE identity/DB lookup.
	w := httptest.NewRecorder()
	s.ServeHTTP(w, makeDelegatedReq(http.MethodGet, "/internal/v1/resources/models?admin=true", adminUser, ""))
	if w.Code != http.StatusBadRequest || authCalls != 0 || store.calls != 0 {
		t.Fatalf("expected 400 with 0 auth/store calls, got status=%d authCalls=%d storeCalls=%d", w.Code, authCalls, store.calls)
	}

	// 2. Non-admin forwarded X-User-Id must fail 403 PERMISSION_DENIED on GET (never trusting selector headers alone).
	w = httptest.NewRecorder()
	s.ServeHTTP(w, makeDelegatedReq(http.MethodGet, "/internal/v1/resources/models?category=CKPT&status=ready&limit=10", memberUsr, ""))
	if w.Code != http.StatusForbidden || store.calls != 0 {
		t.Fatalf("non-admin GET must fail 403 with 0 store calls: status=%d storeCalls=%d body=%s", w.Code, store.calls, w.Body.String())
	}

	// 3. Verified admin GET succeeds, passes filter, and leaks zero host paths.
	w = httptest.NewRecorder()
	s.ServeHTTP(w, makeDelegatedReq(http.MethodGet, "/internal/v1/resources/models?category=CKPT&status=ready&limit=10", adminUser, ""))
	if w.Code != http.StatusOK || store.calls != 1 || store.last.Category != "CKPT" || store.last.Status != "ready" || store.last.Limit != 10 {
		t.Fatalf("admin GET failed: status=%d filter=%+v body=%s", w.Code, store.last, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "/data/models") {
		t.Fatalf("host path leaked in GET response: %s", w.Body.String())
	}
	var listResp struct {
		MountPath string                     `json:"mount_path"`
		Models    []resources.ModelCacheItem `json:"models"`
		RequestID string                     `json:"request_id"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &listResp); err != nil || listResp.MountPath != "/models" || len(listResp.Models) != 1 || listResp.Models[0].RelativeViewPath != "checkpoints/31e35c80fc48__sd_xl_base_1.0.safetensors" {
		t.Fatalf("unexpected GET payload: %+v err=%v", listResp, err)
	}

	// 4. Non-admin POST /evict must fail 403 BEFORE contacting Runtime.
	w = httptest.NewRecorder()
	s.ServeHTTP(w, makeDelegatedReq(http.MethodPost, "/internal/v1/resources/models/"+shaReady+"/evict", memberUsr, `{"idempotency_key":"evict-key-0001"}`))
	if w.Code != http.StatusForbidden || runtimeCalls != 0 {
		t.Fatalf("non-admin evict must fail 403 with 0 runtime calls: status=%d runtimeCalls=%d", w.Code, runtimeCalls)
	}

	// 5. Invalid SHA-256 on POST /evict must fail 400 BEFORE contacting Runtime.
	w = httptest.NewRecorder()
	s.ServeHTTP(w, makeDelegatedReq(http.MethodPost, "/internal/v1/resources/models/NOT_A_SHA256/evict", adminUser, ""))
	if w.Code != http.StatusBadRequest || runtimeCalls != 0 {
		t.Fatalf("invalid sha256 must fail 400 with 0 runtime calls: status=%d runtimeCalls=%d", w.Code, runtimeCalls)
	}

	// 6. Wrong HTTP method (GET on /evict) must fail 405 Method Not Allowed.
	w = httptest.NewRecorder()
	s.ServeHTTP(w, makeDelegatedReq(http.MethodGet, "/internal/v1/resources/models/"+shaReady+"/evict", adminUser, ""))
	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET on /evict expected 405, got %d", w.Code)
	}

	// 7. Pinned model eviction preserves 409 MODEL_IN_USE and scrubs any Runtime host path.
	w = httptest.NewRecorder()
	s.ServeHTTP(w, makeDelegatedReq(http.MethodPost, "/internal/v1/resources/models/"+shaPinned+"/evict", adminUser, `{"idempotency_key":"evict-pinned-0001"}`))
	if w.Code != http.StatusConflict || runtimeCalls != 1 {
		t.Fatalf("pinned evict expected 409, got status=%d runtimeCalls=%d body=%s", w.Code, runtimeCalls, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "/data/models") {
		t.Fatalf("host path leaked in 409 response: %s", w.Body.String())
	}
	var conflictBody modelEvictErrorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &conflictBody); err != nil || conflictBody.Code != "MODEL_IN_USE" || conflictBody.ActivePins != 2 || conflictBody.ReservedPins != 1 {
		t.Fatalf("409 MODEL_IN_USE payload lost details: %+v err=%v", conflictBody, err)
	}

	// 8. Unpinned ready model eviction succeeds 200.
	w = httptest.NewRecorder()
	s.ServeHTTP(w, makeDelegatedReq(http.MethodPost, "/internal/v1/resources/models/"+shaReady+"/evict", adminUser, ""))
	if w.Code != http.StatusOK || runtimeCalls != 2 {
		t.Fatalf("ready evict expected 200, got status=%d runtimeCalls=%d body=%s", w.Code, runtimeCalls, w.Body.String())
	}
}
