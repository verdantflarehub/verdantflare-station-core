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

func TestResourcesGPUSchedulerDelegationRBACAndResidencyActions(t *testing.T) {
	const (
		stationID = "01900000-0000-7000-8000-000000000001"
		adminUser = "01900000-0000-7000-8000-000000000002"
		memberUsr = "01900000-0000-7000-8000-000000000099"
		orgID     = "01900000-0000-7000-8000-000000000003"
	)
	resToken := strings.Repeat("r", 32)
	runtimeToken := strings.Repeat("g", 32)

	runtimeCalls := 0
	var lastEvictBody runtimeGPUResidencyEvictRequest
	var lastForceBody runtimeGPUResidencyForceDrainRequest

	rtSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		runtimeCalls++
		if r.Header.Get("Authorization") != "Bearer "+runtimeToken {
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"error_code":"UNAUTHENTICATED"}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/internal/v1/gpu-scheduler/snapshot":
			_, _ = w.Write([]byte(`{"station_id":"` + stationID + `","app_gpu_quota":12,"cards":[{"gpu_index":1,"total_vram_mib":32607,"grantable_standard_mib":9856,"ledger_uncommitted_mib":21656,"evictable_mib":11800,"telemetry_fresh":true,"admission_open":true}],"active_leases":[],"residencies":[],"recent_events":[]}`))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/gpu-scheduler/residency/comfyuiA/evict":
			_ = json.NewDecoder(r.Body).Decode(&lastEvictBody)
			_, _ = w.Write([]byte(`{"instance_id":"comfyuiA","residency_state":"zero_vram_standby","boot_id":"boot-cfy-7c02","gpu_epoch":8,"residency_seq":65}`))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/gpu-scheduler/residency/blenderA/evict":
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"error_code":"GUI_EDIT_LEASE_HELD","message":"protected by viewport"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/internal/v1/gpu-scheduler/residency/blenderA/force-drain":
			if err := json.NewDecoder(r.Body).Decode(&lastForceBody); err != nil {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte(`{"error_code":"INVALID_REQUEST"}`))
				return
			}
			_, _ = w.Write([]byte(`{"instance_id":"blenderA","residency_state":"zero_vram_standby","boot_id":"boot-bld-a91f","gpu_epoch":4,"residency_seq":42}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error_code":"NOT_FOUND"}`))
		}
	}))
	defer rtSrv.Close()

	gpuCtrl, err := NewRuntimeControl(rtSrv.URL, runtimeToken)
	if err != nil {
		t.Fatalf("NewRuntimeControl: %v", err)
	}

	srv := &Server{
		Identity:            &identity.Service{StationID: stationID},
		Logger:              slog.New(slog.NewTextHandler(io.Discard, nil)),
		GPUSchedulerControl: gpuCtrl,
		AppAccess: &AppAccess{
			resourcesToken: resToken,
			manager: func(ctx context.Context, userID, org string, fn func(context.Context) error) error {
				if org != orgID || userID != adminUser {
					return identity.Denied
				}
				return fn(ctx)
			},
			member: func(ctx context.Context, userID, org string, fn func(context.Context) error) error {
				if org != orgID || (userID != adminUser && userID != memberUsr) {
					return identity.Denied
				}
				return fn(ctx)
			},
		},
	}

	doDelegated := func(method, path, userID, body string) (int, map[string]any) {
		var rdr io.Reader
		if body != "" {
			rdr = strings.NewReader(body)
		}
		req := httptest.NewRequest(method, path, rdr)
		req.Header.Set("Authorization", "Bearer "+resToken)
		req.Header.Set("X-User-Id", userID)
		req.Header.Set("X-Organization-Id", orgID)
		req.Header.Set("X-Station-Id", stationID)
		if body != "" {
			req.Header.Set("Content-Type", "application/json")
		}
		rec := httptest.NewRecorder()
		srv.ServeHTTP(rec, req)
		var out map[string]any
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}

	// 1. Member can read GPU scheduler snapshot, sees viewer_can_force_drain=false.
	code, snapMem := doDelegated(http.MethodGet, "/internal/v1/resources/gpu-scheduler", memberUsr, "")
	if code != http.StatusOK || snapMem["viewer_can_force_drain"] != false {
		t.Fatalf("expected member 200 with viewer_can_force_drain=false, got %d %+v", code, snapMem)
	}

	// 2. Admin can read GPU scheduler snapshot, sees viewer_can_force_drain=true.
	code, snapAdm := doDelegated(http.MethodGet, "/internal/v1/resources/gpu-scheduler", adminUser, "")
	if code != http.StatusOK || snapAdm["viewer_can_force_drain"] != true {
		t.Fatalf("expected admin 200 with viewer_can_force_drain=true, got %d %+v", code, snapAdm)
	}

	// 3. Member can trigger reversible S2 evict with optimistic CAS fields, missing expected_* returns 400 without calling Runtime, and S1 evict propagates 409 GUI_EDIT_LEASE_HELD.
	code, evOK := doDelegated(http.MethodPost, "/internal/v1/resources/gpu-residency/evict", memberUsr,
		`{"idempotency_key":"ev-1","instance_id":"comfyuiA","expected_boot_id":"boot-cfy-7c02","expected_gpu_epoch":8,"expected_residency_seq":64}`)
	if code != http.StatusOK || lastEvictBody.ExpectedBootID != "boot-cfy-7c02" || lastEvictBody.ExpectedGPUEpoch == nil || *lastEvictBody.ExpectedGPUEpoch != 8 || lastEvictBody.ExpectedResidencySeq == nil || *lastEvictBody.ExpectedResidencySeq != 64 {
		t.Fatalf("expected 200 on comfyuiA S2 evict forwarding CAS fields, got %d body=%+v resp=%+v", code, lastEvictBody, evOK)
	}
	beforeEvictNoVer := runtimeCalls
	code, evNoVer := doDelegated(http.MethodPost, "/internal/v1/resources/gpu-residency/evict", memberUsr, `{"idempotency_key":"ev-nover","instance_id":"comfyuiA"}`)
	if code != http.StatusBadRequest || runtimeCalls != beforeEvictNoVer {
		t.Fatalf("expected 400 without calling Runtime when expected_* missing on evict, got %d calls=%d->%d %+v", code, beforeEvictNoVer, runtimeCalls, evNoVer)
	}
	code, evBlocked := doDelegated(http.MethodPost, "/internal/v1/resources/gpu-residency/evict", memberUsr,
		`{"idempotency_key":"ev-2","instance_id":"blenderA","expected_boot_id":"boot-bld-a91f","expected_gpu_epoch":4,"expected_residency_seq":41}`)
	if code != http.StatusConflict || evBlocked["code"] != "GUI_EDIT_LEASE_HELD" {
		t.Fatalf("expected 409 GUI_EDIT_LEASE_HELD on S1 evict, got %d %+v", code, evBlocked)
	}

	// 4. Non-admin member MUST be rejected with 403 on force-drain BEFORE touching Runtime!
	beforeCalls := runtimeCalls
	code, fdDenied := doDelegated(http.MethodPost, "/internal/v1/resources/gpu-residency/force-drain", memberUsr,
		`{"idempotency_key":"fd-mem","instance_id":"blenderA","confirm_force_drain":true,"confirm_instance_id":"blenderA"}`)
	if code != http.StatusForbidden || runtimeCalls != beforeCalls {
		t.Fatalf("expected 403 without calling Runtime for member force-drain, got %d calls=%d->%d %+v", code, beforeCalls, runtimeCalls, fdDenied)
	}

	// 5. Admin without exact confirmation MUST be rejected with 400 CONFIRMATION_REQUIRED BEFORE touching Runtime (S5-N4)!
	code, fdUnconfirmed := doDelegated(http.MethodPost, "/internal/v1/resources/gpu-residency/force-drain", adminUser,
		`{"idempotency_key":"fd-bad","instance_id":"blenderA","confirm_force_drain":true,"confirm_instance_id":"wrong"}`)
	if code != http.StatusBadRequest || fdUnconfirmed["code"] != "CONFIRMATION_REQUIRED" || runtimeCalls != beforeCalls {
		t.Fatalf("expected 400 CONFIRMATION_REQUIRED without calling Runtime, got %d calls=%d->%d %+v", code, beforeCalls, runtimeCalls, fdUnconfirmed)
	}

	// 5b. Admin without complete expected_* target version MUST be rejected with 400 BEFORE touching Runtime!
	code, fdNoVer := doDelegated(http.MethodPost, "/internal/v1/resources/gpu-residency/force-drain", adminUser,
		`{"idempotency_key":"fd-nover","instance_id":"blenderA","confirm_force_drain":true,"confirm_instance_id":"blenderA","expected_boot_id":"boot-bld-a91f"}`)
	if code != http.StatusBadRequest || runtimeCalls != beforeCalls {
		t.Fatalf("expected 400 without calling Runtime when expected_* incomplete on force-drain, got %d calls=%d->%d %+v", code, beforeCalls, runtimeCalls, fdNoVer)
	}

	// 6. Admin with exact two-step confirmation succeeds and forwards actor_user_id, organization_id, and optimistic CAS fields.
	code, fdOK := doDelegated(http.MethodPost, "/internal/v1/resources/gpu-residency/force-drain", adminUser,
		`{"idempotency_key":"fd-ok","instance_id":"blenderA","confirm_force_drain":true,"confirm_instance_id":"blenderA","expected_boot_id":"boot-bld-a91f","expected_gpu_epoch":4,"expected_residency_seq":41}`)
	if code != http.StatusOK || lastForceBody.ActorUserID != adminUser || lastForceBody.OrganizationID != orgID || !lastForceBody.ConfirmForceDrain || lastForceBody.ExpectedBootID != "boot-bld-a91f" || lastForceBody.ExpectedGPUEpoch == nil || *lastForceBody.ExpectedGPUEpoch != 4 || lastForceBody.ExpectedResidencySeq == nil || *lastForceBody.ExpectedResidencySeq != 41 {
		t.Fatalf("expected 200 admin force-drain with forwarded identity & CAS fields, got %d body=%+v resp=%+v", code, lastForceBody, fdOK)
	}
}
