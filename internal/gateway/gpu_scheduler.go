package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

type gpuResidencyEvictInput struct {
	IdempotencyKey       string `json:"idempotency_key"`
	InstanceID           string `json:"instance_id"`
	ExpectedBootID       string `json:"expected_boot_id,omitempty"`
	ExpectedGPUEpoch     *int64 `json:"expected_gpu_epoch,omitempty"`
	ExpectedResidencySeq *int64 `json:"expected_residency_seq,omitempty"`
}

type gpuResidencyForceDrainInput struct {
	IdempotencyKey       string `json:"idempotency_key"`
	InstanceID           string `json:"instance_id"`
	ConfirmForceDrain    bool   `json:"confirm_force_drain"`
	ConfirmInstanceID    string `json:"confirm_instance_id"`
	ExpectedBootID       string `json:"expected_boot_id,omitempty"`
	ExpectedGPUEpoch     *int64 `json:"expected_gpu_epoch,omitempty"`
	ExpectedResidencySeq *int64 `json:"expected_residency_seq,omitempty"`
}

type runtimeGPUResidencyEvictRequest struct {
	IdempotencyKey       string `json:"idempotency_key"`
	ExpectedBootID       string `json:"expected_boot_id,omitempty"`
	ExpectedGPUEpoch     *int64 `json:"expected_gpu_epoch,omitempty"`
	ExpectedResidencySeq *int64 `json:"expected_residency_seq,omitempty"`
	ActorUserID          string `json:"actor_user_id"`
	OrganizationID       string `json:"organization_id"`
}

type runtimeGPUResidencyForceDrainRequest struct {
	IdempotencyKey       string `json:"idempotency_key"`
	ConfirmForceDrain    bool   `json:"confirm_force_drain"`
	ConfirmInstanceID    string `json:"confirm_instance_id"`
	ExpectedBootID       string `json:"expected_boot_id,omitempty"`
	ExpectedGPUEpoch     *int64 `json:"expected_gpu_epoch,omitempty"`
	ExpectedResidencySeq *int64 `json:"expected_residency_seq,omitempty"`
	ActorUserID          string `json:"actor_user_id"`
	OrganizationID       string `json:"organization_id"`
}

func validResidencyInstanceSegment(id string) bool {
	if len(id) < 1 || len(id) > 128 {
		return false
	}
	if id == "." || id == ".." || strings.ContainsAny(id, "/\\?# \t\r\n") {
		return false
	}
	return true
}

func validExpectedResidencyVersion(bootID string, epoch, seq *int64) bool {
	trimmedBoot := strings.TrimSpace(bootID)
	if len(trimmedBoot) < 1 || len(trimmedBoot) > 128 {
		return false
	}
	if epoch == nil || *epoch < 0 || seq == nil || *seq < 0 {
		return false
	}
	return true
}

func (s *Server) internalResourcesGPUScheduler(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string) {
	user, org, ok := s.delegatedApp(w, r, requestID, "resources")
	if !ok {
		return
	}
	s.serveGPUSchedulerSnapshot(w, r, ctx, requestID, user, org, s.AppAccess.withMember, s.AppAccess.manager)
}

func (s *Server) resourcesGPUScheduler(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, user identity.IdentityContext) {
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.URL.ForceQuery {
		failure(w, requestID, identity.Invalid)
		return
	}
	s.serveGPUSchedulerSnapshot(w, r, ctx, requestID, user.UserID, user.OrganizationID, s.Identity.WithMember, s.Identity.WithManager)
}

func (s *Server) serveGPUSchedulerSnapshot(
	w http.ResponseWriter,
	_ *http.Request,
	ctx context.Context,
	requestID, userID, orgID string,
	withMember func(context.Context, string, string, func(context.Context) error) error,
	withManager func(context.Context, string, string, func(context.Context) error) error,
) {
	control := s.GPUSchedulerControl
	if control == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "GPU scheduler runtime is not configured", requestID})
		return
	}
	if err := withMember(ctx, userID, orgID, func(context.Context) error { return nil }); err != nil {
		failure(w, requestID, err)
		return
	}
	canForceDrain := withManager != nil && withManager(ctx, userID, orgID, func(context.Context) error { return nil }) == nil
	allowedActions := []string{"evict"}
	if canForceDrain {
		allowedActions = append(allowedActions, "force_drain")
	}

	qCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(qCtx, http.MethodGet, control.Endpoint+"/internal/v1/gpu-scheduler/snapshot", nil)
	if err != nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Failed to build GPU scheduler snapshot request", requestID})
		return
	}
	req.Header.Set("Authorization", "Bearer "+control.RuntimeToken)
	req.Header.Set("X-Request-ID", requestID)
	resp, err := control.Client.Do(req)
	if err != nil {
		reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Failed to query GPU scheduler snapshot from Runtime", requestID})
		return
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (512<<10)+1))
	if err != nil || resp.StatusCode != 200 || len(raw) > 512<<10 || resp.Header.Get("Content-Encoding") != "" || !json.Valid(raw) {
		reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Invalid GPU scheduler snapshot response from Runtime", requestID})
		return
	}
	reply(w, 200, map[string]any{
		"snapshot":               json.RawMessage(raw),
		"viewer_can_force_drain": canForceDrain,
		"allowed_actions":        allowedActions,
		"request_id":             requestID,
	})
}

func (s *Server) internalResourcesEvictGPUResidency(w http.ResponseWriter, r *http.Request, requestID string) {
	user, org, ok := s.delegatedApp(w, r, requestID, "resources")
	if !ok {
		return
	}
	s.serveEvictGPUResidency(w, r, requestID, user, org, s.AppAccess.withMember)
}

func (s *Server) resourcesEvictGPUResidency(w http.ResponseWriter, r *http.Request, requestID string, user identity.IdentityContext) {
	s.serveEvictGPUResidency(w, r, requestID, user.UserID, user.OrganizationID, s.Identity.WithMember)
}

func (s *Server) serveEvictGPUResidency(
	w http.ResponseWriter,
	r *http.Request,
	requestID, userID, orgID string,
	withMember func(context.Context, string, string, func(context.Context) error) error,
) {
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.URL.ForceQuery || r.Header.Get("Content-Encoding") != "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	control := s.GPUSchedulerControl
	if control == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "GPU scheduler runtime is not configured", requestID})
		return
	}
	var in gpuResidencyEvictInput
	if err := decode(w, r, &in, false); err != nil {
		failure(w, requestID, err)
		return
	}
	if len(in.IdempotencyKey) < 1 || len(in.IdempotencyKey) > 128 || !validResidencyInstanceSegment(in.InstanceID) {
		failure(w, requestID, identity.Invalid)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	client := *control.Client
	client.Timeout = 20 * time.Second

	var status int
	var raw []byte
	err := withMember(ctx, userID, orgID, func(ctx context.Context) error {
		if !validExpectedResidencyVersion(in.ExpectedBootID, in.ExpectedGPUEpoch, in.ExpectedResidencySeq) {
			return identity.Invalid
		}
		body, _ := json.Marshal(runtimeGPUResidencyEvictRequest{
			IdempotencyKey:       in.IdempotencyKey,
			ExpectedBootID:       in.ExpectedBootID,
			ExpectedGPUEpoch:     in.ExpectedGPUEpoch,
			ExpectedResidencySeq: in.ExpectedResidencySeq,
			ActorUserID:          userID,
			OrganizationID:       orgID,
		})
		target := control.Endpoint + "/internal/v1/gpu-scheduler/residency/" + url.PathEscape(in.InstanceID) + "/evict"
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+control.RuntimeToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Request-ID", requestID)
		resp, err := client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		raw, err = io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
		if err != nil || len(raw) > 64<<10 || resp.Header.Get("Content-Encoding") != "" || !json.Valid(raw) {
			return errors.New("invalid Runtime GPU residency evict response")
		}
		status = resp.StatusCode
		if status != 200 && status != 400 && status != 401 && status != 403 && status != 404 && status != 409 && status != 503 {
			return errors.New("invalid Runtime GPU residency evict status")
		}
		return nil
	})
	if err != nil {
		failure(w, requestID, err)
		return
	}
	if status == 200 {
		reply(w, 200, map[string]any{"result": json.RawMessage(raw), "request_id": requestID})
		return
	}
	replyRuntimeResidencyError(w, status, raw, requestID)
}

func (s *Server) internalResourcesForceDrainGPUResidency(w http.ResponseWriter, r *http.Request, requestID string) {
	user, org, ok := s.delegatedApp(w, r, requestID, "resources")
	if !ok {
		return
	}
	s.serveForceDrainGPUResidency(w, r, requestID, user, org, s.AppAccess.manager)
}

func (s *Server) resourcesForceDrainGPUResidency(w http.ResponseWriter, r *http.Request, requestID string, user identity.IdentityContext) {
	s.serveForceDrainGPUResidency(w, r, requestID, user.UserID, user.OrganizationID, s.Identity.WithManager)
}

func (s *Server) serveForceDrainGPUResidency(
	w http.ResponseWriter,
	r *http.Request,
	requestID, userID, orgID string,
	withManager func(context.Context, string, string, func(context.Context) error) error,
) {
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.URL.ForceQuery || r.Header.Get("Content-Encoding") != "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	control := s.GPUSchedulerControl
	if control == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "GPU scheduler runtime is not configured", requestID})
		return
	}
	var in gpuResidencyForceDrainInput
	if err := decode(w, r, &in, false); err != nil {
		failure(w, requestID, err)
		return
	}
	if len(in.IdempotencyKey) < 1 || len(in.IdempotencyKey) > 128 || !validResidencyInstanceSegment(in.InstanceID) {
		failure(w, requestID, identity.Invalid)
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	// Enforce organization admin role BEFORE checking confirmation or calling Runtime.
	if err := withManager(ctx, userID, orgID, func(context.Context) error { return nil }); err != nil {
		failure(w, requestID, err)
		return
	}
	if !in.ConfirmForceDrain || strings.TrimSpace(in.ConfirmInstanceID) != in.InstanceID {
		reply(w, 400, ErrorResponse{"CONFIRMATION_REQUIRED", "confirm_force_drain=true and exact confirm_instance_id match are required", requestID})
		return
	}
	if !validExpectedResidencyVersion(in.ExpectedBootID, in.ExpectedGPUEpoch, in.ExpectedResidencySeq) {
		failure(w, requestID, identity.Invalid)
		return
	}

	client := *control.Client
	client.Timeout = 20 * time.Second

	body, _ := json.Marshal(runtimeGPUResidencyForceDrainRequest{
		IdempotencyKey:       in.IdempotencyKey,
		ConfirmForceDrain:    in.ConfirmForceDrain,
		ConfirmInstanceID:    in.ConfirmInstanceID,
		ExpectedBootID:       in.ExpectedBootID,
		ExpectedGPUEpoch:     in.ExpectedGPUEpoch,
		ExpectedResidencySeq: in.ExpectedResidencySeq,
		ActorUserID:          userID,
		OrganizationID:       orgID,
	})
	target := control.Endpoint + "/internal/v1/gpu-scheduler/residency/" + url.PathEscape(in.InstanceID) + "/force-drain"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target, bytes.NewReader(body))
	if err != nil {
		failure(w, requestID, err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+control.RuntimeToken)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Request-ID", requestID)
	resp, err := client.Do(req)
	if err != nil {
		reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Failed to execute GPU residency force-drain on Runtime", requestID})
		return
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (64<<10)+1))
	if err != nil || len(raw) > 64<<10 || resp.Header.Get("Content-Encoding") != "" || !json.Valid(raw) {
		reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Invalid GPU residency force-drain response from Runtime", requestID})
		return
	}
	status := resp.StatusCode
	if status == 200 {
		reply(w, 200, map[string]any{"result": json.RawMessage(raw), "request_id": requestID})
		return
	}
	if status != 400 && status != 401 && status != 403 && status != 404 && status != 409 && status != 503 {
		reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Unexpected GPU residency force-drain status from Runtime", requestID})
		return
	}
	replyRuntimeResidencyError(w, status, raw, requestID)
}

func replyRuntimeResidencyError(w http.ResponseWriter, status int, raw []byte, requestID string) {
	var errResp struct {
		ErrorCode string `json:"error_code"`
		Code      string `json:"code"`
		Message   string `json:"message"`
	}
	_ = json.Unmarshal(raw, &errResp)
	code := errResp.ErrorCode
	if code == "" {
		code = errResp.Code
	}
	if code == "" {
		code = "SERVICE_UNAVAILABLE"
	}
	msg := errResp.Message
	switch code {
	case "CONFIRMATION_REQUIRED":
		msg = "confirm_force_drain=true and exact confirm_instance_id match are required"
	case "GUI_EDIT_LEASE_HELD":
		msg = "Instance is protected by an active human GUI/viewport session"
	case "INSTANCE_ACTIVE_COMPUTE":
		msg = "Instance holds an active compute lease and cannot be soft-evicted"
	case "ALREADY_ZERO_VRAM":
		msg = "Instance is already in zero-VRAM standby"
	case "INSTANCE_NOT_EVICTABLE":
		msg = "Instance is not in an idle evictable state"
	case "RESIDENCY_NOT_FOUND":
		msg = "GPU residency instance was not found"
	case "RECLAIM_FAILED_QUARANTINED":
		msg = "GPU worker failed to release VRAM and has been quarantined"
	case "UNKNOWN_NODE_STATE":
		msg = "Node GPU verification is unavailable; fail-closed reconciling"
	}
	if msg == "" {
		msg = "GPU residency action failed"
	}
	reply(w, status, ErrorResponse{code, msg, requestID})
}
