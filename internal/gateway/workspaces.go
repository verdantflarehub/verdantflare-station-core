package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

type workspaceReleaseInput struct {
	IdempotencyKey string `json:"idempotency_key"`
	Namespace      string `json:"namespace"`
	PVCName        string `json:"pvc_name"`
}

type runtimeWorkspaceReleaseRequest struct {
	StationID      string `json:"station_id"`
	OrganizationID string `json:"organization_id"`
	UserID         string `json:"user_id"`
	IdempotencyKey string `json:"idempotency_key"`
	Namespace      string `json:"namespace"`
	PVCName        string `json:"pvc_name"`
}

func (s *Server) resourcesWorkspaces(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, user identity.IdentityContext) {
	if r.URL.RawQuery != "" || r.URL.RawPath != "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	control := s.BlenderControl
	if control == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Instance runtime is not configured", requestID})
		return
	}
	var payload struct {
		Workspaces json.RawMessage `json:"workspaces"`
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, control.Endpoint+"/internal/v1/instances/workspaces", nil)
	if err != nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Failed to build workspace query request", requestID})
		return
	}
	req.Header.Set("Authorization", "Bearer "+control.RuntimeToken)
	req.Header.Set("X-Request-ID", requestID)
	resp, err := control.Client.Do(req)
	if err != nil {
		reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Failed to query workspace volumes from Runtime", requestID})
		return
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, (128<<10)+1))
	if err != nil || resp.StatusCode != 200 || len(raw) > 128<<10 || resp.Header.Get("Content-Encoding") != "" || json.Unmarshal(raw, &payload) != nil || len(payload.Workspaces) == 0 {
		reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Invalid workspace volumes response from Runtime", requestID})
		return
	}
	_ = user
	reply(w, 200, map[string]any{"workspaces": payload.Workspaces, "request_id": requestID})
}

func (s *Server) resourcesReleaseWorkspace(w http.ResponseWriter, r *http.Request, requestID string, user identity.IdentityContext) {
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	control := s.BlenderControl
	if control == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Instance runtime is not configured", requestID})
		return
	}
	var in workspaceReleaseInput
	if err := decode(w, r, &in, false); err != nil {
		failure(w, requestID, err)
		return
	}
	if in.IdempotencyKey == "" || in.Namespace == "" || in.PVCName == "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()
	client := *control.Client
	client.Timeout = 20 * time.Second

	var status int
	var raw []byte
	err := s.Identity.WithManager(ctx, user.UserID, user.OrganizationID, func(ctx context.Context) error {
		body, _ := json.Marshal(runtimeWorkspaceReleaseRequest{
			StationID:      s.Identity.StationID,
			OrganizationID: user.OrganizationID,
			UserID:         user.UserID,
			IdempotencyKey: in.IdempotencyKey,
			Namespace:      in.Namespace,
			PVCName:        in.PVCName,
		})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, control.Endpoint+"/internal/v1/instances/workspaces/release", bytes.NewReader(body))
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
		raw, err = io.ReadAll(io.LimitReader(resp.Body, (32<<10)+1))
		if err != nil || len(raw) > 32<<10 || resp.Header.Get("Content-Encoding") != "" || !json.Valid(raw) {
			return errors.New("invalid Runtime workspace release response")
		}
		status = resp.StatusCode
		if status != 200 && status != 400 && status != 401 && status != 403 && status != 404 && status != 409 && status != 503 {
			return errors.New("invalid Runtime workspace release status")
		}
		return nil
	})
	if err != nil {
		failure(w, requestID, err)
		return
	}
	if status == 200 {
		var okResp struct {
			Status    string          `json:"status"`
			Workspace json.RawMessage `json:"workspace"`
		}
		if json.Unmarshal(raw, &okResp) != nil || len(okResp.Workspace) == 0 {
			reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Invalid workspace release payload from Runtime", requestID})
			return
		}
		reply(w, 200, map[string]any{"status": okResp.Status, "workspace": okResp.Workspace, "request_id": requestID})
		return
	}
	var errResp struct {
		Code string `json:"code"`
	}
	_ = json.Unmarshal(raw, &errResp)
	if errResp.Code == "" {
		errResp.Code = "SERVICE_UNAVAILABLE"
	}
	msg := "Workspace volume release failed"
	switch errResp.Code {
	case "INVALID_ARGUMENT":
		msg = "Workspace release arguments are invalid"
	case "NOT_FOUND":
		msg = "Workspace volume was not found"
	case "INSTANCE_NOT_DESTROYED":
		msg = "Instance has not been destroyed; destroy the instance in App Dashboard before releasing its workspace volume"
	case "WORKSPACE_IN_USE":
		msg = "Workspace volume is currently mounted by an active workload"
	case "WORKSPACE_BINDING_CONFLICT":
		msg = "Workspace volume binding metadata conflicts with cluster state"
	}
	reply(w, status, ErrorResponse{errResp.Code, msg, requestID})
}
