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

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/resources"
)

type RuntimeControl struct {
	Endpoint, RuntimeToken string
	Client                 *http.Client
}

func NewRuntimeControl(endpoint, runtimeToken string) (*RuntimeControl, error) {
	if endpoint == "" && runtimeToken == "" {
		return nil, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || (u.Scheme != "http" && u.Scheme != "https") || len(runtimeToken) < 32 || strings.ContainsAny(runtimeToken, " \t\r\n") {
		return nil, errors.New("invalid Runtime control configuration")
	}
	return &RuntimeControl{
		Endpoint:     endpoint,
		RuntimeToken: runtimeToken,
		Client: &http.Client{
			Timeout:       20 * time.Second,
			Transport:     &http.Transport{Proxy: nil},
			CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect denied") },
		},
	}, nil
}

func (s *Server) modelRuntimeControl() *RuntimeControl {
	if s.ModelCacheControl != nil {
		return s.ModelCacheControl
	}
	if s.BlenderControl != nil {
		return &RuntimeControl{
			Endpoint:     s.BlenderControl.Endpoint,
			RuntimeToken: s.BlenderControl.RuntimeToken,
			Client:       s.BlenderControl.Client,
		}
	}
	return nil
}

func matchModelEvictPath(path string) (prefix string, sha string, ok bool) {
	for _, p := range []string{"/internal/v1/resources/models/", "/api/v1/resources/models/"} {
		if strings.HasPrefix(path, p) && strings.HasSuffix(path, "/evict") {
			mid := strings.TrimSuffix(strings.TrimPrefix(path, p), "/evict")
			if mid != "" && !strings.Contains(mid, "/") {
				return p, mid, true
			}
		}
	}
	return "", "", false
}

type modelEvictInput struct {
	IdempotencyKey string `json:"idempotency_key"`
}

type runtimeModelEvictRequest struct {
	StationID      string `json:"station_id"`
	OrganizationID string `json:"organization_id"`
	UserID         string `json:"user_id"`
	SHA256         string `json:"sha256"`
	IdempotencyKey string `json:"idempotency_key"`
}

type modelEvictErrorResponse struct {
	Code         string `json:"code"`
	Message      string `json:"message"`
	ActivePins   int    `json:"active_pins,omitempty"`
	ReservedPins int    `json:"reserved_pins,omitempty"`
	RequestID    string `json:"request_id"`
}

func validIdempotencyKey(value string) bool {
	return len(value) >= 8 && len(value) <= 128 && !strings.ContainsAny(value, " \t\r\n")
}

func (s *Server) internalResourcesModels(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string) {
	user, org, ok := s.delegatedAppQuery(w, r, requestID, "resources", true)
	if !ok {
		return
	}
	filter, err := resources.ParseModelListFilter(r.URL.RawQuery)
	if err != nil {
		failure(w, requestID, identity.Invalid)
		return
	}
	s.resourcesModelsWithManager(w, ctx, requestID, user, org, filter, s.AppAccess.manager)
}

func (s *Server) resourcesModels(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, user identity.IdentityContext) {
	if (r.URL.ForceQuery && r.URL.RawQuery == "") || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	filter, err := resources.ParseModelListFilter(r.URL.RawQuery)
	if err != nil {
		failure(w, requestID, identity.Invalid)
		return
	}
	s.resourcesModelsWithManager(w, ctx, requestID, user.UserID, user.OrganizationID, filter, s.Identity.WithManager)
}

func (s *Server) resourcesModelsWithManager(
	w http.ResponseWriter,
	ctx context.Context,
	requestID, userID, orgID string,
	filter resources.ModelListFilter,
	withManager func(context.Context, string, string, func(context.Context) error) error,
) {
	if withManager == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Station authorization is not configured", requestID})
		return
	}
	var page resources.ModelListPage
	var storeMissing bool
	err := withManager(ctx, userID, orgID, func(ctx context.Context) error {
		store := s.ModelStore
		if store == nil && s.Identity != nil && s.Identity.Pool != nil {
			store = &resources.PGModelStore{Pool: s.Identity.Pool, StationID: s.Identity.StationID}
		}
		if store == nil {
			storeMissing = true
			return nil
		}
		var listErr error
		page, listErr = store.ListModels(ctx, filter)
		return listErr
	})
	if err != nil {
		failure(w, requestID, err)
		return
	}
	if storeMissing {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Model cache inventory is not configured", requestID})
		return
	}
	reply(w, 200, struct {
		resources.ModelListPage
		RequestID string `json:"request_id"`
	}{
		ModelListPage: page,
		RequestID:     requestID,
	})
}

func (s *Server) internalResourcesEvictModel(w http.ResponseWriter, r *http.Request, requestID, sha string) {
	user, org, ok := s.delegatedApp(w, r, requestID, "resources")
	if !ok {
		return
	}
	s.resourcesEvictModelWithManager(w, r, requestID, sha, user, org, s.AppAccess.manager)
}

func (s *Server) resourcesEvictModel(w http.ResponseWriter, r *http.Request, requestID, sha string, user identity.IdentityContext) {
	s.resourcesEvictModelWithManager(w, r, requestID, sha, user.UserID, user.OrganizationID, s.Identity.WithManager)
}

func (s *Server) resourcesEvictModelWithManager(
	w http.ResponseWriter,
	r *http.Request,
	requestID, sha, userID, orgID string,
	withManager func(context.Context, string, string, func(context.Context) error) error,
) {
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	if !resources.ValidSHA256(sha) {
		failure(w, requestID, identity.Invalid)
		return
	}
	var in modelEvictInput
	if err := decode(w, r, &in, true); err != nil {
		failure(w, requestID, err)
		return
	}
	if in.IdempotencyKey == "" {
		id, err := uuid.NewV7()
		if err != nil {
			reply(w, 500, ErrorResponse{"INTERNAL", "Request failed", requestID})
			return
		}
		in.IdempotencyKey = "evict-" + id.String()
	} else if !validIdempotencyKey(in.IdempotencyKey) {
		failure(w, requestID, identity.Invalid)
		return
	}
	if withManager == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Station authorization is not configured", requestID})
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), 20*time.Second)
	defer cancel()

	var status int
	var raw []byte
	var controlMissing bool
	var badUpstream bool

	err := withManager(ctx, userID, orgID, func(ctx context.Context) error {
		control := s.modelRuntimeControl()
		if control == nil || control.Client == nil {
			controlMissing = true
			return nil
		}
		client := *control.Client
		client.Timeout = 20 * time.Second
		body, _ := json.Marshal(runtimeModelEvictRequest{
			StationID:      s.Identity.StationID,
			OrganizationID: orgID,
			UserID:         userID,
			SHA256:         sha,
			IdempotencyKey: in.IdempotencyKey,
		})
		req, reqErr := http.NewRequestWithContext(
			ctx,
			http.MethodPost,
			control.Endpoint+"/internal/v1/model-cache/models/"+sha+"/evict",
			bytes.NewReader(body),
		)
		if reqErr != nil {
			badUpstream = true
			return nil
		}
		req.Header.Set("Authorization", "Bearer "+control.RuntimeToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Request-ID", requestID)
		resp, doErr := client.Do(req)
		if doErr != nil {
			badUpstream = true
			return nil
		}
		defer resp.Body.Close()
		var readErr error
		raw, readErr = io.ReadAll(io.LimitReader(resp.Body, (32<<10)+1))
		if readErr != nil || len(raw) > 32<<10 || resp.Header.Get("Content-Encoding") != "" || !json.Valid(raw) {
			badUpstream = true
			return nil
		}
		status = resp.StatusCode
		if status != 200 && status != 400 && status != 403 && status != 404 && status != 409 && status != 503 {
			badUpstream = true
			return nil
		}
		return nil
	})
	if err != nil {
		failure(w, requestID, err)
		return
	}
	if controlMissing {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Model cache provisioner is not configured", requestID})
		return
	}
	if badUpstream {
		reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Failed to execute model cache eviction on Runtime", requestID})
		return
	}

	if status == 200 {
		var okResp struct {
			Status string          `json:"status"`
			Model  json.RawMessage `json:"model"`
		}
		if json.Unmarshal(raw, &okResp) != nil || okResp.Status == "" || strings.Contains(string(raw), "/data/models") {
			reply(w, 502, ErrorResponse{"BAD_GATEWAY", "Invalid model cache eviction payload from Runtime", requestID})
			return
		}
		out := map[string]any{
			"status":     okResp.Status,
			"sha256":     sha,
			"request_id": requestID,
		}
		if len(okResp.Model) > 0 {
			out["model"] = okResp.Model
		}
		reply(w, 200, out)
		return
	}

	var errResp struct {
		Code         string `json:"code"`
		Message      string `json:"message"`
		ActivePins   int    `json:"active_pins"`
		ReservedPins int    `json:"reserved_pins"`
	}
	_ = json.Unmarshal(raw, &errResp)
	if errResp.Code == "" {
		errResp.Code = "SERVICE_UNAVAILABLE"
	}
	msg := resources.SanitizeErrorMessage(errResp.Message)
	if msg == "" {
		switch errResp.Code {
		case "MODEL_IN_USE":
			msg = "Model cache blob is currently pinned by an active task or published workflow API"
		case "MODEL_NOT_FOUND":
			msg = "Model cache blob was not found"
		case "MODEL_NOT_EVICTABLE":
			msg = "Model cache blob is not in ready state for eviction"
		case "INVALID_ARGUMENT":
			msg = "Model cache eviction arguments are invalid"
		default:
			msg = "Model cache eviction failed"
		}
	}
	reply(w, status, modelEvictErrorResponse{
		Code:         errResp.Code,
		Message:      msg,
		ActivePins:   errResp.ActivePins,
		ReservedPins: errResp.ReservedPins,
		RequestID:    requestID,
	})
}
