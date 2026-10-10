package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

type BlenderControl struct {
	Token, Endpoint, RuntimeToken string
	Client                        *http.Client
}

func NewBlenderControl(token, endpoint, runtimeToken string) (*BlenderControl, error) {
	if token == "" && endpoint == "" && runtimeToken == "" {
		return nil, nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Path != "" || (u.Scheme != "http" && u.Scheme != "https") || len(token) < 32 || len(runtimeToken) < 32 {
		return nil, errors.New("invalid Blender control configuration")
	}
	return &BlenderControl{token, endpoint, runtimeToken, &http.Client{Timeout: 8 * time.Second, Transport: &http.Transport{Proxy: nil}, CheckRedirect: func(*http.Request, []*http.Request) error { return errors.New("redirect denied") }}}, nil
}

// This scalar request is intentionally separate from AppRuntime install actions.
type blenderCreate struct {
	OperationID      string `json:"operation_id"`
	StationID        string `json:"station_id"`
	OrganizationID   string `json:"organization_id"`
	UserID           string `json:"user_id"`
	ProjectID        string `json:"project_id"`
	InstanceID       string `json:"instance_id"`
	ProfileID        string `json:"profile_id"`
	SourceRevisionID string `json:"source_revision_id"`
	SourceSHA256     string `json:"source_sha256"`
	SourceSize       int64  `json:"source_size"`
	EmptySource      bool   `json:"empty_source"`
}

type blenderStart struct {
	OperationID       string `json:"operation_id"`
	CreateOperationID string `json:"create_operation_id"`
	StationID         string `json:"station_id"`
	OrganizationID    string `json:"organization_id"`
	UserID            string `json:"user_id"`
	ProjectID         string `json:"project_id"`
	InstanceID        string `json:"instance_id"`
	ProfileID         string `json:"profile_id"`
	SourceRevisionID  string `json:"source_revision_id"`
	SHA256            string `json:"sha256"`
	Size              int64  `json:"size"`
	Empty             bool   `json:"empty"`
}

type blenderAccess struct {
	StationID        string `json:"station_id"`
	OrganizationID   string `json:"organization_id"`
	UserID           string `json:"user_id"`
	ProjectID        string `json:"project_id"`
	InstanceID       string `json:"instance_id"`
	StartOperationID string `json:"start_operation_id"`
	Kind             string `json:"kind"`
}

type blenderStop struct {
	OperationID      string `json:"operation_id"`
	StartOperationID string `json:"start_operation_id"`
	StationID        string `json:"station_id"`
	OrganizationID   string `json:"organization_id"`
	UserID           string `json:"user_id"`
	ProjectID        string `json:"project_id"`
	InstanceID       string `json:"instance_id"`
	Generation       string `json:"generation"`
	SavedRevisionID  string `json:"saved_revision_id"`
	CommitID         string `json:"commit_id"`
	StoreID          string `json:"store_id"`
	ArtifactID       string `json:"artifact_id"`
	VersionID        string `json:"version_id"`
	AssetID          string `json:"asset_id"`
	SHA256           string `json:"sha256"`
	Size             int64  `json:"size"`
	SceneVersion     int64  `json:"scene_version"`
}

type blenderDestroy struct {
	OperationID         string `json:"operation_id"`
	PreviousOperationID string `json:"previous_operation_id"`
	StationID           string `json:"station_id"`
	OrganizationID      string `json:"organization_id"`
	UserID              string `json:"user_id"`
	ProjectID           string `json:"project_id"`
	InstanceID          string `json:"instance_id"`
}

func (s *Server) blenderDestroy(w http.ResponseWriter, r *http.Request, requestID string) {
	control, ok := s.blenderControlCaller(w, r, requestID)
	if !ok {
		return
	}
	var in blenderDestroy
	if err := decode(w, r, &in, false); err != nil {
		failure(w, requestID, err)
		return
	}
	s.blenderExecute(w, r, requestID, control, "destroy", in, in.UserID, in.OrganizationID, in.StationID)
}

func (s *Server) blenderStop(w http.ResponseWriter, r *http.Request, requestID string) {
	control, ok := s.blenderControlCaller(w, r, requestID)
	if !ok {
		return
	}
	var in blenderStop
	if err := decode(w, r, &in, false); err != nil {
		failure(w, requestID, err)
		return
	}
	s.blenderExecute(w, r, requestID, control, "stop", in, in.UserID, in.OrganizationID, in.StationID)
}

func (s *Server) blenderControlCaller(w http.ResponseWriter, r *http.Request, requestID string) (*BlenderControl, bool) {
	control := s.BlenderControl
	if control == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Blender execution is not configured", requestID})
		return nil, false
	}
	expected := sha256.Sum256([]byte("Bearer " + control.Token))
	actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
	if control.Token == "" || len(r.Header.Values("Authorization")) != 1 || len(r.Header.Values("X-User-Id")) != 1 || len(r.Header.Values("X-Organization-Id")) != 1 || subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
		failure(w, requestID, identity.Unauthenticated)
		return nil, false
	}
	if r.URL.RawQuery != "" || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" {
		failure(w, requestID, identity.Invalid)
		return nil, false
	}
	return control, true
}

func (s *Server) blenderCreate(w http.ResponseWriter, r *http.Request, requestID string) {
	control, ok := s.blenderControlCaller(w, r, requestID)
	if !ok {
		return
	}
	var in blenderCreate
	if err := decode(w, r, &in, false); err != nil {
		failure(w, requestID, err)
		return
	}
	s.blenderExecute(w, r, requestID, control, "create", in, in.UserID, in.OrganizationID, in.StationID)
}

func (s *Server) blenderStart(w http.ResponseWriter, r *http.Request, requestID string) {
	control, ok := s.blenderControlCaller(w, r, requestID)
	if !ok {
		return
	}
	var in blenderStart
	if err := decode(w, r, &in, false); err != nil {
		failure(w, requestID, err)
		return
	}
	s.blenderExecute(w, r, requestID, control, "start", in, in.UserID, in.OrganizationID, in.StationID)
}

func (s *Server) blenderExecute(w http.ResponseWriter, r *http.Request, requestID string, control *BlenderControl, action string, input any, user, org, station string) {
	if user != r.Header.Get("X-User-Id") || org != r.Header.Get("X-Organization-Id") || station != s.Identity.StationID {
		failure(w, requestID, identity.Denied)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 9*time.Second)
	defer cancel()
	var data []byte
	status := 0
	authorize := s.Identity.WithManager
	if action == "access" {
		authorize = s.Identity.WithMember
	}
	err := authorize(ctx, user, org, func(ctx context.Context) error {
		raw, _ := json.Marshal(input)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, control.Endpoint+"/internal/v1/instances/"+action, bytes.NewReader(raw))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+control.RuntimeToken)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Request-ID", requestID)
		response, err := control.Client.Do(req)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		data, err = io.ReadAll(io.LimitReader(response.Body, (32<<10)+1))
		if err != nil || len(data) > 32<<10 || response.Header.Get("Content-Encoding") != "" || !json.Valid(data) {
			return errors.New("invalid Runtime response")
		}
		status = response.StatusCode
		if status != 200 && status != 400 && status != 401 && status != 403 && status != 409 && status != 503 {
			return errors.New("invalid Runtime status")
		}
		return nil
	})
	if err != nil {
		failure(w, requestID, err)
		return
	}
	// Private service response can contain scoped worker credentials. This handler
	// is never authorized by a user bearer and logs no body, headers or tokens.
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(data)
}

func (s *Server) blenderAccess(w http.ResponseWriter, r *http.Request, requestID string) {
	control, ok := s.blenderControlCaller(w, r, requestID)
	if !ok {
		return
	}
	var in blenderAccess
	if err := decode(w, r, &in, false); err != nil {
		failure(w, requestID, err)
		return
	}
	s.blenderExecute(w, r, requestID, control, "access", in, in.UserID, in.OrganizationID, in.StationID)
}

func (s *Server) blenderOptions(w http.ResponseWriter, r *http.Request, requestID string) {
	control, ok := s.blenderControlCaller(w, r, requestID)
	if !ok {
		return
	}
	if err := decode(w, r, &struct{}{}, false); err != nil {
		failure(w, requestID, err)
		return
	}
	user, org := r.Header.Get("X-User-Id"), r.Header.Get("X-Organization-Id")
	ctx, cancel := context.WithTimeout(r.Context(), 9*time.Second)
	defer cancel()
	var profiles struct {
		Profiles []struct {
			ID      string `json:"profile_id"`
			Storage int64  `json:"storage_reserved_bytes"`
			MaxFile int64  `json:"max_file_bytes"`
		} `json:"profiles"`
	}
	err := s.Identity.WithManager(ctx, user, org, func(ctx context.Context) error {
		req, err := http.NewRequestWithContext(ctx, "GET", control.Endpoint+"/internal/v1/instances/profiles", nil)
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+control.RuntimeToken)
		resp, err := control.Client.Do(req)
		if err != nil {
			return err
		}
		defer resp.Body.Close()
		raw, err := io.ReadAll(io.LimitReader(resp.Body, (32<<10)+1))
		if err != nil || resp.StatusCode != 200 || len(raw) > 32<<10 || resp.Header.Get("Content-Encoding") != "" || json.Unmarshal(raw, &profiles) != nil || profiles.Profiles == nil {
			return errors.New("Runtime profiles unavailable")
		}
		return nil
	})
	if err != nil {
		failure(w, requestID, err)
		return
	}
	reply(w, 200, map[string]any{"station_id": s.Identity.StationID, "user_id": user, "organization_id": org, "can_manage": true, "profiles": profiles.Profiles})
}
