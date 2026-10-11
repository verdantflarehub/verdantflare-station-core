package gateway

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

// AppAccess is host configuration, never client-controlled identity or policy.
type AppAccess struct {
	networkToken, resourcesToken string
	manager                      func(context.Context, string, string, func(context.Context) error) error
}

func NewAppAccess(networkToken, resourcesToken string, service *identity.Service) (*AppAccess, error) {
	if networkToken == "" && resourcesToken == "" {
		return nil, nil
	}
	for _, token := range []string{networkToken, resourcesToken} {
		if token != "" && (len(token) < 32 || strings.ContainsAny(token, " \t\r\n")) {
			return nil, errors.New("invalid application delegation configuration")
		}
	}
	if service == nil || (networkToken != "" && networkToken == resourcesToken) {
		return nil, errors.New("invalid application delegation configuration")
	}
	return &AppAccess{networkToken: networkToken, resourcesToken: resourcesToken, manager: service.WithManager}, nil
}

func canonicalSubject(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

func (s *Server) delegatedApp(w http.ResponseWriter, r *http.Request, requestID, app string) (string, string, bool) {
	if s.AppAccess == nil || s.Identity == nil || s.AppAccess.manager == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Application delegation is not configured", requestID})
		return "", "", false
	}
	token := s.AppAccess.networkToken
	if app == "resources" {
		token = s.AppAccess.resourcesToken
	}
	actual := sha256.Sum256([]byte(r.Header.Get("Authorization")))
	expected := sha256.Sum256([]byte("Bearer " + token))
	if token == "" || len(r.Header.Values("Authorization")) != 1 || subtle.ConstantTimeCompare(actual[:], expected[:]) != 1 {
		failure(w, requestID, identity.Unauthenticated)
		return "", "", false
	}
	for _, header := range []string{"X-User-Id", "X-Organization-Id", "X-Station-Id"} {
		if len(r.Header.Values(header)) != 1 || !canonicalSubject(r.Header.Get(header)) {
			failure(w, requestID, identity.Invalid)
			return "", "", false
		}
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || r.Header.Get("Content-Encoding") != "" {
		failure(w, requestID, identity.Invalid)
		return "", "", false
	}
	if r.Header.Get("X-Station-Id") != s.Identity.StationID {
		failure(w, requestID, identity.Denied)
		return "", "", false
	}
	return r.Header.Get("X-User-Id"), r.Header.Get("X-Organization-Id"), true
}

func (s *Server) appAccess(w http.ResponseWriter, r *http.Request, requestID string) {
	user, org, ok := s.delegatedApp(w, r, requestID, "network")
	if !ok {
		return
	}
	var in struct {
		AppID string `json:"app_id"`
	}
	if err := decode(w, r, &in, false); err != nil || in.AppID != "network" {
		failure(w, requestID, identity.Invalid)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	if err := s.AppAccess.manager(ctx, user, org, func(context.Context) error { return nil }); err != nil {
		failure(w, requestID, err)
		return
	}
	reply(w, 200, map[string]any{"schema_version": 1, "app_id": "network", "user_id": user,
		"organization_id": org, "station_id": s.Identity.StationID, "roles": []string{"admin"},
		"scopes": []string{"app:read", "app:manage"}, "request_id": requestID})
}
