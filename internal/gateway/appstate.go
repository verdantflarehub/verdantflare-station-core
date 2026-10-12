package gateway

import (
	"context"
	"errors"
	"io"
	"net/http"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func (s *Server) serveAppStates(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, user identity.IdentityContext) {
	w.Header().Set("Cache-Control", "no-store")
	if !slices.Contains(user.Roles, "admin") || !slices.Contains(user.Scopes, "app:read") {
		failure(w, requestID, identity.Denied)
		return
	}
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || r.ContentLength > 0 || len(r.TransferEncoding) > 0 {
		failure(w, requestID, identity.Invalid)
		return
	}
	if r.Body != nil {
		body, err := io.ReadAll(io.LimitReader(r.Body, 1))
		if err != nil || len(body) != 0 {
			failure(w, requestID, identity.Invalid)
			return
		}
	}
	id := ""
	if r.URL.Path != "/app-states/v1" {
		id = strings.TrimPrefix(r.URL.Path, "/app-states/v1/installations/")
		parsed, err := uuid.Parse(id)
		if err != nil || parsed == uuid.Nil || parsed.String() != id {
			failure(w, requestID, identity.Invalid)
			return
		}
	}
	if s.AppStates == nil {
		reply(w, 503, ErrorResponse{"APP_STATE_UNAVAILABLE", "Application state unavailable", requestID})
		return
	}
	snapshot, err := s.AppStates.Snapshot(ctx, user, id)
	if err != nil {
		switch {
		case errors.Is(err, appstate.ErrLimit):
			reply(w, 503, ErrorResponse{"APP_STATE_LIMIT_EXCEEDED", "Application state limit exceeded", requestID})
		case errors.Is(err, appstate.ErrNotFound):
			reply(w, 404, ErrorResponse{"NOT_FOUND", "Installation not found", requestID})
		case errors.Is(err, identity.Denied), errors.Is(err, identity.Invalid):
			failure(w, requestID, err)
		default:
			reply(w, 503, ErrorResponse{"APP_STATE_UNAVAILABLE", "Application state unavailable", requestID})
		}
		return
	}
	reply(w, 200, snapshot)
}
