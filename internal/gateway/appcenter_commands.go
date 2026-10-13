package gateway

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
)

func (s *Server) serveAppCenterCommand(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, actor identity.IdentityContext) {
	w.Header().Set("Cache-Control", "no-store")
	if !slices.Contains(actor.Roles, "admin") || !slices.Contains(actor.Scopes, "app:read") || !slices.Contains(actor.Scopes, "app:manage") {
		failure(w, requestID, identity.Denied)
		return
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if r.Method != http.MethodPost || r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" || r.Body == nil || err != nil || media != "application/json" {
		failure(w, requestID, identity.Invalid)
		return
	}
	input, err := operations.DecodeAppCenterCommand(r.Body)
	if err != nil {
		failure(w, requestID, err)
		return
	}
	if s.Operations == nil || s.Operations.Pool == nil {
		reply(w, 503, ErrorResponse{"APPCENTER_DEPENDENCY_UNAVAILABLE", "Command service unavailable", requestID})
		return
	}
	// Disabled/withdrawn configuration blocks only new commands. Submit checks
	// authenticated durable replay before calling this production reader.
	op, err := s.Operations.SubmitAppCenter(ctx, actor, input, s.Operations.AppCenterAdmissionSource.Admit)
	if err != nil {
		switch {
		case errors.Is(err, operations.ErrAppCenterBinding):
			reply(w, 409, ErrorResponse{"APPCENTER_BINDING_CHANGED", "Command binding changed", requestID})
		case errors.Is(err, operations.ErrAppCenterRelease):
			reply(w, 409, ErrorResponse{"RELEASE_UNVERIFIED", "Trusted release unavailable", requestID})
		case errors.Is(err, operations.ErrAppCenterUnsupported):
			reply(w, 409, ErrorResponse{"ACTION_UNSUPPORTED", "Command is not enabled", requestID})
		case errors.Is(err, operations.ErrAppCenterDispatch):
			reply(w, 503, ErrorResponse{"APPCENTER_EXECUTION_BINDING_UNSUPPORTED", "Protected runtime unavailable", requestID})
		case errors.Is(err, appcenter.ErrUnavailable):
			reply(w, 503, ErrorResponse{"APPCENTER_DEPENDENCY_UNAVAILABLE", "Command observation unavailable", requestID})
		default:
			failure(w, requestID, err)
		}
		return
	}
	reply(w, http.StatusAccepted, op)
}

func (s *Server) serveAppCenterOperations(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, actor identity.IdentityContext) {
	w.Header().Set("Cache-Control", "no-store")
	if !slices.Contains(actor.Roles, "admin") || !slices.Contains(actor.Scopes, "app:read") {
		failure(w, requestID, identity.Denied)
		return
	}
	if r.Method != http.MethodGet || r.URL.RawPath != "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	key := r.URL.Query().Get("idempotency_key")
	if key == "" || len(key) < 8 || len(key) > 128 || len(r.URL.Query()) != 1 {
		failure(w, requestID, identity.Invalid)
		return
	}
	if s.Operations == nil || s.Operations.Pool == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Operation service unavailable", requestID})
		return
	}
	op, err := s.Operations.GetByIdempotencyKey(ctx, actor, key)
	if errors.Is(err, pgx.ErrNoRows) {
		reply(w, 404, ErrorResponse{"NOT_FOUND", "Operation not found", requestID})
		return
	}
	if err != nil {
		failure(w, requestID, err)
		return
	}
	reply(w, http.StatusOK, op)
}

