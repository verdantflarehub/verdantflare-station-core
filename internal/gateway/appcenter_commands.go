package gateway

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"slices"

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
