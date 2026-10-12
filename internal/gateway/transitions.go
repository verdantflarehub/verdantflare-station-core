package gateway

import (
	"context"
	"errors"
	"mime"
	"net/http"
	"slices"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
)

const transitionPath = "/app-installation-transitions"

type transitionResponse struct {
	RequestID  string                `json:"request_id"`
	Transition operations.Transition `json:"transition"`
}

func (s *Server) serveTransitions(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, actor identity.IdentityContext) {
	if !slices.Contains(actor.Roles, "admin") || !slices.Contains(actor.Scopes, "app:read") ||
		(r.Method == http.MethodPost && !slices.Contains(actor.Scopes, "app:manage")) {
		failure(w, requestID, identity.Denied)
		return
	}
	if r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery || len(r.Header.Values("Authorization")) != 1 ||
		len(r.Header.Values("X-Request-ID")) > 1 || len(r.TransferEncoding) != 0 || r.Header.Get("Transfer-Encoding") != "" ||
		r.Header.Get("Content-Encoding") != "" || r.ContentLength > 16<<10 {
		failure(w, requestID, identity.Invalid)
		return
	}
	var command operations.TransitionCommand
	if r.Method == http.MethodPost {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if r.URL.Path != transitionPath || r.Body == nil || err != nil || media != "application/json" ||
			len(r.Header.Values("Content-Type")) != 1 || decode(w, r, &command, false) != nil {
			failure(w, requestID, identity.Invalid)
			return
		}
	} else if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, transitionPath+"/") || r.ContentLength != 0 {
		failure(w, requestID, identity.Invalid)
		return
	}
	if s.Operations == nil || s.Operations.Pool == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Transition service unavailable", requestID})
		return
	}
	var out operations.Transition
	var err error
	status := http.StatusOK
	if r.Method == http.MethodPost {
		out, err = s.Operations.SubmitManagedTransition(ctx, actor, command)
		status = http.StatusAccepted
	} else {
		out, err = s.Operations.GetTransition(ctx, actor, strings.TrimPrefix(r.URL.Path, transitionPath+"/"))
	}
	if err != nil {
		switch {
		case errors.Is(err, operations.ErrTransitionAdmissionDisabled):
			reply(w, 409, ErrorResponse{"TRANSITION_ADMISSION_DISABLED", "New transitions are disabled", requestID})
		case errors.Is(err, operations.ErrTransitionRelease):
			reply(w, 503, ErrorResponse{"TRANSITION_RELEASE_UNAVAILABLE", "Verified release unavailable", requestID})
		case errors.Is(err, pgx.ErrNoRows):
			reply(w, 404, ErrorResponse{"NOT_FOUND", "Transition or installation not found", requestID})
		default:
			failure(w, requestID, err)
		}
		return
	}
	out.CreatedAt, out.UpdatedAt = out.CreatedAt.UTC(), out.UpdatedAt.UTC()
	reply(w, status, transitionResponse{requestID, out})
}
