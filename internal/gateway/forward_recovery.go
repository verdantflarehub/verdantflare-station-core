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

const forwardRecoveryPath = "/app-installation-forward-recoveries"

func (s *Server) serveForwardRecovery(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, a identity.IdentityContext) {
	if !slices.Contains(a.Roles, "admin") || !slices.Contains(a.Scopes, "app:read") || !slices.Contains(a.Scopes, "app:manage") {
		failure(w, requestID, identity.Denied)
		return
	}
	if r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery || len(r.Header.Values("Authorization")) != 1 || len(r.Header.Values("X-Request-ID")) > 1 || len(r.TransferEncoding) != 0 || r.Header.Get("Transfer-Encoding") != "" || r.Header.Get("Content-Encoding") != "" || r.ContentLength > 16<<10 {
		failure(w, requestID, identity.Invalid)
		return
	}
	var c operations.ForwardRecoveryCommand
	if r.Method == http.MethodPost {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if r.URL.Path != forwardRecoveryPath || r.Body == nil || err != nil || media != "application/json" || len(r.Header.Values("Content-Type")) != 1 || decode(w, r, &c, false) != nil {
			failure(w, requestID, identity.Invalid)
			return
		}
	} else if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, forwardRecoveryPath+"/") || r.ContentLength != 0 {
		failure(w, requestID, identity.Invalid)
		return
	}
	if s.Operations == nil || s.Operations.Pool == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Recovery service unavailable", requestID})
		return
	}
	var out operations.ForwardRecovery
	var err error
	if r.Method == http.MethodPost {
		out, err = s.Operations.SubmitForwardRecovery(ctx, a, c)
	} else {
		out, err = s.Operations.GetForwardRecovery(ctx, a, strings.TrimPrefix(r.URL.Path, forwardRecoveryPath+"/"))
	}
	if err != nil {
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			reply(w, 404, ErrorResponse{"NOT_FOUND", "Recovery or transition not found", requestID})
		case errors.Is(err, operations.ErrTransitionBusy):
			reply(w, 409, ErrorResponse{"TRANSITION_BUSY", "Original transition is being reconciled", requestID})
		case errors.Is(err, operations.ErrTransitionRelease), errors.Is(err, operations.ErrTransitionProgress):
			reply(w, 503, ErrorResponse{"RECOVERY_PROOF_UNAVAILABLE", "Verified recovery evidence unavailable; retry the original request", requestID})
		default:
			failure(w, requestID, err)
		}
		return
	}
	status := http.StatusOK
	if out.State == "pending" {
		status = http.StatusAccepted
	}
	reply(w, status, struct {
		RequestID string                     `json:"request_id"`
		Recovery  operations.ForwardRecovery `json:"recovery"`
	}{requestID, out})
}
