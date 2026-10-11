package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"slices"

	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func (s *Server) serveAppCenter(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, user identity.IdentityContext) {
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
	if s.Operations == nil {
		reply(w, 503, ErrorResponse{"APPCENTER_DEPENDENCY_UNAVAILABLE", "Application facts are unavailable", requestID})
		return
	}
	facts, err := s.Operations.AppCenterFacts(ctx, user, requestID)
	if err != nil {
		if errors.Is(err, identity.Denied) {
			failure(w, requestID, err)
			return
		}
		code := "APPCENTER_DEPENDENCY_UNAVAILABLE"
		if errors.Is(err, appcenter.ErrLimit) {
			code = "APPCENTER_LIMIT_EXCEEDED"
		}
		reply(w, 503, ErrorResponse{code, "Application facts are unavailable", requestID})
		return
	}
	raw, err := json.Marshal(facts)
	if err != nil || len(raw) > 4<<20 {
		reply(w, 503, ErrorResponse{"APPCENTER_LIMIT_EXCEEDED", "Application facts exceed the response limit", requestID})
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(raw)
}
