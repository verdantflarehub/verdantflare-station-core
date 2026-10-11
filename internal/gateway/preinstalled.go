package gateway

import (
	"context"
	"net/http"
	"slices"
	"strings"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
	"github.com/verdantflarehub/verdantflare-station-core/internal/strictjson"
)

func (s *Server) servePreinstalled(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, user identity.IdentityContext) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	if !slices.Contains(user.Roles, "admin") || !slices.Contains(user.Scopes, "app:read") {
		failure(w, requestID, identity.Denied)
		return
	}
	if s.Operations == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Preinstalled lifecycle is unavailable", requestID})
		return
	}
	if r.URL.Path == "/preinstalled/commands" {
		if !slices.Contains(user.Scopes, "app:manage") {
			failure(w, requestID, identity.Denied)
			return
		}
		var in operations.PreinstalledCommand
		if strictjson.Decode(r.Body, 16384, &in) != nil {
			failure(w, requestID, identity.Invalid)
			return
		}
		out, err := s.Operations.SubmitPreinstalled(ctx, user, in)
		if err != nil {
			failure(w, requestID, err)
			return
		}
		reply(w, 202, out)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/preinstalled/apps/") {
		out, err := s.Operations.PreinstalledInventory(ctx, user, strings.TrimPrefix(r.URL.Path, "/preinstalled/apps/"))
		if err != nil {
			failure(w, requestID, err)
			return
		}
		reply(w, 200, out)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/preinstalled/releases/") {
		out, err := s.Operations.Releases.Resolve(strings.TrimPrefix(r.URL.Path, "/preinstalled/releases/"))
		if err != nil {
			reply(w, 503, ErrorResponse{"TRUSTED_RELEASE_UNAVAILABLE", "Trusted release evidence is unavailable", requestID})
			return
		}
		reply(w, 200, out)
		return
	}
	reply(w, 404, ErrorResponse{"NOT_FOUND", "Endpoint not found", requestID})
}
