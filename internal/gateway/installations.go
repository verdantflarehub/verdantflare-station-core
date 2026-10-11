package gateway

import (
	"context"
	"errors"
	"net/http"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func (s *Server) serveInstallations(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, user identity.IdentityContext) {
	w.Header().Set("Cache-Control", "no-store")
	if r.URL.RawQuery != "" || r.URL.ForceQuery || r.URL.RawPath != "" {
		failure(w, requestID, identity.Invalid)
		return
	}
	if s.Operations == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Installation records are unavailable", requestID})
		return
	}
	if r.URL.Path == "/app-installations" {
		items, err := s.Operations.ListInstallations(ctx, user)
		if err != nil {
			failure(w, requestID, err)
			return
		}
		reply(w, 200, map[string]any{"request_id": requestID, "station_id": user.StationID, "items": items})
		return
	}
	item, err := s.Operations.GetInstallation(ctx, user, strings.TrimPrefix(r.URL.Path, "/app-installations/"))
	if errors.Is(err, pgx.ErrNoRows) {
		reply(w, 404, ErrorResponse{"NOT_FOUND", "Installation not found", requestID})
		return
	}
	if err != nil {
		failure(w, requestID, err)
		return
	}
	reply(w, 200, map[string]any{"request_id": requestID, "installation": item})
}
