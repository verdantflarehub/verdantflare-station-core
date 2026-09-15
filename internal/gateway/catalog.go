package gateway

import (
	"context"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

func (s *Server) serveCatalog(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, user identity.IdentityContext) {
	if !slices.Contains(user.Roles, "admin") || !slices.Contains(user.Scopes, "app:read") {
		failure(w, requestID, identity.Denied)
		return
	}
	q, err := url.ParseQuery(r.URL.RawQuery)
	if err != nil {
		failure(w, requestID, identity.Invalid)
		return
	}
	group := ""
	for key, values := range q {
		if r.URL.Path != "/catalog/apps" || key != "group_id" || len(values) != 1 || !catalog.ValidGroup(values[0]) {
			failure(w, requestID, identity.Invalid)
			return
		}
		group = values[0]
	}
	if s.Catalog == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Application catalog is not configured", requestID})
		return
	}
	if r.URL.Path == "/catalog/apps" {
		reply(w, 200, catalog.List{RequestID: requestID, StationID: user.StationID, Items: s.Catalog.List(ctx, group)})
		return
	}
	app, found := s.Catalog.Get(ctx, strings.TrimPrefix(r.URL.Path, "/catalog/apps/"))
	if !found {
		reply(w, 404, ErrorResponse{"NOT_FOUND", "Application not found", requestID})
		return
	}
	reply(w, 200, catalog.Detail{RequestID: requestID, StationID: user.StationID, App: app})
}
