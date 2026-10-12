package gateway

import (
	"context"
	"errors"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"net/http"
	"net/url"
	"slices"
	"strings"
)

func (s *Server) serveCatalog(w http.ResponseWriter, r *http.Request, ctx context.Context, requestID string, user identity.IdentityContext) {
	w.Header().Set("Cache-Control", "no-store")
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
	if s.AppStates == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Application catalog is not configured", requestID})
		return
	}
	items, err := s.AppStates.RegisteredCatalog(ctx, user)
	if err != nil {
		if errors.Is(err, identity.Denied) {
			failure(w, requestID, err)
		} else {
			reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Application catalog is unavailable", requestID})
		}
		return
	}
	if r.URL.Path == "/catalog/apps" {
		items = slices.DeleteFunc(items, func(item catalog.Item) bool { return group != "" && item.GroupID != group })
		reply(w, 200, catalog.List{RequestID: requestID, StationID: user.StationID, Items: items})
		return
	}
	for _, app := range items {
		if app.AppID == strings.TrimPrefix(r.URL.Path, "/catalog/apps/") {
			reply(w, 200, catalog.Detail{RequestID: requestID, StationID: user.StationID, App: app})
			return
		}
	}
	reply(w, 404, ErrorResponse{"NOT_FOUND", "Application not found", requestID})
}
