package gateway

import (
	"context"
	"errors"
	"github.com/verdantflarehub/verdantflare-station-core/internal/egress"
	"net/http"
	"strings"
	"time"
)

func (s *Server) serveEgress(w http.ResponseWriter, r *http.Request, requestID string) {
	ctx, cancel := context.WithTimeout(r.Context(), 55*time.Second)
	defer cancel()
	user, e := s.Identity.Me(ctx, requestID, bearer(r))
	if e != nil {
		failure(w, requestID, e)
		return
	}
	if s.Egress == nil {
		reply(w, 503, ErrorResponse{"EGRESS_NOT_CONFIGURED", "Proxy management is not configured", requestID})
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/proxies"), "/")
	id := ""
	test := false
	if len(parts) == 1 && parts[0] == "" {
	} else if len(parts) == 2 && parts[0] == "" && parts[1] != "" {
		id = parts[1]
	} else if len(parts) == 3 && parts[0] == "" && parts[1] != "" && parts[2] == "test" {
		id = parts[1]
		test = true
	} else {
		reply(w, 404, ErrorResponse{"NOT_FOUND", "Endpoint not found", requestID})
		return
	}
	methods := "GET, POST"
	if id != "" {
		methods = "GET, PUT, DELETE"
	}
	if test {
		methods = "POST"
	}
	if !strings.Contains(", "+methods+", ", ", "+r.Method+", ") {
		w.Header().Set("Allow", methods)
		reply(w, 405, ErrorResponse{"INVALID_ARGUMENT", "Method not allowed", requestID})
		return
	}
	fail := func(e error) {
		switch {
		case errors.Is(e, egress.ErrNotFound):
			reply(w, 404, ErrorResponse{"NOT_FOUND", "Proxy not found", requestID})
		case errors.Is(e, egress.ErrConflict):
			reply(w, 409, ErrorResponse{"PROXY_CONFLICT", "Proxy changed, duplicate endpoint, or existing references", requestID})
		case errors.Is(e, egress.ErrBusy):
			reply(w, 409, ErrorResponse{"PROBE_BUSY", "Probe already running or capacity reached", requestID})
		case errors.Is(e, egress.ErrDisabled):
			reply(w, 409, ErrorResponse{"PROXY_DISABLED_OR_EXPIRED", "Enable a valid proxy before testing", requestID})
		default:
			failure(w, requestID, e)
		}
	}
	if test {
		if e = decode(w, r, &struct{}{}, true); e != nil {
			fail(e)
			return
		}
		p, e := s.Egress.Test(ctx, user, id)
		if e != nil {
			fail(e)
			return
		}
		reply(w, 200, map[string]any{"item": p})
		return
	}
	switch r.Method {
	case "GET":
		if id == "" {
			items, e := s.Egress.List(ctx, user)
			if e != nil {
				fail(e)
				return
			}
			reply(w, 200, map[string]any{"items": items})
		} else {
			item, e := s.Egress.Get(ctx, user, id)
			if e != nil {
				fail(e)
				return
			}
			reply(w, 200, map[string]any{"item": item})
		}
	case "POST", "PUT":
		var in egress.Input
		if e = decode(w, r, &in, false); e != nil {
			fail(e)
			return
		}
		item, e := s.Egress.Save(ctx, user, id, in)
		if e != nil {
			fail(e)
			return
		}
		status := 200
		if id == "" {
			status = 201
		}
		reply(w, status, map[string]any{"item": item})
	case "DELETE":
		if e = s.Egress.Delete(ctx, user, id); e != nil {
			fail(e)
			return
		}
		w.WriteHeader(204)
	}
}
