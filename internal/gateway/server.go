package gateway

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"reflect"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

const Version = "0.1.0"

var requestIDPattern = regexp.MustCompile(`^[a-zA-Z0-9._:-]{1,128}$`)

type ErrorResponse struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}
type Probe struct {
	Status           string `json:"status"`
	Version          string `json:"version"`
	ContractsMajor   int    `json:"contracts_major"`
	MigrationVersion int    `json:"migration_version"`
	RequestID        string `json:"request_id"`
}
type StationHealth struct {
	StationID  string            `json:"station_id"`
	Version    string            `json:"version"`
	Status     string            `json:"status"`
	Components map[string]string `json:"components"`
	RequestID  string            `json:"request_id"`
}
type Server struct {
	Identity       *identity.Service
	BootstrapToken string
	Logger         *slog.Logger
	Catalog        *catalog.Service
}
type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *responseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(p)
}
func reply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if value != nil {
		_ = json.NewEncoder(w).Encode(value)
	}
}
func failure(w http.ResponseWriter, requestID string, err error) {
	var fault identity.Fault
	code := "SERVICE_UNAVAILABLE"
	message := "Station database is unavailable"
	status := 503
	if errors.As(err, &fault) {
		code = string(fault)
		switch fault {
		case identity.Invalid:
			status = 400
			message = "Request fields are invalid"
		case identity.Unauthenticated:
			status = 401
			message = "Valid credentials are required"
		case identity.Revoked:
			status = 401
			message = "Session has been revoked"
		case identity.Denied:
			status = 403
			message = "Access is not permitted"
		case identity.Locked:
			status = 423
			message = "Account is temporarily locked"
		case identity.Conflict:
			status = 409
			message = "Station has already been initialized"
		}
	}
	if status == 401 {
		w.Header().Set("WWW-Authenticate", `Bearer realm="station"`)
	}
	reply(w, status, ErrorResponse{code, message, requestID})
}

// Decode uses a bounded body and rejects duplicate keys before unmarshalling.
// All current identity request fields are scalars; nested objects are rejected
// by the typed second pass. Credentials never appear in decode errors or logs.
func decode(w http.ResponseWriter, r *http.Request, target any, allowEmpty bool) error {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 16<<10))
	if err != nil {
		return identity.Invalid
	}
	if len(bytes.TrimSpace(data)) == 0 {
		if allowEmpty {
			return nil
		}
		return identity.Invalid
	}
	media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || media != "application/json" {
		return identity.Invalid
	}
	d := json.NewDecoder(bytes.NewReader(data))
	t, err := d.Token()
	if err != nil || t != json.Delim('{') {
		return identity.Invalid
	}
	allowed := map[string]bool{}
	fields := reflect.TypeOf(target).Elem()
	for i := 0; i < fields.NumField(); i++ {
		allowed[strings.Split(fields.Field(i).Tag.Get("json"), ",")[0]] = true
	}
	seen := map[string]bool{}
	for d.More() {
		key, e := d.Token()
		if e != nil {
			return identity.Invalid
		}
		name, ok := key.(string)
		if !ok || seen[name] || !allowed[name] {
			return identity.Invalid
		}
		seen[name] = true
		var raw json.RawMessage
		if d.Decode(&raw) != nil || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return identity.Invalid
		}
	}
	if _, err = d.Token(); err != nil {
		return identity.Invalid
	}
	if _, err = d.Token(); !errors.Is(err, io.EOF) {
		return identity.Invalid
	}
	typed := json.NewDecoder(bytes.NewReader(data))
	typed.DisallowUnknownFields()
	if typed.Decode(target) != nil {
		return identity.Invalid
	}
	return nil
}
func bearer(r *http.Request) string {
	parts := strings.Fields(r.Header.Get("Authorization"))
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return ""
	}
	return parts[1]
}
func (s *Server) ServeHTTP(original http.ResponseWriter, r *http.Request) {
	w := &responseWriter{ResponseWriter: original}
	started := time.Now()
	requestID := r.Header.Get("X-Request-ID")
	valid := requestID == "" || requestIDPattern.MatchString(requestID)
	if requestID == "" || !valid {
		id, err := uuid.NewV7()
		if err != nil {
			http.Error(w, "Request failed", 500)
			return
		}
		requestID = id.String()
	}
	w.Header().Set("X-Request-ID", requestID)
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	route := "unknown"
	sessionID := ""
	defer func() {
		if recover() != nil {
			reply(w, 500, ErrorResponse{"INTERNAL", "Request failed", requestID})
		}
		s.Logger.Info("http_request", "request_id", requestID, "session_id", sessionID, "task_id", "", "attempt_id", "", "route", route, "status", w.status, "duration_ms", time.Since(started).Milliseconds())
	}()
	if !valid {
		failure(w, requestID, identity.Invalid)
		return
	}
	routes := map[string]string{"/healthz": "GET", "/readyz": "GET", "/identity/bootstrap": "POST", "/identity/login": "POST", "/identity/refresh": "POST", "/identity/logout": "POST", "/identity/me": "GET", "/identity/scopes": "GET", "/station/health": "GET"}
	if r.URL.Path == "/catalog/apps" || strings.HasPrefix(r.URL.Path, "/catalog/apps/") {
		routes[r.URL.Path] = "GET"
	}
	method, ok := routes[r.URL.Path]
	if !ok {
		reply(w, 404, ErrorResponse{"NOT_FOUND", "Endpoint not found", requestID})
		return
	}
	route = r.URL.Path
	if r.Method != method {
		w.Header().Set("Allow", method)
		reply(w, 405, ErrorResponse{"INVALID_ARGUMENT", "Method not allowed", requestID})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()
	switch route {
	case "/healthz":
		reply(w, 200, Probe{"Alive", Version, migrations.ContractsMajor, migrations.Version, requestID})
		return
	case "/readyz":
		readyCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		if migrations.Check(readyCtx, s.Identity.Pool, s.Identity.StationID) != nil {
			reply(w, 503, Probe{"NotReady", Version, migrations.ContractsMajor, migrations.Version, requestID})
			return
		}
		reply(w, 200, Probe{"Ready", Version, migrations.ContractsMajor, migrations.Version, requestID})
		return
	case "/identity/bootstrap":
		expected := sha256.Sum256([]byte(s.BootstrapToken))
		actual := sha256.Sum256([]byte(r.Header.Get("X-Bootstrap-Token")))
		if s.BootstrapToken == "" || subtle.ConstantTimeCompare(expected[:], actual[:]) != 1 {
			failure(w, requestID, identity.Denied)
			return
		}
		var input identity.BootstrapRequest
		if err := decode(w, r, &input, false); err != nil {
			failure(w, requestID, err)
			return
		}
		session, err := s.Identity.Bootstrap(ctx, requestID, input)
		if err != nil {
			failure(w, requestID, err)
			return
		}
		sessionID = session.SessionID
		reply(w, 201, session)
		return
	case "/identity/login":
		var input identity.LoginRequest
		if err := decode(w, r, &input, false); err != nil {
			failure(w, requestID, err)
			return
		}
		session, err := s.Identity.Login(ctx, requestID, input)
		if err != nil {
			failure(w, requestID, err)
			return
		}
		sessionID = session.SessionID
		reply(w, 201, session)
		return
	case "/identity/refresh":
		if err := decode(w, r, &struct{}{}, true); err != nil {
			failure(w, requestID, err)
			return
		}
		session, err := s.Identity.Refresh(ctx, requestID, bearer(r))
		if err != nil {
			failure(w, requestID, err)
			return
		}
		sessionID = session.SessionID
		reply(w, 200, session)
		return
	case "/identity/logout":
		if err := decode(w, r, &struct{}{}, true); err != nil {
			failure(w, requestID, err)
			return
		}
		if err := s.Identity.Logout(ctx, requestID, bearer(r)); err != nil {
			failure(w, requestID, err)
			return
		}
		w.WriteHeader(204)
		return
	}
	user, err := s.Identity.Me(ctx, requestID, bearer(r))
	if err != nil {
		failure(w, requestID, err)
		return
	}
	sessionID = user.SessionID
	if route == "/catalog/apps" || strings.HasPrefix(route, "/catalog/apps/") {
		s.serveCatalog(w, r, ctx, requestID, user)
		return
	}
	if route == "/station/health" {
		core := "Ready"
		status := "Degraded"
		if migrations.Check(ctx, s.Identity.Pool, s.Identity.StationID) != nil {
			core = "NotReady"
			status = "Unavailable"
		}
		reply(w, 200, StationHealth{s.Identity.StationID, Version, status, map[string]string{"core": core, "mcp": "NotReady", "runtime": "NotReady", "artifact": "NotReady"}, requestID})
		return
	}
	reply(w, 200, user)
}
