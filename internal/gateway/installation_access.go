package gateway

import (
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"os"
	"regexp"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
	"github.com/verdantflarehub/verdantflare-station-core/internal/strictjson"
)

// InstallationAccess authenticates a registered App, then checks live Core facts.
// It never exchanges the service credential for a user session or cached grant.
type InstallationAccess struct {
	callers      map[[32]byte]string
	manager      func(context.Context, string, string, func(context.Context) error) error
	installation func(context.Context, identity.IdentityContext, string) (operations.Installation, error)
	worker       func(context.Context, operations.Installation, string) error
}

func LoadInstallationAccess(path string, lookup func(string) string, service *identity.Service, ops *operations.Service) (*InstallationAccess, error) {
	if path == "" {
		return nil, nil
	}
	if service == nil || ops == nil || lookup == nil {
		return nil, errors.New("invalid installation access configuration")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.New("installation access registry unavailable")
	}
	defer f.Close()
	var entries []struct {
		AppID    string `json:"app_id"`
		TokenEnv string `json:"token_env"`
	}
	if strictjson.Decode(f, 64<<10, &entries) != nil || len(entries) == 0 || len(entries) > 128 {
		return nil, errors.New("invalid installation access registry")
	}
	out := &InstallationAccess{callers: map[[32]byte]string{}, manager: service.WithManager, installation: ops.GetInstallation, worker: ops.AuthorizeWorkerVersion}
	seen := map[string]bool{}
	for _, entry := range entries {
		if !regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`).MatchString(entry.AppID) || seen[entry.AppID] ||
			!regexp.MustCompile(`^STATION_APP_INSTALLATION_[A-Z0-9_]+_TOKEN$`).MatchString(entry.TokenEnv) {
			return nil, errors.New("invalid installation access binding")
		}
		token := lookup(entry.TokenEnv)
		if len(token) < 32 || len(token) > 512 || strings.IndexFunc(token, func(r rune) bool { return r <= 32 || r > 126 }) >= 0 {
			return nil, errors.New("invalid installation access credential")
		}
		hash := sha256.Sum256([]byte("Bearer " + token))
		if _, exists := out.callers[hash]; exists {
			return nil, errors.New("duplicate installation access credential")
		}
		out.callers[hash], seen[entry.AppID] = entry.AppID, true
	}
	return out, nil
}

func (s *Server) installationAccess(w http.ResponseWriter, r *http.Request, requestID string) {
	w.Header().Set("Cache-Control", "no-store")
	a := s.InstallationAccess
	if a == nil || a.manager == nil || a.installation == nil || s.Identity == nil {
		reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Installation delegation is not configured", requestID})
		return
	}
	app, exists := a.callers[sha256.Sum256([]byte(r.Header.Get("Authorization")))]
	if !exists || len(r.Header.Values("Authorization")) != 1 {
		failure(w, requestID, identity.Unauthenticated)
		return
	}
	for _, header := range []string{"X-User-Id", "X-Organization-Id", "X-Station-Id"} {
		if len(r.Header.Values(header)) != 1 || !canonicalSubject(r.Header.Get(header)) {
			failure(w, requestID, identity.Invalid)
			return
		}
	}
	if r.URL.RawPath != "" || r.URL.RawQuery != "" || r.URL.ForceQuery || len(r.Header.Values("Content-Type")) != 1 ||
		r.Header.Get("Content-Encoding") != "" || len(r.TransferEncoding) != 0 {
		failure(w, requestID, identity.Invalid)
		return
	}
	if r.Header.Get("X-Station-Id") != s.Identity.StationID {
		failure(w, requestID, identity.Denied)
		return
	}
	var in struct {
		InstallationID string `json:"installation_id"`
		AppVersion     string `json:"app_version"`
	}
	worker := r.URL.Path == "/internal/v1/app-installations/authorize-worker"
	var decodeErr error
	if worker {
		var request struct {
			InstallationID string `json:"installation_id"`
			WorkerVersion  string `json:"worker_version"`
		}
		decodeErr = decode(w, r, &request, false)
		in.InstallationID, in.AppVersion = request.InstallationID, request.WorkerVersion
	} else {
		decodeErr = decode(w, r, &in, false)
	}
	if decodeErr != nil {
		failure(w, requestID, identity.Invalid)
		return
	}
	id, err := uuid.Parse(in.InstallationID)
	if err != nil || id.Version() != 7 || id.String() != in.InstallationID ||
		!regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`).MatchString(in.AppVersion) {
		failure(w, requestID, identity.Invalid)
		return
	}
	user, org := r.Header.Get("X-User-Id"), r.Header.Get("X-Organization-Id")
	var item operations.Installation
	err = a.manager(r.Context(), user, org, func(ctx context.Context) error {
		actor := identity.IdentityContext{UserID: user, OrganizationID: org, StationID: s.Identity.StationID,
			Roles: []string{"admin"}, Scopes: []string{"app:read", "app:manage"}}
		var err error
		item, err = a.installation(ctx, actor, in.InstallationID)
		if errors.Is(err, pgx.ErrNoRows) {
			return identity.Denied
		}
		if err != nil {
			return err
		}
		if item.InstallationID != in.InstallationID || item.StationID != actor.StationID || item.OrganizationID != org ||
			item.AppID != app || item.State != "installed" || item.Revision < 1 {
			return identity.Denied
		}
		if worker {
			if a.worker == nil {
				return operations.ErrTransitionRelease
			}
			return a.worker(ctx, item, in.AppVersion)
		}
		if item.AppVersion != in.AppVersion {
			return identity.Denied
		}
		return nil
	})
	if err != nil {
		if errors.Is(err, operations.ErrTransitionRelease) {
			reply(w, 503, ErrorResponse{"SERVICE_UNAVAILABLE", "Worker compatibility is unavailable", requestID})
			return
		}
		failure(w, requestID, err)
		return
	}
	out := map[string]any{"schema_version": 1, "installation_id": item.InstallationID, "app_id": app,
		"app_version": item.AppVersion, "station_id": item.StationID, "organization_id": org, "user_id": user,
		"installation_revision": item.Revision, "request_id": requestID}
	if worker {
		out["worker_version"] = in.AppVersion
	}
	reply(w, 200, out)
}
