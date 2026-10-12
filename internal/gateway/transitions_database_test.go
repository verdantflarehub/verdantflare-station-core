package gateway

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

// Synthetic release bytes exercise the production material reader. They are
// not evidence of an actual image upgrade or publisher verification.
func gatewayTransitionMaterials(t *testing.T) *operations.TransitionReleases {
	t.Helper()
	root := t.TempDir()
	digest := func(raw []byte) string { h := sha256.Sum256(raw); return "sha256:" + hex.EncodeToString(h[:]) }
	encode := func(v any) []byte {
		raw, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	write := func(ref string, raw []byte) {
		p := filepath.Join(root, filepath.FromSlash(ref))
		if os.MkdirAll(filepath.Dir(p), 0700) != nil || os.WriteFile(p, raw, 0600) != nil {
			t.Fatal("fixture material write failed")
		}
	}
	entries := []any{}
	for _, version := range []string{"1.0.0", "1.0.1"} {
		snapshot := operations.TransitionSnapshot{AppID: "test-app", AppVersion: version, ReleaseRef: "test-app-v" + version, TemplateDigest: "sha256:" + strings.Repeat("a", 64), DataContract: "test-management-v1", DefaultWorkerVersion: "1.0.0", CompatibleWorkerVersions: []string{"1.0.0"}}
		asset := []byte("<!doctype html><title>Test independent Dashboard</title>")
		index := encode(map[string]any{"schemaVersion": 1, "files": map[string]string{"index.html": strings.TrimPrefix(digest(asset), "sha256:")}})
		snapshot.DashboardDigest = digest(index)
		manifest := encode(map[string]string{"app_id": snapshot.AppID, "version": version})
		snapshot.ManifestDigest = digest(manifest)
		materials := map[string][]byte{
			"VERSION":                              []byte(version + "\n"),
			"workspace-index.json":                 []byte(`{"schemaVersion":1,"files":{}}`),
			"management-release.json":              encode(map[string]any{"schema_version": 1, "app_id": snapshot.AppID, "app_version": version, "data_contract": snapshot.DataContract, "default_worker_version": "1.0.0", "compatible_worker_versions": []string{"1.0.0"}}),
			"dashboard/dashboard-declaration.json": encode(map[string]any{"schemaVersion": 1, "appId": snapshot.AppID, "appVersion": version, "dashboard": map[string]string{"artifactDigest": snapshot.DashboardDigest, "adapter": "wujie"}}),
			"dashboard/asset-index.json":           index, "dashboard/index.html": asset,
		}
		files := map[string]string{}
		for name, raw := range materials {
			write(snapshot.ReleaseRef+"/"+name, raw)
			files[name] = strings.TrimPrefix(digest(raw), "sha256:")
		}
		release := encode(map[string]any{"schemaVersion": 1, "appId": snapshot.AppID, "appVersion": version, "sourceCommit": strings.Repeat("b", 40), "actionsRunId": "123", "actionsRunAttempt": "1", "appImage": "registry.example/app:test-app-v" + version, "appImageDigest": "sha256:" + strings.Repeat("c", 64), "files": files})
		snapshot.ReleaseDigest = digest(release)
		write(snapshot.ReleaseRef+"/manifest.json", manifest)
		write(snapshot.ReleaseRef+"/release.json", release)
		entries = append(entries, map[string]any{"snapshot": snapshot, "manifest_ref": snapshot.ReleaseRef + "/manifest.json", "release_index_ref": snapshot.ReleaseRef + "/release.json"})
	}
	write("registry.json", encode(map[string]any{"schema_version": 1, "releases": entries}))
	return &operations.TransitionReleases{Path: filepath.Join(root, "registry.json")}
}

func TestTransitionGatewayHTTPAcceptanceReplayAndScope(t *testing.T) {
	ctx := t.Context()
	pool := testdb.New(t)
	station := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, pool, station); err != nil {
		t.Fatal(err)
	}
	auth, err := identity.New(pool, station, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := auth.Bootstrap(ctx, "bootstrap", identity.BootstrapRequest{Username: "transition-admin", Password: secret(), OrganizationName: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	installation := uuid.Must(uuid.NewV7()).String()
	if _, err = pool.Exec(ctx, `INSERT INTO station.app_installations(installation_id,created_by,organization_id,station_id,app_id,app_version,state,revision) VALUES($1,$2,$3,$4,'test-app','1.0.0','installed',1)`, installation, session.UserID, session.OrganizationID, station); err != nil {
		t.Fatal(err)
	}
	var original string
	if err = pool.QueryRow(ctx, `SELECT row_to_json(i)::text FROM station.app_installations i WHERE installation_id=$1`, installation).Scan(&original); err != nil {
		t.Fatal(err)
	}
	registry := gatewayTransitionMaterials(t)
	ops := &operations.Service{Pool: pool, StationID: station, TransitionReleases: registry}
	runtime, err := operations.NewHTTPTransitionRuntime("http://127.0.0.1:1", strings.Repeat("test-only-", 5))
	if err != nil {
		t.Fatal(err)
	}
	server := &Server{Identity: auth, Operations: ops, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	listener := httptest.NewServer(server)
	defer func() { listener.Close() }()
	client := &http.Client{Timeout: 5 * time.Second}
	command := operations.TransitionCommand{RequestID: "original-trace", IdempotencyKey: "fixed-transition-key", InstallationID: installation, ExpectedRevision: 1, SourceVersion: "1.0.0", TargetReleaseRef: "test-app-v1.0.1", Action: "upgrade"}
	call := func(method, path, token string, input any, want int) transitionResponse {
		t.Helper()
		var body io.Reader
		if input != nil {
			raw, _ := json.Marshal(input)
			body = bytes.NewReader(raw)
		}
		req, err := http.NewRequestWithContext(ctx, method, listener.URL+path, body)
		if err != nil {
			t.Fatal(err)
		}
		req.Header.Set("X-Request-ID", "http-trace")
		if input != nil {
			req.Header.Set("Content-Type", "application/json")
		}
		if token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
		response, err := client.Do(req)
		if err != nil {
			t.Fatal("HTTP request failed")
		}
		defer response.Body.Close()
		raw, err := io.ReadAll(response.Body)
		if err != nil || response.StatusCode != want {
			t.Fatal("incorrect HTTP result", response.StatusCode, want, err)
		}
		if response.Header.Get("Cache-Control") != "no-store" || response.Header.Get("X-Request-ID") != "http-trace" {
			t.Fatal("missing response controls")
		}
		var result transitionResponse
		if want == 200 || want == 202 {
			if json.Unmarshal(raw, &result) != nil || result.RequestID != "http-trace" {
				t.Fatal("missing response trace")
			}
		}
		return result
	}
	call("POST", transitionPath, "", command, 401)
	call("GET", transitionPath, session.AccessToken, nil, 405)
	call("POST", transitionPath+"?other=scope", session.AccessToken, command, 400)
	call("POST", transitionPath, session.AccessToken, command, 409)
	listener.Close()
	if err := ops.ConfigureTransitionAdmission("true", runtime); err != nil {
		t.Fatal(err)
	}
	listener = httptest.NewServer(server)
	// An invalid closed command cannot be accepted even with enabled config.
	bad := command
	bad.Action = "install"
	call("POST", transitionPath, session.AccessToken, bad, 400)
	bad = command
	bad.ExpectedRevision = 2
	call("POST", transitionPath, session.AccessToken, bad, 409)
	// Missing trusted bytes fail without creating a durable operation.
	manifest := filepath.Join(filepath.Dir(registry.Path), "test-app-v1.0.1", "manifest.json")
	originalManifest, err := os.ReadFile(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(manifest, []byte("tampered"), 0600); err != nil {
		t.Fatal(err)
	}
	call("POST", transitionPath, session.AccessToken, command, 503)
	if err = os.WriteFile(manifest, originalManifest, 0600); err != nil {
		t.Fatal(err)
	}
	var count int
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM station.app_installation_transitions`).Scan(&count); err != nil || count != 0 {
		t.Fatal("invalid requests created transitions", err)
	}
	accepted := call("POST", transitionPath, session.AccessToken, command, 202).Transition
	if accepted.Status != "accepted" || accepted.Phase != "accepted" || accepted.RequestID != "original-trace" {
		t.Fatal("invalid acceptance receipt")
	}
	command.RequestID = "retry-trace"
	if out := call("POST", transitionPath, session.AccessToken, command, 202); out.Transition.OperationID != accepted.OperationID {
		t.Fatal("replay changed identity")
	}
	bad = command
	bad.TargetReleaseRef = "different"
	call("POST", transitionPath, session.AccessToken, bad, 409)
	listener.Close()
	if err = ops.ConfigureTransitionAdmission("false", nil); err != nil {
		t.Fatal(err)
	}
	ops.TransitionReleases = nil
	listener = httptest.NewServer(server)
	if out := call("POST", transitionPath, session.AccessToken, command, 202); out.Transition.OperationID != accepted.OperationID {
		t.Fatal("disabled gate blocked durable replay")
	}
	path := transitionPath + "/" + accepted.OperationID
	if out := call("GET", path, session.AccessToken, nil, 200); out.Transition.OperationID != accepted.OperationID {
		t.Fatal("query changed identity")
	}
	call("GET", transitionPath+"/"+uuid.Must(uuid.NewV7()).String(), session.AccessToken, nil, 404)
	for _, otherOrg := range []bool{false, true} {
		user, org := uuid.Must(uuid.NewV7()).String(), session.OrganizationID
		if otherOrg {
			org = uuid.Must(uuid.NewV7()).String()
			if _, err = pool.Exec(ctx, `INSERT INTO station.organizations(organization_id,name) VALUES($1,'Other')`, org); err != nil {
				t.Fatal(err)
			}
		}
		if _, err = pool.Exec(ctx, `INSERT INTO station.users(user_id,username,password_hash) VALUES($1,$2,'disabled-test-password')`, user, "other-"+user); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, `INSERT INTO station.memberships(user_id,organization_id,role) VALUES($1,$2,'admin')`, user, org); err != nil {
			t.Fatal(err)
		}
		token := secret()
		hash := sha256.Sum256([]byte(token))
		if _, err = pool.Exec(ctx, `INSERT INTO station.sessions(session_id,user_id,organization_id,token_hash,issued_at,expires_at,revocation_version) VALUES($1,$2,$3,$4,now(),now()+interval '1 hour',0)`, uuid.Must(uuid.NewV7()), user, org, hash[:]); err != nil {
			t.Fatal(err)
		}
		call("GET", path, token, nil, 404)
	}
	var after string
	if err = pool.QueryRow(ctx, `SELECT row_to_json(i)::text FROM station.app_installations i WHERE installation_id=$1`, installation).Scan(&after); err != nil || after != original {
		t.Fatal("acceptance changed installation", err)
	}
	if err = pool.QueryRow(ctx, `SELECT count(*) FROM station.audit_events WHERE action='app.upgrade.accepted'`).Scan(&count); err != nil || count != 1 {
		t.Fatal("duplicate or missing audit", count, err)
	}
	call("POST", "/identity/logout", session.AccessToken, map[string]any{}, 204)
	call("GET", path, session.AccessToken, nil, 401)
}
