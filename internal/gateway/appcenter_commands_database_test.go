package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/operations"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func TestAppCenterCommandGatewayDatabaseReplay(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	station := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, pool, station); err != nil {
		t.Fatal(err)
	}
	auth, err := identity.New(pool, station, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := auth.Bootstrap(ctx, "test", identity.BootstrapRequest{Username: "testadmin", Password: secret(), OrganizationName: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	ops := &operations.Service{Pool: pool, StationID: station}
	server := &Server{Identity: auth, Operations: ops, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	digest := "sha256:" + strings.Repeat("a", 64)
	input := operations.AppCenterCommand{SchemaVersion: 1, Command: operations.Command{RequestID: "original-trace", IdempotencyKey: "fixed-command-key", OrganizationID: session.OrganizationID, StationID: station, AppID: "test-app", AppVersion: "1.0.0", Action: "install"}, Expected: operations.AppCenterExpected{AppVersion: "1.0.0", ReleaseDigest: &digest, PolicyRevision: digest}}
	call := func(method, path, token string, input any, want int) *httptest.ResponseRecorder {
		t.Helper()
		raw, _ := json.Marshal(input)
		r := httptest.NewRequest(method, path, strings.NewReader(string(raw)))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Request-ID", "http-trace")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		server.ServeHTTP(w, r)
		if w.Code != want {
			t.Fatalf("status %d want %d", w.Code, want)
		}
		return w
	}
	call("POST", "/appcenter/v1/commands", "", input, 401)
	call("GET", "/appcenter/v1/commands", session.AccessToken, input, 405)
	call("POST", "/appcenter/v1/commands?scope=other", session.AccessToken, input, 400)
	out := call("POST", "/appcenter/v1/commands", session.AccessToken, input, 409)
	var failure map[string]any
	if json.Unmarshal(out.Body.Bytes(), &failure) != nil || failure["code"] != "ACTION_UNSUPPORTED" || failure["requestId"] != "http-trace" || len(failure) != 2 {
		t.Fatal("invalid AppCenter error contract")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM station.app_operations`).Scan(&count); err != nil || count != 0 {
		t.Fatal("disabled source accepted new operation", err)
	}
	// Seed a durably accepted command through the existing tested submitter;
	// route replay must not require an enabled current release configuration.
	admit := func(context.Context, identity.IdentityContext, operations.Command, *operations.Installation) (operations.AppCenterExecution, error) {
		return operations.AppCenterExecution{SchemaVersion: 1, AppID: "test-app", AppVersion: "1.0.0", Action: "install", ReleaseDigest: digest, TemplateDigest: digest, PolicyRevision: digest}, nil
	}
	op, err := ops.SubmitAppCenter(ctx, session.IdentityContext, input, admit)
	if err != nil {
		t.Fatal(err)
	}
	out = call("POST", "/appcenter/v1/commands", session.AccessToken, input, 202)
	var replay operations.Operation
	if json.Unmarshal(out.Body.Bytes(), &replay) != nil || replay.OperationID != op.OperationID || replay.RequestID != "original-trace" || out.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("HTTP replay lost durable receipt")
	}
	changed := input
	changed.Expected.PolicyRevision = "sha256:" + strings.Repeat("b", 64)
	call("POST", "/appcenter/v1/commands", session.AccessToken, changed, 409)
	call("POST", "/identity/logout", session.AccessToken, map[string]any{}, 204)
	call("POST", "/appcenter/v1/commands", session.AccessToken, input, 401)
}
