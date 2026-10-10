package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func TestBlenderDelegationRechecksCurrentManagerAndIsolatesCredentials(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	station := uuid.NewString()
	if err := migrations.Apply(ctx, pool, station); err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(pool, station, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := identities.Bootstrap(ctx, "test-bootstrap", identity.BootstrapRequest{Username: "test-manager", Password: strings.Repeat("x", 32), OrganizationName: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	calls := 0
	optionsMode := false
	expectedAction := "create"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if optionsMode {
			if r.URL.Path != "/internal/v1/instances/profiles" || r.Method != "GET" || r.Header.Get("Authorization") != "Bearer "+strings.Repeat("r", 32) {
				t.Error("invalid profiles forwarding")
			}
			_, _ = io.WriteString(w, `{"profiles":[{"profile_id":"standard","storage_reserved_bytes":1024,"max_file_bytes":512,"node_name":"private-node"}]}`)
			return
		}
		if r.URL.Path != "/internal/v1/instances/"+expectedAction || r.Header.Get("Authorization") != "Bearer "+strings.Repeat("r", 32) || r.Header.Get("Cookie") != "" {
			t.Error("untrusted forwarding")
		}
		_, _ = io.WriteString(w, `{"status":"running","phase":"awaiting_content","access":{"worker_token":"worker-private-secret"}}`)
	}))
	defer upstream.Close()
	control, err := NewBlenderControl(strings.Repeat("c", 32), upstream.URL, strings.Repeat("r", 32))
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	server := &Server{Identity: identities, BlenderControl: control, Logger: slog.New(slog.NewJSONHandler(&logs, nil))}
	input := blenderCreate{OperationID: uuid.NewString(), StationID: station, OrganizationID: manager.OrganizationID, UserID: manager.UserID, ProjectID: uuid.NewString(), InstanceID: uuid.NewString(), ProfileID: "blender-standard", SourceRevisionID: uuid.NewString(), EmptySource: true}
	call := func(token string, change func(*http.Request)) *httptest.ResponseRecorder {
		t.Helper()
		body, _ := json.Marshal(input)
		r := httptest.NewRequest("POST", "/internal/v1/blender/create", bytes.NewReader(body))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("X-User-Id", manager.UserID)
		r.Header.Set("X-Organization-Id", manager.OrganizationID)
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Cookie", "external=private")
		if change != nil {
			change(r)
		}
		w := httptest.NewRecorder()
		server.ServeHTTP(w, r)
		return w
	}
	// Even a valid admin browser bearer cannot call the private control route.
	if w := call(manager.AccessToken, nil); w.Code != 401 || calls != 0 || strings.Contains(w.Body.String(), "worker-private") {
		t.Fatal("browser bearer entered private route", w.Code)
	}
	if w := call(control.Token, func(r *http.Request) { r.Header.Add("X-User-Id", manager.UserID) }); w.Code != 401 || calls != 0 {
		t.Fatal("duplicate identity accepted")
	}
	if w := call(control.Token, func(r *http.Request) { r.Header.Set("X-Organization-Id", uuid.NewString()) }); w.Code != 403 || calls != 0 {
		t.Fatal("cross-org accepted")
	}
	if w := call(control.Token, nil); w.Code != 200 || calls != 1 || !strings.Contains(w.Body.String(), "worker-private-secret") {
		t.Fatal("authorized private handoff failed", w.Code)
	}
	optionsMode = true
	options := func(token string) *httptest.ResponseRecorder {
		return call(token, func(r *http.Request) {
			r.URL.Path = "/internal/v1/blender/options"
			r.Body = io.NopCloser(strings.NewReader(`{}`))
			r.ContentLength = 2
		})
	}
	if w := options(manager.AccessToken); w.Code != 401 || calls != 1 {
		t.Fatal("browser bearer entered options")
	}
	if w := options(control.Token); w.Code != 200 || calls != 2 || strings.Contains(w.Body.String(), "private-node") || !strings.Contains(w.Body.String(), station) {
		t.Fatal("invalid options response", w.Code)
	}
	optionsMode = false
	expectedAction = "start"
	start := func(token string) *httptest.ResponseRecorder {
		return call(token, func(r *http.Request) {
			r.URL.Path = "/internal/v1/blender/start"
			body, _ := json.Marshal(blenderStart{OperationID: uuid.NewString(), CreateOperationID: input.OperationID, StationID: station, OrganizationID: manager.OrganizationID, UserID: manager.UserID, ProjectID: input.ProjectID, InstanceID: input.InstanceID, ProfileID: input.ProfileID, SourceRevisionID: input.SourceRevisionID, Empty: true})
			r.Body = io.NopCloser(bytes.NewReader(body))
			r.ContentLength = int64(len(body))
		})
	}
	if w := start(manager.AccessToken); w.Code != 401 || calls != 2 {
		t.Fatal("browser entered private startup", w.Code)
	}
	if w := start(control.Token); w.Code != 200 || calls != 3 {
		t.Fatal("private startup forwarding failed", w.Code)
	}
	if _, err = pool.Exec(ctx, `UPDATE station.memberships SET active=false WHERE user_id=$1 AND organization_id=$2`, manager.UserID, manager.OrganizationID); err != nil {
		t.Fatal(err)
	}
	if w := call(control.Token, nil); w.Code != 403 || calls != 3 {
		t.Fatal("revoked manager continued execution", w.Code)
	}
	if w := options(control.Token); w.Code != 403 || calls != 3 {
		t.Fatal("revoked manager read options")
	}
	if w := start(control.Token); w.Code != 403 || calls != 3 {
		t.Fatal("revoked manager started worker", w.Code)
	}
	if _, err = pool.Exec(ctx, `UPDATE station.memberships SET active=true WHERE user_id=$1 AND organization_id=$2`, manager.UserID, manager.OrganizationID); err != nil {
		t.Fatal(err)
	}
	expectedAction = "access"
	access := func(token string) *httptest.ResponseRecorder {
		return call(token, func(r *http.Request) {
			r.URL.Path = "/internal/v1/blender/access"
			body, _ := json.Marshal(blenderAccess{StationID: station, OrganizationID: manager.OrganizationID, UserID: manager.UserID,
				ProjectID: input.ProjectID, InstanceID: input.InstanceID, StartOperationID: input.OperationID, Kind: "worker"})
			r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		})
	}
	if w := access(manager.AccessToken); w.Code != 401 || calls != 3 {
		t.Fatal("user bearer entered credential resolver")
	}
	if w := access(control.Token); w.Code != 200 || calls != 4 {
		t.Fatal("authorized member cannot resolve application-scoped access", w.Code)
	}
	if _, err = pool.Exec(ctx, `UPDATE station.users SET status='disabled' WHERE user_id=$1`, manager.UserID); err != nil {
		t.Fatal(err)
	}
	if w := access(control.Token); w.Code != 403 || calls != 4 {
		t.Fatal("disabled member resolved credentials", w.Code)
	}
	expectedAction = "stop"
	stop := func(token string) *httptest.ResponseRecorder {
		return call(token, func(r *http.Request) {
			r.URL.Path = "/internal/v1/blender/stop"
			body, _ := json.Marshal(blenderStop{OperationID: uuid.NewString(), StartOperationID: input.OperationID,
				StationID: station, OrganizationID: manager.OrganizationID, UserID: manager.UserID,
				ProjectID: input.ProjectID, InstanceID: input.InstanceID})
			r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		})
	}
	if w := stop(control.Token); w.Code != 403 || calls != 4 {
		t.Fatal("disabled manager stopped worker", w.Code)
	}
	if _, err = pool.Exec(ctx, `UPDATE station.users SET status='active' WHERE user_id=$1`, manager.UserID); err != nil {
		t.Fatal(err)
	}
	if w := stop(manager.AccessToken); w.Code != 401 || calls != 4 {
		t.Fatal("browser bearer stopped worker", w.Code)
	}
	if w := stop(control.Token); w.Code != 200 || calls != 5 {
		t.Fatal("private stop forwarding failed", w.Code)
	}
	if _, err = pool.Exec(ctx, `UPDATE station.memberships SET active=false WHERE user_id=$1 AND organization_id=$2`, manager.UserID, manager.OrganizationID); err != nil {
		t.Fatal(err)
	}
	if w := stop(control.Token); w.Code != 403 || calls != 5 {
		t.Fatal("revoked manager stopped worker", w.Code)
	}
	expectedAction = "destroy"
	destroy := func(token string) *httptest.ResponseRecorder {
		return call(token, func(r *http.Request) {
			r.URL.Path = "/internal/v1/blender/destroy"
			body, _ := json.Marshal(blenderDestroy{OperationID: uuid.NewString(), PreviousOperationID: input.OperationID,
				StationID: station, OrganizationID: manager.OrganizationID, UserID: manager.UserID, ProjectID: input.ProjectID, InstanceID: input.InstanceID})
			r.Body, r.ContentLength = io.NopCloser(bytes.NewReader(body)), int64(len(body))
		})
	}
	if w := destroy(control.Token); w.Code != 403 || calls != 5 {
		t.Fatal("revoked manager destroyed worker", w.Code)
	}
	if _, err = pool.Exec(ctx, `UPDATE station.memberships SET active=true WHERE user_id=$1 AND organization_id=$2`, manager.UserID, manager.OrganizationID); err != nil {
		t.Fatal(err)
	}
	if w := destroy(manager.AccessToken); w.Code != 401 || calls != 5 {
		t.Fatal("browser bearer destroyed worker", w.Code)
	}
	if w := destroy(control.Token); w.Code != 200 || calls != 6 {
		t.Fatal("private destroy forwarding failed", w.Code)
	}
	if strings.Contains(logs.String(), "worker-private-secret") || strings.Contains(logs.String(), manager.AccessToken) || strings.Contains(logs.String(), control.RuntimeToken) {
		t.Fatal("credentials logged")
	}
}

func TestBlenderControlConfigurationIsOriginOnly(t *testing.T) {
	for _, endpoint := range []string{"https://example.test/mcp", "http://user:pass@example.test", "https://example.test?target=other", "file:///tmp/runtime"} {
		if _, err := NewBlenderControl(strings.Repeat("c", 32), endpoint, strings.Repeat("r", 32)); err == nil {
			t.Fatal("invalid origin accepted")
		}
	}
	if c, err := NewBlenderControl("", "", ""); err != nil || c != nil {
		t.Fatal("optional feature enabled without config")
	}
}
