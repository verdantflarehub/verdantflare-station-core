package gateway

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/egress"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEgressHTTPContract(t *testing.T) {
	pool := testdb.New(t)
	station := uuid.Must(uuid.NewV7()).String()
	if e := migrations.Apply(t.Context(), pool, station); e != nil {
		t.Fatal(e)
	}
	ids, e := identity.New(pool, station, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	session, e := ids.Bootstrap(t.Context(), "egress-http", identity.BootstrapRequest{Username: "egress-admin", Password: "fixture-password-long", OrganizationName: "fixture"})
	if e != nil {
		t.Fatal(e)
	}
	key := make([]byte, 32)
	rand.Read(key)
	probe, _ := egress.NewProber("")
	service, _ := egress.New(pool, station, key, probe)
	var log bytes.Buffer
	s := &Server{Identity: ids, Egress: service, Logger: slog.New(slog.NewJSONHandler(&log, nil))}
	call := func(method, path, body, token string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	root := "/api/v1/proxies"
	if w := call("GET", root, "", ""); w.Code != 401 {
		t.Fatal("authentication bypass", w.Code)
	}
	body := `{"name":"API test","host":"127.0.0.1","protocol":"socks5","port":7891,"username":"credential-fixture","password":"secret-fixture","note":"retained"}`
	w := call("POST", root, body, session.AccessToken)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	var out struct {
		Item egress.Endpoint `json:"item"`
	}
	if e = json.Unmarshal(w.Body.Bytes(), &out); e != nil {
		t.Fatal(e)
	}
	id := out.Item.ID
	if id == "" {
		t.Fatal("missing ID")
	}
	if strings.Contains(w.Body.String(), "secret-fixture") || strings.Contains(w.Body.String(), "credential-fixture") {
		t.Fatal("credential response leak")
	}
	for _, bad := range []string{`{"name":"a","name":"b"}`, `{"name":"a","target":"https://evil.example"}`, `{"name":null}`, `{"status":"active"}`, `{} {}`} {
		if w = call("POST", root, bad, session.AccessToken); w.Code != 400 {
			t.Fatal("invalid accepted", bad, w.Code)
		}
	}
	if w = call("POST", root+"/"+id+"/test", `{"target":"https://evil.example"}`, session.AccessToken); w.Code != 400 {
		t.Fatal("custom probe target allowed")
	}
	w = call("POST", root+"/"+id+"/test", "{}", session.AccessToken)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	json.Unmarshal(w.Body.Bytes(), &out)
	if out.Item.Status != "pending" || out.Item.Probe == nil || out.Item.Probe.Status != "failed" {
		t.Fatal("forbidden endpoint false success")
	}
	if w = call("GET", root+"/"+id, "", session.AccessToken); w.Code != 200 || !strings.Contains(w.Body.String(), "retained") {
		t.Fatal("get failed")
	}
	if w = call("POST", root+"/"+id+"/assign", "{}", session.AccessToken); w.Code != 404 {
		t.Fatal("H2 exposed")
	}
	if w = call("PATCH", root, "{}", session.AccessToken); w.Code != 405 {
		t.Fatal("method boundary")
	}
	if w = call("DELETE", root+"/"+id, "", session.AccessToken); w.Code != 204 {
		t.Fatal("unreferenced fixture delete failed")
	}
	if w = call("GET", root+"/"+id, "", session.AccessToken); w.Code != 404 {
		t.Fatal("deleted record still found")
	}
	if strings.Contains(log.String(), "secret-fixture") || strings.Contains(log.String(), session.AccessToken) {
		t.Fatal("logs disclose credentials")
	}
	_ = io.Discard
}
