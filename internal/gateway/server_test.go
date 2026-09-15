package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func secret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}
func TestIdentityHTTP(t *testing.T) {
	ctx := context.Background()
	p := testdb.New(t)
	id := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, p, id); err != nil {
		t.Fatal(err)
	}
	svc, err := identity.New(p, id, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	bootstrap, password := secret(), secret()
	var logs bytes.Buffer
	// Concurrent requests use a discard logger; capture sequential requests below.
	s := &Server{Identity: svc, BootstrapToken: bootstrap, Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	call := func(path, method, token, body string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("X-Request-ID", "integration-request")
		r.Header.Set("X-Bootstrap-Token", bootstrap)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		w := httptest.NewRecorder()
		s.ServeHTTP(w, r)
		return w
	}
	expect := func(w *httptest.ResponseRecorder, status int) {
		t.Helper()
		if w.Code != status {
			t.Fatalf("status=%d want=%d", w.Code, status)
		}
		if w.Header().Get("X-Request-ID") != "integration-request" {
			t.Fatal("request trace missing")
		}
	}
	read := func(w *httptest.ResponseRecorder) identity.Session {
		t.Helper()
		var v identity.Session
		if json.Unmarshal(w.Body.Bytes(), &v) != nil || v.AccessToken == "" {
			t.Fatal("missing session")
		}
		return v
	}
	body, _ := json.Marshal(identity.BootstrapRequest{Username: "Admin", Password: password, OrganizationName: "Local"})
	results := make(chan *httptest.ResponseRecorder, 2)
	var wg sync.WaitGroup
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- call("/identity/bootstrap", "POST", "", string(body)) }()
	}
	wg.Wait()
	close(results)
	var session identity.Session
	success, conflict := 0, 0
	for w := range results {
		switch w.Code {
		case 201:
			success++
			session = read(w)
		case 409:
			conflict++
		default:
			t.Fatalf("bootstrap status %d", w.Code)
		}
	}
	if success != 1 || conflict != 1 {
		t.Fatal("bootstrap was not exclusive")
	}
	s.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	expect(call("/readyz", "GET", "", ""), 200)
	expect(call("/identity/me", "GET", session.AccessToken, ""), 200)
	expect(call("/station/health", "GET", session.AccessToken, ""), 200)
	for _, body := range []string{`{"username":"admin","username":"other"}`, `{"username":null}`, `{"USERNAME":"admin"}`, `{} {}`, `{"scopes":["admin"]}`, `[]`, strings.Repeat("x", 17000)} {
		expect(call("/identity/login", "POST", "", body), 400)
	}
	login := func(pass, org string) *httptest.ResponseRecorder {
		b, _ := json.Marshal(identity.LoginRequest{Username: "ADMIN", Password: pass, OrganizationID: org})
		return call("/identity/login", "POST", "", string(b))
	}
	expect(login(password, uuid.Must(uuid.NewV7()).String()), 403)
	for i := 0; i < 5; i++ {
		status := 401
		if i == 4 {
			status = 423
		}
		expect(login("incorrect", ""), status)
	}
	expect(login(password, ""), 423)
	if _, err = p.Exec(ctx, "UPDATE station.users SET locked_until=now()-interval '1 second'"); err != nil {
		t.Fatal(err)
	}
	w := login(password, "")
	expect(w, 201)
	session = read(w)
	s.Logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	results = make(chan *httptest.ResponseRecorder, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- call("/identity/refresh", "POST", session.AccessToken, "") }()
	}
	wg.Wait()
	close(results)
	var rotated identity.Session
	success = 0
	for w := range results {
		if w.Code == 200 {
			success++
			rotated = read(w)
		} else if w.Code != 401 {
			t.Fatalf("refresh status %d", w.Code)
		}
	}
	if success != 1 {
		t.Fatal("refresh token reused")
	}
	s.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	expect(call("/identity/me", "GET", session.AccessToken, ""), 401)
	// Reconstruct service, representing loss of all in-memory identity state.
	svc, err = identity.New(p, id, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	s.Identity = svc
	expect(call("/identity/me", "GET", rotated.AccessToken, ""), 200)
	expect(call("/identity/logout", "POST", rotated.AccessToken, ""), 204)
	expect(call("/identity/logout", "POST", rotated.AccessToken, ""), 204)
	expect(call("/identity/me", "GET", rotated.AccessToken, ""), 401)
	w = login(password, "")
	expect(w, 201)
	session = read(w)
	if _, err = p.Exec(ctx, "UPDATE station.memberships SET active=false"); err != nil {
		t.Fatal(err)
	}
	expect(call("/identity/me", "GET", session.AccessToken, ""), 403)
	if _, err = p.Exec(ctx, "UPDATE station.memberships SET active=true; UPDATE station.sessions SET expires_at=issued_at+interval '1 microsecond'"); err != nil {
		t.Fatal(err)
	}
	expect(call("/identity/me", "GET", session.AccessToken, ""), 401)
	var hash string
	var tokenHash []byte
	var count int
	if err = p.QueryRow(ctx, "SELECT password_hash FROM station.users").Scan(&hash); err != nil {
		t.Fatal(err)
	}
	if hash == password || !strings.HasPrefix(hash, "$2") {
		t.Fatal("password not hashed")
	}
	if err = p.QueryRow(ctx, "SELECT token_hash FROM station.sessions LIMIT 1").Scan(&tokenHash); err != nil || len(tokenHash) != 32 {
		t.Fatal("token digest invalid")
	}
	if err = p.QueryRow(ctx, "SELECT count(*) FROM station.audit_events WHERE request_id='integration-request'").Scan(&count); err != nil || count < 10 {
		t.Fatal("audit missing")
	}
	for _, sql := range []string{"UPDATE station.audit_events SET outcome='denied'", "DELETE FROM station.audit_events", "TRUNCATE station.audit_events"} {
		if _, err = p.Exec(ctx, sql); err == nil {
			t.Fatal("audit mutation allowed")
		}
	}
	for _, value := range []string{password, bootstrap, session.AccessToken, rotated.AccessToken} {
		if strings.Contains(logs.String(), value) {
			t.Fatal("secret in logs")
		}
	}
}
func TestInvalidRequestID(t *testing.T) {
	s := &Server{Logger: slog.New(slog.NewJSONHandler(io.Discard, nil))}
	r := httptest.NewRequest("GET", "/healthz", nil)
	r.Header.Set("X-Request-ID", "invalid request id")
	w := httptest.NewRecorder()
	s.ServeHTTP(w, r)
	if w.Code != 400 || w.Header().Get("X-Request-ID") == r.Header.Get("X-Request-ID") {
		t.Fatal("invalid request id accepted")
	}
}
