package egress

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func serviceFixture(t *testing.T) (*Service, identity.IdentityContext) {
	t.Helper()
	pool := testdb.New(t)
	station := uuid.Must(uuid.NewV7()).String()
	if e := migrations.Apply(t.Context(), pool, station); e != nil {
		t.Fatal(e)
	}
	ids, e := identity.New(pool, station, time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	session, e := ids.Bootstrap(t.Context(), "egress-fixture", identity.BootstrapRequest{Username: "egress-test", Password: "integration-only-passphrase", OrganizationName: "Egress fixture"})
	if e != nil {
		t.Fatal(e)
	}
	key := make([]byte, 32)
	rand.Read(key)
	prober, _ := NewProber("")
	s, e := New(pool, station, key, prober)
	if e != nil {
		t.Fatal(e)
	}
	return s, session.IdentityContext
}
func inputFrom(p Endpoint) Input {
	expiry := ""
	if p.ExpiresAt != nil {
		expiry = p.ExpiresAt.Format(time.RFC3339)
	}
	return Input{Name: p.Name, Protocol: p.Protocol, Host: p.Host, Port: p.Port, Tag: p.Tag, Note: p.Note, ExpiresAt: expiry, Revision: p.Revision}
}
func TestPersistenceSecretsIsolationAndReferences(t *testing.T) {
	s, u := serviceFixture(t)
	ctx := t.Context()
	user, pass := "private-user", "private-password"
	in := Input{Name: "review", Protocol: "SOCKS5", Host: "proxy.example.com", Port: 1080, Username: &user, Password: &pass, Note: "retain this note", Tag: "qa", ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339)}
	p, e := s.Save(ctx, u, "", in)
	if e != nil {
		t.Fatal(e)
	}
	if p.Status != "pending" || !p.AuthConfigured || p.Note != in.Note || p.ExpiresAt == nil {
		t.Fatal("fields lost")
	}
	var encrypted []byte
	if e = s.Pool.QueryRow(ctx, `SELECT credentials FROM station.egress_proxies WHERE proxy_id=$1`, p.ID).Scan(&encrypted); e != nil {
		t.Fatal(e)
	}
	if bytes.Contains(encrypted, []byte(pass)) || bytes.Contains(encrypted, []byte(user)) {
		t.Fatal("plaintext stored")
	}
	raw, _ := json.Marshal(p)
	if strings.Contains(string(raw), pass) || strings.Contains(string(raw), user) {
		t.Fatal("plaintext returned")
	}
	if _, e = s.Save(ctx, u, "", in); !errors.Is(e, ErrConflict) {
		t.Fatal("duplicate accepted", e)
	}
	restarted := *s
	list, e := restarted.List(ctx, u)
	if e != nil || len(list) != 1 || list[0].ID != p.ID {
		t.Fatal("not durable", e)
	}
	edit := inputFrom(p)
	edit.Note = "updated note"
	p, e = s.Save(ctx, u, p.ID, edit)
	if e != nil {
		t.Fatal(e)
	}
	if !p.AuthConfigured {
		t.Fatal("omission cleared credentials")
	}
	c, e := unseal(s.cipher, p.credentials, s.aad(u.OrganizationID, p.ID))
	if e != nil || c.Password != pass {
		t.Fatal("credential preservation failed")
	}
	if _, e = s.Save(ctx, u, p.ID, edit); !errors.Is(e, ErrConflict) {
		t.Fatal("stale write accepted")
	}
	denied := u
	denied.Roles = []string{"reader"}
	if _, e = s.List(ctx, denied); !errors.Is(e, identity.Denied) {
		t.Fatal("role bypass")
	}
	other := u
	other.OrganizationID = uuid.NewString()
	if _, e = s.Get(ctx, other, p.ID); !errors.Is(e, ErrNotFound) {
		t.Fatal("cross organization read")
	}
	other.StationID = uuid.NewString()
	if _, e = s.Get(ctx, other, p.ID); !errors.Is(e, identity.Denied) {
		t.Fatal("cross station read")
	}
	edit = inputFrom(p)
	edit.ClearAuth = true
	edit.Status = "off"
	p, e = s.Save(ctx, u, p.ID, edit)
	if e != nil || p.AuthConfigured || p.Status != "off" {
		t.Fatal("clear or off failed", e)
	}
	if _, e = s.Test(ctx, u, p.ID); !errors.Is(e, ErrDisabled) {
		t.Fatal("disabled probe allowed")
	}
	if _, e = s.Pool.Exec(ctx, `UPDATE station.egress_proxies SET assigned_apps=ARRAY['fixture-app'] WHERE proxy_id=$1`, p.ID); e != nil {
		t.Fatal(e)
	}
	if e = s.Delete(ctx, u, p.ID); !errors.Is(e, ErrConflict) {
		t.Fatal("referenced off proxy deleted")
	}
	if _, e = unseal(s.cipher, encrypted, s.aad(u.OrganizationID, uuid.NewString())); e == nil {
		t.Fatal("AAD not bound to record")
	}
}
func TestProbePersistenceAndConcurrentEdits(t *testing.T) {
	s, u := serviceFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	prober, ep, _ := probeFixture(t, "socks5h", Credentials{}, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/exit" {
			once.Do(func() { close(entered); <-release })
		}
		standardTarget(w, r)
	})
	s.Prober = prober
	in := Input{Name: "concurrent", Protocol: ep.Protocol, Host: ep.Host, Port: ep.Port}
	p, e := s.Save(t.Context(), u, "", in)
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() { _, e := s.Test(context.Background(), u, p.ID); done <- e }()
	<-entered
	if _, e = s.Test(t.Context(), u, p.ID); !errors.Is(e, ErrBusy) {
		t.Fatal("duplicate test allowed", e)
	}
	edit := inputFrom(p)
	edit.Status = "off"
	if _, e = s.Save(t.Context(), u, p.ID, edit); e != nil {
		t.Fatal(e)
	}
	close(release)
	if e = <-done; !errors.Is(e, ErrConflict) {
		t.Fatal("old result accepted", e)
	}
	p, e = s.Get(t.Context(), u, p.ID)
	if e != nil || p.Status != "off" || p.Probe != nil {
		t.Fatal("stale probe overwrote disable")
	}
	edit = inputFrom(p)
	edit.Status = "pending"
	p, e = s.Save(t.Context(), u, p.ID, edit)
	if e != nil {
		t.Fatal(e)
	}
	p, e = s.Test(t.Context(), u, p.ID)
	if e != nil || p.Status != "active" || p.Probe == nil || p.Testing {
		t.Fatal("real probe persistence", e, p)
	}
	p, e = s.Get(t.Context(), u, p.ID)
	if e != nil || len(p.Probe.Observations) != 2 {
		t.Fatal("result lost after reread")
	}
	edit = inputFrom(p)
	edit.Port = 1
	p, e = s.Save(t.Context(), u, p.ID, edit)
	if e != nil {
		t.Fatal(e)
	}
	if p.Probe != nil || p.Status != "pending" {
		t.Fatal("configuration edit retained old success")
	}
	p, e = s.Test(t.Context(), u, p.ID)
	if e != nil || p.Status != "pending" || p.Probe.Status != "failed" {
		t.Fatal("invalid endpoint false success", e)
	}
}
func TestInputValidation(t *testing.T) {
	for _, mutate := range []func(*Input){func(i *Input) { i.Protocol = "ftp" }, func(i *Input) { i.Port = 0 }, func(i *Input) { i.Port = 65536 }, func(i *Input) { i.Host = "x@y" }, func(i *Input) { i.Host = "169.254.1.1/path" }, func(i *Input) { i.Status = "active" }, func(i *Input) { i.ExpiresAt = "tomorrow" }, func(i *Input) { i.Name = "" }} {
		in := Input{Name: "x", Protocol: "socks5", Host: "example.com", Port: 1080}
		mutate(&in)
		if _, e := normalize(&in); e == nil {
			t.Fatalf("invalid accepted: %+v", in)
		}
	}
}
