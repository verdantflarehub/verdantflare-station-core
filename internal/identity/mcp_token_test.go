package identity

import (
	"crypto/sha256"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func TestBearerCredentialFormat(t *testing.T) {
	for _, token := range []string{"", "short", strings.Repeat("a", 513), strings.Repeat("a", 32) + "\n", strings.Repeat("a", 32) + " secret", strings.Repeat("a", 32) + ":bad"} {
		if validBearerToken(token) {
			t.Fatal("invalid bearer format accepted")
		}
	}
	for _, token := range []string{strings.Repeat("a", 32), strings.Repeat("b", 64), "mcp_" + strings.Repeat("x", 40), strings.Repeat("a", 510) + "=="} {
		if !validBearerToken(token) {
			t.Fatal("opaque bearer format rejected")
		}
	}
}

func TestBoundMCPTokenIdentityRevocationAndIdempotency(t *testing.T) {
	ctx := t.Context()
	pool := testdb.New(t)
	id := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, pool, id); err != nil {
		t.Fatal(err)
	}
	svc, err := New(pool, id, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	owner, err := svc.Bootstrap(ctx, "fixture-bootstrap", BootstrapRequest{Username: "fixture", Password: "fixture-password-only", OrganizationName: "fixture"})
	if err != nil {
		t.Fatal(err)
	}
	token := "mcp_" + strings.Repeat("a", 60)
	expires := time.Now().UTC().Add(24 * time.Hour).Truncate(time.Second)
	bind := func(value, user, org string, expiry time.Time) (IdentityContext, error) {
		return svc.BindMCPToken(ctx, "fixture-bind", value, user, org, expiry)
	}
	// Two operator retries must create exactly one stable credential record.
	var wg sync.WaitGroup
	results := make(chan IdentityContext, 2)
	errs := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			out, e := bind(token, owner.UserID, owner.OrganizationID, expires)
			results <- out
			errs <- e
		}()
	}
	wg.Wait()
	first, second := <-results, <-results
	if e1, e2 := <-errs, <-errs; e1 != nil || e2 != nil || first.SessionID != second.SessionID {
		t.Fatal("non-idempotent binding", e1, e2)
	}
	me, err := svc.Me(ctx, "fixture-me", token)
	if err != nil || me.UserID != owner.UserID || me.OrganizationID != owner.OrganizationID || me.SessionID != first.SessionID {
		t.Fatal("token did not represent its bound owner", err)
	}
	sum := sha256.Sum256([]byte(token))
	var count int
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM station.sessions WHERE token_hash=$1 AND credential_kind='mcp'", sum[:]).Scan(&count); err != nil || count != 1 {
		t.Fatal("credential hash missing", err)
	}
	if err = pool.QueryRow(ctx, "SELECT count(*) FROM station.audit_events WHERE action='identity.bind_mcp_token'").Scan(&count); err != nil || count != 1 {
		t.Fatal("binding audit duplicated or missing", err)
	}
	if _, err = svc.Refresh(ctx, "fixture-refresh", token); !errors.Is(err, Denied) {
		t.Fatal("MCP credential lost its stable revocation history", err)
	}
	for _, tc := range []struct {
		user, org string
		expiry    time.Time
	}{
		{uuid.Must(uuid.NewV7()).String(), owner.OrganizationID, expires},
		{owner.UserID, uuid.Must(uuid.NewV7()).String(), expires},
		{owner.UserID, owner.OrganizationID, expires.Add(time.Hour)},
	} {
		if _, err = bind(token, tc.user, tc.org, tc.expiry); !errors.Is(err, Conflict) {
			t.Fatal("credential silently rebound or extended", err)
		}
	}
	if _, err = bind("mcp_"+strings.Repeat("b", 60), uuid.Must(uuid.NewV7()).String(), owner.OrganizationID, expires); !errors.Is(err, Denied) {
		t.Fatal("unknown principal received a credential", err)
	}
	if _, err = bind(owner.AccessToken, owner.UserID, owner.OrganizationID, expires); !errors.Is(err, Conflict) {
		t.Fatal("browser session converted implicitly", err)
	}
	if err = svc.Logout(ctx, "fixture-revoke", token); err != nil {
		t.Fatal(err)
	}
	if _, err = svc.Me(ctx, "fixture-me", token); !errors.Is(err, Revoked) {
		t.Fatal("revoked credential accepted", err)
	}
	if _, err = bind(token, owner.UserID, owner.OrganizationID, expires); !errors.Is(err, Conflict) {
		t.Fatal("revoked credential resurrected", err)
	}
	// User, organization and membership changes are rechecked on every call.
	for i, statement := range []string{
		"UPDATE station.users SET status='disabled' WHERE user_id=$1",
		"UPDATE station.memberships SET active=false WHERE user_id=$1",
		"UPDATE station.organizations SET status='disabled' WHERE organization_id=(SELECT organization_id FROM station.memberships WHERE user_id=$1)",
		"UPDATE station.users SET revocation_version=revocation_version+1 WHERE user_id=$1",
	} {
		value := "mcp_" + strings.Repeat(string(rune('c'+i)), 60)
		if _, err = bind(value, owner.UserID, owner.OrganizationID, expires); err != nil {
			t.Fatal(err)
		}
		if _, err = pool.Exec(ctx, statement, owner.UserID); err != nil {
			t.Fatal(err)
		}
		if _, err = svc.Me(ctx, "fixture-me", value); err == nil {
			t.Fatal("principal state was not revalidated")
		}
		if _, err = bind(value, owner.UserID, owner.OrganizationID, expires); err == nil {
			t.Fatal("denied credential rebound")
		}
		for _, restore := range []string{"UPDATE station.users SET status='active'", "UPDATE station.memberships SET active=true", "UPDATE station.organizations SET status='active'"} {
			if _, err = pool.Exec(ctx, restore); err != nil {
				t.Fatal(err)
			}
		}
	}
	expiring := "mcp_" + strings.Repeat("z", 60)
	bound, err := bind(expiring, owner.UserID, owner.OrganizationID, expires)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = pool.Exec(ctx, "UPDATE station.sessions SET issued_at=now()-interval '2 hours',expires_at=now()-interval '1 hour' WHERE session_id=$1", bound.SessionID); err != nil {
		t.Fatal(err)
	}
	if _, err = svc.Me(ctx, "fixture-me", expiring); !errors.Is(err, Unauthenticated) {
		t.Fatal("expired credential accepted", err)
	}
}
