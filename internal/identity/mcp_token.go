package identity

import (
	"context"
	"crypto/sha256"
	"errors"
	"regexp"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Opaque bearer credentials may predate Core's base64url session generator.
// Format acceptance never grants access: the exact hash must be registered.
var bearerTokenPattern = regexp.MustCompile(`^[A-Za-z0-9._~+/-]{32,512}={0,2}$`)

func validBearerToken(token string) bool {
	return len(token) <= 512 && bearerTokenPattern.MatchString(token)
}

func validUUIDv7(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id.Version() == 7 && id.String() == value
}

// BindMCPToken is an operator-only operation. It is deliberately not exposed
// through HTTP. Only a hash is stored; existing ownership and revocation cannot
// be overwritten. Runtime authentication still checks current membership.
func (s *Service) BindMCPToken(ctx context.Context, requestID, token, userID, orgID string, expires time.Time) (IdentityContext, error) {
	var out IdentityContext
	now := time.Now().UTC()
	expires = expires.UTC()
	if !validBearerToken(token) || !validUUIDv7(userID) || !validUUIDv7(orgID) || !expires.After(now) || expires.Nanosecond() != 0 {
		return out, Invalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(context.Background())
	sum := sha256.Sum256([]byte(token))
	// Serialize enrollment and lock existing sessions before users, matching
	// withSession's row-lock order. The unique hash remains the final guard.
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(2026101001)"); err != nil {
		return out, err
	}
	var old IdentityContext
	var revoked *time.Time
	var kind string
	err = tx.QueryRow(ctx, `SELECT session_id::text,user_id::text,organization_id::text,issued_at,expires_at,revocation_version,revoked_at,credential_kind FROM station.sessions WHERE token_hash=$1 FOR UPDATE`, sum[:]).Scan(&old.SessionID, &old.UserID, &old.OrganizationID, &old.IssuedAt, &old.ExpiresAt, &old.RevocationVersion, &revoked, &kind)
	exists := err == nil
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return out, err
	}
	if exists && (kind != "mcp" || old.UserID != userID || old.OrganizationID != orgID || !old.ExpiresAt.Equal(expires) || revoked != nil || !old.ExpiresAt.After(now)) {
		return out, Conflict
	}
	var username, orgName, userStatus, orgStatus, role string
	var version, policy int64
	var active bool
	var lockedUntil *time.Time
	err = tx.QueryRow(ctx, `SELECT u.username,u.status,u.revocation_version,u.locked_until,o.name,o.status,o.policy_version,m.role,m.active FROM station.users u JOIN station.memberships m USING(user_id) JOIN station.organizations o USING(organization_id) WHERE u.user_id=$1 AND o.organization_id=$2 FOR SHARE OF u,o,m`, userID, orgID).Scan(&username, &userStatus, &version, &lockedUntil, &orgName, &orgStatus, &policy, &role, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, Denied
	}
	if err != nil {
		return out, err
	}
	if userStatus != "active" || orgStatus != "active" || role != "admin" || !active || (lockedUntil != nil && lockedUntil.After(now)) {
		return out, Denied
	}
	if exists && old.RevocationVersion != version {
		return out, Conflict
	}
	id := old.SessionID
	issued := old.IssuedAt
	if !exists {
		id, err = newID()
		if err != nil {
			return out, err
		}
		issued = now
		_, err = tx.Exec(ctx, `INSERT INTO station.sessions(session_id,user_id,organization_id,token_hash,issued_at,expires_at,revocation_version,credential_kind) VALUES($1,$2,$3,$4,$5,$6,$7,'mcp')`, id, userID, orgID, sum[:], issued, expires, version)
		if err != nil {
			return out, err
		}
		if err = audit(ctx, tx, requestID, "identity.bind_mcp_token", "succeeded", &userID, &orgID, &id); err != nil {
			return out, err
		}
	}
	out = IdentityContext{requestID, s.StationID, id, userID, username, orgID, orgName, []string{"admin"}, append([]string(nil), adminScopes...), issued.UTC(), expires, version, policy}
	return out, tx.Commit(ctx)
}
