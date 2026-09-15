package identity

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"golang.org/x/crypto/bcrypt"
)

type Fault string

func (f Fault) Error() string { return string(f) }

const (
	Invalid         Fault = "INVALID_ARGUMENT"
	Unauthenticated Fault = "UNAUTHENTICATED"
	Revoked         Fault = "SESSION_REVOKED"
	Denied          Fault = "PERMISSION_DENIED"
	Locked          Fault = "ACCOUNT_LOCKED"
	Conflict        Fault = "CONFLICT"
)

var usernamePattern = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.@-]{2,63}$`)
var adminScopes = []string{"identity:read", "station:read", "app:read", "app:manage", "model:read", "model:manage"}

type LoginRequest struct {
	Username       string `json:"username"`
	Password       string `json:"password"`
	OrganizationID string `json:"organization_id,omitempty"`
}
type BootstrapRequest struct {
	Username         string `json:"username"`
	Password         string `json:"password"`
	OrganizationName string `json:"organization_name"`
}
type IdentityContext struct {
	RequestID         string    `json:"request_id"`
	StationID         string    `json:"station_id"`
	SessionID         string    `json:"session_id"`
	UserID            string    `json:"user_id"`
	Username          string    `json:"username"`
	OrganizationID    string    `json:"organization_id"`
	Roles             []string  `json:"roles"`
	Scopes            []string  `json:"scopes"`
	IssuedAt          time.Time `json:"issued_at"`
	ExpiresAt         time.Time `json:"expires_at"`
	RevocationVersion int64     `json:"revocation_version"`
	PolicyVersion     int64     `json:"policy_version"`
}
type Session struct {
	IdentityContext
	AccessToken string `json:"access_token"`
}
type Service struct {
	Pool       *pgxpool.Pool
	StationID  string
	SessionTTL time.Duration
	dummyHash  []byte
}

func New(pool *pgxpool.Pool, stationID string, ttl time.Duration) (*Service, error) {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return nil, err
	}
	hash, err := bcrypt.GenerateFromPassword(raw, bcrypt.DefaultCost)
	if err != nil {
		return nil, err
	}
	return &Service{pool, stationID, ttl, hash}, nil
}
func validLogin(username, password, org string) bool {
	if !usernamePattern.MatchString(username) || len(password) < 1 || len(password) > 72 {
		return false
	}
	if org != "" {
		id, e := uuid.Parse(org)
		if e != nil || id.Version() != 7 || id.String() != org {
			return false
		}
	}
	return true
}
func newID() (string, error) { id, e := uuid.NewV7(); return id.String(), e }
func newToken() (string, []byte, error) {
	raw := make([]byte, 32)
	if _, e := rand.Read(raw); e != nil {
		return "", nil, e
	}
	value := base64.RawURLEncoding.EncodeToString(raw)
	sum := sha256.Sum256([]byte(value))
	return value, sum[:], nil
}
func audit(ctx context.Context, tx pgx.Tx, requestID, action, outcome string, user, org, session *string) error {
	id, err := newID()
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, "INSERT INTO station.audit_events(event_id,request_id,user_id,organization_id,session_id,action,outcome) VALUES($1,$2,$3,$4,$5,$6,$7)", id, requestID, user, org, session, action, outcome)
	return err
}
func (s *Service) session(ctx context.Context, tx pgx.Tx, requestID, userID, username, org string, version, policy int64) (Session, error) {
	var out Session
	id, err := newID()
	if err != nil {
		return out, err
	}
	token, hash, err := newToken()
	if err != nil {
		return out, err
	}
	now := time.Now().UTC()
	out = Session{IdentityContext{requestID, s.StationID, id, userID, username, org, []string{"admin"}, append([]string(nil), adminScopes...), now, now.Add(s.SessionTTL), version, policy}, token}
	_, err = tx.Exec(ctx, `INSERT INTO station.sessions(session_id,user_id,organization_id,token_hash,issued_at,expires_at,revocation_version) VALUES($1,$2,$3,$4,$5,$6,$7)`, id, userID, org, hash, now, out.ExpiresAt, version)
	return out, err
}
func (s *Service) Bootstrap(ctx context.Context, requestID string, in BootstrapRequest) (Session, error) {
	var out Session
	if !validLogin(in.Username, in.Password, "") || len(in.Password) < 12 || strings.TrimSpace(in.OrganizationName) == "" || len([]rune(in.OrganizationName)) > 128 {
		return out, Invalid
	}
	hash, err := bcrypt.GenerateFromPassword([]byte(in.Password), bcrypt.DefaultCost)
	if err != nil {
		return out, err
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "SELECT pg_advisory_xact_lock(2026091502)"); err != nil {
		return out, err
	}
	var exists bool
	if err = tx.QueryRow(ctx, "SELECT EXISTS(SELECT 1 FROM station.users)").Scan(&exists); err != nil {
		return out, err
	}
	if exists {
		return out, Conflict
	}
	user, err := newID()
	if err != nil {
		return out, err
	}
	org, err := newID()
	if err != nil {
		return out, err
	}
	username := strings.ToLower(in.Username)
	if _, err = tx.Exec(ctx, "INSERT INTO station.users(user_id,username,password_hash) VALUES($1,$2,$3)", user, username, string(hash)); err != nil {
		return out, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO station.organizations(organization_id,name) VALUES($1,$2)", org, strings.TrimSpace(in.OrganizationName)); err != nil {
		return out, err
	}
	if _, err = tx.Exec(ctx, "INSERT INTO station.memberships(user_id,organization_id,role) VALUES($1,$2,'admin')", user, org); err != nil {
		return out, err
	}
	out, err = s.session(ctx, tx, requestID, user, username, org, 0, 1)
	if err != nil {
		return out, err
	}
	if err = audit(ctx, tx, requestID, "identity.bootstrap", "succeeded", &user, &org, &out.SessionID); err != nil {
		return out, err
	}
	return out, tx.Commit(ctx)
}
func (s *Service) Login(ctx context.Context, requestID string, in LoginRequest) (Session, error) {
	var out Session
	if !validLogin(in.Username, in.Password, in.OrganizationID) {
		return out, Invalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(context.Background())
	var user, hash, status string
	var failed int
	var until *time.Time
	var version int64
	err = tx.QueryRow(ctx, `SELECT user_id::text,password_hash,status,failed_logins,locked_until,revocation_version FROM station.users WHERE username=$1 FOR UPDATE`, strings.ToLower(in.Username)).Scan(&user, &hash, &status, &failed, &until, &version)
	if errors.Is(err, pgx.ErrNoRows) {
		_ = bcrypt.CompareHashAndPassword(s.dummyHash, []byte(in.Password))
		if err = audit(ctx, tx, requestID, "identity.login", "denied", nil, nil, nil); err != nil {
			return out, err
		}
		if err = tx.Commit(ctx); err != nil {
			return out, err
		}
		return out, Unauthenticated
	}
	if err != nil {
		return out, err
	}
	deny := func(code Fault) (Session, error) {
		if e := audit(ctx, tx, requestID, "identity.login", "denied", &user, nil, nil); e != nil {
			return out, e
		}
		if e := tx.Commit(ctx); e != nil {
			return out, e
		}
		return out, code
	}
	now := time.Now().UTC()
	if status != "active" {
		return deny(Unauthenticated)
	}
	if until != nil && until.After(now) {
		return deny(Locked)
	}
	if until != nil {
		failed = 0
		if err = audit(ctx, tx, requestID, "identity.unlock", "succeeded", &user, nil, nil); err != nil {
			return out, err
		}
		if _, err = tx.Exec(ctx, "UPDATE station.users SET failed_logins=0,locked_until=NULL WHERE user_id=$1", user); err != nil {
			return out, err
		}
	}
	if bcrypt.CompareHashAndPassword([]byte(hash), []byte(in.Password)) != nil {
		failed++
		var lock *time.Time
		code := Unauthenticated
		if failed >= 5 {
			t := now.Add(15 * time.Minute)
			lock = &t
			code = Locked
			if err = audit(ctx, tx, requestID, "identity.lock", "succeeded", &user, nil, nil); err != nil {
				return out, err
			}
		}
		if _, err = tx.Exec(ctx, "UPDATE station.users SET failed_logins=$2,locked_until=$3 WHERE user_id=$1", user, failed, lock); err != nil {
			return out, err
		}
		return deny(code)
	}
	var org string
	var policy int64
	err = tx.QueryRow(ctx, `SELECT o.organization_id::text,o.policy_version FROM station.memberships m JOIN station.organizations o USING(organization_id) WHERE m.user_id=$1 AND m.active AND m.role='admin' AND o.status='active' AND ($2='' OR o.organization_id::text=$2) ORDER BY o.created_at,o.organization_id LIMIT 1 FOR SHARE OF m,o`, user, in.OrganizationID).Scan(&org, &policy)
	if errors.Is(err, pgx.ErrNoRows) {
		return deny(Denied)
	}
	if err != nil {
		return out, err
	}
	if _, err = tx.Exec(ctx, "UPDATE station.users SET failed_logins=0,locked_until=NULL WHERE user_id=$1", user); err != nil {
		return out, err
	}
	out, err = s.session(ctx, tx, requestID, user, strings.ToLower(in.Username), org, version, policy)
	if err != nil {
		return out, err
	}
	if err = audit(ctx, tx, requestID, "identity.login", "succeeded", &user, &org, &out.SessionID); err != nil {
		return out, err
	}
	return out, tx.Commit(ctx)
}

// withSession serializes refresh and logout on the same row. Every authorization
// reads current user, membership and organization state; no client claims are trusted.
func (s *Service) withSession(ctx context.Context, requestID, token, action string) (Session, error) {
	var out Session
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 32 {
		return out, Unauthenticated
	}
	sum := sha256.Sum256([]byte(token))
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return out, err
	}
	defer tx.Rollback(context.Background())
	var revoked, lockedUntil *time.Time
	var userVersion int64
	var userStatus, orgStatus, role string
	var active bool
	c := IdentityContext{RequestID: requestID, StationID: s.StationID}
	err = tx.QueryRow(ctx, `SELECT se.session_id::text,se.user_id::text,u.username,se.organization_id::text,se.issued_at,se.expires_at,se.revocation_version,se.revoked_at,u.revocation_version,u.status,u.locked_until,o.status,o.policy_version,m.role,m.active
 FROM station.sessions se JOIN station.users u USING(user_id) JOIN station.organizations o USING(organization_id) JOIN station.memberships m ON m.user_id=se.user_id AND m.organization_id=se.organization_id
 WHERE se.token_hash=$1 FOR UPDATE OF se FOR SHARE OF u,o,m`, sum[:]).Scan(&c.SessionID, &c.UserID, &c.Username, &c.OrganizationID, &c.IssuedAt, &c.ExpiresAt, &c.RevocationVersion, &revoked, &userVersion, &userStatus, &lockedUntil, &orgStatus, &c.PolicyVersion, &role, &active)
	if errors.Is(err, pgx.ErrNoRows) {
		return out, Unauthenticated
	}
	if err != nil {
		return out, err
	}
	deny := func(code Fault) (Session, error) {
		if e := audit(ctx, tx, requestID, "identity."+action, "denied", &c.UserID, &c.OrganizationID, &c.SessionID); e != nil {
			return out, e
		}
		if e := tx.Commit(ctx); e != nil {
			return out, e
		}
		return out, code
	}
	now := time.Now().UTC()
	if revoked != nil && action == "logout" {
		out.IdentityContext = c
		return out, tx.Commit(ctx)
	}
	if revoked != nil || c.RevocationVersion != userVersion {
		return deny(Revoked)
	}
	if !c.ExpiresAt.After(now) {
		return deny(Unauthenticated)
	}
	if userStatus != "active" || orgStatus != "active" || !active || role != "admin" {
		return deny(Denied)
	}
	if lockedUntil != nil && lockedUntil.After(now) {
		return deny(Locked)
	}
	c.IssuedAt = c.IssuedAt.UTC()
	c.ExpiresAt = c.ExpiresAt.UTC()
	c.Roles = []string{"admin"}
	c.Scopes = append([]string(nil), adminScopes...)
	out.IdentityContext = c
	switch action {
	case "refresh":
		value, hash, e := newToken()
		if e != nil {
			return out, e
		}
		out.AccessToken = value
		out.IssuedAt = now
		out.ExpiresAt = now.Add(s.SessionTTL)
		if _, err = tx.Exec(ctx, "UPDATE station.sessions SET token_hash=$2,issued_at=$3,expires_at=$4 WHERE session_id=$1", c.SessionID, hash, now, out.ExpiresAt); err != nil {
			return out, err
		}
	case "logout":
		if _, err = tx.Exec(ctx, "UPDATE station.sessions SET revoked_at=$2 WHERE session_id=$1", c.SessionID, now); err != nil {
			return out, err
		}
	}
	if action == "refresh" || action == "logout" {
		if err = audit(ctx, tx, requestID, "identity."+action, "succeeded", &c.UserID, &c.OrganizationID, &c.SessionID); err != nil {
			return out, err
		}
	}
	return out, tx.Commit(ctx)
}
func (s *Service) Me(ctx context.Context, requestID, token string) (IdentityContext, error) {
	v, e := s.withSession(ctx, requestID, token, "me")
	return v.IdentityContext, e
}
func (s *Service) Refresh(ctx context.Context, requestID, token string) (Session, error) {
	return s.withSession(ctx, requestID, token, "refresh")
}
func (s *Service) Logout(ctx context.Context, requestID, token string) error {
	_, e := s.withSession(ctx, requestID, token, "logout")
	return e
}
