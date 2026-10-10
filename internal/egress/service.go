package egress

import (
	"context"
	"crypto/cipher"
	"encoding/json"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

type Service struct {
	Pool      *pgxpool.Pool
	StationID string
	cipher    cipher.AEAD
	Prober    *Prober
	slots     chan struct{}
}

func New(pool *pgxpool.Pool, station string, key []byte, p *Prober) (*Service, error) {
	if len(key) != 32 {
		return nil, errors.New("egress encryption key must be 32 bytes")
	}
	a, e := newCipher(key)
	if e != nil {
		return nil, e
	}
	return &Service{pool, station, a, p, make(chan struct{}, 4)}, nil
}
func (s *Service) aad(org, id string) string { return s.StationID + ":" + org + ":" + id }

const columns = `proxy_id::text,name,protocol,host,port,status,tag,note,expires_at,credentials,assigned_apps,revision,probe,COALESCE(probe_until>now(),false),updated_at`

func (s *Service) scan(row pgx.Row, org string) (Endpoint, error) {
	var p Endpoint
	var raw []byte
	e := row.Scan(&p.ID, &p.Name, &p.Protocol, &p.Host, &p.Port, &p.Status, &p.Tag, &p.Note, &p.ExpiresAt, &p.credentials, &p.AssignedApps, &p.Revision, &raw, &p.Testing, &p.UpdatedAt)
	if errors.Is(e, pgx.ErrNoRows) {
		return p, ErrNotFound
	}
	if e != nil {
		return p, e
	}
	c, e := unseal(s.cipher, p.credentials, s.aad(org, p.ID))
	if e != nil {
		return p, e
	}
	p.AuthConfigured = c.Username != ""
	if raw != nil {
		if e = json.Unmarshal(raw, &p.Probe); e != nil {
			return p, e
		}
	}
	if p.AssignedApps == nil {
		p.AssignedApps = []string{}
	}
	return p, nil
}
func (s *Service) List(ctx context.Context, u identity.IdentityContext) ([]Endpoint, error) {
	if e := authorize(u, s.StationID); e != nil {
		return nil, e
	}
	rows, e := s.Pool.Query(ctx, `SELECT `+columns+` FROM station.egress_proxies WHERE station_id=$1 AND organization_id=$2 ORDER BY created_at DESC`, s.StationID, u.OrganizationID)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []Endpoint{}
	for rows.Next() {
		p, e := s.scan(rows, u.OrganizationID)
		if e != nil {
			return nil, e
		}
		out = append(out, p)
	}
	return out, rows.Err()
}
func (s *Service) Get(ctx context.Context, u identity.IdentityContext, id string) (Endpoint, error) {
	if e := authorize(u, s.StationID); e != nil {
		return Endpoint{}, e
	}
	if _, e := uuid.Parse(id); e != nil {
		return Endpoint{}, ErrNotFound
	}
	return s.scan(s.Pool.QueryRow(ctx, `SELECT `+columns+` FROM station.egress_proxies WHERE proxy_id=$1 AND station_id=$2 AND organization_id=$3`, id, s.StationID, u.OrganizationID), u.OrganizationID)
}
func translate(err error) error {
	var e *pgconn.PgError
	if errors.As(err, &e) && e.Code == "23505" {
		return ErrConflict
	}
	return err
}
func audit(ctx context.Context, tx pgx.Tx, u identity.IdentityContext, action string) error {
	_, e := tx.Exec(ctx, `INSERT INTO station.audit_events(event_id,request_id,user_id,organization_id,session_id,action,outcome) VALUES($1,$2,$3,$4,$5,$6,'succeeded')`, uuid.Must(uuid.NewV7()).String(), u.RequestID, u.UserID, u.OrganizationID, u.SessionID, "egress."+action)
	return e
}
func (s *Service) Save(ctx context.Context, u identity.IdentityContext, id string, in Input) (Endpoint, error) {
	var out Endpoint
	if e := authorize(u, s.StationID); e != nil {
		return out, e
	}
	expiry, e := normalize(&in)
	if e != nil {
		return out, e
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return out, e
	}
	defer tx.Rollback(context.Background())
	c := Credentials{}
	status := "pending"
	oldRevision := int64(0)
	action := "create"
	if id != "" {
		if _, e = uuid.Parse(id); e != nil {
			return out, ErrNotFound
		}
		old, e := s.scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM station.egress_proxies WHERE proxy_id=$1 AND station_id=$2 AND organization_id=$3 FOR UPDATE`, id, s.StationID, u.OrganizationID), u.OrganizationID)
		if e != nil {
			return out, e
		}
		if in.Revision != old.Revision {
			return out, ErrConflict
		}
		oldRevision = old.Revision
		c, e = unseal(s.cipher, old.credentials, s.aad(u.OrganizationID, id))
		if e != nil {
			return out, e
		}
		if old.Status == "off" {
			status = "off"
		}
		action = "update"
	} else {
		if in.Revision != 0 {
			return out, identity.Invalid
		}
		id = uuid.Must(uuid.NewV7()).String()
	}
	if in.ClearAuth {
		c = Credentials{}
	}
	if in.Username != nil {
		c.Username = *in.Username
	}
	if in.Password != nil {
		c.Password = *in.Password
	}
	if !validCredentials(c) {
		return out, identity.Invalid
	}
	if in.Status != "" {
		status = in.Status
	}
	encrypted, e := seal(s.cipher, c, s.aad(u.OrganizationID, id))
	if e != nil {
		return out, e
	}
	if oldRevision == 0 {
		_, e = tx.Exec(ctx, `INSERT INTO station.egress_proxies(proxy_id,station_id,organization_id,name,protocol,host,port,credentials,endpoint_key,status,tag,note,expires_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)`, id, s.StationID, u.OrganizationID, in.Name, in.Protocol, in.Host, in.Port, encrypted, endpointKey(in, c), status, in.Tag, in.Note, expiry)
	} else {
		_, e = tx.Exec(ctx, `UPDATE station.egress_proxies SET name=$4,protocol=$5,host=$6,port=$7,credentials=$8,endpoint_key=$9,status=$10,tag=$11,note=$12,expires_at=$13,revision=revision+1,probe=NULL,probe_token=NULL,probe_until=NULL,updated_at=now() WHERE proxy_id=$1 AND station_id=$2 AND organization_id=$3`, id, s.StationID, u.OrganizationID, in.Name, in.Protocol, in.Host, in.Port, encrypted, endpointKey(in, c), status, in.Tag, in.Note, expiry)
	}
	if e != nil {
		return out, translate(e)
	}
	if e = audit(ctx, tx, u, action); e != nil {
		return out, e
	}
	if e = tx.Commit(ctx); e != nil {
		return out, e
	}
	return s.Get(ctx, u, id)
}
func (s *Service) Delete(ctx context.Context, u identity.IdentityContext, id string) error {
	if e := authorize(u, s.StationID); e != nil {
		return e
	}
	if _, e := uuid.Parse(id); e != nil {
		return ErrNotFound
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return e
	}
	defer tx.Rollback(context.Background())
	p, e := s.scan(tx.QueryRow(ctx, `SELECT `+columns+` FROM station.egress_proxies WHERE proxy_id=$1 AND station_id=$2 AND organization_id=$3 FOR UPDATE`, id, s.StationID, u.OrganizationID), u.OrganizationID)
	if e != nil {
		return e
	}
	if len(p.AssignedApps) > 0 || p.Testing {
		return ErrConflict
	}
	if _, e = tx.Exec(ctx, `DELETE FROM station.egress_proxies WHERE proxy_id=$1`, id); e != nil {
		return e
	}
	if e = audit(ctx, tx, u, "delete"); e != nil {
		return e
	}
	return tx.Commit(ctx)
}
func (s *Service) Test(ctx context.Context, u identity.IdentityContext, id string) (Endpoint, error) {
	p, e := s.Get(ctx, u, id)
	if e != nil {
		return p, e
	}
	if p.Status == "off" || (p.ExpiresAt != nil && !p.ExpiresAt.After(time.Now())) {
		return p, ErrDisabled
	}
	select {
	case s.slots <- struct{}{}:
		defer func() { <-s.slots }()
	default:
		return p, ErrBusy
	}
	token := uuid.Must(uuid.NewV7()).String()
	tag, e := s.Pool.Exec(ctx, `UPDATE station.egress_proxies SET probe_token=$4,probe_until=now()+interval '65 seconds' WHERE proxy_id=$1 AND station_id=$2 AND organization_id=$3 AND revision=$5 AND status<>'off' AND (probe_until IS NULL OR probe_until<now())`, id, s.StationID, u.OrganizationID, token, p.Revision)
	if e != nil {
		return p, e
	}
	if tag.RowsAffected() != 1 {
		return p, ErrBusy
	}
	defer func() {
		clean, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, _ = s.Pool.Exec(clean, `UPDATE station.egress_proxies SET probe_token=NULL,probe_until=NULL WHERE proxy_id=$1 AND probe_token=$2`, id, token)
	}()
	c, e := unseal(s.cipher, p.credentials, s.aad(u.OrganizationID, id))
	if e != nil {
		return p, e
	}
	result := s.Prober.Run(ctx, p, c)
	if ctx.Err() != nil {
		return p, ctx.Err()
	}
	status := "pending"
	if result.Status == "passed" {
		status = "active"
	}
	raw, e := json.Marshal(result)
	if e != nil {
		return p, e
	}
	tag, e = s.Pool.Exec(ctx, `UPDATE station.egress_proxies SET probe=$4,status=$5,probe_token=NULL,probe_until=NULL,updated_at=now() WHERE proxy_id=$1 AND station_id=$2 AND organization_id=$3 AND probe_token=$6 AND revision=$7 AND status<>'off'`, id, s.StationID, u.OrganizationID, raw, status, token, p.Revision)
	if e != nil {
		return p, e
	}
	if tag.RowsAffected() != 1 {
		return p, ErrConflict
	}
	return s.Get(ctx, u, id)
}
