package operations

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"time"
)

type Command struct {
	RequestID      string `json:"request_id"`
	IdempotencyKey string `json:"idempotency_key"`
	OrganizationID string `json:"organization_id"`
	StationID      string `json:"station_id"`
	AppID          string `json:"app_id"`
	AppVersion     string `json:"app_version"`
	Action         string `json:"action"`
}
type Operation struct {
	RequestID      string `json:"request_id"`
	OperationID    string `json:"operation_id"`
	OrganizationID string `json:"organization_id"`
	StationID      string `json:"station_id"`
	AppID          string `json:"app_id"`
	AppVersion     string `json:"app_version"`
	Action         string `json:"action"`
	Status         string `json:"status"`
	Phase          string `json:"phase"`
	Download       struct {
		DownloadedBytes int64    `json:"downloaded_bytes"`
		TotalBytes      *int64   `json:"total_bytes"`
		Percent         *float64 `json:"percent"`
	} `json:"download"`
	Error     any       `json:"error"`
	UpdatedAt time.Time `json:"updated_at"`
}
type Service struct {
	Pool      *pgxpool.Pool
	Catalog   *catalog.Service
	StationID string
}

func (s *Service) Submit(ctx context.Context, u identity.IdentityContext, c Command) (Operation, error) {
	var o Operation
	if c.StationID != s.StationID || c.OrganizationID != u.OrganizationID || len(c.IdempotencyKey) < 8 || len(c.IdempotencyKey) > 128 {
		return o, identity.Invalid
	}
	if c.Action != "install" && c.Action != "start" && c.Action != "stop" && c.Action != "restart" && c.Action != "delete" {
		return o, identity.Invalid
	}
	a, ok := s.Catalog.Get(ctx, c.AppID)
	if !ok || a.Version != c.AppVersion {
		return o, identity.Invalid
	}
	raw, _ := json.Marshal(c)
	sum := sha256.Sum256(raw)
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return o, e
	}
	defer tx.Rollback(ctx)
	var id, old []byte
	var status, phase string
	var at time.Time
	e = tx.QueryRow(ctx, `SELECT operation_id,request_hash,status,phase,updated_at FROM station.app_operations WHERE user_id=$1 AND organization_id=$2 AND station_id=$3 AND idempotency_key=$4`, u.UserID, u.OrganizationID, s.StationID, c.IdempotencyKey).Scan(&id, &old, &status, &phase, &at)
	if e == nil {
		if string(old) != string(sum[:]) {
			return o, identity.Conflict
		}
		o = Operation{c.RequestID, uuid.UUID(id).String(), u.OrganizationID, s.StationID, c.AppID, c.AppVersion, c.Action, status, phase, struct {
			DownloadedBytes int64    `json:"downloaded_bytes"`
			TotalBytes      *int64   `json:"total_bytes"`
			Percent         *float64 `json:"percent"`
		}{}, nil, at}
		return o, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return o, e
	}
	id = uuid.Must(uuid.NewV7()).NodeID()
	oid := uuid.Must(uuid.NewV7())
	now := time.Now().UTC()
	phase = "checking"
	if c.Action == "stop" || c.Action == "delete" {
		phase = "stopping"
	}
	if c.Action == "restart" {
		phase = "starting"
	}
	_, e = tx.Exec(ctx, `INSERT INTO station.app_operations(operation_id,request_id,user_id,organization_id,station_id,app_id,app_version,action,status,phase,idempotency_key,request_hash) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'accepted',$9,$10,$11)`, oid, c.RequestID, u.UserID, u.OrganizationID, s.StationID, c.AppID, c.AppVersion, c.Action, phase, c.IdempotencyKey, sum[:])
	if e != nil {
		return o, e
	}
	o = Operation{c.RequestID, oid.String(), u.OrganizationID, s.StationID, c.AppID, c.AppVersion, c.Action, "accepted", phase, struct {
		DownloadedBytes int64    `json:"downloaded_bytes"`
		TotalBytes      *int64   `json:"total_bytes"`
		Percent         *float64 `json:"percent"`
	}{}, nil, now}
	return o, tx.Commit(ctx)
}
