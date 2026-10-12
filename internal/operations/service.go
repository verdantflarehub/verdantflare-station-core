package operations

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
	"slices"
	"time"
)

type Command struct {
	ExpectedWorkloadUID string `json:"expected_workload_uid,omitempty"`
	RequestID           string `json:"request_id"`
	IdempotencyKey      string `json:"idempotency_key"`
	OrganizationID      string `json:"organization_id"`
	StationID           string `json:"station_id"`
	AppID               string `json:"app_id"`
	AppVersion          string `json:"app_version"`
	Action              string `json:"action"`
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
	transitionAdmissionEnabled bool
	Pool                       *pgxpool.Pool
	Catalog                    *catalog.Service
	AppStates                  *appstate.Service
	StationID                  string
	Releases                   *releasebinding.Source
	TransitionReleases         *TransitionReleases
	AppCenterRuntime           pb.AppCenterRuntimeClient
	AppCenterAdmissionSource   *AppCenterAdmissionSource
}

func (s *Service) Submit(ctx context.Context, u identity.IdentityContext, c Command) (Operation, error) {
	return s.submit(ctx, u, c, nil, nil, nil)
}
func (s *Service) submit(ctx context.Context, u identity.IdentityContext, c Command, preinstalled *PreinstalledCommand, constrained *AppCenterCommand, admission AppCenterAdmission) (Operation, error) {
	var o Operation
	if !slices.Contains(u.Roles, "admin") || !slices.Contains(u.Scopes, "app:manage") {
		return o, identity.Denied
	}
	if len(c.RequestID) == 0 || len(c.RequestID) > 128 {
		return o, identity.Invalid
	}
	if c.StationID != s.StationID || c.OrganizationID != u.OrganizationID || len(c.IdempotencyKey) < 8 || len(c.IdempotencyKey) > 128 {
		return o, identity.Invalid
	}
	if c.Action != "install" && c.Action != "start" && c.Action != "stop" && c.Action != "restart" && c.Action != "delete" && c.Action != "adopt" {
		return o, identity.Invalid
	}
	if c.Action == "adopt" {
		if _, e := uuid.Parse(c.ExpectedWorkloadUID); e != nil {
			return o, identity.Invalid
		}
	} else if c.ExpectedWorkloadUID != "" {
		return o, identity.Invalid
	}
	sum := commandHash(c, preinstalled)
	if constrained != nil {
		sum = appCenterHash(*constrained)
	}
	tx, e := s.Pool.Begin(ctx)
	if e != nil {
		return o, e
	}
	defer tx.Rollback(ctx)
	var old []byte
	var id, originalRequestID string
	var phase string
	_, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "idempotency:"+u.UserID+":"+u.OrganizationID+":"+s.StationID+":"+c.IdempotencyKey)
	if e != nil {
		return o, e
	}
	// Serialize submissions for an app so concurrent operations cannot both be accepted.
	_, e = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.StationID+":"+c.AppID)
	if e != nil {
		return o, e
	}
	e = tx.QueryRow(ctx, `SELECT operation_id::text,request_hash,request_id FROM station.app_operations WHERE user_id=$1 AND organization_id=$2 AND station_id=$3 AND idempotency_key=$4`, u.UserID, u.OrganizationID, s.StationID, c.IdempotencyKey).Scan(&id, &old, &originalRequestID)
	if e == nil {
		// Legacy digests included request_id; compare using the stored tracing ID.
		legacyCommand := c
		legacyCommand.RequestID = originalRequestID
		legacyRaw, _ := json.Marshal(legacyCommand)
		legacy := sha256.Sum256(legacyRaw)
		if string(old) != string(sum[:]) && (preinstalled != nil || constrained != nil || string(old) != string(legacy[:])) {
			return o, identity.Conflict
		}
		o, e = read(ctx, tx, u, s.StationID, id)
		if e != nil {
			return o, e
		}
		return o, tx.Commit(ctx)
	}
	if !errors.Is(e, pgx.ErrNoRows) {
		return o, e
	}
	var execution AppCenterExecution
	if constrained != nil {
		installed, err := validateAppCenterInstallation(ctx, tx, u, *constrained)
		if err != nil {
			return o, err
		}
		if admission == nil {
			return o, ErrAppCenterRelease
		}
		execution, err = admission(ctx, u, c, installed)
		if err != nil {
			return o, err
		}
		if err = validateAppCenterExecution(*constrained, execution); err != nil {
			return o, err
		}
	} else {
		if s.Catalog == nil {
			return o, identity.Invalid
		}
		a, ok := s.Catalog.Entry(c.AppID)
		if !ok || a.Version != c.AppVersion {
			return o, identity.Invalid
		}
	}
	if preinstalled != nil {
		if e = s.validatePreinstalled(ctx, tx, u, preinstalled); e != nil {
			return o, e
		}
	}
	var busy bool
	e = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.app_operations WHERE station_id=$1 AND app_id=$2 AND status IN ('accepted','running')) OR EXISTS(SELECT 1 FROM station.app_installation_transitions WHERE station_id=$1 AND app_id=$2 AND status NOT IN ('succeeded','failed'))`, s.StationID, c.AppID).Scan(&busy)
	if e != nil {
		return o, e
	}
	if busy {
		return o, identity.Conflict
	}
	installationID, e := reserveInstallation(ctx, tx, u, c)
	if e != nil {
		return o, e
	}
	oid := uuid.Must(uuid.NewV7())
	now := time.Now().UTC()
	phase = "checking"
	if c.Action == "stop" || c.Action == "delete" {
		phase = "stopping"
	}
	if c.Action == "restart" {
		phase = "starting"
	}
	_, e = tx.Exec(ctx, `INSERT INTO station.app_operations(operation_id,request_id,user_id,organization_id,station_id,app_id,app_version,action,status,phase,idempotency_key,request_hash,expected_workload_uid) VALUES($1,$2,$3,$4,$5,$6,$7,$8,'accepted',$9,$10,$11,$12)`, oid, c.RequestID, u.UserID, u.OrganizationID, s.StationID, c.AppID, c.AppVersion, c.Action, phase, c.IdempotencyKey, sum[:], c.ExpectedWorkloadUID)
	if e != nil {
		return o, e
	}
	if e = bindInstallationOperation(ctx, tx, installationID, oid.String()); e != nil {
		return o, e
	}
	if constrained != nil {
		if e = bindAppCenterInput(ctx, tx, oid.String(), *constrained, execution); e != nil {
			return o, e
		}
	}
	if preinstalled != nil {
		if e = bindPreinstalled(ctx, tx, oid.String(), installationID, preinstalled.Release); e != nil {
			return o, e
		}
	}
	_, e = tx.Exec(ctx, `INSERT INTO station.audit_events(event_id,request_id,user_id,organization_id,session_id,action,outcome) VALUES($1,$2,$3,$4,$5,$6,'succeeded')`, uuid.Must(uuid.NewV7()), c.RequestID, u.UserID, u.OrganizationID, u.SessionID, "app."+c.Action+".accepted")
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

// Get never reveals operations owned by another user or organization.
func (s *Service) Get(ctx context.Context, u identity.IdentityContext, id string) (Operation, error) {
	if !slices.Contains(u.Roles, "admin") || !slices.Contains(u.Scopes, "app:read") {
		return Operation{}, identity.Denied
	}
	if _, err := uuid.Parse(id); err != nil {
		return Operation{}, identity.Invalid
	}
	return read(ctx, s.Pool, u, s.StationID, id)
}

type querier interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}

func read(ctx context.Context, db querier, u identity.IdentityContext, stationID, id string) (Operation, error) {
	var o Operation
	var code, message *string
	err := db.QueryRow(ctx, `SELECT request_id,operation_id::text,organization_id::text,station_id::text,app_id,app_version,action,status,phase,downloaded_bytes,total_bytes,error_code,error_message,updated_at FROM station.app_operations WHERE operation_id=$1 AND user_id=$2 AND organization_id=$3 AND station_id=$4`, id, u.UserID, u.OrganizationID, stationID).Scan(&o.RequestID, &o.OperationID, &o.OrganizationID, &o.StationID, &o.AppID, &o.AppVersion, &o.Action, &o.Status, &o.Phase, &o.Download.DownloadedBytes, &o.Download.TotalBytes, &code, &message, &o.UpdatedAt)
	if err != nil {
		return o, err
	}
	if o.Download.TotalBytes != nil {
		percent := float64(0)
		if *o.Download.TotalBytes > 0 {
			percent = min(100, 100*float64(o.Download.DownloadedBytes)/float64(*o.Download.TotalBytes))
		}
		o.Download.Percent = &percent
	}
	if o.Status == "failed" {
		c, m := "INTERNAL", "Operation failed"
		if code != nil {
			c = *code
		}
		if message != nil {
			m = *message
		}
		o.Error = map[string]string{"code": c, "message": m, "request_id": o.RequestID}
	}
	return o, nil
}
