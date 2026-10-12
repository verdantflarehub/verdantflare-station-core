package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

var ErrTransitionProgress = errors.New("TRANSITION_PROGRESS_UNAVAILABLE")
var ErrTransitionBusy = errors.New("TRANSITION_COORDINATOR_BUSY")

// TransitionRuntime is a private authenticated transport, never supplied by an
// App or user request. The Runtime must re-read the accepted Core binding and
// independently prove the workload and maintenance state on every call.
type TransitionRuntime interface {
	ReconcileTransition(context.Context, TransitionStep) (TransitionProgress, error)
}

type TransitionStep struct {
	OperationID string `json:"operation_id"`
	RequestID   string `json:"request_id"`
	InputDigest string `json:"input_digest"`
	Mode        string `json:"mode"`
}

type TransitionProgress struct {
	TransitionStep
	Phase string `json:"phase"`
}

// The field order and JSON shape match Runtime ManagementBinding. Neither a
// credential nor the caller's chosen release reference can replace this input.
type transitionBinding struct {
	OperationID      string             `json:"operation_id"`
	InstallationID   string             `json:"installation_id"`
	StationID        string             `json:"station_id"`
	OrganizationID   string             `json:"organization_id"`
	UserID           string             `json:"user_id"`
	ExpectedRevision int64              `json:"expected_revision"`
	Source           TransitionSnapshot `json:"source"`
	Target           TransitionSnapshot `json:"target"`
}

type dispatchTransition struct {
	Transition
	UserID, OrganizationID string
	Installed              Installation
}

func (r dispatchTransition) inputDigest() string {
	raw, _ := json.Marshal(transitionBinding{r.OperationID, r.InstallationID, r.Installed.StationID,
		r.OrganizationID, r.UserID, r.ExpectedRevision, r.Source, r.Target})
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func readDispatchTransition(ctx context.Context, db interface {
	QueryRow(context.Context, string, ...any) pgx.Row
}, station, operation string) (dispatchTransition, error) {
	var r dispatchTransition
	if err := db.QueryRow(ctx, `SELECT user_id::text,organization_id::text FROM station.app_installation_transitions WHERE operation_id=$1 AND station_id=$2`, operation, station).Scan(&r.UserID, &r.OrganizationID); err != nil {
		return r, err
	}
	var err error
	r.Transition, err = readTransition(ctx, db, identity.IdentityContext{UserID: r.UserID, OrganizationID: r.OrganizationID}, station, operation)
	if err != nil {
		return r, err
	}
	r.Installed, err = scanInstallation(db.QueryRow(ctx, `SELECT `+installationColumns+` FROM station.app_installations WHERE installation_id=$1 AND station_id=$2 AND organization_id=$3`, r.InstallationID, station, r.OrganizationID))
	if err != nil {
		return r, err
	}
	command := TransitionCommand{Action: r.Action, SourceVersion: r.Source.AppVersion, TargetReleaseRef: r.Target.ReleaseRef}
	original := r.Installed
	original.AppVersion = r.Source.AppVersion
	if r.ExpectedRevision > math.MaxInt64-2 || !compatibleTransition(command, original, r.Source, r.Target) {
		return r, ErrTransitionProgress
	}
	return r, nil
}

func (r dispatchTransition) installationState() string {
	i := r.Installed
	if i.State != "installed" || i.AppID != r.Source.AppID || i.Revision < 1 {
		return ""
	}
	switch {
	case i.AppVersion == r.Source.AppVersion && i.Revision == r.ExpectedRevision:
		return "original"
	case i.AppVersion == r.Target.AppVersion && i.Revision == r.ExpectedRevision+1:
		return "target"
	case i.AppVersion == r.Source.AppVersion && i.Revision == r.ExpectedRevision+2:
		return "restored"
	}
	return ""
}

// DispatchTransition advances one durable barrier. It intentionally does not
// expose or enable a public command route. A lost external response leaves the
// original phase available to the same operation, including during release.
func (s *Service) DispatchTransition(ctx context.Context, operation string, runtime TransitionRuntime) (Transition, error) {
	if s == nil || s.Pool == nil || runtime == nil || !canonicalInstallation(operation) || !canonicalInstallation(s.StationID) {
		return Transition{}, ErrTransitionProgress
	}
	conn, err := s.Pool.Acquire(ctx)
	if err != nil {
		return Transition{}, err
	}
	defer conn.Release()
	key := "core-transition:" + s.StationID + ":" + operation
	var locked bool
	if err := conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, key).Scan(&locked); err != nil {
		discardTransitionConnection(conn)
		return Transition{}, err
	}
	if !locked {
		return Transition{}, ErrTransitionBusy
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var unlocked bool
		if err := conn.QueryRow(cleanup, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, key).Scan(&unlocked); err != nil || !unlocked {
			discardTransitionConnection(conn)
		}
	}()
	r, err := readDispatchTransition(ctx, conn, s.StationID, operation)
	if err != nil {
		return Transition{}, err
	}
	if slices.Contains([]string{"succeeded", "failed"}, r.Status) {
		return r.Transition, nil
	}
	installed := r.installationState()
	if installed == "" || (slices.Contains([]string{"accepted", "checking", "sealing", "applying", "verifying", "publishing"}, r.Phase) && installed != "original") {
		return r.Transition, ErrTransitionProgress
	}
	switch r.Phase {
	case "accepted":
		return s.commitTransition(ctx, conn, r, "running", "checking", "")
	case "recovery_required":
		return s.commitTransition(ctx, conn, r, "running", "restoring", "")
	}
	mode := "prepare"
	if r.Phase == "restoring" {
		mode = "restore"
	} else if r.Phase == "releasing" {
		mode = "release_source"
		if installed == "target" {
			mode = "release_target"
		}
	}
	step := TransitionStep{operation, uuid.NewString(), r.inputDigest(), mode}
	progress, err := runtime.ReconcileTransition(ctx, step)
	if err != nil {
		return r.Transition, err
	}
	if progress.TransitionStep != step {
		return r.Transition, ErrTransitionProgress
	}
	status, phase, version := "running", r.Phase, ""
	switch mode {
	case "release_source", "release_target":
		// Once release could have happened, new work could be admitted. Neither
		// an error nor a recovery response permits swapping out that workload.
		if progress.Phase != "released" {
			return r.Transition, ErrTransitionProgress
		}
		status, phase = "failed", "completed"
		if mode == "release_target" {
			status = "succeeded"
		}
	case "restore":
		switch progress.Phase {
		case "restored":
			phase = "releasing"
			if installed == "target" {
				version = r.Source.AppVersion
			}
		case "restoring":
		case "recovery_required":
			status, phase = "recovery_required", "recovery_required"
		default:
			return r.Transition, ErrTransitionProgress
		}
	case "prepare":
		switch progress.Phase {
		case "recovery_required":
			status, phase = "recovery_required", "recovery_required"
		case "rejected":
			// Runtime may certify rejection only before any seal or mutation.
			if r.Phase != "checking" && r.Phase != "sealing" {
				return r.Transition, ErrTransitionProgress
			}
			status, phase = "failed", "completed"
		case "prepared":
			phase = "publishing"
			if r.Phase == "publishing" {
				version, phase = r.Target.AppVersion, "releasing"
			}
		default:
			stages := []string{"checking", "sealing", "applying", "verifying", "publishing"}
			current, next := slices.Index(stages, r.Phase), slices.Index(stages[:4], progress.Phase)
			if current < 0 || next < current {
				return r.Transition, ErrTransitionProgress
			}
			phase = progress.Phase
		}
	default:
		return r.Transition, ErrTransitionProgress
	}
	return s.commitTransition(ctx, conn, r, status, phase, version)
}

func discardTransitionConnection(conn *pgxpool.Conn) {
	cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = conn.Conn().Close(cleanup)
}

// No HTTP or Kubernetes calls occur inside this transaction. Publication,
// compensation, terminal status, and the matching audit are atomic.
func (s *Service) commitTransition(ctx context.Context, conn *pgxpool.Conn, prior dispatchTransition, status, phase, version string) (Transition, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return prior.Transition, err
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.StationID+":"+prior.Source.AppID); err != nil {
		return prior.Transition, err
	}
	if _, err = tx.Exec(ctx, `SELECT operation_id FROM station.app_installation_transitions WHERE operation_id=$1 FOR UPDATE`, prior.OperationID); err != nil {
		return prior.Transition, err
	}
	if _, err = tx.Exec(ctx, `SELECT installation_id FROM station.app_installations WHERE installation_id=$1 FOR UPDATE`, prior.InstallationID); err != nil {
		return prior.Transition, err
	}
	current, err := readDispatchTransition(ctx, tx, s.StationID, prior.OperationID)
	if err != nil {
		return prior.Transition, err
	}
	if current.Status != prior.Status || current.Phase != prior.Phase || current.inputDigest() != prior.inputDigest() || current.Installed != prior.Installed {
		return prior.Transition, identity.Conflict
	}
	if version != "" {
		updated, err := tx.Exec(ctx, `UPDATE station.app_installations SET app_version=$2,revision=revision+1,updated_at=now() WHERE installation_id=$1 AND app_version=$3 AND revision=$4 AND state='installed'`, current.InstallationID, version, current.Installed.AppVersion, current.Installed.Revision)
		if err != nil {
			return prior.Transition, err
		}
		if updated.RowsAffected() != 1 {
			return prior.Transition, identity.Conflict
		}
		outcome := "published"
		if version == current.Source.AppVersion {
			outcome = "restored"
		}
		if err := transitionAudit(ctx, tx, current, outcome); err != nil {
			return prior.Transition, err
		}
	}
	if phase == "completed" {
		if err := transitionAudit(ctx, tx, current, status); err != nil {
			return prior.Transition, err
		}
	}
	if _, err := tx.Exec(ctx, `UPDATE station.app_installation_transitions SET status=$2,phase=$3,updated_at=now() WHERE operation_id=$1`, current.OperationID, status, phase); err != nil {
		return prior.Transition, err
	}
	actor := identity.IdentityContext{UserID: current.UserID, OrganizationID: current.OrganizationID}
	out, err := readTransition(ctx, tx, actor, s.StationID, current.OperationID)
	if err != nil {
		return prior.Transition, err
	}
	return out, tx.Commit(ctx)
}

func transitionAudit(ctx context.Context, tx pgx.Tx, r dispatchTransition, result string) error {
	// Accepted session identity is retained in the acceptance event. Completion
	// is a service action; never invent a new active user session for recovery.
	// outcome records successful persistence of this fact; the action suffix
	// distinguishes a failed transition from a successful upgrade.
	_, err := tx.Exec(ctx, `INSERT INTO station.audit_events(event_id,request_id,user_id,organization_id,action,outcome) VALUES($1,$2,$3,$4,$5,'succeeded')`, uuid.Must(uuid.NewV7()), r.RequestID, r.UserID, r.OrganizationID, "app."+r.Action+"."+result)
	return err
}
