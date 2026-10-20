package operations

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

type ForwardRecoveryCommand struct {
	RequestID        string `json:"request_id"`
	IdempotencyKey   string `json:"idempotency_key"`
	OperationID      string `json:"operation_id"`
	ExpectedRevision int64  `json:"expected_revision"`
	TargetReleaseRef string `json:"target_release_ref"`
}
type ForwardRecovery struct {
	RecoveryID        string             `json:"recovery_id"`
	OperationID       string             `json:"operation_id"`
	ExpectedRevision  int64              `json:"expected_revision"`
	InstalledVersion  string             `json:"installed_version"`
	ParentInputDigest string             `json:"parent_input_digest"`
	Target            TransitionSnapshot `json:"target"`
	InputDigest       string             `json:"input_digest"`
	State             string             `json:"state"`
	Proof             json.RawMessage    `json:"proof,omitempty"`
}

// Field order is shared with Runtime; identities are read from the original binding.
type forwardBinding struct {
	RecoveryID        string             `json:"recovery_id"`
	ParentInputDigest string             `json:"parent_input_digest"`
	ExpectedRevision  int64              `json:"expected_revision"`
	InstalledVersion  string             `json:"installed_version"`
	Target            TransitionSnapshot `json:"target"`
}

func (r ForwardRecovery) digest() string {
	raw, _ := json.Marshal(forwardBinding{r.RecoveryID, r.ParentInputDigest, r.ExpectedRevision, r.InstalledVersion, r.Target})
	h := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(h[:])
}
func (c ForwardRecoveryCommand) valid() bool {
	id, err := uuid.Parse(c.RequestID)
	return err == nil && id.Version() == 4 && id.Variant() == uuid.RFC4122 && id.String() == c.RequestID && canonicalInstallation(c.OperationID) && c.ExpectedRevision > 0 && len(c.IdempotencyKey) >= 8 && len(c.IdempotencyKey) <= 128 && transitionName.MatchString(c.TargetReleaseRef)
}
func forwardRequestHash(c ForwardRecoveryCommand) []byte {
	c.RequestID = ""
	raw, _ := json.Marshal(c)
	h := sha256.Sum256(raw)
	return h[:]
}
func (s *Service) canRecover(actor identity.IdentityContext) bool {
	return actor.StationID == s.StationID && slices.Contains(actor.Roles, "admin") && slices.Contains(actor.Scopes, "app:read") && slices.Contains(actor.Scopes, "app:manage")
}
func readForward(ctx context.Context, q querier, actor identity.IdentityContext, station, id string) (ForwardRecovery, error) {
	var r ForwardRecovery
	var raw []byte
	err := q.QueryRow(ctx, `SELECT recovery_id::text,operation_id::text,expected_revision,installed_version,parent_input_digest,target_snapshot,input_digest,state FROM station.app_installation_forward_recoveries WHERE recovery_id=$1 AND user_id=$2 AND organization_id=$3 AND station_id=$4`, id, actor.UserID, actor.OrganizationID, station).Scan(&r.RecoveryID, &r.OperationID, &r.ExpectedRevision, &r.InstalledVersion, &r.ParentInputDigest, &raw, &r.InputDigest, &r.State)
	if err != nil {
		return r, err
	}
	if json.Unmarshal(raw, &r.Target) != nil || !r.Target.valid() || r.digest() != r.InputDigest {
		return r, ErrTransitionProgress
	}
	if r.State == "succeeded" {
		err = q.QueryRow(ctx, `SELECT proof FROM station.runtime_forward_recovery_proofs WHERE recovery_id=$1 AND input_digest=$2`, r.RecoveryID, r.InputDigest).Scan(&r.Proof)
	}
	return r, err
}
func (s *Service) GetForwardRecovery(ctx context.Context, a identity.IdentityContext, id string) (ForwardRecovery, error) {
	if !s.canRecover(a) {
		return ForwardRecovery{}, identity.Denied
	}
	if !canonicalInstallation(id) {
		return ForwardRecovery{}, pgx.ErrNoRows
	}
	return readForward(ctx, s.Pool, a, s.StationID, id)
}
func allowedWorldForward(parent dispatchTransition, target TransitionSnapshot) bool {
	return parent.Source.AppID == "world" && parent.Source.AppVersion == "0.1.4" && parent.Target.AppVersion == "0.1.5" && target.AppID == "world" && target.AppVersion == "0.1.6" && parent.Source.ReleaseKind == "dashboard-release" && parent.Target.ReleaseKind == "dashboard-release" && target.ReleaseKind == "dashboard-release" && target.valid() && parent.installationState() == "original" && slices.Contains([]string{"restoring", "recovery_required"}, parent.Phase) && slices.Contains([]string{"running", "recovery_required"}, parent.Status)
}
func lockForwardParent(ctx context.Context, pool *pgxpool.Pool, station, operation string) (*pgxpool.Conn, func(), error) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	key := "core-transition:" + station + ":" + operation
	var locked bool
	if err = conn.QueryRow(ctx, `SELECT pg_try_advisory_lock(hashtextextended($1,0))`, key).Scan(&locked); err != nil || !locked {
		if err != nil {
			discardTransitionConnection(conn)
		}
		conn.Release()
		if err == nil {
			err = ErrTransitionBusy
		}
		return nil, nil, err
	}
	release := func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var ok bool
		if err := conn.QueryRow(cleanup, `SELECT pg_advisory_unlock(hashtextextended($1,0))`, key).Scan(&ok); err != nil || !ok {
			discardTransitionConnection(conn)
		}
		conn.Release()
	}
	return conn, release, nil
}
func (s *Service) SubmitForwardRecovery(ctx context.Context, a identity.IdentityContext, c ForwardRecoveryCommand) (ForwardRecovery, error) {
	if !s.canRecover(a) {
		return ForwardRecovery{}, identity.Denied
	}
	if !c.valid() {
		return ForwardRecovery{}, identity.Invalid
	}
	conn, release, err := lockForwardParent(ctx, s.Pool, s.StationID, c.OperationID)
	if err != nil {
		return ForwardRecovery{}, err
	}
	defer release()
	r, err := s.acceptForward(ctx, conn, a, c)
	if err != nil || r.State != "pending" {
		return r, err
	}
	if s.ForwardRecoveryRuntime == nil {
		return r, ErrTransitionProgress
	}
	step := TransitionStep{r.RecoveryID, uuid.NewString(), r.InputDigest, "verify_forward"}
	out, err := s.ForwardRecoveryRuntime.ReconcileTransition(ctx, step)
	if err != nil {
		return r, err
	}
	if out.TransitionStep != step || !slices.Contains([]string{"verified", "rejected"}, out.Phase) {
		return r, ErrTransitionProgress
	}
	return s.finishForward(ctx, conn, a, r, out.Phase == "verified")
}
func (s *Service) acceptForward(ctx context.Context, conn *pgxpool.Conn, a identity.IdentityContext, c ForwardRecoveryCommand) (ForwardRecovery, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return ForwardRecovery{}, err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "forward:"+a.UserID+":"+a.OrganizationID+":"+s.StationID+":"+c.IdempotencyKey); err != nil {
		return ForwardRecovery{}, err
	}
	var id string
	var hash []byte
	err = tx.QueryRow(ctx, `SELECT recovery_id::text,request_hash FROM station.app_installation_forward_recoveries WHERE user_id=$1 AND organization_id=$2 AND station_id=$3 AND idempotency_key=$4`, a.UserID, a.OrganizationID, s.StationID, c.IdempotencyKey).Scan(&id, &hash)
	if err == nil {
		if string(hash) != string(forwardRequestHash(c)) {
			return ForwardRecovery{}, identity.Conflict
		}
		r, e := readForward(ctx, tx, a, s.StationID, id)
		if e != nil {
			return r, e
		}
		return r, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return ForwardRecovery{}, err
	}
	parent, err := readDispatchTransition(ctx, tx, s.StationID, c.OperationID)
	if err != nil {
		return ForwardRecovery{}, err
	}
	if parent.UserID != a.UserID || parent.OrganizationID != a.OrganizationID {
		return ForwardRecovery{}, pgx.ErrNoRows
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.StationID+":"+parent.Source.AppID); err != nil {
		return ForwardRecovery{}, err
	}
	if parent.Installed.Revision != c.ExpectedRevision || !s.hasTransitionReleases() {
		return ForwardRecovery{}, identity.Conflict
	}
	// Source/target material is verified through the normal immutable release registry.
	source, target, err := s.transitionReleaseReader(ctx, a, parent.Installed, c.TargetReleaseRef)
	if err != nil {
		return ForwardRecovery{}, ErrTransitionRelease
	}
	sourceBytes, _ := json.Marshal(source)
	parentBytes, _ := json.Marshal(parent.Source)
	if string(sourceBytes) != string(parentBytes) || !allowedWorldForward(parent, target) {
		return ForwardRecovery{}, identity.Conflict
	}
	var busy bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.app_operations WHERE station_id=$1 AND app_id=$2 AND status IN ('accepted','running')) OR EXISTS(SELECT 1 FROM station.app_installation_forward_recoveries WHERE operation_id=$3 AND state='pending')`, s.StationID, parent.Source.AppID, parent.OperationID).Scan(&busy); err != nil {
		return ForwardRecovery{}, err
	}
	if busy {
		return ForwardRecovery{}, identity.Conflict
	}
	if err = retainTransitionReferences(ctx, tx, source, target); err != nil {
		return ForwardRecovery{}, err
	}
	r := ForwardRecovery{RecoveryID: uuid.Must(uuid.NewV7()).String(), OperationID: c.OperationID, ExpectedRevision: c.ExpectedRevision, InstalledVersion: parent.Installed.AppVersion, ParentInputDigest: parent.inputDigest(), Target: target, State: "pending"}
	r.InputDigest = r.digest()
	raw, _ := json.Marshal(target)
	_, err = tx.Exec(ctx, `INSERT INTO station.app_installation_forward_recoveries(recovery_id,operation_id,user_id,organization_id,station_id,request_id,idempotency_key,request_hash,expected_revision,installed_version,parent_input_digest,target_snapshot,input_digest,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'pending')`, r.RecoveryID, r.OperationID, a.UserID, a.OrganizationID, s.StationID, c.RequestID, c.IdempotencyKey, forwardRequestHash(c), r.ExpectedRevision, r.InstalledVersion, r.ParentInputDigest, raw, r.InputDigest)
	if err != nil {
		return r, err
	}
	if err = transitionAudit(ctx, tx, parent, "forward_recovery.accepted"); err != nil {
		return r, err
	}
	return r, tx.Commit(ctx)
}
func (s *Service) finishForward(ctx context.Context, conn *pgxpool.Conn, a identity.IdentityContext, r ForwardRecovery, verified bool) (ForwardRecovery, error) {
	tx, err := conn.Begin(ctx)
	if err != nil {
		return r, err
	}
	defer tx.Rollback(context.Background())
	parent, err := readDispatchTransition(ctx, tx, s.StationID, r.OperationID)
	if err != nil {
		return r, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.StationID+":"+parent.Source.AppID); err != nil {
		return r, err
	}
	if _, err = tx.Exec(ctx, `SELECT installation_id FROM station.app_installations WHERE installation_id=$1 FOR UPDATE`, parent.InstallationID); err != nil {
		return r, err
	}
	if _, err = tx.Exec(ctx, `SELECT recovery_id FROM station.app_installation_forward_recoveries WHERE recovery_id=$1 FOR UPDATE`, r.RecoveryID); err != nil {
		return r, err
	}
	current, err := readForward(ctx, tx, a, s.StationID, r.RecoveryID)
	if err != nil {
		return r, err
	}
	parent, err = readDispatchTransition(ctx, tx, s.StationID, r.OperationID)
	if err != nil {
		return r, err
	}
	if current.State != "pending" || current.InputDigest != r.InputDigest || parent.inputDigest() != r.ParentInputDigest || parent.Installed.Revision != r.ExpectedRevision || !allowedWorldForward(parent, r.Target) {
		return r, identity.Conflict
	}
	state := "failed"
	if verified {
		var proof []byte
		if err = tx.QueryRow(ctx, `SELECT proof FROM station.runtime_forward_recovery_proofs WHERE recovery_id=$1 AND input_digest=$2`, r.RecoveryID, r.InputDigest).Scan(&proof); err != nil {
			return r, ErrTransitionProgress
		}
		tag, e := tx.Exec(ctx, `UPDATE station.app_installations SET app_version=$2,revision=revision+1,updated_at=now() WHERE installation_id=$1 AND revision=$3 AND app_version=$4 AND state='installed'`, parent.InstallationID, r.Target.AppVersion, r.ExpectedRevision, r.InstalledVersion)
		if e != nil {
			return r, e
		}
		if tag.RowsAffected() != 1 {
			return r, identity.Conflict
		}
		if _, err = tx.Exec(ctx, `UPDATE station.app_installation_transitions SET status='failed',phase='completed',updated_at=now() WHERE operation_id=$1`, r.OperationID); err != nil {
			return r, err
		}
		state = "succeeded"
	}
	if _, err = tx.Exec(ctx, `UPDATE station.app_installation_forward_recoveries SET state=$2,updated_at=now() WHERE recovery_id=$1`, r.RecoveryID, state); err != nil {
		return r, err
	}
	if err = transitionAudit(ctx, tx, parent, "forward_recovery."+state); err != nil {
		return r, err
	}
	out, err := readForward(ctx, tx, a, s.StationID, r.RecoveryID)
	if err != nil {
		return r, err
	}
	return out, tx.Commit(ctx)
}
