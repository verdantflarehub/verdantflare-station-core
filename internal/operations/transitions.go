package operations

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

// Separate from AppCommand v1. The managed gateway gates new acceptance on
// explicit startup configuration; durable replay remains available.
type TransitionCommand struct {
	RequestID        string `json:"request_id"`
	IdempotencyKey   string `json:"idempotency_key"`
	InstallationID   string `json:"installation_id"`
	ExpectedRevision int64  `json:"expected_revision"`
	SourceVersion    string `json:"source_version"`
	TargetReleaseRef string `json:"target_release_ref"`
	Action           string `json:"action"`
}

// Snapshot comes from a platform-controlled, verified release reader, never
// from a caller's body. Digests bind the original immutable release materials.
type TransitionSnapshot struct {
	AppID                    string   `json:"app_id"`
	AppVersion               string   `json:"app_version"`
	ReleaseRef               string   `json:"release_ref"`
	ReleaseDigest            string   `json:"release_digest"`
	ManifestDigest           string   `json:"manifest_digest"`
	TemplateDigest           string   `json:"template_digest"`
	DashboardDigest          string   `json:"dashboard_digest"`
	DataContract             string   `json:"data_contract"`
	DefaultWorkerVersion     string   `json:"default_worker_version"`
	CompatibleWorkerVersions []string `json:"compatible_worker_versions"`
	ReleaseKind              string   `json:"release_kind,omitempty"`
	AppImageDigest           string   `json:"app_image_digest,omitempty"`
}

type Transition struct {
	OperationID      string             `json:"operation_id"`
	InstallationID   string             `json:"installation_id"`
	RequestID        string             `json:"request_id"`
	Action           string             `json:"action"`
	ExpectedRevision int64              `json:"expected_revision"`
	Source           TransitionSnapshot `json:"source"`
	Target           TransitionSnapshot `json:"target"`
	Status           string             `json:"status"`
	Phase            string             `json:"phase"`
	CreatedAt        time.Time          `json:"created_at"`
	UpdatedAt        time.Time          `json:"updated_at"`
}

type TransitionReader func(context.Context, identity.IdentityContext, Installation, string) (TransitionSnapshot, TransitionSnapshot, error)

var transitionVersion = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
var transitionName = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]{0,127}$`)
var transitionDigest = regexp.MustCompile(`^sha256:[a-f0-9]{64}$`)

func validTransitionVersion(v string) bool { return len(v) <= 64 && transitionVersion.MatchString(v) }
func canonicalInstallation(id string) bool {
	v, err := uuid.Parse(id)
	return err == nil && v.Version() == 7 && v.Variant() == uuid.RFC4122 && v.String() == id
}
func (c TransitionCommand) valid() bool {
	return len(c.RequestID) > 0 && len(c.RequestID) <= 128 && len(c.IdempotencyKey) >= 8 && len(c.IdempotencyKey) <= 128 &&
		canonicalInstallation(c.InstallationID) && c.ExpectedRevision > 0 && validTransitionVersion(c.SourceVersion) &&
		transitionName.MatchString(c.TargetReleaseRef) && (c.Action == "upgrade" || c.Action == "rollback")
}
func (s TransitionSnapshot) valid() bool {
	if !transitionName.MatchString(s.AppID) || !validTransitionVersion(s.AppVersion) || !transitionName.MatchString(s.ReleaseRef) ||
		!transitionDigest.MatchString(s.ReleaseDigest) || !transitionDigest.MatchString(s.ManifestDigest) || !transitionDigest.MatchString(s.TemplateDigest) || !transitionDigest.MatchString(s.DashboardDigest) {
		return false
	}
	if s.ReleaseKind == "dashboard-release" {
		return s.DataContract == "" && s.DefaultWorkerVersion == "" && s.CompatibleWorkerVersions != nil && len(s.CompatibleWorkerVersions) == 0 && transitionDigest.MatchString(s.AppImageDigest)
	}
	if s.ReleaseKind != "" || s.AppImageDigest != "" || !transitionName.MatchString(s.DataContract) || len(s.CompatibleWorkerVersions) == 0 || len(s.CompatibleWorkerVersions) > 128 || !slices.Contains(s.CompatibleWorkerVersions, s.DefaultWorkerVersion) {
		return false
	}
	seen := map[string]bool{}
	for _, v := range s.CompatibleWorkerVersions {
		if !validTransitionVersion(v) || seen[v] {
			return false
		}
		seen[v] = true
	}
	return true
}
func compareTransitionVersions(a, b string) int {
	x, y := strings.Split(a, "."), strings.Split(b, ".")
	for i := range x {
		if len(x[i]) < len(y[i]) {
			return -1
		}
		if len(x[i]) > len(y[i]) {
			return 1
		}
		if x[i] < y[i] {
			return -1
		}
		if x[i] > y[i] {
			return 1
		}
	}
	return 0
}
func compatibleTransition(c TransitionCommand, installed Installation, source, target TransitionSnapshot) bool {
	if !source.valid() || !target.valid() || source.AppID != installed.AppID || target.AppID != installed.AppID ||
		source.AppVersion != installed.AppVersion || source.AppVersion != c.SourceVersion || target.ReleaseRef != c.TargetReleaseRef ||
		source.DataContract != target.DataContract || source.ReleaseKind != target.ReleaseKind || source.ReleaseRef == target.ReleaseRef {
		return false
	}
	direction := compareTransitionVersions(target.AppVersion, source.AppVersion)
	if (c.Action == "upgrade" && direction <= 0) || (c.Action == "rollback" && direction >= 0) {
		return false
	}
	for _, v := range source.CompatibleWorkerVersions {
		if !slices.Contains(target.CompatibleWorkerVersions, v) {
			return false
		}
	}
	return true
}
func transitionHash(c TransitionCommand) [32]byte {
	c.RequestID = ""
	raw, _ := json.Marshal(c)
	return sha256.Sum256(raw)
}

const transitionColumns = `operation_id::text,installation_id::text,request_id,action,expected_revision,source_snapshot,target_snapshot,status,phase,created_at,updated_at`

func readTransition(ctx context.Context, q querier, actor identity.IdentityContext, station, id string) (Transition, error) {
	var out Transition
	var source, target []byte
	err := q.QueryRow(ctx, `SELECT `+transitionColumns+` FROM station.app_installation_transitions WHERE operation_id=$1 AND user_id=$2 AND organization_id=$3 AND station_id=$4`, id, actor.UserID, actor.OrganizationID, station).Scan(
		&out.OperationID, &out.InstallationID, &out.RequestID, &out.Action, &out.ExpectedRevision, &source, &target, &out.Status, &out.Phase, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return out, err
	}
	if json.Unmarshal(source, &out.Source) != nil || json.Unmarshal(target, &out.Target) != nil || !out.Source.valid() || !out.Target.valid() {
		return Transition{}, errors.New("invalid saved transition snapshots")
	}
	return out, nil
}

func (s *Service) GetTransition(ctx context.Context, actor identity.IdentityContext, id string) (Transition, error) {
	if !s.canReadInstallations(actor) {
		return Transition{}, identity.Denied
	}
	if !canonicalInstallation(id) {
		return Transition{}, pgx.ErrNoRows
	}
	return readTransition(ctx, s.Pool, actor, s.StationID, id)
}

// SubmitTransition commits only acceptance and immutable input. It does not
// switch installation versions, drain App work, or execute Kubernetes changes.
func (s *Service) SubmitTransition(ctx context.Context, actor identity.IdentityContext, c TransitionCommand, reader TransitionReader) (Transition, error) {
	if actor.StationID != s.StationID || !slices.Contains(actor.Roles, "admin") || !slices.Contains(actor.Scopes, "app:manage") {
		return Transition{}, identity.Denied
	}
	if !c.valid() {
		return Transition{}, identity.Invalid
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return Transition{}, err
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "transition:"+actor.UserID+":"+actor.OrganizationID+":"+s.StationID+":"+c.IdempotencyKey); err != nil {
		return Transition{}, err
	}
	hash := transitionHash(c)
	var originalID string
	var originalHash []byte
	err = tx.QueryRow(ctx, `SELECT operation_id::text,request_hash FROM station.app_installation_transitions WHERE user_id=$1 AND organization_id=$2 AND station_id=$3 AND idempotency_key=$4`, actor.UserID, actor.OrganizationID, s.StationID, c.IdempotencyKey).Scan(&originalID, &originalHash)
	if err == nil {
		if string(originalHash) != string(hash[:]) {
			return Transition{}, identity.Conflict
		}
		out, e := readTransition(ctx, tx, actor, s.StationID, originalID)
		if e != nil {
			return Transition{}, e
		}
		return out, tx.Commit(ctx)
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return Transition{}, err
	}
	// Read the immutable App identity to acquire the same lock as AppCommand.
	var app string
	if err = tx.QueryRow(ctx, `SELECT app_id FROM station.app_installations WHERE installation_id=$1 AND station_id=$2 AND organization_id=$3`, c.InstallationID, s.StationID, actor.OrganizationID).Scan(&app); err != nil {
		return Transition{}, err
	}
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, s.StationID+":"+app); err != nil {
		return Transition{}, err
	}
	installed, err := scanInstallation(tx.QueryRow(ctx, `SELECT `+installationColumns+` FROM station.app_installations WHERE installation_id=$1 AND station_id=$2 AND organization_id=$3 FOR UPDATE`, c.InstallationID, s.StationID, actor.OrganizationID))
	if err != nil {
		return Transition{}, err
	}
	if installed.State != "installed" || installed.Revision != c.ExpectedRevision || installed.AppVersion != c.SourceVersion {
		return Transition{}, identity.Conflict
	}
	var busy bool
	if err = tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.app_operations WHERE station_id=$1 AND app_id=$2 AND status IN ('accepted','running')) OR EXISTS(SELECT 1 FROM station.app_installation_transitions WHERE installation_id=$3 AND status NOT IN ('succeeded','failed'))`, s.StationID, app, c.InstallationID).Scan(&busy); err != nil {
		return Transition{}, err
	}
	if busy {
		return Transition{}, identity.Conflict
	}
	if reader == nil && s.TransitionReleases != nil {
		reader = s.TransitionReleases.Read
	}
	if reader == nil {
		return Transition{}, errors.New("verified transition release reader unavailable")
	}
	source, target, err := reader(ctx, actor, installed, c.TargetReleaseRef)
	if err != nil {
		return Transition{}, err
	}
	if !compatibleTransition(c, installed, source, target) {
		return Transition{}, identity.Invalid
	}
	if err := retainTransitionReferences(ctx, tx, source, target); err != nil {
		return Transition{}, err
	}
	sourceRaw, _ := json.Marshal(source)
	targetRaw, _ := json.Marshal(target)
	id := uuid.Must(uuid.NewV7()).String()
	if _, err = tx.Exec(ctx, `INSERT INTO station.app_installation_transitions(operation_id,installation_id,user_id,organization_id,station_id,app_id,request_id,idempotency_key,request_hash,action,expected_revision,source_version,target_version,source_snapshot,target_snapshot,status,phase) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,'accepted','accepted')`,
		id, c.InstallationID, actor.UserID, actor.OrganizationID, s.StationID, app, c.RequestID, c.IdempotencyKey, hash[:], c.Action, c.ExpectedRevision, source.AppVersion, target.AppVersion, sourceRaw, targetRaw); err != nil {
		return Transition{}, err
	}
	if _, err = tx.Exec(ctx, `INSERT INTO station.audit_events(event_id,request_id,user_id,organization_id,session_id,action,outcome) VALUES($1,$2,$3,$4,$5,$6,'succeeded')`, uuid.Must(uuid.NewV7()), c.RequestID, actor.UserID, actor.OrganizationID, actor.SessionID, "app."+c.Action+".accepted"); err != nil {
		return Transition{}, err
	}
	out, err := readTransition(ctx, tx, actor, s.StationID, id)
	if err != nil {
		return Transition{}, err
	}
	return out, tx.Commit(ctx)
}

// Reuse the durable accepted snapshots as the immutable reference ledger.
// Failed and recovered operations still pin their release content. Locks are
// global to a release reference and acquired in order across installations.
func retainTransitionReferences(ctx context.Context, tx pgx.Tx, source, target TransitionSnapshot) error {
	snapshots := []TransitionSnapshot{source, target}
	slices.SortFunc(snapshots, func(a, b TransitionSnapshot) int { return strings.Compare(a.ReleaseRef, b.ReleaseRef) })
	for _, snapshot := range snapshots {
		if _, err := tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,0))`, "transition-release:"+snapshot.ReleaseRef); err != nil {
			return err
		}
		raw, err := json.Marshal(snapshot)
		if err != nil {
			return err
		}
		var changed bool
		if err := tx.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM station.app_installation_transitions
			WHERE (source_snapshot->>'release_ref'=$1 AND source_snapshot<>$2::jsonb)
			OR (target_snapshot->>'release_ref'=$1 AND target_snapshot<>$2::jsonb))`, snapshot.ReleaseRef, raw).Scan(&changed); err != nil {
			return err
		}
		if changed {
			return identity.Conflict
		}
	}
	return nil
}
