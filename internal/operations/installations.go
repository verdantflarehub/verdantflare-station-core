package operations

import (
	"context"
	"errors"
	"slices"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

// Installation describes the installed application, never an App's work instance.
// The existence of this record does not grant Dashboard publisher trust.
type Installation struct {
	InstallationID  string    `json:"installation_id"`
	AppID           string    `json:"app_id"`
	AppVersion      string    `json:"app_version"`
	StationID       string    `json:"station_id"`
	OrganizationID  string    `json:"organization_id"`
	State           string    `json:"state"`
	Revision        int64     `json:"revision"`
	LastOperationID string    `json:"last_operation_id"`
	CreatedAt       time.Time `json:"created_at"`
	UpdatedAt       time.Time `json:"updated_at"`
}

// reserveInstallation runs under Submit's Station/App advisory lock and transaction.
// Idempotent operation replay is resolved before this function is called.
func reserveInstallation(ctx context.Context, tx pgx.Tx, user identity.IdentityContext, c Command, trustedAdoption bool) (string, error) {
	var id, org, installedVersion, state string
	var bound bool
	err := tx.QueryRow(ctx, `SELECT installation_id::text,organization_id::text,app_version,state,EXISTS(SELECT 1 FROM station.preinstalled_installation_bindings b WHERE b.installation_id=station.app_installations.installation_id) FROM station.app_installations WHERE station_id=$1 AND app_id=$2 AND state <> 'removed' FOR UPDATE`, c.StationID, c.AppID).Scan(&id, &org, &installedVersion, &state, &bound)
	if err == nil {
		if org != user.OrganizationID || (installedVersion != c.AppVersion && !legacyVersionAdoption(trustedAdoption, c.Action, state, bound)) {
			return "", identity.Conflict
		}
		return id, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return "", err
	}
	if !slices.Contains([]string{"install", "start", "adopt"}, c.Action) {
		return "", nil
	}
	id = uuid.Must(uuid.NewV7()).String()
	_, err = tx.Exec(ctx, `INSERT INTO station.app_installations(installation_id,created_by,organization_id,station_id,app_id,app_version,state) VALUES($1,$2,$3,$4,$5,$6,'pending')`, id, user.UserID, user.OrganizationID, c.StationID, c.AppID, c.AppVersion)
	return id, err
}

func legacyVersionAdoption(trusted bool, action, state string, bound bool) bool {
	return trusted && action == "adopt" && state == "installed" && !bound
}

func bindInstallationOperation(ctx context.Context, tx pgx.Tx, installationID, operationID string) error {
	if installationID == "" {
		return nil
	}
	if _, err := tx.Exec(ctx, `UPDATE station.app_operations SET installation_id=$2 WHERE operation_id=$1`, operationID, installationID); err != nil {
		return err
	}
	_, err := tx.Exec(ctx, `UPDATE station.app_installations SET last_operation_id=$2,revision=revision+1,updated_at=now() WHERE installation_id=$1`, installationID, operationID)
	return err
}

func installationState(current, action, result string) string {
	if current == "removed" {
		return current
	}
	if result == "succeeded" {
		switch action {
		case "delete":
			return "removed"
		case "install", "start", "restart", "adopt", "stop":
			return "installed"
		}
	}
	if result == "failed" && current != "installed" {
		return "failed"
	}
	return current
}

// recordInstallationProgress shares the transaction that records Runtime progress.
// Legacy operations with no bound identity are intentionally not backfilled.
func recordInstallationProgress(ctx context.Context, tx pgx.Tx, operationID, action, result string) error {
	var id, state, installedVersion, observedVersion string
	var signedAdoption bool
	err := tx.QueryRow(ctx, `SELECT i.installation_id::text,i.state,i.app_version,o.app_version,EXISTS(SELECT 1 FROM station.preinstalled_operation_bindings b WHERE b.operation_id=o.operation_id) FROM station.app_installations i JOIN station.app_operations o ON o.installation_id=i.installation_id WHERE o.operation_id=$1 AND i.last_operation_id=o.operation_id FOR UPDATE OF i`, operationID).Scan(&id, &state, &installedVersion, &observedVersion, &signedAdoption)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil
	}
	if err != nil {
		return err
	}
	if installedVersion != observedVersion && result == "succeeded" {
		if !legacyVersionAdoption(signedAdoption, action, state, false) {
			return identity.Conflict
		}
		// The operation was admitted against the inventory revision and Runtime
		// verified the original workload. Bind only now, in this result transaction.
		bound, e := tx.Exec(ctx, `INSERT INTO station.preinstalled_installation_bindings(installation_id,binding) SELECT $2,binding FROM station.preinstalled_operation_bindings WHERE operation_id=$1 ON CONFLICT(installation_id) DO NOTHING`, operationID, id)
		if e != nil {
			return e
		}
		if bound.RowsAffected() != 1 {
			return identity.Conflict
		}
		_, e = tx.Exec(ctx, `UPDATE station.app_installations SET app_version=$2,revision=revision+1,updated_at=now() WHERE installation_id=$1`, id, observedVersion)
		return e
	}
	next := installationState(state, action, result)
	if next == state {
		return nil
	}
	_, err = tx.Exec(ctx, `UPDATE station.app_installations SET state=$2,revision=revision+1,updated_at=now() WHERE installation_id=$1`, id, next)
	return err
}

const installationColumns = `installation_id::text,app_id,app_version,station_id::text,organization_id::text,state,revision,COALESCE(last_operation_id::text,''),created_at,updated_at`

func scanInstallation(row interface{ Scan(...any) error }) (Installation, error) {
	var i Installation
	err := row.Scan(&i.InstallationID, &i.AppID, &i.AppVersion, &i.StationID, &i.OrganizationID, &i.State, &i.Revision, &i.LastOperationID, &i.CreatedAt, &i.UpdatedAt)
	return i, err
}

func (s *Service) canReadInstallations(user identity.IdentityContext) bool {
	return user.StationID == s.StationID && slices.Contains(user.Roles, "admin") && slices.Contains(user.Scopes, "app:read")
}

func (s *Service) ListInstallations(ctx context.Context, user identity.IdentityContext) ([]Installation, error) {
	if !s.canReadInstallations(user) {
		return nil, identity.Denied
	}
	rows, err := s.Pool.Query(ctx, `SELECT `+installationColumns+` FROM station.app_installations WHERE station_id=$1 AND organization_id=$2 AND state <> 'removed' ORDER BY created_at,installation_id LIMIT 1001`, s.StationID, user.OrganizationID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []Installation{}
	for rows.Next() {
		i, err := scanInstallation(rows)
		if err != nil {
			return nil, err
		}
		items = append(items, i)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	// Do not silently omit installed apps. Pagination is a later protocol extension.
	if len(items) > 1000 {
		return nil, errors.New("installation listing limit exceeded")
	}
	return items, nil
}

func (s *Service) GetInstallation(ctx context.Context, user identity.IdentityContext, id string) (Installation, error) {
	if !s.canReadInstallations(user) {
		return Installation{}, identity.Denied
	}
	parsed, err := uuid.Parse(id)
	if err != nil || parsed.Version() != 7 || parsed.String() != id {
		return Installation{}, pgx.ErrNoRows
	}
	return scanInstallation(s.Pool.QueryRow(ctx, `SELECT `+installationColumns+` FROM station.app_installations WHERE installation_id=$1 AND station_id=$2 AND organization_id=$3`, id, s.StationID, user.OrganizationID))
}
