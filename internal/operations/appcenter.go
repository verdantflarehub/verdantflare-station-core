package operations

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

// AppCenterFacts reads installations and operations in one read-only snapshot.
// Workload observations remain independently timestamped; this is not a
// transaction spanning PostgreSQL and Kubernetes.
func (s *Service) AppCenterFacts(ctx context.Context, user identity.IdentityContext, requestID string) (appcenter.Facts, error) {
	var empty appcenter.Facts
	if s == nil || !s.canReadInstallations(user) {
		return empty, identity.Denied
	}
	if s.Pool == nil || s.Catalog == nil {
		return empty, appcenter.ErrUnavailable
	}
	// Network observations precede the database snapshot so no database
	// transaction stays open while waiting for Kubernetes or discovery.
	catalogItems, err := s.Catalog.Snapshot(ctx)
	if err != nil {
		return empty, appcenter.ErrUnavailable
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return empty, appcenter.ErrUnavailable
	}
	defer func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = tx.Rollback(cleanup)
	}()
	rows, err := tx.Query(ctx, `SELECT `+installationColumns+` FROM station.app_installations WHERE station_id=$1 AND organization_id=$2 AND state<>'removed' ORDER BY installation_id LIMIT 1001`, s.StationID, user.OrganizationID)
	if err != nil {
		return empty, appcenter.ErrUnavailable
	}
	installations := []appcenter.Record{}
	for rows.Next() {
		value, err := scanInstallation(rows)
		if err != nil {
			rows.Close()
			return empty, appcenter.ErrUnavailable
		}
		installations = append(installations, appcenter.Record{AppID: value.AppID, Installation: appcenter.Installation{
			InstallationID: value.InstallationID, Revision: value.Revision, AppVersion: value.AppVersion, State: value.State, LastOperationID: appcenter.Text(value.LastOperationID),
		}})
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return empty, appcenter.ErrUnavailable
	}
	if len(installations) > 1000 {
		return empty, appcenter.ErrLimit
	}
	// Station/App exclusion spans principals. These private records are never
	// serialized: the projector redacts every operation outside the caller.
	rows, err = tx.Query(ctx, `SELECT app_id,operation_id::text,user_id::text,organization_id::text,'lifecycle',status,phase,updated_at
 FROM station.app_operations WHERE station_id=$1 AND (status IN ('accepted','running') OR
 (user_id=$2 AND organization_id=$3 AND operation_id IN
 (SELECT last_operation_id FROM station.app_installations WHERE station_id=$1 AND organization_id=$3 AND state<>'removed')))
 UNION ALL SELECT app_id,operation_id::text,user_id::text,organization_id::text,'transition',status,phase,updated_at
 FROM station.app_installation_transitions WHERE station_id=$1 AND status NOT IN ('succeeded','failed')
 ORDER BY app_id,operation_id LIMIT 1001`, s.StationID, user.UserID, user.OrganizationID)
	if err != nil {
		return empty, appcenter.ErrUnavailable
	}
	operations := []appcenter.OperationRecord{}
	for rows.Next() {
		var value appcenter.OperationRecord
		if err := rows.Scan(&value.AppID, &value.OperationID, &value.UserID, &value.OrganizationID, &value.Protocol, &value.Status, &value.Phase, &value.UpdatedAt); err != nil {
			rows.Close()
			return empty, appcenter.ErrUnavailable
		}
		operations = append(operations, value)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return empty, appcenter.ErrUnavailable
	}
	if len(operations) > 1000 {
		return empty, appcenter.ErrLimit
	}
	// Preserve global installation exclusion without exposing another owner's
	// installation identity through the scope-filtered facts response.
	foreign := map[string]bool{}
	if s.AppCenterAdmissionSource != nil {
		rows, err = tx.Query(ctx, `SELECT app_id FROM station.app_installations WHERE station_id=$1 AND organization_id<>$2 AND state<>'removed' LIMIT 1001`, s.StationID, user.OrganizationID)
		if err != nil {
			return empty, appcenter.ErrUnavailable
		}
		for rows.Next() {
			var app string
			if err := rows.Scan(&app); err != nil {
				rows.Close()
				return empty, appcenter.ErrUnavailable
			}
			foreign[app] = true
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			return empty, appcenter.ErrUnavailable
		}
		if len(foreign) > 1000 {
			return empty, appcenter.ErrLimit
		}
	}
	if err = tx.Commit(ctx); err != nil {
		return empty, appcenter.ErrUnavailable
	}
	facts, err := appcenter.Build(user, requestID, time.Now(), catalogItems, installations, operations)
	if err != nil {
		return empty, err
	}
	s.AppCenterAdmissionSource.project(ctx, user, &facts, foreign)
	return facts, nil
}
