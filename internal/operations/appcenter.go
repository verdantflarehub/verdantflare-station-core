package operations

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

// AppCenterFacts reads installations and operations in one read-only snapshot.
// Workload observations are copied from Core memory in that transaction. No
// listing dependency contacts Kubernetes, Runtime, etcd, or an App.
func (s *Service) AppCenterFacts(ctx context.Context, user identity.IdentityContext, requestID string) (appcenter.Facts, error) {
	var empty appcenter.Facts
	if s == nil || !s.canReadInstallations(user) {
		return empty, identity.Denied
	}
	if s.Pool == nil || s.Catalog == nil {
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
	states := s.AppStates
	if states == nil {
		// Unconfigured collection still preserves the persistent installation
		// and local catalog, with unknown observations and no probe fallback.
		states = &appstate.Service{StationID: s.StationID}
	}
	if states.StationID != s.StationID {
		return empty, appcenter.ErrUnavailable
	}
	catalogItems, views, err := states.CatalogSnapshot(ctx, tx, user.OrganizationID, s.Catalog.Entries())
	if err != nil {
		return empty, appcenter.ErrUnavailable
	}
	services := map[string]appcenter.Service{}
	for id, v := range views {
		service := appcenter.Service{Quality: v.Runtime.Quality, State: v.Runtime.State, ReasonCode: appcenter.Text(v.Runtime.ReasonCode), ObservedAt: v.Runtime.ObservedAt}
		if r := v.Resource; r != nil {
			service.WorkloadUID = appcenter.Text(r.UID)
			service.ObservationKey = appcenter.Text("obs:" + appcenter.Digest([]string{r.Namespace, r.Name, r.UID})[7:])
		}
		services[id] = service
	}
	if err = tx.Commit(ctx); err != nil {
		return empty, appcenter.ErrUnavailable
	}
	facts, err := appcenter.BuildState(user, requestID, time.Now(), catalogItems, installations, operations, services)
	if err != nil {
		return empty, err
	}
	s.AppCenterAdmissionSource.project(ctx, user, &facts, foreign)
	return facts, nil
}
