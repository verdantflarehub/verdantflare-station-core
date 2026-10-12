package appstate

import (
	"context"
	"encoding/json"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

type Service struct {
	Observations *Memory
	memoryOnce   sync.Once
	Pool         *pgxpool.Pool
	StationID    string
}

func rollback(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
func validID(value string) bool {
	id, err := uuid.Parse(value)
	return err == nil && id != uuid.Nil && id.String() == value
}

// Snapshot uses a single PostgreSQL snapshot and never contacts Runtime,
// Kubernetes, etcd, an App, or a request-time catalog discovery adapter.
func (s *Service) Snapshot(ctx context.Context, user identity.IdentityContext, installationID string) (Snapshot, error) {
	var empty Snapshot
	if s == nil || user.StationID != s.StationID || !validID(user.StationID) || !validID(user.OrganizationID) || !validID(user.UserID) || !slices.Contains(user.Roles, "admin") || !slices.Contains(user.Scopes, "app:read") {
		return empty, identity.Denied
	}
	if installationID != "" && !validID(installationID) {
		return empty, identity.Invalid
	}
	if s.Pool == nil {
		return empty, ErrUnavailable
	}
	tx, err := s.Pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		return empty, ErrUnavailable
	}
	defer rollback(tx)
	out := Snapshot{SchemaVersion: 1, Scope: Scope{s.StationID, user.OrganizationID}, Items: []Item{}}
	var revision int64
	err = tx.QueryRow(ctx, `SELECT COALESCE((SELECT revision FROM station.app_state_scopes WHERE station_id=$1 AND organization_id=$2),0),transaction_timestamp()`, s.StationID, user.OrganizationID).Scan(&revision, &out.GeneratedAt)
	if err != nil {
		return empty, ErrUnavailable
	}
	out.GeneratedAt = out.GeneratedAt.UTC()
	out.Revision = strconv.FormatInt(revision, 10)
	rows, err := tx.Query(ctx, `WITH visible AS (
 SELECT i.app_id,i.installation_id,i.revision,i.app_version,i.state,i.last_operation_id
 FROM station.app_installations i WHERE i.station_id=$1 AND i.organization_id=$2
 AND (($4='' AND i.state<>'removed') OR i.installation_id::text=$4)
 UNION ALL
 SELECT c.app_id,NULL::uuid,NULL::bigint,NULL::text,NULL::text,NULL::uuid
 FROM station.app_state_catalog c WHERE c.station_id=$1 AND $4=''
 AND NOT EXISTS(SELECT 1 FROM station.app_installations i WHERE i.station_id=$1 AND i.organization_id=$2 AND i.app_id=c.app_id AND i.state<>'removed')
 )
 SELECT v.app_id,v.installation_id::text,COALESCE(m.revision,0),v.revision,v.app_version,v.state,
 CASE WHEN last_op.user_id=$3 AND last_op.organization_id=$2 THEN v.last_operation_id::text ELSE NULL END,
 op.operation_id,op.protocol,op.status,op.phase
 FROM visible v
 LEFT JOIN station.app_state_items m ON m.station_id=$1 AND m.organization_id=$2 AND m.app_id=v.app_id
 LEFT JOIN station.app_operations last_op ON last_op.operation_id=v.last_operation_id
 LEFT JOIN LATERAL (
  SELECT q.operation_id,q.protocol,q.status,q.phase FROM (
   SELECT x.operation_id::text,'lifecycle'::text AS protocol,x.status,x.phase,x.updated_at
   FROM station.app_operations x WHERE x.station_id=$1 AND x.organization_id=$2 AND x.user_id=$3
    AND x.app_id=v.app_id AND x.installation_id IS NOT DISTINCT FROM v.installation_id
    AND (x.status IN ('accepted','running') OR x.operation_id=v.last_operation_id)
   UNION ALL
   SELECT x.operation_id::text,'transition',x.status,x.phase,x.updated_at
   FROM station.app_installation_transitions x WHERE x.station_id=$1 AND x.organization_id=$2 AND x.user_id=$3
    AND x.installation_id=v.installation_id
  ) q ORDER BY (q.status NOT IN ('succeeded','failed')) DESC,q.updated_at DESC,q.operation_id DESC LIMIT 1
 ) op ON true
 ORDER BY v.app_id,v.installation_id LIMIT 1001`, s.StationID, user.OrganizationID, user.UserID, installationID)
	if err != nil {
		return empty, ErrUnavailable
	}
	for rows.Next() {
		var item Item
		var itemRev int64
		var installRev *int64
		var version, state, lastOperation, opID, protocol, status, phase *string
		if err = rows.Scan(&item.AppID, &item.InstallationID, &itemRev, &installRev, &version, &state, &lastOperation, &opID, &protocol, &status, &phase); err != nil {
			rows.Close()
			return empty, ErrUnavailable
		}
		item.StateRevision = strconv.FormatInt(itemRev, 10)
		if item.InstallationID != nil {
			if installRev == nil || version == nil || state == nil {
				rows.Close()
				return empty, ErrUnavailable
			}
			item.Installation = &Installation{*installRev, *version, *state, lastOperation}
		}
		if opID != nil {
			if protocol == nil || status == nil || phase == nil {
				rows.Close()
				return empty, ErrUnavailable
			}
			item.Operation = &Operation{*opID, *protocol, *status, *phase, *status != "succeeded" && *status != "failed"}
		}
		out.Items = append(out.Items, item)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return empty, ErrUnavailable
	}
	if len(out.Items) > 1000 {
		return empty, ErrLimit
	}
	if installationID != "" && len(out.Items) == 0 {
		return empty, ErrNotFound
	}
	bindings, err := s.readBindings(ctx, tx, installationID)
	if err != nil {
		return empty, err
	}
	memory := s.memory()
	views, scopes, err := memory.Views(bindings)
	if err != nil {
		return empty, err
	}
	out.GeneratedAt = time.Now().UTC()
	scopeToken := scopes[user.OrganizationID]
	if scopeToken == "" {
		scopeToken = memory.token(0)
	}
	out.Revision += ":" + scopeToken
	for index := range out.Items {
		item := &out.Items[index]
		view := MemoryView{Runtime: Runtime{State: "unknown", Quality: "unknown", ReasonCode: "NOT_SYNCED"}, Revision: memory.token(0)}
		if item.InstallationID != nil {
			if known, ok := views[*item.InstallationID]; ok {
				view = known
			}
		}
		item.Runtime = view.Runtime
		item.StateRevision += ":" + view.Revision
	}
	encoded, err := json.Marshal(out)
	if err != nil {
		return empty, ErrUnavailable
	}
	if len(encoded) > 4<<20 {
		return empty, ErrLimit
	}
	if err = tx.Commit(ctx); err != nil {
		return empty, ErrUnavailable
	}
	return out, nil
}

var appIDPattern = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

// SyncCatalog persists only trusted catalog identities. A failed/missing
// configuration is not an empty catalog and must not call this method.
func (s *Service) SyncCatalog(ctx context.Context, apps []string) error {
	if s == nil || s.Pool == nil || !validID(s.StationID) {
		return ErrUnavailable
	}
	if len(apps) > 1000 {
		return ErrLimit
	}
	seen := map[string]bool{}
	for _, app := range apps {
		if len(app) > 63 || !appIDPattern.MatchString(app) || seen[app] {
			return identity.Invalid
		}
		seen[app] = true
	}
	apps = slices.Clone(apps)
	slices.Sort(apps)
	if apps == nil {
		apps = []string{}
	}
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return ErrUnavailable
	}
	defer rollback(tx)
	if _, err = tx.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended($1,18))`, s.StationID); err != nil {
		return ErrUnavailable
	}
	rows, err := tx.Query(ctx, `SELECT app_id FROM station.app_state_catalog WHERE station_id=$1 ORDER BY app_id`, s.StationID)
	if err != nil {
		return ErrUnavailable
	}
	old := []string{}
	for rows.Next() {
		var app string
		if err = rows.Scan(&app); err != nil {
			rows.Close()
			return ErrUnavailable
		}
		old = append(old, app)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return ErrUnavailable
	}
	if slices.Equal(old, apps) {
		return tx.Commit(ctx)
	}
	changed := []string{}
	for _, app := range old {
		if !seen[app] {
			changed = append(changed, app)
		}
	}
	for _, app := range apps {
		if !slices.Contains(old, app) {
			changed = append(changed, app)
		}
	}
	slices.Sort(changed)
	if _, err = tx.Exec(ctx, `DELETE FROM station.app_state_catalog WHERE station_id=$1 AND NOT(app_id=ANY($2::text[]))`, s.StationID, apps); err != nil {
		return ErrUnavailable
	}
	if _, err = tx.Exec(ctx, `INSERT INTO station.app_state_catalog(station_id,app_id) SELECT $1,unnest($2::text[]) ON CONFLICT DO NOTHING`, s.StationID, apps); err != nil {
		return ErrUnavailable
	}
	// Every existing organization sees the same trusted catalog identities,
	// while installation/operation details remain organization scoped.
	if _, err = tx.Exec(ctx, `SELECT station.mark_app_state($1,o.organization_id,a.app_id) FROM station.organizations o CROSS JOIN unnest($2::text[]) a(app_id) ORDER BY o.organization_id,a.app_id`, s.StationID, changed); err != nil {
		return ErrUnavailable
	}
	return tx.Commit(ctx)
}

func (s *Service) memory() *Memory {
	s.memoryOnce.Do(func() {
		if s.Observations == nil {
			s.Observations, _ = NewMemory(s.StationID, nil)
		}
	})
	return s.Observations
}

// readBindings uses the same transaction as installation/operation facts.
// It supplies trusted identities to the reconstructible observation cache.
func (s *Service) readBindings(ctx context.Context, tx pgx.Tx, extra string) ([]Binding, error) {
	rows, err := tx.Query(ctx, `SELECT installation_id::text,organization_id::text,app_id FROM station.app_installations WHERE station_id=$1 AND (state<>'removed' OR installation_id::text=$2) ORDER BY installation_id LIMIT 1001`, s.StationID, extra)
	if err != nil {
		return nil, ErrUnavailable
	}
	defer rows.Close()
	bindings := []Binding{}
	for rows.Next() {
		var b Binding
		if rows.Scan(&b.InstallationID, &b.OrganizationID, &b.AppID) != nil {
			return nil, ErrUnavailable
		}
		bindings = append(bindings, b)
	}
	if rows.Err() != nil {
		return nil, ErrUnavailable
	}
	if len(bindings) > maxBindings {
		return nil, ErrLimit
	}
	return bindings, nil
}

func (s *Service) Expire(ctx context.Context) error {
	if s == nil || !validID(s.StationID) {
		return ErrUnavailable
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.memory().Expire()
	return nil
}
