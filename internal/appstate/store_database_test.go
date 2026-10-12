package appstate

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func fixture(t *testing.T) (*Service, identity.IdentityContext, string) {
	t.Helper()
	ctx := t.Context()
	pool := testdb.New(t)
	station := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, pool, station); err != nil {
		t.Fatal(err)
	}
	user := identity.IdentityContext{StationID: station, OrganizationID: uuid.Must(uuid.NewV7()).String(), UserID: uuid.Must(uuid.NewV7()).String(), Roles: []string{"admin"}, Scopes: []string{"app:read"}}
	for _, q := range []struct {
		sql  string
		args []any
	}{
		{`INSERT INTO station.users(user_id,username,password_hash) VALUES($1,'state-fixture','disabled')`, []any{user.UserID}},
		{`INSERT INTO station.organizations(organization_id,name) VALUES($1,'State fixture')`, []any{user.OrganizationID}},
	} {
		if _, err := pool.Exec(ctx, q.sql, q.args...); err != nil {
			t.Fatal(err)
		}
	}
	s := &Service{Pool: pool, StationID: station}
	id := uuid.Must(uuid.NewV7()).String()
	if _, err := pool.Exec(ctx, `INSERT INTO station.app_installations(installation_id,created_by,organization_id,station_id,app_id,app_version,state) VALUES($1,$2,$3,$4,'fixture-app','1.0.0','installed')`, id, user.UserID, user.OrganizationID, station); err != nil {
		t.Fatal(err)
	}
	return s, user, id
}

func snapshot(t *testing.T, s *Service, u identity.IdentityContext, id string) Snapshot {
	t.Helper()
	v, err := s.Snapshot(t.Context(), u, id)
	if err != nil {
		t.Fatal(err)
	}
	validateSnapshot(t, v)
	return v
}
func query(t *testing.T, s *Service, sql string, args ...any) {
	t.Helper()
	if _, err := s.Pool.Exec(t.Context(), sql, args...); err != nil {
		t.Fatal(err)
	}
}
func revision(t *testing.T, v string) int64 {
	t.Helper()
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestAppStateDatabaseSnapshotAndIsolation(t *testing.T) {
	s, u, id := fixture(t)
	if err := s.SyncCatalog(t.Context(), []string{"fixture-app", "uninstalled-app"}); err != nil {
		t.Fatal(err)
	}
	v := snapshot(t, s, u, "")
	if len(v.Items) != 2 || v.Items[0].Installation.State != "installed" || v.Items[0].Runtime.State != "unknown" || v.Items[1].Installation != nil {
		t.Fatal("installation and observation conflated", v)
	}
	if got := snapshot(t, &Service{Pool: s.Pool, StationID: s.StationID}, u, id); got.Revision != v.Revision || len(got.Items) != 1 {
		t.Fatal("restart lost snapshot")
	}
	foreign := u
	foreign.OrganizationID = uuid.Must(uuid.NewV7()).String()
	query(t, s, `INSERT INTO station.organizations(organization_id,name) VALUES($1,'Other')`, foreign.OrganizationID)
	if _, err := s.Snapshot(t.Context(), foreign, id); !errors.Is(err, ErrNotFound) {
		t.Fatal("cross organization identity leaked", err)
	}
	if got := snapshot(t, s, foreign, ""); len(got.Items) != 2 || got.Items[0].Installation != nil {
		t.Fatal("foreign installation exposed")
	}
	for _, mutate := range []func(*identity.IdentityContext){func(u *identity.IdentityContext) { u.StationID = uuid.NewString() }, func(u *identity.IdentityContext) { u.Scopes = nil }, func(u *identity.IdentityContext) { u.Roles = nil }} {
		bad := u
		mutate(&bad)
		if _, err := s.Snapshot(t.Context(), bad, ""); !errors.Is(err, identity.Denied) {
			t.Fatal("unauthorized snapshot", err)
		}
	}
	query(t, s, `UPDATE station.app_installations SET state='removed',revision=revision+1 WHERE installation_id=$1`, id)
	if got := snapshot(t, s, u, ""); got.Items[0].Installation != nil {
		t.Fatal("removed installation remains current")
	}
	if got := snapshot(t, s, u, id); got.Items[0].Installation.State != "removed" {
		t.Fatal("history discarded")
	}
	if err := s.SyncCatalog(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	if got := snapshot(t, s, u, ""); len(got.Items) != 0 {
		t.Fatal("complete empty inventory not empty")
	}
}

func TestAppStateDatabaseObservationExpiryAndOutbox(t *testing.T) {
	s, u, id := fixture(t)
	query(t, s, `INSERT INTO station.app_state_observations(installation_id,workload_uid,state,reason_code,received_at,expires_at) VALUES($1,'resource-original','ready','READY',clock_timestamp(),clock_timestamp()+interval '30 seconds')`, id)
	before := snapshot(t, s, u, id)
	if before.Items[0].Runtime.State != "ready" {
		t.Fatal("fresh observation missing")
	}
	query(t, s, `UPDATE station.app_state_observations SET received_at=clock_timestamp(),expires_at=clock_timestamp()+interval '30 seconds' WHERE installation_id=$1`, id)
	if got := snapshot(t, s, u, id); got.Revision != before.Revision {
		t.Fatal("heartbeat changed semantic revision")
	}
	pending, err := s.Pending(t.Context(), 10)
	if err != nil || len(pending) != 1 {
		t.Fatal("missing durable notification", err)
	}
	// Force deadline passage without running the expiry worker; reads must
	// already be unknown, while the original installation remains installed.
	query(t, s, `UPDATE station.app_state_observations SET received_at=clock_timestamp()-interval '60 seconds',expires_at=clock_timestamp()-interval '1 second' WHERE installation_id=$1`, id)
	got := snapshot(t, s, u, id)
	if got.Items[0].Runtime.State != "unknown" || got.Items[0].Runtime.ReasonCode != "OBSERVATION_EXPIRED" || got.Items[0].Installation.State != "installed" {
		t.Fatal("expired evidence erased installation or stayed ready")
	}
	if err = s.Expire(t.Context()); err != nil {
		t.Fatal(err)
	}
	after := snapshot(t, s, u, id)
	if revision(t, after.Revision) <= revision(t, before.Revision) {
		t.Fatal("expiry not notified")
	}
	if err = s.Acknowledge(t.Context(), pending[0]); err != nil {
		t.Fatal(err)
	}
	remaining, err := s.Pending(t.Context(), 10)
	if err != nil || len(remaining) != 1 || remaining[0].Revision != after.Revision {
		t.Fatal("stale acknowledgment lost a later change", err)
	}
	if err = s.Expire(t.Context()); err != nil {
		t.Fatal(err)
	}
	if snapshot(t, s, u, id).Revision != after.Revision {
		t.Fatal("expiry replay created another change")
	}
	if err = s.Acknowledge(t.Context(), remaining[0]); err != nil {
		t.Fatal(err)
	}
	if remaining, err = s.Pending(t.Context(), 10); err != nil || len(remaining) != 0 {
		t.Fatal("acknowledged notification pending", err)
	}
	query(t, s, `UPDATE station.app_state_observations SET workload_uid=NULL,state='absent',reason_code='RESOURCE_ABSENT',received_at=clock_timestamp(),expires_at=clock_timestamp()+interval '30 seconds',expired=false WHERE installation_id=$1`, id)
	got = snapshot(t, s, u, id)
	if got.Items[0].Runtime.State != "absent" || got.Items[0].Installation.State != "installed" {
		t.Fatal("resource missing was treated as uninstall")
	}
	query(t, s, `UPDATE station.app_state_observations SET received_at=clock_timestamp()-interval '60 seconds',expires_at=clock_timestamp()-interval '1 second' WHERE installation_id=$1`, id)
	stale := snapshot(t, s, u, id)
	query(t, s, `UPDATE station.app_state_observations SET received_at=clock_timestamp(),expires_at=clock_timestamp()+interval '30 seconds' WHERE installation_id=$1`, id)
	refreshed := snapshot(t, s, u, id)
	if revision(t, refreshed.Revision) <= revision(t, stale.Revision) {
		t.Fatal("refresh after deadline failed to notify before scanner ran")
	}
	query(t, s, `DELETE FROM station.app_state_observations WHERE installation_id=$1`, id)
	cleared := snapshot(t, s, u, id)
	if cleared.Items[0].Runtime.ReasonCode != "NOT_OBSERVED" || revision(t, cleared.Revision) <= revision(t, refreshed.Revision) {
		t.Fatal("discarded evidence failed to notify")
	}
}

func TestAppStateDatabaseAtomicRevisionAndConcurrentChanges(t *testing.T) {
	s, u, id := fixture(t)
	before := snapshot(t, s, u, id)
	tx, err := s.Pool.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = tx.Exec(t.Context(), `UPDATE station.app_installations SET revision=revision+1 WHERE installation_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	if err = tx.Rollback(t.Context()); err != nil {
		t.Fatal(err)
	}
	if snapshot(t, s, u, id).Revision != before.Revision {
		t.Fatal("rolled back business transaction published a change")
	}
	const count = 12
	var wg sync.WaitGroup
	errs := make(chan error, count)
	for range count {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Pool.Exec(t.Context(), `UPDATE station.app_installations SET revision=revision+1 WHERE installation_id=$1`, id)
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	after := snapshot(t, s, u, id)
	if revision(t, after.Revision)-revision(t, before.Revision) != count || after.Items[0].Installation.Revision-before.Items[0].Installation.Revision != count {
		t.Fatal("concurrent business changes lost", before, after)
	}
	query(t, s, `UPDATE station.app_installations SET updated_at=clock_timestamp() WHERE installation_id=$1`, id)
	if snapshot(t, s, u, id).Revision != after.Revision {
		t.Fatal("poll timestamp caused a business change")
	}
	pending, err := s.Pending(t.Context(), 10)
	if err != nil {
		t.Fatal(err)
	}
	pending[0].Revision = strconv.FormatInt(revision(t, after.Revision)+1, 10)
	if err = s.Acknowledge(t.Context(), pending[0]); !errors.Is(err, identity.Conflict) {
		t.Fatal("uncommitted future acknowledged", err)
	}
}

func TestAppStateDatabaseOperationAndTransitionProjection(t *testing.T) {
	s, u, id := fixture(t)
	op := uuid.Must(uuid.NewV7()).String()
	query(t, s, `INSERT INTO station.app_operations(operation_id,installation_id,request_id,user_id,organization_id,station_id,app_id,app_version,action,status,phase,idempotency_key,request_hash) VALUES($1,$2,'fixture',$3,$4,$5,'fixture-app','1.0.0','start','running','starting','fixture-operation-key',decode(repeat('01',32),'hex'))`, op, id, u.UserID, u.OrganizationID, u.StationID)
	query(t, s, `UPDATE station.app_installations SET last_operation_id=$2,revision=revision+1 WHERE installation_id=$1`, id, op)
	v := snapshot(t, s, u, id)
	if v.Items[0].Operation == nil || !v.Items[0].Operation.Blocking {
		t.Fatal("active operation missing")
	}
	other := u
	other.UserID = uuid.Must(uuid.NewV7()).String()
	if got := snapshot(t, s, other, id); got.Items[0].Operation != nil || got.Items[0].Installation.LastOperationID != nil {
		t.Fatal("another principal's operation leaked")
	}
	query(t, s, `UPDATE station.app_operations SET status='succeeded',phase='completed' WHERE operation_id=$1`, op)
	if got := snapshot(t, s, u, id); got.Items[0].Operation.Blocking || got.Revision == v.Revision {
		t.Fatal("operation completion not reflected")
	}
	transition := uuid.Must(uuid.NewV7()).String()
	query(t, s, `INSERT INTO station.app_installation_transitions(operation_id,installation_id,user_id,organization_id,station_id,app_id,request_id,idempotency_key,request_hash,action,expected_revision,source_version,target_version,source_snapshot,target_snapshot,status,phase) VALUES($1,$2,$3,$4,$5,'fixture-app','fixture','fixture-transition-key',decode(repeat('02',32),'hex'),'upgrade',2,'1.0.0','2.0.0','{}','{}','accepted','accepted')`, transition, id, u.UserID, u.OrganizationID, u.StationID)
	if got := snapshot(t, s, u, id); got.Items[0].Operation.Protocol != "transition" || !got.Items[0].Operation.Blocking {
		t.Fatal("upgrade state not projected")
	}
}

func TestAppStateDatabaseCatalogRecoveryAndLimits(t *testing.T) {
	s, u, _ := fixture(t)
	if err := s.SyncCatalog(t.Context(), []string{"new-app"}); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, s, u, "")
	if err := s.SyncCatalog(t.Context(), []string{"new-app"}); err != nil {
		t.Fatal(err)
	}
	if snapshot(t, s, u, "").Revision != before.Revision {
		t.Fatal("catalog restart changed revision")
	}
	if err := s.SyncCatalog(t.Context(), []string{"new-app", "../invalid"}); !errors.Is(err, identity.Invalid) {
		t.Fatal("invalid catalog accepted")
	}
	if snapshot(t, s, u, "").Revision != before.Revision {
		t.Fatal("invalid configuration replaced previous catalog")
	}
	apps := make([]string, 1000)
	for i := range apps {
		apps[i] = fmt.Sprintf("catalog-%04d", i)
	}
	if err := s.SyncCatalog(t.Context(), apps); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Snapshot(t.Context(), u, ""); !errors.Is(err, ErrLimit) {
		t.Fatal("over-limit snapshot silently truncated", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.Snapshot(ctx, u, ""); !errors.Is(err, ErrUnavailable) {
		t.Fatal("cancelled read returned success", err)
	}
	done := make(chan struct{})
	go func() { defer close(done); s.RunExpiry(ctx, nil) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expiry worker did not cancel")
	}
}
