package operations

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

// Requires STATION_TEST_DATABASE_URL on 5090. The helper creates and removes
// only a disposable station_test_* database; no application database is used.
func TestAppCenterDatabaseSnapshot(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	station := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, pool, station); err != nil {
		t.Fatal(err)
	}
	auth, err := identity.New(pool, station, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := auth.Bootstrap(ctx, "appcenter-test", identity.BootstrapRequest{Username: "testadmin", Password: "Test-only-appcenter-password-294!", OrganizationName: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	user := session.IdentityContext
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(path, []byte(`[{"app_id":"test-app","display_name":"Test","group_id":"video","brand":"vf","models":[],"source":"deploys/test.yaml","namespace":"test","workload_name":"test-app","version":"1.0.0","images":[{"component":"server","image":"example.invalid/test:1.0.0"}]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	apps, err := catalog.Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := &Service{Pool: pool, Catalog: apps, StationID: station}
	read := func(actor identity.IdentityContext) appcenter.Facts {
		t.Helper()
		facts, err := s.AppCenterFacts(ctx, actor, "snapshot-request")
		if err != nil {
			t.Fatal(err)
		}
		return facts
	}
	f := read(user)
	if len(f.Items) != 1 || f.Items[0].Installation != nil || f.Items[0].Origin != "catalog" {
		t.Fatal("empty database fabricated installation")
	}
	command := Command{RequestID: "appcenter-install", IdempotencyKey: "appcenter-install-1", OrganizationID: user.OrganizationID, StationID: station, AppID: "test-app", AppVersion: "1.0.0", Action: "install"}
	op, err := s.Submit(ctx, user, command)
	if err != nil {
		t.Fatal(err)
	}
	f = read(user)
	if len(f.Items) != 1 || f.Items[0].Installation.State != "pending" || !f.Items[0].Operation.Blocking || !f.Items[0].Operation.CanRead || *f.Items[0].Operation.OperationID != op.OperationID {
		t.Fatal("accepted operation missing from transaction snapshot")
	}
	other := user
	other.UserID = uuid.Must(uuid.NewV7()).String()
	f = read(other)
	if !f.Items[0].Operation.Blocking || f.Items[0].Operation.CanRead || f.Items[0].Operation.OperationID != nil || f.Items[0].Installation.LastOperationID != nil {
		t.Fatal("foreign actor operation leaked")
	}
	other.OrganizationID = uuid.Must(uuid.NewV7()).String()
	f = read(other)
	if f.Items[0].Installation != nil || !f.Items[0].Operation.Blocking || f.Items[0].Operation.CanRead {
		t.Fatal("organization filter or station conflict projection failed")
	}
	if _, err := s.DispatchOne(ctx, installationRuntime{"succeeded"}); err != nil {
		t.Fatal(err)
	}
	f = read(user)
	if f.Items[0].Installation.State != "installed" || f.Items[0].Operation.Blocking || f.Items[0].Operation.Status == nil || *f.Items[0].Operation.Status != "succeeded" {
		t.Fatal("terminal operation still blocking")
	}
	if f.Items[0].Service.State != "unknown" {
		t.Fatal("mock runtime success fabricated workload readiness")
	}
	var count int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM station.app_operations`).Scan(&count); err != nil || count != 1 {
		t.Fatal("read projection wrote operations", err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE station.app_installation_transitions RENAME TO unavailable_transitions`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.AppCenterFacts(ctx, user, "after-failure"); !errors.Is(err, appcenter.ErrUnavailable) {
		t.Fatal("database source failure turned into successful empty facts", err)
	}
}
