package operations

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func TestOperationRecovery(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	station := uuid.NewString()
	if err := migrations.Apply(ctx, pool, station); err != nil {
		t.Fatal(err)
	}
	auth, err := identity.New(pool, station, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := auth.Bootstrap(ctx, "test", identity.BootstrapRequest{Username: "admin", Password: "Test-only-strong-password-934!", OrganizationName: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	err = os.WriteFile(path, []byte(`[{"app_id":"test-app","display_name":"Test","group_id":"video","brand":"vf","models":[],"source":"deploys/test.yaml","namespace":"test","workload_name":"test-app","version":"1.0.0","images":[{"component":"server","image":"example.invalid/test:1.0.0"}]}]`), 0600)
	if err != nil {
		t.Fatal(err)
	}
	apps, err := catalog.Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	svc := Service{Pool: pool, Catalog: apps, StationID: station}
	u := session.IdentityContext
	c := Command{RequestID: "request-1", IdempotencyKey: "recovery-key-1", OrganizationID: u.OrganizationID, StationID: station, AppID: "test-app", AppVersion: "1.0.0", Action: "install"}
	first, err := svc.Submit(ctx, u, c)
	if err != nil {
		t.Fatal(err)
	}
	c.RequestID = "request-2"
	replay, err := svc.Submit(ctx, u, c)
	if err != nil || replay.OperationID != first.OperationID {
		t.Fatalf("replay: %v", err)
	}
	c.IdempotencyKey = "recovery-key-2"
	if _, err = svc.Submit(ctx, u, c); !errors.Is(err, identity.Conflict) {
		t.Fatalf("concurrent command: %v", err)
	}
	_, err = pool.Exec(ctx, `UPDATE station.app_operations SET status='failed',phase='downloading',downloaded_bytes=25,total_bytes=100,error_code='SERVICE_UNAVAILABLE',error_message='Download interrupted' WHERE operation_id=$1`, first.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	c.IdempotencyKey = "recovery-key-1"
	replay, err = svc.Submit(ctx, u, c)
	if err != nil {
		t.Fatal(err)
	}
	if replay.Status != "failed" || replay.Error == nil || replay.Download.Percent == nil || *replay.Download.Percent != 25 {
		t.Fatalf("lost recovery details: %+v", replay)
	}
	got, err := svc.Get(ctx, u, first.OperationID)
	if err != nil || got.Status != "failed" {
		t.Fatalf("get: %v", err)
	}
	other := u
	other.OrganizationID = uuid.NewString()
	if _, err = svc.Get(ctx, other, first.OperationID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("cross-organization: %v", err)
	}
	other = u
	other.Scopes = nil
	if _, err = svc.Submit(ctx, other, c); !errors.Is(err, identity.Denied) {
		t.Fatalf("missing scope: %v", err)
	}
	c.Action = "start"
	if _, err = svc.Submit(ctx, u, c); !errors.Is(err, identity.Conflict) {
		t.Fatalf("changed payload: %v", err)
	}
	c.IdempotencyKey = "recovery-key-2"
	if _, err = svc.Submit(ctx, u, c); err != nil {
		t.Fatalf("new command after failure: %v", err)
	}
}
