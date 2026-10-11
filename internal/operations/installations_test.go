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
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
	"google.golang.org/grpc"
)

type installationRuntime struct{ result string }

type checkingInstallationRuntime struct {
	t          *testing.T
	id, result string
}

func (r checkingInstallationRuntime) Reconcile(ctx context.Context, in *pb.AppRequest, options ...grpc.CallOption) (*pb.AppProgress, error) {
	r.t.Helper()
	if in.InstallationId != r.id {
		r.t.Fatal("dispatch did not preserve the operation installation identity")
	}
	return (installationRuntime{r.result}).Reconcile(ctx, in, options...)
}

func (r installationRuntime) Reconcile(_ context.Context, in *pb.AppRequest, _ ...grpc.CallOption) (*pb.AppProgress, error) {
	p := &pb.AppProgress{OperationId: in.OperationId, Status: r.result, Phase: "completed"}
	if r.result == "failed" {
		p.Phase = "starting"
		p.ErrorCode = "SERVICE_UNAVAILABLE"
		p.ErrorMessage = "Test runtime unavailable"
	}
	return p, nil
}

func TestInstallationScopeBeforeDatabase(t *testing.T) {
	s := &Service{StationID: "station-a"}
	ctx := context.Background()
	for _, user := range []identity.IdentityContext{
		{StationID: "station-b", Roles: []string{"admin"}, Scopes: []string{"app:read"}},
		{StationID: "station-a", Roles: []string{"member"}, Scopes: []string{"app:read"}},
		{StationID: "station-a", Roles: []string{"admin"}},
	} {
		if _, err := s.ListInstallations(ctx, user); !errors.Is(err, identity.Denied) {
			t.Fatal(err)
		}
		if _, err := s.GetInstallation(ctx, user, uuid.Must(uuid.NewV7()).String()); !errors.Is(err, identity.Denied) {
			t.Fatal(err)
		}
	}
	user := identity.IdentityContext{StationID: "station-a", Roles: []string{"admin"}, Scopes: []string{"app:read"}}
	for _, id := range []string{"../other", "", "not-an-installation", uuid.NewString(), uuid.Must(uuid.NewV7()).String() + "/extra"} {
		if _, err := s.GetInstallation(ctx, user, id); !errors.Is(err, pgx.ErrNoRows) {
			t.Fatal(err)
		}
	}
}

// Run only against a disposable database on 5090 dev; testdb never reuses the
// application database. The Runtime stand-in reports outcomes without workloads.
func TestInstallationDatabaseLifecycle(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	stationID := uuid.Must(uuid.NewV7()).String()
	if err := migrations.Apply(ctx, pool, stationID); err != nil {
		t.Fatal(err)
	}
	auth, err := identity.New(pool, stationID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := auth.Bootstrap(ctx, "test", identity.BootstrapRequest{Username: "testadmin", Password: "Test-only-installation-password-934!", OrganizationName: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err = os.WriteFile(path, []byte(`[{"app_id":"test-app","display_name":"Test","group_id":"video","brand":"vf","models":[],"source":"deploys/test.yaml","namespace":"test","workload_name":"test-app","version":"1.0.0","images":[{"component":"server","image":"example.invalid/test:1.0.0"}]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	apps, err := catalog.Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	s := Service{Pool: pool, Catalog: apps, StationID: stationID}
	user := session.IdentityContext
	command := Command{RequestID: "installation-test", IdempotencyKey: "installation-attempt-1", OrganizationID: user.OrganizationID, StationID: stationID, AppID: "test-app", AppVersion: "1.0.0", Action: "install"}
	first, err := s.Submit(ctx, user, command)
	if err != nil {
		t.Fatal(err)
	}
	items, err := s.ListInstallations(ctx, user)
	if err != nil || len(items) != 1 {
		t.Fatal(items, err)
	}
	id := items[0].InstallationID
	if items[0].State != "pending" || id == first.OperationID || items[0].LastOperationID != first.OperationID {
		t.Fatal("installation and operation identity conflated")
	}
	command.RequestID = "retried-request"
	replayed, err := s.Submit(ctx, user, command)
	if err != nil || replayed.OperationID != first.OperationID {
		t.Fatal("idempotent operation changed", err)
	}
	if _, err = s.DispatchOne(ctx, checkingInstallationRuntime{t, id, "failed"}); err != nil {
		t.Fatal(err)
	}
	failed, err := s.GetInstallation(ctx, user, id)
	if err != nil || failed.State != "failed" {
		t.Fatal(failed, err)
	}
	command.IdempotencyKey = "installation-attempt-2"
	if _, err = s.Submit(ctx, user, command); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DispatchOne(ctx, checkingInstallationRuntime{t, id, "succeeded"}); err != nil {
		t.Fatal(err)
	}
	installed, err := s.GetInstallation(ctx, user, id)
	if err != nil || installed.State != "installed" || installed.Revision <= failed.Revision {
		t.Fatal(installed, err)
	}
	command.Action = "stop"
	command.IdempotencyKey = "installation-stop-1"
	if _, err = s.Submit(ctx, user, command); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DispatchOne(ctx, installationRuntime{"succeeded"}); err != nil {
		t.Fatal(err)
	}
	stopped, err := s.GetInstallation(ctx, user, id)
	if err != nil || stopped.State != "installed" {
		t.Fatal("stop removed installation", err)
	}
	other := user
	other.OrganizationID = uuid.Must(uuid.NewV7()).String()
	if _, err = s.GetInstallation(ctx, other, id); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("foreign installation exposed", err)
	}
	if hidden, err := s.ListInstallations(ctx, other); err != nil || len(hidden) != 0 {
		t.Fatal("foreign installation listed", err)
	}
	foreignCommand := command
	foreignCommand.OrganizationID = other.OrganizationID
	foreignCommand.IdempotencyKey = "foreign-command-1"
	if _, err = s.Submit(ctx, other, foreignCommand); !errors.Is(err, identity.Conflict) {
		t.Fatal("foreign installation reused", err)
	}
	command.Action = "delete"
	command.IdempotencyKey = "installation-delete-1"
	if _, err = s.Submit(ctx, user, command); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DispatchOne(ctx, installationRuntime{"failed"}); err != nil {
		t.Fatal(err)
	}
	retained, err := s.GetInstallation(ctx, user, id)
	if err != nil || retained.State != "installed" {
		t.Fatal("failed delete erased installation", err)
	}
	command.IdempotencyKey = "installation-delete-2"
	if _, err = s.Submit(ctx, user, command); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DispatchOne(ctx, installationRuntime{"succeeded"}); err != nil {
		t.Fatal(err)
	}
	removed, err := s.GetInstallation(ctx, user, id)
	if err != nil || removed.State != "removed" {
		t.Fatal(removed, err)
	}
	if items, err = s.ListInstallations(ctx, user); err != nil || len(items) != 0 {
		t.Fatal("removed installation still current", err)
	}
	command.Action = "install"
	command.IdempotencyKey = "installation-reinstall-1"
	// Fail the final audit insert after a new installation and operation have been
	// inserted. Neither earlier write may escape the transaction on that failure.
	invalidSession := user
	invalidSession.SessionID = uuid.Must(uuid.NewV7()).String()
	if _, err = s.Submit(ctx, invalidSession, command); err == nil {
		t.Fatal("missing audit session accepted")
	}
	if items, err = s.ListInstallations(ctx, user); err != nil || len(items) != 0 {
		t.Fatal("rolled-back installation leaked", err)
	}
	if _, err = s.Submit(ctx, user, command); err != nil {
		t.Fatal(err)
	}
	items, err = s.ListInstallations(ctx, user)
	if err != nil || len(items) != 1 || items[0].InstallationID == id {
		t.Fatal("reinstall reused retired identity", err)
	}
	// Simulate a historical operation without migration-10 identity. Dispatch
	// must not infer the current installation by matching App/Station.
	if _, err = pool.Exec(ctx, `UPDATE station.app_operations SET installation_id=NULL WHERE operation_id=$1`, items[0].LastOperationID); err != nil {
		t.Fatal(err)
	}
	if _, err = s.DispatchOne(ctx, checkingInstallationRuntime{t, "", "succeeded"}); err != nil {
		t.Fatal(err)
	}
}
