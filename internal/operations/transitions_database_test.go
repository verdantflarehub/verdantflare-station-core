package operations

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

func transitionDatabase(t *testing.T) (*Service, identity.IdentityContext, TransitionCommand, TransitionReader) {
	return transitionDatabaseApp(t, "test-app", "1.9.0")
}

func transitionDatabaseApp(t *testing.T, app, version string) (*Service, identity.IdentityContext, TransitionCommand, TransitionReader) {
	t.Helper()
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
	user, err := auth.Bootstrap(ctx, "test", identity.BootstrapRequest{Username: "testadmin", Password: "Test-only-transition-password-789!", OrganizationName: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	catalogJSON, _ := json.Marshal([]any{map[string]any{"app_id": app, "display_name": "Test", "group_id": "video", "brand": "vf", "models": []string{}, "source": "deploys/test.yaml", "namespace": "test", "workload_name": app, "version": version, "images": []any{map[string]string{"component": "server", "image": "example.invalid/test:" + version}}}})
	if err = os.WriteFile(path, catalogJSON, 0600); err != nil {
		t.Fatal(err)
	}
	apps, err := catalog.Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	svc := &Service{Pool: pool, Catalog: apps, StationID: station}
	_, err = svc.Submit(ctx, user.IdentityContext, Command{RequestID: "initial-install", IdempotencyKey: "initial-install-key", OrganizationID: user.OrganizationID, StationID: station, AppID: app, AppVersion: version, Action: "install"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = svc.DispatchOne(ctx, installationRuntime{"succeeded"}); err != nil {
		t.Fatal(err)
	}
	items, err := svc.ListInstallations(ctx, user.IdentityContext)
	if err != nil || len(items) != 1 {
		t.Fatal(items, err)
	}
	c, _, source, target := transitionInputs()
	c.InstallationID = items[0].InstallationID
	c.ExpectedRevision = items[0].Revision
	reader := func(context.Context, identity.IdentityContext, Installation, string) (TransitionSnapshot, TransitionSnapshot, error) {
		return source, target, nil
	}
	return svc, user.IdentityContext, c, reader
}

func TestTransitionDatabaseAcceptanceReplayAndScope(t *testing.T) {
	svc, user, c, reader := transitionDatabase(t)
	ctx := context.Background()
	before, err := svc.GetInstallation(ctx, user, c.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	first, err := svc.SubmitTransition(ctx, user, c, reader)
	if err != nil || first.Status != "accepted" {
		t.Fatal(first, err)
	}
	after, err := svc.GetInstallation(ctx, user, c.InstallationID)
	if err != nil || before.AppVersion != after.AppVersion || before.Revision != after.Revision || before.LastOperationID != after.LastOperationID {
		t.Fatal("acceptance changed installation", err)
	}
	c.RequestID = "another-trace"
	// Restarted coordinator can return the saved input without re-reading a
	// changed or missing release registration.
	restarted := &Service{Pool: svc.Pool, StationID: svc.StationID}
	replay, err := restarted.SubmitTransition(ctx, user, c, nil)
	if err != nil || replay.OperationID != first.OperationID || replay.RequestID != first.RequestID || replay.Target.DashboardDigest != first.Target.DashboardDigest {
		t.Fatal("original snapshot not recovered", err)
	}
	c.TargetReleaseRef = "other-target"
	if _, err = svc.SubmitTransition(ctx, user, c, reader); !errors.Is(err, identity.Conflict) {
		t.Fatal("same key changed target", err)
	}
	foreign := user
	foreign.UserID = uuid.Must(uuid.NewV7()).String()
	if _, err = svc.GetTransition(ctx, foreign, first.OperationID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("foreign operation leaked", err)
	}
	foreign = user
	foreign.OrganizationID = uuid.Must(uuid.NewV7()).String()
	if _, err = svc.GetTransition(ctx, foreign, first.OperationID); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("foreign organization leaked", err)
	}
}

func TestTransitionDatabaseMutualExclusionAndRecoveryRequired(t *testing.T) {
	svc, user, c, reader := transitionDatabase(t)
	ctx := context.Background()
	ordinary := Command{RequestID: "stop-request", IdempotencyKey: "stop-test-before", OrganizationID: user.OrganizationID, StationID: user.StationID, AppID: "test-app", AppVersion: c.SourceVersion, Action: "stop"}
	if _, err := svc.Submit(ctx, user, ordinary); err != nil {
		t.Fatal(err)
	}
	installed, err := svc.GetInstallation(ctx, user, c.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	c.ExpectedRevision = installed.Revision
	if _, err = svc.SubmitTransition(ctx, user, c, reader); !errors.Is(err, identity.Conflict) {
		t.Fatal("ordinary operation overlapped", err)
	}
	if _, err = svc.DispatchOne(ctx, installationRuntime{"failed"}); err != nil {
		t.Fatal(err)
	}
	first, err := svc.SubmitTransition(ctx, user, c, reader)
	if err != nil {
		t.Fatal(err)
	}
	ordinary.IdempotencyKey = "stop-test-after"
	if _, err = svc.Submit(ctx, user, ordinary); !errors.Is(err, identity.Conflict) {
		t.Fatal("transition did not block ordinary operation", err)
	}
	if _, err = svc.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET status='recovery_required',phase='recovery_required' WHERE operation_id=$1`, first.OperationID); err != nil {
		t.Fatal(err)
	}
	c.IdempotencyKey = "another-transition"
	if _, err = svc.SubmitTransition(ctx, user, c, reader); !errors.Is(err, identity.Conflict) {
		t.Fatal("unresolved recovery treated as terminal", err)
	}
	if _, err = svc.Submit(ctx, user, ordinary); !errors.Is(err, identity.Conflict) {
		t.Fatal(err)
	}
}

func TestTransitionDatabaseConcurrentAcceptance(t *testing.T) {
	svc, user, c, reader := transitionDatabase(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	out := make(chan Transition, 2)
	fail := make(chan error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); v, err := svc.SubmitTransition(ctx, user, c, reader); out <- v; fail <- err }()
	}
	wg.Wait()
	close(out)
	close(fail)
	for err := range fail {
		if err != nil {
			t.Fatal(err)
		}
	}
	ids := map[string]bool{}
	for item := range out {
		ids[item.OperationID] = true
	}
	if len(ids) != 1 {
		t.Fatal("duplicate transition accepted")
	}
	c.IdempotencyKey = "different-key"
	if _, err := svc.SubmitTransition(ctx, user, c, reader); !errors.Is(err, identity.Conflict) {
		t.Fatal("concurrent distinct transition accepted", err)
	}
}

func TestTransitionDatabaseRejectedInputsAndAuditRollback(t *testing.T) {
	svc, user, c, reader := transitionDatabase(t)
	ctx := context.Background()
	checkEmpty := func() {
		t.Helper()
		var count int
		if err := svc.Pool.QueryRow(ctx, `SELECT count(*) FROM station.app_installation_transitions`).Scan(&count); err != nil || count != 0 {
			t.Fatal("failed acceptance persisted", count, err)
		}
	}
	stale := c
	stale.ExpectedRevision--
	if _, err := svc.SubmitTransition(ctx, user, stale, reader); !errors.Is(err, identity.Conflict) {
		t.Fatal("stale revision accepted", err)
	}
	checkEmpty()
	if _, err := svc.SubmitTransition(ctx, user, c, nil); err == nil {
		t.Fatal("unverified release accepted")
	}
	checkEmpty()
	badReader := func(ctx context.Context, u identity.IdentityContext, i Installation, ref string) (TransitionSnapshot, TransitionSnapshot, error) {
		s, d, err := reader(ctx, u, i, ref)
		d.DataContract = "changed-schema"
		return s, d, err
	}
	if _, err := svc.SubmitTransition(ctx, user, c, badReader); !errors.Is(err, identity.Invalid) {
		t.Fatal("unsupported migration accepted", err)
	}
	checkEmpty()
	invalidSession := user
	invalidSession.SessionID = uuid.Must(uuid.NewV7()).String()
	if _, err := svc.SubmitTransition(ctx, invalidSession, c, reader); err == nil {
		t.Fatal("missing audit session accepted")
	}
	checkEmpty()
	if _, err := svc.SubmitTransition(ctx, user, c, reader); err != nil {
		t.Fatal("failed transaction left the installation locked", err)
	}
}

func TestTransitionDatabaseReferencesRemainImmutableAfterFailure(t *testing.T) {
	svc, user, command, reader := transitionDatabase(t)
	ctx := context.Background()
	first, err := svc.SubmitTransition(ctx, user, command, reader)
	if err != nil {
		t.Fatal(err)
	}
	// Fixture-only terminal result; no execution or installation version change.
	if _, err := svc.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET status='failed',phase='completed' WHERE operation_id=$1`, first.OperationID); err != nil {
		t.Fatal(err)
	}
	neverRead := func(context.Context, identity.IdentityContext, Installation, string) (TransitionSnapshot, TransitionSnapshot, error) {
		t.Fatal("replay re-resolved release contents")
		return TransitionSnapshot{}, TransitionSnapshot{}, nil
	}
	if replay, err := svc.SubmitTransition(ctx, user, command, neverRead); err != nil || replay.OperationID != first.OperationID || replay.Status != "failed" {
		t.Fatal("original operation was not preserved", err)
	}
	command.IdempotencyKey = "next-transition-key"
	for _, sourceChanged := range []bool{true, false} {
		changed := func(ctx context.Context, actor identity.IdentityContext, installed Installation, ref string) (TransitionSnapshot, TransitionSnapshot, error) {
			source, target, err := reader(ctx, actor, installed, ref)
			if sourceChanged {
				source.ManifestDigest = target.DashboardDigest
			} else {
				target.ManifestDigest = source.DashboardDigest
			}
			return source, target, err
		}
		if _, err := svc.SubmitTransition(ctx, user, command, changed); !errors.Is(err, identity.Conflict) {
			t.Fatal("same release reference changed across durable operations", err)
		}
	}
	if next, err := svc.SubmitTransition(ctx, user, command, reader); err != nil || next.OperationID == first.OperationID {
		t.Fatal("original immutable release could not be reused", err)
	}
}
