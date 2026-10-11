package operations

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func commandFixture() AppCenterCommand {
	id := "00000000-0000-7000-8000-000000000004"
	revision := int64(3)
	uid := "00000000-0000-4000-8000-000000000005"
	digest := "sha256:" + strings.Repeat("a", 64)
	return AppCenterCommand{1, Command{RequestID: "trace-1", IdempotencyKey: "command-key-1",
		OrganizationID: "00000000-0000-7000-8000-000000000002", StationID: "00000000-0000-7000-8000-000000000003",
		AppID: "test-app", AppVersion: "1.9.0", Action: "start"}, AppCenterExpected{&id, &revision, "1.9.0", &digest, &uid, digest}}
}

func TestAppCenterCommandProtocol(t *testing.T) {
	in := commandFixture()
	raw, _ := json.Marshal(in)
	if _, err := DecodeAppCenterCommand(strings.NewReader(string(raw))); err != nil {
		t.Fatal(err)
	}
	install := in
	install.Command.Action = "install"
	install.Expected.InstallationID, install.Expected.InstallationRevision, install.Expected.WorkloadUID = nil, nil, nil
	installRaw, _ := json.Marshal(install)
	if _, err := DecodeAppCenterCommand(strings.NewReader(string(installRaw))); err != nil {
		t.Fatal("explicit nulls rejected", err)
	}
	for _, bad := range []string{
		strings.Replace(string(raw), `"schemaVersion":1`, `"schemaVersion":1,"schemaVersion":1`, 1),
		strings.Replace(string(raw), `"schemaVersion":1`, `"schemaVersion":2`, 1),
		strings.Replace(string(raw), `"schemaVersion":1,`, ``, 1),
		strings.Replace(string(raw), `"start"`, `"adopt"`, 1),
		strings.Replace(string(raw), `"start"`, `"upgrade"`, 1),
		strings.Replace(string(raw), `"installationRevision":3`, `"installationRevision":0`, 1),
		string(raw) + `{}`,
		strings.Replace(string(raw), `"command":{`, `"command":{"endpoint":"https://other.invalid",`, 1),
		strings.Replace(string(raw), `"installationRevision":3`, `"installationRevision":9007199254740992`, 1),
		strings.Repeat(" ", 16<<10) + string(raw),
	} {
		if _, err := DecodeAppCenterCommand(strings.NewReader(bad)); !errors.Is(err, identity.Invalid) {
			t.Fatal("invalid command accepted", err)
		}
	}
	initial := appCenterHash(in)
	in.Command.RequestID = "new-trace"
	if appCenterHash(in) != initial {
		t.Fatal("trace changed replay identity")
	}
	if commandHash(in.Command, nil) == initial {
		t.Fatal("new and old protocol hash domains collided")
	}
	in.Expected.PolicyRevision = "sha256:" + strings.Repeat("b", 64)
	if appCenterHash(in) == initial {
		t.Fatal("expected policy omitted from replay identity")
	}
}

func TestAppCenterCommandExecutionBinding(t *testing.T) {
	in := commandFixture()
	valid := AppCenterExecution{AppID: in.Command.AppID, AppVersion: in.Command.AppVersion, Action: in.Command.Action, SchemaVersion: 1, ReleaseDigest: *in.Expected.ReleaseDigest, TemplateDigest: "sha256:" + strings.Repeat("b", 64), PolicyRevision: in.Expected.PolicyRevision, WorkloadUID: *in.Expected.WorkloadUID}
	if err := validateAppCenterExecution(in, valid); err != nil {
		t.Fatal(err)
	}
	for _, alter := range []func(*AppCenterExecution){
		func(v *AppCenterExecution) { v.AppID = "different-app" },
		func(v *AppCenterExecution) { v.AppVersion = "2.0.0" },
		func(v *AppCenterExecution) { v.Action = "stop" },
		func(v *AppCenterExecution) { v.SchemaVersion = 2 },
		func(v *AppCenterExecution) { v.ReleaseDigest = "sha256:" + strings.Repeat("c", 64) },
		func(v *AppCenterExecution) { v.TemplateDigest = "latest" },
		func(v *AppCenterExecution) { v.PolicyRevision = "sha256:" + strings.Repeat("c", 64) },
		func(v *AppCenterExecution) { v.WorkloadUID = uuid.NewString() },
	} {
		bad := valid
		alter(&bad)
		if err := validateAppCenterExecution(in, bad); err == nil {
			t.Fatal("execution identity substitution accepted")
		}
	}
	in.Expected.ReleaseDigest = nil
	if err := validateAppCenterExecution(in, valid); !errors.Is(err, ErrAppCenterRelease) {
		t.Fatal("unverified release accepted", err)
	}
}

func appCenterDatabase(t *testing.T) (*Service, identity.IdentityContext, AppCenterCommand, AppCenterAdmission) {
	t.Helper()
	svc, actor, transition, _ := transitionDatabase(t)
	in := commandFixture()
	in.Command.OrganizationID, in.Command.StationID = actor.OrganizationID, svc.StationID
	in.Expected.InstallationID, in.Expected.InstallationRevision = &transition.InstallationID, &transition.ExpectedRevision
	reader := func(_ context.Context, _ identity.IdentityContext, c Command, installed *Installation) (AppCenterExecution, error) {
		if c.AppVersion != "1.9.0" || c.Action != "start" || installed == nil || installed.InstallationID != transition.InstallationID {
			return AppCenterExecution{}, ErrAppCenterRelease
		}
		return AppCenterExecution{AppID: c.AppID, AppVersion: c.AppVersion, Action: c.Action, SchemaVersion: 1, ReleaseDigest: *in.Expected.ReleaseDigest, TemplateDigest: "sha256:" + strings.Repeat("b", 64), PolicyRevision: in.Expected.PolicyRevision, WorkloadUID: *in.Expected.WorkloadUID}, nil
	}
	return svc, actor, in, reader
}

func TestAppCenterCommandDatabaseReplayAndDispatchGate(t *testing.T) {
	svc, actor, in, reader := appCenterDatabase(t)
	ctx := context.Background()
	// A fixed installed release must remain selectable without Catalog's latest.
	svc.Catalog = nil
	first, err := svc.SubmitAppCenter(ctx, actor, in, reader)
	if err != nil || first.Status != "accepted" {
		t.Fatal(first, err)
	}
	var prepared, execution []byte
	if err := svc.Pool.QueryRow(ctx, `SELECT prepared,execution FROM station.appcenter_operation_inputs WHERE operation_id=$1`, first.OperationID).Scan(&prepared, &execution); err != nil {
		t.Fatal(err)
	}
	var saved AppCenterCommand
	var bound AppCenterExecution
	if json.Unmarshal(prepared, &saved) != nil || json.Unmarshal(execution, &bound) != nil || appCenterHash(saved) != appCenterHash(in) || bound.TemplateDigest != "sha256:"+strings.Repeat("b", 64) {
		t.Fatal("durable input missing")
	}
	// A fresh Service has no view cache, catalog, or admission reader. Replay must
	// still return the original receipt, even after acceptance changed revision.
	restarted := &Service{Pool: svc.Pool, StationID: svc.StationID}
	in.Command.RequestID = "recovered-trace"
	replay, err := restarted.SubmitAppCenter(ctx, actor, in, nil)
	if err != nil || replay.OperationID != first.OperationID || replay.RequestID != first.RequestID {
		t.Fatal("replay lost", err)
	}
	changed := in
	changed.Expected.PolicyRevision = "sha256:" + strings.Repeat("c", 64)
	if _, err = restarted.SubmitAppCenter(ctx, actor, changed, nil); !errors.Is(err, identity.Conflict) {
		t.Fatal("changed body replay", err)
	}
	changed = in
	changed.Expected.AppVersion = "9.0.0"
	if _, err = restarted.SubmitAppCenter(ctx, actor, changed, nil); !errors.Is(err, identity.Conflict) {
		t.Fatal("semantic preconditions ran before replay conflict", err)
	}
	if _, err = restarted.Submit(ctx, actor, in.Command); !errors.Is(err, identity.Conflict) {
		t.Fatal("cross protocol replay", err)
	}
	denied := actor
	denied.Scopes = []string{"app:read"}
	if _, err = restarted.SubmitAppCenter(ctx, denied, in, nil); !errors.Is(err, identity.Denied) {
		t.Fatal("revoked permission replay", err)
	}
	other := actor
	other.UserID = uuid.Must(uuid.NewV7()).String()
	if op, err := restarted.SubmitAppCenter(ctx, other, in, nil); err == nil || op.OperationID != "" {
		t.Fatal("foreign actor receipt exposed")
	}
	// nil Runtime client deliberately panics if the new journal is stripped and
	// accidentally dispatched through the old protocol.
	if found, err := svc.DispatchOne(ctx, nil); !found || !errors.Is(err, ErrAppCenterDispatch) {
		t.Fatal("execution binding discarded", found, err)
	}
	var status string
	if err := svc.Pool.QueryRow(ctx, `SELECT status FROM station.app_operations WHERE operation_id=$1`, first.OperationID).Scan(&status); err != nil || status != "accepted" {
		t.Fatal("gate changed execution outcome", err)
	}
}

func TestAppCenterCommandDatabaseStaleAndTransactionRollback(t *testing.T) {
	svc, actor, in, reader := appCenterDatabase(t)
	ctx := context.Background()
	original, err := svc.GetInstallation(ctx, actor, *in.Expected.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	for _, variant := range []string{"identity", "revision", "version", "workload", "release", "policy", "missing-reader", "foreign-org"} {
		bad := in
		caller := actor
		use := reader
		switch variant {
		case "identity":
			id := uuid.Must(uuid.NewV7()).String()
			bad.Expected.InstallationID = &id
		case "revision":
			rev := original.Revision + 1
			bad.Expected.InstallationRevision = &rev
		case "version":
			bad.Expected.AppVersion = "2.0.0"
			bad.Command.AppVersion = "2.0.0"
		case "workload":
			uid := uuid.NewString()
			bad.Expected.WorkloadUID = &uid
		case "release":
			digest := "sha256:" + strings.Repeat("d", 64)
			bad.Expected.ReleaseDigest = &digest
		case "policy":
			bad.Expected.PolicyRevision = "sha256:" + strings.Repeat("e", 64)
		case "missing-reader":
			use = nil
		case "foreign-org":
			caller.OrganizationID = uuid.Must(uuid.NewV7()).String()
			bad.Command.OrganizationID = caller.OrganizationID
		}
		if _, err := svc.SubmitAppCenter(ctx, caller, bad, use); err == nil {
			t.Fatalf("accepted %s", variant)
		}
	}
	var count int
	if err := svc.Pool.QueryRow(ctx, `SELECT count(*) FROM station.appcenter_operation_inputs`).Scan(&count); err != nil || count != 0 {
		t.Fatal("rejected request persisted", err)
	}
	// Force the journal write to fail after reserve/bind: the whole transaction,
	// including app_operations and installation revision, must roll back.
	if _, err := svc.Pool.Exec(ctx, `ALTER TABLE station.appcenter_operation_inputs ADD CONSTRAINT deny_test_input CHECK(false) NOT VALID`); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.SubmitAppCenter(ctx, actor, in, reader); err == nil {
		t.Fatal("journal failure ignored")
	}
	after, err := svc.GetInstallation(ctx, actor, original.InstallationID)
	if err != nil || after.Revision != original.Revision || after.LastOperationID != original.LastOperationID {
		t.Fatal("partial acceptance committed", err)
	}
	if err := svc.Pool.QueryRow(ctx, `SELECT count(*) FROM station.app_operations`).Scan(&count); err != nil || count != 1 {
		t.Fatal("orphan operation", err)
	}
}

func TestAppCenterCommandDatabaseConcurrentAcceptance(t *testing.T) {
	svc, actor, in, reader := appCenterDatabase(t)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, key := range []string{"concurrent-key-1", "concurrent-key-2"} {
		wg.Add(1)
		go func(key string) {
			defer wg.Done()
			copy := in
			copy.Command.IdempotencyKey = key
			_, err := svc.SubmitAppCenter(context.Background(), actor, copy, reader)
			results <- err
		}(key)
	}
	wg.Wait()
	close(results)
	accepted, rejected := 0, 0
	for err := range results {
		if err == nil {
			accepted++
		} else if errors.Is(err, ErrAppCenterBinding) || errors.Is(err, identity.Conflict) {
			rejected++
		} else {
			t.Fatal(err)
		}
	}
	if accepted != 1 || rejected != 1 {
		t.Fatal("concurrent acceptance", accepted, rejected)
	}
}

func TestAppCenterCommandDatabaseRemovedReinstalledAndTransition(t *testing.T) {
	svc, actor, in, reader := appCenterDatabase(t)
	ctx := context.Background()
	oldID := *in.Expected.InstallationID
	if _, err := svc.Pool.Exec(ctx, `UPDATE station.app_installations SET state='removed',revision=revision+1 WHERE installation_id=$1`, oldID); err != nil {
		t.Fatal(err)
	}
	// The same app version receives a NEW installation identity through the
	// existing installation path; an old prepared command cannot target it.
	c := in.Command
	c.Action = "install"
	c.IdempotencyKey = "replacement-install"
	if _, err := svc.Submit(ctx, actor, c); err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DispatchOne(ctx, installationRuntime{"succeeded"}); err != nil {
		t.Fatal(err)
	}
	current, err := svc.ListInstallations(ctx, actor)
	if err != nil || len(current) != 1 || current[0].InstallationID == oldID {
		t.Fatal("installation replacement", err)
	}
	if _, err := svc.SubmitAppCenter(ctx, actor, in, reader); !errors.Is(err, ErrAppCenterBinding) {
		t.Fatal("old identity rebound", err)
	}
	in.Expected.InstallationID = &current[0].InstallationID
	in.Expected.InstallationRevision = &current[0].Revision
	transition, _, source, target := transitionInputs()
	transition.InstallationID = current[0].InstallationID
	transition.ExpectedRevision = current[0].Revision
	if _, err := svc.SubmitTransition(ctx, actor, transition, func(context.Context, identity.IdentityContext, Installation, string) (TransitionSnapshot, TransitionSnapshot, error) {
		return source, target, nil
	}); err != nil {
		t.Fatal(err)
	}
	reader = func(_ context.Context, _ identity.IdentityContext, c Command, _ *Installation) (AppCenterExecution, error) {
		return AppCenterExecution{AppID: c.AppID, AppVersion: c.AppVersion, Action: c.Action, SchemaVersion: 1, ReleaseDigest: *in.Expected.ReleaseDigest, TemplateDigest: "sha256:" + strings.Repeat("b", 64), PolicyRevision: in.Expected.PolicyRevision, WorkloadUID: *in.Expected.WorkloadUID}, nil
	}
	if _, err := svc.SubmitAppCenter(ctx, actor, in, reader); !errors.Is(err, identity.Conflict) {
		t.Fatal("transition exclusion bypassed", err)
	}
}

func TestAppCenterCommandDatabaseInstallRequiresAbsence(t *testing.T) {
	svc, actor, in, _ := appCenterDatabase(t)
	ctx := context.Background()
	in.Command.Action = "install"
	in.Command.AppID = "new-app"
	in.Expected.InstallationID = nil
	in.Expected.InstallationRevision = nil
	in.Expected.WorkloadUID = nil
	reader := func(_ context.Context, _ identity.IdentityContext, c Command, installed *Installation) (AppCenterExecution, error) {
		if installed != nil {
			t.Error("install reader received an active installation")
		}
		return AppCenterExecution{AppID: c.AppID, AppVersion: c.AppVersion, Action: c.Action, SchemaVersion: 1, ReleaseDigest: *in.Expected.ReleaseDigest, TemplateDigest: "sha256:" + strings.Repeat("b", 64), PolicyRevision: in.Expected.PolicyRevision}, nil
	}
	first, err := svc.SubmitAppCenter(ctx, actor, in, reader)
	if err != nil || first.Status != "accepted" {
		t.Fatal(first, err)
	}
	in.Command.IdempotencyKey = "duplicate-install-key"
	if _, err := svc.SubmitAppCenter(ctx, actor, in, reader); !errors.Is(err, ErrAppCenterBinding) {
		t.Fatal("second install accepted", err)
	}
	var count int
	if err := svc.Pool.QueryRow(ctx, `SELECT count(*) FROM station.app_installations WHERE app_id='new-app'`).Scan(&count); err != nil || count != 1 {
		t.Fatal("duplicate installation", err)
	}
}
