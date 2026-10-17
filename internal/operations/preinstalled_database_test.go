package operations

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type preinstalledObservation struct{ t *testing.T }

func (r *preinstalledObservation) Observe(context.Context, catalog.Entry) catalog.Observation {
	r.t.Error("preinstalled/command path called Kubernetes Observe")
	return catalog.Observation{}
}

func (r *preinstalledObservation) Discover(context.Context) ([]catalog.Item, error) {
	r.t.Error("preinstalled/command path called Kubernetes Discover")
	return nil, nil
}

type preinstalledDatabase struct {
	service  *Service
	actor    identity.IdentityContext
	binding  releasebinding.Binding
	stream   *appstate.StreamSession
	registry string
	original []byte
}

func preinstalledDatabaseFixture(t *testing.T) *preinstalledDatabase {
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
	session, err := auth.Bootstrap(ctx, "preinstalled-test", identity.BootstrapRequest{Username: "testadmin", Password: "Test-only-preinstalled-password-934!", OrganizationName: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "catalog.json")
	if err = os.WriteFile(path, []byte(`[{"app_id":"test-app","display_name":"Test","group_id":"video","brand":"vf","models":[],"source":"deploys/test.yaml","namespace":"test","workload_name":"test-app","version":"1.0.0","images":[{"component":"server","image":"example.invalid/test:1.0.0"}]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	reader := &preinstalledObservation{t}
	apps, err := catalog.Load(path, reader)
	if err != nil {
		t.Fatal(err)
	}
	digest := func(raw []byte) string { h := sha256.Sum256(raw); return "sha256:" + hex.EncodeToString(h[:]) }
	packageBytes, chartBytes := []byte(`{"appId":"test-app","version":"1.0.0"}`), []byte("controlled test chart bytes")
	binding := releasebinding.Binding{ReleaseRef: "test-app-v1", AppID: "test-app", AppVersion: "1.0.0", PackageDigest: digest(packageBytes), ChartDigest: digest(chartBytes), TemplateDigest: "sha256:" + strings.Repeat("c", 64)}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(releasebinding.SignedRelease{SchemaVersion: 1, Binding: binding, NotBefore: time.Now().Add(-time.Hour).UTC().Format(time.RFC3339), ExpiresAt: time.Now().Add(time.Hour).UTC().Format(time.RFC3339)})
	if err != nil {
		t.Fatal(err)
	}
	signature := ed25519.Sign(priv, append([]byte("verdantflare.preinstalled.release.v1\n"), payload...))
	raw, err := json.Marshal(map[string]any{"schemaVersion": 1, "keys": map[string]string{"test-key": base64.StdEncoding.EncodeToString(pub)}, "releases": []any{map[string]string{"releaseRef": binding.ReleaseRef, "payload": base64.StdEncoding.EncodeToString(payload), "signature": base64.StdEncoding.EncodeToString(signature), "keyId": "test-key", "packageRef": "package.json", "chartRef": "chart.tgz"}}})
	if err != nil {
		t.Fatal(err)
	}
	registry := filepath.Join(dir, "releases.json")
	for name, content := range map[string][]byte{"package.json": packageBytes, "chart.tgz": chartBytes, "releases.json": raw} {
		if err = os.WriteFile(filepath.Join(dir, name), content, 0600); err != nil {
			t.Fatal(err)
		}
	}
	memory, err := appstate.NewMemory(station, []appstate.Collector{{ID: "runtime", Namespaces: []string{"test"}}})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := memory.Open("runtime")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(stream.Close)
	syncID := uuid.NewString()
	if err = stream.Begin(syncID); err != nil {
		t.Fatal(err)
	}
	if err = stream.End(syncID, 0); err != nil {
		t.Fatal(err)
	}
	states := &appstate.Service{Pool: pool, Catalog: apps, StationID: station, Observations: memory}
	return &preinstalledDatabase{service: &Service{Pool: pool, Catalog: apps, AppStates: states, StationID: station, Releases: &releasebinding.Source{Path: registry}}, actor: session.IdentityContext, binding: binding, stream: stream, registry: registry, original: raw}
}
func (f *preinstalledDatabase) inventory(t *testing.T) PreinstalledInventory {
	t.Helper()
	out, err := f.service.PreinstalledInventory(context.Background(), f.actor, f.binding.AppID)
	if err != nil || !out.Complete {
		t.Fatal("inventory unavailable", err)
	}
	return out
}
func (f *preinstalledDatabase) command(t *testing.T, key string) PreinstalledCommand {
	t.Helper()
	return PreinstalledCommand{Command: Command{RequestID: uuid.Must(uuid.NewV7()).String(), IdempotencyKey: key, OrganizationID: f.actor.OrganizationID, StationID: f.actor.StationID, AppID: f.binding.AppID, AppVersion: f.binding.AppVersion, Action: "install"}, Release: f.binding, InventoryRevision: f.inventory(t).Revision}
}
func (f *preinstalledDatabase) counts(t *testing.T) [5]int {
	t.Helper()
	var out [5]int
	err := f.service.Pool.QueryRow(context.Background(), `SELECT (SELECT count(*) FROM station.app_operations),(SELECT count(*) FROM station.app_installations),(SELECT count(*) FROM station.preinstalled_operation_bindings),(SELECT count(*) FROM station.preinstalled_installation_bindings),(SELECT count(*) FROM station.audit_events WHERE action LIKE 'app.%.accepted')`).Scan(&out[0], &out[1], &out[2], &out[3], &out[4])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type preinstalledRuntimeResult struct {
	binding             releasebinding.Binding
	action, uid, result string
	unavailable         bool
}

func (r preinstalledRuntimeResult) Reconcile(_ context.Context, in *pb.AppRequest, _ ...grpc.CallOption) (*pb.AppProgress, error) {
	p := in.PreinstalledRelease
	if p == nil || p.ReleaseRef != r.binding.ReleaseRef || p.PackageDigest != r.binding.PackageDigest || p.ChartDigest != r.binding.ChartDigest || p.TemplateDigest != r.binding.TemplateDigest || in.Action != r.action || in.ExpectedWorkloadUid != r.uid {
		return nil, status.Error(codes.InvalidArgument, "lost fixed preinstalled evidence")
	}
	if r.unavailable {
		return nil, status.Error(codes.Unavailable, "test response lost")
	}
	out := &pb.AppProgress{OperationId: in.OperationId, Status: r.result, Phase: "completed"}
	if r.result == "failed" {
		out.Phase = "checking"
		out.ErrorCode = "CONFLICT"
		out.ErrorMessage = "Test adoption preflight conflict"
	}
	return out, nil
}
func (f *preinstalledDatabase) dispatch(t *testing.T, result, action, uid string) {
	t.Helper()
	found, err := f.service.DispatchOne(context.Background(), preinstalledRuntimeResult{binding: f.binding, action: action, uid: uid, result: result})
	if !found || err != nil {
		t.Fatal("dispatch", found, err)
	}
}

func TestPreinstalledDatabaseTransactionalBindingAndRecovery(t *testing.T) {
	f := preinstalledDatabaseFixture(t)
	ctx := context.Background()
	command := f.command(t, "preinstalled-transaction-1")
	invalid := f.actor
	invalid.SessionID = uuid.Must(uuid.NewV7()).String()
	if _, err := f.service.SubmitPreinstalled(ctx, invalid, command); err == nil {
		t.Fatal("late audit failure not exercised")
	}
	if got := f.counts(t); got != [5]int{} {
		t.Fatal("transaction leaked operation/installation/binding/audit", got)
	}
	first, err := f.service.SubmitPreinstalled(ctx, f.actor, command)
	if err != nil {
		t.Fatal(err)
	}
	if got := f.counts(t); got != [5]int{1, 1, 1, 1, 1} {
		t.Fatal(got)
	}
	view := f.inventory(t)
	if len(view.History) != 1 || view.History[0].Confirmed || view.History[0].Release == nil || *view.History[0].Release != f.binding || view.History[0].Installation.State != "pending" {
		t.Fatal("acceptance presented as confirmed", view)
	}
	command.Command.RequestID = "lost-response-replay"
	replayed, err := f.service.SubmitPreinstalled(ctx, f.actor, command)
	if err != nil || replayed.OperationID != first.OperationID || f.counts(t) != [5]int{1, 1, 1, 1, 1} {
		t.Fatal("replay duplicated transaction", err)
	}
	found, err := f.service.DispatchOne(ctx, preinstalledRuntimeResult{binding: f.binding, action: "install", unavailable: true})
	if !found || status.Code(err) != codes.Unavailable {
		t.Fatal(found, err)
	}
	if f.inventory(t).History[0].Confirmed {
		t.Fatal("lost runtime response confirmed installation")
	}
	f.dispatch(t, "failed", "install", "")
	failed := f.inventory(t)
	if failed.History[0].Confirmed || failed.History[0].Installation.State != "failed" {
		t.Fatal("failed execution confirmed", failed)
	}
	retry := f.command(t, "preinstalled-transaction-2")
	if _, err = f.service.SubmitPreinstalled(ctx, f.actor, retry); err != nil {
		t.Fatal(err)
	}
	f.dispatch(t, "succeeded", "install", "")
	installed := f.inventory(t)
	if !installed.History[0].Confirmed || installed.History[0].Installation.State != "installed" || installed.History[0].Installation.InstallationID != view.History[0].Installation.InstallationID {
		t.Fatal("successful retry lost installation identity", installed)
	}
	changed := retry
	changed.Release.TemplateDigest = "sha256:" + strings.Repeat("e", 64)
	if _, err = f.service.SubmitPreinstalled(ctx, f.actor, changed); !errors.Is(err, identity.Conflict) {
		t.Fatal("changed payload reused idempotency key", err)
	}
	if err = os.WriteFile(f.registry, []byte(`{"schemaVersion":1,"keys":{},"releases":[]}`), 0600); err != nil {
		t.Fatal(err)
	}
	withdrawn := f.command(t, "preinstalled-withdrawn")
	before := f.counts(t)
	if _, err = f.service.SubmitPreinstalled(ctx, f.actor, withdrawn); !errors.Is(err, releasebinding.ErrUntrusted) || f.counts(t) != before {
		t.Fatal("withdrawn release accepted", err)
	}
	if replayed, err = f.service.SubmitPreinstalled(ctx, f.actor, retry); err != nil || replayed.Status != "succeeded" || f.counts(t) != before {
		t.Fatal("read-only terminal replay changed after withdrawal", err)
	}
}

func TestPreinstalledDatabaseRemovedInventoryCAS(t *testing.T) {
	f := preinstalledDatabaseFixture(t)
	ctx := context.Background()
	initial := f.command(t, "preinstalled-before-remove")
	if _, err := f.service.SubmitPreinstalled(ctx, f.actor, initial); err != nil {
		t.Fatal(err)
	}
	f.dispatch(t, "succeeded", "install", "")
	stale := f.command(t, "preinstalled-stale-reinstall")
	remove := stale.Command
	remove.Action = "delete"
	remove.IdempotencyKey = "preinstalled-remove"
	if _, err := f.service.Submit(ctx, f.actor, remove); err != nil {
		t.Fatal(err)
	}
	if found, err := f.service.DispatchOne(ctx, installationRuntime{"succeeded"}); !found || err != nil {
		t.Fatal(found, err)
	}
	removed := f.inventory(t)
	if len(removed.History) != 1 || removed.History[0].Installation.State != "removed" {
		t.Fatal("tombstone missing", removed)
	}
	if _, err := f.service.SubmitPreinstalled(ctx, f.actor, stale); !errors.Is(err, identity.Conflict) {
		t.Fatal("stale history bypassed removal", err)
	}
	command := f.command(t, "preinstalled-without-consent")
	if _, err := f.service.SubmitPreinstalled(ctx, f.actor, command); !errors.Is(err, identity.Conflict) {
		t.Fatal("removed application recreated without intent", err)
	}
	command.ReinstallRemovedID = removed.History[0].Installation.InstallationID
	start := make(chan struct{})
	results := make(chan error, 2)
	for _, key := range []string{"preinstalled-reinstall-a", "preinstalled-reinstall-b"} {
		go func(key string) {
			<-start
			copy := command
			copy.Command.IdempotencyKey = key
			_, err := f.service.SubmitPreinstalled(ctx, f.actor, copy)
			results <- err
		}(key)
	}
	close(start)
	succeeded, conflicts := 0, 0
	for i := 0; i < 2; i++ {
		err := <-results
		if err == nil {
			succeeded++
		} else if errors.Is(err, identity.Conflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if succeeded != 1 || conflicts != 1 {
		t.Fatal("concurrent reinstall not serialized", succeeded, conflicts)
	}
	view := f.inventory(t)
	if len(view.History) != 2 || view.History[0].Installation.State != "removed" || view.History[1].Installation.State != "pending" || view.History[1].Installation.InstallationID == view.History[0].Installation.InstallationID || view.History[1].Confirmed {
		t.Fatal("reinstall lost tombstone or reused identity", view)
	}
	if got := f.counts(t); got != [5]int{3, 2, 2, 2, 3} {
		t.Fatal("concurrent reinstall leaked writes", got)
	}
}

func TestPreinstalledDatabaseLegacyAdoptionNeedsSuccessfulBoundOperation(t *testing.T) {
	f := preinstalledDatabaseFixture(t)
	ctx := context.Background()
	legacy := f.command(t, "legacy-install-before-adoption").Command
	if _, err := f.service.Submit(ctx, f.actor, legacy); err != nil {
		t.Fatal(err)
	}
	if found, err := f.service.DispatchOne(ctx, installationRuntime{"succeeded"}); !found || err != nil {
		t.Fatal(found, err)
	}
	uid := uuid.NewString()
	if err := f.stream.Upsert(appstate.ResourceObservation{Namespace: "test", Name: "test-app", UID: uid, ResourceVersion: "1", Generation: 1, ObservedGeneration: 1, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked", Metadata: &appstate.ResourceMetadata{Images: []appstate.ResourceImage{{Component: "server", Image: "example.invalid/test:1.0.0"}}, ImagesComplete: true}}); err != nil {
		t.Fatal(err)
	}
	command := f.command(t, "preinstalled-adoption-1")
	command.Command.Action = "adopt"
	command.Command.ExpectedWorkloadUID = uid
	if _, err := f.service.SubmitPreinstalled(ctx, f.actor, command); err != nil {
		t.Fatal(err)
	}
	view := f.inventory(t)
	if view.History[0].Installation.State != "installed" || view.History[0].Confirmed || view.ManagementStatus != "unmanaged" {
		t.Fatal("legacy installed inherited confirmation", view)
	}
	f.dispatch(t, "failed", "adopt", uid)
	view = f.inventory(t)
	if view.History[0].Installation.State != "installed" || view.History[0].Confirmed {
		t.Fatal("failed adoption confirmed release", view)
	}
	command = f.command(t, "preinstalled-adoption-2")
	command.Command.Action = "adopt"
	command.Command.ExpectedWorkloadUID = uid
	if _, err := f.service.SubmitPreinstalled(ctx, f.actor, command); err != nil {
		t.Fatal(err)
	}
	f.dispatch(t, "succeeded", "adopt", uid)
	if !f.inventory(t).History[0].Confirmed {
		t.Fatal("successful bound adoption not confirmed")
	}
}

func TestPreinstalledDatabaseLegacyVersionAdoptionDefersBinding(t *testing.T) {
	f := preinstalledDatabaseFixture(t)
	ctx := context.Background()
	legacy := f.command(t, "legacy-version-install").Command
	if _, err := f.service.Submit(ctx, f.actor, legacy); err != nil {
		t.Fatal(err)
	}
	if found, err := f.service.DispatchOne(ctx, installationRuntime{"succeeded"}); !found || err != nil {
		t.Fatal(found, err)
	}
	id := f.inventory(t).History[0].Installation.InstallationID
	// Reproduce an old unbound installation whose durable version predates its workload.
	if _, err := f.service.Pool.Exec(ctx, `UPDATE station.app_installations SET app_version='0.1.0' WHERE installation_id=$1`, id); err != nil {
		t.Fatal(err)
	}
	uid := uuid.NewString()
	if err := f.stream.Upsert(appstate.ResourceObservation{Namespace: "test", Name: "test-app", UID: uid, ResourceVersion: "1", Generation: 1, ObservedGeneration: 1, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked", Metadata: &appstate.ResourceMetadata{Images: []appstate.ResourceImage{{Component: "server", Image: "example.invalid/test:1.0.0"}}, ImagesComplete: true}}); err != nil {
		t.Fatal(err)
	}
	check := func(version string, bound bool) {
		t.Helper()
		item := f.inventory(t).History[0]
		if item.Installation.InstallationID != id || item.Installation.AppVersion != version || item.Installation.State != "installed" || (item.Release != nil) != bound {
			t.Fatal("premature or incorrect installation mutation", item)
		}
	}
	command := f.command(t, "legacy-version-adopt")
	command.Command.Action, command.Command.ExpectedWorkloadUID = "adopt", uid
	if _, err := f.service.Submit(ctx, f.actor, command.Command); !errors.Is(err, identity.Conflict) {
		t.Fatal("unsigned adoption accepted", err)
	}
	first, err := f.service.SubmitPreinstalled(ctx, f.actor, command)
	if err != nil {
		t.Fatal(err)
	}
	check("0.1.0", false)
	if found, err := f.service.DispatchOne(ctx, preinstalledRuntimeResult{binding: f.binding, action: "adopt", uid: uid, unavailable: true}); !found || status.Code(err) != codes.Unavailable {
		t.Fatal(found, err)
	}
	check("0.1.0", false)
	f.dispatch(t, "failed", "adopt", uid)
	check("0.1.0", false)
	replay, err := f.service.SubmitPreinstalled(ctx, f.actor, command)
	if err != nil || replay.OperationID != first.OperationID {
		t.Fatal("failed replay duplicated operation", err)
	}
	command = f.command(t, "legacy-version-adopt-retry")
	command.Command.Action, command.Command.ExpectedWorkloadUID = "adopt", uid
	accepted, err := f.service.SubmitPreinstalled(ctx, f.actor, command)
	if err != nil {
		t.Fatal(err)
	}
	check("0.1.0", false)
	f.dispatch(t, "succeeded", "adopt", uid)
	check("1.0.0", true)
	if !f.inventory(t).History[0].Confirmed {
		t.Fatal("successful adoption not confirmed")
	}
	counts := f.counts(t)
	replay, err = f.service.SubmitPreinstalled(ctx, f.actor, command)
	if err != nil || replay.OperationID != accepted.OperationID || f.counts(t) != counts {
		t.Fatal("successful replay changed binding", err)
	}
	check("1.0.0", true)
}

func TestPreinstalledDatabaseMemoryInventoryAndDurableRevision(t *testing.T) {
	f := preinstalledDatabaseFixture(t)
	ctx := context.Background()
	initial := f.inventory(t)
	if initial.WorkloadUID != "" || len(initial.History) != 0 {
		t.Fatal("empty inventory fabricated identity", initial)
	}
	object := appstate.ResourceObservation{Namespace: "test", Name: "test-app", UID: uuid.NewString(), ResourceVersion: "1", Generation: 1, ObservedGeneration: 1, Desired: 1, Replicas: 1, Ready: 1, Updated: 1, Available: 1, Health: "not_checked", Metadata: &appstate.ResourceMetadata{Images: []appstate.ResourceImage{{Component: "server", Image: "example.invalid/test:1.0.0"}}, ImagesComplete: true}}
	if err := f.stream.Upsert(object); err != nil {
		t.Fatal(err)
	}
	observed := f.inventory(t)
	if observed.ManagementStatus != "unmanaged" || observed.WorkloadUID != object.UID || observed.Revision != initial.Revision {
		t.Fatal("observation became durable history", observed)
	}
	object.UID = uuid.NewString()
	if err := f.stream.Upsert(object); err != nil {
		t.Fatal(err)
	}
	replaced := f.inventory(t)
	if replaced.WorkloadUID == observed.WorkloadUID || replaced.WorkloadUID != object.UID || replaced.Revision != observed.Revision {
		t.Fatal("candidate replacement lost UID or altered history CAS", replaced)
	}
	readOnly := f.actor
	readOnly.Scopes = []string{"app:read"}
	view, err := f.service.PreinstalledInventory(ctx, readOnly, f.binding.AppID)
	if err != nil || view.Complete || view.WorkloadUID != "" {
		t.Fatal("read-only actor received adoption candidate", view, err)
	}
	states := f.service.AppStates
	memory, err := appstate.NewMemory(f.actor.StationID, []appstate.Collector{{ID: "runtime", Namespaces: []string{"test"}}})
	if err != nil {
		t.Fatal(err)
	}
	f.service.AppStates = &appstate.Service{Pool: f.service.Pool, Catalog: f.service.Catalog, StationID: f.actor.StationID, Observations: memory}
	view, err = f.service.PreinstalledInventory(ctx, f.actor, f.binding.AppID)
	if err != nil || view.Complete || view.WorkloadUID != "" || view.Revision != initial.Revision {
		t.Fatal("Core restart fabricated absence or lost durable revision", view, err)
	}
	f.service.AppStates = nil
	if _, err = f.service.PreinstalledInventory(ctx, f.actor, f.binding.AppID); !errors.Is(err, appstate.ErrUnavailable) {
		t.Fatal("missing state service silently fell back", err)
	}
	f.service.AppStates = states
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = f.service.PreinstalledInventory(canceled, f.actor, f.binding.AppID); err == nil {
		t.Fatal("database failure returned complete inventory")
	}
}

// This transaction-level test seeds historical metadata only in a disposable DB.
// Admission signature/inventory checks remain covered by the service tests above.
func TestPreinstalledDatabaseVideoSourceRecoveryTransaction(t *testing.T) {
	f := preinstalledDatabaseFixture(t)
	ctx := context.Background()
	from, to := videoOriginalSource(), videoObservedSource()
	// Create the historical Video identity from the outset. Application scope
	// is immutable, even in an isolated fixture; never disable its DB trigger.
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(path, []byte(`[{"app_id":"video-mcp","display_name":"Video","group_id":"video","brand":"vf","models":[],"source":"deploys/test.yaml","namespace":"test","workload_name":"video-mcp","version":"0.17.28","images":[{"component":"server","image":"example.invalid/video:0.17.28"}]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	apps, err := catalog.Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.service.Catalog = apps
	c := Command{RequestID: "video-history", IdempotencyKey: "video-history", OrganizationID: f.actor.OrganizationID, StationID: f.actor.StationID, AppID: from.AppID, AppVersion: from.AppVersion, Action: "install"}
	op, err := f.service.Submit(ctx, f.actor, c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.DispatchOne(ctx, installationRuntime{"succeeded"}); err != nil {
		t.Fatal(err)
	}
	items, err := f.service.ListInstallations(ctx, f.actor)
	if err != nil || len(items) != 1 {
		t.Fatal(err)
	}
	id := items[0].InstallationID
	prior, _ := json.Marshal(from)
	if _, err = f.service.Pool.Exec(ctx, `INSERT INTO station.preinstalled_installation_bindings(installation_id,binding) VALUES($1,$2)`, id, prior); err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.Pool.Exec(ctx, `INSERT INTO station.preinstalled_operation_bindings(operation_id,binding) VALUES($1,$2)`, op.OperationID, prior); err != nil {
		t.Fatal(err)
	}
	check := func(want releasebinding.Binding) {
		t.Helper()
		var version string
		var raw []byte
		if err := f.service.Pool.QueryRow(ctx, `SELECT i.app_version,b.binding FROM station.app_installations i JOIN station.preinstalled_installation_bindings b USING(installation_id) WHERE installation_id=$1`, id).Scan(&version, &raw); err != nil {
			t.Fatal(err)
		}
		var got releasebinding.Binding
		if json.Unmarshal(raw, &got) != nil || got != want || version != want.AppVersion {
			t.Fatal("version and binding changed non-atomically")
		}
	}
	c.AppID, c.AppVersion, c.Action = to.AppID, to.AppVersion, "adopt"
	tx, err := f.service.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = reserveInstallation(ctx, tx, f.actor, c, nil); !errors.Is(err, identity.Conflict) {
		t.Fatal("unsigned recovery admitted", err)
	}
	gotID, err := reserveInstallation(ctx, tx, f.actor, c, &PreinstalledCommand{Command: c, Release: to})
	if err != nil || gotID != id {
		t.Fatal("recovery lost installation", err)
	}
	// A second operation models an admitted recovery and retains the old history.
	newOp := uuid.Must(uuid.NewV7()).String()
	if _, err = tx.Exec(ctx, `INSERT INTO station.app_operations SELECT (jsonb_populate_record(NULL::station.app_operations,to_jsonb(o)||jsonb_build_object('operation_id',$2::text,'idempotency_key','video-recovery','app_id','video-mcp','app_version','0.17.30','action','adopt','status','accepted'))).* FROM station.app_operations o WHERE operation_id=$1`, op.OperationID, newOp); err != nil {
		t.Fatal(err)
	}
	if err = bindInstallationOperation(ctx, tx, id, newOp); err != nil {
		t.Fatal(err)
	}
	if err = bindPreinstalled(ctx, tx, newOp, id, to); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	check(from)
	// Changed metadata between admission and completion cannot be overwritten.
	conflictTx, err := f.service.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed := from
	changed.TemplateDigest = to.TemplateDigest
	changedRaw, _ := json.Marshal(changed)
	if _, err = conflictTx.Exec(ctx, `UPDATE station.preinstalled_installation_bindings SET binding=$2 WHERE installation_id=$1`, id, changedRaw); err != nil {
		conflictTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = recordInstallationProgress(ctx, conflictTx, newOp, "adopt", "succeeded"); !errors.Is(err, identity.Conflict) {
		conflictTx.Rollback(ctx)
		t.Fatal("changed source overwritten", err)
	}
	if err = conflictTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	check(from)
	for _, result := range []string{"running", "failed", "succeeded", "succeeded"} {
		tx, err := f.service.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = recordInstallationProgress(ctx, tx, newOp, "adopt", result)
		if err != nil {
			tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if result == "succeeded" {
			check(to)
		} else {
			check(from)
		}
	}
	var history []byte
	if err = f.service.Pool.QueryRow(ctx, `SELECT binding FROM station.preinstalled_operation_bindings WHERE operation_id=$1`, op.OperationID).Scan(&history); err != nil {
		t.Fatal(err)
	}
	var original releasebinding.Binding
	if json.Unmarshal(history, &original) != nil || original != from {
		t.Fatal("original signed history replaced")
	}
}

func TestPreinstalledDatabaseVideoParallelSourceRecoveryTransaction(t *testing.T) {
	f := preinstalledDatabaseFixture(t)
	ctx := context.Background()
	from, to := videoObservedSource(), videoParallelObservedSource()
	// Create the historical Video identity from the outset. Application scope
	// is immutable, even in an isolated fixture; never disable its DB trigger.
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(path, []byte(`[{"app_id":"video-mcp","display_name":"Video","group_id":"video","brand":"vf","models":[],"source":"deploys/test.yaml","namespace":"test","workload_name":"video-mcp","version":"0.17.31","images":[{"component":"server","image":"example.invalid/video:0.17.31"}]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	apps, err := catalog.Load(path, nil)
	if err != nil {
		t.Fatal(err)
	}
	f.service.Catalog = apps
	c := Command{RequestID: "video-history", IdempotencyKey: "video-history", OrganizationID: f.actor.OrganizationID, StationID: f.actor.StationID, AppID: from.AppID, AppVersion: "0.17.31", Action: "install"}
	op, err := f.service.Submit(ctx, f.actor, c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.DispatchOne(ctx, installationRuntime{"succeeded"}); err != nil {
		t.Fatal(err)
	}
	items, err := f.service.ListInstallations(ctx, f.actor)
	if err != nil || len(items) != 1 {
		t.Fatal(err)
	}
	id := items[0].InstallationID
	prior, _ := json.Marshal(from)
	if _, err = f.service.Pool.Exec(ctx, `INSERT INTO station.preinstalled_installation_bindings(installation_id,binding) VALUES($1,$2)`, id, prior); err != nil {
		t.Fatal(err)
	}
	if _, err = f.service.Pool.Exec(ctx, `INSERT INTO station.preinstalled_operation_bindings(operation_id,binding) VALUES($1,$2)`, op.OperationID, prior); err != nil {
		t.Fatal(err)
	}
	check := func(want releasebinding.Binding) {
		t.Helper()
		var version string
		var raw []byte
		if err := f.service.Pool.QueryRow(ctx, `SELECT i.app_version,b.binding FROM station.app_installations i JOIN station.preinstalled_installation_bindings b USING(installation_id) WHERE installation_id=$1`, id).Scan(&version, &raw); err != nil {
			t.Fatal(err)
		}
		var got releasebinding.Binding
		expectedVersion := want.AppVersion
		if want == from {
			expectedVersion = "0.17.31"
		}
		if json.Unmarshal(raw, &got) != nil || got != want || version != expectedVersion {
			t.Fatal("version and binding changed non-atomically")
		}
	}
	c.AppID, c.AppVersion, c.Action = to.AppID, to.AppVersion, "adopt"
	tx, err := f.service.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if _, err = reserveInstallation(ctx, tx, f.actor, c, nil); !errors.Is(err, identity.Conflict) {
		t.Fatal("unsigned recovery admitted", err)
	}

	if _, err = reserveInstallation(ctx, tx, f.actor, c, &PreinstalledCommand{Command: c, Release: to}); !errors.Is(err, identity.Conflict) {
		t.Fatal("unproven transition chain accepted", err)
	}
	transitionID := uuid.Must(uuid.NewV7()).String()
	_, err = tx.Exec(ctx, `INSERT INTO station.app_installation_transitions(operation_id,installation_id,user_id,organization_id,station_id,app_id,request_id,idempotency_key,request_hash,action,expected_revision,source_version,target_version,source_snapshot,target_snapshot,status,phase)
    VALUES($1,$2,$3,$4,$5,'video-mcp','credential-chain','credential-chain',decode(repeat('00',32),'hex'),'upgrade',1,'0.17.30','0.17.31',$6,$7,'failed','completed')`, transitionID, id, f.actor.UserID, f.actor.OrganizationID, f.actor.StationID, videoCredentialSourceSnapshot, videoCredentialTargetSnapshot)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = reserveInstallation(ctx, tx, f.actor, c, &PreinstalledCommand{Command: c, Release: to}); !errors.Is(err, identity.Conflict) {
		t.Fatal("failed transition chain accepted", err)
	}
	if _, err = tx.Exec(ctx, `UPDATE station.app_installation_transitions SET status='succeeded' WHERE operation_id=$1`, transitionID); err != nil {
		t.Fatal(err)
	}
	gotID, err := reserveInstallation(ctx, tx, f.actor, c, &PreinstalledCommand{Command: c, Release: to})
	if err != nil || gotID != id {
		t.Fatal("recovery lost installation", err)
	}
	// A second operation models an admitted recovery and retains the old history.
	newOp := uuid.Must(uuid.NewV7()).String()
	if _, err = tx.Exec(ctx, `INSERT INTO station.app_operations SELECT (jsonb_populate_record(NULL::station.app_operations,to_jsonb(o)||jsonb_build_object('operation_id',$2::text,'idempotency_key','video-recovery','app_id','video-mcp','app_version','0.17.35','action','adopt','status','accepted'))).* FROM station.app_operations o WHERE operation_id=$1`, op.OperationID, newOp); err != nil {
		t.Fatal(err)
	}
	if err = bindInstallationOperation(ctx, tx, id, newOp); err != nil {
		t.Fatal(err)
	}
	if err = bindPreinstalled(ctx, tx, newOp, id, to); err != nil {
		t.Fatal(err)
	}
	if err = tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	check(from)
	// Changed metadata between admission and completion cannot be overwritten.
	conflictTx, err := f.service.Pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	changed := from
	changed.TemplateDigest = to.TemplateDigest
	changedRaw, _ := json.Marshal(changed)
	if _, err = conflictTx.Exec(ctx, `UPDATE station.preinstalled_installation_bindings SET binding=$2 WHERE installation_id=$1`, id, changedRaw); err != nil {
		conflictTx.Rollback(ctx)
		t.Fatal(err)
	}
	if err = recordInstallationProgress(ctx, conflictTx, newOp, "adopt", "succeeded"); !errors.Is(err, identity.Conflict) {
		conflictTx.Rollback(ctx)
		t.Fatal("changed source overwritten", err)
	}
	if err = conflictTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	check(from)
	for _, result := range []string{"running", "failed", "succeeded", "succeeded"} {
		tx, err := f.service.Pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		err = recordInstallationProgress(ctx, tx, newOp, "adopt", result)
		if err != nil {
			tx.Rollback(ctx)
			t.Fatal(err)
		}
		if err = tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		if result == "succeeded" {
			check(to)
		} else {
			check(from)
		}
	}
	var history []byte
	if err = f.service.Pool.QueryRow(ctx, `SELECT binding FROM station.preinstalled_operation_bindings WHERE operation_id=$1`, op.OperationID).Scan(&history); err != nil {
		t.Fatal(err)
	}
	var original releasebinding.Binding
	if json.Unmarshal(history, &original) != nil || original != from {
		t.Fatal("original signed history replaced")
	}
}
