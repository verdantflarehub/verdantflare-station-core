package operations

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appstate"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
)

func TestAppCenterAdmissionDatabaseFactsSubmitReplay(t *testing.T) {
	svc, actor, transition, _ := transitionDatabase(t)
	source, _, _, policy, runtime := admissionFixture(t)
	svc.AppCenterAdmissionSource = source
	ctx := context.Background()
	installed, err := svc.GetInstallation(ctx, actor, transition.InstallationID)
	if err != nil {
		t.Fatal(err)
	}
	local, err := source.resolve(ctx, actor, admissionCommand(actor, installed, "start"), &installed)
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(path, []byte(`[{"app_id":"test-app","display_name":"Test","group_id":"video","brand":"vf","models":[],"source":"deploys/test.yaml","namespace":"apps","workload_name":"test-app","version":"1.9.0","images":[{"component":"server","image":"example.invalid/test:1.9.0"}]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	svc.Catalog, err = catalog.Load(path, noAppCenterProbe{})
	if err != nil {
		t.Fatal(err)
	}
	memory, err := appstate.NewMemory(actor.StationID, []appstate.Collector{{ID: "runtime", Namespaces: []string{"apps"}}})
	if err != nil {
		t.Fatal(err)
	}
	svc.AppStates = &appstate.Service{Pool: svc.Pool, StationID: actor.StationID, Observations: memory}
	stream, err := memory.Open("runtime")
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	syncID := uuid.NewString()
	o := appstate.ResourceObservation{Namespace: "apps", Name: installed.AppID, UID: runtime.uid, ResourceVersion: "opaque", StationID: actor.StationID, OrganizationID: actor.OrganizationID, InstallationID: installed.InstallationID, AppID: installed.AppID, Generation: 1, ObservedGeneration: 1, Health: "not_checked",
		Metadata: &appstate.ResourceMetadata{OwnershipMarked: true, TemplateHash: strings.TrimPrefix(local.TemplateDigest, "sha256:"), ImagesComplete: true, Images: []appstate.ResourceImage{{Component: "server", Image: "example.invalid/test:1.9.0"}}}}
	if stream.Begin(syncID) != nil || stream.Objects(syncID, []appstate.ResourceObservation{o}) != nil || stream.End(syncID, 1) != nil {
		t.Fatal("memory snapshot setup failed")
	}
	facts, err := svc.AppCenterFacts(ctx, actor, "read")
	if err != nil || len(facts.Items) != 1 {
		t.Fatal(facts, err)
	}
	if runtime.calls != 0 {
		t.Fatal("facts consulted Runtime before command submission")
	}
	snapshot, err := svc.AppStates.Snapshot(ctx, actor, installed.InstallationID)
	if err != nil || len(snapshot.Items) != 1 || snapshot.Items[0].Runtime.State != facts.Items[0].Service.State || snapshot.Items[0].Runtime.Quality != facts.Items[0].Service.Quality {
		t.Fatal("facts disagrees with the independent Core state API", err)
	}
	fact := facts.Items[0]
	var candidate appcenter.Candidate
	for _, c := range fact.Lifecycle.Candidates {
		if c.Kind == "start" {
			candidate = c
		}
	}
	if !candidate.Enabled || candidate.ReleaseDigest == nil || candidate.ExpectedWorkloadUID == nil || *candidate.ExpectedWorkloadUID != runtime.uid {
		t.Fatal("production reader did not project fixed candidate", candidate)
	}
	input := AppCenterCommand{SchemaVersion: 1, Command: admissionCommand(actor, installed, "start"), Expected: AppCenterExpected{
		InstallationID: &installed.InstallationID, InstallationRevision: &installed.Revision, AppVersion: installed.AppVersion, ReleaseDigest: candidate.ReleaseDigest,
		WorkloadUID: candidate.ExpectedWorkloadUID, PolicyRevision: fact.Lifecycle.PolicyRevision}}
	originalUID := runtime.uid
	runtime.uid = uuid.NewString()
	if _, err := svc.SubmitAppCenter(ctx, actor, input, source.Admit); !errors.Is(err, ErrAppCenterBinding) {
		t.Fatal("stale observation accepted", err)
	}
	runtime.uid = originalUID
	op, err := svc.SubmitAppCenter(ctx, actor, input, source.Admit)
	if err != nil || op.Status != "accepted" {
		t.Fatal(op, err)
	}
	before := runtime.calls
	facts, err = svc.AppCenterFacts(ctx, actor, "after")
	if err != nil {
		t.Fatal(err)
	}
	if !facts.Items[0].Operation.Blocking || runtime.calls != before {
		t.Fatal("active command consulted admission again")
	}
	for _, c := range facts.Items[0].Lifecycle.Candidates {
		if c.Enabled {
			t.Fatal("active command allowed another action")
		}
	}
	writeCommandPolicies(t, source, false, []AppCenterReleasePolicy{policy})
	source.Runtime = nil
	replay, err := svc.SubmitAppCenter(ctx, actor, input, source.Admit)
	if err != nil || replay.OperationID != op.OperationID {
		t.Fatal("policy withdrawal broke accepted replay", err)
	}
	// Finish via the protected worker stub, then prove a foreign organization
	// cannot see the installation or get an install candidate for its app ID.
	svc.AppCenterRuntime = &protectedRuntimeStub{}
	if _, err := svc.DispatchOne(ctx, nil); err != nil {
		t.Fatal(err)
	}
	writeCommandPolicies(t, source, true, []AppCenterReleasePolicy{policy})
	source.Runtime = runtime
	other := actor
	other.OrganizationID = uuid.NewString()
	other.UserID = uuid.NewString()
	before = runtime.calls
	facts, err = svc.AppCenterFacts(ctx, other, "foreign")
	if err != nil {
		t.Fatal(err)
	}
	if len(facts.Items) != 1 || facts.Items[0].Installation != nil || runtime.calls != before {
		t.Fatal("foreign installation authority leaked")
	}
	for _, c := range facts.Items[0].Lifecycle.Candidates {
		if c.Enabled || c.ReasonCode == nil || *c.ReasonCode != "INSTALLATION_STATE_CONFLICT" {
			t.Fatal("foreign pending installation not excluded", c)
		}
	}
}
