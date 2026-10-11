package operations

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
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
	reader := &preinstalledObservation{current: catalog.Observation{State: "stopped", Reason: "scaled_to_zero", ObservedAt: time.Now(), UID: runtime.uid, Images: []catalog.Image{{Component: "server", Image: "example.invalid/test:1.9.0"}}, Ownership: map[string]string{
		"station.verdantflare.com/station-id": actor.StationID, "station.verdantflare.com/organization-id": actor.OrganizationID,
		"station.verdantflare.com/app-id": installed.AppID, "station.verdantflare.com/installation-id": installed.InstallationID, "station.verdantflare.com/template-hash": strings.Repeat("a", 64)}}}
	path := filepath.Join(t.TempDir(), "catalog.json")
	if err := os.WriteFile(path, []byte(`[{"app_id":"test-app","display_name":"Test","group_id":"video","brand":"vf","models":[],"source":"deploys/test.yaml","namespace":"apps","workload_name":"test-app","version":"1.9.0","images":[{"component":"server","image":"example.invalid/test:1.9.0"}]}]`), 0600); err != nil {
		t.Fatal(err)
	}
	svc.Catalog, err = catalog.Load(path, reader)
	if err != nil {
		t.Fatal(err)
	}
	facts, err := svc.AppCenterFacts(ctx, actor, "read")
	if err != nil || len(facts.Items) != 1 {
		t.Fatal(facts, err)
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
	reader.current = catalog.Observation{State: "not_installed", Reason: "not_found", ObservedAt: time.Now(), Images: []catalog.Image{}}
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
