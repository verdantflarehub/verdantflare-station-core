package operations

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
)

func TestAppCenterCandidatesUseLocalEvidence(t *testing.T) {
	for _, variant := range []string{"start", "stop", "restart", "degraded-stop", "install", "runtime-offline", "template-mismatch", "uid-mismatch", "missing-catalog", "stale", "foreign", "blocking", "policy-withdrawn", "runtime-unconfigured"} {
		t.Run(variant, func(t *testing.T) {
			s, actor, installed, policy, runtime := admissionFixture(t)
			now := time.Now()
			if variant == "stop" || variant == "restart" || variant == "degraded-stop" {
				policy.Actions = []string{"start", "install", "stop", "restart"}
				writeCommandPolicies(t, s, true, []AppCenterReleasePolicy{policy})
			}
			command := admissionCommand(actor, installed, "start")
			execution, err := s.resolve(context.Background(), actor, command, &installed)
			if err != nil {
				t.Fatal(err)
			}
			kind := "start"
			fact := appcenter.Fact{AppID: installed.AppID, Origin: "installation", InstallationQuality: "known",
				Installation: &appcenter.Installation{InstallationID: installed.InstallationID, Revision: installed.Revision, AppVersion: installed.AppVersion, State: "installed"},
				Service:      appcenter.Service{State: "stopped", Quality: "known", ObservedAt: &now, WorkloadUID: &runtime.uid}, Operation: appcenter.Operation{Quality: "known"},
				Catalog: &catalog.Item{Entry: catalog.Entry{Version: installed.AppVersion, Namespace: "apps", WorkloadName: installed.AppID}, Deployment: catalog.Observation{UID: runtime.uid, Ownership: map[string]string{"station.verdantflare.com/template-hash": strings.TrimPrefix(execution.TemplateDigest, "sha256:")}}}}
			want := false
			switch variant {
			case "start":
				want = true
			case "stop":
				kind, fact.Service.State = "stop", "ready"
				want = true
			case "restart":
				kind, fact.Service.State = "restart", "ready"
				want = true
			case "degraded-stop":
				kind, fact.Service.State = "stop", "degraded"
				want = true
			case "install":
				kind, fact.Origin, fact.Installation, fact.Service.State, fact.Service.WorkloadUID = "install", "catalog", nil, "absent", nil
				want = true
			case "runtime-offline":
				runtime.err = errors.New("offline")
				want = true
			case "template-mismatch":
				fact.Catalog.Deployment.Ownership["station.verdantflare.com/template-hash"] = "!invalid"
			case "uid-mismatch":
				fact.Catalog.Deployment.UID = "different"
			case "missing-catalog":
				fact.Catalog = nil
			case "stale":
				at := now.Add(-15 * time.Second)
				fact.Service.ObservedAt = &at
			case "blocking":
				fact.Operation.Blocking = true
			case "policy-withdrawn":
				writeCommandPolicies(t, s, false, []AppCenterReleasePolicy{policy})
			case "runtime-unconfigured":
				s.Runtime = nil
			}
			fact.Lifecycle.Candidates = []appcenter.Candidate{{Kind: kind, AppVersion: installed.AppVersion}}
			s.projectCandidate(context.Background(), actor, &fact, variant == "foreign", now)
			if runtime.calls != 0 || fact.Lifecycle.Candidates[0].Enabled != want {
				t.Fatal("candidate depended on live Runtime or ignored evidence", runtime.calls, fact.Lifecycle.Candidates[0])
			}
			if variant == "runtime-offline" {
				if _, err := s.Read(context.Background(), actor, command, &installed); err == nil || runtime.calls != 1 {
					t.Fatal("command admission skipped live preflight")
				}
			}
		})
	}
}
