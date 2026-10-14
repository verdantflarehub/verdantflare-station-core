package operations

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func dashboardTransitionInputs(t *testing.T) (*DashboardReleases, identity.IdentityContext, Installation, dashboardReleaseEntry) {
	t.Helper()
	s, source := dashboardMaterialFixture(t)
	root := filepath.Dir(s.Path)
	target := source
	target.AppVersion = "1.10.0"
	target.ReleaseRef = "test-v1-10"
	target.ManifestRef = target.ReleaseRef + "/manifest.json"
	target.ReleaseIndexRef = target.ReleaseRef + "/release.json"
	raw, err := os.ReadFile(filepath.Join(root, source.ReleaseIndexRef))
	if err != nil {
		t.Fatal(err)
	}
	var release transitionReleaseIndex
	if err = json.Unmarshal(raw, &release); err != nil {
		t.Fatal(err)
	}
	release.AppVersion = target.AppVersion
	release.AppImage = "registry.example/apps:test-app-v" + target.AppVersion
	for name := range release.Files {
		content, err := os.ReadFile(filepath.Join(root, source.ReleaseRef, filepath.FromSlash(name)))
		if err != nil {
			t.Fatal(err)
		}
		if name == "dashboard/dashboard-declaration.json" {
			content = []byte(strings.ReplaceAll(string(content), source.AppVersion, target.AppVersion))
		}
		writeMaterial(t, root, target.ReleaseRef+"/"+name, content)
		release.Files[name] = strings.TrimPrefix(materialDigest(content), "sha256:")
	}
	raw, _ = json.Marshal(release)
	writeMaterial(t, root, target.ReleaseIndexRef, raw)
	target.ReleaseDigest = materialDigest(raw)
	raw, err = os.ReadFile(filepath.Join(root, source.ManifestRef))
	if err != nil {
		t.Fatal(err)
	}
	raw = []byte(strings.ReplaceAll(string(raw), source.AppVersion, target.AppVersion))
	writeMaterial(t, root, target.ManifestRef, raw)
	target.ManifestDigest = materialDigest(raw)
	writeDashboardRegistry(t, s, []dashboardReleaseEntry{source, target})
	actor := identity.IdentityContext{StationID: "station", OrganizationID: "org"}
	i := Installation{AppID: source.AppID, AppVersion: source.AppVersion, StationID: actor.StationID, OrganizationID: actor.OrganizationID, State: "installed", Revision: 1}
	return s, actor, i, target
}

func TestDashboardTransitionUsesExistingReleaseAndNoWorkerMaterials(t *testing.T) {
	s, actor, i, target := dashboardTransitionInputs(t)
	a, b, err := s.ReadTransition(context.Background(), actor, i, target.ReleaseRef)
	if err != nil || a.ReleaseKind != "dashboard-release" || b.AppVersion != target.AppVersion || len(a.CompatibleWorkerVersions) != 0 || a.DefaultWorkerVersion != "" {
		t.Fatal(a, b, err)
	}
	c := TransitionCommand{Action: "upgrade", SourceVersion: i.AppVersion, TargetReleaseRef: target.ReleaseRef}
	if !compatibleTransition(c, i, a, b) {
		t.Fatal("ordinary upgrade rejected")
	}
	i.AppVersion = b.AppVersion
	c.Action = "rollback"
	c.SourceVersion = b.AppVersion
	c.TargetReleaseRef = a.ReleaseRef
	if !compatibleTransition(c, i, b, a) {
		t.Fatal("ordinary rollback rejected")
	}
	b.ReleaseKind = ""
	b.AppImageDigest = ""
	b.DefaultWorkerVersion = "1.0.0"
	b.CompatibleWorkerVersions = []string{"1.0.0"}
	b.DataContract = "old-v1"
	if compatibleTransition(c, i, b, a) {
		t.Fatal("cross release source accepted")
	}
}

func TestDashboardTransitionRejectsMissingTamperedAndCrossOrganization(t *testing.T) {
	for _, scenario := range []string{"missing target", "modified image", "modified dashboard", "wrong organization", "withdrawn"} {
		t.Run(scenario, func(t *testing.T) {
			s, actor, i, target := dashboardTransitionInputs(t)
			switch scenario {
			case "missing target":
				target.ReleaseRef = "absent"
			case "modified image":
				writeMaterial(t, filepath.Dir(s.Path), target.ReleaseIndexRef, []byte(`{}`))
			case "modified dashboard":
				writeMaterial(t, filepath.Dir(s.Path), target.ReleaseRef+"/dashboard/index.html", []byte("changed"))
			case "wrong organization":
				actor.OrganizationID = "other"
			case "withdrawn":
				writeDashboardRegistry(t, s, []dashboardReleaseEntry{})
			}
			if _, _, err := s.ReadTransition(context.Background(), actor, i, target.ReleaseRef); err == nil {
				t.Fatal("invalid release accepted")
			}
		})
	}
}

func TestDashboardTransitionNeverGrantsWorkerEvenAtSameVersion(t *testing.T) {
	d, _, i, _ := dashboardTransitionInputs(t)
	s := &Service{StationID: i.StationID, AppCenterAdmissionSource: &AppCenterAdmissionSource{Dashboard: d}}
	for _, version := range []string{i.AppVersion, "0.1.0"} {
		if err := s.AuthorizeWorkerVersion(context.Background(), i, version); !errors.Is(err, identity.Denied) {
			t.Fatal("worker authorized", err)
		}
	}
}

func TestDashboardTransitionOptionalFieldsPreserveLegacyBytes(t *testing.T) {
	_, _, source, _ := transitionInputs()
	raw, _ := json.Marshal(source)
	want := `{"app_id":"test-app","app_version":"1.9.0","release_ref":"test-v1-9","release_digest":"sha256:` + strings.Repeat("e", 64) + `","manifest_digest":"sha256:` + strings.Repeat("a", 64) + `","template_digest":"sha256:` + strings.Repeat("b", 64) + `","dashboard_digest":"sha256:` + strings.Repeat("c", 64) + `","data_contract":"app-state-v1","default_worker_version":"1.0.0","compatible_worker_versions":["1.0.0"]}`
	if string(raw) != want {
		t.Fatal("historical serialized input changed")
	}
}

func TestDashboardTransitionUsesExistingPolicy(t *testing.T) {
	for _, scenario := range []string{"enabled", "disabled", "withdrawn", "wrong version", "missing registry"} {
		t.Run(scenario, func(t *testing.T) {
			d, actor, installed, target := dashboardTransitionInputs(t)
			admission := &AppCenterAdmissionSource{Path: filepath.Join(t.TempDir(), "commands.json"), Dashboard: d}
			policies := []AppCenterReleasePolicy{{AppID: installed.AppID, AppVersion: target.AppVersion, ReleaseKind: "dashboard-release", ReleaseRef: target.ReleaseRef, Actions: []string{"install", "start"}}}
			if scenario == "withdrawn" {
				policies = []AppCenterReleasePolicy{}
			}
			if scenario == "wrong version" {
				policies[0].AppVersion = "9.9.9"
			}
			if scenario == "missing registry" {
				admission.Dashboard = nil
			}
			writeCommandPolicies(t, admission, scenario != "disabled", policies)
			s := &Service{AppCenterAdmissionSource: admission}
			runtime, err := NewHTTPTransitionRuntime("http://runtime.test:5053", strings.Repeat("test-only-", 5))
			if err != nil {
				t.Fatal(err)
			}
			err = s.ConfigureTransitionAdmission("true", runtime)
			if (err == nil) != (scenario != "missing registry") {
				t.Fatal("startup admission", err)
			}
			_, b, err := s.transitionReleaseReader(context.Background(), actor, installed, target.ReleaseRef)
			if (err == nil) != (scenario == "enabled") {
				t.Fatal("policy ignored", err)
			}
			if err == nil && b.AppVersion != target.AppVersion {
				t.Fatal("wrong release selected")
			}
		})
	}
}
