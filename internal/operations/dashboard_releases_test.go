package operations

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func dashboardMaterialFixture(t *testing.T) (*DashboardReleases, dashboardReleaseEntry) {
	t.Helper()
	previous, _, _, entries := transitionMaterialFixture(t)
	root := filepath.Dir(previous.Path)
	old := entries[0]
	prefix := old.Snapshot.ReleaseRef
	raw, err := os.ReadFile(filepath.Join(root, prefix, "release.json"))
	if err != nil {
		t.Fatal(err)
	}
	var release transitionReleaseIndex
	if json.Unmarshal(raw, &release) != nil {
		t.Fatal("fixture release")
	}
	for _, name := range []string{"VERSION", "management-release.json", "workspace-index.json"} {
		delete(release.Files, name)
	}
	declaration, _ := json.Marshal(map[string]any{"schemaVersion": 1, "appId": release.AppID, "appVersion": release.AppVersion,
		"dashboard": map[string]any{"artifactDigest": old.Snapshot.DashboardDigest, "entryRef": "dashboard/index.html", "adapter": "wujie"}})
	writeMaterial(t, root, prefix+"/dashboard/dashboard-declaration.json", declaration)
	release.Files["dashboard/dashboard-declaration.json"] = strings.TrimPrefix(materialDigest(declaration), "sha256:")
	raw, _ = json.Marshal(release)
	writeMaterial(t, root, prefix+"/release.json", raw)
	manifest, _ := json.Marshal(map[string]any{"app_id": release.AppID, "version": release.AppVersion, "deployment": map[string]any{"images": []any{map[string]any{"component": "app", "image": release.AppImage}}}})
	writeMaterial(t, root, prefix+"/manifest.json", manifest)
	e := dashboardReleaseEntry{AppID: release.AppID, AppVersion: release.AppVersion, ReleaseRef: prefix,
		ReleaseIndexRef: prefix + "/release.json", ReleaseDigest: materialDigest(raw), ManifestRef: prefix + "/manifest.json",
		ManifestDigest: materialDigest(manifest), TemplateDigest: old.Snapshot.TemplateDigest}
	s := &DashboardReleases{Path: filepath.Join(root, "dashboard-releases.json")}
	writeDashboardRegistry(t, s, []dashboardReleaseEntry{e})
	return s, e
}
func writeDashboardRegistry(t *testing.T, s *DashboardReleases, entries []dashboardReleaseEntry) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"schemaVersion": 1, "releases": entries})
	writeMaterial(t, filepath.Dir(s.Path), filepath.Base(s.Path), raw)
}
func TestDashboardReleasesRevocationAndExactIdentity(t *testing.T) {
	s, e := dashboardMaterialFixture(t)
	ctx := context.Background()
	if got, err := s.Resolve(ctx, e.AppID, e.AppVersion, e.ReleaseRef); err != nil || got != e {
		t.Fatal(got, err)
	}
	for _, in := range [][3]string{{"other", e.AppVersion, e.ReleaseRef}, {e.AppID, "99.0.0", e.ReleaseRef}, {e.AppID, e.AppVersion, "other"}} {
		if _, err := s.Resolve(ctx, in[0], in[1], in[2]); err == nil {
			t.Fatal("wrong identity accepted")
		}
	}
	writeDashboardRegistry(t, s, []dashboardReleaseEntry{})
	if _, err := s.Resolve(ctx, e.AppID, e.AppVersion, e.ReleaseRef); err == nil {
		t.Fatal("withdrawn release cached")
	}
	writeDashboardRegistry(t, s, []dashboardReleaseEntry{e})
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.Resolve(cancelled, e.AppID, e.AppVersion, e.ReleaseRef); err == nil {
		t.Fatal("cancel ignored")
	}
}
func TestDashboardReleasesRejectMaterialAndRegistryTampering(t *testing.T) {
	for _, name := range []string{"manifest.json", "release.json", "dashboard/dashboard-declaration.json", "dashboard/asset-index.json", "dashboard/index.html"} {
		t.Run(name, func(t *testing.T) {
			s, e := dashboardMaterialFixture(t)
			writeMaterial(t, filepath.Dir(s.Path), e.ReleaseRef+"/"+name, []byte("changed"))
			if _, err := s.Resolve(context.Background(), e.AppID, e.AppVersion, e.ReleaseRef); err == nil {
				t.Fatal("tamper accepted")
			}
		})
	}
	for name, change := range map[string]func(*dashboardReleaseEntry){
		"escape":          func(e *dashboardReleaseEntry) { e.ManifestRef = "../manifest.json" },
		"absolute":        func(e *dashboardReleaseEntry) { e.ReleaseIndexRef = "/release.json" },
		"url":             func(e *dashboardReleaseEntry) { e.ManifestRef = "https://invalid/manifest.json" },
		"manifest digest": func(e *dashboardReleaseEntry) { e.ManifestDigest = "sha256:" + strings.Repeat("f", 64) },
		"template digest": func(e *dashboardReleaseEntry) { e.TemplateDigest = "latest" },
	} {
		t.Run(name, func(t *testing.T) {
			s, e := dashboardMaterialFixture(t)
			change(&e)
			writeDashboardRegistry(t, s, []dashboardReleaseEntry{e})
			if _, err := s.Resolve(context.Background(), e.AppID, e.AppVersion, e.ReleaseRef); err == nil {
				t.Fatal("invalid claim accepted")
			}
		})
	}
	for _, raw := range []string{`{"schemaVersion":1,"schemaVersion":1,"releases":[]}`, `{"schemaVersion":1,"releases":[],"extra":true}`, `{"schemaVersion":1,"releases":null}`} {
		s, e := dashboardMaterialFixture(t)
		writeMaterial(t, filepath.Dir(s.Path), filepath.Base(s.Path), []byte(raw))
		if _, err := s.Resolve(context.Background(), e.AppID, e.AppVersion, e.ReleaseRef); err == nil {
			t.Fatal("malformed registry accepted")
		}
	}
	for _, differentRef := range []bool{false, true} {
		s, e := dashboardMaterialFixture(t)
		other := e
		if differentRef {
			other.ReleaseRef = "alias"
		}
		writeDashboardRegistry(t, s, []dashboardReleaseEntry{e, other})
		if _, err := s.Resolve(context.Background(), e.AppID, e.AppVersion, e.ReleaseRef); err == nil {
			t.Fatal("duplicate accepted")
		}
	}
}
func TestDashboardReleasesRejectRehashedInvalidRelease(t *testing.T) {
	for name, change := range map[string]func(*transitionReleaseIndex){
		"wrong app": func(r *transitionReleaseIndex) { r.AppID = "other" },
		"unexpected components": func(r *transitionReleaseIndex) {
			r.Components = []dashboardReleaseComponent{{Name: "app", Image: r.AppImage, ImageDigest: r.AppImageDigest}}
		},
		"wrong version":   func(r *transitionReleaseIndex) { r.AppVersion = "99.0.0" },
		"source":          func(r *transitionReleaseIndex) { r.SourceCommit = "main" },
		"run":             func(r *transitionReleaseIndex) { r.ActionsRunID = "0" },
		"image mismatch":  func(r *transitionReleaseIndex) { r.AppImage = "registry.example/other:app-v" + r.AppVersion },
		"missing asset":   func(r *transitionReleaseIndex) { delete(r.Files, "dashboard/index.html") },
		"worker material": func(r *transitionReleaseIndex) { r.Files["VERSION"] = strings.Repeat("a", 64) },
		"path escape":     func(r *transitionReleaseIndex) { r.Files["dashboard/../release.json"] = strings.Repeat("a", 64) },
	} {
		t.Run(name, func(t *testing.T) {
			s, e := dashboardMaterialFixture(t)
			root := filepath.Dir(s.Path)
			raw, _ := os.ReadFile(filepath.Join(root, e.ReleaseIndexRef))
			var release transitionReleaseIndex
			json.Unmarshal(raw, &release)
			change(&release)
			raw, _ = json.Marshal(release)
			writeMaterial(t, root, e.ReleaseIndexRef, raw)
			e.ReleaseDigest = materialDigest(raw)
			writeDashboardRegistry(t, s, []dashboardReleaseEntry{e})
			if _, err := s.Resolve(context.Background(), e.AppID, e.AppVersion, e.ReleaseRef); err == nil {
				t.Fatal("rehashed invalid release accepted")
			}
		})
	}
}
func TestDashboardReleaseAdmissionSupportsLifecycleActions(t *testing.T) {
	source, e := dashboardMaterialFixture(t)
	actor := identity.IdentityContext{UserID: uuid.NewString(), OrganizationID: uuid.NewString(), StationID: uuid.NewString(), Roles: []string{"admin"}, Scopes: []string{"app:manage", "app:read"}}
	runtime := &admissionRuntime{}
	admission := &AppCenterAdmissionSource{Path: filepath.Join(filepath.Dir(source.Path), "commands.json"), Dashboard: source, Runtime: runtime}
	policy := AppCenterReleasePolicy{e.AppID, e.AppVersion, "dashboard-release", e.ReleaseRef, []string{"install", "start", "stop", "restart", "delete"}}
	writeCommandPolicies(t, admission, true, []AppCenterReleasePolicy{policy})
	command := Command{AppID: e.AppID, AppVersion: e.AppVersion, Action: "install", OrganizationID: actor.OrganizationID, StationID: actor.StationID}
	first, err := admission.Read(context.Background(), actor, command, nil)
	if err != nil || first.Execution.TemplateDigest != e.TemplateDigest || first.Execution.ReleaseDigest == e.ReleaseDigest || runtime.calls != 1 {
		t.Fatal(first, err)
	}
	// Binding the manifest and template prevents a different registration from
	// retaining the same admission proof even if release.json itself is unchanged.
	e.TemplateDigest = "sha256:" + strings.Repeat("e", 64)
	writeDashboardRegistry(t, source, []dashboardReleaseEntry{e})
	second, err := admission.resolve(context.Background(), actor, command, nil)
	if err != nil || second.PolicyRevision == first.Execution.PolicyRevision || second.ReleaseDigest == first.Execution.ReleaseDigest {
		t.Fatal("binding proof not changed", err)
	}
	for _, action := range []string{"adopt", "upgrade"} {
		policy.Actions = []string{"install", action}
		writeCommandPolicies(t, admission, true, []AppCenterReleasePolicy{policy})
		if _, err := admission.Read(context.Background(), actor, command, nil); !errors.Is(err, ErrAppCenterRelease) {
			t.Fatal("unsupported policy accepted", err)
		}
	}
	if runtime.calls != 1 {
		t.Fatal("invalid policy contacted runtime")
	}
}
