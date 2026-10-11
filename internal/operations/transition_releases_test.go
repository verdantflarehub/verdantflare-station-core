package operations

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func transitionMaterialFixture(t *testing.T) (*TransitionReleases, identity.IdentityContext, Installation, []transitionReleaseEntry) {
	t.Helper()
	root := t.TempDir()
	_, installed, source, target := transitionInputs()
	installed.StationID, installed.OrganizationID = "station-a", "org-a"
	actor := identity.IdentityContext{StationID: installed.StationID, OrganizationID: installed.OrganizationID}
	entries := []transitionReleaseEntry{}
	for _, snapshot := range []TransitionSnapshot{source, target} {
		prefix := snapshot.ReleaseRef
		manifest := []byte(`{"app_id":"test-app","version":"` + snapshot.AppVersion + `"}`)
		snapshot.ManifestDigest = materialDigest(manifest)
		asset := []byte("<!doctype html><title>Independent Dashboard</title>")
		index, _ := json.Marshal(map[string]any{"schemaVersion": 1, "files": map[string]string{"index.html": strings.TrimPrefix(materialDigest(asset), "sha256:")}})
		snapshot.DashboardDigest = materialDigest(index)
		management, _ := json.Marshal(map[string]any{"schema_version": 1, "app_id": snapshot.AppID, "app_version": snapshot.AppVersion,
			"data_contract": snapshot.DataContract, "default_worker_version": snapshot.DefaultWorkerVersion, "compatible_worker_versions": snapshot.CompatibleWorkerVersions})
		declaration, _ := json.Marshal(map[string]any{"schemaVersion": 1, "appId": snapshot.AppID, "appVersion": snapshot.AppVersion,
			"dashboard": map[string]any{"artifactDigest": snapshot.DashboardDigest, "adapter": "wujie"}})
		materials := map[string][]byte{"VERSION": []byte(snapshot.AppVersion + "\n"), "management-release.json": management,
			"workspace-index.json": []byte(`{"schemaVersion":1,"files":{}}`), "dashboard/dashboard-declaration.json": declaration,
			"dashboard/asset-index.json": index, "dashboard/index.html": asset}
		release := transitionReleaseIndex{1, snapshot.AppID, snapshot.AppVersion, strings.Repeat("a", 40), "123", "1",
			"registry.example/apps:" + snapshot.AppID + "-v" + snapshot.AppVersion, "sha256:" + strings.Repeat("f", 64), map[string]string{}}
		for name, raw := range materials {
			writeMaterial(t, root, prefix+"/"+name, raw)
			release.Files[name] = strings.TrimPrefix(materialDigest(raw), "sha256:")
		}
		raw, _ := json.Marshal(release)
		snapshot.ReleaseDigest = materialDigest(raw)
		writeMaterial(t, root, prefix+"/release.json", raw)
		writeMaterial(t, root, prefix+"/manifest.json", manifest)
		entries = append(entries, transitionReleaseEntry{snapshot, prefix + "/manifest.json", prefix + "/release.json"})
	}
	s := &TransitionReleases{Path: filepath.Join(root, "registry.json")}
	writeTransitionRegistry(t, s, entries)
	return s, actor, installed, entries
}

func writeMaterial(t *testing.T, root, ref string, raw []byte) {
	t.Helper()
	p := filepath.Join(root, filepath.FromSlash(ref))
	if err := os.MkdirAll(filepath.Dir(p), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func writeTransitionRegistry(t *testing.T, s *TransitionReleases, entries []transitionReleaseEntry) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"schema_version": 1, "releases": entries})
	if err := os.WriteFile(s.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}

func TestTransitionReleasesVerifyOriginalMaterialsAndReloadRevocation(t *testing.T) {
	s, actor, installed, entries := transitionMaterialFixture(t)
	source, target, err := s.Read(context.Background(), actor, installed, entries[1].Snapshot.ReleaseRef)
	if err != nil || source.AppVersion != installed.AppVersion || target.AppVersion != entries[1].Snapshot.AppVersion {
		t.Fatal("valid materials rejected", err)
	}
	target.CompatibleWorkerVersions[0] = "999.0.0"
	_, reread, err := s.Read(context.Background(), actor, installed, entries[1].Snapshot.ReleaseRef)
	if err != nil || reread.CompatibleWorkerVersions[0] == "999.0.0" {
		t.Fatal("caller mutated release source", err)
	}
	writeTransitionRegistry(t, s, entries[:1])
	if _, _, err := s.Read(context.Background(), actor, installed, entries[1].Snapshot.ReleaseRef); err == nil {
		t.Fatal("withdrawn release reused cached material")
	}
}

func TestTransitionReleasesRejectChangedBytesAndSnapshotClaims(t *testing.T) {
	for _, name := range []string{"manifest.json", "release.json", "VERSION", "management-release.json", "workspace-index.json", "dashboard/dashboard-declaration.json", "dashboard/asset-index.json", "dashboard/index.html"} {
		t.Run(name, func(t *testing.T) {
			s, actor, installed, entries := transitionMaterialFixture(t)
			writeMaterial(t, filepath.Dir(s.Path), entries[1].Snapshot.ReleaseRef+"/"+name, []byte("tampered"))
			if _, _, err := s.Read(context.Background(), actor, installed, entries[1].Snapshot.ReleaseRef); err == nil {
				t.Fatal("changed material accepted")
			}
		})
	}
	for name, change := range map[string]func(*transitionReleaseEntry){
		"compatibility": func(e *transitionReleaseEntry) { e.Snapshot.CompatibleWorkerVersions = []string{"1.0.0", "2.0.0"} },
		"data contract": func(e *transitionReleaseEntry) { e.Snapshot.DataContract = "changed-v1" },
		"dashboard":     func(e *transitionReleaseEntry) { e.Snapshot.DashboardDigest = "sha256:" + strings.Repeat("e", 64) },
		"absolute path": func(e *transitionReleaseEntry) { e.ManifestRef = "/manifest.json" },
		"escape":        func(e *transitionReleaseEntry) { e.ReleaseIndexRef = "../release.json" },
		"windows path":  func(e *transitionReleaseEntry) { e.ManifestRef = `C:\manifest.json` },
		"URL":           func(e *transitionReleaseEntry) { e.ManifestRef = "https://example.invalid/manifest.json" },
	} {
		t.Run(name, func(t *testing.T) {
			s, actor, installed, entries := transitionMaterialFixture(t)
			change(&entries[1])
			writeTransitionRegistry(t, s, entries)
			if _, _, err := s.Read(context.Background(), actor, installed, entries[1].Snapshot.ReleaseRef); err == nil {
				t.Fatal("unbound claim accepted")
			}
		})
	}
}

func TestTransitionReleasesRejectAmbiguityMalformedJSONAndCancellation(t *testing.T) {
	s, actor, installed, entries := transitionMaterialFixture(t)
	check := func() {
		t.Helper()
		if _, _, err := s.Read(context.Background(), actor, installed, entries[1].Snapshot.ReleaseRef); err == nil {
			t.Fatal("ambiguous registry accepted")
		}
	}
	for _, change := range []func(*transitionReleaseEntry){func(*transitionReleaseEntry) {}, func(e *transitionReleaseEntry) { e.Snapshot.ReleaseRef = "alias" }} {
		duplicate := entries[1]
		change(&duplicate)
		writeTransitionRegistry(t, s, append(slices.Clone(entries), duplicate))
		check()
	}
	for _, raw := range []string{`{"schema_version":1,"schema_version":1,"releases":[]}`, `{"schema_version":1,"releases":[],"other":1}`, strings.Repeat(" ", 2<<20+1)} {
		writeMaterial(t, filepath.Dir(s.Path), "registry.json", []byte(raw))
		check()
	}
	writeTransitionRegistry(t, s, entries)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := s.Read(ctx, actor, installed, entries[1].Snapshot.ReleaseRef); err == nil {
		t.Fatal("cancelled operation read release")
	}
	actor.OrganizationID = "other"
	check()
}
