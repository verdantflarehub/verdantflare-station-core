package operations

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVideoDashboardReleaseBindsBothImages(t *testing.T) {
	for _, version := range []string{"0.17.28", "0.17.29", "0.17.30", "0.17.31", "0.17.32", "0.17.35", "0.17.37"} {
		for _, scenario := range []string{"valid", "missing-component", "extra-component", "wrong-primary-digest", "wrong-sidecar-digest", "wrong-sidecar-image", "wrong-manifest", "duplicate-component", "unsupported-version", "other-app"} {
			t.Run(version+"/"+scenario, func(t *testing.T) {
				s, e := dashboardMaterialFixture(t)
				root := filepath.Dir(s.Path)
				raw, err := os.ReadFile(filepath.Join(root, e.ReleaseIndexRef))
				if err != nil {
					t.Fatal(err)
				}
				var release transitionReleaseIndex
				if err := json.Unmarshal(raw, &release); err != nil {
					t.Fatal(err)
				}
				release.AppImage = strings.ReplaceAll(release.AppImage, release.AppVersion, version)
				e.AppID, e.AppVersion = "video-mcp", version
				if scenario == "unsupported-version" {
					e.AppVersion = "0.17.33"
					release.AppImage = strings.ReplaceAll(release.AppImage, version, e.AppVersion)
				}
				if scenario == "other-app" {
					e.AppID = "other-app"
				}
				release.AppID, release.AppVersion = e.AppID, e.AppVersion
				release.Components = []dashboardReleaseComponent{
					{Name: "api", Image: release.AppImage, ImageDigest: release.AppImageDigest},
					{Name: "artifact-authority", Image: "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:video-mcp-server-v0.17.12", ImageDigest: "sha256:b4a6eed372cca5ede2a3722a45de210b3bac223050e3e478e89943f997b0a4a4"},
				}
				images := []dashboardManifestImage{{"api", release.Components[0].Image}, {"artifact-authority", release.Components[1].Image}}
				switch scenario {
				case "missing-component":
					release.Components = release.Components[:1]
				case "extra-component":
					release.Components = append(release.Components, release.Components[1])
				case "wrong-primary-digest":
					release.Components[0].ImageDigest = "sha256:" + strings.Repeat("c", 64)
				case "wrong-sidecar-digest":
					release.Components[1].ImageDigest = "sha256:" + strings.Repeat("f", 64)
				case "wrong-sidecar-image":
					release.Components[1].Image = "registry.cn-qingdao.aliyuncs.com/wod/verdantflare-app:video-mcp-server-v0.17.11"
					images[1].Image = release.Components[1].Image
				case "wrong-manifest":
					images[1].Image = "other:1"
				case "duplicate-component":
					release.Components[1] = release.Components[0]
				}
				declarationPath := e.ReleaseRef + "/dashboard/dashboard-declaration.json"
				raw, err = os.ReadFile(filepath.Join(root, declarationPath))
				if err != nil {
					t.Fatal(err)
				}
				var declaration map[string]any
				if err := json.Unmarshal(raw, &declaration); err != nil {
					t.Fatal(err)
				}
				declaration["appId"], declaration["appVersion"] = e.AppID, e.AppVersion
				raw, _ = json.Marshal(declaration)
				writeMaterial(t, root, declarationPath, raw)
				release.Files["dashboard/dashboard-declaration.json"] = strings.TrimPrefix(materialDigest(raw), "sha256:")
				raw, _ = json.Marshal(map[string]any{"app_id": e.AppID, "version": e.AppVersion, "deployment": map[string]any{"images": images}})
				writeMaterial(t, root, e.ManifestRef, raw)
				e.ManifestDigest = materialDigest(raw)
				raw, _ = json.Marshal(release)
				writeMaterial(t, root, e.ReleaseIndexRef, raw)
				e.ReleaseDigest = materialDigest(raw)
				writeDashboardRegistry(t, s, []dashboardReleaseEntry{e})
				_, err = s.Resolve(context.Background(), e.AppID, e.AppVersion, e.ReleaseRef)
				if (err == nil) != (scenario == "valid") {
					t.Fatal("dual-container release admission", err)
				}
				if scenario == "valid" {
					dir, err := os.OpenRoot(root)
					if err != nil {
						t.Fatal(err)
					}
					defer dir.Close()
					snapshot, err := dashboardTransitionSnapshot(context.Background(), dir, e)
					if err != nil || snapshot.AppImageDigest != release.AppImageDigest || snapshot.ReleaseKind != "dashboard-release" {
						t.Fatal("transition snapshot", err)
					}
				}
			})
		}
	}
}
