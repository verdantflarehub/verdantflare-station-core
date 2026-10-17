package operations

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/strictjson"
)

// DashboardReleases is an operator-controlled source for applications without
// workers. Lifecycle admission and transition admission remain separate; this
// material reader never grants worker compatibility rights.
type DashboardReleases struct{ Path string }

type dashboardReleaseEntry struct {
	AppID           string `json:"appId"`
	AppVersion      string `json:"appVersion"`
	ReleaseRef      string `json:"releaseRef"`
	ReleaseIndexRef string `json:"releaseIndexRef"`
	ReleaseDigest   string `json:"releaseDigest"`
	ManifestRef     string `json:"manifestRef"`
	ManifestDigest  string `json:"manifestDigest"`
	TemplateDigest  string `json:"templateDigest"`
}

func (s *DashboardReleases) load(ctx context.Context) (*os.Root, []dashboardReleaseEntry, error) {
	if s == nil || s.Path == "" || ctx.Err() != nil {
		return nil, nil, ErrAppCenterRelease
	}
	root, err := os.OpenRoot(filepath.Dir(s.Path))
	if err != nil {
		return nil, nil, ErrAppCenterRelease
	}
	fail := func() (*os.Root, []dashboardReleaseEntry, error) { root.Close(); return nil, nil, ErrAppCenterRelease }
	raw, err := materialRead(ctx, root, filepath.Base(s.Path), 2<<20)
	if err != nil {
		return fail()
	}
	var registry struct {
		SchemaVersion int                     `json:"schemaVersion"`
		Releases      []dashboardReleaseEntry `json:"releases"`
	}
	if appcenter.Decode(bytes.NewReader(raw), "dashboard-release-registry", 2<<20, &registry) != nil {
		return fail()
	}
	versions, refs := map[string]bool{}, map[string]bool{}
	for _, e := range registry.Releases {
		key := e.AppID + "@" + e.AppVersion
		if versions[key] || refs[e.ReleaseRef] || !materialPath(e.ReleaseIndexRef) || !materialPath(e.ManifestRef) || e.ReleaseIndexRef == e.ManifestRef {
			return fail()
		}
		versions[key], refs[e.ReleaseRef] = true, true
	}
	return root, registry.Releases, nil
}

func (s *DashboardReleases) Resolve(ctx context.Context, appID, version, ref string) (dashboardReleaseEntry, error) {
	root, entries, err := s.load(ctx)
	if err != nil {
		return dashboardReleaseEntry{}, err
	}
	defer root.Close()
	for _, e := range entries {
		if e.AppID == appID && e.AppVersion == version && e.ReleaseRef == ref {
			if verifyDashboardRelease(ctx, root, e) != nil {
				return dashboardReleaseEntry{}, ErrAppCenterRelease
			}
			return e, nil
		}
	}
	return dashboardReleaseEntry{}, ErrAppCenterRelease
}

func verifyDashboardRelease(ctx context.Context, root *os.Root, e dashboardReleaseEntry) error {
	manifest, err := materialRead(ctx, root, e.ManifestRef, 4<<20)
	if err != nil || materialDigest(manifest) != e.ManifestDigest {
		return ErrAppCenterRelease
	}
	// Runtime owns the full manifest schema; this reader verifies original bytes,
	// identity and image correspondence without creating a second manifest dialect.
	validated, err := strictjson.ReadValue(bytes.NewReader(manifest), 4<<20)
	var identity struct {
		AppID      string `json:"app_id"`
		Version    string `json:"version"`
		Deployment struct {
			Images []dashboardManifestImage `json:"images"`
		} `json:"deployment"`
	}
	if err != nil || json.Unmarshal(validated, &identity) != nil || identity.AppID != e.AppID || identity.Version != e.AppVersion {
		return ErrAppCenterRelease
	}
	raw, err := materialRead(ctx, root, e.ReleaseIndexRef, 1<<20)
	if err != nil || materialDigest(raw) != e.ReleaseDigest {
		return ErrAppCenterRelease
	}
	var release transitionReleaseIndex
	if materialDecode(raw, &release) != nil || release.SchemaVersion != 1 || release.AppID != e.AppID || release.AppVersion != e.AppVersion ||
		!regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(release.SourceCommit) || !transitionDigest.MatchString(release.AppImageDigest) ||
		!regexp.MustCompile(`^[1-9][0-9]{0,19}$`).MatchString(release.ActionsRunID) || !regexp.MustCompile(`^[1-9][0-9]{0,9}$`).MatchString(release.ActionsRunAttempt) ||
		!regexp.MustCompile(`^[a-z0-9][a-z0-9./:_-]*:[a-z0-9][a-z0-9._-]*-v`+regexp.QuoteMeta(e.AppVersion)+`$`).MatchString(release.AppImage) ||
		!dashboardImagesMatch(identity.Deployment.Images, release) || len(release.Files) < 3 || len(release.Files) > 258 {
		return ErrAppCenterRelease
	}
	materials, total := map[string][]byte{}, 0
	for name, digest := range release.Files {
		if !materialPath(name) || !strings.HasPrefix(name, "dashboard/") || !transitionDigest.MatchString("sha256:"+digest) {
			return ErrAppCenterRelease
		}
		content, err := materialRead(ctx, root, path.Join(path.Dir(e.ReleaseIndexRef), name), 4<<20)
		total += len(content)
		if err != nil || total > 32<<20 || materialDigest(content) != "sha256:"+digest {
			return ErrAppCenterRelease
		}
		materials[name] = content
	}
	var declaration struct {
		SchemaVersion int            `json:"schemaVersion"`
		AppID         string         `json:"appId"`
		AppVersion    string         `json:"appVersion"`
		Dashboard     map[string]any `json:"dashboard"`
	}
	var index struct {
		SchemaVersion int               `json:"schemaVersion"`
		Files         map[string]string `json:"files"`
	}
	if materialDecode(materials["dashboard/dashboard-declaration.json"], &declaration) != nil || declaration.SchemaVersion != 1 || declaration.AppID != e.AppID || declaration.AppVersion != e.AppVersion ||
		declaration.Dashboard["artifactDigest"] != materialDigest(materials["dashboard/asset-index.json"]) || declaration.Dashboard["entryRef"] != "dashboard/index.html" ||
		materialDecode(materials["dashboard/asset-index.json"], &index) != nil || index.SchemaVersion != 1 || len(index.Files) < 1 || len(index.Files) > 256 || index.Files["index.html"] == "" || len(materials) != len(index.Files)+2 {
		return ErrAppCenterRelease
	}
	for name, digest := range index.Files {
		content, present := materials["dashboard/"+name]
		if !materialPath(name) || name == "asset-index.json" || name == "dashboard-declaration.json" || !present || !transitionDigest.MatchString("sha256:"+digest) || materialDigest(content) != "sha256:"+digest {
			return ErrAppCenterRelease
		}
	}
	return nil
}
