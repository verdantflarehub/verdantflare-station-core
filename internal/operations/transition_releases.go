package operations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/strictjson"
)

var ErrTransitionRelease = errors.New("TRANSITION_RELEASE_UNAVAILABLE")

// TransitionReleases reads an operator-controlled, read-only material registry.
// The expected index digest is trusted configuration, not a publisher claim.
// Runtime independently verifies Snapshot.TemplateDigest against its registry.
type TransitionReleases struct{ Path string }

type transitionReleaseEntry struct {
	Snapshot        TransitionSnapshot `json:"snapshot"`
	ManifestRef     string             `json:"manifest_ref"`
	ReleaseIndexRef string             `json:"release_index_ref"`
}

type transitionReleaseIndex struct {
	SchemaVersion     int                         `json:"schemaVersion"`
	AppID             string                      `json:"appId"`
	AppVersion        string                      `json:"appVersion"`
	SourceCommit      string                      `json:"sourceCommit"`
	ActionsRunID      string                      `json:"actionsRunId"`
	ActionsRunAttempt string                      `json:"actionsRunAttempt"`
	AppImage          string                      `json:"appImage"`
	AppImageDigest    string                      `json:"appImageDigest"`
	Components        []dashboardReleaseComponent `json:"components,omitempty"`
	Files             map[string]string           `json:"files"`
}

type dashboardReleaseComponent struct {
	Name        string `json:"name"`
	Image       string `json:"image"`
	ImageDigest string `json:"imageDigest"`
}

func materialPath(ref string) bool {
	return len(ref) <= 512 && fs.ValidPath(ref) && ref != "." && !strings.ContainsAny(ref, "\\:")
}

func materialRead(ctx context.Context, root *os.Root, ref string, maximum int64) ([]byte, error) {
	if ctx.Err() != nil || !materialPath(ref) {
		return nil, ErrTransitionRelease
	}
	f, err := root.Open(ref)
	if err != nil {
		return nil, ErrTransitionRelease
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > maximum {
		return nil, ErrTransitionRelease
	}
	raw, err := io.ReadAll(io.LimitReader(f, maximum+1))
	if err != nil || int64(len(raw)) > maximum || ctx.Err() != nil {
		return nil, ErrTransitionRelease
	}
	return raw, nil
}

func materialDigest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func materialDecode(raw []byte, out any) error {
	if strictjson.Decode(bytes.NewReader(raw), 4<<20, out) != nil {
		return ErrTransitionRelease
	}
	return nil
}

// Read resolves both versions from one bounded registry observation. No cache
// survives revocation, and no caller can choose a filesystem path or origin.
func (s *TransitionReleases) Read(ctx context.Context, actor identity.IdentityContext, installed Installation, targetRef string) (TransitionSnapshot, TransitionSnapshot, error) {
	var zero TransitionSnapshot
	if s == nil || s.Path == "" || ctx.Err() != nil || !transitionName.MatchString(targetRef) ||
		actor.StationID != installed.StationID || actor.OrganizationID != installed.OrganizationID || installed.State != "installed" {
		return zero, zero, ErrTransitionRelease
	}
	root, entries, err := s.load(ctx)
	if err != nil {
		return zero, zero, err
	}
	defer root.Close()
	var source, target *transitionReleaseEntry
	for i := range entries {
		e := &entries[i]
		if e.Snapshot.AppID == installed.AppID && e.Snapshot.AppVersion == installed.AppVersion {
			source = e
		}
		if e.Snapshot.ReleaseRef == targetRef && e.Snapshot.AppID == installed.AppID {
			target = e
		}
	}
	if source == nil || target == nil || verifyTransitionMaterial(ctx, root, *source) != nil || verifyTransitionMaterial(ctx, root, *target) != nil {
		return zero, zero, ErrTransitionRelease
	}
	return source.Snapshot, target.Snapshot, nil
}

// Resolve verifies one exact immutable release for ordinary lifecycle admission.
// It does not create an installation or infer authority from an app name.
func (s *TransitionReleases) Resolve(ctx context.Context, appID, version, ref string) (TransitionSnapshot, error) {
	root, entries, err := s.load(ctx)
	if err != nil {
		return TransitionSnapshot{}, err
	}
	defer root.Close()
	for _, entry := range entries {
		if entry.Snapshot.AppID == appID && entry.Snapshot.AppVersion == version && entry.Snapshot.ReleaseRef == ref {
			if err := verifyTransitionMaterial(ctx, root, entry); err != nil {
				return TransitionSnapshot{}, err
			}
			return entry.Snapshot, nil
		}
	}
	return TransitionSnapshot{}, ErrTransitionRelease
}

// AuthorizeWorkerVersion keeps management authentication exact. Only worker
// callers use verified compatibility for a different installed manager version.
func (s *Service) AuthorizeWorkerVersion(ctx context.Context, installed Installation, version string) error {
	if installed.State != "installed" || installed.StationID != s.StationID || installed.Revision < 1 {
		return identity.Denied
	}
	if s.AppCenterAdmissionSource != nil && s.AppCenterAdmissionSource.Dashboard != nil && s.AppCenterAdmissionSource.Dashboard.Path != "" {
		root, entries, err := s.AppCenterAdmissionSource.Dashboard.load(ctx)
		if err != nil {
			return ErrTransitionRelease
		}
		root.Close()
		for _, entry := range entries {
			if entry.AppID == installed.AppID {
				return identity.Denied
			}
		}
	}
	if installed.AppVersion == version {
		return nil
	}
	root, entries, err := s.TransitionReleases.load(ctx)
	if err != nil {
		return ErrTransitionRelease
	}
	defer root.Close()
	for _, entry := range entries {
		if entry.Snapshot.AppID != installed.AppID || entry.Snapshot.AppVersion != installed.AppVersion {
			continue
		}
		if err := verifyTransitionMaterial(ctx, root, entry); err != nil {
			return ErrTransitionRelease
		}
		if slices.Contains(entry.Snapshot.CompatibleWorkerVersions, version) {
			return nil
		}
		return identity.Denied
	}
	return ErrTransitionRelease
}

func (s *TransitionReleases) load(ctx context.Context) (*os.Root, []transitionReleaseEntry, error) {
	if s == nil || s.Path == "" || ctx.Err() != nil {
		return nil, nil, ErrTransitionRelease
	}
	root, err := os.OpenRoot(filepath.Dir(s.Path))
	if err != nil {
		return nil, nil, ErrTransitionRelease
	}
	fail := func() (*os.Root, []transitionReleaseEntry, error) {
		root.Close()
		return nil, nil, ErrTransitionRelease
	}
	raw, err := materialRead(ctx, root, filepath.Base(s.Path), 2<<20)
	if err != nil {
		return fail()
	}
	var registry struct {
		SchemaVersion int                      `json:"schema_version"`
		Releases      []transitionReleaseEntry `json:"releases"`
	}
	if materialDecode(raw, &registry) != nil || registry.SchemaVersion != 1 || len(registry.Releases) == 0 || len(registry.Releases) > 128 {
		return fail()
	}
	refs, versions := map[string]bool{}, map[string]bool{}
	for _, entry := range registry.Releases {
		version := entry.Snapshot.AppID + "@" + entry.Snapshot.AppVersion
		if !entry.Snapshot.valid() || refs[entry.Snapshot.ReleaseRef] || versions[version] || !materialPath(entry.ManifestRef) || !materialPath(entry.ReleaseIndexRef) {
			return fail()
		}
		refs[entry.Snapshot.ReleaseRef], versions[version] = true, true
	}
	return root, registry.Releases, nil
}

func verifyTransitionMaterial(ctx context.Context, root *os.Root, entry transitionReleaseEntry) error {
	snapshot := entry.Snapshot
	manifest, err := materialRead(ctx, root, entry.ManifestRef, 4<<20)
	if err != nil || materialDigest(manifest) != snapshot.ManifestDigest {
		return ErrTransitionRelease
	}
	// Full AppManifest schema and execution semantics belong to preparation and
	// Runtime. Core checks identity and original bytes without redefining it.
	valid, err := strictjson.ReadValue(bytes.NewReader(manifest), 4<<20)
	var manifestIdentity struct {
		AppID   string `json:"app_id"`
		Version string `json:"version"`
	}
	if err != nil || json.Unmarshal(valid, &manifestIdentity) != nil || manifestIdentity.AppID != snapshot.AppID || manifestIdentity.Version != snapshot.AppVersion {
		return ErrTransitionRelease
	}
	raw, err := materialRead(ctx, root, entry.ReleaseIndexRef, 1<<20)
	if err != nil || materialDigest(raw) != snapshot.ReleaseDigest {
		return ErrTransitionRelease
	}
	var index transitionReleaseIndex
	if materialDecode(raw, &index) != nil || index.SchemaVersion != 1 || index.AppID != snapshot.AppID || index.AppVersion != snapshot.AppVersion ||
		!regexp.MustCompile(`^[a-f0-9]{40}$`).MatchString(index.SourceCommit) || !transitionDigest.MatchString(index.AppImageDigest) ||
		!regexp.MustCompile(`^[1-9][0-9]{0,19}$`).MatchString(index.ActionsRunID) || !regexp.MustCompile(`^[1-9][0-9]{0,9}$`).MatchString(index.ActionsRunAttempt) ||
		!regexp.MustCompile(`^[a-z0-9][a-z0-9./:_-]*:[a-z0-9][a-z0-9._-]*-v`+regexp.QuoteMeta(snapshot.AppVersion)+`$`).MatchString(index.AppImage) ||
		len(index.Components) != 0 || len(index.Files) < 5 || len(index.Files) > 260 {
		return ErrTransitionRelease
	}
	materials, total := map[string][]byte{}, 0
	for name, digest := range index.Files {
		if !materialPath(name) || !transitionDigest.MatchString("sha256:"+digest) ||
			!(slices.Contains([]string{"VERSION", "management-release.json", "workspace-index.json"}, name) || strings.HasPrefix(name, "dashboard/")) {
			return ErrTransitionRelease
		}
		content, err := materialRead(ctx, root, path.Join(path.Dir(entry.ReleaseIndexRef), name), 4<<20)
		total += len(content)
		if err != nil || total > 32<<20 || materialDigest(content) != "sha256:"+digest {
			return ErrTransitionRelease
		}
		materials[name] = content
	}
	for _, name := range []string{"VERSION", "management-release.json", "workspace-index.json", "dashboard/dashboard-declaration.json", "dashboard/asset-index.json"} {
		if _, present := materials[name]; !present {
			return ErrTransitionRelease
		}
	}
	if strings.TrimSpace(string(materials["VERSION"])) != snapshot.AppVersion {
		return ErrTransitionRelease
	}
	var management struct {
		SchemaVersion            int      `json:"schema_version"`
		AppID                    string   `json:"app_id"`
		AppVersion               string   `json:"app_version"`
		DataContract             string   `json:"data_contract"`
		DefaultWorkerVersion     string   `json:"default_worker_version"`
		CompatibleWorkerVersions []string `json:"compatible_worker_versions"`
	}
	if materialDecode(materials["management-release.json"], &management) != nil || management.SchemaVersion != 1 ||
		management.AppID != snapshot.AppID || management.AppVersion != snapshot.AppVersion || management.DataContract != snapshot.DataContract ||
		management.DefaultWorkerVersion != snapshot.DefaultWorkerVersion || !slices.Equal(management.CompatibleWorkerVersions, snapshot.CompatibleWorkerVersions) {
		return ErrTransitionRelease
	}
	var declaration struct {
		SchemaVersion int            `json:"schemaVersion"`
		AppID         string         `json:"appId"`
		AppVersion    string         `json:"appVersion"`
		Dashboard     map[string]any `json:"dashboard"`
	}
	if materialDecode(materials["dashboard/dashboard-declaration.json"], &declaration) != nil || declaration.SchemaVersion != 1 ||
		declaration.AppID != snapshot.AppID || declaration.AppVersion != snapshot.AppVersion || declaration.Dashboard["artifactDigest"] != snapshot.DashboardDigest ||
		materialDigest(materials["dashboard/asset-index.json"]) != snapshot.DashboardDigest {
		return ErrTransitionRelease
	}
	var assets struct {
		SchemaVersion int               `json:"schemaVersion"`
		Files         map[string]string `json:"files"`
	}
	if materialDecode(materials["dashboard/asset-index.json"], &assets) != nil || assets.SchemaVersion != 1 ||
		len(assets.Files) == 0 || len(assets.Files) > 256 || assets.Files["index.html"] == "" || len(materials) != len(assets.Files)+5 {
		return ErrTransitionRelease
	}
	for name, digest := range assets.Files {
		content, present := materials["dashboard/"+name]
		if !materialPath(name) || !present || !transitionDigest.MatchString("sha256:"+digest) || materialDigest(content) != "sha256:"+digest {
			return ErrTransitionRelease
		}
	}
	return nil
}
