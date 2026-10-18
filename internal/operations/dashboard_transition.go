package operations

import (
	"context"
	"os"
	"path"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

// ReadTransition reuses the ordinary Dashboard registry and the original
// Actions bytes. No second registration or compatibility document is needed.
func (s *DashboardReleases) ReadTransition(ctx context.Context, actor identity.IdentityContext, installed Installation, targetRef string) (TransitionSnapshot, TransitionSnapshot, error) {
	var zero TransitionSnapshot
	if actor.StationID != installed.StationID || actor.OrganizationID != installed.OrganizationID || installed.State != "installed" {
		return zero, zero, ErrAppCenterRelease
	}
	root, entries, err := s.load(ctx)
	if err != nil {
		return zero, zero, err
	}
	defer root.Close()
	var source, target *dashboardReleaseEntry
	for i := range entries {
		e := &entries[i]
		if e.AppID != installed.AppID {
			continue
		}
		if e.AppVersion == installed.AppVersion {
			source = e
		}
		if e.ReleaseRef == targetRef {
			target = e
		}
	}
	if source == nil || target == nil {
		return zero, zero, ErrAppCenterRelease
	}
	a, err := dashboardTransitionSnapshot(ctx, root, *source)
	if err != nil {
		return zero, zero, err
	}
	b, err := dashboardTransitionSnapshot(ctx, root, *target)
	if err != nil {
		return zero, zero, err
	}
	return a, b, nil
}

func dashboardTransitionSnapshot(ctx context.Context, root *os.Root, e dashboardReleaseEntry) (TransitionSnapshot, error) {
	var zero TransitionSnapshot
	if verifyDashboardRelease(ctx, root, e) != nil {
		return zero, ErrAppCenterRelease
	}
	if e.ReleaseIndexRef == e.ManifestRef && e.ReleaseDigest == e.ManifestDigest {
		s := TransitionSnapshot{AppID: e.AppID, AppVersion: e.AppVersion, ReleaseRef: e.ReleaseRef, ReleaseDigest: e.ReleaseDigest, ManifestDigest: e.ManifestDigest,
			TemplateDigest: e.TemplateDigest, DashboardDigest: e.ManifestDigest, CompatibleWorkerVersions: []string{}, ReleaseKind: "dashboard-release", AppImageDigest: e.ManifestDigest}
		if !s.valid() {
			return zero, ErrAppCenterRelease
		}
		return s, nil
	}
	raw, err := materialRead(ctx, root, e.ReleaseIndexRef, 1<<20)
	var release transitionReleaseIndex
	if err != nil || materialDecode(raw, &release) != nil || materialDigest(raw) != e.ReleaseDigest {
		return zero, ErrAppCenterRelease
	}
	index, err := materialRead(ctx, root, path.Join(path.Dir(e.ReleaseIndexRef), "dashboard/asset-index.json"), 4<<20)
	if err != nil || materialDigest(index) != "sha256:"+release.Files["dashboard/asset-index.json"] {
		return zero, ErrAppCenterRelease
	}
	s := TransitionSnapshot{AppID: e.AppID, AppVersion: e.AppVersion, ReleaseRef: e.ReleaseRef, ReleaseDigest: e.ReleaseDigest, ManifestDigest: e.ManifestDigest,
		TemplateDigest: e.TemplateDigest, DashboardDigest: materialDigest(index), CompatibleWorkerVersions: []string{}, ReleaseKind: "dashboard-release", AppImageDigest: release.AppImageDigest}
	if !s.valid() {
		return zero, ErrAppCenterRelease
	}
	return s, nil
}

func (s *Service) transitionReleaseReader(ctx context.Context, actor identity.IdentityContext, installed Installation, ref string) (TransitionSnapshot, TransitionSnapshot, error) {
	var zero TransitionSnapshot
	// The same operator policy used by AppCenter selects the release source.
	// Invalid or withdrawn Dashboard material must never fall back to workers.
	if s.AppCenterAdmissionSource != nil {
		policies, err := s.AppCenterAdmissionSource.policies(ctx)
		if err != nil {
			return zero, zero, err
		}
		for _, p := range policies {
			if p.AppID == installed.AppID && p.ReleaseRef == ref && p.ReleaseKind == "dashboard-release" {
				source, target, err := s.AppCenterAdmissionSource.Dashboard.ReadTransition(ctx, actor, installed, ref)
				if err != nil || target.AppVersion != p.AppVersion {
					return zero, zero, ErrAppCenterRelease
				}
				return source, target, nil
			}
		}
	}
	return s.TransitionReleases.Read(ctx, actor, installed, ref)
}
