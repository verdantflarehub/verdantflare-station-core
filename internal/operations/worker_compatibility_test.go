package operations

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func TestWorkerCompatibilityRequiresCurrentVerifiedRelease(t *testing.T) {
	ctx := context.Background()
	for _, scenario := range []string{"compatible", "unsupported", "withdrawn", "tampered", "missing registry", "removed", "wrong station", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			registry, _, installed, entries := transitionMaterialFixture(t)
			worker := entries[0].Snapshot.DefaultWorkerVersion
			installed.AppVersion, installed.Revision = entries[1].Snapshot.AppVersion, 7
			svc := &Service{StationID: installed.StationID, TransitionReleases: registry}
			want := error(nil)
			requestCtx := ctx
			switch scenario {
			case "unsupported":
				worker, want = "9.9.9", identity.Denied
			case "withdrawn":
				if err := svc.AuthorizeWorkerVersion(ctx, installed, worker); err != nil {
					t.Fatal(err)
				}
				writeTransitionRegistry(t, registry, entries[:1])
				want = ErrTransitionRelease
			case "tampered":
				writeMaterial(t, filepath.Dir(registry.Path), entries[1].Snapshot.ReleaseRef+"/management-release.json", []byte(`{}`))
				want = ErrTransitionRelease
			case "missing registry":
				svc.TransitionReleases, want = nil, ErrTransitionRelease
			case "removed":
				installed.State, want = "removed", identity.Denied
			case "wrong station":
				installed.StationID, want = "another-station", identity.Denied
			case "canceled":
				var cancel context.CancelFunc
				requestCtx, cancel = context.WithCancel(ctx)
				cancel()
				want = ErrTransitionRelease
			}
			if err := svc.AuthorizeWorkerVersion(requestCtx, installed, worker); !errors.Is(err, want) {
				t.Fatalf("got %v, want %v", err, want)
			}
		})
	}
}

func TestWorkerCompatibilityExactVersionNeedsNoTransitionConfiguration(t *testing.T) {
	s := &Service{StationID: "station-a"}
	i := Installation{StationID: s.StationID, State: "installed", Revision: 1, AppVersion: "0.1.3"}
	if err := s.AuthorizeWorkerVersion(context.Background(), i, "0.1.3"); err != nil {
		t.Fatal(err)
	}
	if err := s.AuthorizeWorkerVersion(context.Background(), i, "0.1.2"); !errors.Is(err, ErrTransitionRelease) {
		t.Fatal("undeclared old worker authorized", err)
	}
}

func TestWorkerCompatibilityHelmCatalogTakesPrecedenceOverLegacyDashboardEntry(t *testing.T) {
	ctx := context.Background()
	dash, dashEntry := dashboardMaterialFixture(t)
	installed := Installation{
		StationID:  "station-helm",
		AppID:      dashEntry.AppID,
		AppVersion: "0.1.17",
		State:      "installed",
		Revision:   2,
	}

	// Without a Helm-managed catalog entry, a Dashboard-only app must still be denied.
	dashOnlySvc := &Service{
		StationID:                installed.StationID,
		AppCenterAdmissionSource: &AppCenterAdmissionSource{Dashboard: dash},
	}
	if err := dashOnlySvc.AuthorizeWorkerVersion(ctx, installed, "0.1.17"); !errors.Is(err, identity.Denied) {
		t.Fatalf("expected Dashboard-only app to be denied, got %v", err)
	}

	// Once upgraded into a Helm-managed catalog entry (matching appcenter_admission.go),
	// worker version authorization succeeds even when legacy DashboardReleases still lists the app.
	catPath := filepath.Join(t.TempDir(), "catalog.json")
	catRaw, err := json.Marshal([]catalog.Entry{{
		AppID:        dashEntry.AppID,
		DisplayName:  "ComfyUI",
		GroupID:      "image",
		Brand:        "vf",
		Models:       []string{},
		Source:       "deploys/comfyui.yaml",
		Namespace:    "verdantflare-app",
		WorkloadName: dashEntry.AppID,
		Version:      "0.1.17",
		Helm: &catalog.Helm{
			OCIRef:           "oci://example.invalid/charts/" + dashEntry.AppID,
			ChartName:        dashEntry.AppID,
			ChartVersion:     "0.1.17",
			ManifestDigest:   "sha256:" + strings.Repeat("a", 64),
			PackageDigest:    "sha256:" + strings.Repeat("b", 64),
			PackageSizeBytes: 1024,
			TargetNamespace:  "verdantflare-app",
			ReleaseName:      dashEntry.AppID,
			ValuesSource:     "deploys/comfyui.yaml",
		},
		Images: []catalog.Image{{Component: "server", Image: "example.invalid/comfyui:0.1.17"}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(catPath, catRaw, 0600); err != nil {
		t.Fatal(err)
	}
	catSvc, err := catalog.Load(catPath, nil)
	if err != nil {
		t.Fatal(err)
	}
	helmSvc := &Service{
		StationID:                installed.StationID,
		Catalog:                  catSvc,
		AppCenterAdmissionSource: &AppCenterAdmissionSource{Dashboard: dash},
	}
	if err := helmSvc.AuthorizeWorkerVersion(ctx, installed, "0.1.17"); err != nil {
		t.Fatalf("expected Helm-managed app worker version authorization to succeed, got %v", err)
	}
}
