package operations

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

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
