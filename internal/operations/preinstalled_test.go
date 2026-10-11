package operations

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"
)

func TestPreinstalledCommandScopeAndReleaseGate(t *testing.T) {
	s := &Service{StationID: "station"}
	ctx := context.Background()
	user := identity.IdentityContext{StationID: "station", Roles: []string{"admin"}, Scopes: []string{"app:read", "app:manage"}}
	if _, err := s.SubmitPreinstalled(ctx, identity.IdentityContext{}, PreinstalledCommand{}); !errors.Is(err, identity.Denied) {
		t.Fatal(err)
	}
	if _, err := s.SubmitPreinstalled(ctx, user, PreinstalledCommand{}); !errors.Is(err, identity.Invalid) {
		t.Fatal(err)
	}
	if err := s.validatePreinstalled(ctx, nil, user, &PreinstalledCommand{}); !errors.Is(err, releasebinding.ErrUntrusted) {
		t.Fatal("missing trust reached database", err)
	}
}
func TestPreinstalledDigestPreservesLegacyAndBindsExecution(t *testing.T) {
	c := Command{RequestID: "trace", IdempotencyKey: "existing-key", OrganizationID: "org", StationID: "station", AppID: "resources", AppVersion: "0.1.0", Action: "install"}
	normalized := c
	normalized.RequestID = ""
	raw, _ := json.Marshal(normalized)
	if commandHash(c, nil) != sha256.Sum256(raw) {
		t.Fatal("legacy digest changed")
	}
	p := &PreinstalledCommand{Command: c, Release: releasebinding.Binding{ReleaseRef: "resources-v1", AppID: c.AppID, AppVersion: c.AppVersion, PackageDigest: "sha256:" + strings.Repeat("a", 64), ChartDigest: "sha256:" + strings.Repeat("b", 64), TemplateDigest: "sha256:" + strings.Repeat("c", 64)}, InventoryRevision: "sha256:" + strings.Repeat("d", 64)}
	first := commandHash(c, p)
	c.RequestID = "new-trace"
	if commandHash(c, p) != first {
		t.Fatal("trace changed recovery key")
	}
	p.Release.ChartDigest = "sha256:" + strings.Repeat("e", 64)
	if commandHash(c, p) == first {
		t.Fatal("changed chart reused operation digest")
	}
}
func TestPreinstalledHistoryRevisionIncludesRemovedFacts(t *testing.T) {
	empty := inventoryRevision([]InstallationHistory{})
	items := []InstallationHistory{{Installation: Installation{InstallationID: "removed-install", State: "removed", Revision: 2}}}
	removed := inventoryRevision(items)
	if removed == empty {
		t.Fatal("removed history disappeared")
	}
	items[0].Installation.Revision++
	if inventoryRevision(items) == removed {
		t.Fatal("history revision ignored")
	}
}
