package operations

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func transitionInputs() (TransitionCommand, Installation, TransitionSnapshot, TransitionSnapshot) {
	source := TransitionSnapshot{AppID: "test-app", AppVersion: "1.9.0", ReleaseRef: "test-v1-9", ReleaseDigest: "sha256:" + strings.Repeat("e", 64), ManifestDigest: "sha256:" + strings.Repeat("a", 64),
		TemplateDigest: "sha256:" + strings.Repeat("b", 64), DashboardDigest: "sha256:" + strings.Repeat("c", 64), DataContract: "app-state-v1",
		DefaultWorkerVersion: "1.0.0", CompatibleWorkerVersions: []string{"1.0.0"}}
	target := source
	target.AppVersion = "1.10.0"
	target.ReleaseRef = "test-v1-10"
	target.DashboardDigest = "sha256:" + strings.Repeat("d", 64)
	installed := Installation{InstallationID: uuid.Must(uuid.NewV7()).String(), AppID: source.AppID, AppVersion: source.AppVersion, State: "installed", Revision: 2}
	c := TransitionCommand{RequestID: "request-a", IdempotencyKey: "upgrade-test-a", InstallationID: installed.InstallationID, ExpectedRevision: 2, SourceVersion: source.AppVersion, TargetReleaseRef: target.ReleaseRef, Action: "upgrade"}
	return c, installed, source, target
}

func TestTransitionCompatibilityRetainsWorkerVersions(t *testing.T) {
	c, installed, source, target := transitionInputs()
	if !c.valid() || !compatibleTransition(c, installed, source, target) {
		t.Fatal("compatible management update rejected")
	}
	for name, change := range map[string]func(*TransitionSnapshot){
		"another app":    func(v *TransitionSnapshot) { v.AppID = "another" },
		"another ref":    func(v *TransitionSnapshot) { v.ReleaseRef = "other" },
		"data migration": func(v *TransitionSnapshot) { v.DataContract = "app-state-v2" },
		"lost worker": func(v *TransitionSnapshot) {
			v.DefaultWorkerVersion = "2.0.0"
			v.CompatibleWorkerVersions = []string{"2.0.0"}
		},
		"unknown default":         func(v *TransitionSnapshot) { v.DefaultWorkerVersion = "2.0.0" },
		"duplicate worker":        func(v *TransitionSnapshot) { v.CompatibleWorkerVersions = []string{"1.0.0", "1.0.0"} },
		"wrong direction":         func(v *TransitionSnapshot) { v.AppVersion = "1.8.0" },
		"same version":            func(v *TransitionSnapshot) { v.AppVersion = source.AppVersion },
		"mutable digest":          func(v *TransitionSnapshot) { v.ManifestDigest = "latest" },
		"missing release binding": func(v *TransitionSnapshot) { v.ReleaseDigest = "" },
		"prerelease":              func(v *TransitionSnapshot) { v.AppVersion = "1.10.0-beta" },
	} {
		t.Run(name, func(t *testing.T) {
			modified := target
			change(&modified)
			if compatibleTransition(c, installed, source, modified) {
				t.Fatal("incompatible update accepted")
			}
		})
	}
	c.Action = "rollback"
	c.SourceVersion = target.AppVersion
	c.TargetReleaseRef = source.ReleaseRef
	installed.AppVersion = target.AppVersion
	if !compatibleTransition(c, installed, target, source) {
		t.Fatal("compatible explicit rollback rejected")
	}
	target.CompatibleWorkerVersions = []string{"1.0.0", "2.0.0"}
	if compatibleTransition(c, installed, target, source) {
		t.Fatal("rollback lost supported instance version")
	}
}

func TestTransitionVersionsAndReplayHash(t *testing.T) {
	c, _, _, _ := transitionInputs()
	hash := transitionHash(c)
	c.RequestID = "another-trace"
	if transitionHash(c) != hash {
		t.Fatal("trace changed operation identity")
	}
	c.ExpectedRevision++
	if transitionHash(c) == hash {
		t.Fatal("concurrent version precondition not bound")
	}
	for _, pair := range [][2]string{{"1.9.0", "1.10.0"}, {"1.0.999999999999999999999999", "1.1.0"}, {"2.0.0", "10.0.0"}} {
		if compareTransitionVersions(pair[0], pair[1]) != -1 || compareTransitionVersions(pair[1], pair[0]) != 1 {
			t.Fatal("numeric version ordering failed")
		}
	}
}

func TestTransitionPermissionAndInvalidInputBeforeDatabase(t *testing.T) {
	c, _, _, _ := transitionInputs()
	svc := Service{StationID: "station-a"}
	user := identity.IdentityContext{StationID: "station-a", Roles: []string{"admin"}, Scopes: []string{"app:read", "app:manage"}}
	for _, bad := range []identity.IdentityContext{{StationID: "station-b", Roles: user.Roles, Scopes: user.Scopes}, {StationID: user.StationID, Roles: []string{"member"}, Scopes: user.Scopes}, {StationID: user.StationID, Roles: user.Roles}} {
		if _, err := svc.SubmitTransition(context.Background(), bad, c, nil); !errors.Is(err, identity.Denied) {
			t.Fatal(err)
		}
	}
	for name, change := range map[string]func(*TransitionCommand){
		"v4 installation":      func(v *TransitionCommand) { v.InstallationID = uuid.NewString() },
		"wrong variant":        func(v *TransitionCommand) { v.InstallationID = "019a0000-0000-7000-0000-000000000001" },
		"installation newline": func(v *TransitionCommand) { v.InstallationID += "\n" },
		"version newline":      func(v *TransitionCommand) { v.SourceVersion += "\n" },
		"reference newline":    func(v *TransitionCommand) { v.TargetReleaseRef += "\n" },
		"revision":             func(v *TransitionCommand) { v.ExpectedRevision = 0 },
		"action":               func(v *TransitionCommand) { v.Action = "install" },
		"target path":          func(v *TransitionCommand) { v.TargetReleaseRef = "../escape" },
		"version":              func(v *TransitionCommand) { v.SourceVersion = "01.0.0" },
		"key":                  func(v *TransitionCommand) { v.IdempotencyKey = "short" },
	} {
		t.Run(name, func(t *testing.T) {
			bad := c
			change(&bad)
			if _, err := svc.SubmitTransition(context.Background(), user, bad, nil); !errors.Is(err, identity.Invalid) {
				t.Fatal(err)
			}
		})
	}
	if _, err := svc.GetTransition(context.Background(), user, uuid.NewString()); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal(err)
	}
}
