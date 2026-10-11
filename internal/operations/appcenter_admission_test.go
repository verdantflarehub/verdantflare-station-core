package operations

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type admissionRuntime struct {
	calls  int
	uid    string
	err    error
	change func(*pb.AppCenterObservation)
}

func (r *admissionRuntime) ReconcileBound(context.Context, *pb.AppCenterRequest, ...grpc.CallOption) (*pb.AppProgress, error) {
	panic("admission must never execute an operation")
}
func (r *admissionRuntime) InspectBound(_ context.Context, in *pb.AppCenterInspection, _ ...grpc.CallOption) (*pb.AppCenterObservation, error) {
	r.calls++
	if r.err != nil {
		return nil, r.err
	}
	out := &pb.AppCenterObservation{SchemaVersion: 1, TemplateDigest: in.TemplateDigest, Namespace: "apps", WorkloadName: in.AppId, State: "absent"}
	if in.Action == "start" {
		out.State, out.WorkloadUid = "stopped", r.uid
	}
	if r.change != nil {
		r.change(out)
	}
	return out, nil
}
func writeCommandPolicies(t *testing.T, source *AppCenterAdmissionSource, enabled bool, policies []AppCenterReleasePolicy) {
	t.Helper()
	raw, _ := json.Marshal(map[string]any{"schemaVersion": 1, "enabled": enabled, "releases": policies})
	if err := os.WriteFile(source.Path, raw, 0600); err != nil {
		t.Fatal(err)
	}
}
func admissionFixture(t *testing.T) (*AppCenterAdmissionSource, identity.IdentityContext, Installation, AppCenterReleasePolicy, *admissionRuntime) {
	t.Helper()
	management, actor, installed, entries := transitionMaterialFixture(t)
	actor.UserID, actor.OrganizationID, actor.StationID = uuid.NewString(), uuid.NewString(), uuid.NewString()
	actor.Roles, actor.Scopes = []string{"admin"}, []string{"app:read", "app:manage"}
	installed.StationID, installed.OrganizationID = actor.StationID, actor.OrganizationID
	runtime := &admissionRuntime{uid: uuid.NewString()}
	s := &AppCenterAdmissionSource{Path: filepath.Join(t.TempDir(), "commands.json"), Management: management, Runtime: runtime}
	policy := AppCenterReleasePolicy{installed.AppID, installed.AppVersion, "management-release", entries[0].Snapshot.ReleaseRef, []string{"start", "install"}}
	writeCommandPolicies(t, s, true, []AppCenterReleasePolicy{policy})
	return s, actor, installed, policy, runtime
}
func admissionCommand(actor identity.IdentityContext, installed Installation, action string) Command {
	return Command{RequestID: "test", IdempotencyKey: "test-command", OrganizationID: actor.OrganizationID, StationID: actor.StationID, AppID: installed.AppID, AppVersion: installed.AppVersion, Action: action}
}

func TestAppCenterAdmissionFixedReleaseAndLiveInspection(t *testing.T) {
	s, actor, installed, policy, runtime := admissionFixture(t)
	ctx := context.Background()
	command := admissionCommand(actor, installed, "start")
	first, err := s.Read(ctx, actor, command, &installed)
	if err != nil || first.Execution.WorkloadUID != runtime.uid || first.Execution.TemplateDigest == "" {
		t.Fatal(first, err)
	}
	policy.Actions = []string{"install", "start"}
	other := policy
	other.AppID = "different-app"
	writeCommandPolicies(t, s, true, []AppCenterReleasePolicy{other, policy})
	second, err := s.Read(ctx, actor, command, &installed)
	if err != nil || first.Execution != second.Execution {
		t.Fatal("unrelated entry/order changed policy", err)
	}
	command.Action = "install"
	if got, err := s.Read(ctx, actor, command, nil); err != nil || got.Execution.WorkloadUID != "" {
		t.Fatal(got, err)
	}
	writeCommandPolicies(t, s, false, []AppCenterReleasePolicy{policy})
	before := runtime.calls
	if _, err := s.Read(ctx, actor, command, nil); !errors.Is(err, ErrAppCenterUnsupported) || runtime.calls != before {
		t.Fatal("disabled registry admitted", err)
	}
	writeCommandPolicies(t, s, true, []AppCenterReleasePolicy{})
	if _, err := s.Read(ctx, actor, command, nil); !errors.Is(err, ErrAppCenterRelease) {
		t.Fatal("withdrawal ignored", err)
	}
}

func TestAppCenterAdmissionRejectsInvalidTrustIdentityAndProtocol(t *testing.T) {
	for _, variant := range []string{"manifest", "foreign-org", "missing-installation", "wrong-version", "unsupported-action", "duplicate-policy", "duplicate-action", "unknown-field", "no-runtime", "old-runtime", "unavailable", "wrong-template", "wrong-state", "missing-uid", "unknown-protobuf", "invalid-name"} {
		t.Run(variant, func(t *testing.T) {
			s, actor, installed, policy, runtime := admissionFixture(t)
			command := admissionCommand(actor, installed, "start")
			ptr := &installed
			switch variant {
			case "manifest":
				os.WriteFile(filepath.Join(filepath.Dir(s.Management.Path), policy.ReleaseRef, "manifest.json"), []byte("tampered"), 0600)
			case "foreign-org":
				actor.OrganizationID = uuid.NewString()
			case "missing-installation":
				ptr = nil
			case "wrong-version":
				command.AppVersion = "99.0.0"
			case "unsupported-action":
				command.Action = "stop"
			case "duplicate-policy":
				writeCommandPolicies(t, s, true, []AppCenterReleasePolicy{policy, policy})
			case "duplicate-action":
				policy.Actions = []string{"start", "start"}
				writeCommandPolicies(t, s, true, []AppCenterReleasePolicy{policy})
			case "unknown-field":
				os.WriteFile(s.Path, []byte(`{"schemaVersion":1,"enabled":true,"releases":[],"endpoint":"external"}`), 0600)
			case "no-runtime":
				s.Runtime = nil
			case "old-runtime":
				runtime.err = status.Error(codes.Unimplemented, "unsupported")
			case "unavailable":
				runtime.err = status.Error(codes.Unavailable, "unavailable")
			case "wrong-template":
				runtime.change = func(o *pb.AppCenterObservation) { o.TemplateDigest = "sha256:" + strings.Repeat("e", 64) }
			case "wrong-state":
				runtime.change = func(o *pb.AppCenterObservation) { o.State = "ready" }
			case "missing-uid":
				runtime.uid = ""
			case "unknown-protobuf":
				runtime.change = func(o *pb.AppCenterObservation) { o.ProtoReflect().SetUnknown([]byte{0x78, 1}) }
			case "invalid-name":
				runtime.change = func(o *pb.AppCenterObservation) { o.Namespace = "../other" }
			}
			if _, err := s.Read(context.Background(), actor, command, ptr); err == nil {
				t.Fatal("invalid admission accepted")
			}
		})
	}
}

func TestAppCenterAdmissionSignedPackage(t *testing.T) {
	s, actor, installed, policy, runtime := admissionFixture(t)
	root := t.TempDir()
	pub, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	manifest, chart := []byte("original package"), []byte("original chart")
	binding := releasebinding.Binding{ReleaseRef: "signed-app", AppID: installed.AppID, AppVersion: installed.AppVersion, PackageDigest: materialDigest(manifest), ChartDigest: materialDigest(chart), TemplateDigest: "sha256:" + strings.Repeat("c", 64)}
	now := time.Now().UTC()
	payload, _ := json.Marshal(releasebinding.SignedRelease{SchemaVersion: 1, Binding: binding, NotBefore: now.Add(-time.Hour).Format(time.RFC3339), ExpiresAt: now.Add(time.Hour).Format(time.RFC3339)})
	signature := ed25519.Sign(private, append([]byte("verdantflare.preinstalled.release.v1\n"), payload...))
	raw, _ := json.Marshal(map[string]any{"schemaVersion": 1, "keys": map[string]string{"publisher": base64.StdEncoding.EncodeToString(pub)}, "releases": []any{map[string]any{
		"releaseRef": binding.ReleaseRef, "payload": base64.StdEncoding.EncodeToString(payload), "signature": base64.StdEncoding.EncodeToString(signature), "keyId": "publisher", "packageRef": "package.json", "chartRef": "chart.tgz"}}})
	for name, data := range map[string][]byte{"registry.json": raw, "package.json": manifest, "chart.tgz": chart} {
		if err := os.WriteFile(filepath.Join(root, name), data, 0600); err != nil {
			t.Fatal(err)
		}
	}
	s.Signed = &releasebinding.Source{Path: filepath.Join(root, "registry.json")}
	policy.ReleaseKind, policy.ReleaseRef = "signed-package", binding.ReleaseRef
	writeCommandPolicies(t, s, true, []AppCenterReleasePolicy{policy})
	command := admissionCommand(actor, installed, "start")
	out, err := s.Read(context.Background(), actor, command, &installed)
	want := appcenter.Digest(struct {
		Protocol string
		Binding  releasebinding.Binding
	}{"studio.appcenter.signed-release.v1", binding})
	if err != nil || out.Execution.ReleaseDigest != want || out.Execution.TemplateDigest != binding.TemplateDigest {
		t.Fatal(out, err)
	}
	before := runtime.calls
	os.WriteFile(filepath.Join(root, "chart.tgz"), []byte("changed"), 0600)
	if _, err := s.Read(context.Background(), actor, command, &installed); !errors.Is(err, ErrAppCenterRelease) || runtime.calls != before {
		t.Fatal("tampered signed package admitted", err)
	}
}
