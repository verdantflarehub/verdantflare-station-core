package operations

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var ErrAppCenterUnsupported = errors.New("ACTION_UNSUPPORTED")
var appCenterAppName = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,62}$`)
var appCenterVersion = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)
var appCenterKubernetesName = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type AppCenterReleasePolicy struct {
	AppID       string   `json:"appId"`
	AppVersion  string   `json:"appVersion"`
	ReleaseKind string   `json:"releaseKind"`
	ReleaseRef  string   `json:"releaseRef"`
	Actions     []string `json:"actions"`
}

// Configuration and original release bytes are operator-controlled. Neither a
// browser request nor the catalog chooses a source path, key, or RPC endpoint.
type AppCenterAdmissionSource struct {
	Path       string
	Signed     *releasebinding.Source
	Management *TransitionReleases
	Runtime    pb.AppCenterRuntimeClient
}

type AppCenterAdmissionResult struct {
	Execution   AppCenterExecution
	Observation *pb.AppCenterObservation
}

func (s *AppCenterAdmissionSource) policies(ctx context.Context) ([]AppCenterReleasePolicy, error) {
	if s == nil || s.Path == "" || ctx.Err() != nil {
		return nil, ErrAppCenterUnsupported
	}
	root, err := os.OpenRoot(filepath.Dir(s.Path))
	if err != nil {
		return nil, ErrAppCenterRelease
	}
	defer root.Close()
	raw, err := materialRead(ctx, root, filepath.Base(s.Path), 2<<20)
	if err != nil {
		return nil, ErrAppCenterRelease
	}
	var cfg struct {
		SchemaVersion int                      `json:"schemaVersion"`
		Enabled       *bool                    `json:"enabled"`
		Releases      []AppCenterReleasePolicy `json:"releases"`
	}
	if appcenter.Decode(bytes.NewReader(raw), "command-registry", 2<<20, &cfg) != nil || cfg.SchemaVersion != 1 || cfg.Enabled == nil || cfg.Releases == nil || len(cfg.Releases) > 128 {
		return nil, ErrAppCenterRelease
	}
	seen := map[string]bool{}
	for _, p := range cfg.Releases {
		key := p.AppID + "@" + p.AppVersion
		if !appCenterAppName.MatchString(p.AppID) || len(p.AppVersion) > 64 || !appCenterVersion.MatchString(p.AppVersion) ||
			!slices.Contains([]string{"signed-package", "management-release"}, p.ReleaseKind) || !transitionName.MatchString(p.ReleaseRef) ||
			len(p.Actions) == 0 || len(p.Actions) > 4 || seen[key] {
			return nil, ErrAppCenterRelease
		}
		seen[key] = true
		actions := map[string]bool{}
		for _, action := range p.Actions {
			if !slices.Contains([]string{"install", "start", "stop", "restart"}, action) || actions[action] {
				return nil, ErrAppCenterRelease
			}
			actions[action] = true
		}
	}
	if !*cfg.Enabled {
		return nil, ErrAppCenterUnsupported
	}
	return cfg.Releases, nil
}

func (s *AppCenterAdmissionSource) Admit(ctx context.Context, actor identity.IdentityContext, command Command, installed *Installation) (AppCenterExecution, error) {
	result, err := s.Read(ctx, actor, command, installed)
	return result.Execution, err
}

// resolve verifies only local policy and immutable release material. Listing
// candidates must never use Runtime inspection as a read dependency.
func (s *AppCenterAdmissionSource) resolve(ctx context.Context, actor identity.IdentityContext, command Command, installed *Installation) (AppCenterExecution, error) {
	var zero AppCenterExecution
	if !slices.Contains(actor.Roles, "admin") || !slices.Contains(actor.Scopes, "app:manage") || !slices.Contains(actor.Scopes, "app:read") ||
		actor.StationID != command.StationID || actor.OrganizationID != command.OrganizationID {
		return zero, identity.Denied
	}
	if command.Action == "install" {
		if installed != nil {
			return zero, ErrAppCenterBinding
		}
	} else if command.Action == "start" || command.Action == "stop" || command.Action == "restart" {
		if installed == nil || installed.StationID != actor.StationID || installed.OrganizationID != actor.OrganizationID || installed.AppID != command.AppID ||
			installed.AppVersion != command.AppVersion || installed.State != "installed" {
			return zero, ErrAppCenterBinding
		}
	} else {
		return zero, ErrAppCenterUnsupported
	}
	policies, err := s.policies(ctx)
	if err != nil {
		return zero, err
	}
	var policy *AppCenterReleasePolicy
	for i := range policies {
		p := &policies[i]
		if p.AppID == command.AppID && p.AppVersion == command.AppVersion {
			policy = p
		}
	}
	if policy == nil {
		return zero, ErrAppCenterRelease
	}
	if !slices.Contains(policy.Actions, command.Action) {
		return zero, ErrAppCenterUnsupported
	}
	slices.Sort(policy.Actions)
	execution := AppCenterExecution{SchemaVersion: 1, AppID: policy.AppID, AppVersion: policy.AppVersion, Action: command.Action}
	switch policy.ReleaseKind {
	case "signed-package":
		binding, err := s.Signed.Resolve(policy.ReleaseRef)
		if err != nil || binding.AppID != policy.AppID || binding.AppVersion != policy.AppVersion {
			return zero, ErrAppCenterRelease
		}
		execution.ReleaseDigest = appcenter.Digest(struct {
			Protocol string
			Binding  releasebinding.Binding
		}{"studio.appcenter.signed-release.v1", binding})
		execution.TemplateDigest = binding.TemplateDigest
	case "management-release":
		snapshot, err := s.Management.Resolve(ctx, policy.AppID, policy.AppVersion, policy.ReleaseRef)
		if err != nil {
			return zero, ErrAppCenterRelease
		}
		execution.ReleaseDigest, execution.TemplateDigest = snapshot.ReleaseDigest, snapshot.TemplateDigest
	}
	execution.PolicyRevision = appcenter.Digest(struct {
		Protocol          string
		Policy            AppCenterReleasePolicy
		Release, Template string
	}{
		"studio.appcenter.admission.v1", *policy, execution.ReleaseDigest, execution.TemplateDigest})
	return execution, nil
}

// Read performs the live preflight only when admitting an actual command.
func (s *AppCenterAdmissionSource) Read(ctx context.Context, actor identity.IdentityContext, command Command, installed *Installation) (AppCenterAdmissionResult, error) {
	var zero AppCenterAdmissionResult
	execution, err := s.resolve(ctx, actor, command, installed)
	if err != nil {
		return zero, err
	}
	installationID := ""
	if installed != nil {
		installationID = installed.InstallationID
	}
	if s.Runtime == nil {
		return zero, ErrAppCenterDispatch
	}
	observation, err := s.Runtime.InspectBound(ctx, &pb.AppCenterInspection{SchemaVersion: 1, UserId: actor.UserID, OrganizationId: actor.OrganizationID, StationId: actor.StationID,
		AppId: command.AppID, AppVersion: command.AppVersion, Action: command.Action, InstallationId: installationID, TemplateDigest: execution.TemplateDigest})
	if err != nil {
		switch status.Code(err) {
		case codes.Unimplemented:
			return zero, ErrAppCenterDispatch
		case codes.FailedPrecondition:
			return zero, ErrAppCenterBinding
		case codes.PermissionDenied:
			return zero, identity.Denied
		default:
			return zero, appcenter.ErrUnavailable
		}
	}
	if observation == nil || observation.SchemaVersion != 1 || len(observation.ProtoReflect().GetUnknown()) != 0 || observation.TemplateDigest != execution.TemplateDigest ||
		len(observation.Namespace) > 63 || !appCenterKubernetesName.MatchString(observation.Namespace) || len(observation.WorkloadName) > 63 || !appCenterKubernetesName.MatchString(observation.WorkloadName) {
		return zero, ErrAppCenterBinding
	}
	if command.Action == "install" {
		if observation.State != "absent" || observation.WorkloadUid != "" {
			return zero, ErrAppCenterBinding
		}
	} else {
		uid, err := uuid.Parse(observation.WorkloadUid)
		if err != nil || uid == uuid.Nil || uid.String() != observation.WorkloadUid {
			return zero, ErrAppCenterBinding
		}
		switch command.Action {
		case "start":
			if observation.State != "stopped" {
				return zero, ErrAppCenterBinding
			}
		case "stop":
			if observation.State != "running" {
				return zero, ErrAppCenterBinding
			}
		case "restart":
			if observation.State != "running" && observation.State != "stopped" {
				return zero, ErrAppCenterBinding
			}
		default:
			return zero, ErrAppCenterUnsupported
		}
	}
	execution.WorkloadUID = observation.WorkloadUid
	return AppCenterAdmissionResult{execution, observation}, nil
}
