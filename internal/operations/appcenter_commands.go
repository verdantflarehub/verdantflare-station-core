package operations

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"slices"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/appcenter"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

type AppCenterExpected struct {
	InstallationID       *string `json:"installationId"`
	InstallationRevision *int64  `json:"installationRevision"`
	AppVersion           string  `json:"appVersion"`
	ReleaseDigest        *string `json:"releaseDigest"`
	WorkloadUID          *string `json:"workloadUID"`
	PolicyRevision       string  `json:"policyRevision"`
}

type AppCenterCommand struct {
	SchemaVersion int               `json:"schemaVersion"`
	Command       Command           `json:"command"`
	Expected      AppCenterExpected `json:"expected"`
}

// This is server-verified execution input, never a client-supplied template or
// endpoint. Runtime must consume it before this submitter is exposed publicly.
type AppCenterExecution struct {
	AppID          string `json:"appId"`
	AppVersion     string `json:"appVersion"`
	Action         string `json:"action"`
	SchemaVersion  int    `json:"schemaVersion"`
	ReleaseDigest  string `json:"releaseDigest"`
	TemplateDigest string `json:"templateDigest"`
	PolicyRevision string `json:"policyRevision"`
	WorkloadUID    string `json:"workloadUID"`
	marketRelease  bool
}

// The platform-owned reader resolves the exact version and action, checks live
// workload identity/absence and safety support, and verifies original release
// materials. It runs under the Station/App lock, only for a NEW operation.
// An error means no operation or installation may be committed.
type AppCenterAdmission func(context.Context, identity.IdentityContext, Command, *Installation) (AppCenterExecution, error)

var ErrAppCenterBinding = errors.New("APPCENTER_BINDING_CHANGED")
var ErrAppCenterRelease = errors.New("RELEASE_UNVERIFIED")
var ErrAppCenterDispatch = errors.New("APPCENTER_EXECUTION_BINDING_UNSUPPORTED")

func DecodeAppCenterCommand(r io.Reader) (AppCenterCommand, error) {
	var in AppCenterCommand
	if err := appcenter.Decode(r, "command-core", 16<<10, &in); err != nil {
		return in, identity.Invalid
	}
	return in, nil
}

func appCenterHash(in AppCenterCommand) [32]byte {
	in.Command.RequestID = ""
	raw, _ := json.Marshal(struct {
		Protocol string           `json:"protocol"`
		Input    AppCenterCommand `json:"input"`
	}{"studio.appcenter.command.v1", in})
	return sha256.Sum256(raw)
}

// The gateway supplies a production reader from controlled configuration.
// Keeping admission outside the request also makes prepared data no authority.
func (s *Service) SubmitAppCenter(ctx context.Context, u identity.IdentityContext, in AppCenterCommand, admission AppCenterAdmission) (Operation, error) {
	if !s.canReadInstallations(u) || !slices.Contains(u.Scopes, "app:manage") {
		return Operation{}, identity.Denied
	}
	raw, err := json.Marshal(in)
	if err != nil {
		return Operation{}, identity.Invalid
	}
	if _, err := DecodeAppCenterCommand(bytes.NewReader(raw)); err != nil {
		return Operation{}, err
	}
	return s.submit(ctx, u, in.Command, nil, &in, admission)
}

func validateAppCenterInstallation(ctx context.Context, tx pgx.Tx, actor identity.IdentityContext, in AppCenterCommand) (*Installation, error) {
	// Semantic preconditions follow durable replay lookup. Changing even an
	// inconsistent expected tuple under an accepted key is a payload conflict.
	if in.Expected.AppVersion != in.Command.AppVersion || (in.Expected.InstallationID == nil) != (in.Expected.InstallationRevision == nil) {
		return nil, identity.Invalid
	}
	i, err := scanInstallation(tx.QueryRow(ctx, `SELECT `+installationColumns+` FROM station.app_installations WHERE station_id=$1 AND app_id=$2 AND state<>'removed' FOR UPDATE`, in.Command.StationID, in.Command.AppID))
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return nil, err
	}
	if in.Command.Action == "install" {
		if err == nil || in.Expected.InstallationID != nil || in.Expected.InstallationRevision != nil || in.Expected.WorkloadUID != nil {
			return nil, ErrAppCenterBinding
		}
		return nil, nil
	}
	if in.Command.Action == "upgrade" {
		if err != nil || in.Expected.InstallationID == nil || in.Expected.InstallationRevision == nil ||
			i.InstallationID != *in.Expected.InstallationID || i.Revision != *in.Expected.InstallationRevision ||
			i.AppVersion == in.Expected.AppVersion || i.OrganizationID != actor.OrganizationID || i.State != "installed" {
			return nil, ErrAppCenterBinding
		}
		return &i, nil
	}
	if err != nil || in.Expected.InstallationID == nil || in.Expected.InstallationRevision == nil ||
		i.InstallationID != *in.Expected.InstallationID || i.Revision != *in.Expected.InstallationRevision ||
		i.AppVersion != in.Expected.AppVersion || i.OrganizationID != actor.OrganizationID || i.State != "installed" {
		return nil, ErrAppCenterBinding
	}
	return &i, nil
}

func validateAppCenterExecution(in AppCenterCommand, execution AppCenterExecution) error {
	if execution.AppID != in.Command.AppID || execution.AppVersion != in.Command.AppVersion || execution.Action != in.Command.Action ||
		execution.SchemaVersion != 1 || !transitionDigest.MatchString(execution.ReleaseDigest) ||
		!transitionDigest.MatchString(execution.TemplateDigest) || !transitionDigest.MatchString(execution.PolicyRevision) || in.Expected.ReleaseDigest == nil {
		return ErrAppCenterRelease
	}
	wantUID := ""
	if in.Expected.WorkloadUID != nil {
		wantUID = *in.Expected.WorkloadUID
	}
	if execution.ReleaseDigest != *in.Expected.ReleaseDigest || execution.PolicyRevision != in.Expected.PolicyRevision || execution.WorkloadUID != wantUID {
		return ErrAppCenterBinding
	}
	return nil
}

func bindAppCenterInput(ctx context.Context, tx pgx.Tx, id string, in AppCenterCommand, execution AppCenterExecution) error {
	prepared, err := json.Marshal(in)
	if err != nil {
		return err
	}
	bound, err := json.Marshal(execution)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `INSERT INTO station.appcenter_operation_inputs(operation_id,prepared,execution) VALUES($1,$2,$3)`, id, prepared, bound)
	return err
}
