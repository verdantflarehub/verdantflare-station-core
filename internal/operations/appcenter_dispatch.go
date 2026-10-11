package operations

import (
	"bytes"
	"encoding/json"
	"io"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
)

// Use only the durable inputs accepted in the original transaction. Neither a
// changed catalog nor a new prepare call can replace an operation's identity.
func appCenterDispatchInput(row *pb.AppRequest, prepared, execution, savedHash []byte, key string) (*pb.AppCenterRequest, error) {
	in, err := DecodeAppCenterCommand(bytes.NewReader(prepared))
	if err != nil {
		return nil, ErrAppCenterBinding
	}
	hash := appCenterHash(in)
	c := in.Command
	id, idErr := uuid.Parse(row.InstallationId)
	if !bytes.Equal(hash[:], savedHash) || c.IdempotencyKey != key || c.RequestID != row.RequestId ||
		c.OrganizationID != row.OrganizationId || c.StationID != row.StationId || c.AppID != row.AppId ||
		c.AppVersion != row.AppVersion || c.Action != row.Action || c.ExpectedWorkloadUID != row.ExpectedWorkloadUid ||
		row.PreinstalledRelease != nil || idErr != nil || id.Version() != 7 || id.String() != row.InstallationId {
		return nil, ErrAppCenterBinding
	}
	if in.Expected.AppVersion != row.AppVersion || (in.Expected.InstallationID == nil) != (in.Expected.InstallationRevision == nil) {
		return nil, ErrAppCenterBinding
	}
	if c.Action == "install" {
		if in.Expected.InstallationID != nil || in.Expected.WorkloadUID != nil {
			return nil, ErrAppCenterBinding
		}
	} else if in.Expected.InstallationID == nil || *in.Expected.InstallationID != row.InstallationId {
		return nil, ErrAppCenterBinding
	}
	var fixed AppCenterExecution
	decoder := json.NewDecoder(bytes.NewReader(execution))
	decoder.DisallowUnknownFields()
	if len(execution) > 4096 || decoder.Decode(&fixed) != nil || decoder.Decode(new(any)) != io.EOF || validateAppCenterExecution(in, fixed) != nil {
		return nil, ErrAppCenterBinding
	}
	return &pb.AppCenterRequest{Command: row, Guard: &pb.AppCenterGuard{SchemaVersion: uint32(fixed.SchemaVersion),
		ReleaseDigest: fixed.ReleaseDigest, TemplateDigest: fixed.TemplateDigest, PolicyRevision: fixed.PolicyRevision, WorkloadUid: fixed.WorkloadUID}}, nil
}
