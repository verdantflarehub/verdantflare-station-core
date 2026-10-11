package operations

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type protectedRuntimeStub struct {
	calls []*pb.AppCenterRequest
	err   error
}

func (s *protectedRuntimeStub) ReconcileBound(_ context.Context, in *pb.AppCenterRequest, _ ...grpc.CallOption) (*pb.AppProgress, error) {
	s.calls = append(s.calls, proto.Clone(in).(*pb.AppCenterRequest))
	if s.err != nil {
		return nil, s.err
	}
	return &pb.AppProgress{OperationId: in.Command.OperationId, Status: "succeeded", Phase: "completed"}, nil
}

func TestAppCenterCommandDatabaseProtectedDispatch(t *testing.T) {
	svc, actor, input, reader := appCenterDatabase(t)
	ctx := context.Background()
	operation, err := svc.SubmitAppCenter(ctx, actor, input, reader)
	if err != nil {
		t.Fatal(err)
	}
	stub := &protectedRuntimeStub{}
	svc.AppCenterRuntime = stub
	for _, code := range []codes.Code{codes.Unimplemented, codes.Unavailable} {
		stub.err = status.Error(code, "response unavailable")
		// A nil legacy client proves neither rejection nor lost response falls back.
		if found, err := svc.DispatchOne(ctx, nil); !found || status.Code(err) != code {
			t.Fatal(found, err)
		}
		var state string
		if err := svc.Pool.QueryRow(ctx, `SELECT status FROM station.app_operations WHERE operation_id=$1`, operation.OperationID).Scan(&state); err != nil || state != "accepted" {
			t.Fatal("retryable result lost", state, err)
		}
	}
	// Recreate the worker without catalog/admission. Only persisted input is used.
	restored := &Service{Pool: svc.Pool, StationID: svc.StationID, AppCenterRuntime: stub}
	stub.err = nil
	if found, err := restored.DispatchOne(ctx, nil); !found || err != nil {
		t.Fatal(found, err)
	}
	if len(stub.calls) != 3 || !proto.Equal(stub.calls[0], stub.calls[1]) || !proto.Equal(stub.calls[0], stub.calls[2]) {
		t.Fatal("recovery changed protected request")
	}
	in := stub.calls[0]
	if in.Command.InstallationId != *input.Expected.InstallationID || in.Guard.WorkloadUid != *input.Expected.WorkloadUID || in.Guard.ReleaseDigest != *input.Expected.ReleaseDigest || in.Guard.SchemaVersion != 1 {
		t.Fatal("dispatch lost original binding")
	}
	completed, err := restored.Get(ctx, actor, operation.OperationID)
	if err != nil || completed.Status != "succeeded" {
		t.Fatal(completed, err)
	}
}

func TestAppCenterCommandDatabaseCorruptJournal(t *testing.T) {
	for _, variant := range []string{"prepared", "execution", "installation", "key", "hash"} {
		t.Run(variant, func(t *testing.T) {
			svc, actor, input, reader := appCenterDatabase(t)
			ctx := context.Background()
			op, err := svc.SubmitAppCenter(ctx, actor, input, reader)
			if err != nil {
				t.Fatal(err)
			}
			var sql string
			switch variant {
			case "prepared":
				sql = `UPDATE station.appcenter_operation_inputs SET prepared=jsonb_set(prepared,'{command,app_version}','"2.0.0"') WHERE operation_id=$1`
			case "execution":
				sql = `UPDATE station.appcenter_operation_inputs SET execution=jsonb_set(execution,'{workloadUID}','"00000000-0000-4000-8000-000000000099"') WHERE operation_id=$1`
			case "installation":
				sql = `UPDATE station.app_operations SET installation_id=NULL WHERE operation_id=$1`
			case "key":
				sql = `UPDATE station.app_operations SET idempotency_key='different-key' WHERE operation_id=$1`
			case "hash":
				sql = `UPDATE station.app_operations SET request_hash=decode(repeat('00',32),'hex') WHERE operation_id=$1`
			}
			if _, err := svc.Pool.Exec(ctx, sql, op.OperationID); err != nil {
				t.Fatal(err)
			}
			if _, err := svc.Pool.Exec(ctx, `UPDATE station.app_operations SET updated_at=now()-interval '1 day' WHERE operation_id=$1`, op.OperationID); err != nil {
				t.Fatal(err)
			}
			stub := &protectedRuntimeStub{}
			svc.AppCenterRuntime = stub
			if found, err := svc.DispatchOne(ctx, nil); !found || !errors.Is(err, ErrAppCenterBinding) || len(stub.calls) != 0 {
				t.Fatal("corrupt journal dispatched", found, err)
			}
			var updated time.Time
			var state string
			if err := svc.Pool.QueryRow(ctx, `SELECT updated_at,status FROM station.app_operations WHERE operation_id=$1`, op.OperationID).Scan(&updated, &state); err != nil || time.Since(updated) > time.Minute || state != "accepted" {
				t.Fatal("invalid journal blocks the queue or loses recovery state", state, err)
			}
		})
	}
}

func TestAppCenterDispatchInput(t *testing.T) {
	in := commandFixture()
	exec := AppCenterExecution{AppID: in.Command.AppID, AppVersion: in.Command.AppVersion, Action: in.Command.Action, SchemaVersion: 1,
		ReleaseDigest: *in.Expected.ReleaseDigest, TemplateDigest: *in.Expected.ReleaseDigest, PolicyRevision: in.Expected.PolicyRevision, WorkloadUID: *in.Expected.WorkloadUID}
	raw, _ := json.Marshal(in)
	fixed, _ := json.Marshal(exec)
	hash := appCenterHash(in)
	row := &pb.AppRequest{OperationId: uuid.NewString(), UserId: uuid.NewString(), OrganizationId: in.Command.OrganizationID,
		StationId: in.Command.StationID, RequestId: in.Command.RequestID, AppId: in.Command.AppID, AppVersion: in.Command.AppVersion, Action: in.Command.Action, InstallationId: *in.Expected.InstallationID}
	if _, err := appCenterDispatchInput(row, raw, fixed, hash[:], in.Command.IdempotencyKey); err != nil {
		t.Fatal(err)
	}
	row.InstallationId = uuid.Must(uuid.NewV7()).String()
	if _, err := appCenterDispatchInput(row, raw, fixed, hash[:], in.Command.IdempotencyKey); !errors.Is(err, ErrAppCenterBinding) {
		t.Fatal("reinstallation changed identity", err)
	}
}
