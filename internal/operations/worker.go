package operations

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/releasebinding"
	pb "github.com/verdantflarehub/verdantflare-station-core/internal/runtimev1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DispatchOne holds a Core row lock until the observed Runtime result is recorded.
// A failed/lost RPC leaves the original operation available for reconciliation.
func (s *Service) DispatchOne(ctx context.Context, client pb.AppRuntimeClient) (bool, error) {
	tx, err := s.Pool.Begin(ctx)
	if err != nil {
		return false, err
	}
	defer tx.Rollback(context.Background())
	in := &pb.AppRequest{}
	var bindingRaw []byte
	err = tx.QueryRow(ctx, `SELECT operation_id::text,request_id,user_id::text,organization_id::text,station_id::text,app_id,app_version,action,expected_workload_uid,(SELECT binding FROM station.preinstalled_operation_bindings b WHERE b.operation_id=station.app_operations.operation_id),COALESCE(installation_id::text,'') FROM station.app_operations WHERE station_id=$1 AND status IN ('accepted','running') ORDER BY updated_at,operation_id LIMIT 1 FOR UPDATE SKIP LOCKED`, s.StationID).Scan(&in.OperationId, &in.RequestId, &in.UserId, &in.OrganizationId, &in.StationId, &in.AppId, &in.AppVersion, &in.Action, &in.ExpectedWorkloadUid, &bindingRaw, &in.InstallationId)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	var preparedRaw, executionRaw, requestHash []byte
	var idempotencyKey string
	err = tx.QueryRow(ctx, `SELECT i.prepared,i.execution,o.request_hash,o.idempotency_key FROM station.appcenter_operation_inputs i JOIN station.app_operations o USING(operation_id) WHERE i.operation_id=$1`, in.OperationId).Scan(&preparedRaw, &executionRaw, &requestHash, &idempotencyKey)
	if err != nil && !errors.Is(err, pgx.ErrNoRows) {
		return true, err
	}
	constrained := err == nil
	var bound *pb.AppCenterRequest
	if constrained && s.AppCenterRuntime == nil {
		return postponeDispatch(ctx, tx, in.OperationId, ErrAppCenterDispatch)
	}
	if constrained {
		if len(bindingRaw) != 0 {
			return postponeDispatch(ctx, tx, in.OperationId, ErrAppCenterBinding)
		}
		bound, err = appCenterDispatchInput(in, preparedRaw, executionRaw, requestHash, idempotencyKey)
		if err != nil {
			return postponeDispatch(ctx, tx, in.OperationId, err)
		}
	}
	if len(bindingRaw) > 0 {
		var binding releasebinding.Binding
		if json.Unmarshal(bindingRaw, &binding) != nil || !binding.Valid() || binding.AppID != in.AppId || binding.AppVersion != in.AppVersion {
			return true, errors.New("invalid preinstalled operation binding")
		}
		in.PreinstalledRelease = &pb.PreinstalledRelease{ReleaseRef: binding.ReleaseRef, PackageDigest: binding.PackageDigest, ChartDigest: binding.ChartDigest, TemplateDigest: binding.TemplateDigest}
	}
	callCtx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	var progress *pb.AppProgress
	var callErr error
	if constrained {
		progress, callErr = s.AppCenterRuntime.ReconcileBound(callCtx, bound)
	} else {
		progress, callErr = client.Reconcile(callCtx, in)
	}
	if callErr != nil {
		switch status.Code(callErr) {
		case codes.InvalidArgument, codes.PermissionDenied, codes.AlreadyExists, codes.FailedPrecondition:
			code := "INVALID_ARGUMENT"
			message := "Runtime rejected the command or execution template; check registered application version"
			if status.Code(callErr) == codes.AlreadyExists {
				code = "CONFLICT"
			}
			if status.Code(callErr) == codes.PermissionDenied {
				code = "PERMISSION_DENIED"
			}
			progress = &pb.AppProgress{OperationId: in.OperationId, Status: "failed", Phase: "checking", ErrorCode: code, ErrorMessage: message}
		default:
			// Touch only scheduling time so one offline/slow app cannot starve the queue.
			return postponeDispatch(ctx, tx, in.OperationId, callErr)
		}
	}
	progress = normalizeRuntimeProgress(progress)
	if progress == nil || progress.OperationId != in.OperationId || !validProgress(progress) {
		return true, errors.New("invalid runtime progress")
	}
	_, err = tx.Exec(ctx, `UPDATE station.app_operations SET status=$2,phase=$3,downloaded_bytes=$4,total_bytes=$5,error_code=NULLIF($6,''),error_message=NULLIF($7,''),updated_at=now() WHERE operation_id=$1`, in.OperationId, progress.Status, progress.Phase, progress.DownloadedBytes, progress.TotalBytes, progress.ErrorCode, progress.ErrorMessage)
	if err != nil {
		return true, err
	}
	if err = recordInstallationProgress(ctx, tx, in.OperationId, in.Action, progress.Status); err != nil {
		return true, err
	}
	return true, tx.Commit(ctx)
}

func postponeDispatch(ctx context.Context, tx pgx.Tx, id string, pending error) (bool, error) {
	if _, err := tx.Exec(ctx, `UPDATE station.app_operations SET updated_at=now() WHERE operation_id=$1`, id); err != nil {
		return true, err
	}
	if err := tx.Commit(ctx); err != nil {
		return true, err
	}
	return true, pending
}

// Older Runtime releases durably recorded this Helm-specific code. Preserve
// terminal replay and its diagnostic while translating to the lifecycle contract.
func normalizeRuntimeProgress(p *pb.AppProgress) *pb.AppProgress {
	if p != nil && p.Status == "failed" && p.ErrorCode == "CHART_EXECUTION_FAILED" {
		p.ErrorCode = "SERVICE_UNAVAILABLE"
	}
	return p
}

func validProgress(p *pb.AppProgress) bool {
	if p.DownloadedBytes < 0 || p.TotalBytes != nil && (*p.TotalBytes < 0 || p.DownloadedBytes > *p.TotalBytes) {
		return false
	}
	phases := map[string]bool{"checking": true, "downloading": true, "verifying": true, "installing": true, "starting": true, "warming": true, "stopping": true, "deleting": true, "completed": true}
	if !phases[p.Phase] {
		return false
	}
	switch p.Status {
	case "succeeded":
		return p.Phase == "completed" && p.ErrorCode == "" && p.ErrorMessage == ""
	case "failed":
		return p.Phase != "completed" && p.ErrorMessage != "" && (p.ErrorCode == "INVALID_ARGUMENT" || p.ErrorCode == "NOT_FOUND" || p.ErrorCode == "CONFLICT" || p.ErrorCode == "SERVICE_UNAVAILABLE" || p.ErrorCode == "INTERNAL" || p.ErrorCode == "PERMISSION_DENIED")
	case "running":
		return p.Phase != "completed" && p.ErrorCode == "" && p.ErrorMessage == ""
	}
	return false
}
func (s *Service) Run(ctx context.Context, client pb.AppRuntimeClient, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			step, cancel := context.WithTimeout(ctx, 10*time.Second)
			_, err := s.DispatchOne(step, client)
			cancel()
			if err != nil && ctx.Err() == nil {
				logger.Warn("runtime_reconciliation_pending", "grpc_code", status.Code(err).String())
			}
		}
	}
}
