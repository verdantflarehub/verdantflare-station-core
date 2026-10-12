package operations

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func TestTransitionWorkerFairRecoveryAndReleaseBarrier(t *testing.T) {
	svc, user, command, reader := transitionDatabase(t)
	ctx := context.Background()
	first, err := svc.SubmitTransition(ctx, user, command, reader)
	if err != nil {
		t.Fatal(err)
	}
	other := command
	other.InstallationID = uuid.Must(uuid.NewV7()).String()
	other.IdempotencyKey = "second-installation-transition"
	other.TargetReleaseRef = "other-app-target"
	if _, err := svc.Pool.Exec(ctx, `INSERT INTO station.app_installations(installation_id,created_by,organization_id,station_id,app_id,app_version,state,revision) VALUES($1,$2,$3,$4,'other-app',$5,'installed',$6)`, other.InstallationID, user.UserID, user.OrganizationID, svc.StationID, command.SourceVersion, command.ExpectedRevision); err != nil {
		t.Fatal(err)
	}
	otherReader := func(ctx context.Context, u identity.IdentityContext, i Installation, ref string) (TransitionSnapshot, TransitionSnapshot, error) {
		s, d, err := reader(ctx, u, i, ref)
		s.AppID = "other-app"
		d.AppID = "other-app"
		s.ReleaseRef = "other-app-source"
		d.ReleaseRef = other.TargetReleaseRef
		return s, d, err
	}
	second, err := svc.SubmitTransition(ctx, user, other, otherReader)
	if err != nil {
		t.Fatal(err)
	}
	releasing := true
	runtime := transitionRuntimeFunc(func(_ context.Context, step TransitionStep) (TransitionProgress, error) {
		if step.OperationID == first.OperationID {
			return TransitionProgress{}, context.DeadlineExceeded
		}
		phase := "prepared"
		if step.Mode == "release_target" {
			phase = "released"
			if releasing {
				phase = "releasing"
				releasing = false
			}
		}
		return TransitionProgress{step, phase}, nil
	})
	cursor := ""
	seen := map[string]int{}
	for range 10 {
		next, err := svc.DispatchNextTransition(ctx, runtime, cursor)
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
		seen[next]++
		cursor = next
	}
	if seen[first.OperationID] != 5 || seen[second.OperationID] != 5 {
		t.Fatal("failed operation starved the other installation", seen)
	}
	out, err := svc.GetTransition(ctx, user, second.OperationID)
	if err != nil || out.Status != "succeeded" {
		t.Fatal("release barrier did not finish", out.Phase, err)
	}
	// New process restarts scheduling without losing either durable result.
	svc = &Service{Pool: svc.Pool, StationID: svc.StationID}
	if next, err := svc.DispatchNextTransition(ctx, runtime, ""); next != first.OperationID || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("restart forgot pending operation", err)
	}
	if _, err := svc.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET status='failed',phase='completed' WHERE operation_id=$1`, first.OperationID); err != nil {
		t.Fatal(err)
	}
	if next, err := svc.DispatchNextTransition(ctx, runtime, cursor); next != "" || err != nil {
		t.Fatal("terminal record redispatched", err)
	}
}

func TestTransitionWorkerCancellation(t *testing.T) {
	svc, user, command, reader := transitionDatabase(t)
	if _, err := svc.SubmitTransition(context.Background(), user, command, reader); err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	runtime := transitionRuntimeFunc(func(ctx context.Context, _ TransitionStep) (TransitionProgress, error) {
		close(entered)
		<-ctx.Done()
		return TransitionProgress{}, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); svc.RunTransitions(ctx, runtime, nil) }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("worker never called Runtime")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker retained canceled call")
	}
}
