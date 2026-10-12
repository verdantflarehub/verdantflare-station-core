package operations

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

type transitionRuntimeFunc func(context.Context, TransitionStep) (TransitionProgress, error)

func (f transitionRuntimeFunc) ReconcileTransition(ctx context.Context, step TransitionStep) (TransitionProgress, error) {
	return f(ctx, step)
}

func TestTransitionDispatchPublicationAndLostRelease(t *testing.T) {
	svc, user, command, reader := transitionDatabase(t)
	ctx := context.Background()
	before, _ := svc.GetInstallation(ctx, user, command.InstallationID)
	accepted, err := svc.SubmitTransition(ctx, user, command, reader)
	if err != nil {
		t.Fatal(err)
	}
	var requests []TransitionStep
	releaseLost := true
	runtime := transitionRuntimeFunc(func(ctx context.Context, step TransitionStep) (TransitionProgress, error) {
		requests = append(requests, step)
		// Model Runtime's real Core read on another database connection. Core
		// cannot keep a transition row lock across this external call.
		tx, err := svc.Pool.Begin(ctx)
		if err != nil {
			return TransitionProgress{}, err
		}
		defer tx.Rollback(context.Background())
		if _, err := tx.Exec(ctx, `SELECT operation_id FROM station.app_installation_transitions WHERE operation_id=$1 FOR SHARE NOWAIT`, step.OperationID); err != nil {
			return TransitionProgress{}, err
		}
		var phase string
		if err := tx.QueryRow(ctx, `SELECT phase FROM station.app_installation_transitions WHERE operation_id=$1`, step.OperationID).Scan(&phase); err != nil {
			return TransitionProgress{}, err
		}
		if phase == "accepted" {
			t.Fatal("external effect before checking commit")
		}
		if step.Mode == "release_target" {
			installed, err := svc.GetInstallation(ctx, user, command.InstallationID)
			if err != nil || phase != "releasing" || installed.AppVersion != accepted.Target.AppVersion || installed.Revision != before.Revision+1 {
				t.Fatal("release before durable publication", installed, err)
			}
			if releaseLost {
				releaseLost = false
				return TransitionProgress{}, context.DeadlineExceeded
			}
			return TransitionProgress{step, "released"}, nil
		}
		return TransitionProgress{step, "prepared"}, nil
	})
	advance := func(want string) {
		t.Helper()
		out, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime)
		if err != nil || out.Phase != want {
			t.Fatal(out.Phase, err)
		}
	}
	advance("checking")
	if len(requests) != 0 {
		t.Fatal("accepted called Runtime")
	}
	advance("publishing")
	unchanged, _ := svc.GetInstallation(ctx, user, command.InstallationID)
	if unchanged != before {
		t.Fatal("first prepared observation published version")
	}
	// Simulate a new Core process; fixed accepted snapshots remain authoritative.
	svc = &Service{Pool: svc.Pool, StationID: svc.StationID}
	advance("releasing")
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	pending, _ := svc.GetTransition(ctx, user, accepted.OperationID)
	if pending.Phase != "releasing" || pending.Status != "running" {
		t.Fatal("lost release claimed terminal result", pending)
	}
	advance("completed")
	installed, _ := svc.GetInstallation(ctx, user, command.InstallationID)
	if installed.InstallationID != before.InstallationID || installed.LastOperationID != before.LastOperationID || installed.Revision != before.Revision+1 {
		t.Fatal("publication changed identity/history or incremented twice")
	}
	var audits int
	if err := svc.Pool.QueryRow(ctx, `SELECT count(*) FROM station.audit_events WHERE request_id=$1 AND action IN ('app.upgrade.published','app.upgrade.succeeded')`, command.RequestID).Scan(&audits); err != nil || audits != 2 {
		t.Fatal(audits, err)
	}
	calls := len(requests)
	advance("completed")
	if len(requests) != calls {
		t.Fatal("terminal replay invoked Runtime")
	}
	for i, request := range requests {
		if !transitionDigest.MatchString(request.InputDigest) {
			t.Fatal("missing binding digest")
		}
		for _, prior := range requests[:i] {
			if prior.RequestID == request.RequestID {
				t.Fatal("fresh observation reused request identity")
			}
		}
	}
}

func TestTransitionDispatchRestoresBeforeSourceRelease(t *testing.T) {
	for _, published := range []bool{false, true} {
		t.Run(map[bool]string{false: "before publication", true: "after publication"}[published], func(t *testing.T) {
			svc, user, command, reader := transitionDatabase(t)
			ctx := context.Background()
			original, _ := svc.GetInstallation(ctx, user, command.InstallationID)
			accepted, err := svc.SubmitTransition(ctx, user, command, reader)
			if err != nil {
				t.Fatal(err)
			}
			// Seed a previously durable recovery intent. A production transport
			// must prove target re-sealing before requesting post-publication recovery.
			if _, err := svc.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET status='recovery_required',phase='recovery_required' WHERE operation_id=$1`, accepted.OperationID); err != nil {
				t.Fatal(err)
			}
			if published {
				if _, err := svc.Pool.Exec(ctx, `UPDATE station.app_installations SET app_version=$2,revision=revision+1 WHERE installation_id=$1`, command.InstallationID, accepted.Target.AppVersion); err != nil {
					t.Fatal(err)
				}
			}
			var modes []string
			released := false
			runtime := transitionRuntimeFunc(func(ctx context.Context, step TransitionStep) (TransitionProgress, error) {
				modes = append(modes, step.Mode)
				if step.Mode == "restore" {
					return TransitionProgress{step, "restored"}, nil
				}
				installed, err := svc.GetInstallation(ctx, user, command.InstallationID)
				if err != nil || installed.AppVersion != original.AppVersion || step.Mode != "release_source" {
					t.Fatal("source released before Core compensation", err)
				}
				if !released {
					released = true
					return TransitionProgress{}, context.DeadlineExceeded
				}
				return TransitionProgress{step, "released"}, nil
			})
			for _, phase := range []string{"restoring", "releasing"} {
				out, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime)
				if err != nil || out.Phase != phase {
					t.Fatal(out.Phase, err)
				}
			}
			if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal(err)
			}
			out, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime)
			if err != nil || out.Status != "failed" || out.Phase != "completed" {
				t.Fatal(out, err)
			}
			installed, _ := svc.GetInstallation(ctx, user, command.InstallationID)
			wantRevision := original.Revision
			if published {
				wantRevision += 2
			}
			if installed.Revision != wantRevision || installed.LastOperationID != original.LastOperationID {
				t.Fatal("compensation rewrote history", installed)
			}
			if len(modes) != 3 || modes[0] != "restore" || modes[1] != "release_source" || modes[2] != "release_source" {
				t.Fatal("release retry reverted to resource restore", modes)
			}
		})
	}
}

func TestTransitionDispatchAuditFailureAndInvalidProof(t *testing.T) {
	svc, user, command, reader := transitionDatabase(t)
	ctx := context.Background()
	accepted, err := svc.SubmitTransition(ctx, user, command, reader)
	if err != nil {
		t.Fatal(err)
	}
	bad := false
	runtime := transitionRuntimeFunc(func(_ context.Context, step TransitionStep) (TransitionProgress, error) {
		if bad {
			step.InputDigest = "sha256:" + string(make([]byte, 64))
		}
		return TransitionProgress{step, "prepared"}, nil
	})
	for range 2 {
		if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); err != nil {
			t.Fatal(err)
		}
	}
	before, _ := svc.GetInstallation(ctx, user, command.InstallationID)
	bad = true
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); !errors.Is(err, ErrTransitionProgress) {
		t.Fatal("unbound proof accepted", err)
	}
	bad = false
	_, err = svc.Pool.Exec(ctx, `CREATE FUNCTION station.fail_publish_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='app.upgrade.published' THEN RAISE EXCEPTION 'test audit failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_publish_audit BEFORE INSERT ON station.audit_events FOR EACH ROW EXECUTE FUNCTION station.fail_publish_audit()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); err == nil {
		t.Fatal("publication without audit succeeded")
	}
	after, _ := svc.GetInstallation(ctx, user, command.InstallationID)
	if after != before {
		t.Fatal("audit failure did not roll back version CAS")
	}
	current, _ := svc.GetTransition(ctx, user, accepted.OperationID)
	if current.Phase != "publishing" {
		t.Fatal("failed commit advanced phase")
	}
	if _, err := svc.Pool.Exec(ctx, `DROP TRIGGER fail_publish_audit ON station.audit_events`); err != nil {
		t.Fatal(err)
	}
	if out, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); err != nil || out.Phase != "releasing" {
		t.Fatal(out, err)
	}
	// Even a well-bound recovery response cannot undo a potentially released target.
	runtime = transitionRuntimeFunc(func(_ context.Context, step TransitionStep) (TransitionProgress, error) {
		return TransitionProgress{step, "recovery_required"}, nil
	})
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); !errors.Is(err, ErrTransitionProgress) {
		t.Fatal(err)
	}
	current, _ = svc.GetTransition(ctx, user, accepted.OperationID)
	if current.Phase != "releasing" {
		t.Fatal("release uncertainty caused unsafe restore")
	}
}

func TestTransitionDispatchConcurrencyCancellationAndDrift(t *testing.T) {
	svc, user, command, reader := transitionDatabase(t)
	ctx := context.Background()
	accepted, err := svc.SubmitTransition(ctx, user, command, reader)
	if err != nil {
		t.Fatal(err)
	}
	entered := make(chan struct{})
	var once sync.Once
	runtime := transitionRuntimeFunc(func(ctx context.Context, step TransitionStep) (TransitionProgress, error) {
		once.Do(func() { close(entered) })
		<-ctx.Done()
		return TransitionProgress{}, ctx.Err()
	})
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := svc.DispatchTransition(canceled, accepted.OperationID, runtime); done <- err }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("dispatch did not reach Runtime")
	}
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); !errors.Is(err, ErrTransitionBusy) {
		t.Fatal("duplicate coordinator not excluded", err)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	runtime = transitionRuntimeFunc(func(_ context.Context, step TransitionStep) (TransitionProgress, error) {
		return TransitionProgress{step, "prepared"}, nil
	})
	if out, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); err != nil || out.Phase != "publishing" {
		t.Fatal("cancellation retained lock", out.Phase, err)
	}
	runtime = transitionRuntimeFunc(func(ctx context.Context, step TransitionStep) (TransitionProgress, error) {
		_, err := svc.Pool.Exec(ctx, `UPDATE station.app_installations SET revision=revision+1 WHERE installation_id=$1`, command.InstallationID)
		return TransitionProgress{step, "prepared"}, err
	})
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); !errors.Is(err, identity.Conflict) {
		t.Fatal("late proof overwrote concurrent installation revision", err)
	}
	installed, _ := svc.GetInstallation(ctx, user, command.InstallationID)
	if installed.AppVersion != command.SourceVersion {
		t.Fatal("drift published target")
	}
	if _, err := svc.DispatchTransition(ctx, uuid.Must(uuid.NewV7()).String(), runtime); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatal("missing operation not rejected", err)
	}
}

func TestTransitionDispatchProofBindingAndMonotonicPhases(t *testing.T) {
	svc, user, command, reader := transitionDatabase(t)
	ctx := context.Background()
	accepted, err := svc.SubmitTransition(ctx, user, command, reader)
	if err != nil {
		t.Fatal(err)
	}
	runtime := transitionRuntimeFunc(func(_ context.Context, step TransitionStep) (TransitionProgress, error) {
		return TransitionProgress{step, "sealing"}, nil
	})
	for range 2 {
		if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); err != nil {
			t.Fatal(err)
		}
	}
	before, err := svc.GetTransition(ctx, user, accepted.OperationID)
	if err != nil || before.Phase != "sealing" {
		t.Fatal(before.Phase, err)
	}
	for name, mutate := range map[string]func(*TransitionProgress){
		"operation":     func(p *TransitionProgress) { p.OperationID = uuid.Must(uuid.NewV7()).String() },
		"nonce":         func(p *TransitionProgress) { p.RequestID = uuid.NewString() },
		"mode":          func(p *TransitionProgress) { p.Mode = "release_target" },
		"digest":        func(p *TransitionProgress) { p.InputDigest = "sha256:invalid" },
		"regression":    func(p *TransitionProgress) { p.Phase = "checking" },
		"unknown":       func(p *TransitionProgress) { p.Phase = "ready" },
		"early release": func(p *TransitionProgress) { p.Phase = "released" },
	} {
		t.Run(name, func(t *testing.T) {
			runtime := transitionRuntimeFunc(func(_ context.Context, step TransitionStep) (TransitionProgress, error) {
				proof := TransitionProgress{step, "prepared"}
				mutate(&proof)
				return proof, nil
			})
			if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); !errors.Is(err, ErrTransitionProgress) {
				t.Fatal("invalid proof accepted", err)
			}
			after, err := svc.GetTransition(ctx, user, accepted.OperationID)
			if err != nil || after.Phase != before.Phase || after.Status != before.Status || !after.UpdatedAt.Equal(before.UpdatedAt) {
				t.Fatal("invalid proof changed durable operation", err)
			}
		})
	}
	runtime = transitionRuntimeFunc(func(_ context.Context, step TransitionStep) (TransitionProgress, error) {
		return TransitionProgress{step, "applying"}, nil
	})
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); err != nil {
		t.Fatal(err)
	}
	runtime = transitionRuntimeFunc(func(_ context.Context, step TransitionStep) (TransitionProgress, error) {
		return TransitionProgress{step, "rejected"}, nil
	})
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); !errors.Is(err, ErrTransitionProgress) {
		t.Fatal("rejection after mutation treated as safe terminal failure", err)
	}
}

func TestTransitionDispatchRejectionAuditRollbackAndAdmission(t *testing.T) {
	svc, user, command, reader := transitionDatabase(t)
	ctx := context.Background()
	accepted, err := svc.SubmitTransition(ctx, user, command, reader)
	if err != nil {
		t.Fatal(err)
	}
	runtime := transitionRuntimeFunc(func(_ context.Context, step TransitionStep) (TransitionProgress, error) {
		return TransitionProgress{step, "rejected"}, nil
	})
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); err != nil {
		t.Fatal(err)
	}
	_, err = svc.Pool.Exec(ctx, `CREATE FUNCTION station.fail_terminal_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='app.upgrade.failed' THEN RAISE EXCEPTION 'test audit failure'; END IF; RETURN NEW; END $$; CREATE TRIGGER fail_terminal_audit BEFORE INSERT ON station.audit_events FOR EACH ROW EXECUTE FUNCTION station.fail_terminal_audit()`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime); err == nil {
		t.Fatal("terminal result without audit")
	}
	current, err := svc.GetTransition(ctx, user, accepted.OperationID)
	if err != nil || current.Phase != "checking" || current.Status != "running" {
		t.Fatal("failed audit advanced operation", err)
	}
	ordinary := Command{RequestID: "ordinary-stop", IdempotencyKey: "ordinary-stop-key", OrganizationID: user.OrganizationID, StationID: svc.StationID, AppID: "test-app", AppVersion: command.SourceVersion, Action: "stop"}
	if _, err := svc.Submit(ctx, user, ordinary); !errors.Is(err, identity.Conflict) {
		t.Fatal("pending transition allowed ordinary command", err)
	}
	if _, err := svc.Pool.Exec(ctx, `DROP TRIGGER fail_terminal_audit ON station.audit_events`); err != nil {
		t.Fatal(err)
	}
	out, err := svc.DispatchTransition(ctx, accepted.OperationID, runtime)
	if err != nil || out.Phase != "completed" || out.Status != "failed" {
		t.Fatal("safe pre-seal rejection failed", err)
	}
	if _, err := svc.Submit(ctx, user, ordinary); err != nil {
		t.Fatal("terminal rejection retained admission block", err)
	}
}
