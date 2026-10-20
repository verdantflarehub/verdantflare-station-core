package operations

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
)

func forwardSnapshot(version string) TransitionSnapshot {
	d := "sha256:" + strings.Repeat("a", 64)
	return TransitionSnapshot{AppID: "world", AppVersion: version, ReleaseRef: "world-v" + version, ReleaseDigest: d, ManifestDigest: d, TemplateDigest: d, DashboardDigest: d, CompatibleWorkerVersions: []string{}, ReleaseKind: "dashboard-release", AppImageDigest: d}
}

func TestForwardCommandAndExactScope(t *testing.T) {
	c := ForwardRecoveryCommand{uuid.NewString(), "test-forward-key", uuid.Must(uuid.NewV7()).String(), 5, "world-v0.1.6"}
	if !c.valid() {
		t.Fatal("valid command rejected")
	}
	original := forwardRequestHash(c)
	c.RequestID = uuid.NewString()
	if string(original) != string(forwardRequestHash(c)) {
		t.Fatal("retry trace changed idempotency")
	}
	c.TargetReleaseRef = "world-v0.1.7"
	if string(original) == string(forwardRequestHash(c)) {
		t.Fatal("changed target reused key")
	}
	c.RequestID = c.OperationID
	if c.valid() {
		t.Fatal("non-v4 request accepted")
	}
	p := dispatchTransition{Transition: Transition{ExpectedRevision: 5, Source: forwardSnapshot("0.1.4"), Target: forwardSnapshot("0.1.5"), Status: "running", Phase: "restoring"}, Installed: Installation{AppID: "world", AppVersion: "0.1.4", State: "installed", Revision: 5}}
	target := forwardSnapshot("0.1.6")
	if !allowedWorldForward(p, target) {
		t.Fatal("expected repair rejected")
	}
	for _, change := range []func(*dispatchTransition, *TransitionSnapshot){
		func(p *dispatchTransition, t *TransitionSnapshot) { p.Source.AppID = "project" },
		func(p *dispatchTransition, t *TransitionSnapshot) { t.AppVersion = "0.1.7" },
		func(p *dispatchTransition, t *TransitionSnapshot) { p.Installed.Revision++ },
		func(p *dispatchTransition, t *TransitionSnapshot) { p.Phase = "publishing" },
	} {
		q, x := p, target
		change(&q, &x)
		if allowedWorldForward(q, x) {
			t.Fatal("broadened repair accepted")
		}
	}
}

// Seed only a disposable integration database with a failed original operation
// and durable accepted recovery, then exercise transport retry and publication.
func TestForwardDatabaseRetryProofCASAndIsolation(t *testing.T) {
	for _, scenario := range []string{"success", "missing proof", "revision changed", "rejected"} {
		t.Run(scenario, func(t *testing.T) {
			svc, user, cmd, _ := transitionDatabaseApp(t, "world", "0.1.4")
			ctx := context.Background()
			cmd.SourceVersion = "0.1.4"
			cmd.TargetReleaseRef = "world-v0.1.5"
			old, err := svc.SubmitTransition(ctx, user, cmd, func(context.Context, identity.IdentityContext, Installation, string) (TransitionSnapshot, TransitionSnapshot, error) {
				return forwardSnapshot("0.1.4"), forwardSnapshot("0.1.5"), nil
			})
			if err != nil {
				t.Fatal(err)
			}
			if _, err = svc.Pool.Exec(ctx, `UPDATE station.app_installation_transitions SET status='running',phase='restoring' WHERE operation_id=$1`, old.OperationID); err != nil {
				t.Fatal(err)
			}
			parent, err := readDispatchTransition(ctx, svc.Pool, svc.StationID, old.OperationID)
			if err != nil {
				t.Fatal(err)
			}
			c := ForwardRecoveryCommand{uuid.NewString(), "forward-test-key", old.OperationID, cmd.ExpectedRevision, "world-v0.1.6"}
			r := ForwardRecovery{RecoveryID: uuid.Must(uuid.NewV7()).String(), OperationID: old.OperationID, ExpectedRevision: c.ExpectedRevision, InstalledVersion: "0.1.4", ParentInputDigest: parent.inputDigest(), Target: forwardSnapshot("0.1.6"), State: "pending"}
			r.InputDigest = r.digest()
			raw, _ := json.Marshal(r.Target)
			_, err = svc.Pool.Exec(ctx, `INSERT INTO station.app_installation_forward_recoveries(recovery_id,operation_id,user_id,organization_id,station_id,request_id,idempotency_key,request_hash,expected_revision,installed_version,parent_input_digest,target_snapshot,input_digest,state) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,'pending')`, r.RecoveryID, r.OperationID, user.UserID, user.OrganizationID, svc.StationID, c.RequestID, c.IdempotencyKey, forwardRequestHash(c), r.ExpectedRevision, r.InstalledVersion, r.ParentInputDigest, raw, r.InputDigest)
			if err != nil {
				t.Fatal(err)
			}
			calls := 0
			svc.ForwardRecoveryRuntime = transitionRuntimeFunc(func(ctx context.Context, step TransitionStep) (TransitionProgress, error) {
				calls++
				if step.OperationID != r.RecoveryID || step.InputDigest != r.InputDigest || step.Mode != "verify_forward" {
					t.Fatal("wrong binding")
				}
				if calls == 1 {
					return TransitionProgress{}, context.DeadlineExceeded
				}
				if scenario == "rejected" {
					return TransitionProgress{step, "rejected"}, nil
				}
				if scenario != "missing proof" {
					if _, err := svc.Pool.Exec(ctx, `INSERT INTO station.runtime_forward_recovery_proofs(recovery_id,input_digest,proof) VALUES($1,$2,'{"verified":true}')`, r.RecoveryID, r.InputDigest); err != nil {
						t.Fatal(err)
					}
				}
				if scenario == "revision changed" {
					if _, err := svc.Pool.Exec(ctx, `UPDATE station.app_installations SET revision=revision+1 WHERE installation_id=$1`, cmd.InstallationID); err != nil {
						t.Fatal(err)
					}
				}
				return TransitionProgress{step, "verified"}, nil
			})
			if _, err = svc.SubmitForwardRecovery(ctx, user, c); !errors.Is(err, context.DeadlineExceeded) {
				t.Fatal("lost response", err)
			}
			if _, err = svc.DispatchTransition(ctx, old.OperationID, svc.ForwardRecoveryRuntime); !errors.Is(err, ErrTransitionBusy) {
				t.Fatal("pending did not suspend old recovery", err)
			}
			foreign := user
			foreign.UserID = uuid.Must(uuid.NewV7()).String()
			if _, err = svc.GetForwardRecovery(ctx, foreign, r.RecoveryID); !errors.Is(err, pgx.ErrNoRows) {
				t.Fatal("scope leak", err)
			}
			denied := user
			denied.Roles = nil
			if _, err = svc.SubmitForwardRecovery(ctx, denied, c); !errors.Is(err, identity.Denied) {
				t.Fatal("missing admin accepted", err)
			}
			changed := c
			changed.TargetReleaseRef = "world-v0.1.7"
			if _, err = svc.SubmitForwardRecovery(ctx, user, changed); !errors.Is(err, identity.Conflict) {
				t.Fatal("changed replay accepted", err)
			}
			c.RequestID = uuid.NewString()
			out, err := svc.SubmitForwardRecovery(ctx, user, c)
			installed, _ := svc.GetInstallation(ctx, user, cmd.InstallationID)
			final, _ := svc.GetTransition(ctx, user, old.OperationID)
			if !reflect.DeepEqual(final.Source, old.Source) || !reflect.DeepEqual(final.Target, old.Target) {
				t.Fatal("original snapshots changed")
			}
			if scenario == "success" {
				if err != nil || out.State != "succeeded" || installed.AppVersion != "0.1.6" || installed.Revision != c.ExpectedRevision+1 || final.Status != "failed" || final.Phase != "completed" {
					t.Fatal(out, installed, final, err)
				}
				if _, err = svc.SubmitForwardRecovery(ctx, user, c); err != nil || calls != 2 {
					t.Fatal("terminal replay repeated Runtime", err, calls)
				}
			} else {
				if installed.AppVersion != "0.1.4" || final.Phase != "restoring" {
					t.Fatal("unproven release published")
				}
				if scenario == "rejected" {
					if err != nil || out.State != "failed" {
						t.Fatal(out, err)
					}
				} else if err == nil {
					t.Fatal("invalid proof/CAS accepted")
				}
			}
		})
	}
}
