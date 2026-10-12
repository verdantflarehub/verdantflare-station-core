package operations

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Invoked by Runtime's matching test on 5090 with a private temporary config.
// Core and Runtime execute their real transport/journal code in separate processes.
func TestCrossServiceTransition(t *testing.T) {
	path := os.Getenv("STATION_TRANSITION_BRIDGE_CONFIG")
	if path == "" {
		t.Skip("remote cross-service fixture is required")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal("bridge configuration unavailable")
	}
	var cfg struct{ DatabaseURL, StationID, OperationID, Origin, Token string }
	if json.Unmarshal(raw, &cfg) != nil {
		t.Fatal("invalid bridge configuration")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	pool, err := pgxpool.New(ctx, cfg.DatabaseURL)
	if err != nil {
		t.Fatal("bridge database unavailable")
	}
	defer pool.Close()
	client, err := NewHTTPTransitionRuntime(cfg.Origin, cfg.Token)
	if err != nil {
		t.Fatal("bridge transport configuration invalid")
	}
	defer client.client.CloseIdleConnections()
	transientErrors := 0
	for attempt := 0; attempt < 32; attempt++ {
		// A new coordinator object has no remembered phase or publication proof.
		svc := &Service{Pool: pool, StationID: cfg.StationID}
		out, err := svc.DispatchTransition(ctx, cfg.OperationID, client)
		if err != nil {
			transientErrors++
			if transientErrors > 1 {
				t.Fatal("unexpected bridge reconciliation failure")
			}
			continue
		}
		if out.Status == "succeeded" {
			if transientErrors != 1 {
				t.Fatal("lost release response was not exercised")
			}
			var version, status, phase string
			var revision int64
			if pool.QueryRow(ctx, `SELECT i.app_version,i.revision,t.status,t.phase FROM station.app_installations i JOIN station.app_installation_transitions t USING(installation_id) WHERE t.operation_id=$1`, cfg.OperationID).Scan(&version, &revision, &status, &phase) != nil {
				t.Fatal("bridge final state unavailable")
			}
			if version != out.Target.AppVersion || revision != out.ExpectedRevision+1 || status != "succeeded" || phase != "completed" {
				t.Fatal("bridge published inconsistent final state")
			}
			var audits int
			if pool.QueryRow(ctx, `SELECT count(*) FROM station.audit_events WHERE request_id=$1 AND action IN ('app.upgrade.published','app.upgrade.succeeded')`, out.RequestID).Scan(&audits) != nil || audits != 2 {
				t.Fatal("bridge publication/terminal audit mismatch")
			}
			return
		}
	}
	t.Fatal("cross-service transition did not finish")
}
