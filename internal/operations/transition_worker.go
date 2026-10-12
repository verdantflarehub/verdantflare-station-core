package operations

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
)

// The cursor schedules fairly; the accepted DB record owns all execution state.
// Returning the selected ID even on failure lets later installations progress.
func (s *Service) DispatchNextTransition(ctx context.Context, runtime TransitionRuntime, after string) (string, error) {
	if s == nil || s.Pool == nil || runtime == nil || !canonicalInstallation(s.StationID) || (after != "" && !canonicalInstallation(after)) {
		return after, ErrTransitionProgress
	}
	var operation string
	err := s.Pool.QueryRow(ctx, `SELECT operation_id::text FROM station.app_installation_transitions WHERE station_id=$1 AND status NOT IN ('succeeded','failed') ORDER BY CASE WHEN $2='' OR operation_id>NULLIF($2,'')::uuid THEN 0 ELSE 1 END,operation_id LIMIT 1`, s.StationID, after).Scan(&operation)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return after, err
	}
	_, err = s.DispatchTransition(ctx, operation, runtime)
	return operation, err
}

func (s *Service) RunTransitions(ctx context.Context, runtime TransitionRuntime, logger *slog.Logger) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	cursor := ""
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			step, cancel := context.WithTimeout(ctx, 30*time.Second)
			next, err := s.DispatchNextTransition(step, runtime, cursor)
			cancel()
			cursor = next
			if err != nil && !errors.Is(err, ErrTransitionBusy) && ctx.Err() == nil && logger != nil {
				logger.Warn("installation_transition_reconciliation_pending")
			}
			timer.Reset(time.Second)
		}
	}
}
