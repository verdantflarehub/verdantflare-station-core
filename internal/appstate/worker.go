package appstate

import (
	"context"
	"log/slog"
	"time"
)

func (s *Service) RunExpiry(ctx context.Context, logger *slog.Logger) {
	timer := time.NewTicker(time.Second)
	defer timer.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		step, cancel := context.WithTimeout(ctx, 5*time.Second)
		err := s.Expire(step)
		cancel()
		if err != nil && ctx.Err() == nil && logger != nil {
			logger.Warn("app_state_expiry_unavailable")
		}
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
	}
}
