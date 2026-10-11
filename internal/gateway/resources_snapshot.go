package gateway

import (
	"context"
	"net/http"
	"time"

	"github.com/verdantflarehub/verdantflare-station-core/internal/telemetry"
)

func (s *Server) resourcesSnapshot(w http.ResponseWriter, r *http.Request, requestID string) {
	user, org, ok := s.delegatedApp(w, r, requestID, "resources")
	if !ok {
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 7*time.Second)
	defer cancel()
	var snapshot *telemetry.Snapshot
	err := s.AppAccess.manager(ctx, user, org, func(ctx context.Context) error {
		if s.Telemetry == nil {
			return context.Canceled
		}
		var err error
		snapshot, err = s.Telemetry.GetSnapshot(ctx)
		return err
	})
	if err != nil {
		failure(w, requestID, err)
		return
	}
	reply(w, 200, snapshot)
}
