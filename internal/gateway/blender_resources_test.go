package gateway

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/verdantflarehub/verdantflare-station-core/internal/catalog"
	"github.com/verdantflarehub/verdantflare-station-core/internal/identity"
	"github.com/verdantflarehub/verdantflare-station-core/internal/telemetry"
	"github.com/verdantflarehub/verdantflare-station-core/internal/testdb"
	"github.com/verdantflarehub/verdantflare-station-core/migrations"
)

type blenderResourceReader struct{}

func (blenderResourceReader) ListPods(context.Context) ([]catalog.RawPodItem, error) {
	return []catalog.RawPodItem{}, nil
}
func (blenderResourceReader) ListPodMetrics(context.Context) ([]catalog.RawPodMetricItem, error) {
	return nil, nil
}

func TestBlenderWorkloadsGatewayPreservesGPUSamples(t *testing.T) {
	ctx := context.Background()
	pool := testdb.New(t)
	station := uuid.NewString()
	if err := migrations.Apply(ctx, pool, station); err != nil {
		t.Fatal(err)
	}
	ids, err := identity.New(pool, station, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	session, err := ids.Bootstrap(ctx, "test-bootstrap", identity.BootstrapRequest{Username: "test-manager", Password: strings.Repeat("x", 32), OrganizationName: "Test"})
	if err != nil {
		t.Fatal(err)
	}
	stamp := time.Now().UTC().Truncate(time.Millisecond)
	gpu := "GPU-12345678-1234-1234-1234-123456789abc"
	prom := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("query") != "DCGM_FI_DEV_FB_USED[35s]" {
			t.Error("unexpected query")
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": []any{map[string]any{"metric": map[string]string{"UUID": gpu}, "values": [][]any{{float64(stamp.UnixMilli()) / 1000, "829"}}}}}})
	}))
	defer prom.Close()
	service := telemetry.NewService(prom.URL, time.Second)
	service.SetKubernetes(blenderResourceReader{})
	server := &Server{Identity: ids, Telemetry: service, Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	request := httptest.NewRequest("GET", "/api/v1/resources/workloads", nil)
	request.Header.Set("Authorization", "Bearer "+session.AccessToken)
	request.Header.Set("X-Request-ID", "gpu-contract")
	response := httptest.NewRecorder()
	server.ServeHTTP(response, request)
	var body struct {
		telemetry.WorkloadsResponse
		RequestID string `json:"request_id"`
	}
	if response.Code != 200 || json.Unmarshal(response.Body.Bytes(), &body) != nil {
		t.Fatal("workloads HTTP failed", response.Code)
	}
	if body.SchemaVersion != 2 || body.RequestID != "gpu-contract" || len(body.GPUSamples) != 1 {
		t.Fatal("gateway dropped telemetry fields")
	}
	sample := body.GPUSamples[0]
	if sample.UUID != gpu || sample.Memory.Value == nil || *sample.Memory.Value != 829*1024*1024 || sample.Memory.Quality != "fresh" || sample.Memory.SampledAt == nil {
		t.Fatal("GPU sample lost identity, quality or measurement")
	}
}
