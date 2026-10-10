package telemetry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestGPUUsagePreservesScrapeTimeAndRejectsAmbiguity(t *testing.T) {
	now := time.Now().UTC()
	for _, mode := range []string{"zero", "stale", "future", "duplicate", "invalid", "sentinel", "missing"} {
		t.Run(mode, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("query") != "DCGM_FI_DEV_FB_USED[35s]" {
					t.Error("query loses scrape timestamp")
				}
				at := now.Add(-time.Second)
				value := "0"
				if mode == "stale" {
					at = now.Add(-31 * time.Second)
				}
				if mode == "future" {
					at = now.Add(10 * time.Second)
				}
				if mode == "invalid" {
					value = "NaN"
				}
				if mode == "sentinel" {
					value = "9223372036854775794"
				}
				row := map[string]any{"metric": map[string]string{"UUID": "GPU-12345678-1234-4234-8234-123456789abc"}, "values": [][]any{{float64(at.UnixMilli()) / 1000, value}}}
				rows := []any{row}
				if mode == "duplicate" {
					rows = append(rows, row)
				}
				if mode == "missing" {
					rows = nil
				}
				_ = json.NewEncoder(w).Encode(map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": rows}})
			}))
			defer server.Close()
			got := NewService(server.URL, time.Second).gpuUsageSamples(context.Background(), now)
			if mode == "zero" {
				if len(got) != 1 || got[0].Memory.Value == nil || *got[0].Memory.Value != 0 || got[0].Memory.Quality != "fresh" {
					t.Fatal(got)
				}
			} else if mode == "stale" {
				if len(got) != 1 || got[0].Memory.Value != nil || got[0].Memory.Quality != "stale" {
					t.Fatal(got)
				}
			} else if len(got) != 0 {
				t.Fatal("invalid GPU sample accepted", got)
			}
		})
	}
}
